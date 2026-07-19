use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    ffi::OsString,
    fs,
    io::{self, Read, Write},
    net::{Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use agent_lab_catalog_source::{AnalysisSource, CatalogSource, SourceObserver};
use agent_lab_driver_protocol::{
    CommandBody, ControllerCommand, DriverBody, DriverDescriptor, DriverLaunch, DriverProcess,
    DriverTranscript, PROTOCOL_VERSION, ProcessError, RawDriverMessage,
};
use axum::{
    Router,
    extract::{Request, State},
    http::{HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::Response,
};
use rand::Rng;
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use tokio::{net::TcpListener, sync::broadcast};
use tokio_util::sync::CancellationToken;

const DRIVER_POLL: Duration = Duration::from_millis(250);
// The extracted v0 adapter loads the production agent module graph before it
// can announce readiness. A cold TypeScript process can take over a minute on
// a development checkout, while subsequent protocol replies remain fast.
const DRIVER_READY_TIMEOUT: Duration = Duration::from_mins(2);
const DRIVER_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_EVIDENCE_FILE_BYTES: u64 = 8 * 1024 * 1024;
const MAX_EVIDENCE_TOTAL_BYTES: u64 = 64 * 1024 * 1024;
const CATALOG_REQUIRED_SOURCES: &[&str] = &["catalog", "analysis"];

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScenarioManifest {
    pub version: u32,
    pub id: String,
    pub title: String,
    pub description: String,
    pub question: String,
    pub seed: PathBuf,
    pub prompt: String,
    pub output: PathBuf,
    pub limits: ScenarioLimits,
    pub assertions: CatalogAssertions,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[allow(clippy::struct_field_names)]
pub struct ScenarioLimits {
    pub max_duration_ms: u64,
    pub max_command_count: u32,
    pub max_orchestrator_invocations: u32,
    pub max_tool_invocations: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogAssertions {
    pub active_names: Vec<String>,
    pub total_score: i64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartRunRequest {
    pub scenario_id: String,
    pub model_id: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrepareRunRequest {
    pub scenario_id: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartPreparedRunRequest {
    pub model_id: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum RunStatus {
    Exploring,
    Starting,
    Running,
    Passed,
    Failed,
    Cancelled,
}

impl RunStatus {
    #[must_use]
    pub fn is_finished(&self) -> bool {
        matches!(self, Self::Passed | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunSummary {
    pub id: String,
    pub scenario_id: String,
    pub scenario_title: String,
    pub model_id: String,
    pub status: RunStatus,
    pub started_at_ms: u128,
    pub finished_at_ms: Option<u128>,
    pub event_count: u64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunEvent {
    pub sequence: u64,
    pub at_ms: u128,
    #[serde(rename = "type")]
    pub kind: String,
    pub payload: JsonValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunReview {
    pub version: u32,
    pub status: RunStatus,
    pub metrics: ReviewMetrics,
    pub steps: Vec<ReviewStep>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewMetrics {
    pub model_turns: u32,
    pub capability_calls: u32,
    pub native_actions: u32,
    pub workspace_changes: u32,
    pub duration_ms: Option<u128>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewStep {
    pub ordinal: u32,
    pub kind: String,
    pub title: String,
    pub detail: Option<String>,
    pub status: String,
    pub event_sequences: Vec<u64>,
    pub source: Option<String>,
    pub path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssemblySnapshot {
    pub question: String,
    pub scenario: AssemblyScenario,
    pub harness: HarnessAssembly,
    pub workspace: WorkspaceAssembly,
    pub capability_sources: Vec<CapabilityAssembly>,
    pub limits: ScenarioLimits,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssemblyScenario {
    pub id: String,
    pub title: String,
    pub description: String,
    pub version: u32,
    #[serde(default)]
    pub output: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessAssembly {
    pub adapter: String,
    pub model_id: Option<String>,
    pub driver: Option<DriverDescriptor>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceAssembly {
    pub id: String,
    pub seed: PathBuf,
    pub seed_revision: String,
    pub attachment: String,
    pub change_tracking: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CapabilityAssembly {
    pub id: String,
    pub revision: String,
    pub protocol: String,
    pub projections: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunDetail {
    pub summary: RunSummary,
    pub assembly: AssemblySnapshot,
    pub review: RunReview,
    pub events: Vec<RunEvent>,
    pub score: Option<JsonValue>,
    pub output: Option<JsonValue>,
    pub output_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RunControllerConfig {
    pub scenarios_dir: PathBuf,
    pub data_dir: PathBuf,
    pub driver: DriverLaunch,
}

#[derive(Clone)]
pub struct RunController {
    inner: Arc<ControllerInner>,
}

struct ControllerInner {
    scenarios: BTreeMap<String, ScenarioManifest>,
    scenarios_dir: PathBuf,
    data_dir: PathBuf,
    driver: DriverLaunch,
    runs: Mutex<HashMap<String, Arc<RunState>>>,
    prepare_lock: tokio::sync::Mutex<()>,
}

impl Drop for ControllerInner {
    fn drop(&mut self) {
        let runs = self
            .runs
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for state in runs.values() {
            state.cancel.cancel();
            if let Ok(capabilities) = state.capabilities.lock() {
                for capability in capabilities.iter() {
                    capability.cancel.cancel();
                }
            }
        }
    }
}

struct RunState {
    summary: Mutex<RunSummary>,
    assembly: Mutex<AssemblySnapshot>,
    events: Mutex<Vec<RunEvent>>,
    sender: broadcast::Sender<RunEvent>,
    cancel: CancellationToken,
    bundle_dir: PathBuf,
    workspace: PathBuf,
    output: PathBuf,
    initial_snapshot: Option<BTreeMap<String, Vec<u8>>>,
    capabilities: Mutex<Vec<CapabilityEndpoint>>,
    secret_values: Mutex<Vec<Vec<u8>>>,
    replay_failed: bool,
}

struct RunCompletion {
    status: RunStatus,
    error: Option<String>,
    score: JsonValue,
}

enum ExitWait {
    Exited(Option<i32>),
    Cancelled,
}

impl Drop for RunState {
    fn drop(&mut self) {
        if let Ok(capabilities) = self.capabilities.get_mut() {
            for capability in capabilities.iter() {
                capability.cancel.cancel();
            }
        }
    }
}

#[derive(Clone)]
struct CapabilityEndpoint {
    id: String,
    revision: String,
    human_url: String,
    human_token: String,
    agent_url: String,
    agent_token: String,
    cancel: CancellationToken,
}

/// The private connection details needed to attach a human shell to a run.
#[derive(Debug, Clone)]
pub struct TerminalBinding {
    pub workspace: PathBuf,
    pub sources: Vec<TerminalCapabilityBinding>,
}

/// One authenticated MCP source attached to a human shell.
#[derive(Debug, Clone)]
pub struct TerminalCapabilityBinding {
    pub id: String,
    pub url: String,
    pub token: String,
}

impl RunController {
    /// Load checked-in scenarios and prepare the local evidence store.
    ///
    /// # Errors
    ///
    /// Returns an error for an unreadable or invalid scenario directory.
    pub fn new(config: RunControllerConfig) -> Result<Self, RunError> {
        let scenarios_dir = canonical_directory(&config.scenarios_dir)?;
        fs::create_dir_all(&config.data_dir)?;
        let data_dir = fs::canonicalize(&config.data_dir)?;
        let scenarios = load_scenarios(&scenarios_dir)?;
        if scenarios.is_empty() {
            return Err(RunError::InvalidScenario(
                "scenario directory contains no TOML manifests".to_owned(),
            ));
        }
        let runs = load_runs(&data_dir, &scenarios)?;
        Ok(Self {
            inner: Arc::new(ControllerInner {
                scenarios,
                scenarios_dir,
                data_dir,
                driver: config.driver,
                runs: Mutex::new(runs),
                prepare_lock: tokio::sync::Mutex::new(()),
            }),
        })
    }

    #[must_use]
    pub fn scenarios(&self) -> Vec<ScenarioManifest> {
        self.inner.scenarios.values().cloned().collect()
    }

    #[must_use]
    pub fn list(&self) -> Vec<RunSummary> {
        let mut runs = lock(&self.inner.runs)
            .values()
            .map(|run| lock(&run.summary).clone())
            .filter(|run| run.status != RunStatus::Exploring)
            .collect::<Vec<_>>();
        runs.sort_by_key(|run| std::cmp::Reverse(run.started_at_ms));
        runs
    }

    /// Read one run and its persisted evidence projection.
    ///
    /// # Errors
    ///
    /// Returns an error when the run is unknown or its stored JSON cannot be read.
    pub fn get(&self, id: &str) -> Result<RunDetail, RunError> {
        let state = self.state(id)?;
        let summary = lock(&state.summary).clone();
        let events = lock(&state.events).clone();
        let score = read_optional_json(&state.bundle_dir.join("score.json"))?;
        let review = if summary.status.is_finished() && !state.replay_failed {
            match read_optional_json(&state.bundle_dir.join("review.json"))? {
                Some(value) => serde_json::from_value(value)?,
                None => build_review(&summary, &events),
            }
        } else {
            build_review(&summary, &events)
        };
        let evidence_root = if summary.status.is_finished() {
            state.bundle_dir.join("final")
        } else {
            state.workspace.clone()
        };
        let secret_values = lock(&state.secret_values).clone();
        let (output, output_error) =
            match read_optional_confined_json(&evidence_root, &state.output) {
                Ok(output) => (
                    output.map(|value| redact_value(value, &secret_values)),
                    None,
                ),
                Err(error) => (None, Some(error.to_string())),
            };
        Ok(RunDetail {
            summary,
            assembly: lock(&state.assembly).clone(),
            review,
            events,
            score,
            output,
            output_error,
        })
    }

    /// Subscribe to new events after returning the run's recorded event prefix.
    ///
    /// # Errors
    ///
    /// Returns an error when the run is unknown.
    pub fn subscribe(
        &self,
        id: &str,
    ) -> Result<(Vec<RunEvent>, broadcast::Receiver<RunEvent>), RunError> {
        let state = self.state(id)?;
        // `record_event` sends only after releasing this lock. Creating the receiver while the
        // prefix is locked makes every event belong to exactly one side of the handoff.
        let events = lock(&state.events);
        let receiver = state.sender.subscribe();
        Ok((events.clone(), receiver))
    }

    /// Re-read the durable event suffix after a streaming receiver reports lag.
    ///
    /// # Errors
    ///
    /// Returns an error when the run is unknown.
    pub fn events_after(&self, id: &str, sequence: u64) -> Result<Vec<RunEvent>, RunError> {
        let state = self.state(id)?;
        Ok(lock(&state.events)
            .iter()
            .filter(|event| event.sequence > sequence)
            .cloned()
            .collect())
    }

    /// Request cancellation for an active run.
    ///
    /// # Errors
    ///
    /// Returns an error when the run is unknown.
    pub fn cancel(&self, id: &str) -> Result<(), RunError> {
        let state = self.state(id)?;
        let cancel_prepared = {
            let mut summary = lock(&state.summary);
            if summary.status == RunStatus::Exploring {
                // Claim the transition before releasing the lock so a concurrent start cannot
                // attach a driver to a workspace that cancellation is finalizing.
                summary.status = RunStatus::Cancelled;
                true
            } else {
                false
            }
        };
        if cancel_prepared {
            return finish_run(
                &state,
                RunStatus::Cancelled,
                None,
                &json!({ "passed": false, "cancelled": true }),
            );
        }
        state.cancel.cancel();
        Ok(())
    }

    /// Return the root-confined physical workspace for a run.
    ///
    /// # Errors
    ///
    /// Returns an error when the run is unknown.
    pub fn workspace(&self, id: &str) -> Result<PathBuf, RunError> {
        Ok(self.state(id)?.workspace.clone())
    }

    /// Return the workspace and authenticated MCP endpoint for a human shell.
    ///
    /// # Errors
    ///
    /// Returns an error when the run is unknown or its capability source is unavailable.
    pub fn terminal_binding(&self, id: &str) -> Result<TerminalBinding, RunError> {
        let state = self.state(id)?;
        let capabilities = lock(&state.capabilities).clone();
        if capabilities.is_empty() {
            return Err(RunError::RunUnavailable(id.to_owned()));
        }
        Ok(TerminalBinding {
            workspace: state.workspace.clone(),
            sources: capabilities
                .into_iter()
                .map(|capability| TerminalCapabilityBinding {
                    id: capability.id,
                    url: capability.human_url,
                    token: capability.human_token,
                })
                .collect(),
        })
    }

    /// Prepare one scenario workspace and its controller-owned capability sources for exploration.
    ///
    /// Reuses the unfinished exploration for a scenario so a page reload reconnects to the same
    /// workspace instead of creating hidden fixture state.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown scenario, unsafe path, I/O failure, or capability-source
    /// startup failure.
    pub async fn prepare(&self, request: PrepareRunRequest) -> Result<RunSummary, RunError> {
        let scenario = self
            .inner
            .scenarios
            .get(&request.scenario_id)
            .cloned()
            .ok_or_else(|| RunError::UnknownScenario(request.scenario_id.clone()))?;
        let _prepare = self.inner.prepare_lock.lock().await;
        let existing = lock(&self.inner.runs)
            .values()
            .find(|state| {
                let summary = lock(&state.summary);
                summary.scenario_id == scenario.id && summary.status == RunStatus::Exploring
            })
            .cloned();
        if let Some(state) = existing {
            lock(&state.secret_values).clone_from(&driver_secret_values(&self.inner.driver));
            let attached_capabilities = lock(&state.capabilities).clone();
            extend_capability_secrets(&state, &attached_capabilities);
            if attached_capabilities.is_empty() {
                let capabilities = start_capability_sources(state.clone()).await?;
                extend_capability_secrets(&state, &capabilities);
                lock(&state.capabilities).clone_from(&capabilities);
                update_assembly_capabilities(&state, &capabilities)?;
            }
            return Ok(lock(&state.summary).clone());
        }

        let id = run_id();
        let bundle_dir = confined_child(&self.inner.data_dir, &id)?;
        fs::create_dir(&bundle_dir)?;
        let workspace = bundle_dir.join("workspace");
        let seed = confined_existing_child(&self.inner.scenarios_dir, &scenario.seed)?;
        let initial_snapshot = snapshot_tree(&seed)?;
        copy_tree(&seed, &workspace)?;
        copy_tree(&seed, &bundle_dir.join("initial"))?;

        let summary = RunSummary {
            id: id.clone(),
            scenario_id: scenario.id.clone(),
            scenario_title: scenario.title.clone(),
            model_id: String::new(),
            status: RunStatus::Exploring,
            started_at_ms: now_ms(),
            finished_at_ms: None,
            event_count: 0,
            error: None,
        };
        let (sender, _) = broadcast::channel(256);
        let assembly = initial_assembly(&summary, &scenario);
        let state = Arc::new(RunState {
            summary: Mutex::new(summary),
            assembly: Mutex::new(assembly),
            events: Mutex::new(Vec::new()),
            sender,
            cancel: CancellationToken::new(),
            bundle_dir,
            workspace,
            output: scenario.output.clone(),
            initial_snapshot: Some(initial_snapshot),
            capabilities: Mutex::new(Vec::new()),
            secret_values: Mutex::new(driver_secret_values(&self.inner.driver)),
            replay_failed: false,
        });
        lock(&self.inner.runs).insert(id, state.clone());
        persist_manifest(&state)?;
        persist_assembly(&state)?;
        record_event(&state, "run.prepared", json!({ "scenario": scenario.id }))?;
        let capabilities = start_capability_sources(state.clone()).await?;
        extend_capability_secrets(&state, &capabilities);
        lock(&state.capabilities).clone_from(&capabilities);
        update_assembly_capabilities(&state, &capabilities)?;
        Ok(lock(&state.summary).clone())
    }

    /// Start the agent driver in an already-explorable scenario workspace.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid input, an unknown run, or a run that has already started.
    pub fn start_prepared(
        &self,
        id: &str,
        request: StartPreparedRunRequest,
    ) -> Result<RunSummary, RunError> {
        validate_model_id(&request.model_id)?;
        let state = self.state(id)?;
        let capabilities = lock(&state.capabilities).clone();
        if capabilities.is_empty() {
            return Err(RunError::RunUnavailable(id.to_owned()));
        }
        let scenario = {
            let mut summary = lock(&state.summary);
            if summary.status != RunStatus::Exploring {
                return Err(RunError::RunUnavailable(id.to_owned()));
            }
            let scenario = self
                .inner
                .scenarios
                .get(&summary.scenario_id)
                .cloned()
                .ok_or_else(|| RunError::UnknownScenario(summary.scenario_id.clone()))?;
            summary.model_id.clone_from(&request.model_id);
            summary.status = RunStatus::Starting;
            scenario
        };
        lock(&state.assembly).harness.model_id = Some(request.model_id);
        let persist_start = (|| -> Result<(), RunError> {
            persist_manifest(&state)?;
            persist_assembly(&state)?;
            record_event(
                &state,
                "run.status",
                json!({ "status": RunStatus::Starting }),
            )
        })();
        if let Err(error) = persist_start {
            rollback_prepared_start(&state);
            return Err(error);
        }
        let summary = lock(&state.summary).clone();
        let driver = self.inner.driver.clone();
        tokio::task::spawn_blocking(move || {
            execute_run(&state, &scenario, driver, &capabilities);
        });
        Ok(summary)
    }

    /// Create an explorable scenario workspace and immediately start its agent driver.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid input, unknown scenarios, unsafe paths, I/O failures, or
    /// capability-source startup failures.
    pub async fn start(&self, request: StartRunRequest) -> Result<RunSummary, RunError> {
        validate_model_id(&request.model_id)?;
        let prepared = self
            .prepare(PrepareRunRequest {
                scenario_id: request.scenario_id,
            })
            .await?;
        self.start_prepared(
            &prepared.id,
            StartPreparedRunRequest {
                model_id: request.model_id,
            },
        )
    }

    fn state(&self, id: &str) -> Result<Arc<RunState>, RunError> {
        lock(&self.inner.runs)
            .get(id)
            .cloned()
            .ok_or_else(|| RunError::UnknownRun(id.to_owned()))
    }
}

fn rollback_prepared_start(state: &RunState) {
    {
        let mut summary = lock(&state.summary);
        summary.status = RunStatus::Exploring;
        summary.model_id.clear();
    }
    lock(&state.assembly).harness.model_id = None;
    let _ = persist_manifest(state);
    let _ = persist_assembly(state);
    let _ = record_event(
        state,
        "run.status",
        json!({ "status": RunStatus::Exploring, "reason": "start persistence failed" }),
    );
}

fn validate_model_id(model_id: &str) -> Result<(), RunError> {
    if model_id.trim().is_empty() || model_id.len() > 200 {
        return Err(RunError::InvalidRequest(
            "modelId must be between 1 and 200 characters".to_owned(),
        ));
    }
    Ok(())
}

fn validate_turn_identity(
    actual_session: &str,
    actual_turn: &str,
    expected_session: &str,
    expected_turn: &str,
    message_kind: &str,
) -> Result<(), RunError> {
    if actual_session == expected_session && actual_turn == expected_turn {
        return Ok(());
    }
    Err(RunError::Protocol(format!(
        "{message_kind} identity {actual_session}/{actual_turn} does not match active turn {expected_session}/{expected_turn}"
    )))
}

fn configured_limit_error(
    events: &[RunEvent],
    after_sequence: u64,
    limits: &ScenarioLimits,
) -> Option<String> {
    let relevant = events
        .iter()
        .filter(|event| event.sequence > after_sequence);
    let mut commands = HashSet::new();
    let mut orchestrator_invocations = 0_u32;
    let mut tool_invocations = 0_u32;
    for event in relevant {
        if is_native_action_event(&event.kind, &event.payload) {
            commands.insert(json_string(&event.payload, "id").map_or_else(
                || format!("{}:{}", event.kind, event.sequence),
                str::to_owned,
            ));
        }
        if event.kind.ends_with(".turn-start") || event.kind == "model.turn.started" {
            orchestrator_invocations = orchestrator_invocations.saturating_add(1);
        }
        if (event.kind == "mcp.tool.started" && capability_event_is_agent(&event.payload))
            || event.kind == "tool.call"
        {
            tool_invocations = tool_invocations.saturating_add(1);
        }
    }
    let command_count = u32::try_from(commands.len()).unwrap_or(u32::MAX);
    [
        ("command", command_count, limits.max_command_count),
        (
            "orchestrator invocation",
            orchestrator_invocations,
            limits.max_orchestrator_invocations,
        ),
        (
            "tool invocation",
            tool_invocations,
            limits.max_tool_invocations,
        ),
    ]
    .into_iter()
    .find_map(|(name, actual, maximum)| {
        (actual > maximum).then(|| {
            format!("scenario exceeded max {name} count: observed {actual}, allowed {maximum}")
        })
    })
}

async fn start_capability_sources(
    state: Arc<RunState>,
) -> Result<Vec<CapabilityEndpoint>, RunError> {
    let catalog = start_mcp_source(
        state.clone(),
        "catalog",
        "catalog-v2",
        CatalogSource::new(source_observer(state.clone(), "catalog", "human")),
        CatalogSource::new(source_observer(state.clone(), "catalog", "agent")),
    )
    .await?;
    let analysis = match start_mcp_source(
        state.clone(),
        "analysis",
        "analysis-v1",
        AnalysisSource::new(source_observer(state.clone(), "analysis", "human")),
        AnalysisSource::new(source_observer(state, "analysis", "agent")),
    )
    .await
    {
        Ok(analysis) => analysis,
        Err(error) => {
            catalog.cancel.cancel();
            return Err(error);
        }
    };
    Ok(vec![catalog, analysis])
}

fn source_observer(
    state: Arc<RunState>,
    source: &'static str,
    actor: &'static str,
) -> SourceObserver {
    Arc::new(move |kind, mut payload| {
        if let Some(payload) = payload.as_object_mut() {
            payload.insert("source".to_owned(), JsonValue::String(source.to_owned()));
            payload.insert("actor".to_owned(), JsonValue::String(actor.to_owned()));
        }
        let secrets = lock(&state.secret_values).clone();
        payload = redact_value(payload, &secrets);
        let _ = record_event(&state, kind, payload);
    })
}

async fn start_mcp_source<S>(
    state: Arc<RunState>,
    id: &'static str,
    revision: &'static str,
    human_source: S,
    agent_source: S,
) -> Result<CapabilityEndpoint, RunError>
where
    S: rmcp::ServerHandler + Clone + Send + Sync + 'static,
{
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let address = listener.local_addr()?;
    let human_token = random_token();
    let agent_token = random_token();
    let human_service: StreamableHttpService<S, LocalSessionManager> = StreamableHttpService::new(
        move || Ok(human_source.clone()),
        Arc::new(LocalSessionManager::default()),
        StreamableHttpServerConfig::default().with_allowed_hosts([address.to_string()]),
    );
    let agent_service: StreamableHttpService<S, LocalSessionManager> = StreamableHttpService::new(
        move || Ok(agent_source.clone()),
        Arc::new(LocalSessionManager::default()),
        StreamableHttpServerConfig::default().with_allowed_hosts([address.to_string()]),
    );
    let human =
        Router::new()
            .nest_service("/mcp", human_service)
            .layer(middleware::from_fn_with_state(
                human_token.clone(),
                source_authorization,
            ));
    let agent =
        Router::new()
            .nest_service("/mcp", agent_service)
            .layer(middleware::from_fn_with_state(
                agent_token.clone(),
                source_authorization,
            ));
    let app = Router::new().nest("/human", human).nest("/agent", agent);
    let cancel = CancellationToken::new();
    let server_cancel = cancel.clone();
    record_event(
        &state,
        "capability.source.started",
        json!({ "id": id, "revision": revision, "transport": "streamable-http" }),
    )?;
    let server_state = state.clone();
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app)
            .with_graceful_shutdown(server_cancel.cancelled_owned())
            .await
        {
            let _ = record_event(
                &server_state,
                "mcp.server.failed",
                json!({ "message": error.to_string() }),
            );
        }
    });
    Ok(CapabilityEndpoint {
        id: id.to_owned(),
        revision: revision.to_owned(),
        human_url: format!("http://{address}/human/mcp"),
        human_token,
        agent_url: format!("http://{address}/agent/mcp"),
        agent_token,
        cancel,
    })
}

async fn source_authorization(
    State(token): State<String>,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let expected = format!("Bearer {token}");
    let authorized = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == expected);
    if !authorized {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(next.run(request).await)
}

fn execute_run(
    state: &Arc<RunState>,
    scenario: &ScenarioManifest,
    driver_launch: DriverLaunch,
    capabilities: &[CapabilityEndpoint],
) {
    if let Err(error) = run_driver(state, scenario, driver_launch, capabilities) {
        if lock(&state.summary).status.is_finished() {
            return;
        }
        let message = redacted_run_error(state, &error);
        let score = json!({ "passed": false });
        let _ = finish_run(state, RunStatus::Failed, Some(&message), &score);
    }
}

fn redacted_run_error(state: &RunState, error: &RunError) -> String {
    redact_string(&error.to_string(), &lock(&state.secret_values).clone())
}

fn driver_secret_values(driver_launch: &DriverLaunch) -> Vec<Vec<u8>> {
    driver_launch
        .env
        .iter()
        .map(|(_, value)| value.to_string_lossy().as_bytes().to_vec())
        .filter(|value| value.len() >= 4)
        .collect()
}

fn extend_capability_secrets(state: &RunState, capabilities: &[CapabilityEndpoint]) {
    lock(&state.secret_values).extend(
        capabilities
            .iter()
            .flat_map(|capability| [&capability.human_token, &capability.agent_token])
            .map(|token| token.as_bytes().to_vec()),
    );
}

fn receive_with_cancellation(
    driver: &mut DriverProcess,
    timeout: Duration,
    cancel: &CancellationToken,
) -> Result<Option<RawDriverMessage>, ProcessError> {
    let deadline = Instant::now() + timeout;
    loop {
        if cancel.is_cancelled() {
            return Ok(None);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(ProcessError::Timeout);
        }
        match driver.receive(remaining.min(DRIVER_POLL)) {
            Ok(message) => return Ok(Some(message)),
            Err(ProcessError::Timeout) => {}
            Err(error) => return Err(error),
        }
    }
}

fn wait_for_exit_with_cancellation(
    driver: &mut DriverProcess,
    timeout: Duration,
    cancel: &CancellationToken,
) -> Result<ExitWait, ProcessError> {
    let deadline = Instant::now() + timeout;
    loop {
        if cancel.is_cancelled() {
            return Ok(ExitWait::Cancelled);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(ProcessError::ExitTimeout);
        }
        match driver.wait_for_exit(remaining.min(DRIVER_POLL)) {
            Ok(exit_code) => return Ok(ExitWait::Exited(exit_code)),
            Err(ProcessError::ExitTimeout) => {}
            Err(error) => return Err(error),
        }
    }
}

fn cancelled_completion() -> RunCompletion {
    RunCompletion {
        status: RunStatus::Cancelled,
        error: None,
        score: json!({ "passed": false, "cancelled": true }),
    }
}

#[allow(clippy::too_many_lines)]
fn run_driver(
    state: &Arc<RunState>,
    scenario: &ScenarioManifest,
    driver_launch: DriverLaunch,
    capabilities: &[CapabilityEndpoint],
) -> Result<(), RunError> {
    update_status(state, RunStatus::Running)?;
    record_event(state, "driver.starting", JsonValue::Null)?;
    let mut secret_values = driver_secret_values(&driver_launch);
    secret_values.extend(
        capabilities
            .iter()
            .flat_map(|capability| [&capability.human_token, &capability.agent_token])
            .map(|token| token.as_bytes().to_vec()),
    );
    lock(&state.secret_values).clone_from(&secret_values);
    let mut driver = DriverProcess::spawn_with(driver_launch)?;
    let result = (|| -> Result<RunCompletion, RunError> {
        let Some(ready) =
            receive_with_cancellation(&mut driver, DRIVER_READY_TIMEOUT, &state.cancel)?
        else {
            return Ok(cancelled_completion());
        };
        let DriverBody::Ready { driver: descriptor } = ready.parsed.body else {
            return Err(RunError::Protocol("expected driver.ready".to_owned()));
        };
        let descriptor = redact_driver_descriptor(descriptor, &secret_values);
        lock(&state.assembly).harness.driver = Some(descriptor.clone());
        persist_assembly(state)?;
        record_event(state, "driver.ready", serde_json::to_value(&descriptor)?)?;

        let summary = lock(&state.summary).clone();
        let session_id = format!("{}-session", summary.id);
        let turn_id = format!("{}-turn", summary.id);
        let capability_sources = capabilities
            .iter()
            .map(|capability| {
                json!({
                    "type": "mcp",
                    "id": capability.id,
                    "revision": capability.revision,
                    "transport": {
                        "type": "http",
                        "url": capability.agent_url,
                        "headers": { "Authorization": format!("Bearer {}", capability.agent_token) }
                    }
                })
            })
            .collect::<Vec<_>>();
        driver.send(&command(
            "run-open",
            CommandBody::OpenSession {
                session_id: session_id.clone(),
                config: json!({
                    "files": {},
                    "modelId": summary.model_id,
                    "workspaceRoot": state.workspace,
                }),
                limits: serde_json::to_value(&scenario.limits)?,
            },
        ))?;
        let Some(opened) =
            receive_with_cancellation(&mut driver, DRIVER_RESPONSE_TIMEOUT, &state.cancel)?
        else {
            return Ok(cancelled_completion());
        };
        match opened.parsed.body {
            DriverBody::SessionOpened {
                session_id: opened_session,
                ..
            } if opened_session == session_id => {}
            DriverBody::Failed { code, message, .. } => {
                return Err(RunError::Protocol(format!(
                    "driver failed while opening session: {code}: {message}"
                )));
            }
            _ => {
                return Err(RunError::Protocol(
                    "expected session.opened for the requested session".to_owned(),
                ));
            }
        }
        record_event(state, "driver.session-opened", JsonValue::Null)?;

        driver.send(&command(
            "run-turn",
            CommandBody::StartTurn {
                session_id: session_id.clone(),
                turn_id: turn_id.clone(),
                task: json!({
                    "mode": "real",
                    "prompt": scenario.prompt,
                    "turns": [],
                }),
                capability_sources: JsonValue::Array(capability_sources),
            },
        ))?;
        record_event(
            state,
            "driver.turn-started",
            json!({ "sessionId": session_id, "turnId": turn_id }),
        )?;
        let turn_start_sequence = lock(&state.events).last().map_or(0, |event| event.sequence);

        let mut outcome = None;
        let mut evidence = JsonValue::Null;
        let mut abort_sent = false;
        let mut timed_out = false;
        let mut limit_error = None;
        let started = Instant::now();
        let mut abort_sent_at = None;
        while outcome.is_none() {
            if started.elapsed() >= Duration::from_millis(scenario.limits.max_duration_ms) {
                timed_out = true;
            }
            if limit_error.is_none() {
                limit_error = configured_limit_error(
                    &lock(&state.events),
                    turn_start_sequence,
                    &scenario.limits,
                );
            }
            if (state.cancel.is_cancelled() || timed_out || limit_error.is_some()) && !abort_sent {
                let reason = limit_error.clone().unwrap_or_else(|| {
                    if timed_out {
                        "scenario execution limit exceeded".to_owned()
                    } else {
                        "cancelled from Agent Lab".to_owned()
                    }
                });
                if limit_error.is_some() {
                    record_event(
                        state,
                        "controller.limit-exceeded",
                        json!({ "message": reason }),
                    )?;
                }
                driver.send(&command(
                    "run-abort",
                    CommandBody::AbortTurn {
                        session_id: session_id.clone(),
                        turn_id: turn_id.clone(),
                        reason: Some(reason),
                    },
                ))?;
                abort_sent = true;
                abort_sent_at = Some(Instant::now());
            }
            if abort_sent_at.is_some_and(|sent| sent.elapsed() >= Duration::from_secs(10)) {
                return Err(RunError::Protocol(
                    "driver did not finish within 10 seconds of abort".to_owned(),
                ));
            }
            match driver.receive(DRIVER_POLL) {
                Ok(message) => match message.parsed.body {
                    DriverBody::TurnEvent {
                        session_id: event_session,
                        turn_id: event_turn,
                        event_type,
                        payload,
                    } => {
                        validate_turn_identity(
                            &event_session,
                            &event_turn,
                            &session_id,
                            &turn_id,
                            "turn.event",
                        )?;
                        record_event(
                            state,
                            &driver_event_kind(&event_type),
                            redact_value(payload, &secret_values),
                        )?;
                    }
                    DriverBody::TurnFinished {
                        session_id: finished_session,
                        turn_id: finished_turn,
                        outcome: result,
                        evidence: result_evidence,
                    } => {
                        validate_turn_identity(
                            &finished_session,
                            &finished_turn,
                            &session_id,
                            &turn_id,
                            "turn.finished",
                        )?;
                        record_event(
                            state,
                            "driver.turn-finished",
                            json!({ "sessionId": finished_session, "turnId": finished_turn, "outcome": result }),
                        )?;
                        outcome = Some(result);
                        evidence = result_evidence;
                    }
                    DriverBody::Failed { code, message, .. } => {
                        return Err(RunError::Protocol(format!(
                            "driver failed: {code}: {message}"
                        )));
                    }
                    _ => {}
                },
                Err(agent_lab_driver_protocol::ProcessError::Timeout) => {}
                Err(error) => return Err(error.into()),
            }
        }

        driver.send(&command(
            "run-close",
            CommandBody::CloseSession {
                session_id: session_id.clone(),
            },
        ))?;
        let Some(closed) =
            receive_with_cancellation(&mut driver, DRIVER_RESPONSE_TIMEOUT, &state.cancel)?
        else {
            return Ok(cancelled_completion());
        };
        match closed.parsed.body {
            DriverBody::SessionClosed {
                session_id: closed_session,
            } if closed_session == session_id => {}
            DriverBody::Failed { code, message, .. } => {
                return Err(RunError::Protocol(format!(
                    "driver failed while closing session: {code}: {message}"
                )));
            }
            _ => {
                return Err(RunError::Protocol(
                    "expected session.closed for the active session".to_owned(),
                ));
            }
        }
        let exit_code = match wait_for_exit_with_cancellation(
            &mut driver,
            DRIVER_RESPONSE_TIMEOUT,
            &state.cancel,
        )? {
            ExitWait::Exited(exit_code) => exit_code,
            ExitWait::Cancelled => return Ok(cancelled_completion()),
        };
        require_successful_driver_exit(exit_code)?;
        write_json_atomic(
            &state.bundle_dir.join("evidence.json"),
            &redact_value(evidence, &secret_values),
        )?;

        if timed_out {
            let message = format!(
                "scenario exceeded its {} ms execution limit",
                scenario.limits.max_duration_ms
            );
            return Ok(RunCompletion {
                status: RunStatus::Failed,
                error: Some(message),
                score: json!({ "passed": false, "timedOut": true }),
            });
        }
        if let Some(message) = limit_error {
            return Ok(RunCompletion {
                status: RunStatus::Failed,
                error: Some(message),
                score: json!({ "passed": false, "limitExceeded": true }),
            });
        }
        if abort_sent || outcome.as_deref() == Some("aborted") {
            return Ok(RunCompletion {
                status: RunStatus::Cancelled,
                error: None,
                score: json!({ "passed": false, "cancelled": true }),
            });
        }
        let score = score_catalog(state, scenario)?;
        let passed =
            score["passed"].as_bool() == Some(true) && outcome.as_deref() == Some("completed");
        Ok(RunCompletion {
            status: if passed {
                RunStatus::Passed
            } else {
                RunStatus::Failed
            },
            error: None,
            score,
        })
    })();
    let transcript = redact_transcript(driver.transcript(), &secret_values);
    let transcript_result = (|| -> Result<(), RunError> {
        fs::write(
            state.bundle_dir.join("driver.stderr.log"),
            &transcript.driver_stderr,
        )?;
        write_json_atomic(
            &state.bundle_dir.join("driver.json"),
            &serde_json::to_value(transcript)?,
        )
    })();
    drop(driver);
    let completion = result?;
    transcript_result?;
    finish_run(
        state,
        completion.status,
        completion.error.as_deref(),
        &completion.score,
    )
}

fn require_successful_driver_exit(exit_code: Option<i32>) -> Result<(), RunError> {
    if exit_code == Some(0) {
        Ok(())
    } else {
        Err(RunError::Protocol(format!(
            "driver exited unsuccessfully after session.close: {exit_code:?}"
        )))
    }
}

fn driver_event_kind(event_type: &str) -> String {
    const CONTROLLER_PREFIXES: &[&str] = &["controller.", "driver.", "mcp.", "run.", "workspace."];
    if CONTROLLER_PREFIXES
        .iter()
        .any(|prefix| event_type.starts_with(prefix))
    {
        format!("driver.event.{event_type}")
    } else {
        event_type.to_owned()
    }
}

fn redact_driver_descriptor(
    mut descriptor: DriverDescriptor,
    secrets: &[Vec<u8>],
) -> DriverDescriptor {
    descriptor.name = redact_string(&descriptor.name, secrets);
    descriptor.version = redact_string(&descriptor.version, secrets);
    descriptor.revision = descriptor
        .revision
        .map(|revision| redact_string(&revision, secrets));
    descriptor.features = descriptor
        .features
        .into_iter()
        .map(|feature| redact_string(&feature, secrets))
        .collect();
    descriptor
}

fn score_catalog(state: &RunState, scenario: &ScenarioManifest) -> Result<JsonValue, RunError> {
    let output = read_optional_confined_json(&state.workspace, &scenario.output)?;
    let schema_valid = output.as_ref().is_some_and(catalog_output_schema_valid);
    let active_names = output
        .as_ref()
        .and_then(|value| value.get("active"))
        .and_then(JsonValue::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.get("name").and_then(JsonValue::as_str))
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let total_score = output
        .as_ref()
        .and_then(|value| value.get("totalScore"))
        .and_then(JsonValue::as_i64);
    let names_match = active_names == scenario.assertions.active_names;
    let score_matches = total_score == Some(scenario.assertions.total_score);
    let capability_sources_used = lock(&state.events)
        .iter()
        .filter(|event| {
            event.kind == "mcp.tool.completed"
                && event.payload["actor"].as_str() == Some("agent")
                && event.payload["isError"].as_bool() != Some(true)
        })
        .filter_map(|event| event.payload["source"].as_str().map(str::to_owned))
        .collect::<std::collections::BTreeSet<_>>();
    let capability_evidence_complete = CATALOG_REQUIRED_SOURCES
        .iter()
        .all(|source| capability_sources_used.contains(*source));
    let analysis_result = catalog_analysis_result(&lock(&state.events));
    let catalog_analysis_composed = analysis_result.is_some();
    let analysis_result_matches = analysis_result
        .as_ref()
        .is_some_and(|expected| output.as_ref() == Some(expected));
    Ok(json!({
        "passed": output.is_some()
            && schema_valid
            && names_match
            && score_matches
            && capability_evidence_complete
            && catalog_analysis_composed
            && analysis_result_matches,
        "outputPresent": output.is_some(),
        "schemaValid": schema_valid,
        "activeNames": active_names,
        "expectedActiveNames": scenario.assertions.active_names,
        "namesMatch": names_match,
        "totalScore": total_score,
        "expectedTotalScore": scenario.assertions.total_score,
        "scoreMatches": score_matches,
        "capabilitySourcesUsed": capability_sources_used,
        "expectedCapabilitySources": CATALOG_REQUIRED_SOURCES,
        "capabilityEvidenceComplete": capability_evidence_complete,
        "catalogAnalysisComposed": catalog_analysis_composed,
        "analysisResultMatches": analysis_result_matches,
    }))
}

fn catalog_analysis_result(events: &[RunEvent]) -> Option<JsonValue> {
    let mut catalog_results = Vec::new();
    for event in events {
        let is_agent = event.payload["actor"].as_str() == Some("agent");
        let source = event.payload["source"].as_str();
        let name = event.payload["name"].as_str();
        if event.kind == "mcp.tool.completed"
            && is_agent
            && source == Some("catalog")
            && name == Some("list")
            && event.payload["isError"].as_bool() != Some(true)
        {
            if let Some(items) = event.payload["result"]["items"].as_array() {
                catalog_results.push(items.clone());
            }
        } else if event.kind == "mcp.tool.completed"
            && is_agent
            && source == Some("analysis")
            && name == Some("summarize")
            && event.payload["isError"].as_bool() != Some(true)
        {
            let Some(items) = event.payload["arguments"]["items"].as_array() else {
                continue;
            };
            if catalog_results.iter().any(|result| result == items) {
                return event
                    .payload
                    .get("result")
                    .filter(|result| result.is_object())
                    .cloned();
            }
        }
    }
    None
}

fn catalog_output_schema_valid(output: &JsonValue) -> bool {
    let Some(object) = output.as_object() else {
        return false;
    };
    if object.len() != 3
        || !object.contains_key("active")
        || !object.contains_key("activeCount")
        || !object.contains_key("totalScore")
    {
        return false;
    }
    let Some(active) = object.get("active").and_then(JsonValue::as_array) else {
        return false;
    };
    let Some(active_count) = object.get("activeCount").and_then(JsonValue::as_u64) else {
        return false;
    };
    let Some(total_score) = object.get("totalScore").and_then(JsonValue::as_i64) else {
        return false;
    };
    if active_count != active.len() as u64 {
        return false;
    }
    let mut score_sum = 0_i64;
    for item in active {
        let Some(item) = item.as_object() else {
            return false;
        };
        if item.len() != 3
            || item.get("name").and_then(JsonValue::as_str).is_none()
            || item.get("active").and_then(JsonValue::as_bool) != Some(true)
        {
            return false;
        }
        let Some(score) = item.get("score").and_then(JsonValue::as_i64) else {
            return false;
        };
        let Some(sum) = score_sum.checked_add(score) else {
            return false;
        };
        score_sum = sum;
    }
    score_sum == total_score
}

fn finish_run(
    state: &RunState,
    status: RunStatus,
    error: Option<&str>,
    score: &JsonValue,
) -> Result<(), RunError> {
    let mut status = status;
    let mut error = error.map(str::to_owned);
    let mut score = score.clone();
    let mut persistence_errors = Vec::new();
    match finalize_workspace(state) {
        Ok(diff) => {
            if let Err(event_error) = record_event(state, "workspace.finalized", diff) {
                persistence_errors.push(format!(
                    "workspace.finalized event could not be written: {event_error}"
                ));
            }
        }
        Err(finalization_error) => {
            status = RunStatus::Failed;
            let message = format!("failed to finalize workspace evidence: {finalization_error}");
            error = Some(message.clone());
            if let Some(score) = score.as_object_mut() {
                score.insert("passed".to_owned(), JsonValue::Bool(false));
                score.insert("finalizationError".to_owned(), JsonValue::String(message));
            }
        }
    }
    let secrets = lock(&state.secret_values).clone();
    score = redact_value(score, &secrets);
    error = error.map(|message| redact_string(&message, &secrets));
    if let Err(score_error) = write_json_atomic(&state.bundle_dir.join("score.json"), &score) {
        persistence_errors.push(format!("score.json could not be written: {score_error}"));
    }
    apply_persistence_failure(&mut status, &mut error, &mut score, &persistence_errors);
    {
        let mut summary = lock(&state.summary);
        summary.status = status;
        summary.finished_at_ms = Some(now_ms());
        summary.error.clone_from(&error);
    }
    if let Err(event_error) = record_event(
        state,
        "run.finished",
        json!({ "status": status, "error": error, "score": score }),
    ) {
        persistence_errors.push(format!(
            "run.finished event could not be written: {event_error}"
        ));
    }
    if let Err(review_error) = persist_review(state) {
        persistence_errors.push(format!("review.json could not be written: {review_error}"));
    }
    if let Err(manifest_error) = persist_manifest(state) {
        persistence_errors.push(format!(
            "manifest.json could not be written: {manifest_error}"
        ));
    }

    if !persistence_errors.is_empty() {
        apply_persistence_failure(&mut status, &mut error, &mut score, &persistence_errors);
        {
            let mut summary = lock(&state.summary);
            summary.status = status;
            summary.finished_at_ms = Some(now_ms());
            summary.error.clone_from(&error);
        }
        // These corrective writes are best-effort because the original storage failure may still
        // be active. The in-memory state and returned error remain authoritative either way.
        let _ = write_json_atomic(&state.bundle_dir.join("score.json"), &score);
        let _ = record_event(
            state,
            "run.persistence-failed",
            json!({ "status": status, "error": error, "score": score }),
        );
        let _ = persist_review(state);
        let _ = persist_manifest(state);
    }
    for capability in lock(&state.capabilities).drain(..) {
        capability.cancel.cancel();
    }
    state.cancel.cancel();
    if persistence_errors.is_empty() {
        Ok(())
    } else {
        Err(RunError::EvidencePersistence(persistence_errors.join("; ")))
    }
}

fn apply_persistence_failure(
    status: &mut RunStatus,
    error: &mut Option<String>,
    score: &mut JsonValue,
    persistence_errors: &[String],
) {
    if persistence_errors.is_empty() {
        return;
    }
    *status = RunStatus::Failed;
    let message = format!(
        "failed to persist run evidence: {}",
        persistence_errors.join("; ")
    );
    *error = Some(message.clone());
    if let Some(score) = score.as_object_mut() {
        score.insert("passed".to_owned(), JsonValue::Bool(false));
        score.insert("persistenceError".to_owned(), JsonValue::String(message));
    }
}

fn update_status(state: &RunState, status: RunStatus) -> Result<(), RunError> {
    lock(&state.summary).status = status;
    persist_manifest(state)?;
    record_event(state, "run.status", json!({ "status": status }))?;
    Ok(())
}

fn record_event(state: &RunState, kind: &str, payload: JsonValue) -> Result<(), RunError> {
    let event = {
        let mut events = lock(&state.events);
        let event = RunEvent {
            sequence: events.len() as u64 + 1,
            at_ms: now_ms(),
            kind: kind.to_owned(),
            payload,
        };
        let mut line = serde_json::to_vec(&event)?;
        line.push(b'\n');
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(state.bundle_dir.join("events.jsonl"))?;
        file.write_all(&line)?;
        events.push(event.clone());
        event
    };
    let mut summary = lock(&state.summary);
    summary.event_count = summary.event_count.max(event.sequence);
    drop(summary);
    let _ = state.sender.send(event);
    Ok(())
}

fn persist_manifest(state: &RunState) -> Result<(), RunError> {
    write_json_atomic(
        &state.bundle_dir.join("manifest.json"),
        &serde_json::to_value(lock(&state.summary).clone())?,
    )
}

fn persist_assembly(state: &RunState) -> Result<(), RunError> {
    write_json_atomic(
        &state.bundle_dir.join("assembly.json"),
        &serde_json::to_value(lock(&state.assembly).clone())?,
    )
}

fn persist_review(state: &RunState) -> Result<(), RunError> {
    let summary = lock(&state.summary).clone();
    let events = lock(&state.events).clone();
    write_json_atomic(
        &state.bundle_dir.join("review.json"),
        &serde_json::to_value(build_review(&summary, &events))?,
    )
}

#[allow(clippy::too_many_lines)]
fn build_review(summary: &RunSummary, events: &[RunEvent]) -> RunReview {
    let mut review = RunReview {
        version: 1,
        status: summary.status,
        metrics: ReviewMetrics {
            duration_ms: summary
                .finished_at_ms
                .map(|finished| finished.saturating_sub(summary.started_at_ms)),
            ..ReviewMetrics::default()
        },
        steps: Vec::new(),
    };
    let mut current_turn = None;
    let mut pending_capabilities: HashMap<(String, String), VecDeque<u64>> = HashMap::new();
    let mut native_actions = HashSet::new();
    let mut driver_turn_active = false;
    let mut outcome_recorded = false;

    for event in events {
        if outcome_recorded && event.kind != "run.persistence-failed" {
            continue;
        }
        match event.kind.as_str() {
            "driver.ready" => {
                let name = json_string(&event.payload, "name").unwrap_or("External driver");
                let version = json_string(&event.payload, "version");
                push_review_step(
                    &mut review,
                    "harness",
                    "Harness ready".to_owned(),
                    Some(match version {
                        Some(version) => format!("{name} v{version}"),
                        None => name.to_owned(),
                    }),
                    "completed",
                    vec![event.sequence],
                    None,
                    None,
                );
            }
            "driver.turn-started" | "driver.session-opened" => {
                driver_turn_active = true;
            }
            "driver.turn-finished" => {
                driver_turn_active = false;
                pending_capabilities.clear();
            }
            "v0.turn-start" => {
                review.metrics.model_turns += 1;
                let turn = review.metrics.model_turns;
                push_review_step(
                    &mut review,
                    "model-turn",
                    format!("Model turn {turn}"),
                    None,
                    "completed",
                    vec![event.sequence],
                    None,
                    None,
                );
                current_turn = review.steps.len().checked_sub(1);
            }
            "v0.mdx" => {
                if let (Some(index), Some(content)) =
                    (current_turn, json_string(&event.payload, "content"))
                {
                    append_review_detail(&mut review.steps[index], content);
                    review.steps[index].event_sequences.push(event.sequence);
                }
            }
            "v0.turn-finish" => {
                if let Some(index) = current_turn.take() {
                    review.steps[index].event_sequences.push(event.sequence);
                }
            }
            "v0.orchestrator-error" => {
                let model = json_string(&event.payload, "modelId").unwrap_or("Model provider");
                let detail = json_string(&event.payload, "message")
                    .map(|message| format!("{model}: {message}"));
                if let Some(index) = current_turn {
                    "failed".clone_into(&mut review.steps[index].status);
                    review.steps[index].event_sequences.push(event.sequence);
                    if let Some(detail) = detail {
                        if review.steps[index].detail.is_some() {
                            append_review_detail(&mut review.steps[index], "\n");
                        }
                        append_review_detail(&mut review.steps[index], &detail);
                    }
                } else {
                    push_review_step(
                        &mut review,
                        "model-turn",
                        "Model provider failed".to_owned(),
                        detail,
                        "failed",
                        vec![event.sequence],
                        None,
                        None,
                    );
                }
            }
            "mcp.tool.started"
                if driver_turn_active && capability_event_is_agent(&event.payload) =>
            {
                if let Some(key) = capability_key(&event.payload) {
                    pending_capabilities
                        .entry(key)
                        .or_default()
                        .push_back(event.sequence);
                }
            }
            "mcp.tool.completed"
                if driver_turn_active && capability_event_is_agent(&event.payload) =>
            {
                if let Some((source, name)) = capability_key(&event.payload) {
                    let mut sequences = Vec::new();
                    let key = (source.clone(), name.clone());
                    if let Some(started) = pending_capabilities
                        .get_mut(&key)
                        .and_then(VecDeque::pop_front)
                    {
                        sequences.push(started);
                    }
                    if pending_capabilities
                        .get(&key)
                        .is_some_and(VecDeque::is_empty)
                    {
                        pending_capabilities.remove(&key);
                    }
                    sequences.push(event.sequence);
                    let failed = event.payload["isError"].as_bool() == Some(true);
                    review.metrics.capability_calls += 1;
                    push_review_step(
                        &mut review,
                        "capability",
                        format!("{source} · {name}"),
                        Some(if failed {
                            "Capability returned an error".to_owned()
                        } else {
                            "Capability completed".to_owned()
                        }),
                        if failed { "failed" } else { "completed" },
                        sequences,
                        Some(source),
                        None,
                    );
                }
            }
            "tool.call" => {
                if let Some((source, name)) = capability_key(&event.payload) {
                    review.metrics.capability_calls += 1;
                    push_review_step(
                        &mut review,
                        "capability",
                        format!("{source} · {name}"),
                        Some("Capability completed".to_owned()),
                        "completed",
                        vec![event.sequence],
                        Some(source),
                        None,
                    );
                }
            }
            "workspace.finalized" => add_workspace_steps(&mut review, event),
            "run.finished" | "run.persistence-failed" => {
                review.steps.retain(|step| step.kind != "outcome");
                add_outcome_step(&mut review, event);
                outcome_recorded = true;
            }
            kind if is_completed_native_action(kind, &event.payload) => {
                let id = json_string(&event.payload, "id").unwrap_or(kind);
                if native_actions.insert(id.to_owned()) {
                    review.metrics.native_actions += 1;
                    let title = json_string(&event.payload, "taskNameComplete")
                        .unwrap_or("Native action completed")
                        .to_owned();
                    let path = native_action_path(&event.payload);
                    push_review_step(
                        &mut review,
                        "native-action",
                        title,
                        Some("Harness-native tool completed".to_owned()),
                        "completed",
                        vec![event.sequence],
                        None,
                        path,
                    );
                }
            }
            _ => {}
        }
    }
    for step in &mut review.steps {
        if step.kind == "model-turn" {
            normalize_review_detail(step);
        }
    }
    review
}

#[allow(clippy::too_many_arguments)]
fn push_review_step(
    review: &mut RunReview,
    kind: &str,
    title: String,
    detail: Option<String>,
    status: &str,
    event_sequences: Vec<u64>,
    source: Option<String>,
    path: Option<String>,
) {
    let ordinal = u32::try_from(review.steps.len())
        .unwrap_or(u32::MAX)
        .saturating_add(1);
    review.steps.push(ReviewStep {
        ordinal,
        kind: kind.to_owned(),
        title,
        detail,
        status: status.to_owned(),
        event_sequences,
        source,
        path,
    });
}

fn append_review_detail(step: &mut ReviewStep, content: &str) {
    let detail = step.detail.get_or_insert_with(String::new);
    detail.push_str(content);
}

fn normalize_review_detail(step: &mut ReviewStep) {
    if let Some(detail) = &mut step.detail {
        let normalized = detail.split_whitespace().collect::<Vec<_>>().join(" ");
        *detail = normalized.chars().take(500).collect();
        if detail.is_empty() {
            step.detail = None;
        }
    }
}

fn json_string<'a>(value: &'a JsonValue, key: &str) -> Option<&'a str> {
    value.get(key).and_then(JsonValue::as_str)
}

fn capability_key(payload: &JsonValue) -> Option<(String, String)> {
    Some((
        json_string(payload, "source")?.to_owned(),
        json_string(payload, "name")
            .or_else(|| json_string(payload, "tool"))?
            .to_owned(),
    ))
}

fn capability_event_is_agent(payload: &JsonValue) -> bool {
    json_string(payload, "actor").is_none_or(|actor| actor == "agent")
}

fn is_completed_native_action(kind: &str, payload: &JsonValue) -> bool {
    is_native_action_event(kind, payload)
        && payload
            .get("finishedAt")
            .is_some_and(|value| !value.is_null())
}

fn is_native_action_event(kind: &str, payload: &JsonValue) -> bool {
    kind.starts_with("v0.task-")
        && !kind.contains("dynamic-tool")
        && !kind.contains("waiting")
        && !kind.contains("programmatic-result")
        && !kind.contains("finished-file-edits")
        && payload.get("id").is_some()
}

fn native_action_path(payload: &JsonValue) -> Option<String> {
    json_string(payload, "filePath")
        .map(str::to_owned)
        .or_else(|| {
            payload["parts"].as_array().and_then(|parts| {
                parts
                    .iter()
                    .rev()
                    .find_map(|part| json_string(part, "filePath").map(str::to_owned))
            })
        })
}

fn add_workspace_steps(review: &mut RunReview, event: &RunEvent) {
    let Some(changes) = event.payload["changes"].as_array() else {
        return;
    };
    for change in changes {
        let Some(path) = json_string(change, "path") else {
            continue;
        };
        let operation = json_string(change, "kind").unwrap_or("changed");
        let title = match operation {
            "created" => format!("Created {path}"),
            "deleted" | "removed" => format!("Deleted {path}"),
            _ => format!("Updated {path}"),
        };
        review.metrics.workspace_changes += 1;
        push_review_step(
            review,
            "workspace-effect",
            title,
            Some("Effect captured in the finalized workspace".to_owned()),
            "completed",
            vec![event.sequence],
            None,
            Some(path.to_owned()),
        );
    }
}

fn add_outcome_step(review: &mut RunReview, event: &RunEvent) {
    let status = json_string(&event.payload, "status").unwrap_or("failed");
    let title = match status {
        "passed" => "Evaluation passed",
        "cancelled" => "Run cancelled",
        _ => "Evaluation failed",
    };
    let score = &event.payload["score"];
    let detail = match (
        score["activeNames"].as_array(),
        score["totalScore"].as_i64(),
    ) {
        (Some(active), Some(total)) => Some(format!(
            "{} active items · total score {total}",
            active.len()
        )),
        _ => json_string(&event.payload, "error").map(str::to_owned),
    };
    push_review_step(
        review,
        "outcome",
        title.to_owned(),
        detail,
        status,
        vec![event.sequence],
        None,
        None,
    );
}

fn initial_assembly(summary: &RunSummary, scenario: &ScenarioManifest) -> AssemblySnapshot {
    AssemblySnapshot {
        question: scenario.question.clone(),
        scenario: AssemblyScenario {
            id: scenario.id.clone(),
            title: scenario.title.clone(),
            description: scenario.description.clone(),
            version: scenario.version,
            output: scenario.output.clone(),
        },
        harness: HarnessAssembly {
            adapter: "external-driver".to_owned(),
            model_id: (!summary.model_id.is_empty()).then(|| summary.model_id.clone()),
            driver: None,
        },
        workspace: WorkspaceAssembly {
            id: format!("{}/workspace", summary.id),
            seed: scenario.seed.clone(),
            seed_revision: format!("{}@{}", scenario.id, scenario.version),
            attachment: "root-confined-physical".to_owned(),
            change_tracking: "initial-and-final-snapshots".to_owned(),
        },
        capability_sources: Vec::new(),
        limits: scenario.limits.clone(),
    }
}

fn recover_legacy_assembly(
    summary: &RunSummary,
    scenario: &ScenarioManifest,
    events: &[RunEvent],
) -> AssemblySnapshot {
    let mut assembly = initial_assembly(summary, scenario);
    for event in events {
        match event.kind.as_str() {
            "driver.ready" if assembly.harness.driver.is_none() => {
                assembly.harness.driver = serde_json::from_value(event.payload.clone()).ok();
            }
            "capability.source.started" => {
                let Some(id) = json_string(&event.payload, "id") else {
                    continue;
                };
                if assembly
                    .capability_sources
                    .iter()
                    .any(|source| source.id == id)
                {
                    continue;
                }
                let revision = json_string(&event.payload, "revision")
                    .unwrap_or("unknown")
                    .to_owned();
                let protocol = match json_string(&event.payload, "transport") {
                    Some("streamable-http") => "mcp-streamable-http",
                    Some(transport) => transport,
                    None => "mcp",
                };
                assembly.capability_sources.push(CapabilityAssembly {
                    id: id.to_owned(),
                    revision,
                    protocol: protocol.to_owned(),
                    projections: vec!["nushell".to_owned(), "agent-mcp".to_owned()],
                });
            }
            _ => {}
        }
    }
    assembly
}

fn update_assembly_capabilities(
    state: &RunState,
    capabilities: &[CapabilityEndpoint],
) -> Result<(), RunError> {
    lock(&state.assembly).capability_sources = capabilities
        .iter()
        .map(|capability| CapabilityAssembly {
            id: capability.id.clone(),
            revision: capability.revision.clone(),
            protocol: "mcp-streamable-http".to_owned(),
            projections: vec!["nushell".to_owned(), "agent-mcp".to_owned()],
        })
        .collect();
    persist_assembly(state)
}

fn command(message_id: &str, body: CommandBody) -> ControllerCommand {
    ControllerCommand {
        protocol_version: PROTOCOL_VERSION,
        message_id: message_id.to_owned(),
        body,
    }
}

fn workspace_relative_path(path: &Path) -> Result<PathBuf, RunError> {
    let workspace_root = Path::new("/agent-lab-workspace");
    let normalized = confined_child(workspace_root, path)?;
    let relative = normalized
        .strip_prefix(workspace_root)
        .map_err(|_| RunError::PathEscape(normalized.clone()))?;
    if relative.as_os_str().is_empty() {
        return Err(RunError::InvalidScenario(
            "scenario output must name a file inside the workspace".to_owned(),
        ));
    }
    Ok(relative.to_path_buf())
}

fn load_scenarios(root: &Path) -> Result<BTreeMap<String, ScenarioManifest>, RunError> {
    let mut scenarios = BTreeMap::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if entry.path().extension().and_then(|value| value.to_str()) != Some("toml") {
            continue;
        }
        let source = fs::read_to_string(entry.path())?;
        let mut manifest: ScenarioManifest = toml::from_str(&source)?;
        if manifest.version != 1 || manifest.id.is_empty() {
            return Err(RunError::InvalidScenario(format!(
                "{} has an unsupported version or empty id",
                entry.path().display()
            )));
        }
        let seed = confined_existing_child(root, &manifest.seed)?;
        if !seed.is_dir() {
            return Err(RunError::InvalidScenario(format!(
                "{} refers to a missing seed directory",
                entry.path().display()
            )));
        }
        manifest.output = workspace_relative_path(&manifest.output)?;
        if scenarios.insert(manifest.id.clone(), manifest).is_some() {
            return Err(RunError::InvalidScenario(
                "scenario ids must be unique".to_owned(),
            ));
        }
    }
    Ok(scenarios)
}

fn load_runs(
    data_dir: &Path,
    scenarios: &BTreeMap<String, ScenarioManifest>,
) -> Result<HashMap<String, Arc<RunState>>, RunError> {
    let mut runs = HashMap::new();
    for entry in fs::read_dir(data_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let bundle_dir = entry.path();
        let manifest_path = bundle_dir.join("manifest.json");
        if !manifest_path.is_file() {
            continue;
        }
        let mut summary: RunSummary = serde_json::from_slice(&fs::read(&manifest_path)?)?;
        if summary.id != entry.file_name().to_string_lossy() {
            continue;
        }
        let scenario = scenarios.get(&summary.scenario_id);
        let workspace = bundle_dir.join("workspace");
        if !workspace.is_dir() && !summary.status.is_finished() {
            continue;
        }
        let interrupted = matches!(summary.status, RunStatus::Starting | RunStatus::Running);
        let (events, replay_failed, malformed_event_log) =
            match read_events(&bundle_dir.join("events.jsonl")) {
                Ok(events) => (events, false, false),
                Err(error) => {
                    let message = format!("stored event replay failed: {error}");
                    if interrupted {
                        (Vec::new(), false, true)
                    } else {
                        summary.status = RunStatus::Failed;
                        summary.error = Some(message.clone());
                        (
                            vec![RunEvent {
                                sequence: 1,
                                at_ms: now_ms(),
                                kind: "run.finished".to_owned(),
                                payload: json!({
                                    "status": RunStatus::Failed,
                                    "error": message,
                                    "recovered": true,
                                }),
                            }],
                            true,
                            true,
                        )
                    }
                }
            };
        let mut assembly = if bundle_dir.join("assembly.json").is_file() {
            serde_json::from_slice(&fs::read(bundle_dir.join("assembly.json"))?)?
        } else {
            let Some(scenario) = scenario else {
                continue;
            };
            recover_legacy_assembly(&summary, scenario, &events)
        };
        if assembly.scenario.output.as_os_str().is_empty() {
            let Some(scenario) = scenario else {
                continue;
            };
            assembly.scenario.output.clone_from(&scenario.output);
        }
        assembly.scenario.output = workspace_relative_path(&assembly.scenario.output)?;
        let output = assembly.scenario.output.clone();
        let initial_snapshot = (summary.status == RunStatus::Exploring)
            .then(|| snapshot_tree(&bundle_dir.join("initial")).ok())
            .flatten();
        summary.event_count = events.len() as u64;
        let (sender, _) = broadcast::channel(256);
        let state = Arc::new(RunState {
            summary: Mutex::new(summary),
            assembly: Mutex::new(assembly),
            events: Mutex::new(events),
            sender,
            cancel: CancellationToken::new(),
            bundle_dir,
            workspace,
            output,
            initial_snapshot,
            capabilities: Mutex::new(Vec::new()),
            secret_values: Mutex::new(Vec::new()),
            replay_failed,
        });
        if interrupted && !recover_finalized_run(&state)? {
            recover_interrupted_run(&state, malformed_event_log)?;
        }
        let id = lock(&state.summary).id.clone();
        runs.insert(id, state);
    }
    Ok(runs)
}

fn recover_finalized_run(state: &RunState) -> Result<bool, RunError> {
    let terminal_event = lock(&state.events)
        .iter()
        .rev()
        .find(|event| {
            matches!(
                event.kind.as_str(),
                "run.finished" | "run.persistence-failed"
            )
        })
        .cloned();
    let Some(terminal_event) = terminal_event else {
        return Ok(false);
    };
    let Ok(status) = serde_json::from_value::<RunStatus>(terminal_event.payload["status"].clone())
    else {
        return Ok(false);
    };
    if !status.is_finished() {
        return Ok(false);
    }
    let final_dir = state.bundle_dir.join("final");
    if validate_evidence_tree(&final_dir).is_err() {
        return Ok(false);
    }
    let Ok(final_files) = snapshot_tree(&final_dir) else {
        return Ok(false);
    };
    let Ok(workspace_files) = snapshot_tree(&state.workspace) else {
        return Ok(false);
    };
    if final_files != workspace_files
        || read_optional_json(&state.bundle_dir.join("diff.json"))
            .ok()
            .flatten()
            .is_none()
    {
        return Ok(false);
    }
    let Some(score) = terminal_event.payload.get("score").cloned() else {
        return Ok(false);
    };
    write_json_atomic(&state.bundle_dir.join("score.json"), &score)?;
    {
        let mut summary = lock(&state.summary);
        summary.status = status;
        summary.finished_at_ms = Some(terminal_event.at_ms);
        summary.error = terminal_event
            .payload
            .get("error")
            .and_then(JsonValue::as_str)
            .map(str::to_owned);
    }
    persist_review(state)?;
    persist_manifest(state)?;
    state.cancel.cancel();
    Ok(true)
}

fn recover_interrupted_run(state: &RunState, reset_event_log: bool) -> Result<(), RunError> {
    for directory in [
        state.workspace.clone(),
        state.bundle_dir.join("final"),
        state.bundle_dir.join("final.tmp"),
    ] {
        remove_evidence_entry(&directory)?;
    }
    remove_evidence_entry(&state.bundle_dir.join("diff.json"))?;
    let score = json!({
        "passed": false,
        "cancelled": true,
        "recovered": true,
        "workspaceEvidence": "discarded",
    });
    write_json_atomic(&state.bundle_dir.join("score.json"), &score)?;
    {
        let mut summary = lock(&state.summary);
        summary.status = RunStatus::Cancelled;
        summary.finished_at_ms = Some(now_ms());
        summary.error = Some("controller stopped before the run finalized".to_owned());
    }
    if reset_event_log {
        lock(&state.events).clear();
        fs::write(state.bundle_dir.join("events.jsonl"), [])?;
    }
    record_event(
        state,
        "run.finished",
        json!({
            "status": RunStatus::Cancelled,
            "error": "controller stopped before the run finalized",
            "score": score,
            "workspaceEvidence": "discarded because redaction material was unavailable",
        }),
    )?;
    persist_review(state)?;
    persist_manifest(state)?;
    state.cancel.cancel();
    Ok(())
}

fn read_events(path: &Path) -> Result<Vec<RunEvent>, RunError> {
    let source = fs::read_to_string(path)?;
    source
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let mut event: RunEvent = serde_json::from_str(line)?;
            redact_json(&mut event.payload);
            Ok(event)
        })
        .collect()
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn finalize_workspace(state: &RunState) -> Result<JsonValue, RunError> {
    let final_dir = state.bundle_dir.join("final");
    // A driver can address paths outside its declared workspace. Rebuild the controller-owned
    // snapshot every time instead of trusting anything already present at the sibling path.
    remove_evidence_entry(&final_dir)?;
    let staging_dir = state.bundle_dir.join("final.tmp");
    remove_evidence_entry(&staging_dir)?;
    let result = (|| {
        validate_evidence_tree(&state.workspace)?;
        copy_tree(&state.workspace, &staging_dir)?;
        let secret_values = lock(&state.secret_values).clone();
        redact_tree(&staging_dir, &secret_values)?;
        let initial = state.initial_snapshot.as_ref().ok_or_else(|| {
            RunError::EvidencePersistence(
                "protected initial workspace snapshot is unavailable".to_owned(),
            )
        })?;
        persist_initial_snapshot(state, initial)?;
        let final_files = snapshot_tree(&staging_dir)?;
        let paths = initial
            .keys()
            .chain(final_files.keys())
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let changes = paths
            .into_iter()
            .filter_map(|path| {
                let before = initial.get(&path);
                let after = final_files.get(&path);
                (before != after).then(|| {
                    json!({
                        "path": path,
                        "kind": match (before, after) {
                            (None, Some(_)) => "created",
                            (Some(_), None) => "deleted",
                            _ => "modified",
                        },
                        "before": before.and_then(|bytes| redacted_evidence_text(bytes, &secret_values)),
                        "after": after.and_then(|bytes| redacted_evidence_text(bytes, &secret_values)),
                    })
                })
            })
            .collect::<Vec<_>>();
        let diff = json!({ "changes": changes });
        write_json_atomic(&state.bundle_dir.join("diff.json"), &diff)?;
        // The workspace is part of the run bundle too. Keep it usable for an attached shell while
        // applying the same redaction boundary as the immutable final snapshot.
        redact_tree(&state.workspace, &secret_values)?;
        fs::rename(&staging_dir, final_dir)?;
        Ok(diff)
    })();
    if result.is_err() {
        let _ = remove_evidence_entry(&staging_dir);
    }
    result
}

fn persist_initial_snapshot(
    state: &RunState,
    snapshot: &BTreeMap<String, Vec<u8>>,
) -> Result<(), RunError> {
    let initial_dir = state.bundle_dir.join("initial");
    let staging_dir = state.bundle_dir.join("initial.tmp");
    remove_evidence_entry(&staging_dir)?;
    fs::create_dir(&staging_dir)?;
    for (relative, contents) in snapshot {
        let path = confined_child(&staging_dir, relative)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, contents)?;
    }
    remove_evidence_entry(&initial_dir)?;
    fs::rename(staging_dir, initial_dir)?;
    Ok(())
}

fn remove_evidence_entry(path: &Path) -> Result<(), RunError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if metadata.is_dir() && !metadata.file_type().is_symlink() {
        fs::remove_dir_all(path)?;
    } else {
        fs::remove_file(path)?;
    }
    Ok(())
}

fn redact_tree(root: &Path, secrets: &[Vec<u8>]) -> Result<(), RunError> {
    fn visit(
        directory: &Path,
        secrets: &[Vec<u8>],
        retained_bytes: &mut u64,
    ) -> Result<(), RunError> {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_symlink() {
                return Err(RunError::PathEscape(entry.path()));
            }
            if file_type.is_dir() {
                visit(&entry.path(), secrets, retained_bytes)?;
            } else if file_type.is_file() {
                let original = read_evidence_file(&entry.path(), retained_bytes)?;
                let redacted = redact_evidence_bytes(&original, secrets);
                if redacted != original {
                    fs::write(entry.path(), redacted)?;
                }
            } else {
                return Err(RunError::UnsupportedWorkspaceEntry(entry.path()));
            }
        }
        Ok(())
    }

    visit(root, secrets, &mut 0)
}

fn redacted_evidence_text(bytes: &[u8], secrets: &[Vec<u8>]) -> Option<String> {
    String::from_utf8(redact_evidence_bytes(bytes, secrets)).ok()
}

fn redact_evidence_bytes(bytes: &[u8], secrets: &[Vec<u8>]) -> Vec<u8> {
    if let Ok(mut value) = serde_json::from_slice::<JsonValue>(bytes) {
        let original = value.clone();
        redact_json(&mut value);
        redact_secret_strings(&mut value, secrets);
        if value != original {
            return serde_json::to_vec_pretty(&value).unwrap_or_else(|_| bytes.to_vec());
        }
        return bytes.to_vec();
    }
    let mut redacted = bytes.to_vec();
    replace_secrets(&mut redacted, secrets);
    redacted
}

fn snapshot_tree(root: &Path) -> Result<BTreeMap<String, Vec<u8>>, RunError> {
    fn visit(
        root: &Path,
        directory: &Path,
        files: &mut BTreeMap<String, Vec<u8>>,
        retained_bytes: &mut u64,
    ) -> Result<(), RunError> {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_symlink() {
                return Err(RunError::PathEscape(entry.path()));
            }
            if file_type.is_dir() {
                visit(root, &entry.path(), files, retained_bytes)?;
            } else if file_type.is_file() {
                let relative = entry
                    .path()
                    .strip_prefix(root)
                    .map_err(|_| RunError::PathEscape(entry.path()))?
                    .to_string_lossy()
                    .into_owned();
                files.insert(relative, read_evidence_file(&entry.path(), retained_bytes)?);
            } else {
                return Err(RunError::UnsupportedWorkspaceEntry(entry.path()));
            }
        }
        Ok(())
    }

    let mut files = BTreeMap::new();
    visit(root, root, &mut files, &mut 0)?;
    Ok(files)
}

fn validate_evidence_tree(root: &Path) -> Result<(), RunError> {
    fn visit(directory: &Path, retained_bytes: &mut u64) -> Result<(), RunError> {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_symlink() {
                return Err(RunError::PathEscape(entry.path()));
            }
            if file_type.is_dir() {
                visit(&entry.path(), retained_bytes)?;
            } else if file_type.is_file() {
                validate_evidence_file(&entry.path(), &entry.metadata()?, retained_bytes)?;
            } else {
                return Err(RunError::UnsupportedWorkspaceEntry(entry.path()));
            }
        }
        Ok(())
    }

    visit(root, &mut 0)
}

fn validate_evidence_file(
    path: &Path,
    metadata: &fs::Metadata,
    retained_bytes: &mut u64,
) -> Result<(), RunError> {
    reject_multiply_linked_file(path, metadata)?;
    let file_bytes = metadata.len();
    if file_bytes > MAX_EVIDENCE_FILE_BYTES {
        return Err(RunError::EvidenceLimit(format!(
            "{} is {file_bytes} bytes; per-file limit is {MAX_EVIDENCE_FILE_BYTES}",
            path.display()
        )));
    }
    let total = retained_bytes
        .checked_add(file_bytes)
        .ok_or_else(|| RunError::EvidenceLimit("workspace byte count overflowed".to_owned()))?;
    if total > MAX_EVIDENCE_TOTAL_BYTES {
        return Err(RunError::EvidenceLimit(format!(
            "workspace retains {total} bytes; total limit is {MAX_EVIDENCE_TOTAL_BYTES}"
        )));
    }
    *retained_bytes = total;
    Ok(())
}

fn read_evidence_file(path: &Path, retained_bytes: &mut u64) -> Result<Vec<u8>, RunError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(RunError::PathEscape(path.to_path_buf()));
    }
    validate_evidence_file(path, &metadata, retained_bytes)?;
    let capacity = usize::try_from(metadata.len())
        .map_err(|_| RunError::EvidenceLimit("file size does not fit this platform".to_owned()))?;
    let mut bytes = Vec::with_capacity(capacity);
    fs::File::open(path)?
        .take(MAX_EVIDENCE_FILE_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_EVIDENCE_FILE_BYTES {
        return Err(RunError::EvidenceLimit(format!(
            "{} grew beyond the per-file limit while being read",
            path.display()
        )));
    }
    Ok(bytes)
}

fn copy_tree(source: &Path, destination: &Path) -> Result<(), RunError> {
    let metadata = fs::symlink_metadata(source)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(RunError::InvalidScenario(format!(
            "seed must be a real directory: {}",
            source.display()
        )));
    }
    fs::create_dir(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            return Err(RunError::InvalidScenario(format!(
                "scenario seeds may not contain symlinks: {}",
                entry.path().display()
            )));
        }
        let target = destination.join(entry.file_name());
        if file_type.is_dir() {
            copy_tree(&entry.path(), &target)?;
        } else if file_type.is_file() {
            reject_multiply_linked_file(&entry.path(), &entry.metadata()?)?;
            fs::copy(entry.path(), target)?;
        } else {
            return Err(RunError::UnsupportedWorkspaceEntry(entry.path()));
        }
    }
    Ok(())
}

fn canonical_directory(path: &Path) -> Result<PathBuf, RunError> {
    let canonical = fs::canonicalize(path)?;
    if !canonical.is_dir() {
        return Err(RunError::InvalidScenario(format!(
            "not a directory: {}",
            path.display()
        )));
    }
    Ok(canonical)
}

fn reject_multiply_linked_file(path: &Path, metadata: &fs::Metadata) -> Result<(), RunError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() > 1 {
            return Err(RunError::PathEscape(path.to_path_buf()));
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if metadata.number_of_links().is_some_and(|links| links > 1) {
            return Err(RunError::PathEscape(path.to_path_buf()));
        }
    }
    Ok(())
}

fn confined_child(root: &Path, child: impl AsRef<Path>) -> Result<PathBuf, RunError> {
    let candidate = root.join(child.as_ref());
    let normalized = normalize_path(&candidate);
    if normalized != root && !normalized.starts_with(root) {
        return Err(RunError::PathEscape(candidate));
    }
    Ok(normalized)
}

fn confined_existing_child(root: &Path, child: impl AsRef<Path>) -> Result<PathBuf, RunError> {
    let candidate = confined_child(root, child)?;
    let canonical_root = fs::canonicalize(root)?;
    let canonical_candidate = fs::canonicalize(&candidate)?;
    if canonical_candidate != canonical_root && !canonical_candidate.starts_with(&canonical_root) {
        return Err(RunError::PathEscape(candidate));
    }
    Ok(canonical_candidate)
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            std::path::Component::CurDir => {}
            component => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

fn write_json_atomic(path: &Path, value: &JsonValue) -> Result<(), RunError> {
    let temporary = path.with_extension("json.tmp");
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    fs::write(&temporary, bytes)?;
    fs::rename(temporary, path)?;
    Ok(())
}

fn read_optional_json(path: &Path) -> Result<Option<JsonValue>, RunError> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn read_optional_confined_json(
    root: &Path,
    child: impl AsRef<Path>,
) -> Result<Option<JsonValue>, RunError> {
    let path = confined_child(root, child)?;
    let relative = path
        .strip_prefix(root)
        .map_err(|_| RunError::PathEscape(path.clone()))?;
    let Some(bytes) = read_optional_confined_file(root, relative, &path)? else {
        return Ok(None);
    };
    Ok(Some(serde_json::from_slice(&bytes)?))
}

#[cfg(unix)]
fn read_optional_confined_file(
    root: &Path,
    relative: &Path,
    display_path: &Path,
) -> Result<Option<Vec<u8>>, RunError> {
    let components = relative
        .components()
        .map(|component| match component {
            std::path::Component::Normal(component) => Ok(component.to_owned()),
            _ => Err(RunError::PathEscape(display_path.to_path_buf())),
        })
        .collect::<Result<Vec<_>, _>>()?;
    if components.is_empty() {
        return Err(RunError::PathEscape(display_path.to_path_buf()));
    }
    let mut directory = rustix::fs::open(
        root,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::CLOEXEC
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW,
        rustix::fs::Mode::empty(),
    )
    .map_err(io::Error::from)?;
    for (index, component) in components.iter().enumerate() {
        let is_file = index + 1 == components.len();
        let mut flags =
            rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::CLOEXEC | rustix::fs::OFlags::NOFOLLOW;
        if !is_file {
            flags |= rustix::fs::OFlags::DIRECTORY;
        }
        let opened =
            match rustix::fs::openat(&directory, component, flags, rustix::fs::Mode::empty()) {
                Ok(opened) => opened,
                Err(rustix::io::Errno::NOENT) => return Ok(None),
                Err(rustix::io::Errno::LOOP | rustix::io::Errno::NOTDIR) => {
                    return Err(RunError::PathEscape(display_path.to_path_buf()));
                }
                Err(error) => {
                    return Err(io::Error::from(error).into());
                }
            };
        if !is_file {
            directory = opened;
            continue;
        }
        let opened = fs::File::from(opened);
        let metadata = opened.metadata()?;
        if !metadata.is_file() {
            return Err(RunError::PathEscape(display_path.to_path_buf()));
        }
        let mut retained_bytes = 0;
        validate_evidence_file(display_path, &metadata, &mut retained_bytes)?;
        let capacity = usize::try_from(metadata.len()).map_err(|_| {
            RunError::EvidenceLimit("file size does not fit this platform".to_owned())
        })?;
        let mut bytes = Vec::with_capacity(capacity);
        opened
            .take(MAX_EVIDENCE_FILE_BYTES + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_EVIDENCE_FILE_BYTES {
            return Err(RunError::EvidenceLimit(format!(
                "{} grew beyond the per-file limit while being read",
                display_path.display()
            )));
        }
        return Ok(Some(bytes));
    }
    unreachable!("a non-empty component list must open or reject its final file")
}

#[cfg(not(unix))]
fn read_optional_confined_file(
    root: &Path,
    relative: &Path,
    display_path: &Path,
) -> Result<Option<Vec<u8>>, RunError> {
    match fs::symlink_metadata(display_path) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let path = confined_existing_child(root, relative)?;
    let metadata = fs::symlink_metadata(&path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(RunError::PathEscape(display_path.to_path_buf()));
    }
    read_evidence_file(&path, &mut 0).map(Some)
}

fn redact_transcript(mut transcript: DriverTranscript, secrets: &[Vec<u8>]) -> DriverTranscript {
    for record in transcript
        .controller_records
        .iter_mut()
        .chain(transcript.driver_records.iter_mut())
    {
        if let Ok(mut value) = serde_json::from_slice::<JsonValue>(record) {
            redact_json(&mut value);
            redact_secret_strings(&mut value, secrets);
            if let Ok(mut redacted) = serde_json::to_vec(&value) {
                redacted.push(b'\n');
                *record = redacted;
            }
        } else {
            replace_secrets(record, secrets);
        }
    }
    replace_secrets(&mut transcript.driver_stderr, secrets);
    transcript
}

fn redact_value(mut value: JsonValue, secrets: &[Vec<u8>]) -> JsonValue {
    redact_json(&mut value);
    redact_secret_strings(&mut value, secrets);
    value
}

fn redact_json(value: &mut JsonValue) {
    match value {
        JsonValue::Object(object) => {
            for (key, value) in object {
                if sensitive_name(key) {
                    *value = JsonValue::String("[REDACTED]".to_owned());
                } else {
                    redact_json(value);
                }
            }
        }
        JsonValue::Array(values) => values.iter_mut().for_each(redact_json),
        _ => {}
    }
}

fn redact_secret_strings(value: &mut JsonValue, secrets: &[Vec<u8>]) {
    match value {
        JsonValue::Object(object) => {
            let mut redacted = serde_json::Map::new();
            for (key, mut value) in std::mem::take(object) {
                redact_secret_strings(&mut value, secrets);
                let key = redact_string(&key, secrets);
                let unique_key = unique_redacted_key(&redacted, key);
                redacted.insert(unique_key, value);
            }
            *object = redacted;
        }
        JsonValue::Array(values) => values
            .iter_mut()
            .for_each(|value| redact_secret_strings(value, secrets)),
        JsonValue::String(value) => *value = redact_string(value, secrets),
        _ => {}
    }
}

fn redact_string(value: &str, secrets: &[Vec<u8>]) -> String {
    secrets
        .iter()
        .filter_map(|secret| std::str::from_utf8(secret).ok())
        .filter(|secret| secret.len() >= 4)
        .fold(value.to_owned(), |value, secret| {
            value.replace(secret, "[REDACTED]")
        })
}

fn unique_redacted_key(object: &serde_json::Map<String, JsonValue>, key: String) -> String {
    if !object.contains_key(&key) {
        return key;
    }
    for suffix in 2_u64..=u64::MAX {
        let candidate = format!("{key}#{suffix}");
        if !object.contains_key(&candidate) {
            return candidate;
        }
    }
    unreachable!("a finite JSON object must have an unused redacted key")
}

fn sensitive_name(name: &str) -> bool {
    let normalized = name.to_ascii_lowercase().replace(['-', '_'], "");
    normalized.contains("authorization")
        || normalized.contains("apikey")
        || normalized.contains("secret")
        || normalized.contains("password")
        || normalized.contains("passwd")
        || normalized.contains("passphrase")
        || normalized.contains("credential")
        || normalized.contains("privatekey")
        || normalized.ends_with("pass")
        || normalized.ends_with("token")
}

fn replace_secrets(bytes: &mut Vec<u8>, secrets: &[Vec<u8>]) {
    const REDACTED: &[u8] = b"[REDACTED]";
    for secret in secrets.iter().filter(|secret| secret.len() >= 4) {
        let mut offset = 0;
        while let Some(relative) = bytes[offset..]
            .windows(secret.len())
            .position(|window| window == secret.as_slice())
        {
            let start = offset + relative;
            bytes.splice(start..start + secret.len(), REDACTED.iter().copied());
            offset = start + REDACTED.len();
        }
    }
}

fn run_id() -> String {
    format!("run-{}-{}", now_ms(), random_suffix())
}

fn random_suffix() -> String {
    format!("{:08x}", rand::rng().random::<u32>())
}

fn random_token() -> String {
    format!("{:064x}", rand::rng().random::<u128>())
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[derive(Debug, thiserror::Error)]
pub enum RunError {
    #[error("unknown scenario: {0}")]
    UnknownScenario(String),
    #[error("unknown run: {0}")]
    UnknownRun(String),
    #[error("run is not ready for an attached terminal: {0}")]
    RunUnavailable(String),
    #[error("invalid run request: {0}")]
    InvalidRequest(String),
    #[error("invalid scenario: {0}")]
    InvalidScenario(String),
    #[error("path escapes its configured root: {0}")]
    PathEscape(PathBuf),
    #[error("driver protocol failed: {0}")]
    Protocol(String),
    #[error("failed to persist run evidence: {0}")]
    EvidencePersistence(String),
    #[error("workspace evidence exceeds its retention limit: {0}")]
    EvidenceLimit(String),
    #[error("workspace contains an unsupported filesystem entry: {0}")]
    UnsupportedWorkspaceEntry(PathBuf),
    #[error(transparent)]
    Process(#[from] agent_lab_driver_protocol::ProcessError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Toml(#[from] toml::de::Error),
}

impl From<RunError> for (StatusCode, String) {
    fn from(error: RunError) -> Self {
        let status = match error {
            RunError::UnknownRun(_) | RunError::UnknownScenario(_) => StatusCode::NOT_FOUND,
            RunError::RunUnavailable(_) => StatusCode::CONFLICT,
            RunError::InvalidRequest(_)
            | RunError::InvalidScenario(_)
            | RunError::PathEscape(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, error.to_string())
    }
}

#[allow(dead_code)]
fn _assert_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<RunController>();
    assert_send_sync::<DriverTranscript>();
    assert_send_sync::<SocketAddr>();
    assert_send_sync::<HeaderValue>();
    assert_send_sync::<OsString>();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(sequence: u64, kind: &str, payload: JsonValue) -> RunEvent {
        RunEvent {
            sequence,
            at_ms: u128::from(sequence),
            kind: kind.to_owned(),
            payload,
        }
    }

    fn temporary_root(label: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "agent-lab-{label}-{}-{}",
            std::process::id(),
            random_suffix()
        ));
        fs::create_dir(&root).expect("temporary root should be created");
        root
    }

    fn write_scenario(root: &Path) {
        fs::create_dir_all(root.join("catalog/workspace")).unwrap();
        fs::write(root.join("catalog/workspace/README.md"), "seed\n").unwrap();
        fs::write(
            root.join("catalog.toml"),
            r#"
version = 1
id = "catalog"
title = "Catalog"
description = "test"
question = "How does the harness produce the expected catalog artifact?"
seed = "catalog/workspace"
prompt = "write output"
output = "result.json"

[limits]
maxDurationMs = 1000
maxCommandCount = 1
maxOrchestratorInvocations = 1
maxToolInvocations = 1

[assertions]
activeNames = ["alpha", "gamma"]
totalScore = 11
"#,
        )
        .unwrap();
    }

    fn catalog_scenario() -> ScenarioManifest {
        ScenarioManifest {
            version: 1,
            id: "catalog".to_owned(),
            title: "Catalog".to_owned(),
            description: "test".to_owned(),
            question: "test".to_owned(),
            seed: "catalog/workspace".into(),
            prompt: "write output".to_owned(),
            output: "result.json".into(),
            limits: ScenarioLimits {
                max_duration_ms: 1_000,
                max_command_count: 1,
                max_orchestrator_invocations: 1,
                max_tool_invocations: 1,
            },
            assertions: CatalogAssertions {
                active_names: vec!["alpha".to_owned(), "gamma".to_owned()],
                total_score: 11,
            },
        }
    }

    fn test_run_state(root: &Path) -> RunState {
        let workspace = root.join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        let initial_snapshot = snapshot_tree(&root.join("initial")).ok();
        let (sender, _) = broadcast::channel(1);
        RunState {
            summary: Mutex::new(RunSummary {
                id: "run-events".to_owned(),
                scenario_id: "catalog".to_owned(),
                scenario_title: "Catalog".to_owned(),
                model_id: "test/model".to_owned(),
                status: RunStatus::Running,
                started_at_ms: 1,
                finished_at_ms: None,
                event_count: 0,
                error: None,
            }),
            assembly: Mutex::new(AssemblySnapshot {
                question: "How does the harness create a file?".to_owned(),
                scenario: AssemblyScenario {
                    id: "catalog".to_owned(),
                    title: "Catalog".to_owned(),
                    description: "test".to_owned(),
                    version: 1,
                    output: "result.json".into(),
                },
                harness: HarnessAssembly {
                    adapter: "external-driver".to_owned(),
                    model_id: Some("test/model".to_owned()),
                    driver: None,
                },
                workspace: WorkspaceAssembly {
                    id: "run-events/workspace".to_owned(),
                    seed: "catalog/workspace".into(),
                    seed_revision: "catalog@1".to_owned(),
                    attachment: "root-confined-physical".to_owned(),
                    change_tracking: "initial-and-final-snapshots".to_owned(),
                },
                capability_sources: Vec::new(),
                limits: ScenarioLimits {
                    max_duration_ms: 1_000,
                    max_command_count: 1,
                    max_orchestrator_invocations: 1,
                    max_tool_invocations: 1,
                },
            }),
            events: Mutex::new(Vec::new()),
            sender,
            cancel: CancellationToken::new(),
            bundle_dir: root.to_path_buf(),
            workspace,
            output: "result.json".into(),
            initial_snapshot,
            capabilities: Mutex::new(Vec::new()),
            secret_values: Mutex::new(Vec::new()),
            replay_failed: false,
        }
    }

    #[test]
    fn confined_children_reject_parent_and_absolute_escapes() {
        let root = Path::new("/tmp/agent-lab-root");
        assert!(confined_child(root, "workspace/result.json").is_ok());
        assert!(confined_child(root, "../outside").is_err());
        assert!(confined_child(root, "/outside").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn existing_children_reject_intermediate_symlink_escapes() {
        let root = temporary_root("intermediate-symlink");
        let scenarios = root.join("scenarios");
        let outside = root.join("outside");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, scenarios.join("link")).unwrap();

        assert!(matches!(
            confined_existing_child(&scenarios, "link"),
            Err(RunError::PathEscape(_))
        ));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn workspace_absolute_outputs_are_normalized_to_relative_paths() {
        assert_eq!(
            workspace_relative_path(Path::new("result.json")).unwrap(),
            Path::new("result.json")
        );
        assert_eq!(
            workspace_relative_path(Path::new("/agent-lab-workspace/nested/result.json")).unwrap(),
            Path::new("nested/result.json")
        );
        assert!(workspace_relative_path(Path::new("/outside/result.json")).is_err());
        assert!(workspace_relative_path(Path::new("/agent-lab-workspace")).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn response_wait_observes_cancellation_before_its_timeout() {
        let mut driver = DriverProcess::spawn("/bin/sh", ["-c", "sleep 30"]).unwrap();
        let cancel = CancellationToken::new();
        let trigger = cancel.clone();
        let canceller = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            trigger.cancel();
        });
        let started = Instant::now();
        let message =
            receive_with_cancellation(&mut driver, Duration::from_secs(30), &cancel).unwrap();
        canceller.join().unwrap();
        assert!(message.is_none());
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[cfg(unix)]
    #[test]
    fn exit_wait_observes_cancellation_before_its_timeout() {
        let mut driver = DriverProcess::spawn("/bin/sh", ["-c", "sleep 30"]).unwrap();
        let cancel = CancellationToken::new();
        let trigger = cancel.clone();
        let canceller = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            trigger.cancel();
        });
        let started = Instant::now();
        let exit =
            wait_for_exit_with_cancellation(&mut driver, Duration::from_secs(30), &cancel).unwrap();
        canceller.join().unwrap();
        assert!(matches!(exit, ExitWait::Cancelled));
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn password_and_credential_environment_names_are_sensitive() {
        for name in [
            "PASSWORD",
            "DB_PASSWORD",
            "DATABASE_PASS",
            "SSH_PASSPHRASE",
            "SERVICE_CREDENTIAL",
            "API_TOKEN",
            "PRIVATE_KEY",
            "SSH_PRIVATE_KEY",
        ] {
            assert!(sensitive_name(name), "{name} should be redacted");
        }
        assert!(!sensitive_name("PATH"));
        assert!(!sensitive_name("MODEL_ID"));
    }

    #[test]
    fn turn_identity_rejects_stale_driver_messages() {
        assert!(
            validate_turn_identity("session", "turn", "session", "turn", "turn.finished").is_ok()
        );
        let error = validate_turn_identity(
            "stale-session",
            "stale-turn",
            "session",
            "turn",
            "turn.finished",
        )
        .unwrap_err();
        assert!(error.to_string().contains("does not match active turn"));
    }

    #[test]
    fn driver_events_cannot_claim_controller_owned_kinds() {
        assert_eq!(driver_event_kind("v0.mdx"), "v0.mdx");
        for reserved in [
            "run.finished",
            "mcp.tool.completed",
            "workspace.finalized",
            "driver.ready",
            "controller.limit-exceeded",
        ] {
            assert_eq!(
                driver_event_kind(reserved),
                format!("driver.event.{reserved}")
            );
        }
    }

    #[test]
    fn driver_descriptors_are_redacted_before_persistence() {
        let descriptor = DriverDescriptor {
            name: "driver-provider-secret".to_owned(),
            version: "provider-secret".to_owned(),
            revision: Some("revision-provider-secret".to_owned()),
            features: vec!["feature-provider-secret".to_owned()],
        };
        let redacted = redact_driver_descriptor(descriptor, &[b"provider-secret".to_vec()]);
        let serialized = serde_json::to_string(&redacted).unwrap();
        assert!(!serialized.contains("provider-secret"));
        assert!(serialized.contains("[REDACTED]"));
    }

    #[test]
    fn only_a_zero_driver_exit_is_successful() {
        assert!(require_successful_driver_exit(Some(0)).is_ok());
        assert!(require_successful_driver_exit(Some(17)).is_err());
        assert!(require_successful_driver_exit(None).is_err());
    }

    #[test]
    fn configured_limits_count_only_activity_after_the_turn_starts() {
        let limits = ScenarioLimits {
            max_duration_ms: 1_000,
            max_command_count: 1,
            max_orchestrator_invocations: 1,
            max_tool_invocations: 1,
        };
        let events = vec![
            event(
                1,
                "mcp.tool.started",
                json!({ "source": "catalog", "actor": "human" }),
            ),
            event(2, "driver.turn-started", json!({})),
            event(
                3,
                "mcp.tool.started",
                json!({ "source": "catalog", "actor": "human" }),
            ),
            event(
                4,
                "mcp.tool.started",
                json!({ "source": "catalog", "actor": "agent" }),
            ),
            event(
                5,
                "mcp.tool.started",
                json!({ "source": "analysis", "actor": "agent" }),
            ),
        ];
        let error = configured_limit_error(&events, 2, &limits).unwrap();
        assert!(error.contains("tool invocation"));
        assert!(configured_limit_error(&events, 5, &limits).is_none());
    }

    #[test]
    fn catalog_score_requires_the_complete_output_schema() {
        let root = temporary_root("catalog-schema");
        let state = test_run_state(&root);
        let scenario = catalog_scenario();
        fs::write(
            state.workspace.join("result.json"),
            br#"{"active":[{"name":"alpha"},{"name":"gamma"}],"totalScore":11}"#,
        )
        .unwrap();
        let incomplete = score_catalog(&state, &scenario).unwrap();
        assert_eq!(incomplete["schemaValid"], false);
        assert_eq!(incomplete["passed"], false);

        fs::write(
            state.workspace.join("result.json"),
            br#"{"active":[{"name":"alpha","active":true,"score":3},{"name":"gamma","active":true,"score":8}],"activeCount":2,"totalScore":11}"#,
        )
        .unwrap();
        let without_capability_evidence = score_catalog(&state, &scenario).unwrap();
        assert_eq!(without_capability_evidence["schemaValid"], true);
        assert_eq!(
            without_capability_evidence["capabilityEvidenceComplete"],
            false
        );
        assert_eq!(without_capability_evidence["passed"], false);

        let catalog_items = json!([
            { "name": "alpha", "active": true, "score": 3 },
            { "name": "beta", "active": false, "score": 5 },
            { "name": "gamma", "active": true, "score": 8 }
        ]);
        lock(&state.events).extend([
            event(
                1,
                "mcp.tool.completed",
                json!({
                    "source": "catalog",
                    "name": "list",
                    "actor": "agent",
                    "isError": false,
                    "result": { "items": catalog_items.clone() },
                }),
            ),
            event(
                2,
                "mcp.tool.completed",
                json!({
                    "source": "analysis",
                    "name": "summarize",
                    "actor": "agent",
                    "isError": false,
                    "arguments": { "items": [{ "name": "fabricated", "active": true, "score": 11 }] },
                    "result": {
                        "active": [{ "name": "fabricated", "active": true, "score": 11 }],
                        "activeCount": 1,
                        "totalScore": 11,
                    },
                }),
            ),
        ]);
        let fabricated = score_catalog(&state, &scenario).unwrap();
        assert_eq!(fabricated["capabilityEvidenceComplete"], true);
        assert_eq!(fabricated["catalogAnalysisComposed"], false);
        assert_eq!(fabricated["passed"], false);

        lock(&state.events).push(event(
            3,
            "mcp.tool.completed",
            json!({
                "source": "analysis",
                "name": "summarize",
                "actor": "agent",
                "isError": false,
                "arguments": { "items": catalog_items },
                "result": {
                    "active": [
                        { "name": "alpha", "active": true, "score": 3 },
                        { "name": "gamma", "active": true, "score": 8 },
                    ],
                    "activeCount": 2,
                    "totalScore": 11,
                },
            }),
        ));

        fs::write(
            state.workspace.join("result.json"),
            br#"{"active":[{"name":"alpha","active":true,"score":10},{"name":"gamma","active":true,"score":1}],"activeCount":2,"totalScore":11}"#,
        )
        .unwrap();
        let fabricated_scores = score_catalog(&state, &scenario).unwrap();
        assert_eq!(fabricated_scores["catalogAnalysisComposed"], true);
        assert_eq!(fabricated_scores["analysisResultMatches"], false);
        assert_eq!(fabricated_scores["passed"], false);

        fs::write(
            state.workspace.join("result.json"),
            br#"{"active":[{"name":"alpha","active":true,"score":3},{"name":"gamma","active":true,"score":8}],"activeCount":2,"totalScore":11}"#,
        )
        .unwrap();
        let complete = score_catalog(&state, &scenario).unwrap();
        assert_eq!(complete["catalogAnalysisComposed"], true);
        assert_eq!(complete["analysisResultMatches"], true);
        assert_eq!(complete["passed"], true);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn confined_json_reads_treat_missing_outputs_as_absent() {
        let root = temporary_root("output-missing");
        let workspace = root.join("workspace");
        fs::create_dir(&workspace).unwrap();

        assert_eq!(
            read_optional_confined_json(&workspace, "result.json").unwrap(),
            None
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn confined_json_reads_refuse_symlinks_outside_the_workspace() {
        let root = temporary_root("output-symlink");
        let workspace = root.join("workspace");
        fs::create_dir(&workspace).unwrap();
        fs::write(root.join("outside.json"), br#"{"secret":true}"#).unwrap();
        std::os::unix::fs::symlink(root.join("outside.json"), workspace.join("result.json"))
            .unwrap();

        assert!(matches!(
            read_optional_confined_json(&workspace, "result.json"),
            Err(RunError::PathEscape(_))
        ));
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn confined_json_reads_refuse_symlinked_intermediate_directories() {
        let root = temporary_root("output-intermediate-symlink");
        let workspace = root.join("workspace");
        let outside = root.join("outside");
        fs::create_dir(&workspace).unwrap();
        fs::create_dir(&outside).unwrap();
        fs::write(outside.join("result.json"), br#"{"secret":true}"#).unwrap();
        std::os::unix::fs::symlink(&outside, workspace.join("nested")).unwrap();

        assert!(matches!(
            read_optional_confined_json(&workspace, "nested/result.json"),
            Err(RunError::PathEscape(_))
        ));
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn finalized_workspace_refuses_hard_links_to_outside_files() {
        let root = temporary_root("output-hard-link");
        fs::create_dir(root.join("initial")).unwrap();
        fs::create_dir(root.join("workspace")).unwrap();
        fs::write(root.join("outside.json"), br#"{"secret":true}"#).unwrap();
        fs::hard_link(
            root.join("outside.json"),
            root.join("workspace/result.json"),
        )
        .unwrap();
        let state = test_run_state(&root);

        assert!(matches!(
            read_optional_confined_json(&state.workspace, "result.json"),
            Err(RunError::PathEscape(_))
        ));
        assert!(matches!(
            finalize_workspace(&state),
            Err(RunError::PathEscape(_))
        ));
        assert!(!root.join("final").exists());
        assert!(!root.join("final.tmp").exists());

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn concurrent_events_remain_ordered_replayable_json_lines() {
        const THREADS: usize = 8;
        const EVENTS_PER_THREAD: usize = 50;

        let root = temporary_root("concurrent-events");
        let state = test_run_state(&root);
        let barrier = Arc::new(std::sync::Barrier::new(THREADS));

        std::thread::scope(|scope| {
            for thread_index in 0..THREADS {
                let barrier = Arc::clone(&barrier);
                let state = &state;
                scope.spawn(move || {
                    barrier.wait();
                    for event_index in 0..EVENTS_PER_THREAD {
                        record_event(
                            state,
                            "test.concurrent",
                            json!({
                                "thread": thread_index,
                                "event": event_index,
                                "detail": "x".repeat(8_192),
                            }),
                        )
                        .unwrap();
                    }
                });
            }
        });

        let expected_count = THREADS * EVENTS_PER_THREAD;
        let events = read_events(&root.join("events.jsonl")).unwrap();
        assert_eq!(events.len(), expected_count);
        assert_eq!(lock(&state.events).len(), expected_count);
        assert_eq!(lock(&state.summary).event_count, expected_count as u64);
        for (index, event) in events.iter().enumerate() {
            assert_eq!(event.sequence, index as u64 + 1);
        }

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn capability_observations_are_redacted_before_persistence() {
        let root = temporary_root("capability-redaction");
        let state = Arc::new(test_run_state(&root));
        lock(&state.secret_values).push(b"capability-secret".to_vec());
        let observe = source_observer(state.clone(), "catalog", "human");
        observe(
            "mcp.tool.started",
            json!({
                "arguments": {
                    "apiKey": "key-name-redaction",
                    "note": "the agent sent capability-secret"
                }
            }),
        );
        let events = lock(&state.events);
        assert_eq!(events[0].payload["arguments"]["apiKey"], "[REDACTED]");
        assert_eq!(
            events[0].payload["arguments"]["note"],
            "the agent sent [REDACTED]"
        );
        assert!(
            !fs::read_to_string(root.join("events.jsonl"))
                .unwrap()
                .contains("capability-secret")
        );
        drop(events);
        drop(state);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn prepared_exploration_redacts_driver_environment_secrets() {
        let root = temporary_root("prepared-secret-redaction");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);
        let mut driver = DriverLaunch::new("/driver-is-not-needed-for-exploration");
        driver
            .env
            .push(("PROVIDER_TOKEN".into(), "provider-secret".into()));
        let controller = RunController::new(RunControllerConfig {
            scenarios_dir: scenarios,
            data_dir: data,
            driver,
        })
        .unwrap();
        let summary = controller
            .prepare(PrepareRunRequest {
                scenario_id: "catalog".to_owned(),
            })
            .await
            .unwrap();
        let state = controller.state(&summary.id).unwrap();

        source_observer(state.clone(), "catalog", "human")(
            "mcp.tool.started",
            json!({ "arguments": { "note": "provider-secret" } }),
        );

        assert_eq!(
            lock(&state.events).last().unwrap().payload["arguments"]["note"],
            "[REDACTED]"
        );
        assert!(
            !fs::read_to_string(state.bundle_dir.join("events.jsonl"))
                .unwrap()
                .contains("provider-secret")
        );
        drop(state);
        drop(controller);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn prepared_scenario_reuses_its_workspace_and_sources_after_controller_restart() {
        let root = temporary_root("prepare");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);

        let config = || RunControllerConfig {
            scenarios_dir: scenarios.clone(),
            data_dir: data.clone(),
            driver: DriverLaunch::new("/driver-is-not-needed-for-exploration"),
        };
        let controller = RunController::new(config()).unwrap();
        let prepared = controller
            .prepare(PrepareRunRequest {
                scenario_id: "catalog".to_owned(),
            })
            .await
            .unwrap();
        assert_eq!(prepared.status, RunStatus::Exploring);
        assert!(prepared.model_id.is_empty());
        assert!(controller.list().is_empty());
        let assembly = controller.get(&prepared.id).unwrap().assembly;
        assert_eq!(
            assembly.question,
            "How does the harness produce the expected catalog artifact?"
        );
        assert_eq!(assembly.capability_sources.len(), 2);
        assert_eq!(assembly.capability_sources[0].projections.len(), 2);
        let binding = controller.terminal_binding(&prepared.id).unwrap();
        assert_eq!(binding.sources.len(), 2);
        let workspace = binding.workspace;
        drop(controller);

        let restarted = RunController::new(config()).unwrap();
        let unavailable = restarted
            .start_prepared(
                &prepared.id,
                StartPreparedRunRequest {
                    model_id: "test/model".to_owned(),
                },
            )
            .unwrap_err();
        assert!(matches!(unavailable, RunError::RunUnavailable(_)));
        assert_eq!(
            restarted.get(&prepared.id).unwrap().summary.status,
            RunStatus::Exploring
        );
        let resumed = restarted
            .prepare(PrepareRunRequest {
                scenario_id: "catalog".to_owned(),
            })
            .await
            .unwrap();
        assert_eq!(resumed.id, prepared.id);
        assert_eq!(restarted.workspace(&resumed.id).unwrap(), workspace);
        assert_eq!(
            restarted
                .terminal_binding(&resumed.id)
                .unwrap()
                .sources
                .len(),
            2
        );

        drop(restarted);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn live_workspace_output_is_redacted_before_it_reaches_run_detail() {
        let root = temporary_root("live-output-redaction");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);
        let controller = RunController::new(RunControllerConfig {
            scenarios_dir: scenarios,
            data_dir: data,
            driver: DriverLaunch::new("/driver-is-not-needed-for-exploration"),
        })
        .unwrap();
        let prepared = controller
            .prepare(PrepareRunRequest {
                scenario_id: "catalog".to_owned(),
            })
            .await
            .unwrap();
        let state = controller.state(&prepared.id).unwrap();
        lock(&state.secret_values).push(b"provider-secret".to_vec());
        fs::write(
            state.workspace.join("result.json"),
            br#"{"apiKey":"named-secret","note":"contains provider-secret"}"#,
        )
        .unwrap();

        let detail = controller.get(&prepared.id).unwrap();
        let output = detail.output.unwrap();
        assert_eq!(output["apiKey"], "[REDACTED]");
        assert_eq!(output["note"], "contains [REDACTED]");

        drop(controller);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn cancelling_an_exploration_finalizes_it_and_releases_its_sources() {
        let root = temporary_root("cancel-exploration");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);
        let controller = RunController::new(RunControllerConfig {
            scenarios_dir: scenarios,
            data_dir: data,
            driver: DriverLaunch::new("/driver-is-not-needed-for-exploration"),
        })
        .unwrap();
        let prepared = controller
            .prepare(PrepareRunRequest {
                scenario_id: "catalog".to_owned(),
            })
            .await
            .unwrap();
        let state = controller.state(&prepared.id).unwrap();
        let source_cancellations = lock(&state.capabilities)
            .iter()
            .map(|source| source.cancel.clone())
            .collect::<Vec<_>>();

        controller.cancel(&prepared.id).unwrap();

        let cancelled = controller.get(&prepared.id).unwrap();
        assert_eq!(cancelled.summary.status, RunStatus::Cancelled);
        assert_eq!(cancelled.score.unwrap()["cancelled"], true);
        assert!(state.cancel.is_cancelled());
        assert!(lock(&state.capabilities).is_empty());
        assert!(
            source_cancellations
                .iter()
                .all(CancellationToken::is_cancelled)
        );
        assert!(matches!(
            controller.terminal_binding(&prepared.id),
            Err(RunError::RunUnavailable(_))
        ));

        let replacement = controller
            .prepare(PrepareRunRequest {
                scenario_id: "catalog".to_owned(),
            })
            .await
            .unwrap();
        assert_ne!(replacement.id, prepared.id);

        drop(controller);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn failed_start_persistence_rolls_back_to_exploring() {
        let root = temporary_root("start-rollback");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);
        let controller = RunController::new(RunControllerConfig {
            scenarios_dir: scenarios,
            data_dir: data,
            driver: DriverLaunch::new("/driver-must-not-start"),
        })
        .unwrap();
        let prepared = controller
            .prepare(PrepareRunRequest {
                scenario_id: "catalog".to_owned(),
            })
            .await
            .unwrap();
        let state = controller.state(&prepared.id).unwrap();
        fs::create_dir(state.bundle_dir.join("assembly.json.tmp")).unwrap();

        controller
            .start_prepared(
                &prepared.id,
                StartPreparedRunRequest {
                    model_id: "test/model".to_owned(),
                },
            )
            .unwrap_err();

        let detail = controller.get(&prepared.id).unwrap();
        assert_eq!(detail.summary.status, RunStatus::Exploring);
        assert!(detail.summary.model_id.is_empty());
        assert!(detail.assembly.harness.model_id.is_none());
        let manifest: RunSummary =
            serde_json::from_slice(&fs::read(state.bundle_dir.join("manifest.json")).unwrap())
                .unwrap();
        assert_eq!(manifest.status, RunStatus::Exploring);
        assert!(manifest.model_id.is_empty());
        assert!(!state.cancel.is_cancelled());
        assert!(!lock(&state.capabilities).is_empty());

        drop(controller);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn concurrent_prepare_calls_share_one_scenario_workspace() {
        let root = temporary_root("concurrent-prepare");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);
        let controller = RunController::new(RunControllerConfig {
            scenarios_dir: scenarios,
            data_dir: data,
            driver: DriverLaunch::new("/driver-is-not-needed-for-exploration"),
        })
        .unwrap();
        let request = || PrepareRunRequest {
            scenario_id: "catalog".to_owned(),
        };
        let (first, second) =
            tokio::join!(controller.prepare(request()), controller.prepare(request()));
        assert_eq!(first.unwrap().id, second.unwrap().id);
        assert_eq!(lock(&controller.inner.runs).len(), 1);

        drop(controller);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn interrupted_runs_recover_and_discard_workspace_when_event_replay_is_malformed() {
        let root = temporary_root("interrupted-redaction");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);
        let scenario = load_scenarios(&scenarios).unwrap()["catalog"].clone();
        let bundle = data.join("run-interrupted");
        fs::create_dir_all(bundle.join("workspace")).unwrap();
        fs::create_dir_all(bundle.join("initial")).unwrap();
        fs::create_dir_all(bundle.join("final")).unwrap();
        fs::create_dir_all(bundle.join("final.tmp")).unwrap();
        for path in [
            bundle.join("workspace/result.json"),
            bundle.join("final/result.json"),
            bundle.join("final.tmp/result.json"),
        ] {
            fs::write(path, br#"{"password":"crash-secret"}"#).unwrap();
        }
        fs::write(bundle.join("diff.json"), br#"{"after":"crash-secret"}"#).unwrap();
        let summary = RunSummary {
            id: "run-interrupted".to_owned(),
            scenario_id: "catalog".to_owned(),
            scenario_title: "Catalog".to_owned(),
            model_id: "test/model".to_owned(),
            status: RunStatus::Running,
            started_at_ms: 1,
            finished_at_ms: None,
            event_count: 1,
            error: None,
        };
        write_json_atomic(
            &bundle.join("manifest.json"),
            &serde_json::to_value(&summary).unwrap(),
        )
        .unwrap();
        write_json_atomic(
            &bundle.join("assembly.json"),
            &serde_json::to_value(initial_assembly(&summary, &scenario)).unwrap(),
        )
        .unwrap();
        fs::write(bundle.join("events.jsonl"), b"{malformed event evidence\n").unwrap();

        let config = || RunControllerConfig {
            scenarios_dir: scenarios.clone(),
            data_dir: data.clone(),
            driver: DriverLaunch::new("/driver-is-not-needed-for-replay"),
        };
        let controller = RunController::new(config()).unwrap();
        let detail = controller.get("run-interrupted").unwrap();
        assert_eq!(detail.summary.status, RunStatus::Cancelled);
        assert!(detail.output.is_none());
        assert!(!bundle.join("workspace").exists());
        assert!(!bundle.join("final").exists());
        assert!(!bundle.join("final.tmp").exists());
        assert!(!bundle.join("diff.json").exists());
        assert_eq!(
            detail.events.last().unwrap().payload["workspaceEvidence"],
            "discarded because redaction material was unavailable"
        );
        assert_eq!(read_events(&bundle.join("events.jsonl")).unwrap().len(), 1);
        drop(controller);

        let replayed = RunController::new(config()).unwrap();
        assert_eq!(
            replayed.get("run-interrupted").unwrap().summary.status,
            RunStatus::Cancelled
        );
        drop(replayed);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn interrupted_manifest_recovers_an_already_finalized_run() {
        let root = temporary_root("finalized-recovery");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);
        let scenario = load_scenarios(&scenarios).unwrap()["catalog"].clone();
        let bundle = data.join("run-finalized");
        fs::create_dir_all(bundle.join("workspace")).unwrap();
        fs::create_dir_all(bundle.join("initial")).unwrap();
        fs::create_dir_all(bundle.join("final")).unwrap();
        let output = br#"{"active":[],"activeCount":0,"totalScore":0}"#;
        fs::write(bundle.join("workspace/result.json"), output).unwrap();
        fs::write(bundle.join("final/result.json"), output).unwrap();
        write_json_atomic(
            &bundle.join("diff.json"),
            &json!({ "changes": [{ "path": "result.json", "kind": "created" }] }),
        )
        .unwrap();
        let summary = RunSummary {
            id: "run-finalized".to_owned(),
            scenario_id: "catalog".to_owned(),
            scenario_title: "Catalog".to_owned(),
            model_id: "test/model".to_owned(),
            status: RunStatus::Running,
            started_at_ms: 1,
            finished_at_ms: None,
            event_count: 0,
            error: None,
        };
        write_json_atomic(
            &bundle.join("manifest.json"),
            &serde_json::to_value(&summary).unwrap(),
        )
        .unwrap();
        write_json_atomic(
            &bundle.join("assembly.json"),
            &serde_json::to_value(initial_assembly(&summary, &scenario)).unwrap(),
        )
        .unwrap();
        let finished = event(
            1,
            "run.finished",
            json!({ "status": RunStatus::Passed, "error": null, "score": { "passed": true } }),
        );
        fs::write(
            bundle.join("events.jsonl"),
            format!("{}\n", serde_json::to_string(&finished).unwrap()),
        )
        .unwrap();

        let controller = RunController::new(RunControllerConfig {
            scenarios_dir: scenarios,
            data_dir: data,
            driver: DriverLaunch::new("/driver-is-not-needed-for-replay"),
        })
        .unwrap();
        let detail = controller.get("run-finalized").unwrap();
        assert_eq!(detail.summary.status, RunStatus::Passed);
        assert_eq!(detail.summary.finished_at_ms, Some(1));
        assert_eq!(detail.score.unwrap()["passed"], true);
        assert!(bundle.join("workspace/result.json").is_file());
        assert!(bundle.join("final/result.json").is_file());
        assert_eq!(
            serde_json::from_slice::<RunSummary>(&fs::read(bundle.join("manifest.json")).unwrap())
                .unwrap()
                .status,
            RunStatus::Passed
        );

        drop(controller);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn subscription_captures_events_recorded_after_the_prefix_handoff() {
        let root = temporary_root("subscribe");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);
        let controller = RunController::new(RunControllerConfig {
            scenarios_dir: scenarios,
            data_dir: data,
            driver: DriverLaunch::new("/driver-is-not-needed-for-exploration"),
        })
        .unwrap();
        let prepared = controller
            .prepare(PrepareRunRequest {
                scenario_id: "catalog".to_owned(),
            })
            .await
            .unwrap();
        let (history, mut receiver) = controller.subscribe(&prepared.id).unwrap();
        let state = controller.state(&prepared.id).unwrap();
        record_event(&state, "test.after-subscribe", JsonValue::Null).unwrap();
        let live = receiver.try_recv().unwrap();
        assert_eq!(live.kind, "test.after-subscribe");
        assert!(live.sequence > history.last().map_or(0, |event| event.sequence));

        drop(controller);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn lagged_subscriptions_replay_the_missing_durable_suffix() {
        use futures_util::StreamExt;

        let root = temporary_root("lagged-subscribe");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);
        let controller = RunController::new(RunControllerConfig {
            scenarios_dir: scenarios,
            data_dir: data,
            driver: DriverLaunch::new("/driver-is-not-needed-for-exploration"),
        })
        .unwrap();
        let prepared = controller
            .prepare(PrepareRunRequest {
                scenario_id: "catalog".to_owned(),
            })
            .await
            .unwrap();
        let (history, receiver) = controller.subscribe(&prepared.id).unwrap();
        let state = controller.state(&prepared.id).unwrap();
        for index in 0..300 {
            record_event(&state, "test.lag", json!({ "index": index })).unwrap();
        }
        let expected = lock(&state.events).len();
        let events =
            crate::run_event_stream(controller.clone(), prepared.id.clone(), history, receiver)
                .take(expected)
                .collect::<Vec<_>>()
                .await;
        assert_eq!(events.len(), expected);
        assert!(
            events
                .iter()
                .enumerate()
                .all(|(index, event)| event.sequence == index as u64 + 1)
        );

        drop(controller);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn legacy_bundles_recover_their_assembly_from_events() {
        let root = temporary_root("replay");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);

        let bundle = data.join("run-replay");
        fs::create_dir_all(bundle.join("workspace")).unwrap();
        fs::create_dir_all(bundle.join("final")).unwrap();
        fs::write(
            bundle.join("final/result.json"),
            r#"{"active":[{"name":"alpha"},{"name":"gamma"}],"totalScore":11}"#,
        )
        .unwrap();
        fs::write(
            bundle.join("workspace/result.json"),
            r#"{"active":[],"totalScore":0}"#,
        )
        .unwrap();
        let summary = RunSummary {
            id: "run-replay".to_owned(),
            scenario_id: "catalog".to_owned(),
            scenario_title: "Catalog".to_owned(),
            model_id: "test/model".to_owned(),
            status: RunStatus::Passed,
            started_at_ms: 1,
            finished_at_ms: Some(2),
            event_count: 1,
            error: None,
        };
        fs::write(
            bundle.join("manifest.json"),
            serde_json::to_vec(&summary).unwrap(),
        )
        .unwrap();
        let events = [
            RunEvent {
                sequence: 1,
                at_ms: 1,
                kind: "driver.ready".to_owned(),
                payload: json!({
                    "name": "fixture-driver",
                    "version": "1.0.0",
                    "features": ["streaming"]
                }),
            },
            RunEvent {
                sequence: 2,
                at_ms: 1,
                kind: "capability.source.started".to_owned(),
                payload: json!({
                    "id": "catalog",
                    "revision": "catalog-v2",
                    "transport": "streamable-http"
                }),
            },
            RunEvent {
                sequence: 3,
                at_ms: 2,
                kind: "run.finished".to_owned(),
                payload: json!({ "status": "passed" }),
            },
        ];
        fs::write(
            bundle.join("events.jsonl"),
            events
                .iter()
                .map(|event| serde_json::to_string(event).unwrap())
                .collect::<Vec<_>>()
                .join("\n")
                + "\n",
        )
        .unwrap();
        fs::write(bundle.join("score.json"), br#"{"passed":true}"#).unwrap();

        let controller = RunController::new(RunControllerConfig {
            scenarios_dir: scenarios,
            data_dir: data,
            driver: DriverLaunch::new("/driver-is-not-needed-for-replay"),
        })
        .unwrap();
        let detail = controller.get("run-replay").unwrap();
        assert_eq!(detail.summary.status, RunStatus::Passed);
        assert_eq!(detail.events.len(), 3);
        assert_eq!(detail.output.unwrap()["totalScore"], 11);
        assert_eq!(detail.score.unwrap()["passed"], true);
        assert_eq!(
            detail.assembly.harness.driver.unwrap().name,
            "fixture-driver"
        );
        assert_eq!(detail.assembly.capability_sources[0].revision, "catalog-v2");
        assert_eq!(
            detail.assembly.capability_sources[0].protocol,
            "mcp-streamable-http"
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn finalized_bundles_replay_after_their_scenario_is_removed() {
        let root = temporary_root("scenario-independent-replay");
        let scenarios_dir = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios_dir).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios_dir);
        let scenario = load_scenarios(&scenarios_dir).unwrap()["catalog"].clone();
        let bundle = data.join("run-replay");
        fs::create_dir_all(bundle.join("workspace")).unwrap();
        fs::create_dir_all(bundle.join("final")).unwrap();
        fs::write(bundle.join("final/result.json"), br#"{"totalScore":11}"#).unwrap();
        let summary = RunSummary {
            id: "run-replay".to_owned(),
            scenario_id: "catalog".to_owned(),
            scenario_title: "Catalog".to_owned(),
            model_id: "test/model".to_owned(),
            status: RunStatus::Passed,
            started_at_ms: 1,
            finished_at_ms: Some(2),
            event_count: 1,
            error: None,
        };
        write_json_atomic(
            &bundle.join("manifest.json"),
            &serde_json::to_value(&summary).unwrap(),
        )
        .unwrap();
        write_json_atomic(
            &bundle.join("assembly.json"),
            &serde_json::to_value(initial_assembly(&summary, &scenario)).unwrap(),
        )
        .unwrap();
        fs::write(
            bundle.join("events.jsonl"),
            serde_json::to_string(&event(1, "run.finished", json!({ "status": "passed" })))
                .unwrap()
                + "\n",
        )
        .unwrap();
        write_json_atomic(&bundle.join("score.json"), &json!({ "passed": true })).unwrap();

        let other = fs::read_to_string(scenarios_dir.join("catalog.toml"))
            .unwrap()
            .replacen("id = \"catalog\"", "id = \"other\"", 1);
        fs::write(scenarios_dir.join("catalog.toml"), other).unwrap();
        let controller = RunController::new(RunControllerConfig {
            scenarios_dir,
            data_dir: data,
            driver: DriverLaunch::new("/driver-is-not-needed-for-replay"),
        })
        .unwrap();
        let detail = controller.get("run-replay").unwrap();
        assert_eq!(detail.summary.scenario_id, "catalog");
        assert_eq!(detail.output.unwrap()["totalScore"], 11);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn malformed_output_is_reported_without_breaking_replay() {
        let root = temporary_root("malformed-output");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);
        let scenario = load_scenarios(&scenarios).unwrap()["catalog"].clone();
        let bundle = data.join("run-malformed-output");
        fs::create_dir_all(bundle.join("workspace")).unwrap();
        fs::create_dir_all(bundle.join("final")).unwrap();
        fs::write(bundle.join("final/result.json"), b"{not-json").unwrap();
        let summary = RunSummary {
            id: "run-malformed-output".to_owned(),
            scenario_id: "catalog".to_owned(),
            scenario_title: "Catalog".to_owned(),
            model_id: "test/model".to_owned(),
            status: RunStatus::Failed,
            started_at_ms: 1,
            finished_at_ms: Some(2),
            event_count: 1,
            error: Some("output was malformed".to_owned()),
        };
        write_json_atomic(
            &bundle.join("manifest.json"),
            &serde_json::to_value(&summary).unwrap(),
        )
        .unwrap();
        write_json_atomic(
            &bundle.join("assembly.json"),
            &serde_json::to_value(initial_assembly(&summary, &scenario)).unwrap(),
        )
        .unwrap();
        fs::write(
            bundle.join("events.jsonl"),
            serde_json::to_string(&event(1, "run.finished", json!({ "status": "failed" })))
                .unwrap()
                + "\n",
        )
        .unwrap();
        let controller = RunController::new(RunControllerConfig {
            scenarios_dir: scenarios,
            data_dir: data,
            driver: DriverLaunch::new("/driver-is-not-needed-for-replay"),
        })
        .unwrap();
        let detail = controller.get("run-malformed-output").unwrap();
        assert!(detail.output.is_none());
        assert!(detail.output_error.is_some());

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn malformed_event_evidence_fails_only_that_replayed_run() {
        let root = temporary_root("malformed-replay");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);

        let bundle = data.join("run-malformed");
        fs::create_dir_all(bundle.join("workspace")).unwrap();
        fs::create_dir_all(bundle.join("final")).unwrap();
        fs::write(bundle.join("workspace/result.json"), br"{}").unwrap();
        fs::write(bundle.join("final/result.json"), br"{}").unwrap();
        let summary = RunSummary {
            id: "run-malformed".to_owned(),
            scenario_id: "catalog".to_owned(),
            scenario_title: "Catalog".to_owned(),
            model_id: "test/model".to_owned(),
            status: RunStatus::Passed,
            started_at_ms: 1,
            finished_at_ms: Some(2),
            event_count: 2,
            error: None,
        };
        let manifest = serde_json::to_vec(&summary).unwrap();
        let malformed_events = br#"{"sequence":1}{"sequence":2}\n"#;
        fs::write(bundle.join("manifest.json"), &manifest).unwrap();
        fs::write(bundle.join("events.jsonl"), malformed_events).unwrap();

        let controller = RunController::new(RunControllerConfig {
            scenarios_dir: scenarios,
            data_dir: data,
            driver: DriverLaunch::new("/driver-is-not-needed-for-replay"),
        })
        .unwrap();
        let detail = controller.get("run-malformed").unwrap();
        assert_eq!(detail.summary.status, RunStatus::Failed);
        assert!(
            detail
                .summary
                .error
                .as_deref()
                .unwrap()
                .starts_with("stored event replay failed:")
        );
        assert_eq!(detail.review.status, RunStatus::Failed);
        assert_eq!(detail.events.len(), 1);
        assert_eq!(detail.events[0].payload["recovered"], true);
        assert_eq!(fs::read(bundle.join("manifest.json")).unwrap(), manifest);
        assert_eq!(
            fs::read(bundle.join("events.jsonl")).unwrap(),
            malformed_events
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn finalized_workspace_records_a_replayable_diff() {
        let root = temporary_root("diff");
        fs::create_dir(root.join("initial")).unwrap();
        fs::create_dir(root.join("workspace")).unwrap();
        fs::write(root.join("initial/unchanged.txt"), "same").unwrap();
        fs::write(root.join("workspace/unchanged.txt"), "same").unwrap();
        fs::write(
            root.join("workspace/result.json"),
            r#"{"apiKey":"workspace-secret","note":"environment-secret"}"#,
        )
        .unwrap();
        let (sender, _) = broadcast::channel(1);
        let capability_cancel = CancellationToken::new();
        let state = RunState {
            summary: Mutex::new(RunSummary {
                id: "run-diff".to_owned(),
                scenario_id: "catalog".to_owned(),
                scenario_title: "Catalog".to_owned(),
                model_id: "test/model".to_owned(),
                status: RunStatus::Running,
                started_at_ms: 1,
                finished_at_ms: None,
                event_count: 0,
                error: None,
            }),
            assembly: Mutex::new(AssemblySnapshot {
                question: "How does the harness create a file?".to_owned(),
                scenario: AssemblyScenario {
                    id: "catalog".to_owned(),
                    title: "Catalog".to_owned(),
                    description: "test".to_owned(),
                    version: 1,
                    output: "result.json".into(),
                },
                harness: HarnessAssembly {
                    adapter: "external-driver".to_owned(),
                    model_id: Some("test/model".to_owned()),
                    driver: None,
                },
                workspace: WorkspaceAssembly {
                    id: "run-diff/workspace".to_owned(),
                    seed: "catalog/workspace".into(),
                    seed_revision: "catalog@1".to_owned(),
                    attachment: "root-confined-physical".to_owned(),
                    change_tracking: "initial-and-final-snapshots".to_owned(),
                },
                capability_sources: Vec::new(),
                limits: ScenarioLimits {
                    max_duration_ms: 1_000,
                    max_command_count: 1,
                    max_orchestrator_invocations: 1,
                    max_tool_invocations: 1,
                },
            }),
            events: Mutex::new(Vec::new()),
            sender,
            cancel: CancellationToken::new(),
            bundle_dir: root.clone(),
            workspace: root.join("workspace"),
            output: "result.json".into(),
            initial_snapshot: Some(snapshot_tree(&root.join("initial")).unwrap()),
            capabilities: Mutex::new(vec![CapabilityEndpoint {
                id: "catalog".to_owned(),
                revision: "catalog-v2".to_owned(),
                human_url: "http://127.0.0.1:1/human/mcp".to_owned(),
                human_token: "human-token".to_owned(),
                agent_url: "http://127.0.0.1:1/agent/mcp".to_owned(),
                agent_token: "agent-token".to_owned(),
                cancel: capability_cancel.clone(),
            }]),
            secret_values: Mutex::new(vec![b"environment-secret".to_vec()]),
            replay_failed: false,
        };
        finalize_workspace(&state).unwrap();
        let diff = read_optional_json(&root.join("diff.json"))
            .unwrap()
            .unwrap();
        assert_eq!(diff["changes"][0]["path"], "result.json");
        assert_eq!(diff["changes"][0]["kind"], "created");
        let finalized = fs::read_to_string(root.join("final/result.json")).unwrap();
        assert!(!finalized.contains("workspace-secret"));
        assert!(!finalized.contains("environment-secret"));
        let retained_workspace = fs::read_to_string(root.join("workspace/result.json")).unwrap();
        assert!(!retained_workspace.contains("workspace-secret"));
        assert!(!retained_workspace.contains("environment-secret"));
        assert!(
            !serde_json::to_string(&diff)
                .unwrap()
                .contains("environment-secret")
        );
        finish_run(&state, RunStatus::Passed, None, &json!({ "passed": true })).unwrap();
        assert!(capability_cancel.is_cancelled());
        assert!(lock(&state.capabilities).is_empty());
        let review = read_optional_json(&root.join("review.json"))
            .unwrap()
            .unwrap();
        assert_eq!(review["version"], 1);
        assert_eq!(review["steps"][0]["title"], "Created result.json");
        assert_eq!(review["steps"][1]["title"], "Evaluation passed");

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn driver_failure_messages_are_redacted_before_final_evidence() {
        let root = temporary_root("driver-failure-redaction");
        fs::create_dir(root.join("initial")).unwrap();
        let state = test_run_state(&root);
        lock(&state.secret_values).push(b"provider-secret".to_vec());
        let error = RunError::Protocol(
            "driver failed: provider_error: request contained provider-secret".to_owned(),
        );

        let message = redacted_run_error(&state, &error);
        finish_run(
            &state,
            RunStatus::Failed,
            Some(&message),
            &json!({ "passed": false }),
        )
        .unwrap();

        assert!(!message.contains("provider-secret"));
        assert!(message.contains("[REDACTED]"));
        for path in ["manifest.json", "events.jsonl", "review.json"] {
            assert!(
                !fs::read_to_string(root.join(path))
                    .unwrap()
                    .contains("provider-secret"),
                "{path} retained the driver failure secret"
            );
        }

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scores_are_redacted_before_final_evidence() {
        let root = temporary_root("score-redaction");
        fs::create_dir(root.join("initial")).unwrap();
        let state = test_run_state(&root);
        lock(&state.secret_values).push(b"provider-secret".to_vec());

        finish_run(
            &state,
            RunStatus::Failed,
            Some("score contained provider-secret"),
            &json!({ "passed": false, "activeNames": ["provider-secret"] }),
        )
        .unwrap();

        for path in ["score.json", "manifest.json", "events.jsonl", "review.json"] {
            let evidence = fs::read_to_string(root.join(path)).unwrap();
            assert!(
                !evidence.contains("provider-secret"),
                "{path} retained the score secret"
            );
        }
        assert_eq!(
            read_optional_json(&root.join("score.json"))
                .unwrap()
                .unwrap()["activeNames"][0],
            "[REDACTED]"
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn persistence_failures_mark_the_in_memory_run_failed() {
        let root = temporary_root("persistence-failure");
        fs::create_dir(root.join("initial")).unwrap();
        fs::create_dir(root.join("review.json.tmp")).unwrap();
        let state = test_run_state(&root);

        let error =
            finish_run(&state, RunStatus::Passed, None, &json!({ "passed": true })).unwrap_err();

        assert!(matches!(error, RunError::EvidencePersistence(_)));
        let summary = lock(&state.summary).clone();
        assert_eq!(summary.status, RunStatus::Failed);
        assert!(
            summary
                .error
                .as_deref()
                .unwrap()
                .contains("review.json could not be written")
        );
        let score = read_optional_json(&root.join("score.json"))
            .unwrap()
            .unwrap();
        assert_eq!(score["passed"], false);
        let review = build_review(&summary, &lock(&state.events));
        let outcomes = review
            .steps
            .iter()
            .filter(|step| step.kind == "outcome")
            .collect::<Vec<_>>();
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].title, "Evaluation failed");
        assert!(state.cancel.is_cancelled());

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn oversized_workspace_files_are_rejected_before_snapshotting() {
        let root = temporary_root("oversized-evidence");
        fs::create_dir(root.join("initial")).unwrap();
        let state = test_run_state(&root);
        let oversized = state.workspace.join("oversized.bin");
        fs::File::create(&oversized)
            .unwrap()
            .set_len(MAX_EVIDENCE_FILE_BYTES + 1)
            .unwrap();

        assert!(matches!(
            finalize_workspace(&state),
            Err(RunError::EvidenceLimit(_))
        ));
        assert!(!root.join("final.tmp").exists());
        assert!(!root.join("final").exists());

        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn special_workspace_entries_are_rejected_before_snapshotting() {
        let root = PathBuf::from("/tmp").join(format!(
            "agent-lab-special-{}-{}",
            std::process::id(),
            random_suffix()
        ));
        fs::create_dir(&root).unwrap();
        fs::create_dir(root.join("initial")).unwrap();
        let state = test_run_state(&root);
        let socket =
            std::os::unix::net::UnixListener::bind(state.workspace.join("agent.sock")).unwrap();

        assert!(matches!(
            finalize_workspace(&state),
            Err(RunError::UnsupportedWorkspaceEntry(_))
        ));
        assert!(!root.join("final").exists());

        drop(socket);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn finalization_restores_the_protected_initial_snapshot() {
        let root = temporary_root("protected-initial");
        fs::create_dir(root.join("initial")).unwrap();
        fs::write(root.join("initial/seed.txt"), "original").unwrap();
        let state = test_run_state(&root);
        fs::write(state.workspace.join("seed.txt"), "original").unwrap();
        fs::write(state.workspace.join("result.json"), "{}\n").unwrap();
        fs::write(root.join("initial/seed.txt"), "driver-controlled").unwrap();

        let diff = finalize_workspace(&state).unwrap();

        assert_eq!(
            fs::read_to_string(root.join("initial/seed.txt")).unwrap(),
            "original"
        );
        assert_eq!(diff["changes"].as_array().unwrap().len(), 1);
        assert_eq!(diff["changes"][0]["path"], "result.json");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn finalization_replaces_a_preexisting_driver_controlled_snapshot() {
        let root = temporary_root("preexisting-final");
        fs::create_dir(root.join("initial")).unwrap();
        fs::create_dir(root.join("final")).unwrap();
        fs::write(
            root.join("final/result.json"),
            br#"{"origin":"driver-controlled"}"#,
        )
        .unwrap();
        fs::write(root.join("final/stale.txt"), b"stale").unwrap();
        write_json_atomic(&root.join("diff.json"), &json!({ "changes": [] })).unwrap();
        let state = test_run_state(&root);
        fs::write(
            state.workspace.join("result.json"),
            br#"{"origin":"workspace"}"#,
        )
        .unwrap();

        let diff = finalize_workspace(&state).unwrap();
        assert_eq!(
            fs::read_to_string(root.join("final/result.json")).unwrap(),
            r#"{"origin":"workspace"}"#
        );
        assert!(!root.join("final/stale.txt").exists());
        assert_eq!(diff["changes"][0]["path"], "result.json");

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn causal_review_projects_real_harness_activity_without_post_finish_duplicates() {
        let summary = RunSummary {
            id: "run-review".to_owned(),
            scenario_id: "catalog".to_owned(),
            scenario_title: "Catalog".to_owned(),
            model_id: "test/model".to_owned(),
            status: RunStatus::Passed,
            started_at_ms: 10,
            finished_at_ms: Some(30),
            event_count: 11,
            error: None,
        };
        let events = vec![
            event(
                3,
                "driver.ready",
                json!({ "name": "v0-driver", "version": "1.0.0" }),
            ),
            event(4, "driver.session-opened", JsonValue::Null),
            event(7, "v0.turn-start", json!({})),
            event(8, "v0.mdx", json!({ "content": "I will inspect " })),
            event(9, "v0.mdx", json!({ "content": "the catalog." })),
            event(10, "v0.turn-finish", json!({})),
            event(
                11,
                "mcp.tool.started",
                json!({ "source": "catalog", "name": "list", "actor": "agent" }),
            ),
            event(
                12,
                "mcp.tool.completed",
                json!({ "source": "catalog", "name": "list", "actor": "agent", "isError": false }),
            ),
            event(
                13,
                "v0.task-write-file-v1",
                json!({
                    "id": "write-1",
                    "taskNameComplete": "Wrote result file",
                    "filePath": "result.json",
                    "finishedAt": 20
                }),
            ),
            event(
                14,
                "workspace.finalized",
                json!({ "changes": [{ "path": "result.json", "kind": "created" }] }),
            ),
            event(
                15,
                "run.finished",
                json!({
                    "status": "passed",
                    "score": { "activeNames": ["alpha", "gamma"], "totalScore": 11 }
                }),
            ),
            event(
                16,
                "mcp.tool.completed",
                json!({ "source": "catalog", "name": "list", "isError": false }),
            ),
        ];

        let review = build_review(&summary, &events);
        assert_eq!(review.version, 1);
        assert_eq!(review.metrics.model_turns, 1);
        assert_eq!(review.metrics.capability_calls, 1);
        assert_eq!(review.metrics.native_actions, 1);
        assert_eq!(review.metrics.workspace_changes, 1);
        assert_eq!(review.metrics.duration_ms, Some(20));
        assert_eq!(review.steps.len(), 6);
        assert_eq!(review.steps[0].title, "Harness ready");
        assert_eq!(
            review.steps[1].detail.as_deref(),
            Some("I will inspect the catalog.")
        );
        assert_eq!(review.steps[2].title, "catalog · list");
        assert_eq!(review.steps[3].title, "Wrote result file");
        assert_eq!(review.steps[4].title, "Created result.json");
        assert_eq!(review.steps[5].title, "Evaluation passed");
        assert_eq!(
            review.steps[5].detail.as_deref(),
            Some("2 active items · total score 11")
        );
    }

    #[test]
    fn causal_review_excludes_human_capabilities_during_the_driver_turn() {
        let summary = RunSummary {
            id: "run-attribution".to_owned(),
            scenario_id: "catalog".to_owned(),
            scenario_title: "Catalog".to_owned(),
            model_id: "test/model".to_owned(),
            status: RunStatus::Running,
            started_at_ms: 1,
            finished_at_ms: None,
            event_count: 5,
            error: None,
        };
        let events = vec![
            event(1, "driver.session-opened", JsonValue::Null),
            event(
                2,
                "mcp.tool.started",
                json!({ "source": "catalog", "name": "list", "actor": "human" }),
            ),
            event(
                3,
                "mcp.tool.completed",
                json!({ "source": "catalog", "name": "list", "actor": "human", "isError": false }),
            ),
            event(
                4,
                "mcp.tool.started",
                json!({ "source": "catalog", "name": "list", "actor": "agent" }),
            ),
            event(
                5,
                "mcp.tool.completed",
                json!({ "source": "catalog", "name": "list", "actor": "agent", "isError": false }),
            ),
        ];
        let review = build_review(&summary, &events);
        assert_eq!(review.metrics.capability_calls, 1);
        assert_eq!(review.steps.len(), 1);
        assert_eq!(review.steps[0].title, "catalog · list");
    }

    #[test]
    fn causal_review_pairs_overlapping_capability_calls_in_fifo_order() {
        let summary = RunSummary {
            id: "run-overlap".to_owned(),
            scenario_id: "catalog".to_owned(),
            scenario_title: "Catalog".to_owned(),
            model_id: "test/model".to_owned(),
            status: RunStatus::Running,
            started_at_ms: 1,
            finished_at_ms: None,
            event_count: 5,
            error: None,
        };
        let events = vec![
            event(1, "driver.session-opened", JsonValue::Null),
            event(
                2,
                "mcp.tool.started",
                json!({ "source": "catalog", "name": "list", "actor": "agent" }),
            ),
            event(
                3,
                "mcp.tool.started",
                json!({ "source": "catalog", "name": "list", "actor": "agent" }),
            ),
            event(
                4,
                "mcp.tool.completed",
                json!({ "source": "catalog", "name": "list", "actor": "agent", "isError": false }),
            ),
            event(
                5,
                "mcp.tool.completed",
                json!({ "source": "catalog", "name": "list", "actor": "agent", "isError": false }),
            ),
        ];
        let review = build_review(&summary, &events);
        assert_eq!(review.metrics.capability_calls, 2);
        assert_eq!(review.steps[0].event_sequences, vec![2, 4]);
        assert_eq!(review.steps[1].event_sequences, vec![3, 5]);
    }

    #[test]
    fn causal_review_explains_model_provider_failures() {
        let summary = RunSummary {
            id: "run-provider-failure".to_owned(),
            scenario_id: "catalog".to_owned(),
            scenario_title: "Catalog".to_owned(),
            model_id: "test/model".to_owned(),
            status: RunStatus::Failed,
            started_at_ms: 10,
            finished_at_ms: Some(30),
            event_count: 4,
            error: None,
        };
        let events = vec![
            event(1, "v0.turn-start", json!({})),
            event(
                2,
                "v0.orchestrator-error",
                json!({
                    "message": "provider credential is missing",
                    "modelId": "test/model"
                }),
            ),
            event(3, "v0.turn-finish", json!({})),
            event(
                4,
                "run.finished",
                json!({ "status": "failed", "error": "result.json is missing" }),
            ),
        ];

        let review = build_review(&summary, &events);
        assert_eq!(review.steps.len(), 2);
        assert_eq!(review.steps[0].title, "Model turn 1");
        assert_eq!(review.steps[0].status, "failed");
        assert_eq!(
            review.steps[0].detail.as_deref(),
            Some("test/model: provider credential is missing")
        );
        assert_eq!(review.steps[1].title, "Evaluation failed");
        assert_eq!(
            review.steps[1].detail.as_deref(),
            Some("result.json is missing")
        );
    }

    #[test]
    fn causal_review_labels_deleted_workspace_files() {
        let summary = RunSummary {
            id: "run-delete".to_owned(),
            scenario_id: "catalog".to_owned(),
            scenario_title: "Catalog".to_owned(),
            model_id: "test/model".to_owned(),
            status: RunStatus::Passed,
            started_at_ms: 1,
            finished_at_ms: Some(2),
            event_count: 1,
            error: None,
        };
        let review = build_review(
            &summary,
            &[event(
                1,
                "workspace.finalized",
                json!({ "changes": [{ "path": "old.txt", "kind": "deleted" }] }),
            )],
        );
        assert_eq!(review.steps[0].title, "Deleted old.txt");
    }

    #[test]
    fn native_event_redaction_removes_sensitive_fields_and_known_literals() {
        let mut value = json!({
            "transport": {
                "headers": { "Authorization": "Bearer mcp-secret" }
            },
            "note": "driver saw environment-secret and line\n\"quoted\"\\secret",
            "boolean": true,
            "booleanText": "true"
        });
        value.as_object_mut().unwrap().insert(
            "dynamic-environment-secret-key".to_owned(),
            JsonValue::String("safe".to_owned()),
        );
        let redacted = redact_value(
            value,
            &[
                b"mcp-secret".to_vec(),
                b"environment-secret".to_vec(),
                b"line\n\"quoted\"\\secret".to_vec(),
                b"true".to_vec(),
            ],
        );
        let serialized = serde_json::to_string(&redacted).unwrap();
        assert_eq!(
            redacted["transport"]["headers"]["Authorization"],
            "[REDACTED]"
        );
        assert!(!serialized.contains("mcp-secret"));
        assert!(!serialized.contains("environment-secret"));
        assert!(!serialized.contains("quoted"));
        assert_eq!(redacted["boolean"], true);
        assert_eq!(redacted["booleanText"], "[REDACTED]");
        assert!(serialized.contains("[REDACTED]"));
    }

    #[test]
    fn secret_key_redaction_preserves_distinct_json_entries() {
        let redacted = redact_value(
            json!({
                "first-literal": "first",
                "second-literal": "second"
            }),
            &[b"first-literal".to_vec(), b"second-literal".to_vec()],
        );
        let object = redacted.as_object().unwrap();
        assert_eq!(object.len(), 2);
        assert!(object.keys().all(|key| !key.contains("literal")));
        assert!(object.values().any(|value| value == "first"));
        assert!(object.values().any(|value| value == "second"));
    }

    #[test]
    fn transcript_redaction_removes_protocol_and_environment_credentials() {
        let transcript = DriverTranscript {
            controller_records: vec![br#"{"headers":{"Authorization":"Bearer mcp-secret"},"apiKey":"environment-secret"}"#.to_vec()],
            driver_records: vec![br#"{"safe":"catalog"}"#.to_vec()],
            driver_stderr: b"request failed with environment-secret".to_vec(),
        };
        let redacted = redact_transcript(
            transcript,
            &[b"mcp-secret".to_vec(), b"environment-secret".to_vec()],
        );
        let serialized = redacted
            .controller_records
            .iter()
            .chain(&redacted.driver_records)
            .chain(std::iter::once(&redacted.driver_stderr))
            .flatten()
            .copied()
            .collect::<Vec<_>>();
        assert!(
            !serialized
                .windows(b"mcp-secret".len())
                .any(|value| value == b"mcp-secret")
        );
        assert!(
            !serialized
                .windows(b"environment-secret".len())
                .any(|value| value == b"environment-secret")
        );
        assert!(
            serialized
                .windows(b"[REDACTED]".len())
                .any(|value| value == b"[REDACTED]")
        );
    }
}
