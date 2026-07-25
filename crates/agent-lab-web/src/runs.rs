#[cfg(unix)]
use std::os::unix::ffi::OsStringExt;
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    ffi::{OsStr, OsString},
    fs,
    io::{self, Read, Write},
    net::{Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{Arc, Condvar, Mutex, MutexGuard, Weak, mpsc},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use agent_lab_catalog_source::{AnalysisSource, CatalogSource, SourceObserver};
use agent_lab_driver_protocol::{
    CommandBody, ControllerCommand, DriverBody, DriverDescriptor, DriverFailureScope, DriverLaunch,
    DriverProcess, DriverTranscript, MAX_DRIVER_RECORD_BYTES, PROTOCOL_VERSION, ProcessError,
    ProgressObservation, ProgressPhase, RawDriverMessage, TURN_OBSERVATIONS_FEATURE,
    TurnObservation,
};
use axum::{
    Router,
    extract::{Request, State},
    http::{HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::Response,
};
#[cfg(windows)]
use process_wrap::std::JobObject;
#[cfg(unix)]
use process_wrap::std::ProcessGroup;
use process_wrap::std::{ChildWrapper, CommandWrap};
use rand::Rng;
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use sha2::{Digest, Sha256};
use tokio::{net::TcpListener, sync::broadcast};
use tokio_util::sync::CancellationToken;

const DRIVER_POLL: Duration = Duration::from_millis(250);
// The extracted v0 adapter loads the production agent module graph before it
// can announce readiness. A cold TypeScript process can take over a minute on
// a development checkout, while subsequent protocol replies remain fast.
const DRIVER_READY_TIMEOUT: Duration = Duration::from_mins(2);
const DRIVER_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);
const MODEL_ACCESS_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_EVIDENCE_FILE_BYTES: u64 = 8 * 1024 * 1024;
const MAX_EVIDENCE_TOTAL_BYTES: u64 = 64 * 1024 * 1024;
const MAX_AGENT_TURN_INPUT_BYTES: usize = 1024 * 1024;
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
    #[serde(default)]
    pub model_id: Option<String>,
    #[serde(default)]
    pub harness_id: Option<String>,
    #[serde(default)]
    pub model_profile_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrepareRunRequest {
    pub scenario_id: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartPreparedRunRequest {
    #[serde(default)]
    pub model_id: Option<String>,
    #[serde(default)]
    pub harness_id: Option<String>,
    #[serde(default)]
    pub model_profile_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct HarnessProfile {
    pub id: String,
    pub display_name: String,
    pub launch: DriverLaunch,
    pub models: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessMetadata {
    pub id: String,
    pub display_name: String,
    pub model_profile_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelProfileMetadata {
    pub id: String,
    pub display_name: String,
    pub harness_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ModelAccessProvider {
    pub id: String,
    pub display_name: String,
    pub resolver: Option<DriverLaunch>,
    pub environment_names: Vec<String>,
    pub setup_hint: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ModelAccessStatus {
    Ready,
    NeedsSetup,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelAccessSnapshot {
    pub id: String,
    pub display_name: String,
    pub harness_ids: Vec<String>,
    pub status: ModelAccessStatus,
    pub source: Option<String>,
    pub expires_at_ms: Option<u128>,
    pub message: Option<String>,
    pub setup_hint: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelAccessResolution {
    status: ModelAccessStatus,
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    expires_at_ms: Option<u128>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    environment: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartEvaluationRequest {
    pub scenario_id: String,
    pub model_profile_id: String,
    pub source_workspace_id: String,
    pub harness_ids: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WorkbenchSelection {
    #[serde(default)]
    pub harness_id: Option<String>,
    #[serde(default)]
    pub model_profile_id: Option<String>,
    #[serde(default)]
    pub comparison_harness_ids: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateWorkbenchSelectionRequest {
    #[serde(default)]
    pub harness_id: Option<String>,
    #[serde(default)]
    pub model_profile_id: Option<String>,
    #[serde(default)]
    pub comparison_harness_ids: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompareWorkbenchRequest {
    #[serde(default)]
    pub model_profile_id: Option<String>,
    #[serde(default)]
    pub harness_ids: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartAgentSessionRequest {
    #[serde(default)]
    pub harness_id: Option<String>,
    #[serde(default)]
    pub model_profile_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartAgentTurnRequest {
    pub prompt: String,
    #[serde(default)]
    pub input: Option<JsonValue>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentSessionStatus {
    Starting,
    Ready,
    Running,
    Closing,
    Failed,
    Closed,
    Interrupted,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentTurnStatus {
    Queued,
    Running,
    Completed,
    Intervened,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AgentSessionSummary {
    pub id: String,
    pub workspace_id: String,
    pub harness_id: String,
    pub model_profile_id: String,
    pub model_id: String,
    pub status: AgentSessionStatus,
    pub active: bool,
    pub created_at_ms: u128,
    pub updated_at_ms: u128,
    pub turn_count: u64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentTurnSummary {
    pub id: String,
    pub session_id: String,
    pub prompt: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<JsonValue>,
    #[serde(default)]
    pub source_revision: String,
    #[serde(default)]
    pub capability_revisions: BTreeMap<String, String>,
    pub status: AgentTurnStatus,
    pub started_at_ms: u128,
    pub finished_at_ms: Option<u128>,
    pub outcome: Option<String>,
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub human_intervention_at_ms: Option<u128>,
}

const AGENT_TURN_PRESENTATION_VERSION: u32 = 2;
const AGENT_SESSION_MANIFEST_LEGACY_VERSION: u32 = 1;
const AGENT_SESSION_MANIFEST_VERSION: u32 = 2;
const AGENT_SESSION_PRESENTATION_REQUIRED_VERSION: u32 = 2;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentPresentationCompleteness {
    Complete,
    Partial,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AgentPresentationCompletenessSummary {
    pub assistant_output: AgentPresentationCompleteness,
    pub capability_activity: AgentPresentationCompleteness,
    pub native_activity: AgentPresentationCompleteness,
    pub workspace_effects: AgentPresentationCompleteness,
    pub usage: AgentPresentationCompleteness,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AgentAssistantMessage {
    pub id: String,
    pub text: String,
    pub complete: bool,
    pub source_event_sequences: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentTurnActivity {
    pub kind: String,
    pub title: String,
    pub detail: Option<String>,
    pub status: String,
    pub source: Option<String>,
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub call_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arguments: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub change_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_mode: Option<String>,
    pub source_event_sequences: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentTurnPresentation {
    pub schema_version: u32,
    pub response: Option<String>,
    pub messages: Vec<AgentAssistantMessage>,
    pub activity: Vec<AgentTurnActivity>,
    pub usage: Option<JsonValue>,
    pub completeness: AgentPresentationCompletenessSummary,
    pub source_event_sequences: Vec<u64>,
    pub source_digest: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentTurnDetail {
    #[serde(flatten)]
    pub summary: AgentTurnSummary,
    pub presentation: AgentTurnPresentation,
}

impl std::ops::Deref for AgentTurnDetail {
    type Target = AgentTurnSummary;

    fn deref(&self) -> &Self::Target {
        &self.summary
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentSessionDetail {
    pub projection_version: u32,
    pub summary: AgentSessionSummary,
    pub turns: Vec<AgentTurnDetail>,
    pub events: Vec<RunEvent>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum WorkbenchOrigin {
    Browser,
    Nushell,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum EvaluationStatus {
    Queued,
    Running,
    Passed,
    Failed,
    Cancelled,
}

impl EvaluationStatus {
    #[must_use]
    pub fn is_finished(self) -> bool {
        matches!(self, Self::Passed | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EvaluationArmSummary {
    pub harness_id: String,
    pub run_id: Option<String>,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EvaluationSummary {
    pub id: String,
    pub scenario_id: String,
    pub model_profile_id: String,
    pub source_workspace_id: String,
    pub source_revision: String,
    pub harness_ids: Vec<String>,
    pub arms: Vec<EvaluationArmSummary>,
    pub status: EvaluationStatus,
    pub started_at_ms: u128,
    pub finished_at_ms: Option<u128>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EvaluationDetail {
    pub summary: EvaluationSummary,
    pub events: Vec<RunEvent>,
    pub comparison: Option<JsonValue>,
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
    #[serde(default)]
    pub harness_id: Option<String>,
    #[serde(default)]
    pub model_profile_id: Option<String>,
    pub status: RunStatus,
    pub started_at_ms: u128,
    pub finished_at_ms: Option<u128>,
    pub event_count: u64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AgentProgressProjection {
    pub phase: ProgressPhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    pub source_event_sequence: u64,
    pub source_event_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunEvent {
    pub sequence: u64,
    pub at_ms: u128,
    #[serde(rename = "type")]
    pub kind: String,
    pub payload: JsonValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<AgentProgressProjection>,
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

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkbenchSnapshot {
    pub workspace_id: String,
    pub assembly: AssemblySnapshot,
    pub selection: WorkbenchSelection,
    pub harnesses: Vec<HarnessMetadata>,
    pub model_profiles: Vec<ModelProfileMetadata>,
    pub model_access: Vec<ModelAccessSnapshot>,
    pub latest_evaluation: Option<EvaluationSummary>,
    pub active_agent_session: Option<AgentSessionSummary>,
    pub replay_agent_session: Option<AgentSessionSummary>,
    pub agent_sessions: Vec<AgentSessionSummary>,
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
    harnesses: BTreeMap<String, HarnessProfile>,
    model_profiles: BTreeMap<String, String>,
    model_access_providers: BTreeMap<String, ModelAccessProvider>,
    harness_model_access: BTreeMap<String, String>,
    runs: Mutex<HashMap<String, Arc<RunState>>>,
    prepare_lock: tokio::sync::Mutex<()>,
    evaluations_dir: PathBuf,
    evaluations: Mutex<HashMap<String, Arc<EvaluationState>>>,
    workbench_grants: Mutex<HashMap<String, String>>,
    agent_sessions: Mutex<HashMap<String, Arc<AgentSessionState>>>,
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
        let evaluations = self
            .evaluations
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for state in evaluations.values() {
            state.cancel.cancel();
        }
        let sessions = self
            .agent_sessions
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for state in sessions.values() {
            state.lifecycle_cancel.cancel();
            if let Some(cancel) = lock(&state.turn_cancel).take() {
                cancel.cancel();
            }
            if let Some(commands) = lock(&state.commands).take() {
                let _ = commands.send(AgentSessionCommand::Shutdown);
            }
        }
    }
}

struct RunState {
    summary: Mutex<RunSummary>,
    assembly: Mutex<AssemblySnapshot>,
    selection: Mutex<WorkbenchSelection>,
    events: Mutex<Vec<RunEvent>>,
    sender: broadcast::Sender<RunEvent>,
    cancel: CancellationToken,
    bundle_dir: PathBuf,
    agent_session_directories: AgentSessionDirectoryAnchor,
    workspace: PathBuf,
    output: PathBuf,
    initial_snapshot: Option<BTreeMap<String, Vec<u8>>>,
    capabilities: Mutex<Vec<CapabilityEndpoint>>,
    secret_values: Mutex<Vec<Vec<u8>>>,
    agent_sessions: Mutex<HashMap<String, Weak<AgentSessionState>>>,
    active_agent_session_id: Mutex<Option<String>>,
    active_agent_turn: Mutex<Option<AgentTurnAttribution>>,
    capability_attributions: Mutex<HashMap<String, AgentTurnAttribution>>,
    reusable_explore: bool,
    replay_failed: bool,
}

struct RunCompletion {
    status: RunStatus,
    error: Option<String>,
    score: JsonValue,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AgentTurnTermination {
    Cancelled,
    TimedOut,
}

enum ExitWait {
    Exited(Option<i32>),
    Cancelled,
}

struct EvaluationState {
    summary: Mutex<EvaluationSummary>,
    events: Mutex<Vec<RunEvent>>,
    sender: broadcast::Sender<RunEvent>,
    cancel: CancellationToken,
    bundle_dir: PathBuf,
    snapshot: PathBuf,
    replay_failed: bool,
}

struct AgentSessionEvidenceRoot {
    display_path: PathBuf,
    #[cfg(unix)]
    directory: rustix::fd::OwnedFd,
}

struct AgentSessionDirectoryAnchor {
    display_path: PathBuf,
    #[cfg(unix)]
    run_directory: rustix::fd::OwnedFd,
    #[cfg(unix)]
    session_collection: Mutex<Option<rustix::fd::OwnedFd>>,
}

impl AgentSessionEvidenceRoot {
    #[cfg(all(test, unix))]
    fn open(display_path: PathBuf) -> Result<Self, RunError> {
        let directory = open_confined_evidence_root(&display_path)?;
        Ok(Self::from_opened(display_path, directory))
    }

    #[cfg(all(test, not(unix)))]
    fn open(display_path: PathBuf) -> Result<Self, RunError> {
        Err(RunError::ConfinedReadUnavailable(display_path))
    }

    fn display_path(&self) -> &Path {
        &self.display_path
    }

    #[cfg(unix)]
    fn from_opened(display_path: PathBuf, directory: rustix::fd::OwnedFd) -> Self {
        Self {
            display_path,
            directory,
        }
    }
}

impl AgentSessionDirectoryAnchor {
    #[cfg(unix)]
    fn open(display_path: PathBuf) -> Result<Self, RunError> {
        let run_directory = open_confined_evidence_root(&display_path)?;
        Ok(Self {
            display_path,
            run_directory,
            session_collection: Mutex::new(None),
        })
    }

    #[cfg(not(unix))]
    fn open(display_path: PathBuf) -> Result<Self, RunError> {
        Ok(Self { display_path })
    }

    fn collection_display_path(&self) -> PathBuf {
        self.display_path.join("agent-sessions")
    }

    #[cfg(unix)]
    fn session_collection(&self, create: bool) -> Result<Option<rustix::fd::OwnedFd>, RunError> {
        let mut pinned = lock(&self.session_collection);
        if pinned.is_none() {
            let display_path = self.collection_display_path();
            let flags = rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::CLOEXEC
                | rustix::fs::OFlags::DIRECTORY
                | rustix::fs::OFlags::NOFOLLOW;
            let mut opened = rustix::fs::openat(
                &self.run_directory,
                "agent-sessions",
                flags,
                rustix::fs::Mode::empty(),
            );
            if create
                && opened
                    .as_ref()
                    .is_err_and(|error| *error == rustix::io::Errno::NOENT)
            {
                match rustix::fs::mkdirat(
                    &self.run_directory,
                    "agent-sessions",
                    rustix::fs::Mode::RWXU | rustix::fs::Mode::RWXG | rustix::fs::Mode::RWXO,
                ) {
                    Ok(()) | Err(rustix::io::Errno::EXIST) => {}
                    Err(error) => return Err(io::Error::from(error).into()),
                }
                opened = rustix::fs::openat(
                    &self.run_directory,
                    "agent-sessions",
                    flags,
                    rustix::fs::Mode::empty(),
                );
            }
            let opened = match opened {
                Ok(opened) => opened,
                Err(rustix::io::Errno::NOENT) => return Ok(None),
                Err(rustix::io::Errno::LOOP | rustix::io::Errno::NOTDIR) => {
                    return Err(RunError::PathEscape(display_path));
                }
                Err(error) => return Err(io::Error::from(error).into()),
            };
            *pinned = Some(opened);
        }
        let duplicate = rustix::io::fcntl_dupfd_cloexec(
            pinned
                .as_ref()
                .expect("opened agent session collection should be pinned"),
            0,
        )
        .map_err(io::Error::from)?;
        Ok(Some(duplicate))
    }

    #[cfg(unix)]
    fn session_names(&self) -> Result<Vec<OsString>, RunError> {
        let Some(directory) = self.session_collection(false)? else {
            return Ok(Vec::new());
        };
        let entries = rustix::fs::Dir::read_from(&directory).map_err(io::Error::from)?;
        let mut names = Vec::new();
        for entry in entries {
            let entry = entry.map_err(io::Error::from)?;
            let bytes = entry.file_name().to_bytes();
            if bytes == b"." || bytes == b".." {
                continue;
            }
            names.push(OsString::from_vec(bytes.to_vec()));
        }
        names.sort();
        Ok(names)
    }

    #[cfg(unix)]
    fn open_session(&self, name: &OsStr) -> Result<AgentSessionEvidenceRoot, RunError> {
        let display_path = self.collection_display_path().join(name);
        let components = confined_evidence_components(Path::new(name), &display_path)?;
        if components.len() != 1 {
            return Err(RunError::PathEscape(display_path));
        }
        let collection = self.session_collection(false)?.ok_or_else(|| {
            RunError::InvalidRequest("agent session collection does not exist".to_owned())
        })?;
        let directory = rustix::fs::openat(
            &collection,
            name,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::CLOEXEC
                | rustix::fs::OFlags::DIRECTORY
                | rustix::fs::OFlags::NOFOLLOW,
            rustix::fs::Mode::empty(),
        )
        .map_err(|error| match error {
            rustix::io::Errno::LOOP | rustix::io::Errno::NOTDIR => {
                RunError::PathEscape(display_path.clone())
            }
            _ => io::Error::from(error).into(),
        })?;
        Ok(AgentSessionEvidenceRoot::from_opened(
            display_path,
            directory,
        ))
    }

    #[cfg(unix)]
    fn create_session(&self, id: &str) -> Result<AgentSessionEvidenceRoot, RunError> {
        validate_portable_evidence_id("agent session", id)?;
        let display_path = self.collection_display_path().join(id);
        let collection = self
            .session_collection(true)?
            .expect("created agent session collection should be available");
        let mode = rustix::fs::Mode::RWXU | rustix::fs::Mode::RWXG | rustix::fs::Mode::RWXO;
        rustix::fs::mkdirat(&collection, id, mode).map_err(io::Error::from)?;
        let directory = match rustix::fs::openat(
            &collection,
            id,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::CLOEXEC
                | rustix::fs::OFlags::DIRECTORY
                | rustix::fs::OFlags::NOFOLLOW,
            rustix::fs::Mode::empty(),
        ) {
            Ok(directory) => directory,
            Err(error) => {
                let _ = rustix::fs::unlinkat(&collection, id, rustix::fs::AtFlags::REMOVEDIR);
                return Err(io::Error::from(error).into());
            }
        };
        if let Err(error) = rustix::fs::mkdirat(&directory, "turns", mode) {
            drop(directory);
            let _ = rustix::fs::unlinkat(&collection, id, rustix::fs::AtFlags::REMOVEDIR);
            return Err(io::Error::from(error).into());
        }
        Ok(AgentSessionEvidenceRoot::from_opened(
            display_path,
            directory,
        ))
    }

    #[cfg(not(unix))]
    fn session_names(&self) -> Result<Vec<OsString>, RunError> {
        Err(RunError::ConfinedReadUnavailable(
            self.collection_display_path(),
        ))
    }

    #[cfg(not(unix))]
    fn open_session(&self, name: &OsStr) -> Result<AgentSessionEvidenceRoot, RunError> {
        Err(RunError::ConfinedReadUnavailable(
            self.collection_display_path().join(name),
        ))
    }

    #[cfg(not(unix))]
    fn create_session(&self, id: &str) -> Result<AgentSessionEvidenceRoot, RunError> {
        Err(RunError::ConfinedReadUnavailable(
            self.collection_display_path().join(id),
        ))
    }
}

struct AgentSessionState {
    summary: Mutex<AgentSessionSummary>,
    turns: Mutex<Vec<AgentTurnSummary>>,
    events: Mutex<Vec<RunEvent>>,
    sender: broadcast::Sender<RunEvent>,
    commands: Mutex<Option<mpsc::Sender<AgentSessionCommand>>>,
    lifecycle_cancel: CancellationToken,
    turn_cancel: Mutex<Option<CancellationToken>>,
    actor: Mutex<AgentActorRegistration>,
    actor_registered: Condvar,
    evidence_error: Mutex<Option<String>>,
    evidence_root: AgentSessionEvidenceRoot,
    secret_values: Mutex<Vec<Vec<u8>>>,
}

#[derive(Default)]
struct AgentActorRegistration {
    complete: bool,
    handle: Option<thread::JoinHandle<()>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AgentTurnAttribution {
    session_id: String,
    turn_id: String,
}

struct ActiveAgentTurnGuard<'a> {
    workspace: &'a RunState,
    attribution: AgentTurnAttribution,
}

impl<'a> ActiveAgentTurnGuard<'a> {
    fn new(workspace: &'a RunState, session_id: &str, turn_id: &str) -> Result<Self, RunError> {
        let attribution = AgentTurnAttribution {
            session_id: session_id.to_owned(),
            turn_id: turn_id.to_owned(),
        };
        if lock(&workspace.active_agent_turn).as_ref() != Some(&attribution) {
            return Err(RunError::Protocol(format!(
                "agent turn {turn_id} did not own the workspace turn reservation"
            )));
        }
        Ok(Self {
            workspace,
            attribution,
        })
    }

    fn finish<T>(
        &mut self,
        persist_terminal_state: impl FnOnce() -> Result<T, RunError>,
    ) -> Result<T, RunError> {
        let mut active = lock(&self.workspace.active_agent_turn);
        if active.as_ref() != Some(&self.attribution) {
            return Err(RunError::Protocol(format!(
                "agent turn {} lost the workspace turn reservation",
                self.attribution.turn_id
            )));
        }
        let result = persist_terminal_state()?;
        *active = None;
        Ok(result)
    }
}

impl Drop for ActiveAgentTurnGuard<'_> {
    fn drop(&mut self) {
        let mut active = lock(&self.workspace.active_agent_turn);
        if active.as_ref() == Some(&self.attribution) {
            *active = None;
        }
    }
}

fn release_agent_turn_reservation(workspace: &RunState, attribution: &AgentTurnAttribution) {
    let mut active = lock(&workspace.active_agent_turn);
    if active.as_ref() == Some(attribution) {
        *active = None;
    }
}

fn register_agent_actor(state: &AgentSessionState, handle: Option<thread::JoinHandle<()>>) {
    let mut actor = lock(&state.actor);
    actor.handle = handle;
    actor.complete = true;
    state.actor_registered.notify_all();
}

fn join_agent_actor(state: &AgentSessionState) -> Result<(), RunError> {
    let handle = {
        let actor = lock(&state.actor);
        let mut actor = state
            .actor_registered
            .wait_while(actor, |actor| !actor.complete)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        actor.handle.take()
    };
    if let Some(handle) = handle {
        handle.join().map_err(|_| {
            RunError::Protocol("agent session actor panicked during workspace shutdown".to_owned())
        })?;
    }
    Ok(())
}

fn rollback_agent_turn_start(
    state: &AgentSessionState,
    workspace: &RunState,
    attribution: &AgentTurnAttribution,
    turn_relative: &Path,
) {
    lock(&state.turns).retain(|turn| turn.id != attribution.turn_id);
    *lock(&state.turn_cancel) = None;
    let _ = persist_agent_session(state);
    let _ = remove_confined_evidence_entry(&state.evidence_root, turn_relative);
    release_agent_turn_reservation(workspace, attribution);
}

fn agent_turn_was_intervened(state: &AgentSessionState, turn_id: &str) -> bool {
    lock(&state.turns)
        .iter()
        .find(|turn| turn.id == turn_id)
        .is_some_and(|turn| turn.human_intervention_at_ms.is_some())
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AgentSessionManifest {
    #[serde(default = "legacy_agent_session_manifest_version")]
    version: u32,
    summary: AgentSessionSummary,
    turns: Vec<AgentTurnSummary>,
}

const fn legacy_agent_session_manifest_version() -> u32 {
    AGENT_SESSION_MANIFEST_LEGACY_VERSION
}

enum AgentSessionCommand {
    StartTurn {
        turn_id: String,
        prompt: String,
        input: Option<JsonValue>,
        capabilities: Vec<CapabilityEndpoint>,
        cancel: CancellationToken,
    },
    Close,
    Shutdown,
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
    pub control_token: String,
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
        Self::new_with_harnesses(config, Vec::new(), BTreeMap::new())
    }

    /// Load a server-only harness registry alongside the run store.
    ///
    /// # Errors
    ///
    /// Returns an error for duplicate identifiers, missing model mappings, or invalid paths.
    pub fn new_with_harnesses(
        config: RunControllerConfig,
        harnesses: Vec<HarnessProfile>,
        model_profiles: BTreeMap<String, String>,
    ) -> Result<Self, RunError> {
        Self::new_with_harnesses_and_model_access(
            config,
            harnesses,
            model_profiles,
            Vec::new(),
            BTreeMap::new(),
        )
    }

    /// Load the harness registry with server-only model-access providers.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid provider mappings or registry entries.
    pub fn new_with_harnesses_and_model_access(
        config: RunControllerConfig,
        harnesses: Vec<HarnessProfile>,
        model_profiles: BTreeMap<String, String>,
        model_access_providers: Vec<ModelAccessProvider>,
        harness_model_access: BTreeMap<String, String>,
    ) -> Result<Self, RunError> {
        require_race_free_confined_reads(cfg!(unix))?;
        let scenarios_dir = canonical_directory(&config.scenarios_dir)?;
        fs::create_dir_all(&config.data_dir)?;
        let data_dir = fs::canonicalize(&config.data_dir)?;
        let evaluations_dir = data_dir.join("evaluations");
        fs::create_dir_all(&evaluations_dir)?;
        let evaluations_dir = fs::canonicalize(evaluations_dir)?;
        let scenarios = load_scenarios(&scenarios_dir)?;
        if scenarios.is_empty() {
            return Err(RunError::InvalidScenario(
                "scenario directory contains no TOML manifests".to_owned(),
            ));
        }
        let mut harness_registry = BTreeMap::new();
        for harness in harnesses {
            if harness.id.trim().is_empty() || harness_registry.contains_key(&harness.id) {
                return Err(RunError::InvalidRequest(format!(
                    "duplicate or empty harness id: {}",
                    harness.id
                )));
            }
            for profile_id in harness.models.keys() {
                if !model_profiles.contains_key(profile_id) {
                    return Err(RunError::InvalidRequest(format!(
                        "harness {} maps unknown model profile {profile_id}",
                        harness.id
                    )));
                }
            }
            harness_registry.insert(harness.id.clone(), harness);
        }
        let mut model_access_registry = BTreeMap::new();
        for provider in model_access_providers {
            if provider.id.trim().is_empty() || model_access_registry.contains_key(&provider.id) {
                return Err(RunError::InvalidRequest(format!(
                    "duplicate or empty model-access provider id: {}",
                    provider.id
                )));
            }
            model_access_registry.insert(provider.id.clone(), provider);
        }
        for (harness_id, provider_id) in &harness_model_access {
            if !harness_registry.contains_key(harness_id) {
                return Err(RunError::InvalidRequest(format!(
                    "model-access mapping references unknown harness: {harness_id}"
                )));
            }
            if !model_access_registry.contains_key(provider_id) {
                return Err(RunError::InvalidRequest(format!(
                    "harness {harness_id} references unknown model-access provider: {provider_id}"
                )));
            }
        }
        let runs = load_runs(&data_dir, &scenarios, &harness_registry, &model_profiles)?;
        let evaluations = load_evaluations(&evaluations_dir)?;
        let agent_sessions = load_agent_sessions(&runs);
        for session in agent_sessions.values() {
            let workspace_id = lock(&session.summary).workspace_id.clone();
            if let Some(workspace) = runs.get(&workspace_id) {
                lock(&workspace.agent_sessions)
                    .insert(lock(&session.summary).id.clone(), Arc::downgrade(session));
            }
        }
        for workspace in runs.values() {
            persist_active_agent_session(workspace, None)?;
        }
        Ok(Self {
            inner: Arc::new(ControllerInner {
                scenarios,
                scenarios_dir,
                data_dir,
                driver: config.driver,
                harnesses: harness_registry,
                model_profiles,
                model_access_providers: model_access_registry,
                harness_model_access,
                runs: Mutex::new(runs),
                prepare_lock: tokio::sync::Mutex::new(()),
                evaluations_dir,
                evaluations: Mutex::new(evaluations),
                workbench_grants: Mutex::new(HashMap::new()),
                agent_sessions: Mutex::new(agent_sessions),
            }),
        })
    }

    #[must_use]
    pub fn harnesses(&self) -> Vec<HarnessMetadata> {
        self.inner
            .harnesses
            .values()
            .map(|harness| HarnessMetadata {
                id: harness.id.clone(),
                display_name: harness.display_name.clone(),
                model_profile_ids: harness.models.keys().cloned().collect(),
            })
            .collect()
    }

    #[must_use]
    pub fn model_profiles(&self) -> Vec<ModelProfileMetadata> {
        self.inner
            .model_profiles
            .iter()
            .map(|(id, display_name)| ModelProfileMetadata {
                id: id.clone(),
                display_name: display_name.clone(),
                harness_ids: self
                    .inner
                    .harnesses
                    .values()
                    .filter(|harness| harness.models.contains_key(id))
                    .map(|harness| harness.id.clone())
                    .collect(),
            })
            .collect()
    }

    fn model_access(&self, selection: &WorkbenchSelection) -> Vec<ModelAccessSnapshot> {
        let mut harness_ids = selection.comparison_harness_ids.clone();
        if let Some(harness_id) = &selection.harness_id
            && !harness_ids.contains(harness_id)
        {
            harness_ids.push(harness_id.clone());
        }
        let mut grouped = BTreeMap::<String, Vec<String>>::new();
        for harness_id in harness_ids {
            if let Some(provider_id) = self.inner.harness_model_access.get(&harness_id) {
                grouped
                    .entry(provider_id.clone())
                    .or_default()
                    .push(harness_id);
            }
        }
        grouped
            .into_iter()
            .filter_map(|(provider_id, harness_ids)| {
                let provider = self.inner.model_access_providers.get(&provider_id)?;
                let resolution = resolve_model_access(provider, false).unwrap_or_else(|error| {
                    ModelAccessResolution {
                        status: ModelAccessStatus::NeedsSetup,
                        source: None,
                        expires_at_ms: None,
                        message: Some(error.to_string()),
                        environment: BTreeMap::new(),
                    }
                });
                Some(ModelAccessSnapshot {
                    id: provider.id.clone(),
                    display_name: provider.display_name.clone(),
                    harness_ids,
                    status: resolution.status,
                    source: resolution.source,
                    expires_at_ms: resolution.expires_at_ms,
                    message: resolution.message,
                    setup_hint: provider.setup_hint.clone(),
                })
            })
            .collect()
    }

    /// Return the controller-owned state projected into one attached workbench.
    ///
    /// # Errors
    ///
    /// Returns an error when the workspace does not exist or is no longer an
    /// active Explore workspace.
    pub fn workbench(&self, id: &str) -> Result<WorkbenchSnapshot, RunError> {
        let state = self.state(id)?;
        let summary = lock(&state.summary).clone();
        if summary.status != RunStatus::Exploring {
            return Err(RunError::RunUnavailable(id.to_owned()));
        }
        let latest_evaluation = self
            .list_evaluations()
            .into_iter()
            .find(|evaluation| evaluation.source_workspace_id == id);
        let selection = lock(&state.selection).clone();
        let model_access = self.model_access(&selection);
        let agent_sessions = self.list_agent_sessions(id);
        let active_agent_session = agent_sessions
            .iter()
            .find(|session| {
                session.active
                    && matches!(
                        session.status,
                        AgentSessionStatus::Starting
                            | AgentSessionStatus::Ready
                            | AgentSessionStatus::Running
                            | AgentSessionStatus::Closing
                    )
            })
            .cloned();
        let replay_agent_session = active_agent_session
            .is_none()
            .then(|| {
                let replay_session_id = lock(&state.events)
                    .iter()
                    .rev()
                    .find(|event| event.kind == "workbench.agent.session.activated")
                    .and_then(|event| event.payload.get("sessionId"))
                    .and_then(JsonValue::as_str)
                    .map(str::to_owned)?;
                agent_sessions
                    .iter()
                    .find(|session| {
                        session.id == replay_session_id
                            && session.status == AgentSessionStatus::Interrupted
                    })
                    .cloned()
            })
            .flatten();
        Ok(WorkbenchSnapshot {
            workspace_id: id.to_owned(),
            assembly: lock(&state.assembly).clone(),
            selection,
            harnesses: self.harnesses(),
            model_profiles: self.model_profiles(),
            model_access,
            latest_evaluation,
            active_agent_session,
            replay_agent_session,
            agent_sessions,
        })
    }

    /// Update the shared workbench selection and record its human origin.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested harness or model is unavailable, or
    /// when the workspace cannot persist the new selection.
    pub fn update_workbench_selection(
        &self,
        id: &str,
        request: UpdateWorkbenchSelectionRequest,
        origin: WorkbenchOrigin,
    ) -> Result<WorkbenchSelection, RunError> {
        let state = self.state(id)?;
        if lock(&state.summary).status != RunStatus::Exploring {
            return Err(RunError::RunUnavailable(id.to_owned()));
        }
        let mut selection = lock(&state.selection).clone();
        if let Some(harness_id) = request.harness_id {
            validate_harness(&self.inner.harnesses, &harness_id)?;
            selection.harness_id = Some(harness_id);
        }
        if let Some(harness_ids) = request.comparison_harness_ids {
            validate_comparison_harnesses(&self.inner.harnesses, &harness_ids)?;
            selection.comparison_harness_ids = harness_ids;
        }
        if let Some(model_profile_id) = request.model_profile_id {
            validate_selection_model(&self.inner.harnesses, &model_profile_id, &selection)?;
            selection.model_profile_id = Some(model_profile_id);
        } else if selection
            .model_profile_id
            .as_deref()
            .is_some_and(|profile| {
                validate_selection_model(&self.inner.harnesses, profile, &selection).is_err()
            })
        {
            selection.model_profile_id = first_compatible_model(
                &self.inner.harnesses,
                &self.inner.model_profiles,
                &selection,
            );
        }
        *lock(&state.selection) = selection.clone();
        persist_selection(&state)?;
        record_event(
            &state,
            "workbench.selection.changed",
            json!({ "origin": origin, "selection": selection }),
        )?;
        Ok(selection)
    }

    /// Start a paired evaluation using shared defaults plus invocation-local overrides.
    ///
    /// # Errors
    ///
    /// Returns an error when the selection is invalid, model access is not
    /// ready, or the immutable evaluation snapshot cannot be created.
    pub fn compare_workbench(
        &self,
        id: &str,
        request: CompareWorkbenchRequest,
        origin: WorkbenchOrigin,
    ) -> Result<EvaluationSummary, RunError> {
        let state = self.state(id)?;
        let source = lock(&state.summary).clone();
        if source.status != RunStatus::Exploring {
            return Err(RunError::RunUnavailable(id.to_owned()));
        }
        let selection = lock(&state.selection).clone();
        let harness_ids = request
            .harness_ids
            .unwrap_or_else(|| selection.comparison_harness_ids.clone());
        validate_comparison_harnesses(&self.inner.harnesses, &harness_ids)?;
        let model_profile_id = request
            .model_profile_id
            .or(selection.model_profile_id)
            .ok_or_else(|| RunError::InvalidRequest("choose a model profile first".to_owned()))?;
        for harness_id in &harness_ids {
            let harness = self.inner.harnesses.get(harness_id).ok_or_else(|| {
                RunError::InvalidRequest(format!("unknown harness: {harness_id}"))
            })?;
            if !harness.models.contains_key(&model_profile_id) {
                return Err(RunError::InvalidRequest(format!(
                    "model profile {model_profile_id} is unavailable for harness {harness_id}"
                )));
            }
        }
        let evaluation = self.start_evaluation(StartEvaluationRequest {
            scenario_id: source.scenario_id,
            model_profile_id,
            source_workspace_id: id.to_owned(),
            harness_ids,
        })?;
        record_event(
            &state,
            "workbench.evaluation.started",
            json!({
                "origin": origin,
                "evaluationId": evaluation.id,
                "modelProfileId": evaluation.model_profile_id,
                "harnessIds": evaluation.harness_ids,
            }),
        )?;
        Ok(evaluation)
    }

    /// Read an evaluation only when it belongs to the attached workbench.
    ///
    /// # Errors
    ///
    /// Returns an error when the workbench or evaluation does not exist, or
    /// when the evaluation belongs to another workspace.
    pub fn workbench_evaluation(
        &self,
        workspace_id: &str,
        evaluation_id: Option<&str>,
    ) -> Result<EvaluationDetail, RunError> {
        self.state(workspace_id)?;
        let evaluation_id = match evaluation_id {
            Some(id) => id.to_owned(),
            None => self
                .list_evaluations()
                .into_iter()
                .find(|evaluation| evaluation.source_workspace_id == workspace_id)
                .map(|evaluation| evaluation.id)
                .ok_or_else(|| {
                    RunError::InvalidRequest("this workbench has no evaluations yet".to_owned())
                })?,
        };
        let detail = self.get_evaluation(&evaluation_id)?;
        if detail.summary.source_workspace_id != workspace_id {
            return Err(RunError::InvalidRequest(format!(
                "evaluation {evaluation_id} does not belong to workbench {workspace_id}"
            )));
        }
        Ok(detail)
    }

    #[must_use]
    pub fn list_agent_sessions(&self, workspace_id: &str) -> Vec<AgentSessionSummary> {
        let active_id = self
            .state(workspace_id)
            .ok()
            .and_then(|workspace| lock(&workspace.active_agent_session_id).clone());
        let mut sessions = lock(&self.inner.agent_sessions)
            .values()
            .map(|state| lock(&state.summary).clone())
            .filter(|summary| summary.workspace_id == workspace_id)
            .map(|mut summary| {
                summary.active = active_id.as_deref() == Some(&summary.id);
                summary
            })
            .collect::<Vec<_>>();
        sessions.sort_by_key(|summary| std::cmp::Reverse(summary.created_at_ms));
        sessions
    }

    pub(crate) fn ensure_exploring_workspace(&self, workspace_id: &str) -> Result<(), RunError> {
        let workspace = self.state(workspace_id)?;
        if lock(&workspace.summary).status != RunStatus::Exploring {
            return Err(RunError::RunUnavailable(workspace_id.to_owned()));
        }
        Ok(())
    }

    /// Read one interactive agent session owned by an Explore workspace.
    ///
    /// # Errors
    ///
    /// Returns an error when the workspace or session is unknown, or when the
    /// session belongs to a different workspace.
    pub fn agent_session(
        &self,
        workspace_id: &str,
        session_id: &str,
    ) -> Result<AgentSessionDetail, RunError> {
        let workspace = self.state(workspace_id)?;
        let state = self.agent_session_state(session_id)?;
        let mut summary = lock(&state.summary).clone();
        if summary.workspace_id != workspace_id {
            return Err(RunError::UnknownAgentSession(session_id.to_owned()));
        }
        summary.active = lock(&workspace.active_agent_session_id).as_deref() == Some(session_id);
        let turns = lock(&state.turns)
            .clone()
            .into_iter()
            .map(|summary| {
                let presentation =
                    load_or_build_agent_turn_presentation(&state, &workspace, &summary)?;
                Ok(AgentTurnDetail {
                    summary,
                    presentation,
                })
            })
            .collect::<Result<Vec<_>, RunError>>()?;
        Ok(AgentSessionDetail {
            projection_version: AGENT_TURN_PRESENTATION_VERSION,
            summary,
            turns,
            events: lock(&state.events).clone(),
        })
    }

    /// Start one persistent harness-native session for an Explore workspace.
    ///
    /// # Errors
    ///
    /// Returns an error when the shared selection is invalid or the durable
    /// starting session cannot be created. Model-access and driver-startup
    /// failures are persisted asynchronously on the returned session.
    #[allow(clippy::too_many_lines)]
    pub fn start_agent_session(
        &self,
        workspace_id: &str,
        request: StartAgentSessionRequest,
        origin: WorkbenchOrigin,
    ) -> Result<AgentSessionSummary, RunError> {
        let workspace = self.state(workspace_id)?;
        let active_turn = lock(&workspace.active_agent_turn);
        let source = lock(&workspace.summary).clone();
        if source.status != RunStatus::Exploring {
            return Err(RunError::RunUnavailable(workspace_id.to_owned()));
        }
        if active_turn.is_some() {
            return Err(RunError::InvalidRequest(
                "finish or cancel the active turn before starting another session".to_owned(),
            ));
        }
        let selection = lock(&workspace.selection).clone();
        let harness_id = request
            .harness_id
            .or(selection.harness_id)
            .ok_or_else(|| RunError::InvalidRequest("choose a harness first".to_owned()))?;
        let model_profile_id = request
            .model_profile_id
            .or(selection.model_profile_id)
            .ok_or_else(|| RunError::InvalidRequest("choose a model profile first".to_owned()))?;
        let harness = self
            .inner
            .harnesses
            .get(&harness_id)
            .ok_or_else(|| RunError::InvalidRequest(format!("unknown harness: {harness_id}")))?
            .clone();
        let model_id = harness
            .models
            .get(&model_profile_id)
            .cloned()
            .ok_or_else(|| {
                RunError::InvalidRequest(format!(
                    "model profile {model_profile_id} is unavailable for harness {harness_id}"
                ))
            })?;
        let capabilities = lock(&workspace.capabilities).clone();
        if capabilities.is_empty() {
            return Err(RunError::RunUnavailable(workspace_id.to_owned()));
        }
        let scenario = self
            .inner
            .scenarios
            .get(&source.scenario_id)
            .cloned()
            .ok_or_else(|| RunError::UnknownScenario(source.scenario_id.clone()))?;
        let id = format!("agent-session-{}-{}", now_ms(), random_suffix());
        let evidence_root = workspace.agent_session_directories.create_session(&id)?;
        let (sender, _) = broadcast::channel(256);
        let summary = AgentSessionSummary {
            id: id.clone(),
            workspace_id: workspace_id.to_owned(),
            harness_id,
            model_profile_id,
            model_id,
            status: AgentSessionStatus::Starting,
            active: false,
            created_at_ms: now_ms(),
            updated_at_ms: now_ms(),
            turn_count: 0,
            error: None,
        };
        let secret_values = capabilities
            .iter()
            .map(|capability| capability.agent_token.as_bytes().to_vec())
            .collect();
        let state = Arc::new(AgentSessionState {
            summary: Mutex::new(summary),
            turns: Mutex::new(Vec::new()),
            events: Mutex::new(Vec::new()),
            sender,
            commands: Mutex::new(None),
            lifecycle_cancel: CancellationToken::new(),
            turn_cancel: Mutex::new(None),
            actor: Mutex::new(AgentActorRegistration::default()),
            actor_registered: Condvar::new(),
            evidence_error: Mutex::new(None),
            evidence_root,
            secret_values: Mutex::new(secret_values),
        });
        lock(&workspace.agent_sessions).insert(id.clone(), Arc::downgrade(&state));
        persist_agent_session(&state)?;
        lock(&self.inner.agent_sessions).insert(id.clone(), state.clone());
        if let Err(error) = record_event(
            &workspace,
            "workbench.agent.session.started",
            json!({ "origin": origin, "sessionId": id }),
        ) {
            register_agent_actor(&state, None);
            let message = "workspace session-start evidence could not be persisted";
            let _ = update_agent_session_status(&state, AgentSessionStatus::Failed, Some(message));
            let _ = record_agent_event(
                &state,
                "agent.session.failed",
                json!({ "sessionId": id, "message": message }),
            );
            return Err(error);
        }
        let (commands, receiver) = mpsc::channel();
        *lock(&state.commands) = Some(commands);
        let actor_state = state.clone();
        let actor_workspace = workspace.clone();
        let actor_controller = Arc::downgrade(&self.inner);
        let workspace_path = workspace.workspace.clone();
        let spawn = thread::Builder::new()
            .name(format!("agent-lab-session-{id}"))
            .spawn(move || {
                run_agent_session_actor(
                    &actor_controller,
                    &actor_state,
                    &actor_workspace,
                    &harness,
                    &workspace_path,
                    &scenario.limits,
                    &capabilities,
                    &receiver,
                    origin,
                );
            });
        let handle = match spawn {
            Ok(handle) => handle,
            Err(error) => {
                register_agent_actor(&state, None);
                update_agent_session_status(
                    &state,
                    AgentSessionStatus::Failed,
                    Some("agent session actor could not start"),
                )?;
                return Err(RunError::Process(ProcessError::Spawn(error.to_string())));
            }
        };
        register_agent_actor(&state, Some(handle));
        Ok(lock(&state.summary).clone())
    }

    /// Select one starting or ready session as the active workspace session.
    ///
    /// # Errors
    ///
    /// Returns an error when the session is unknown, belongs to another
    /// workspace, is not ready, or selection persistence fails.
    pub fn activate_agent_session(
        &self,
        workspace_id: &str,
        session_id: &str,
        origin: WorkbenchOrigin,
    ) -> Result<AgentSessionSummary, RunError> {
        let target = self.agent_session_state(session_id)?;
        let target_summary = lock(&target.summary).clone();
        if target_summary.workspace_id != workspace_id
            || target_summary.status != AgentSessionStatus::Ready
        {
            return Err(RunError::RunUnavailable(session_id.to_owned()));
        }
        let workspace = self.state(workspace_id)?;
        let active_turn = lock(&workspace.active_agent_turn);
        if active_turn.is_some() {
            return Err(RunError::InvalidRequest(
                "finish or cancel the active turn before switching sessions".to_owned(),
            ));
        }
        if lock(&workspace.summary).status != RunStatus::Exploring {
            return Err(RunError::RunUnavailable(workspace_id.to_owned()));
        }
        let mut active_id = lock(&workspace.active_agent_session_id);
        let previous_active_id = active_id.clone();
        persist_active_agent_session(&workspace, Some(session_id))?;
        *active_id = Some(session_id.to_owned());
        if let Err(error) = record_event(
            &workspace,
            "workbench.agent.session.activated",
            json!({ "origin": origin, "sessionId": session_id }),
        ) {
            let rollback = persist_active_agent_session(&workspace, previous_active_id.as_deref());
            *active_id = previous_active_id;
            rollback?;
            return Err(error);
        }
        drop(active_id);
        drop(active_turn);
        let mut summary = lock(&target.summary).clone();
        summary.active = true;
        Ok(summary)
    }

    /// Queue one attributable turn on a starting or ready interactive session.
    ///
    /// # Errors
    ///
    /// Returns an error when the prompt or session is invalid, another turn is
    /// active, the workspace snapshot fails, or the actor is unavailable.
    #[allow(clippy::too_many_lines)]
    pub fn start_agent_turn(
        &self,
        workspace_id: &str,
        session_id: &str,
        request: StartAgentTurnRequest,
        origin: WorkbenchOrigin,
    ) -> Result<AgentTurnSummary, RunError> {
        if request.prompt.trim().is_empty() {
            return Err(RunError::InvalidRequest(
                "agent prompt must not be empty".to_owned(),
            ));
        }
        if request.input.as_ref().is_some_and(|input| {
            serde_json::to_vec(input).is_ok_and(|bytes| bytes.len() > MAX_AGENT_TURN_INPUT_BYTES)
        }) {
            return Err(RunError::InvalidRequest(format!(
                "agent turn input exceeds the {MAX_AGENT_TURN_INPUT_BYTES} byte limit"
            )));
        }
        let workspace = self.state(workspace_id)?;
        if lock(&workspace.summary).status != RunStatus::Exploring {
            return Err(RunError::RunUnavailable(workspace_id.to_owned()));
        }
        let state = self.agent_session_state(session_id)?;
        {
            let summary = lock(&state.summary);
            if summary.workspace_id != workspace_id
                || lock(&workspace.active_agent_session_id).as_deref() != Some(session_id)
                || summary.status != AgentSessionStatus::Ready
            {
                return Err(RunError::RunUnavailable(session_id.to_owned()));
            }
        }
        let commands = lock(&state.commands).clone().ok_or_else(|| {
            RunError::RunUnavailable(format!("agent session {session_id} is not live"))
        })?;
        let id = format!("agent-turn-{}-{}", now_ms(), random_suffix());
        let capabilities = lock(&workspace.capabilities).clone();
        validate_agent_turn_command_size(
            session_id,
            &id,
            &request.prompt,
            request.input.as_ref(),
            &capabilities,
        )?;
        let attribution = AgentTurnAttribution {
            session_id: session_id.to_owned(),
            turn_id: id.clone(),
        };
        {
            let mut active = lock(&workspace.active_agent_turn);
            if active.is_some() {
                return Err(RunError::InvalidRequest(
                    "this workspace already has an active agent turn".to_owned(),
                ));
            }
            if lock(&workspace.summary).status != RunStatus::Exploring {
                return Err(RunError::RunUnavailable(workspace_id.to_owned()));
            }
            let summary = lock(&state.summary);
            if lock(&workspace.active_agent_session_id).as_deref() != Some(session_id)
                || summary.status != AgentSessionStatus::Ready
            {
                return Err(RunError::RunUnavailable(session_id.to_owned()));
            }
            *active = Some(attribution.clone());
        }
        let turn_relative = PathBuf::from("turns").join(&id);
        let prepared = (|| -> Result<(AgentTurnSummary, CancellationToken, Vec<CapabilityEndpoint>), RunError> {
            create_confined_evidence_directory(&state.evidence_root, &turn_relative)?;
            let mut snapshot = capture_tree(&workspace.workspace)?;
            redact_captured_tree(&mut snapshot, &lock(&state.secret_values));
            let source_revision = captured_tree_digest(&snapshot);
            write_confined_captured_tree(
                &state.evidence_root,
                &turn_relative.join("initial"),
                &snapshot,
            )?;
            let secrets = lock(&state.secret_values).clone();
            let turn = AgentTurnSummary {
                id: id.clone(),
                session_id: session_id.to_owned(),
                prompt: redact_string(&request.prompt, &secrets),
                input: request
                    .input
                    .clone()
                    .map(|value| redact_value(value, &secrets)),
                source_revision,
                capability_revisions: capabilities
                    .iter()
                    .map(|capability| (capability.id.clone(), capability.revision.clone()))
                    .collect(),
                status: AgentTurnStatus::Queued,
                started_at_ms: now_ms(),
                finished_at_ms: None,
            outcome: None,
            error: None,
            human_intervention_at_ms: None,
        };
            lock(&state.turns).push(turn.clone());
            let cancel = CancellationToken::new();
            *lock(&state.turn_cancel) = Some(cancel.clone());
            persist_agent_session(&state)?;
            record_event(
                &workspace,
                "workbench.agent.turn.started",
                json!({
                    "origin": origin,
                    "sessionId": session_id,
                    "turnId": turn.id,
                    "sourceRevision": turn.source_revision,
                    "capabilityRevisions": turn.capability_revisions,
                }),
            )?;
            Ok((turn, cancel, capabilities))
        })();
        let (turn, cancel, capabilities) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                rollback_agent_turn_start(&state, &workspace, &attribution, &turn_relative);
                return Err(error);
            }
        };
        if commands
            .send(AgentSessionCommand::StartTurn {
                turn_id: id,
                prompt: request.prompt,
                input: request.input,
                capabilities,
                cancel,
            })
            .is_err()
        {
            rollback_agent_turn_start(&state, &workspace, &attribution, &turn_relative);
            let _ = record_event(
                &workspace,
                "workbench.agent.turn.start-failed",
                json!({ "origin": origin, "sessionId": session_id, "turnId": turn.id }),
            );
            return Err(RunError::RunUnavailable(session_id.to_owned()));
        }
        Ok(turn)
    }

    /// Cancel the active turn in one workspace-owned session.
    ///
    /// # Errors
    ///
    /// Returns an error when the session is unknown, belongs to another
    /// workspace, or has no active turn.
    pub fn cancel_agent_turn(&self, workspace_id: &str, session_id: &str) -> Result<(), RunError> {
        let state = self.agent_session_state(session_id)?;
        if lock(&state.summary).workspace_id != workspace_id {
            return Err(RunError::UnknownAgentSession(session_id.to_owned()));
        }
        lock(&state.turn_cancel)
            .as_ref()
            .ok_or_else(|| RunError::InvalidRequest("this session has no active turn".to_owned()))?
            .cancel();
        Ok(())
    }

    /// Conservatively mark terminal input observed while an agent turn owns
    /// the shared workspace. The marker prevents later evaluation promotion
    /// from silently attributing a potentially human-authored effect to the
    /// harness alone.
    ///
    /// # Errors
    ///
    /// Returns an error when the workspace or active turn cannot be found, or
    /// when the intervention marker cannot be persisted.
    pub fn note_terminal_input(&self, workspace_id: &str) -> Result<(), RunError> {
        let workspace = self.state(workspace_id)?;
        let active_turn = lock(&workspace.active_agent_turn);
        let Some(attribution) = active_turn.clone() else {
            return Ok(());
        };
        let session = self.agent_session_state(&attribution.session_id)?;
        let marked_at_ms = {
            let mut turns = lock(&session.turns);
            let turn = turns
                .iter_mut()
                .find(|turn| turn.id == attribution.turn_id)
                .ok_or_else(|| {
                    RunError::InvalidRequest(format!("unknown agent turn: {}", attribution.turn_id))
                })?;
            if turn.human_intervention_at_ms.is_some() {
                return Ok(());
            }
            let marked_at_ms = now_ms();
            turn.human_intervention_at_ms = Some(marked_at_ms);
            marked_at_ms
        };
        persist_agent_session(&session)?;
        let result = record_agent_event(
            &session,
            "agent.turn.human-intervention",
            json!({
                "sessionId": attribution.session_id,
                "turnId": attribution.turn_id,
                "atMs": marked_at_ms,
                "source": "terminal-input",
            }),
        );
        drop(active_turn);
        result
    }

    /// Request an orderly close of one ready session.
    ///
    /// # Errors
    ///
    /// Returns an error when the session is unavailable, belongs to another
    /// workspace, or still has a running turn.
    pub fn close_agent_session(
        &self,
        workspace_id: &str,
        session_id: &str,
    ) -> Result<AgentSessionSummary, RunError> {
        let state = self.agent_session_state(session_id)?;
        if lock(&state.summary).workspace_id != workspace_id {
            return Err(RunError::UnknownAgentSession(session_id.to_owned()));
        }
        let workspace = self.state(workspace_id)?;
        let commands = lock(&state.commands).clone().ok_or_else(|| {
            RunError::RunUnavailable(format!("agent session {session_id} is not live"))
        })?;
        {
            let active = lock(&workspace.active_agent_turn);
            if active.is_some() {
                return Err(RunError::InvalidRequest(
                    "cancel or finish the active turn before closing this session".to_owned(),
                ));
            }
            let mut summary = lock(&state.summary);
            let was_starting = summary.status == AgentSessionStatus::Starting;
            if !matches!(
                summary.status,
                AgentSessionStatus::Starting | AgentSessionStatus::Ready
            ) {
                return Err(RunError::RunUnavailable(session_id.to_owned()));
            }
            summary.status = AgentSessionStatus::Closing;
            summary.updated_at_ms = now_ms();
            drop(summary);
            persist_agent_session(&state)?;
            if was_starting {
                state.lifecycle_cancel.cancel();
            }
        }
        clear_active_agent_session(&workspace, &state)?;
        if commands.send(AgentSessionCommand::Close).is_err() {
            update_agent_session_status(
                &state,
                AgentSessionStatus::Failed,
                Some("agent session actor stopped before close"),
            )?;
            return Err(RunError::RunUnavailable(session_id.to_owned()));
        }
        Ok(lock(&state.summary).clone())
    }

    /// Subscribe to durable and live events for a workspace-owned session.
    ///
    /// # Errors
    ///
    /// Returns an error when the session is unknown or belongs to another
    /// workspace.
    pub fn subscribe_agent_session(
        &self,
        workspace_id: &str,
        session_id: &str,
    ) -> Result<(Vec<RunEvent>, broadcast::Receiver<RunEvent>), RunError> {
        let state = self.agent_session_state(session_id)?;
        if lock(&state.summary).workspace_id != workspace_id {
            return Err(RunError::UnknownAgentSession(session_id.to_owned()));
        }
        let receiver = state.sender.subscribe();
        let history = lock(&state.events).clone();
        Ok((history, receiver))
    }

    /// Read the durable event suffix after a sequence number.
    ///
    /// # Errors
    ///
    /// Returns an error when the session is unknown.
    pub fn agent_session_events_after(
        &self,
        session_id: &str,
        sequence: u64,
    ) -> Result<Vec<RunEvent>, RunError> {
        let state = self.agent_session_state(session_id)?;
        Ok(lock(&state.events)
            .iter()
            .filter(|event| event.sequence > sequence)
            .cloned()
            .collect())
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
        let active_turn = lock(&state.active_agent_turn);
        if active_turn.is_some() {
            return Err(RunError::InvalidRequest(
                "cancel the active agent turn before closing this workspace".to_owned(),
            ));
        }
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
        drop(active_turn);
        if cancel_prepared {
            let sessions = lock(&state.agent_sessions)
                .values()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>();
            for session in &sessions {
                if matches!(
                    lock(&session.summary).status,
                    AgentSessionStatus::Starting
                        | AgentSessionStatus::Ready
                        | AgentSessionStatus::Running
                ) {
                    let mut summary = lock(&session.summary);
                    summary.status = AgentSessionStatus::Closing;
                    summary.updated_at_ms = now_ms();
                    summary.error = None;
                }
                session.lifecycle_cancel.cancel();
                if let Some(cancel) = lock(&session.turn_cancel).as_ref() {
                    cancel.cancel();
                }
                if let Some(commands) = lock(&session.commands).clone() {
                    let _ = commands.send(AgentSessionCommand::Shutdown);
                }
            }
            let mut actor_failures = Vec::new();
            for session in &sessions {
                if let Err(error) = join_agent_actor(session) {
                    actor_failures.push(error.to_string());
                }
                if let Err(error) = clear_active_agent_session(&state, session) {
                    actor_failures.push(error.to_string());
                }
            }
            let actor_error = (!actor_failures.is_empty()).then(|| {
                format!(
                    "agent session shutdown failed: {}",
                    actor_failures.join("; ")
                )
            });
            finish_run(
                &state,
                RunStatus::Cancelled,
                actor_error.as_deref(),
                &json!({ "passed": false, "cancelled": true }),
            )?;
            if let Some(error) = actor_error {
                return Err(RunError::Protocol(error));
            }
            return Ok(());
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
        let control_token = random_token();
        lock(&self.inner.workbench_grants).insert(control_token.clone(), id.to_owned());
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
            control_token,
        })
    }

    #[must_use]
    pub fn workbench_grant_allows(&self, token: &str, workspace_id: &str) -> bool {
        lock(&self.inner.workbench_grants)
            .get(token)
            .is_some_and(|granted| granted == workspace_id)
    }

    pub fn revoke_workbench_grant(&self, token: &str) {
        lock(&self.inner.workbench_grants).remove(token);
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
                state.reusable_explore
                    && summary.scenario_id == scenario.id
                    && summary.status == RunStatus::Exploring
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
        let agent_session_directories = AgentSessionDirectoryAnchor::open(bundle_dir.clone())?;
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
            harness_id: None,
            model_profile_id: None,
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
            selection: Mutex::new(default_workbench_selection(
                &self.inner.harnesses,
                &self.inner.model_profiles,
            )),
            events: Mutex::new(Vec::new()),
            sender,
            cancel: CancellationToken::new(),
            bundle_dir,
            agent_session_directories,
            workspace,
            output: scenario.output.clone(),
            initial_snapshot: Some(initial_snapshot),
            capabilities: Mutex::new(Vec::new()),
            secret_values: Mutex::new(driver_secret_values(&self.inner.driver)),
            agent_sessions: Mutex::new(HashMap::new()),
            active_agent_session_id: Mutex::new(None),
            active_agent_turn: Mutex::new(None),
            capability_attributions: Mutex::new(HashMap::new()),
            reusable_explore: true,
            replay_failed: false,
        });
        lock(&self.inner.runs).insert(id, state.clone());
        persist_manifest(&state)?;
        persist_assembly(&state)?;
        persist_selection(&state)?;
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
        request: &StartPreparedRunRequest,
    ) -> Result<RunSummary, RunError> {
        let (harness_id, model_profile_id, model_id, selected_driver) =
            self.resolve_harness_selection(request)?;
        validate_model_id(&model_id)?;
        let state = self.state(id)?;
        let active_turn = lock(&state.active_agent_turn);
        if active_turn.is_some() {
            return Err(RunError::InvalidRequest(
                "finish or cancel the active agent turn before running the harness".to_owned(),
            ));
        }
        if lock(&self.inner.agent_sessions).values().any(|session| {
            lock(&session.summary).workspace_id == id
                && matches!(
                    lock(&session.summary).status,
                    AgentSessionStatus::Starting
                        | AgentSessionStatus::Ready
                        | AgentSessionStatus::Running
                        | AgentSessionStatus::Closing
                )
        }) {
            return Err(RunError::InvalidRequest(
                "close interactive agent sessions before running the harness".to_owned(),
            ));
        }
        let capabilities = lock(&state.capabilities).clone();
        if capabilities.is_empty() {
            return Err(RunError::RunUnavailable(id.to_owned()));
        }
        let (scenario, previous_summary) = {
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
            let previous_summary = summary.clone();
            summary.model_id.clone_from(&model_id);
            summary.harness_id.clone_from(&harness_id);
            summary.model_profile_id.clone_from(&model_profile_id);
            summary.status = RunStatus::Starting;
            (scenario, previous_summary)
        };
        drop(active_turn);
        let previous_assembly = {
            let mut assembly = lock(&state.assembly);
            let previous = assembly.clone();
            assembly.harness.adapter = harness_id
                .clone()
                .unwrap_or_else(|| "external-jsonl-v1".to_owned());
            assembly.harness.model_id = Some(model_id);
            previous
        };
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
            rollback_prepared_start(
                &state,
                previous_summary,
                previous_assembly,
                "start persistence failed",
            );
            return Err(error);
        }
        let driver = if let Some(harness_id) = &harness_id {
            let harness = self.inner.harnesses.get(harness_id).ok_or_else(|| {
                RunError::InvalidRequest(format!("unknown harness: {harness_id}"))
            })?;
            match self.resolve_harness_driver(harness) {
                Ok(driver) => driver,
                Err(error) => {
                    rollback_prepared_start(
                        &state,
                        previous_summary,
                        previous_assembly,
                        "model access was unavailable at launch",
                    );
                    return Err(error);
                }
            }
        } else {
            selected_driver
        };
        let summary = lock(&state.summary).clone();
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
        let prepared = self
            .prepare(PrepareRunRequest {
                scenario_id: request.scenario_id,
            })
            .await?;
        self.start_prepared(
            &prepared.id,
            &StartPreparedRunRequest {
                model_id: request.model_id,
                harness_id: request.harness_id,
                model_profile_id: request.model_profile_id,
            },
        )
    }

    #[must_use]
    pub fn list_evaluations(&self) -> Vec<EvaluationSummary> {
        let mut evaluations = lock(&self.inner.evaluations)
            .values()
            .map(|state| lock(&state.summary).clone())
            .collect::<Vec<_>>();
        evaluations.sort_by_key(|evaluation| std::cmp::Reverse(evaluation.started_at_ms));
        evaluations
    }

    /// Read a paired evaluation and its durable comparison projection.
    ///
    /// # Errors
    ///
    /// Returns an error when the evaluation is unknown or its projection is unreadable.
    pub fn get_evaluation(&self, id: &str) -> Result<EvaluationDetail, RunError> {
        let state = self.evaluation_state(id)?;
        Ok(EvaluationDetail {
            summary: lock(&state.summary).clone(),
            events: lock(&state.events).clone(),
            comparison: if state.replay_failed {
                None
            } else {
                read_optional_json(&state.bundle_dir.join("comparison.json"))?
            },
        })
    }

    /// Subscribe to the recorded prefix and live paired-evaluation events.
    ///
    /// # Errors
    ///
    /// Returns an error when the evaluation is unknown.
    pub fn subscribe_evaluation(
        &self,
        id: &str,
    ) -> Result<(Vec<RunEvent>, broadcast::Receiver<RunEvent>), RunError> {
        let state = self.evaluation_state(id)?;
        let events = lock(&state.events);
        let receiver = state.sender.subscribe();
        Ok((events.clone(), receiver))
    }

    /// Re-read the durable paired-evaluation suffix after a streaming receiver reports lag.
    ///
    /// # Errors
    ///
    /// Returns an error when the evaluation is unknown.
    pub fn evaluation_events_after(
        &self,
        id: &str,
        sequence: u64,
    ) -> Result<Vec<RunEvent>, RunError> {
        let state = self.evaluation_state(id)?;
        Ok(lock(&state.events)
            .iter()
            .filter(|event| event.sequence > sequence)
            .cloned()
            .collect())
    }

    /// Cancel the active arm and prevent queued arms from starting.
    ///
    /// # Errors
    ///
    /// Returns an error when the evaluation is unknown.
    pub fn cancel_evaluation(&self, id: &str) -> Result<(), RunError> {
        self.evaluation_state(id)?.cancel.cancel();
        Ok(())
    }

    /// Capture an immutable Explore snapshot and queue a sequential harness comparison.
    ///
    /// # Errors
    ///
    /// Returns an error for an unavailable source workspace, invalid harness/model selection, or
    /// evidence-store failure.
    pub fn start_evaluation(
        &self,
        request: StartEvaluationRequest,
    ) -> Result<EvaluationSummary, RunError> {
        let (source_snapshot, source_assembly) = self.validate_evaluation_request(&request)?;

        let id = format!("evaluation-{}", run_id());
        let bundle_dir = confined_child(&self.inner.evaluations_dir, &id)?;
        fs::create_dir(&bundle_dir)?;
        let snapshot = bundle_dir.join("source");
        write_captured_tree(&snapshot, &source_snapshot)?;
        let source_revision = format!("revision-{}", run_id());
        write_json_atomic(
            &bundle_dir.join("source.json"),
            &json!({
                "workspaceId": request.source_workspace_id,
                "revision": source_revision,
                "assembly": source_assembly,
            }),
        )?;
        let summary = EvaluationSummary {
            id: id.clone(),
            scenario_id: request.scenario_id,
            model_profile_id: request.model_profile_id,
            source_workspace_id: request.source_workspace_id,
            source_revision,
            harness_ids: request.harness_ids.clone(),
            arms: request
                .harness_ids
                .iter()
                .map(|harness_id| EvaluationArmSummary {
                    harness_id: harness_id.clone(),
                    run_id: None,
                    status: "queued".to_owned(),
                })
                .collect(),
            status: EvaluationStatus::Queued,
            started_at_ms: now_ms(),
            finished_at_ms: None,
        };
        let (sender, _) = broadcast::channel(256);
        let state = Arc::new(EvaluationState {
            summary: Mutex::new(summary.clone()),
            events: Mutex::new(Vec::new()),
            sender,
            cancel: CancellationToken::new(),
            bundle_dir,
            snapshot,
            replay_failed: false,
        });
        write_json_atomic(
            &state.bundle_dir.join("manifest.json"),
            &serde_json::to_value(&summary)?,
        )?;
        record_evaluation_event(
            &state,
            "evaluation.created",
            json!({
                "sourceRevision": summary.source_revision,
                "harnessIds": summary.harness_ids,
            }),
        )?;
        lock(&self.inner.evaluations).insert(id, state.clone());
        let controller = self.clone();
        tokio::spawn(async move {
            if let Err(error) = controller.execute_evaluation(state.clone()).await {
                let message = error.to_string();
                finish_evaluation(&state, EvaluationStatus::Failed, Some(&message));
            }
        });
        Ok(summary)
    }

    fn validate_evaluation_request(
        &self,
        request: &StartEvaluationRequest,
    ) -> Result<(CapturedTree, AssemblySnapshot), RunError> {
        if request.harness_ids.len() != 2 {
            return Err(RunError::InvalidRequest(
                "an evaluation requires exactly two harness ids".to_owned(),
            ));
        }
        let unique = request.harness_ids.iter().collect::<HashSet<_>>();
        if unique.len() != request.harness_ids.len() {
            return Err(RunError::InvalidRequest(
                "evaluation harness ids must be unique".to_owned(),
            ));
        }
        for harness_id in &request.harness_ids {
            let harness = self.inner.harnesses.get(harness_id).ok_or_else(|| {
                RunError::InvalidRequest(format!("unknown harness: {harness_id}"))
            })?;
            if !harness.models.contains_key(&request.model_profile_id) {
                return Err(RunError::InvalidRequest(format!(
                    "model profile {} is unavailable for harness {harness_id}",
                    request.model_profile_id
                )));
            }
            self.ensure_harness_model_access_ready(harness)?;
        }
        let source = self.state(&request.source_workspace_id)?;
        let source_summary = lock(&source.summary).clone();
        if source_summary.scenario_id != request.scenario_id {
            return Err(RunError::InvalidRequest(
                "source workspace does not belong to the requested scenario".to_owned(),
            ));
        }
        if source_summary.status != RunStatus::Exploring {
            return Err(RunError::InvalidRequest(
                "only an active Explore workspace can be snapshotted".to_owned(),
            ));
        }

        let active_turn = lock(&source.active_agent_turn);
        if active_turn.is_some() {
            return Err(RunError::InvalidRequest(
                "finish or cancel the active agent turn before capturing an evaluation".to_owned(),
            ));
        }

        // Validate and capture the full source before creating a bundle. Writing this exact
        // in-memory snapshot also prevents Explore edits from racing the evaluation copy.
        Ok((
            capture_tree(&source.workspace)?,
            lock(&source.assembly).clone(),
        ))
    }

    #[allow(clippy::too_many_lines)]
    async fn execute_evaluation(&self, state: Arc<EvaluationState>) -> Result<(), RunError> {
        {
            let mut summary = lock(&state.summary);
            summary.status = EvaluationStatus::Running;
        }
        persist_evaluation(&state)?;
        record_evaluation_event(&state, "evaluation.status", json!({ "status": "running" }))?;
        let summary = lock(&state.summary).clone();
        for (index, harness_id) in summary.harness_ids.iter().enumerate() {
            if state.cancel.is_cancelled() {
                set_evaluation_arm(&state, index, None, "cancelled")?;
                continue;
            }
            let prepared = match self
                .prepare_snapshot_run(
                    &summary.scenario_id,
                    &state.snapshot,
                    &summary.source_revision,
                )
                .await
            {
                Ok(prepared) => prepared,
                Err(error) => {
                    set_evaluation_arm(&state, index, None, "failed")?;
                    record_evaluation_event(
                        &state,
                        "evaluation.arm.finished",
                        json!({
                            "harnessId": harness_id,
                            "status": "failed",
                            "error": error.to_string(),
                        }),
                    )?;
                    continue;
                }
            };
            set_evaluation_arm(&state, index, Some(prepared.id.clone()), "starting")?;
            record_evaluation_event(
                &state,
                "evaluation.arm.started",
                json!({
                    "harnessId": harness_id,
                    "runId": prepared.id,
                }),
            )?;
            if let Err(error) = self.start_prepared(
                &prepared.id,
                &StartPreparedRunRequest {
                    model_id: None,
                    harness_id: Some(harness_id.clone()),
                    model_profile_id: Some(summary.model_profile_id.clone()),
                },
            ) {
                let message = error.to_string();
                if let Ok(run_state) = self.state(&prepared.id) {
                    let _ = finish_run(
                        &run_state,
                        RunStatus::Failed,
                        Some(&message),
                        &json!({ "passed": false, "startError": message.clone() }),
                    );
                }
                set_evaluation_arm(&state, index, Some(prepared.id.clone()), "failed")?;
                record_evaluation_event(
                    &state,
                    "evaluation.arm.finished",
                    json!({
                        "harnessId": harness_id,
                        "runId": prepared.id,
                        "status": "failed",
                        "error": message,
                    }),
                )?;
                continue;
            }
            let mut projected_steps = 0;
            let mut projected_events = 0;
            loop {
                if state.cancel.is_cancelled() {
                    let _ = self.cancel(&prepared.id);
                }
                let run = self.get(&prepared.id)?;
                for event in run.events.iter().skip(projected_events) {
                    record_evaluation_event(
                        &state,
                        "evaluation.arm.event",
                        json!({
                            "harnessId": harness_id,
                            "runId": prepared.id,
                            "event": event,
                        }),
                    )?;
                }
                projected_events = run.events.len();
                for step in run.review.steps.iter().skip(projected_steps) {
                    record_evaluation_event(
                        &state,
                        "evaluation.arm.progress",
                        json!({
                            "harnessId": harness_id,
                            "runId": prepared.id,
                            "status": run.summary.status,
                            "step": step,
                        }),
                    )?;
                }
                projected_steps = run.review.steps.len();
                if run.summary.status.is_finished() {
                    let status = match run.summary.status {
                        RunStatus::Passed => "passed",
                        RunStatus::Cancelled => "cancelled",
                        _ => "failed",
                    };
                    set_evaluation_arm(&state, index, Some(prepared.id.clone()), status)?;
                    record_evaluation_event(
                        &state,
                        "evaluation.arm.finished",
                        json!({
                            "harnessId": harness_id,
                            "runId": prepared.id,
                            "status": status,
                        }),
                    )?;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        let comparison = self.build_evaluation_comparison(&state)?;
        write_json_atomic(&state.bundle_dir.join("comparison.json"), &comparison)?;
        let final_status = if state.cancel.is_cancelled() {
            EvaluationStatus::Cancelled
        } else if lock(&state.summary)
            .arms
            .iter()
            .all(|arm| arm.status == "passed")
        {
            EvaluationStatus::Passed
        } else {
            EvaluationStatus::Failed
        };
        finish_evaluation(&state, final_status, None);
        Ok(())
    }

    async fn prepare_snapshot_run(
        &self,
        scenario_id: &str,
        snapshot: &Path,
        source_revision: &str,
    ) -> Result<RunSummary, RunError> {
        let scenario = self
            .inner
            .scenarios
            .get(scenario_id)
            .cloned()
            .ok_or_else(|| RunError::UnknownScenario(scenario_id.to_owned()))?;
        let id = run_id();
        let bundle_dir = confined_child(&self.inner.data_dir, &id)?;
        fs::create_dir(&bundle_dir)?;
        let agent_session_directories = AgentSessionDirectoryAnchor::open(bundle_dir.clone())?;
        let workspace = bundle_dir.join("workspace");
        let initial_snapshot = snapshot_tree(snapshot)?;
        copy_tree(snapshot, &workspace)?;
        copy_tree(snapshot, &bundle_dir.join("initial"))?;
        let summary = RunSummary {
            id: id.clone(),
            scenario_id: scenario.id.clone(),
            scenario_title: scenario.title.clone(),
            model_id: String::new(),
            harness_id: None,
            model_profile_id: None,
            status: RunStatus::Exploring,
            started_at_ms: now_ms(),
            finished_at_ms: None,
            event_count: 0,
            error: None,
        };
        let (sender, _) = broadcast::channel(256);
        let mut assembly = initial_assembly(&summary, &scenario);
        assembly.workspace.seed_revision = source_revision.to_owned();
        let state = Arc::new(RunState {
            summary: Mutex::new(summary),
            assembly: Mutex::new(assembly),
            selection: Mutex::new(default_workbench_selection(
                &self.inner.harnesses,
                &self.inner.model_profiles,
            )),
            events: Mutex::new(Vec::new()),
            sender,
            cancel: CancellationToken::new(),
            bundle_dir,
            agent_session_directories,
            workspace,
            output: scenario.output.clone(),
            initial_snapshot: Some(initial_snapshot),
            capabilities: Mutex::new(Vec::new()),
            secret_values: Mutex::new(driver_secret_values(&self.inner.driver)),
            agent_sessions: Mutex::new(HashMap::new()),
            active_agent_session_id: Mutex::new(None),
            active_agent_turn: Mutex::new(None),
            capability_attributions: Mutex::new(HashMap::new()),
            reusable_explore: false,
            replay_failed: false,
        });
        lock(&self.inner.runs).insert(id, state.clone());
        persist_manifest(&state)?;
        persist_assembly(&state)?;
        persist_selection(&state)?;
        record_event(
            &state,
            "run.prepared",
            json!({
                "scenario": scenario.id,
                "sourceRevision": source_revision,
                "evaluationArm": true,
            }),
        )?;
        let capabilities = start_capability_sources(state.clone()).await?;
        lock(&state.capabilities).clone_from(&capabilities);
        update_assembly_capabilities(&state, &capabilities)?;
        Ok(lock(&state.summary).clone())
    }

    fn build_evaluation_comparison(&self, state: &EvaluationState) -> Result<JsonValue, RunError> {
        let summary = lock(&state.summary).clone();
        let mut arms = Vec::new();
        let mut outputs = Vec::new();
        for arm in &summary.arms {
            let detail = arm.run_id.as_deref().map(|id| self.get(id)).transpose()?;
            if let Some(detail) = detail {
                let (usage, cache) = reported_usage(&detail.events);
                outputs.push(detail.output.clone());
                arms.push(json!({
                    "harnessId": arm.harness_id,
                    "runId": detail.summary.id,
                    "status": arm.status,
                    "score": detail.score,
                    "metrics": detail.review.metrics,
                    "output": detail.output,
                    "firstUsefulAction": detail.review.steps.iter().find(|step| {
                        matches!(step.kind.as_str(), "capability" | "native-action" | "workspace-effect")
                    }),
                    "evidenceComplete": detail.summary.status.is_finished(),
                    "usage": usage,
                    "cache": cache,
                }));
            } else {
                arms.push(json!({
                    "harnessId": arm.harness_id,
                    "status": arm.status,
                    "evidenceComplete": false,
                    "usage": "not reported",
                    "cache": "not reported",
                }));
            }
        }
        let artifact_comparison = match outputs.as_slice() {
            [Some(left), Some(right)] if left == right => "same",
            [Some(_), Some(_)] => "different",
            _ => "missing",
        };
        Ok(json!({
            "version": 2,
            "sourceRevision": summary.source_revision,
            "modelProfileId": summary.model_profile_id,
            "arms": arms,
            "outputsMatch": artifact_comparison == "same",
            "artifactComparison": artifact_comparison,
            "outputDiff": if artifact_comparison == "different" {
                json!({ "left": outputs[0], "right": outputs[1] })
            } else {
                JsonValue::Null
            },
        }))
    }

    fn resolve_harness_selection(
        &self,
        request: &StartPreparedRunRequest,
    ) -> Result<(Option<String>, Option<String>, String, DriverLaunch), RunError> {
        match (&request.harness_id, &request.model_profile_id) {
            (Some(harness_id), Some(profile_id)) => {
                let harness = self.inner.harnesses.get(harness_id).ok_or_else(|| {
                    RunError::InvalidRequest(format!("unknown harness: {harness_id}"))
                })?;
                let concrete_model = harness.models.get(profile_id).ok_or_else(|| {
                    RunError::InvalidRequest(format!(
                        "model profile {profile_id} is unavailable for harness {harness_id}"
                    ))
                })?;
                Ok((
                    Some(harness_id.clone()),
                    Some(profile_id.clone()),
                    concrete_model.clone(),
                    harness.launch.clone(),
                ))
            }
            (None, None) => {
                let model_id = request.model_id.clone().ok_or_else(|| {
                    RunError::InvalidRequest(
                        "modelId or harnessId/modelProfileId is required".to_owned(),
                    )
                })?;
                Ok((None, None, model_id, self.inner.driver.clone()))
            }
            _ => Err(RunError::InvalidRequest(
                "harnessId and modelProfileId must be supplied together".to_owned(),
            )),
        }
    }

    fn resolve_harness_driver(&self, harness: &HarnessProfile) -> Result<DriverLaunch, RunError> {
        self.resolve_harness_driver_with_cancellation(harness, &CancellationToken::new())
    }

    fn resolve_harness_driver_with_cancellation(
        &self,
        harness: &HarnessProfile,
        cancel: &CancellationToken,
    ) -> Result<DriverLaunch, RunError> {
        let Some(provider_id) = self.inner.harness_model_access.get(&harness.id) else {
            return Ok(harness.launch.clone());
        };
        let provider = self
            .inner
            .model_access_providers
            .get(provider_id)
            .ok_or_else(|| {
                RunError::InvalidRequest(format!(
                    "unknown model-access provider for harness {}: {provider_id}",
                    harness.id
                ))
            })?;
        let resolution = resolve_model_access_with_cancellation(
            provider,
            true,
            MODEL_ACCESS_TIMEOUT,
            Some(cancel),
        )?;
        if resolution.status != ModelAccessStatus::Ready {
            return Err(RunError::ModelAccessUnavailable(
                resolution
                    .message
                    .unwrap_or_else(|| provider.setup_hint.clone()),
            ));
        }
        let mut launch = harness.launch.clone();
        for (name, value) in resolution.environment {
            launch.env.retain(|(existing, _)| existing != name.as_str());
            launch
                .env
                .push((OsString::from(name), OsString::from(value)));
        }
        Ok(launch)
    }

    fn ensure_harness_model_access_ready(&self, harness: &HarnessProfile) -> Result<(), RunError> {
        let Some(provider_id) = self.inner.harness_model_access.get(&harness.id) else {
            return Ok(());
        };
        let provider = self
            .inner
            .model_access_providers
            .get(provider_id)
            .ok_or_else(|| {
                RunError::InvalidRequest(format!(
                    "unknown model-access provider for harness {}: {provider_id}",
                    harness.id
                ))
            })?;
        let resolution = resolve_model_access(provider, false)?;
        if resolution.status == ModelAccessStatus::Ready {
            Ok(())
        } else {
            Err(RunError::ModelAccessUnavailable(
                resolution
                    .message
                    .unwrap_or_else(|| provider.setup_hint.clone()),
            ))
        }
    }

    fn state(&self, id: &str) -> Result<Arc<RunState>, RunError> {
        lock(&self.inner.runs)
            .get(id)
            .cloned()
            .ok_or_else(|| RunError::UnknownRun(id.to_owned()))
    }

    fn evaluation_state(&self, id: &str) -> Result<Arc<EvaluationState>, RunError> {
        lock(&self.inner.evaluations)
            .get(id)
            .cloned()
            .ok_or_else(|| RunError::InvalidRequest(format!("unknown evaluation: {id}")))
    }

    fn agent_session_state(&self, id: &str) -> Result<Arc<AgentSessionState>, RunError> {
        lock(&self.inner.agent_sessions)
            .get(id)
            .cloned()
            .ok_or_else(|| RunError::UnknownAgentSession(id.to_owned()))
    }
}

fn rollback_prepared_start(
    state: &RunState,
    previous_summary: RunSummary,
    previous_assembly: AssemblySnapshot,
    reason: &str,
) {
    *lock(&state.summary) = previous_summary;
    *lock(&state.assembly) = previous_assembly;
    let _ = persist_manifest(state);
    let _ = persist_assembly(state);
    let _ = record_event(
        state,
        "run.status",
        json!({ "status": RunStatus::Exploring, "reason": reason }),
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

fn resolve_model_access(
    provider: &ModelAccessProvider,
    include_environment: bool,
) -> Result<ModelAccessResolution, RunError> {
    resolve_model_access_with_timeout(provider, include_environment, MODEL_ACCESS_TIMEOUT)
}

fn resolve_model_access_with_timeout(
    provider: &ModelAccessProvider,
    include_environment: bool,
    timeout: Duration,
) -> Result<ModelAccessResolution, RunError> {
    resolve_model_access_with_cancellation(provider, include_environment, timeout, None)
}

fn resolve_model_access_with_cancellation(
    provider: &ModelAccessProvider,
    include_environment: bool,
    timeout: Duration,
    cancel: Option<&CancellationToken>,
) -> Result<ModelAccessResolution, RunError> {
    if cancel.is_some_and(CancellationToken::is_cancelled) {
        return Err(RunError::RunUnavailable(
            "model-access resolution was cancelled".to_owned(),
        ));
    }
    let allowed = provider
        .environment_names
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let Some(resolver) = &provider.resolver else {
        let environment = allowed_environment(None, &allowed);
        return Ok(ModelAccessResolution {
            status: if environment.is_empty() {
                ModelAccessStatus::NeedsSetup
            } else {
                ModelAccessStatus::Ready
            },
            source: (!environment.is_empty()).then(|| "environment".to_owned()),
            expires_at_ms: None,
            message: environment.is_empty().then(|| provider.setup_hint.clone()),
            environment: if include_environment {
                environment
            } else {
                BTreeMap::new()
            },
        });
    };

    let output =
        run_model_access_resolver(provider, resolver, include_environment, timeout, cancel)?;
    let mut resolution: ModelAccessResolution = serde_json::from_slice(&output).map_err(|_| {
        RunError::ModelAccessUnavailable(format!(
            "{} returned an invalid readiness response",
            provider.display_name
        ))
    })?;
    resolution
        .environment
        .retain(|name, value| allowed.contains(name.as_str()) && !value.is_empty());
    let mut secrets = allowed_environment(Some(resolver), &allowed)
        .into_values()
        .map(String::into_bytes)
        .collect::<Vec<_>>();
    secrets.extend(
        resolution
            .environment
            .values()
            .map(|value| value.as_bytes().to_vec()),
    );
    resolution.message = resolution
        .message
        .map(|message| redact_string(&message, &secrets));
    if !include_environment {
        resolution.environment.clear();
    } else if resolution.status == ModelAccessStatus::Ready && resolution.environment.is_empty() {
        return Err(RunError::ModelAccessUnavailable(format!(
            "{} reported ready without launch credentials",
            provider.display_name
        )));
    }
    Ok(resolution)
}

#[allow(clippy::too_many_lines)]
fn run_model_access_resolver(
    provider: &ModelAccessProvider,
    resolver: &DriverLaunch,
    include_environment: bool,
    timeout: Duration,
    cancel: Option<&CancellationToken>,
) -> Result<Vec<u8>, RunError> {
    let mut command = Command::new(&resolver.executable);
    command.args(&resolver.args).arg(if include_environment {
        "resolve"
    } else {
        "probe"
    });
    if let Some(cwd) = &resolver.cwd {
        command.current_dir(cwd);
    }
    if resolver.clear_env {
        command.env_clear();
    }
    command
        .envs(resolver.env.iter().cloned())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let mut command = CommandWrap::from(command);
    #[cfg(unix)]
    command.wrap(ProcessGroup::leader());
    #[cfg(windows)]
    command.wrap(JobObject);
    let mut child = command.spawn().map_err(|error| {
        RunError::ModelAccessUnavailable(format!(
            "could not check {}: {error}",
            provider.display_name
        ))
    })?;
    let stdout = child.stdout().take().ok_or_else(|| {
        RunError::ModelAccessUnavailable(format!(
            "{} could not capture model-access readiness",
            provider.display_name
        ))
    })?;
    let (output_sender, output_receiver) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let mut output = Vec::new();
        let result = stdout.take(64 * 1024 + 1).read_to_end(&mut output);
        let _ = output_sender.send(result.map(|_| output));
    });
    let deadline = Instant::now() + timeout;
    let status = loop {
        if cancel.is_some_and(CancellationToken::is_cancelled) {
            terminate_resolver_process(child.as_mut());
            return Err(RunError::RunUnavailable(
                "model-access resolution was cancelled".to_owned(),
            ));
        }
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(10)),
            Ok(None) => {
                terminate_resolver_process(child.as_mut());
                return Err(model_access_timeout(provider, timeout));
            }
            Err(error) => {
                terminate_resolver_process(child.as_mut());
                return Err(RunError::ModelAccessUnavailable(format!(
                    "could not check {}: {error}",
                    provider.display_name
                )));
            }
        }
    };
    let output = loop {
        if cancel.is_some_and(CancellationToken::is_cancelled) {
            terminate_resolver_process(child.as_mut());
            return Err(RunError::RunUnavailable(
                "model-access resolution was cancelled".to_owned(),
            ));
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            terminate_resolver_process(child.as_mut());
            return Err(model_access_timeout(provider, timeout));
        }
        match output_receiver.recv_timeout(remaining.min(Duration::from_millis(10))) {
            Ok(Ok(output)) => break output,
            Ok(Err(error)) => {
                terminate_resolver_process(child.as_mut());
                return Err(RunError::ModelAccessUnavailable(format!(
                    "could not read {} readiness: {error}",
                    provider.display_name
                )));
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                terminate_resolver_process(child.as_mut());
                return Err(RunError::ModelAccessUnavailable(format!(
                    "could not read {} readiness",
                    provider.display_name
                )));
            }
        }
    };
    if !status.success() || output.len() > 64 * 1024 {
        return Err(RunError::ModelAccessUnavailable(format!(
            "{} could not establish model access. {}",
            provider.display_name, provider.setup_hint
        )));
    }
    Ok(output)
}

fn model_access_timeout(provider: &ModelAccessProvider, timeout: Duration) -> RunError {
    RunError::ModelAccessUnavailable(format!(
        "{} model-access check timed out after {} ms. {}",
        provider.display_name,
        timeout.as_millis(),
        provider.setup_hint
    ))
}

fn terminate_resolver_process(child: &mut dyn ChildWrapper) {
    let _ = child.kill();
    let _ = child.wait();
}

fn allowed_environment(
    launch: Option<&DriverLaunch>,
    allowed: &HashSet<&str>,
) -> BTreeMap<String, String> {
    let mut environment = if launch.is_none_or(|launch| !launch.clear_env) {
        allowed
            .iter()
            .filter_map(|name| {
                std::env::var(name)
                    .ok()
                    .filter(|value| !value.is_empty())
                    .map(|value| ((*name).to_owned(), value))
            })
            .collect()
    } else {
        BTreeMap::new()
    };
    if let Some(launch) = launch {
        environment.extend(launch.env.iter().filter_map(|(name, value)| {
            let name = name.to_str()?;
            let value = value.to_str()?;
            (allowed.contains(name) && !value.is_empty())
                .then(|| (name.to_owned(), value.to_owned()))
        }));
    }
    environment
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
        let attribution = (actor == "agent")
            .then(|| capability_event_attribution(&state, source, kind, &payload))
            .flatten();
        if let Some(payload) = payload.as_object_mut() {
            payload.insert("source".to_owned(), JsonValue::String(source.to_owned()));
            payload.insert("actor".to_owned(), JsonValue::String(actor.to_owned()));
            if let Some(attribution) = &attribution {
                payload.insert(
                    "sessionId".to_owned(),
                    JsonValue::String(attribution.session_id.clone()),
                );
                payload.insert(
                    "turnId".to_owned(),
                    JsonValue::String(attribution.turn_id.clone()),
                );
            }
        }
        let secrets = lock(&state.secret_values).clone();
        payload = redact_value(payload, &secrets);
        let workspace_error = record_event(&state, kind, payload.clone()).err();
        if let Some(attribution) = attribution {
            let session = lock(&state.agent_sessions)
                .get(&attribution.session_id)
                .and_then(Weak::upgrade);
            if let Some(session) = session {
                let session_error = record_agent_event(&session, kind, payload).err();
                if let Some(error) = workspace_error.or(session_error) {
                    *lock(&session.evidence_error) = Some(format!(
                        "capability evidence could not be persisted: {error}"
                    ));
                }
            }
        }
    })
}

fn capability_event_attribution(
    state: &RunState,
    source: &str,
    kind: &str,
    payload: &JsonValue,
) -> Option<AgentTurnAttribution> {
    let call_id = payload.get("callId").and_then(JsonValue::as_str);
    let key = call_id.map(|call_id| format!("{source}:{call_id}"));
    if kind == "mcp.tool.started" {
        let attribution = lock(&state.active_agent_turn).clone();
        if let (Some(key), Some(attribution)) = (key, attribution.clone()) {
            lock(&state.capability_attributions).insert(key, attribution);
        }
        return attribution;
    }
    if kind == "mcp.tool.completed" {
        return key
            .and_then(|key| lock(&state.capability_attributions).remove(&key))
            .or_else(|| lock(&state.active_agent_turn).clone());
    }
    lock(&state.active_agent_turn).clone()
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
        .filter(|(name, _)| !matches!(name.to_str(), Some("PATH" | "HOME" | "TMPDIR" | "SHELL")))
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
    receive_until_deadline(driver, Instant::now() + timeout, cancel)
}

fn receive_until_deadline(
    driver: &mut DriverProcess,
    deadline: Instant,
    cancel: &CancellationToken,
) -> Result<Option<RawDriverMessage>, ProcessError> {
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
    let mut secret_values = driver_secret_values(&driver_launch);
    secret_values.extend(
        capabilities
            .iter()
            .flat_map(|capability| [&capability.human_token, &capability.agent_token])
            .map(|token| token.as_bytes().to_vec()),
    );
    lock(&state.secret_values).clone_from(&secret_values);
    update_status(state, RunStatus::Running)?;
    record_event(state, "driver.starting", JsonValue::Null)?;
    record_startup_event(
        state,
        "driver-process",
        "started",
        Some("Launching the external driver process"),
    )?;
    let mut driver = DriverProcess::spawn_with(driver_launch)?;
    record_startup_event(
        state,
        "driver-process",
        "completed",
        Some(&format!("Process {} started", driver.process_id())),
    )?;
    record_startup_event(
        state,
        "adapter-load",
        "started",
        Some("Loading the adapter and its module graph"),
    )?;
    let result = (|| -> Result<RunCompletion, RunError> {
        let ready_deadline = Instant::now() + DRIVER_READY_TIMEOUT;
        let descriptor = loop {
            let Some(message) = receive_until_deadline(&mut driver, ready_deadline, &state.cancel)?
            else {
                return Ok(cancelled_completion());
            };
            match message.parsed.body {
                DriverBody::StartupEvent {
                    phase,
                    status,
                    detail,
                } => record_startup_event(state, &phase, &status, detail.as_deref())?,
                DriverBody::Ready { driver } => break driver,
                _ => {
                    return Err(RunError::Protocol(
                        "expected startup.event or driver.ready".to_owned(),
                    ));
                }
            }
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
        record_startup_event(
            state,
            "session",
            "started",
            Some("Opening the harness session"),
        )?;
        driver.send(&command(
            "run-open",
            CommandBody::OpenSession {
                session_id: session_id.clone(),
                config: json!({
                    "files": {},
                    "modelId": summary.model_id,
                    "workspaceRoot": state.workspace,
                    "capabilitySources": capability_sources.clone(),
                }),
                limits: serde_json::to_value(&scenario.limits)?,
            },
        ))?;
        let session_deadline = Instant::now() + DRIVER_RESPONSE_TIMEOUT;
        loop {
            let Some(message) =
                receive_until_deadline(&mut driver, session_deadline, &state.cancel)?
            else {
                return Ok(cancelled_completion());
            };
            match message.parsed.body {
                DriverBody::StartupEvent {
                    phase,
                    status,
                    detail,
                } => record_startup_event(state, &phase, &status, detail.as_deref())?,
                DriverBody::SessionOpened {
                    session_id: opened_session,
                    ..
                } if opened_session == session_id => break,
                DriverBody::Failed { code, message, .. } => {
                    return Err(RunError::Protocol(format!(
                        "driver failed while opening session: {code}: {message}"
                    )));
                }
                _ => {
                    return Err(RunError::Protocol(
                        "expected startup.event or session.opened for the requested session"
                            .to_owned(),
                    ));
                }
            }
        }
        record_event(state, "driver.session-opened", JsonValue::Null)?;
        record_startup_event(
            state,
            "session",
            "completed",
            Some("Harness session opened"),
        )?;

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
                    DriverBody::StartupEvent {
                        phase,
                        status,
                        detail,
                    } => record_startup_event(state, &phase, &status, detail.as_deref())?,
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
        if state.cancel.is_cancelled() || abort_sent || outcome.as_deref() == Some("aborted") {
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

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn run_agent_session_actor(
    controller: &Weak<ControllerInner>,
    state: &AgentSessionState,
    workspace_state: &RunState,
    harness: &HarnessProfile,
    workspace: &Path,
    limits: &ScenarioLimits,
    capabilities: &[CapabilityEndpoint],
    commands: &mpsc::Receiver<AgentSessionCommand>,
    origin: WorkbenchOrigin,
) {
    let result = (|| -> Result<(), RunError> {
        let activation = controller.upgrade().ok_or_else(|| {
            RunError::RunUnavailable("controller stopped during agent session startup".to_owned())
        })?;
        let driver_launch = (RunController {
            inner: Arc::clone(&activation),
        })
        .resolve_harness_driver_with_cancellation(harness, &state.lifecycle_cancel)?;
        if state.lifecycle_cancel.is_cancelled() {
            return Err(RunError::RunUnavailable(
                "agent session startup was cancelled".to_owned(),
            ));
        }
        let mut secrets = lock(&state.secret_values).clone();
        secrets.extend(driver_secret_values(&driver_launch));
        lock(&state.secret_values).clone_from(&secrets);
        record_agent_event(
            state,
            "agent.session.starting",
            json!({ "sessionId": lock(&state.summary).id }),
        )?;
        let mut driver = DriverProcess::spawn_with(driver_launch)?;
        let ready_deadline = Instant::now() + DRIVER_READY_TIMEOUT;
        let descriptor = loop {
            let message =
                receive_until_deadline(&mut driver, ready_deadline, &state.lifecycle_cancel)?
                    .ok_or_else(|| {
                        RunError::Protocol("interactive driver readiness was cancelled".to_owned())
                    })?;
            match message.parsed.body {
                DriverBody::StartupEvent {
                    phase,
                    status,
                    detail,
                } => record_agent_event(
                    state,
                    "startup.event",
                    redact_value(
                        json!({ "phase": phase, "status": status, "detail": detail }),
                        &secrets,
                    ),
                )?,
                DriverBody::Ready { driver } => break redact_driver_descriptor(driver, &secrets),
                _ => {
                    return Err(RunError::Protocol(
                        "expected startup.event or driver.ready for interactive session".to_owned(),
                    ));
                }
            }
        };
        let session_id = lock(&state.summary).id.clone();
        let capability_sources = agent_capability_sources(capabilities);
        driver.send(&command(
            "agent-open",
            CommandBody::OpenSession {
                session_id: session_id.clone(),
                config: json!({
                    "files": {},
                    "modelId": lock(&state.summary).model_id,
                    "workspaceRoot": workspace,
                    "capabilitySources": capability_sources,
                }),
                limits: serde_json::to_value(limits)?,
            },
        ))?;
        let deadline = Instant::now() + DRIVER_RESPONSE_TIMEOUT;
        loop {
            let message = receive_until_deadline(&mut driver, deadline, &state.lifecycle_cancel)?
                .ok_or_else(|| {
                RunError::Protocol("interactive session opening was cancelled".to_owned())
            })?;
            match message.parsed.body {
                DriverBody::StartupEvent {
                    phase,
                    status,
                    detail,
                } => record_agent_event(
                    state,
                    "startup.event",
                    redact_value(
                        json!({ "phase": phase, "status": status, "detail": detail }),
                        &secrets,
                    ),
                )?,
                DriverBody::SessionOpened {
                    session_id: opened,
                    process_id,
                } if opened == session_id => {
                    record_agent_event(
                        state,
                        "agent.session.ready",
                        json!({ "sessionId": session_id, "processId": process_id, "driver": descriptor }),
                    )?;
                    break;
                }
                DriverBody::Failed { code, message, .. } => {
                    return Err(RunError::Protocol(format!(
                        "driver failed while opening interactive session: {code}: {message}"
                    )));
                }
                _ => {
                    return Err(RunError::Protocol(
                        "expected startup.event or session.opened for interactive session"
                            .to_owned(),
                    ));
                }
            }
        }
        update_agent_session_status(state, AgentSessionStatus::Ready, None)?;
        let workspace_id = lock(&state.summary).workspace_id.clone();
        if let Err(error) = (RunController { inner: activation }).activate_agent_session(
            &workspace_id,
            &session_id,
            origin,
        ) {
            record_agent_event(
                state,
                "agent.session.activation-deferred",
                json!({ "sessionId": session_id, "reason": error.to_string() }),
            )?;
        }
        let supports_turn_observations = descriptor
            .features
            .iter()
            .any(|feature| feature == TURN_OBSERVATIONS_FEATURE);

        while let Ok(command) = commands.recv() {
            match command {
                AgentSessionCommand::StartTurn {
                    turn_id,
                    prompt,
                    input,
                    capabilities,
                    cancel,
                } => run_agent_turn(
                    state,
                    workspace_state,
                    &mut driver,
                    &session_id,
                    &turn_id,
                    workspace,
                    &prompt,
                    input.as_ref(),
                    &capabilities,
                    limits,
                    &cancel,
                    &secrets,
                    supports_turn_observations,
                )?,
                AgentSessionCommand::Close => {
                    close_agent_driver(state, &mut driver, &session_id, false)?;
                    return Ok(());
                }
                AgentSessionCommand::Shutdown => {
                    close_agent_driver(state, &mut driver, &session_id, true)?;
                    return Ok(());
                }
            }
        }
        close_agent_driver(state, &mut driver, &session_id, true)
    })();

    if let Err(error) = result {
        if state.lifecycle_cancel.is_cancelled()
            && lock(&state.summary).status == AgentSessionStatus::Closing
        {
            let session_id = lock(&state.summary).id.clone();
            let _ = update_agent_session_status(state, AgentSessionStatus::Closed, None);
            let _ = record_agent_event(
                state,
                "agent.session.closed",
                json!({ "sessionId": session_id, "during": "startup" }),
            );
            let _ = clear_active_agent_session(workspace_state, state);
            return;
        }
        let secrets = lock(&state.secret_values).clone();
        let message = redact_string(&error.to_string(), &secrets);
        let terminal_turn = lock(&state.turn_cancel)
            .as_ref()
            .and_then(|_| lock(&state.turns).last().map(|turn| turn.id.clone()));
        if let Some(turn_id) = terminal_turn {
            let retained_terminal = lock(&state.events)
                .iter()
                .rev()
                .find(|event| {
                    event.kind == "agent.turn.finished"
                        && event.payload.get("turnId").and_then(JsonValue::as_str) == Some(&turn_id)
                })
                .cloned();
            let terminal_event = if let Some(event) = retained_terminal {
                let events = lock(&state.events).clone();
                let _ = repair_agent_turns_from_events(&mut lock(&state.turns), &events);
                if let Some(turn) = lock(&state.turns)
                    .iter()
                    .find(|turn| turn.id == turn_id)
                    .cloned()
                {
                    let _ = load_or_build_agent_turn_presentation(state, workspace_state, &turn);
                }
                let _ = remove_confined_evidence_file(
                    &state.evidence_root,
                    &PathBuf::from("turns")
                        .join(&turn_id)
                        .join("presentation.pending.json"),
                );
                let _ = persist_agent_session(state);
                Ok(event)
            } else {
                let _ = remove_confined_evidence_file(
                    &state.evidence_root,
                    &PathBuf::from("turns")
                        .join(&turn_id)
                        .join("presentation.pending.json"),
                );
                let workspace_diff =
                    finalize_agent_turn_workspace(state, &turn_id, workspace, &secrets)
                        .unwrap_or_else(|_| json!({ "changes": [] }));
                let turn_started_at_ms = lock(&state.turns)
                    .iter()
                    .find(|turn| turn.id == turn_id)
                    .map_or_else(now_ms, |turn| turn.started_at_ms);
                let termination = if lock(&state.turn_cancel)
                    .as_ref()
                    .is_some_and(CancellationToken::is_cancelled)
                {
                    Some(AgentTurnTermination::Cancelled)
                } else if now_ms().saturating_sub(turn_started_at_ms)
                    >= u128::from(limits.max_duration_ms)
                {
                    Some(AgentTurnTermination::TimedOut)
                } else {
                    None
                };
                let (status, outcome, terminal_error) = match termination {
                    Some(AgentTurnTermination::Cancelled) => {
                        (AgentTurnStatus::Cancelled, "cancelled", None)
                    }
                    Some(AgentTurnTermination::TimedOut) => (
                        AgentTurnStatus::Failed,
                        "timed-out",
                        Some("interactive turn duration limit exceeded"),
                    ),
                    None => (AgentTurnStatus::Failed, "failed", Some(message.as_str())),
                };
                let event = record_finished_agent_turn_event(
                    state,
                    workspace_state,
                    &turn_id,
                    status,
                    json!({
                        "sessionId": lock(&state.summary).id,
                        "turnId": turn_id,
                        "outcome": outcome,
                        "error": terminal_error,
                        "workspaceDiff": workspace_diff,
                    }),
                );
                let _ = update_agent_turn_status(
                    state,
                    &turn_id,
                    status,
                    Some(outcome),
                    terminal_error,
                );
                event
            };
            release_agent_turn_reservation(
                workspace_state,
                &AgentTurnAttribution {
                    session_id: lock(&state.summary).id.clone(),
                    turn_id,
                },
            );
            *lock(&state.turn_cancel) = None;
            if let Ok(event) = terminal_event {
                let _ = state.sender.send(event);
            }
        }
        let _ = update_agent_session_status(state, AgentSessionStatus::Failed, Some(&message));
        let _ = record_agent_event(state, "agent.session.failed", json!({ "message": message }));
    }
    let _ = clear_active_agent_session(workspace_state, state);
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn run_agent_turn(
    state: &AgentSessionState,
    workspace_state: &RunState,
    driver: &mut DriverProcess,
    session_id: &str,
    turn_id: &str,
    workspace: &Path,
    prompt: &str,
    input: Option<&JsonValue>,
    capabilities: &[CapabilityEndpoint],
    limits: &ScenarioLimits,
    cancel: &CancellationToken,
    secrets: &[Vec<u8>],
    supports_turn_observations: bool,
) -> Result<(), RunError> {
    let mut attribution = ActiveAgentTurnGuard::new(workspace_state, session_id, turn_id)?;
    let mut assistant_redactor = AssistantObservationRedactor::new(secrets);
    *lock(&state.evidence_error) = None;
    update_agent_turn_status(state, turn_id, AgentTurnStatus::Running, None, None)?;
    update_agent_session_status(state, AgentSessionStatus::Running, None)?;
    let capability_sources = agent_capability_sources(capabilities);
    let mut task = json!({
        "mode": "interactive",
        "prompt": prompt,
    });
    if let Some(input) = input {
        task.as_object_mut()
            .expect("interactive task is an object")
            .insert("input".to_owned(), input.clone());
    }
    driver.send(&command(
        &format!("{turn_id}-start"),
        CommandBody::StartTurn {
            session_id: session_id.to_owned(),
            turn_id: turn_id.to_owned(),
            task,
            capability_sources: capability_sources.clone(),
        },
    ))?;
    record_agent_event(
        state,
        "agent.turn.started",
        redact_value(
            json!({ "sessionId": session_id, "turnId": turn_id, "prompt": prompt, "input": input }),
            secrets,
        ),
    )?;
    let started = Instant::now();
    let mut termination = None;
    let mut abort_sent_at = None;
    let result = (|| -> Result<(), RunError> {
        loop {
            if termination.is_none() {
                termination = if cancel.is_cancelled() {
                    Some(AgentTurnTermination::Cancelled)
                } else if started.elapsed() >= Duration::from_millis(limits.max_duration_ms) {
                    Some(AgentTurnTermination::TimedOut)
                } else {
                    None
                };
            }
            if let Some(cause) = termination.filter(|_| abort_sent_at.is_none()) {
                driver.send(&command(
                    &format!("{turn_id}-abort"),
                    CommandBody::AbortTurn {
                        session_id: session_id.to_owned(),
                        turn_id: turn_id.to_owned(),
                        reason: Some(match cause {
                            AgentTurnTermination::Cancelled => {
                                "cancelled from Agent Lab".to_owned()
                            }
                            AgentTurnTermination::TimedOut => {
                                "interactive turn duration limit exceeded".to_owned()
                            }
                        }),
                    },
                ))?;
                abort_sent_at = Some(Instant::now());
            }
            if abort_sent_at.is_some_and(|sent| sent.elapsed() >= Duration::from_secs(10)) {
                return Err(RunError::Protocol(
                    "driver did not finish within 10 seconds of interactive turn abort".to_owned(),
                ));
            }
            match driver.receive(DRIVER_POLL) {
                Ok(message) => match message.parsed.body {
                    DriverBody::StartupEvent {
                        phase,
                        status,
                        detail,
                    } => record_agent_event(
                        state,
                        "startup.event",
                        redact_value(
                            json!({ "phase": phase, "status": status, "detail": detail }),
                            secrets,
                        ),
                    )?,
                    DriverBody::TurnEvent {
                        session_id: observed_session,
                        turn_id: observed_turn,
                        event_type,
                        payload,
                    } => {
                        validate_turn_identity(
                            &observed_session,
                            &observed_turn,
                            session_id,
                            turn_id,
                            "turn.event",
                        )?;
                        let observation = TurnObservation::parse(&event_type, &payload)
                            .map_err(|error| RunError::Protocol(error.to_string()))?;
                        if event_type.starts_with("observation.") {
                            if !supports_turn_observations {
                                return Err(RunError::Protocol(format!(
                                    "driver emitted {event_type} without advertising {TURN_OBSERVATIONS_FEATURE}"
                                )));
                            }
                            if observation.is_none() {
                                return Err(RunError::Protocol(format!(
                                    "unknown reserved turn observation: {event_type}"
                                )));
                            }
                        }
                        if let Some(observation) = observation {
                            for observation in assistant_redactor.redact(observation)? {
                                record_agent_event(
                                    state,
                                    observation.event_type(),
                                    json!({
                                        "sessionId": session_id,
                                        "turnId": turn_id,
                                        "event": observation.payload(),
                                    }),
                                )?;
                            }
                        } else {
                            record_agent_event(
                                state,
                                &driver_event_kind(&event_type),
                                redact_value(
                                    json!({
                                        "sessionId": session_id,
                                        "turnId": turn_id,
                                        "event": payload,
                                    }),
                                    secrets,
                                ),
                            )?;
                        }
                    }
                    DriverBody::TurnFinished {
                        session_id: observed_session,
                        turn_id: observed_turn,
                        outcome,
                        evidence,
                    } => {
                        if let Some(error) = lock(&state.evidence_error).take() {
                            return Err(RunError::EvidencePersistence(error));
                        }
                        validate_turn_identity(
                            &observed_session,
                            &observed_turn,
                            session_id,
                            turn_id,
                            "turn.finished",
                        )?;
                        if termination.is_none() {
                            termination = if cancel.is_cancelled() {
                                Some(AgentTurnTermination::Cancelled)
                            } else if started.elapsed()
                                >= Duration::from_millis(limits.max_duration_ms)
                            {
                                Some(AgentTurnTermination::TimedOut)
                            } else {
                                None
                            };
                        }
                        flush_pending_assistant_deltas(
                            state,
                            session_id,
                            turn_id,
                            &mut assistant_redactor,
                        )?;
                        if termination.is_none() {
                            termination = if cancel.is_cancelled() {
                                Some(AgentTurnTermination::Cancelled)
                            } else if started.elapsed()
                                >= Duration::from_millis(limits.max_duration_ms)
                            {
                                Some(AgentTurnTermination::TimedOut)
                            } else {
                                None
                            };
                        }
                        if outcome == "completed"
                            && termination.is_none()
                            && supports_turn_observations
                        {
                            require_agent_turn_response(state, turn_id)?;
                        }
                        let workspace_diff =
                            finalize_agent_turn_workspace(state, turn_id, workspace, secrets)?;
                        write_confined_json_atomic(
                            &state.evidence_root,
                            Path::new("transcript.json"),
                            &serde_json::to_value(redact_transcript(driver.transcript(), secrets))?,
                        )?;
                        let terminal_event = attribution.finish(|| {
                            if termination.is_none() {
                                termination = if cancel.is_cancelled() {
                                    Some(AgentTurnTermination::Cancelled)
                                } else if started.elapsed()
                                    >= Duration::from_millis(limits.max_duration_ms)
                                {
                                    Some(AgentTurnTermination::TimedOut)
                                } else {
                                    None
                                };
                            }
                            let (status, recorded_outcome, terminal_error) = match termination {
                                Some(AgentTurnTermination::Cancelled) => {
                                    (AgentTurnStatus::Cancelled, "cancelled".to_owned(), None)
                                }
                                Some(AgentTurnTermination::TimedOut) => (
                                    AgentTurnStatus::Failed,
                                    "timed-out".to_owned(),
                                    Some("interactive turn duration limit exceeded".to_owned()),
                                ),
                                None if outcome == "aborted" => {
                                    (AgentTurnStatus::Cancelled, "aborted".to_owned(), None)
                                }
                                None if outcome == "completed" => {
                                    if agent_turn_was_intervened(state, turn_id) {
                                        (AgentTurnStatus::Intervened, "intervened".to_owned(), None)
                                    } else {
                                        (AgentTurnStatus::Completed, "completed".to_owned(), None)
                                    }
                                }
                                None => (AgentTurnStatus::Failed, outcome.clone(), None),
                            };
                            let terminal_event = record_finished_agent_turn_event(
                                state,
                                workspace_state,
                                turn_id,
                                status,
                                redact_value(
                                    json!({
                                        "sessionId": session_id,
                                        "turnId": turn_id,
                                        "outcome": recorded_outcome,
                                        "driverOutcome": outcome,
                                        "evidence": evidence,
                                        "error": terminal_error,
                                        "workspaceDiff": workspace_diff,
                                    }),
                                    secrets,
                                ),
                            )?;
                            persist_agent_turn_terminal_state(
                                state,
                                turn_id,
                                status,
                                Some(&recorded_outcome),
                                terminal_error.as_deref(),
                            )?;
                            *lock(&state.turn_cancel) = None;
                            Ok(terminal_event)
                        })?;
                        let _ = state.sender.send(terminal_event);
                        return Ok(());
                    }
                    DriverBody::Failed {
                        scope,
                        session_id: failed_session,
                        turn_id: failed_turn,
                        code,
                        message,
                    } => {
                        if scope == DriverFailureScope::Turn
                            && (failed_session.as_deref() != Some(session_id)
                                || failed_turn.as_deref() != Some(turn_id))
                        {
                            record_agent_event(
                                state,
                                "driver.stale-turn-failure",
                                redact_value(
                                    json!({
                                        "sessionId": failed_session,
                                        "turnId": failed_turn,
                                        "code": code,
                                        "message": message,
                                    }),
                                    secrets,
                                ),
                            )?;
                            continue;
                        }
                        if scope == DriverFailureScope::Session
                            && failed_session.as_deref() != Some(session_id)
                        {
                            return Err(RunError::Protocol(format!(
                                "driver reported a failure for unexpected session {}",
                                failed_session.as_deref().unwrap_or("<missing>")
                            )));
                        }
                        let message = redact_string(
                            &format!("driver failed during interactive turn: {code}: {message}"),
                            secrets,
                        );
                        if scope != DriverFailureScope::Turn {
                            return Err(RunError::Protocol(message));
                        }
                        if let Some(error) = lock(&state.evidence_error).take() {
                            return Err(RunError::EvidencePersistence(error));
                        }
                        flush_pending_assistant_deltas(
                            state,
                            session_id,
                            turn_id,
                            &mut assistant_redactor,
                        )?;
                        let workspace_diff =
                            finalize_agent_turn_workspace(state, turn_id, workspace, secrets)?;
                        write_confined_json_atomic(
                            &state.evidence_root,
                            Path::new("transcript.json"),
                            &serde_json::to_value(redact_transcript(driver.transcript(), secrets))?,
                        )?;
                        if termination.is_none() {
                            termination = if cancel.is_cancelled() {
                                Some(AgentTurnTermination::Cancelled)
                            } else if started.elapsed()
                                >= Duration::from_millis(limits.max_duration_ms)
                            {
                                Some(AgentTurnTermination::TimedOut)
                            } else {
                                None
                            };
                        }
                        let terminal_event = attribution.finish(|| {
                            let (status, outcome, terminal_error) = match termination {
                                Some(AgentTurnTermination::Cancelled) => {
                                    (AgentTurnStatus::Cancelled, "cancelled", None)
                                }
                                Some(AgentTurnTermination::TimedOut) => (
                                    AgentTurnStatus::Failed,
                                    "timed-out",
                                    Some("interactive turn duration limit exceeded"),
                                ),
                                None => (AgentTurnStatus::Failed, "failed", Some(message.as_str())),
                            };
                            let terminal_event = record_finished_agent_turn_event(
                                state,
                                workspace_state,
                                turn_id,
                                status,
                                json!({
                                    "sessionId": session_id,
                                    "turnId": turn_id,
                                    "outcome": outcome,
                                    "driverFailure": {
                                        "scope": "turn",
                                        "code": code,
                                        "message": message,
                                    },
                                    "error": terminal_error,
                                    "workspaceDiff": workspace_diff,
                                }),
                            )?;
                            persist_agent_turn_terminal_state(
                                state,
                                turn_id,
                                status,
                                Some(outcome),
                                terminal_error,
                            )?;
                            *lock(&state.turn_cancel) = None;
                            Ok(terminal_event)
                        })?;
                        let _ = state.sender.send(terminal_event);
                        return Ok(());
                    }
                    _ => {}
                },
                Err(ProcessError::Timeout) => {}
                Err(error) => return Err(error.into()),
            }
        }
    })();
    if result.is_err() {
        flush_pending_assistant_deltas(state, session_id, turn_id, &mut assistant_redactor)?;
    }
    result
}

struct AssistantObservationRedactor<'a> {
    secrets: &'a [Vec<u8>],
    secret_patterns: Vec<&'a str>,
    messages: BTreeMap<String, AssistantMessageRedactionState>,
}

#[derive(Default)]
struct AssistantMessageRedactionState {
    original: String,
    pending: String,
    redacted: String,
    saw_delta: bool,
    complete: bool,
}

impl<'a> AssistantObservationRedactor<'a> {
    fn new(secrets: &'a [Vec<u8>]) -> Self {
        let secret_patterns = secrets
            .iter()
            .filter_map(|secret| std::str::from_utf8(secret).ok())
            .filter(|secret| secret.len() >= 4)
            .collect();
        Self {
            secrets,
            secret_patterns,
            messages: BTreeMap::new(),
        }
    }

    fn redact(&mut self, observation: TurnObservation) -> Result<Vec<TurnObservation>, RunError> {
        match observation {
            TurnObservation::AssistantDelta(delta) => {
                let state = self.messages.entry(delta.message_id.clone()).or_default();
                if state.complete {
                    return Err(RunError::Protocol(format!(
                        "assistant delta arrived after completion for {}",
                        delta.message_id
                    )));
                }
                state.saw_delta = true;
                state.original.push_str(&delta.text);
                state.pending.push_str(&delta.text);
                let text = drain_redacted_prefix(&mut state.pending, &self.secret_patterns, false);
                state.redacted.push_str(&text);
                if text.is_empty() {
                    Ok(Vec::new())
                } else {
                    Ok(vec![TurnObservation::AssistantDelta(
                        agent_lab_driver_protocol::AssistantDeltaObservation {
                            message_id: delta.message_id,
                            text,
                        },
                    )])
                }
            }
            TurnObservation::AssistantCompleted(completed) => {
                let state = self
                    .messages
                    .entry(completed.message_id.clone())
                    .or_default();
                if state.complete {
                    return Err(RunError::Protocol(format!(
                        "assistant completion was repeated for {}",
                        completed.message_id
                    )));
                }
                if state.saw_delta && state.original != completed.text {
                    return Err(RunError::Protocol(format!(
                        "assistant completion text disagrees with streamed deltas for {}",
                        completed.message_id
                    )));
                }
                let mut output = Vec::new();
                let suffix = drain_redacted_prefix(&mut state.pending, &self.secret_patterns, true);
                if !suffix.is_empty() {
                    state.redacted.push_str(&suffix);
                    output.push(TurnObservation::AssistantDelta(
                        agent_lab_driver_protocol::AssistantDeltaObservation {
                            message_id: completed.message_id.clone(),
                            text: suffix,
                        },
                    ));
                }
                let completed_text = redact_string(&completed.text, self.secrets);
                if state.saw_delta && state.redacted != completed_text {
                    return Err(RunError::Protocol(format!(
                        "redacted assistant completion disagrees with streamed deltas for {}",
                        completed.message_id
                    )));
                }
                state.complete = true;
                output.push(TurnObservation::AssistantCompleted(
                    agent_lab_driver_protocol::AssistantCompletedObservation {
                        message_id: completed.message_id,
                        text: completed_text,
                    },
                ));
                Ok(output)
            }
            observation => Ok(vec![observation]),
        }
    }

    fn flush_incomplete(&mut self) -> Vec<TurnObservation> {
        let mut output = Vec::new();
        for (message_id, state) in &mut self.messages {
            if state.complete {
                continue;
            }
            let suffix = drain_redacted_prefix(&mut state.pending, &self.secret_patterns, true);
            if suffix.is_empty() {
                continue;
            }
            state.redacted.push_str(&suffix);
            output.push(TurnObservation::AssistantDelta(
                agent_lab_driver_protocol::AssistantDeltaObservation {
                    message_id: message_id.clone(),
                    text: suffix,
                },
            ));
        }
        output
    }
}

fn flush_pending_assistant_deltas(
    state: &AgentSessionState,
    session_id: &str,
    turn_id: &str,
    redactor: &mut AssistantObservationRedactor<'_>,
) -> Result<(), RunError> {
    for observation in redactor.flush_incomplete() {
        record_agent_event(
            state,
            observation.event_type(),
            json!({
                "sessionId": session_id,
                "turnId": turn_id,
                "event": observation.payload(),
            }),
        )?;
    }
    Ok(())
}

fn drain_redacted_prefix(pending: &mut String, secrets: &[&str], finish: bool) -> String {
    let mut output = String::new();
    while !pending.is_empty() {
        if let Some(secret) = secrets
            .iter()
            .copied()
            .find(|secret| pending.starts_with(secret))
        {
            pending.drain(..secret.len());
            output.push_str("[REDACTED]");
            continue;
        }
        if !finish
            && secrets
                .iter()
                .any(|secret| secret.starts_with(pending.as_str()))
        {
            break;
        }
        let first_len = pending
            .chars()
            .next()
            .expect("non-empty string has a first character")
            .len_utf8();
        output.push_str(&pending[..first_len]);
        pending.drain(..first_len);
    }
    output
}

fn require_agent_turn_response(state: &AgentSessionState, turn_id: &str) -> Result<(), RunError> {
    let turn = lock(&state.turns)
        .iter()
        .find(|turn| turn.id == turn_id)
        .cloned()
        .ok_or_else(|| RunError::InvalidRequest(format!("unknown agent turn: {turn_id}")))?;
    let presentation = build_agent_turn_presentation(state, &turn)?;
    if presentation.messages.is_empty()
        || presentation
            .messages
            .iter()
            .any(|message| !message.complete)
    {
        return Err(RunError::Protocol(format!(
            "interactive turn {turn_id} completed without an authoritative assistant response"
        )));
    }
    Ok(())
}

fn finalize_agent_turn_workspace(
    state: &AgentSessionState,
    turn_id: &str,
    workspace: &Path,
    secrets: &[Vec<u8>],
) -> Result<JsonValue, RunError> {
    validate_evidence_tree(workspace)?;
    let turn_relative = PathBuf::from("turns").join(turn_id);
    let final_relative = turn_relative.join("final");
    let staging_relative = turn_relative.join("final.tmp");
    remove_confined_evidence_entry(&state.evidence_root, &staging_relative)?;
    let mut final_snapshot = capture_tree(workspace)?;
    redact_captured_tree(&mut final_snapshot, secrets);
    write_confined_captured_tree(&state.evidence_root, &staging_relative, &final_snapshot)?;
    let initial_snapshot =
        capture_confined_tree(&state.evidence_root, &turn_relative.join("initial"))?;
    let changes = captured_tree_changes(&initial_snapshot, &final_snapshot, secrets);
    let event_changes = changes
        .iter()
        .map(|change| {
            json!({
                "path": change["path"],
                "entryType": change["entryType"],
                "kind": change["kind"],
                "beforeMode": change["beforeMode"],
                "afterMode": change["afterMode"],
            })
        })
        .collect::<Vec<_>>();
    let diff = json!({ "changes": changes });
    write_confined_json_atomic(
        &state.evidence_root,
        &turn_relative.join("diff.json"),
        &diff,
    )?;
    remove_confined_evidence_entry(&state.evidence_root, &final_relative)?;
    rename_confined_evidence_file(&state.evidence_root, &staging_relative, &final_relative)?;
    Ok(json!({ "changes": event_changes }))
}

fn load_or_build_agent_turn_presentation(
    state: &AgentSessionState,
    _workspace: &RunState,
    turn: &AgentTurnSummary,
) -> Result<AgentTurnPresentation, RunError> {
    let events = lock(&state.events);
    let expected =
        build_agent_turn_presentation_from_events(&events, turn, &lock(&state.secret_values))?;
    let terminal_evidence_finalized = events.iter().any(|event| {
        event.kind == "agent.turn.finished"
            && event.payload.get("turnId").and_then(JsonValue::as_str) == Some(&turn.id)
    });
    let relative = PathBuf::from("turns")
        .join(&turn.id)
        .join("presentation.json");
    let stored = read_agent_turn_presentation(&state.evidence_root, &relative)?;
    if !terminal_evidence_finalized {
        return match stored {
            None => Ok(expected),
            Some(stored) if stored == expected => Ok(stored),
            Some(_) => Err(RunError::Protocol(format!(
                "agent turn presentation does not match retained evidence: {}",
                turn.id
            ))),
        };
    }
    validate_agent_turn_presentation(turn, &expected, &events)?;
    if stored.as_ref() != Some(&expected) {
        write_confined_json_atomic(
            &state.evidence_root,
            &relative,
            &serde_json::to_value(&expected)?,
        )?;
    }
    Ok(expected)
}

fn repair_terminal_agent_turn_presentations(
    evidence_root: &AgentSessionEvidenceRoot,
    turns: &[AgentTurnSummary],
    events: &[RunEvent],
) -> Result<bool, RunError> {
    let mut repaired = false;
    for turn in turns {
        let turn_relative = PathBuf::from("turns").join(&turn.id);
        let pending_relative = turn_relative.join("presentation.pending.json");
        if remove_confined_evidence_file(evidence_root, &pending_relative)? {
            repaired = true;
        }
        let terminal_evidence_finalized = events.iter().any(|event| {
            event.kind == "agent.turn.finished"
                && event.payload.get("turnId").and_then(JsonValue::as_str) == Some(&turn.id)
        });
        if !terminal_evidence_finalized {
            continue;
        }

        let expected = build_agent_turn_presentation_from_events(events, turn, &[])?;
        validate_agent_turn_presentation(turn, &expected, events)?;
        let relative = turn_relative.join("presentation.json");
        if read_agent_turn_presentation(evidence_root, &relative)?.as_ref() != Some(&expected) {
            write_confined_json_atomic(evidence_root, &relative, &serde_json::to_value(expected)?)?;
            repaired = true;
        }
    }
    Ok(repaired)
}

fn read_agent_turn_presentation(
    evidence_root: &AgentSessionEvidenceRoot,
    relative: &Path,
) -> Result<Option<AgentTurnPresentation>, RunError> {
    let display_path = evidence_root.display_path().join(relative);
    read_optional_agent_evidence_file(evidence_root, relative, &display_path)
        .map(|bytes| bytes.and_then(|bytes| serde_json::from_slice(&bytes).ok()))
}

fn record_finished_agent_turn_event(
    state: &AgentSessionState,
    _workspace: &RunState,
    turn_id: &str,
    terminal_status: AgentTurnStatus,
    payload: JsonValue,
) -> Result<RunEvent, RunError> {
    let mut turn = lock(&state.turns)
        .iter()
        .find(|turn| turn.id == turn_id)
        .cloned()
        .ok_or_else(|| RunError::InvalidRequest(format!("unknown agent turn: {turn_id}")))?;
    turn.status = terminal_status;
    let payload = redact_value(payload, &lock(&state.secret_values));
    let event = {
        // Hold the history lock across the projection and event commit.
        // Readers and subscribers can therefore observe the terminal event
        // only after its presentation is durable.
        let mut events = lock(&state.events);
        if let Some(existing) = events.iter().find(|event| {
            event.kind == "agent.turn.finished"
                && event.payload.get("turnId").and_then(JsonValue::as_str) == Some(turn_id)
        }) {
            return Ok(existing.clone());
        }
        let event = RunEvent {
            sequence: events.len() as u64 + 1,
            at_ms: now_ms(),
            kind: "agent.turn.finished".to_owned(),
            payload,
            progress: None,
        };
        let mut prospective_events = events.clone();
        prospective_events.push(event.clone());
        let presentation = build_agent_turn_presentation_from_events(
            &prospective_events,
            &turn,
            &lock(&state.secret_values),
        )?;
        validate_agent_turn_presentation(&turn, &presentation, &prospective_events)?;
        #[cfg(test)]
        maybe_inject_agent_presentation_write_failure(state, turn_id)?;
        let turn_relative = PathBuf::from("turns").join(turn_id);
        let pending_relative = turn_relative.join("presentation.pending.json");
        write_confined_json_atomic(
            &state.evidence_root,
            &pending_relative,
            &serde_json::to_value(presentation)?,
        )?;
        #[cfg(test)]
        maybe_inject_agent_terminal_event_append_failure(state, turn_id)?;
        append_agent_event_record_locked(state, &mut events, event.clone())?;
        rename_confined_evidence_file(
            &state.evidence_root,
            &pending_relative,
            &turn_relative.join("presentation.json"),
        )?;
        event
    };
    Ok(event)
}

#[cfg(test)]
fn maybe_inject_agent_terminal_event_append_failure(
    state: &AgentSessionState,
    turn_id: &str,
) -> Result<(), RunError> {
    let marker = state
        .evidence_root
        .display_path()
        .join("turns")
        .join(turn_id)
        .join("fail-terminal-event-append.once");
    if marker.is_file() {
        fs::remove_file(marker)?;
        return Err(RunError::EvidencePersistence(
            "injected agent terminal event append failure".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
fn maybe_inject_agent_presentation_write_failure(
    state: &AgentSessionState,
    turn_id: &str,
) -> Result<(), RunError> {
    let marker = state
        .evidence_root
        .display_path()
        .join("turns")
        .join(turn_id)
        .join("fail-presentation-write.once");
    if marker.is_file() {
        fs::remove_file(marker)?;
        return Err(RunError::EvidencePersistence(
            "injected agent presentation write failure".to_owned(),
        ));
    }
    Ok(())
}

fn validate_agent_turn_presentation(
    turn: &AgentTurnSummary,
    presentation: &AgentTurnPresentation,
    events: &[RunEvent],
) -> Result<(), RunError> {
    if matches!(
        turn.status,
        AgentTurnStatus::Completed | AgentTurnStatus::Intervened
    ) && agent_session_supports_turn_observations(events)
        && (presentation.messages.is_empty()
            || presentation
                .messages
                .iter()
                .any(|message| !message.complete))
    {
        return Err(RunError::Protocol(format!(
            "interactive turn {} completed without an authoritative assistant response",
            turn.id
        )));
    }
    Ok(())
}

fn build_agent_turn_presentation(
    state: &AgentSessionState,
    turn: &AgentTurnSummary,
) -> Result<AgentTurnPresentation, RunError> {
    build_agent_turn_presentation_from_events(
        &lock(&state.events),
        turn,
        &lock(&state.secret_values),
    )
}

#[allow(clippy::too_many_lines)]
fn build_agent_turn_presentation_from_events(
    events: &[RunEvent],
    turn: &AgentTurnSummary,
    secrets: &[Vec<u8>],
) -> Result<AgentTurnPresentation, RunError> {
    let relevant = events
        .iter()
        .filter(|event| event.payload.get("turnId").and_then(JsonValue::as_str) == Some(&turn.id))
        .cloned()
        .collect::<Vec<_>>();
    let observations_supported = agent_session_supports_turn_observations(events);
    let terminal_outcome = relevant
        .iter()
        .rev()
        .find(|event| event.kind == "agent.turn.finished")
        .and_then(|event| event.payload.get("outcome"))
        .and_then(JsonValue::as_str);
    let turn_evidence_finalized = terminal_outcome.is_some();
    let turn_successful = matches!(terminal_outcome, Some("completed" | "intervened"));
    let mut messages = Vec::<AgentAssistantMessage>::new();
    let mut message_indexes = HashMap::<String, usize>::new();
    let mut activity = Vec::<AgentTurnActivity>::new();
    let mut capability_indexes = HashMap::<(String, String), usize>::new();
    let mut native_action_indexes = HashMap::<String, usize>::new();
    let mut usage = None;

    for event in &relevant {
        let body = event.payload.get("event").unwrap_or(&event.payload);
        match event.kind.as_str() {
            "observation.assistant.delta" => {
                let message_id = required_json_string(body, "messageId", &event.kind)?;
                let text = required_json_string(body, "text", &event.kind)?;
                let index = *message_indexes
                    .entry(message_id.to_owned())
                    .or_insert_with(|| {
                        messages.push(AgentAssistantMessage {
                            id: message_id.to_owned(),
                            text: String::new(),
                            complete: false,
                            source_event_sequences: Vec::new(),
                        });
                        messages.len() - 1
                    });
                if messages[index].complete {
                    return Err(RunError::Protocol(format!(
                        "assistant delta arrived after completion for {message_id}"
                    )));
                }
                messages[index].text.push_str(text);
                messages[index].source_event_sequences.push(event.sequence);
            }
            "observation.assistant.completed" => {
                let message_id = required_json_string(body, "messageId", &event.kind)?;
                let completed_text = required_json_string(body, "text", &event.kind)?;
                let index = *message_indexes
                    .entry(message_id.to_owned())
                    .or_insert_with(|| {
                        messages.push(AgentAssistantMessage {
                            id: message_id.to_owned(),
                            text: completed_text.to_owned(),
                            complete: false,
                            source_event_sequences: Vec::new(),
                        });
                        messages.len() - 1
                    });
                if messages[index].complete {
                    return Err(RunError::Protocol(format!(
                        "assistant completion was repeated for {message_id}"
                    )));
                }
                if !messages[index].text.is_empty() && messages[index].text != completed_text {
                    return Err(RunError::Protocol(format!(
                        "assistant completion text disagrees with streamed deltas for {message_id}"
                    )));
                }
                completed_text.clone_into(&mut messages[index].text);
                messages[index].complete = true;
                messages[index].source_event_sequences.push(event.sequence);
            }
            "mcp.tool.started" => {
                let source = body
                    .get("source")
                    .and_then(JsonValue::as_str)
                    .unwrap_or("capability");
                let name = body
                    .get("name")
                    .and_then(JsonValue::as_str)
                    .unwrap_or("call");
                let call_id = body
                    .get("callId")
                    .and_then(JsonValue::as_str)
                    .map_or_else(|| format!("{}:{}", source, event.sequence), str::to_owned);
                let index = activity.len();
                capability_indexes.insert((source.to_owned(), call_id.clone()), index);
                activity.push(AgentTurnActivity {
                    kind: "capability-call".to_owned(),
                    title: format!("{source} · {name}"),
                    detail: None,
                    status: "running".to_owned(),
                    source: Some(source.to_owned()),
                    path: None,
                    operation: Some(name.to_owned()),
                    call_id: Some(call_id),
                    arguments: body
                        .get("arguments")
                        .filter(|arguments| !arguments.is_null())
                        .map(|arguments| redact_value(arguments.clone(), secrets)),
                    result: None,
                    action_id: None,
                    change_kind: None,
                    entry_type: None,
                    before_mode: None,
                    after_mode: None,
                    source_event_sequences: vec![event.sequence],
                });
            }
            "mcp.tool.completed" => {
                let source = body
                    .get("source")
                    .and_then(JsonValue::as_str)
                    .unwrap_or("capability");
                let name = body
                    .get("name")
                    .and_then(JsonValue::as_str)
                    .unwrap_or("call");
                let call_id = body
                    .get("callId")
                    .and_then(JsonValue::as_str)
                    .map(str::to_owned);
                let index = call_id
                    .as_ref()
                    .and_then(|call_id| {
                        capability_indexes
                            .get(&(source.to_owned(), call_id.to_owned()))
                            .copied()
                    })
                    .unwrap_or_else(|| {
                        activity.push(AgentTurnActivity {
                            kind: "capability-call".to_owned(),
                            title: format!("{source} · {name}"),
                            detail: None,
                            status: "running".to_owned(),
                            source: Some(source.to_owned()),
                            path: None,
                            operation: Some(name.to_owned()),
                            call_id: call_id.clone(),
                            arguments: None,
                            result: None,
                            action_id: None,
                            change_kind: None,
                            entry_type: None,
                            before_mode: None,
                            after_mode: None,
                            source_event_sequences: Vec::new(),
                        });
                        activity.len() - 1
                    });
                activity[index].status =
                    if body.get("isError").and_then(JsonValue::as_bool) == Some(true) {
                        "failed".to_owned()
                    } else {
                        "completed".to_owned()
                    };
                activity[index].result = body
                    .get("result")
                    .filter(|result| !result.is_null())
                    .map(|result| redact_value(result.clone(), secrets));
                activity[index].source_event_sequences.push(event.sequence);
            }
            "observation.native-action" => {
                let action_id = required_json_string(body, "actionId", &event.kind)?;
                let name = body
                    .get("name")
                    .and_then(JsonValue::as_str)
                    .unwrap_or("Native action");
                let index = *native_action_indexes
                    .entry(action_id.to_owned())
                    .or_insert_with(|| {
                        activity.push(AgentTurnActivity {
                            kind: "native-action".to_owned(),
                            title: name.to_owned(),
                            detail: None,
                            status: "started".to_owned(),
                            source: None,
                            path: None,
                            operation: Some(name.to_owned()),
                            call_id: None,
                            arguments: None,
                            result: None,
                            action_id: Some(action_id.to_owned()),
                            change_kind: None,
                            entry_type: None,
                            before_mode: None,
                            after_mode: None,
                            source_event_sequences: Vec::new(),
                        });
                        activity.len() - 1
                    });
                name.clone_into(&mut activity[index].title);
                activity[index].operation = Some(name.to_owned());
                if let Some(summary) = body.get("summary").and_then(JsonValue::as_str) {
                    activity[index].detail = Some(redact_string(summary, secrets));
                }
                body.get("status")
                    .and_then(JsonValue::as_str)
                    .unwrap_or("completed")
                    .clone_into(&mut activity[index].status);
                activity[index].source_event_sequences.push(event.sequence);
            }
            "observation.usage" => usage = Some(body.clone()),
            "agent.turn.finished" => {
                if let Some(changes) = body
                    .get("workspaceDiff")
                    .and_then(|diff| diff.get("changes"))
                    .and_then(JsonValue::as_array)
                {
                    for change in changes {
                        let path = change
                            .get("path")
                            .and_then(JsonValue::as_str)
                            .unwrap_or("workspace");
                        let kind = change
                            .get("kind")
                            .and_then(JsonValue::as_str)
                            .unwrap_or("changed");
                        let before_mode = change
                            .get("beforeMode")
                            .and_then(JsonValue::as_str)
                            .map(str::to_owned);
                        let after_mode = change
                            .get("afterMode")
                            .and_then(JsonValue::as_str)
                            .map(str::to_owned);
                        let mode_detail = match (before_mode.as_deref(), after_mode.as_deref()) {
                            (Some(before), Some(after)) if before != after => {
                                Some(format!("mode {before} -> {after}"))
                            }
                            _ => None,
                        };
                        activity.push(AgentTurnActivity {
                            kind: "workspace-effect".to_owned(),
                            title: format!("{} {path}", title_case(kind)),
                            detail: mode_detail,
                            status: "completed".to_owned(),
                            source: None,
                            path: Some(path.to_owned()),
                            operation: None,
                            call_id: None,
                            arguments: None,
                            result: None,
                            action_id: None,
                            change_kind: Some(kind.to_owned()),
                            entry_type: change
                                .get("entryType")
                                .and_then(JsonValue::as_str)
                                .map(str::to_owned),
                            before_mode,
                            after_mode,
                            source_event_sequences: vec![event.sequence],
                        });
                    }
                }
            }
            _ => {}
        }
    }

    for message in &mut messages {
        message.text = redact_string(&message.text, secrets);
    }
    let response = (!messages.is_empty()).then(|| {
        messages
            .iter()
            .map(|message| message.text.as_str())
            .collect::<Vec<_>>()
            .join("\n\n")
    });
    let source_event_sequences = relevant
        .iter()
        .map(|event| event.sequence)
        .collect::<Vec<_>>();
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(&relevant)?);
    let source_digest = format!("sha256:{:x}", hasher.finalize());
    let assistant_output =
        if !messages.is_empty() && messages.iter().all(|message| message.complete) {
            AgentPresentationCompleteness::Complete
        } else if messages.is_empty() {
            AgentPresentationCompleteness::Unavailable
        } else {
            AgentPresentationCompleteness::Partial
        };
    let capability_activity = if turn_evidence_finalized && turn_successful {
        AgentPresentationCompleteness::Complete
    } else {
        AgentPresentationCompleteness::Partial
    };
    let native_activity = if turn_evidence_finalized
        && turn_successful
        && (activity
            .iter()
            .any(|activity| activity.kind == "native-action")
            || observations_supported)
    {
        AgentPresentationCompleteness::Complete
    } else if observations_supported {
        AgentPresentationCompleteness::Partial
    } else {
        AgentPresentationCompleteness::Unavailable
    };
    let workspace_effects = if turn_evidence_finalized && turn_successful {
        AgentPresentationCompleteness::Complete
    } else {
        AgentPresentationCompleteness::Partial
    };
    let usage_completeness = if usage.is_some() {
        AgentPresentationCompleteness::Complete
    } else {
        AgentPresentationCompleteness::Unavailable
    };
    Ok(AgentTurnPresentation {
        schema_version: AGENT_TURN_PRESENTATION_VERSION,
        response,
        messages,
        activity,
        usage,
        completeness: AgentPresentationCompletenessSummary {
            assistant_output,
            capability_activity,
            native_activity,
            workspace_effects,
            usage: usage_completeness,
        },
        source_event_sequences,
        source_digest,
    })
}

fn agent_session_supports_turn_observations(events: &[RunEvent]) -> bool {
    events.iter().any(|event| {
        event.kind == "agent.session.ready"
            && event.payload["driver"]["features"]
                .as_array()
                .is_some_and(|features| {
                    features
                        .iter()
                        .any(|feature| feature.as_str() == Some(TURN_OBSERVATIONS_FEATURE))
                })
    })
}

fn required_json_string<'a>(
    value: &'a JsonValue,
    key: &str,
    event_kind: &str,
) -> Result<&'a str, RunError> {
    value
        .get(key)
        .and_then(JsonValue::as_str)
        .ok_or_else(|| RunError::Protocol(format!("{event_kind} requires a string {key}")))
}

fn title_case(value: &str) -> String {
    let mut chars = value.chars();
    chars.next().map_or_else(String::new, |first| {
        first.to_uppercase().collect::<String>() + chars.as_str()
    })
}

fn close_agent_driver(
    state: &AgentSessionState,
    driver: &mut DriverProcess,
    session_id: &str,
    interrupted: bool,
) -> Result<(), RunError> {
    driver.send(&command(
        "agent-close",
        CommandBody::CloseSession {
            session_id: session_id.to_owned(),
        },
    ))?;
    let closed = driver.receive(DRIVER_RESPONSE_TIMEOUT)?;
    if !matches!(closed.parsed.body, DriverBody::SessionClosed { session_id: ref closed } if closed == session_id)
    {
        return Err(RunError::Protocol(
            "expected session.closed for interactive session".to_owned(),
        ));
    }
    let exit = driver.wait_for_exit(DRIVER_RESPONSE_TIMEOUT)?;
    require_successful_driver_exit(exit)?;
    let status = if interrupted {
        AgentSessionStatus::Interrupted
    } else {
        AgentSessionStatus::Closed
    };
    update_agent_session_status(state, status, None)?;
    record_agent_event(
        state,
        if interrupted {
            "agent.session.interrupted"
        } else {
            "agent.session.closed"
        },
        json!({ "sessionId": session_id }),
    )?;
    Ok(())
}

fn agent_capability_sources(capabilities: &[CapabilityEndpoint]) -> JsonValue {
    JsonValue::Array(
        capabilities
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
            .collect(),
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
    const CONTROLLER_PREFIXES: &[&str] = &[
        "agent.",
        "controller.",
        "driver.",
        "mcp.",
        "run.",
        "workspace.",
    ];
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

fn record_startup_event(
    state: &RunState,
    phase: &str,
    status: &str,
    detail: Option<&str>,
) -> Result<(), RunError> {
    let secrets = lock(&state.secret_values).clone();
    record_event(
        state,
        "startup.event",
        json!({
            "phase": redact_string(phase, &secrets),
            "status": redact_string(status, &secrets),
            "detail": detail.map(|detail| redact_string(detail, &secrets)),
        }),
    )
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
            progress: None,
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

fn record_evaluation_event(
    state: &EvaluationState,
    kind: &str,
    payload: JsonValue,
) -> Result<(), RunError> {
    let event = {
        let mut events = lock(&state.events);
        let event = RunEvent {
            sequence: events.len() as u64 + 1,
            at_ms: now_ms(),
            kind: kind.to_owned(),
            payload,
            progress: None,
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
    let _ = state.sender.send(event);
    Ok(())
}

fn persist_evaluation(state: &EvaluationState) -> Result<(), RunError> {
    write_json_atomic(
        &state.bundle_dir.join("manifest.json"),
        &serde_json::to_value(lock(&state.summary).clone())?,
    )
}

fn persist_agent_session(state: &AgentSessionState) -> Result<(), RunError> {
    let manifest = AgentSessionManifest {
        version: AGENT_SESSION_MANIFEST_VERSION,
        summary: lock(&state.summary).clone(),
        turns: lock(&state.turns).clone(),
    };
    let secrets = lock(&state.secret_values).clone();
    let value = redact_value(serde_json::to_value(manifest)?, &secrets);
    write_confined_json_atomic(&state.evidence_root, Path::new("manifest.json"), &value)
}

fn record_agent_event(
    state: &AgentSessionState,
    kind: &str,
    payload: JsonValue,
) -> Result<(), RunError> {
    let payload = redact_value(payload, &lock(&state.secret_values));
    let event = {
        let mut events = lock(&state.events);
        append_agent_event_locked(state, &mut events, kind, payload)?
    };
    let _ = state.sender.send(event);
    Ok(())
}

fn append_agent_event_locked(
    state: &AgentSessionState,
    events: &mut Vec<RunEvent>,
    kind: &str,
    payload: JsonValue,
) -> Result<RunEvent, RunError> {
    let mut event = RunEvent {
        sequence: events.len() as u64 + 1,
        at_ms: now_ms(),
        kind: kind.to_owned(),
        payload,
        progress: None,
    };
    event.progress = if recent_driver_progress_supersedes_fallback(events, &event) {
        None
    } else {
        project_agent_progress(&event)
    };
    if event.progress.as_ref().is_some_and(|progress| {
        events
            .iter()
            .rev()
            .find(|previous| {
                same_agent_progress_context(previous, &event) && previous.progress.is_some()
            })
            .and_then(|previous| previous.progress.as_ref())
            .is_some_and(|previous| {
                previous.phase == progress.phase
                    && previous.detail == progress.detail
                    && previous.source == progress.source
            })
    }) {
        event.progress = None;
    }
    append_agent_event_record_locked(state, events, event.clone())?;
    Ok(event)
}

fn same_agent_progress_context(previous: &RunEvent, current: &RunEvent) -> bool {
    previous.payload.get("turnId").and_then(JsonValue::as_str)
        == current.payload.get("turnId").and_then(JsonValue::as_str)
}

fn recent_driver_progress_supersedes_fallback(events: &[RunEvent], event: &RunEvent) -> bool {
    if !matches!(
        event.kind.as_str(),
        "observation.assistant.delta"
            | "observation.assistant.completed"
            | "observation.native-action"
            | "v0.turn-start"
            | "v0.task-thinking-v1"
            | "v0.mdx"
            | "v0.agent-finalizing"
            | "v0.task-waiting-v1"
            | "v0.turn-finish"
            | "model.step.started"
            | "model.message.delta"
            | "model.step.completed"
            | "model.session.waiting"
    ) {
        return false;
    }
    events.iter().rev().any(|previous| {
        previous.kind == "observation.progress"
            && same_agent_progress_context(previous, event)
            && event.sequence.saturating_sub(previous.sequence) <= 3
    })
}

#[allow(clippy::too_many_lines)]
fn project_agent_progress(event: &RunEvent) -> Option<AgentProgressProjection> {
    let body = event.payload.get("event").unwrap_or(&event.payload);
    let progress = match event.kind.as_str() {
        "agent.session.starting" => progress(
            ProgressPhase::Starting,
            Some("Starting agent session"),
            Some("controller"),
        ),
        "startup.event" => progress(
            ProgressPhase::Starting,
            body.get("detail")
                .and_then(JsonValue::as_str)
                .or_else(|| body.get("phase").and_then(JsonValue::as_str)),
            Some("harness"),
        ),
        "agent.turn.started" => progress(
            ProgressPhase::Preparing,
            Some("Preparing turn"),
            Some("controller"),
        ),
        "observation.progress" => {
            let Ok(Some(TurnObservation::Progress(observation))) =
                TurnObservation::parse("observation.progress", body)
            else {
                return None;
            };
            progress(
                observation.phase,
                observation.detail.as_deref(),
                observation.source.as_deref().or(Some("harness")),
            )
        }
        "mcp.tool.started" => {
            let detail = capability_progress_detail(body, false);
            progress(ProgressPhase::Acting, Some(&detail), Some("mcp"))
        }
        "mcp.tool.completed" => {
            let detail = capability_progress_detail(body, true);
            progress(ProgressPhase::Waiting, Some(&detail), Some("mcp"))
        }
        "observation.native-action" => {
            let status = body
                .get("status")
                .and_then(JsonValue::as_str)
                .unwrap_or("started");
            let phase = if status == "started" {
                ProgressPhase::Acting
            } else {
                ProgressPhase::Waiting
            };
            progress(
                phase,
                body.get("summary")
                    .and_then(JsonValue::as_str)
                    .or_else(|| body.get("name").and_then(JsonValue::as_str)),
                Some("harness"),
            )
        }
        "observation.assistant.delta" | "observation.assistant.completed" => progress(
            ProgressPhase::Responding,
            Some("Writing response"),
            Some("model"),
        ),
        "v0.turn-start" | "model.step.started" | "model.turn.started" => progress(
            ProgressPhase::Reasoning,
            Some("Model step in progress"),
            Some("harness"),
        ),
        "v0.task-thinking-v1" => progress(
            ProgressPhase::Reasoning,
            Some("Model is reasoning"),
            Some("v0"),
        ),
        "v0.mdx" | "model.message.delta" => progress(
            ProgressPhase::Responding,
            Some("Writing response"),
            Some("harness"),
        ),
        "v0.agent-finalizing" => progress(
            ProgressPhase::Finalizing,
            Some("Finalizing answer"),
            Some("v0"),
        ),
        "v0.task-waiting-v1"
        | "v0.turn-finish"
        | "model.step.completed"
        | "model.session.waiting" => progress(
            ProgressPhase::Waiting,
            Some("Waiting for the next step"),
            Some("harness"),
        ),
        _ => return None,
    };
    Some(AgentProgressProjection {
        phase: progress.phase,
        detail: progress.detail,
        source: progress.source,
        source_event_sequence: event.sequence,
        source_event_type: event.kind.clone(),
    })
}

fn progress(
    phase: ProgressPhase,
    detail: Option<&str>,
    source: Option<&str>,
) -> ProgressObservation {
    ProgressObservation {
        phase,
        detail: detail.and_then(compact_progress_text),
        source: source.and_then(compact_progress_text),
    }
}

fn compact_progress_text(value: &str) -> Option<String> {
    let value = value
        .chars()
        .filter(|character| !character.is_control() || character.is_whitespace())
        .collect::<String>();
    let value = value.split_whitespace().collect::<Vec<_>>().join(" ");
    let value = value.chars().take(160).collect::<String>();
    (!value.is_empty()).then_some(value)
}

fn capability_progress_detail(body: &JsonValue, completed: bool) -> String {
    let source = body
        .get("source")
        .and_then(JsonValue::as_str)
        .unwrap_or("capability");
    let name = body
        .get("name")
        .or_else(|| body.get("tool"))
        .and_then(JsonValue::as_str)
        .unwrap_or("tool");
    if completed {
        format!("{source} · {name} returned")
    } else {
        format!("{source} · {name}")
    }
}

fn append_agent_event_record_locked(
    state: &AgentSessionState,
    events: &mut Vec<RunEvent>,
    event: RunEvent,
) -> Result<(), RunError> {
    if event.sequence != events.len() as u64 + 1 {
        return Err(RunError::Protocol(format!(
            "agent event sequence {} did not follow retained sequence {}",
            event.sequence,
            events.len()
        )));
    }
    let mut line = serde_json::to_vec(&event)?;
    line.push(b'\n');
    append_confined_bytes(&state.evidence_root, Path::new("events.jsonl"), &line)?;
    events.push(event);
    Ok(())
}

fn update_agent_session_status(
    state: &AgentSessionState,
    status: AgentSessionStatus,
    error: Option<&str>,
) -> Result<(), RunError> {
    {
        let mut summary = lock(&state.summary);
        summary.status = status;
        summary.updated_at_ms = now_ms();
        summary.error = error.map(str::to_owned);
        if matches!(
            status,
            AgentSessionStatus::Closed
                | AgentSessionStatus::Failed
                | AgentSessionStatus::Interrupted
        ) {
            summary.active = false;
        }
    }
    persist_agent_session(state)
}

fn update_agent_turn_status(
    state: &AgentSessionState,
    turn_id: &str,
    status: AgentTurnStatus,
    outcome: Option<&str>,
    error: Option<&str>,
) -> Result<(), RunError> {
    let finished = matches!(
        status,
        AgentTurnStatus::Completed
            | AgentTurnStatus::Intervened
            | AgentTurnStatus::Failed
            | AgentTurnStatus::Cancelled
    );
    {
        let mut turns = lock(&state.turns);
        let turn = turns
            .iter_mut()
            .find(|turn| turn.id == turn_id)
            .ok_or_else(|| RunError::InvalidRequest(format!("unknown agent turn: {turn_id}")))?;
        turn.status = status;
        turn.outcome = outcome.map(str::to_owned);
        turn.error = error.map(str::to_owned);
        if finished {
            turn.finished_at_ms = Some(now_ms());
        }
    }
    {
        let mut summary = lock(&state.summary);
        summary.turn_count = lock(&state.turns).len() as u64;
        summary.updated_at_ms = now_ms();
    }
    persist_agent_session(state)
}

fn persist_agent_turn_terminal_state(
    state: &AgentSessionState,
    turn_id: &str,
    status: AgentTurnStatus,
    outcome: Option<&str>,
    error: Option<&str>,
) -> Result<(), RunError> {
    {
        let mut turns = lock(&state.turns);
        let turn = turns
            .iter_mut()
            .find(|turn| turn.id == turn_id)
            .ok_or_else(|| RunError::InvalidRequest(format!("unknown agent turn: {turn_id}")))?;
        turn.status = status;
        turn.outcome = outcome.map(str::to_owned);
        turn.error = error.map(str::to_owned);
        turn.finished_at_ms = Some(now_ms());
    }
    {
        let mut summary = lock(&state.summary);
        summary.status = AgentSessionStatus::Ready;
        summary.turn_count = lock(&state.turns).len() as u64;
        summary.updated_at_ms = now_ms();
        summary.error = None;
    }
    persist_agent_session(state)
}

fn set_evaluation_arm(
    state: &EvaluationState,
    index: usize,
    run_id: Option<String>,
    status: &str,
) -> Result<(), RunError> {
    let mut summary = lock(&state.summary);
    let arm = summary.arms.get_mut(index).ok_or_else(|| {
        RunError::InvalidRequest(format!("evaluation arm index {index} is out of bounds"))
    })?;
    arm.run_id = run_id;
    status.clone_into(&mut arm.status);
    drop(summary);
    persist_evaluation(state)
}

fn finish_evaluation(state: &EvaluationState, status: EvaluationStatus, error: Option<&str>) {
    {
        let mut summary = lock(&state.summary);
        summary.status = status;
        summary.finished_at_ms = Some(now_ms());
    }
    let _ = persist_evaluation(state);
    let _ = record_evaluation_event(
        state,
        "evaluation.finished",
        json!({ "status": status, "error": error }),
    );
    state.cancel.cancel();
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

fn persist_selection(state: &RunState) -> Result<(), RunError> {
    write_json_atomic(
        &state.bundle_dir.join("workbench.json"),
        &serde_json::to_value(lock(&state.selection).clone())?,
    )
}

fn persist_active_agent_session(
    state: &RunState,
    session_id: Option<&str>,
) -> Result<(), RunError> {
    write_json_atomic(
        &state.bundle_dir.join("active-agent-session.json"),
        &json!({ "sessionId": session_id }),
    )
}

fn clear_active_agent_session(
    workspace: &RunState,
    session: &AgentSessionState,
) -> Result<(), RunError> {
    let _operation = lock(&workspace.active_agent_turn);
    let session_id = lock(&session.summary).id.clone();
    {
        let mut active_id = lock(&workspace.active_agent_session_id);
        if active_id.as_deref() == Some(&session_id) {
            persist_active_agent_session(workspace, None)?;
            *active_id = None;
        }
    }
    let summary_changed = {
        let mut summary = lock(&session.summary);
        if summary.active {
            summary.active = false;
            summary.updated_at_ms = now_ms();
            true
        } else {
            false
        }
    };
    if summary_changed {
        persist_agent_session(session)?;
    }
    Ok(())
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
    let mut pending_startup: HashMap<String, Vec<u64>> = HashMap::new();
    let mut native_actions = HashSet::new();
    let mut driver_turn_active = false;
    let mut outcome_recorded = false;

    for event in events {
        if outcome_recorded && event.kind != "run.persistence-failed" {
            continue;
        }
        match event.kind.as_str() {
            "startup.event" => {
                let phase = json_string(&event.payload, "phase").unwrap_or("startup");
                let status = json_string(&event.payload, "status").unwrap_or("completed");
                if status == "started" {
                    pending_startup
                        .entry(phase.to_owned())
                        .or_insert_with(|| vec![event.sequence]);
                    continue;
                }
                let mut sequences = pending_startup.remove(phase).unwrap_or_default();
                sequences.push(event.sequence);
                push_review_step(
                    &mut review,
                    "startup",
                    startup_title(phase),
                    json_string(&event.payload, "detail").map(str::to_owned),
                    status,
                    sequences,
                    Some(phase.to_owned()),
                    None,
                );
            }
            "driver.ready" => {
                let name = json_string(&event.payload, "name").unwrap_or("External driver");
                let version = json_string(&event.payload, "version");
                push_review_step(
                    &mut review,
                    "startup",
                    "Driver protocol ready".to_owned(),
                    Some(match version {
                        Some(version) => format!("{name} v{version}"),
                        None => name.to_owned(),
                    }),
                    "completed",
                    vec![event.sequence],
                    Some("protocol-ready".to_owned()),
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
            "v0.turn-finish" | "model.step.completed" => {
                if let Some(index) = current_turn.take() {
                    review.steps[index].event_sequences.push(event.sequence);
                }
            }
            "model.step.started" => {
                review.metrics.model_turns += 1;
                let turn = review.metrics.model_turns;
                push_review_step(
                    &mut review,
                    "model-turn",
                    format!("Model step {turn}"),
                    None,
                    "completed",
                    vec![event.sequence],
                    None,
                    None,
                );
                current_turn = review.steps.len().checked_sub(1);
            }
            "model.message.delta" => {
                if let (Some(index), Some(content)) = (
                    current_turn,
                    event
                        .payload
                        .pointer("/data/messageDelta")
                        .and_then(JsonValue::as_str),
                ) {
                    append_review_detail(&mut review.steps[index], content);
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
            "harness.action.result" => {
                let result = &event.payload["data"]["result"];
                let tool = json_string(result, "toolName");
                let status = json_string(&event.payload["data"], "status");
                if status == Some("completed")
                    && matches!(tool, Some("write_file" | "read_file" | "bash"))
                {
                    let id = json_string(result, "callId").unwrap_or("eve-native-action");
                    if native_actions.insert(id.to_owned()) {
                        review.metrics.native_actions += 1;
                        let path = result["output"]["path"].as_str().map(str::to_owned);
                        let title = match tool {
                            Some("write_file") => path.as_deref().map_or_else(
                                || "Wrote file".to_owned(),
                                |path| format!("Wrote {path}"),
                            ),
                            Some("read_file") => path.as_deref().map_or_else(
                                || "Read file".to_owned(),
                                |path| format!("Read {path}"),
                            ),
                            _ => "Ran shell command".to_owned(),
                        };
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

fn startup_title(phase: &str) -> String {
    match phase {
        "driver-process" => "Driver process".to_owned(),
        "adapter-load" => "Adapter loaded".to_owned(),
        "runtime-build" => "Harness runtime built".to_owned(),
        "runtime-process" => "Harness runtime process".to_owned(),
        "workspace" => "Workspace attached".to_owned(),
        "capabilities" => "Capabilities ready".to_owned(),
        "session" => "Harness session ready".to_owned(),
        _ => phase.replace('-', " "),
    }
}

fn reported_usage(events: &[RunEvent]) -> (JsonValue, JsonValue) {
    let mut saw_usage = false;
    let mut input_tokens = 0_u64;
    let mut output_tokens = 0_u64;
    let mut cache_read_tokens = 0_u64;
    let mut cache_write_tokens = 0_u64;
    for usage in events
        .iter()
        .filter(|event| event.kind == "model.step.completed")
        .filter_map(|event| event.payload.pointer("/data/usage"))
    {
        saw_usage = true;
        input_tokens = input_tokens.saturating_add(usage["inputTokens"].as_u64().unwrap_or(0));
        output_tokens = output_tokens.saturating_add(usage["outputTokens"].as_u64().unwrap_or(0));
        cache_read_tokens =
            cache_read_tokens.saturating_add(usage["cacheReadTokens"].as_u64().unwrap_or(0));
        cache_write_tokens =
            cache_write_tokens.saturating_add(usage["cacheWriteTokens"].as_u64().unwrap_or(0));
    }
    if saw_usage {
        (
            json!({
                "inputTokens": input_tokens,
                "outputTokens": output_tokens,
            }),
            json!({
                "readTokens": cache_read_tokens,
                "writeTokens": cache_write_tokens,
            }),
        )
    } else {
        (json!("not reported"), json!("not reported"))
    }
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

fn validate_agent_turn_command_size(
    session_id: &str,
    turn_id: &str,
    prompt: &str,
    input: Option<&JsonValue>,
    capabilities: &[CapabilityEndpoint],
) -> Result<(), RunError> {
    let mut task = json!({ "mode": "interactive", "prompt": prompt });
    if let Some(input) = input {
        task.as_object_mut()
            .expect("interactive task is an object")
            .insert("input".to_owned(), input.clone());
    }
    let record = command(
        &format!("{turn_id}-start"),
        CommandBody::StartTurn {
            session_id: session_id.to_owned(),
            turn_id: turn_id.to_owned(),
            task,
            capability_sources: agent_capability_sources(capabilities),
        },
    );
    let encoded = serde_json::to_vec(&record)?;
    if encoded.len().saturating_add(1) > MAX_DRIVER_RECORD_BYTES {
        return Err(RunError::InvalidRequest(format!(
            "agent prompt and structured input exceed the {MAX_DRIVER_RECORD_BYTES}-byte driver record limit"
        )));
    }
    Ok(())
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

fn default_workbench_selection(
    harnesses: &BTreeMap<String, HarnessProfile>,
    model_profiles: &BTreeMap<String, String>,
) -> WorkbenchSelection {
    let harness_id = harnesses.keys().next().cloned();
    let comparison_harness_ids = if harnesses.contains_key("v0") && harnesses.contains_key("eve") {
        vec!["v0".to_owned(), "eve".to_owned()]
    } else {
        harnesses.keys().take(2).cloned().collect()
    };
    let mut selection = WorkbenchSelection {
        harness_id,
        model_profile_id: None,
        comparison_harness_ids,
    };
    selection.model_profile_id = first_compatible_model(harnesses, model_profiles, &selection);
    selection
}

fn repair_workbench_selection(
    selection: &mut WorkbenchSelection,
    harnesses: &BTreeMap<String, HarnessProfile>,
    model_profiles: &BTreeMap<String, String>,
) {
    if selection
        .harness_id
        .as_ref()
        .is_none_or(|id| !harnesses.contains_key(id))
    {
        selection.harness_id = harnesses.keys().next().cloned();
    }
    if validate_comparison_harnesses(harnesses, &selection.comparison_harness_ids).is_err() {
        selection.comparison_harness_ids =
            default_workbench_selection(harnesses, model_profiles).comparison_harness_ids;
    }
    if selection
        .model_profile_id
        .as_deref()
        .is_none_or(|profile| validate_selection_model(harnesses, profile, selection).is_err())
    {
        selection.model_profile_id = first_compatible_model(harnesses, model_profiles, selection);
    }
}

fn first_compatible_model(
    harnesses: &BTreeMap<String, HarnessProfile>,
    model_profiles: &BTreeMap<String, String>,
    selection: &WorkbenchSelection,
) -> Option<String> {
    model_profiles
        .keys()
        .find(|profile| validate_selection_model(harnesses, profile, selection).is_ok())
        .cloned()
}

fn validate_selection_model(
    harnesses: &BTreeMap<String, HarnessProfile>,
    profile: &str,
    selection: &WorkbenchSelection,
) -> Result<(), RunError> {
    let mut selected = selection.comparison_harness_ids.clone();
    if let Some(harness_id) = &selection.harness_id
        && !selected.contains(harness_id)
    {
        selected.push(harness_id.clone());
    }
    for harness_id in selected {
        let harness = harnesses
            .get(&harness_id)
            .ok_or_else(|| RunError::InvalidRequest(format!("unknown harness: {harness_id}")))?;
        if !harness.models.contains_key(profile) {
            return Err(RunError::InvalidRequest(format!(
                "model profile {profile} is unavailable for harness {harness_id}"
            )));
        }
    }
    Ok(())
}

fn validate_harness(
    harnesses: &BTreeMap<String, HarnessProfile>,
    harness_id: &str,
) -> Result<(), RunError> {
    if harnesses.contains_key(harness_id) {
        Ok(())
    } else {
        Err(RunError::InvalidRequest(format!(
            "unknown harness: {harness_id}"
        )))
    }
}

fn validate_comparison_harnesses(
    harnesses: &BTreeMap<String, HarnessProfile>,
    harness_ids: &[String],
) -> Result<(), RunError> {
    if harness_ids.len() != 2 {
        return Err(RunError::InvalidRequest(
            "a comparison requires exactly two harness ids".to_owned(),
        ));
    }
    if harness_ids[0] == harness_ids[1] {
        return Err(RunError::InvalidRequest(
            "comparison harness ids must be unique".to_owned(),
        ));
    }
    for harness_id in harness_ids {
        validate_harness(harnesses, harness_id)?;
    }
    Ok(())
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
    harnesses: &BTreeMap<String, HarnessProfile>,
    model_profiles: &BTreeMap<String, String>,
) -> Result<HashMap<String, Arc<RunState>>, RunError> {
    let mut runs = HashMap::new();
    for entry in fs::read_dir(data_dir)? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                tracing::warn!(%error, "skipping unreadable run directory entry");
                continue;
            }
        };
        let bundle_dir = entry.path();
        let is_directory = match entry.file_type() {
            Ok(file_type) => file_type.is_dir(),
            Err(error) => {
                tracing::warn!(bundle = %bundle_dir.display(), %error, "skipping unreadable run bundle");
                continue;
            }
        };
        if !is_directory {
            continue;
        }
        match load_run_bundle(
            &bundle_dir,
            &entry.file_name(),
            scenarios,
            harnesses,
            model_profiles,
        ) {
            Ok(Some(state)) => {
                let id = lock(&state.summary).id.clone();
                runs.insert(id, state);
            }
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(bundle = %bundle_dir.display(), %error, "skipping malformed run bundle");
            }
        }
    }
    Ok(runs)
}

fn validate_recovered_agent_manifest(manifest: &AgentSessionManifest) -> Result<(), RunError> {
    validate_portable_evidence_id("agent session", &manifest.summary.id)?;
    let mut turn_ids = HashSet::new();
    for turn in &manifest.turns {
        validate_portable_evidence_id("agent turn", &turn.id)?;
        if turn.session_id != manifest.summary.id {
            return Err(RunError::InvalidRequest(format!(
                "agent turn {} does not belong to session {}",
                turn.id, manifest.summary.id
            )));
        }
        if !turn_ids.insert(turn.id.as_str()) {
            return Err(RunError::InvalidRequest(format!(
                "agent session contains duplicate turn id: {}",
                turn.id
            )));
        }
    }
    Ok(())
}

fn validate_portable_evidence_id(kind: &str, id: &str) -> Result<(), RunError> {
    let valid = !id.is_empty()
        && id
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && id
            .bytes()
            .any(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        && matches!(
            Path::new(id).components().collect::<Vec<_>>().as_slice(),
            [std::path::Component::Normal(_)]
        );
    if valid {
        Ok(())
    } else {
        Err(RunError::InvalidRequest(format!(
            "{kind} id is not a portable evidence component: {id:?}"
        )))
    }
}

#[allow(clippy::too_many_lines)]
fn load_agent_sessions(
    runs: &HashMap<String, Arc<RunState>>,
) -> HashMap<String, Arc<AgentSessionState>> {
    let mut sessions = HashMap::new();
    for run in runs.values() {
        let session_names = match run.agent_session_directories.session_names() {
            Ok(session_names) => session_names,
            Err(error) => {
                tracing::warn!(
                    collection = %run.agent_session_directories.collection_display_path().display(),
                    %error,
                    "skipping malformed agent session collection"
                );
                continue;
            }
        };
        for session_name in session_names {
            let bundle_dir = run
                .agent_session_directories
                .collection_display_path()
                .join(&session_name);
            let loaded = (|| -> Result<Arc<AgentSessionState>, RunError> {
                let evidence_root = run.agent_session_directories.open_session(&session_name)?;
                let manifest_relative = Path::new("manifest.json");
                let manifest_path = bundle_dir.join(manifest_relative);
                let manifest_bytes = read_optional_agent_evidence_file(
                    &evidence_root,
                    manifest_relative,
                    &manifest_path,
                )?
                .ok_or_else(|| {
                    RunError::InvalidRequest("agent session bundle has no manifest".to_owned())
                })?;
                let mut manifest: AgentSessionManifest = serde_json::from_slice(&manifest_bytes)?;
                if !(AGENT_SESSION_MANIFEST_LEGACY_VERSION..=AGENT_SESSION_MANIFEST_VERSION)
                    .contains(&manifest.version)
                {
                    return Err(RunError::InvalidRequest(format!(
                        "unsupported agent session manifest version: {}",
                        manifest.version
                    )));
                }
                validate_recovered_agent_manifest(&manifest)?;
                let migrate_legacy_presentations =
                    manifest.version < AGENT_SESSION_PRESENTATION_REQUIRED_VERSION;
                if manifest.summary.id != session_name.to_string_lossy()
                    || manifest.summary.workspace_id != lock(&run.summary).id
                {
                    return Err(RunError::InvalidRequest(
                        "agent session bundle identity does not match its owner".to_owned(),
                    ));
                }
                let was_live = matches!(
                    manifest.summary.status,
                    AgentSessionStatus::Starting
                        | AgentSessionStatus::Ready
                        | AgentSessionStatus::Running
                        | AgentSessionStatus::Closing
                );
                let repaired_inactive = manifest.summary.active
                    && matches!(
                        manifest.summary.status,
                        AgentSessionStatus::Closed
                            | AgentSessionStatus::Failed
                            | AgentSessionStatus::Interrupted
                    );
                if repaired_inactive {
                    manifest.summary.active = false;
                }
                if was_live {
                    manifest.summary.status = AgentSessionStatus::Interrupted;
                    manifest.summary.active = false;
                    manifest.summary.updated_at_ms = now_ms();
                    manifest.summary.error = Some(
                        "the server restarted; start a new agent session to continue".to_owned(),
                    );
                    for turn in &mut manifest.turns {
                        if matches!(
                            turn.status,
                            AgentTurnStatus::Queued | AgentTurnStatus::Running
                        ) {
                            turn.status = AgentTurnStatus::Failed;
                            turn.finished_at_ms = Some(now_ms());
                            turn.error = Some("the server restarted during this turn".to_owned());
                        }
                    }
                }
                let events = read_agent_events_recovering(&evidence_root)?;
                let repaired_terminal =
                    repair_agent_turns_from_events(&mut manifest.turns, &events);
                let repaired_presentations = repair_terminal_agent_turn_presentations(
                    &evidence_root,
                    &manifest.turns,
                    &events,
                )?;
                let (sender, _) = broadcast::channel(256);
                let state = Arc::new(AgentSessionState {
                    summary: Mutex::new(manifest.summary),
                    turns: Mutex::new(manifest.turns),
                    events: Mutex::new(events),
                    sender,
                    commands: Mutex::new(None),
                    lifecycle_cancel: CancellationToken::new(),
                    turn_cancel: Mutex::new(None),
                    actor: Mutex::new(AgentActorRegistration {
                        complete: true,
                        handle: None,
                    }),
                    actor_registered: Condvar::new(),
                    evidence_error: Mutex::new(None),
                    evidence_root,
                    secret_values: Mutex::new(Vec::new()),
                });
                if was_live
                    || repaired_inactive
                    || repaired_terminal
                    || repaired_presentations
                    || migrate_legacy_presentations
                {
                    persist_agent_session(&state)?;
                }
                if was_live {
                    record_agent_event(
                        &state,
                        "agent.session.interrupted",
                        json!({ "reason": "server-restarted" }),
                    )?;
                }
                Ok(state)
            })();
            match loaded {
                Ok(state) => {
                    let id = lock(&state.summary).id.clone();
                    if sessions.insert(id.clone(), state).is_some() {
                        tracing::warn!(%id, "skipping duplicate agent session identity");
                    }
                }
                Err(error) => {
                    tracing::warn!(bundle = %bundle_dir.display(), %error, "skipping malformed agent session bundle");
                }
            }
        }
    }
    sessions
}

#[allow(clippy::too_many_lines)]
fn load_run_bundle(
    bundle_dir: &Path,
    bundle_name: &OsStr,
    scenarios: &BTreeMap<String, ScenarioManifest>,
    harnesses: &BTreeMap<String, HarnessProfile>,
    model_profiles: &BTreeMap<String, String>,
) -> Result<Option<Arc<RunState>>, RunError> {
    let agent_session_directories = AgentSessionDirectoryAnchor::open(bundle_dir.to_path_buf())?;
    let manifest_path = bundle_dir.join("manifest.json");
    if !manifest_path.is_file() {
        return Ok(None);
    }
    let mut summary: RunSummary = serde_json::from_slice(&fs::read(&manifest_path)?)?;
    if summary.id != bundle_name.to_string_lossy() {
        return Ok(None);
    }
    let scenario = scenarios.get(&summary.scenario_id);
    let workspace = bundle_dir.join("workspace");
    if !workspace.is_dir() && !summary.status.is_finished() {
        return Ok(None);
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
                            progress: None,
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
            return Ok(None);
        };
        recover_legacy_assembly(&summary, scenario, &events)
    };
    if assembly.scenario.output.as_os_str().is_empty() {
        let Some(scenario) = scenario else {
            return Ok(None);
        };
        assembly.scenario.output.clone_from(&scenario.output);
    }
    assembly.scenario.output = workspace_relative_path(&assembly.scenario.output)?;
    let output = assembly.scenario.output.clone();
    let initial_snapshot = (summary.status == RunStatus::Exploring)
        .then(|| snapshot_tree(&bundle_dir.join("initial")).ok())
        .flatten();
    let reusable_explore = !events.iter().any(|event| {
        event.kind == "run.prepared"
            && (event.payload["evaluationArm"].as_bool() == Some(true)
                || event.payload.get("sourceRevision").is_some())
    });
    let mut selection = if bundle_dir.join("workbench.json").is_file() {
        serde_json::from_slice(&fs::read(bundle_dir.join("workbench.json"))?)?
    } else {
        default_workbench_selection(harnesses, model_profiles)
    };
    repair_workbench_selection(&mut selection, harnesses, model_profiles);
    summary.event_count = events.len() as u64;
    let (sender, _) = broadcast::channel(256);
    let state = Arc::new(RunState {
        summary: Mutex::new(summary),
        assembly: Mutex::new(assembly),
        selection: Mutex::new(selection),
        events: Mutex::new(events),
        sender,
        cancel: CancellationToken::new(),
        bundle_dir: bundle_dir.to_path_buf(),
        agent_session_directories,
        workspace,
        output,
        initial_snapshot,
        capabilities: Mutex::new(Vec::new()),
        secret_values: Mutex::new(Vec::new()),
        agent_sessions: Mutex::new(HashMap::new()),
        active_agent_session_id: Mutex::new(None),
        active_agent_turn: Mutex::new(None),
        capability_attributions: Mutex::new(HashMap::new()),
        reusable_explore,
        replay_failed,
    });
    persist_selection(&state)?;
    if interrupted && !recover_finalized_run(&state)? {
        recover_interrupted_run(&state, malformed_event_log)?;
    }
    Ok(Some(state))
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
        state.bundle_dir.join("initial"),
        state.bundle_dir.join("initial.tmp"),
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

fn load_evaluations(
    evaluations_dir: &Path,
) -> Result<HashMap<String, Arc<EvaluationState>>, RunError> {
    let mut evaluations = HashMap::new();
    for entry in fs::read_dir(evaluations_dir)? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                tracing::warn!(%error, "skipping unreadable evaluation directory entry");
                continue;
            }
        };
        let bundle_dir = entry.path();
        let is_directory = match entry.file_type() {
            Ok(file_type) => file_type.is_dir(),
            Err(error) => {
                tracing::warn!(bundle = %bundle_dir.display(), %error, "skipping unreadable evaluation bundle");
                continue;
            }
        };
        if !is_directory {
            continue;
        }
        match load_evaluation_bundle(&bundle_dir, &entry.file_name()) {
            Ok(Some(state)) => {
                let id = lock(&state.summary).id.clone();
                evaluations.insert(id, state);
            }
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(bundle = %bundle_dir.display(), %error, "skipping malformed evaluation bundle");
            }
        }
    }
    Ok(evaluations)
}

fn load_evaluation_bundle(
    bundle_dir: &Path,
    bundle_name: &OsStr,
) -> Result<Option<Arc<EvaluationState>>, RunError> {
    let manifest = bundle_dir.join("manifest.json");
    let snapshot = bundle_dir.join("source");
    if !manifest.is_file() || !snapshot.is_dir() {
        return Ok(None);
    }
    let mut summary: EvaluationSummary = serde_json::from_slice(&fs::read(&manifest)?)?;
    if summary.id != bundle_name.to_string_lossy() {
        return Ok(None);
    }
    let (events, replay_failed) = match read_events(&bundle_dir.join("events.jsonl")) {
        Ok(events) => (events, false),
        Err(error) => {
            let message = format!("stored evaluation event replay failed: {error}");
            summary.status = EvaluationStatus::Failed;
            summary.finished_at_ms.get_or_insert_with(now_ms);
            (
                vec![RunEvent {
                    sequence: 1,
                    at_ms: now_ms(),
                    kind: "evaluation.finished".to_owned(),
                    payload: json!({
                        "status": EvaluationStatus::Failed,
                        "error": message,
                        "recovered": true,
                    }),
                    progress: None,
                }],
                true,
            )
        }
    };
    let interrupted = !summary.status.is_finished();
    if interrupted {
        summary.status = EvaluationStatus::Cancelled;
        summary.finished_at_ms = Some(now_ms());
        for arm in &mut summary.arms {
            if arm.status == "queued" || arm.status == "starting" || arm.status == "running" {
                "cancelled".clone_into(&mut arm.status);
            }
        }
    }
    let (sender, _) = broadcast::channel(256);
    let state = Arc::new(EvaluationState {
        summary: Mutex::new(summary),
        events: Mutex::new(events),
        sender,
        cancel: CancellationToken::new(),
        bundle_dir: bundle_dir.to_owned(),
        snapshot,
        replay_failed,
    });
    if interrupted {
        persist_evaluation(&state)?;
        record_evaluation_event(
            &state,
            "evaluation.finished",
            json!({
                "status": EvaluationStatus::Cancelled,
                "error": "controller stopped before the evaluation finalized",
                "recovered": true,
            }),
        )?;
    }
    Ok(Some(state))
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

fn read_agent_events_recovering(
    evidence_root: &AgentSessionEvidenceRoot,
) -> Result<Vec<RunEvent>, RunError> {
    let relative = Path::new("events.jsonl");
    let path = evidence_root.display_path().join(relative);
    let source =
        read_optional_agent_evidence_file(evidence_root, relative, &path)?.ok_or_else(|| {
            RunError::InvalidRequest("agent session bundle has no event log".to_owned())
        })?;
    let mut events = Vec::new();
    let mut valid_bytes = 0_usize;
    for line in source.split_inclusive(|byte| *byte == b'\n') {
        let content = line.strip_suffix(b"\n").unwrap_or(line);
        if content.iter().all(u8::is_ascii_whitespace) {
            valid_bytes += line.len();
            continue;
        }
        let Ok(mut event) = serde_json::from_slice::<RunEvent>(content) else {
            break;
        };
        if event.sequence != events.len() as u64 + 1 {
            break;
        }
        redact_json(&mut event.payload);
        events.push(event);
        valid_bytes += line.len();
    }
    if valid_bytes == source.len() {
        return Ok(events);
    }

    write_confined_bytes_atomic(evidence_root, Path::new("events.corrupt.jsonl"), &source)?;
    let marker = RunEvent {
        sequence: events.len() as u64 + 1,
        at_ms: now_ms(),
        kind: "agent.session.replay-incomplete".to_owned(),
        payload: json!({
            "reason": "a corrupt event-log suffix was quarantined",
            "validBytes": valid_bytes,
            "totalBytes": source.len(),
        }),
        progress: None,
    };
    events.push(marker);
    let mut repaired = Vec::new();
    for event in &events {
        repaired.extend(serde_json::to_vec(event)?);
        repaired.push(b'\n');
    }
    write_confined_bytes_atomic(evidence_root, relative, &repaired)?;
    Ok(events)
}

fn repair_agent_turns_from_events(turns: &mut [AgentTurnSummary], events: &[RunEvent]) -> bool {
    let mut repaired = false;
    for event in events {
        if event.kind != "agent.turn.finished" {
            continue;
        }
        let Some(turn_id) = event.payload.get("turnId").and_then(JsonValue::as_str) else {
            continue;
        };
        let Some(turn) = turns.iter_mut().find(|turn| turn.id == turn_id) else {
            continue;
        };
        let outcome = event
            .payload
            .get("outcome")
            .and_then(JsonValue::as_str)
            .unwrap_or("failed");
        let status = match outcome {
            "completed" => AgentTurnStatus::Completed,
            "intervened" => AgentTurnStatus::Intervened,
            "aborted" | "cancelled" => AgentTurnStatus::Cancelled,
            _ => AgentTurnStatus::Failed,
        };
        if turn.status != status
            || turn.outcome.as_deref() != Some(outcome)
            || turn.finished_at_ms != Some(event.at_ms)
        {
            turn.status = status;
            turn.outcome = Some(outcome.to_owned());
            turn.finished_at_ms = Some(event.at_ms);
            turn.error = event
                .payload
                .get("error")
                .and_then(JsonValue::as_str)
                .map(str::to_owned);
            repaired = true;
        }
    }
    repaired
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

#[derive(Debug)]
struct CapturedFile {
    contents: Vec<u8>,
    permissions: fs::Permissions,
}

#[derive(Debug)]
struct CapturedTree {
    root_permissions: fs::Permissions,
    directories: BTreeMap<String, fs::Permissions>,
    files: BTreeMap<String, CapturedFile>,
}

fn redact_captured_tree(snapshot: &mut CapturedTree, secrets: &[Vec<u8>]) {
    for file in snapshot.files.values_mut() {
        file.contents = redact_evidence_bytes(&file.contents, secrets);
    }
}

fn captured_files_equal(before: Option<&CapturedFile>, after: Option<&CapturedFile>) -> bool {
    match (before, after) {
        (Some(before), Some(after)) => {
            before.contents == after.contents
                && permission_label(&before.permissions) == permission_label(&after.permissions)
        }
        (None, None) => true,
        _ => false,
    }
}

fn captured_tree_changes(
    initial: &CapturedTree,
    final_snapshot: &CapturedTree,
    secrets: &[Vec<u8>],
) -> Vec<JsonValue> {
    let mut changes = Vec::new();
    if permission_label(&initial.root_permissions)
        != permission_label(&final_snapshot.root_permissions)
    {
        changes.push(json!({
            "path": ".",
            "entryType": "directory",
            "kind": "mode-changed",
            "before": JsonValue::Null,
            "after": JsonValue::Null,
            "beforeMode": permission_label(&initial.root_permissions),
            "afterMode": permission_label(&final_snapshot.root_permissions),
        }));
    }

    let directory_paths = initial
        .directories
        .keys()
        .chain(final_snapshot.directories.keys())
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    for path in directory_paths {
        let before = initial.directories.get(&path);
        let after = final_snapshot.directories.get(&path);
        if before.map(permission_label) == after.map(permission_label) {
            continue;
        }
        changes.push(json!({
            "path": path,
            "entryType": "directory",
            "kind": match (before, after) {
                (None, Some(_)) => "created",
                (Some(_), None) => "deleted",
                (Some(_), Some(_)) => "mode-changed",
                (None, None) => unreachable!("a union path has an entry"),
            },
            "before": JsonValue::Null,
            "after": JsonValue::Null,
            "beforeMode": before.map(permission_label),
            "afterMode": after.map(permission_label),
        }));
    }

    let file_paths = initial
        .files
        .keys()
        .chain(final_snapshot.files.keys())
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    for path in file_paths {
        let before = initial.files.get(&path);
        let after = final_snapshot.files.get(&path);
        if captured_files_equal(before, after) {
            continue;
        }
        changes.push(json!({
            "path": path,
            "entryType": "file",
            "kind": match (before, after) {
                (None, Some(_)) => "created",
                (Some(_), None) => "deleted",
                (Some(before), Some(after)) if before.contents == after.contents => "mode-changed",
                _ => "modified",
            },
            "before": before.and_then(|file| redacted_evidence_text(&file.contents, secrets)),
            "after": after.and_then(|file| redacted_evidence_text(&file.contents, secrets)),
            "beforeMode": before.map(|file| permission_label(&file.permissions)),
            "afterMode": after.map(|file| permission_label(&file.permissions)),
        }));
    }
    changes
}

fn permission_label(permissions: &fs::Permissions) -> String {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        format!("{:04o}", permissions.mode() & 0o7777)
    }
    #[cfg(not(unix))]
    {
        if permissions.readonly() {
            "readonly".to_owned()
        } else {
            "writable".to_owned()
        }
    }
}

fn captured_tree_digest(snapshot: &CapturedTree) -> String {
    let mut hasher = Sha256::new();
    hash_permissions(&mut hasher, &snapshot.root_permissions);
    for (path, permissions) in &snapshot.directories {
        hasher.update(b"directory\0");
        hasher.update(path.as_bytes());
        hasher.update(b"\0");
        hash_permissions(&mut hasher, permissions);
    }
    for (path, file) in &snapshot.files {
        hasher.update(b"file\0");
        hasher.update(path.as_bytes());
        hasher.update(b"\0");
        hash_permissions(&mut hasher, &file.permissions);
        hasher.update((file.contents.len() as u64).to_le_bytes());
        hasher.update(&file.contents);
    }
    format!("sha256:{:x}", hasher.finalize())
}

fn hash_permissions(hasher: &mut Sha256, permissions: &fs::Permissions) {
    hasher.update([u8::from(permissions.readonly())]);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        hasher.update(permissions.mode().to_le_bytes());
    }
}

fn write_captured_tree(destination: &Path, snapshot: &CapturedTree) -> Result<(), RunError> {
    fs::create_dir(destination)?;
    for relative in snapshot.directories.keys() {
        fs::create_dir_all(confined_child(destination, relative)?)?;
    }
    for (relative, file) in &snapshot.files {
        let path = confined_child(destination, relative)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, &file.contents)?;
        fs::set_permissions(path, file.permissions.clone())?;
    }
    for (relative, permissions) in snapshot.directories.iter().rev() {
        fs::set_permissions(confined_child(destination, relative)?, permissions.clone())?;
    }
    fs::set_permissions(destination, snapshot.root_permissions.clone())?;
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
    Ok(capture_tree(root)?
        .files
        .into_iter()
        .map(|(path, file)| (path, file.contents))
        .collect())
}

fn capture_tree(root: &Path) -> Result<CapturedTree, RunError> {
    fn visit(
        root: &Path,
        directory: &Path,
        directories: &mut BTreeMap<String, fs::Permissions>,
        files: &mut BTreeMap<String, CapturedFile>,
        retained_bytes: &mut u64,
    ) -> Result<(), RunError> {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_symlink() {
                return Err(RunError::PathEscape(entry.path()));
            }
            if file_type.is_dir() {
                let entry_path = entry.path();
                let relative = relative_evidence_path(root, &entry_path)?;
                directories.insert(relative, entry.metadata()?.permissions());
                visit(root, &entry_path, directories, files, retained_bytes)?;
            } else if file_type.is_file() {
                let entry_path = entry.path();
                let relative = relative_evidence_path(root, &entry_path)?;
                let permissions = entry.metadata()?.permissions();
                let contents = read_evidence_file(&entry_path, retained_bytes)?;
                files.insert(
                    relative,
                    CapturedFile {
                        contents,
                        permissions,
                    },
                );
            } else {
                return Err(RunError::UnsupportedWorkspaceEntry(entry.path()));
            }
        }
        Ok(())
    }

    let metadata = fs::symlink_metadata(root)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(RunError::PathEscape(root.to_path_buf()));
    }
    let mut directories = BTreeMap::new();
    let mut files = BTreeMap::new();
    visit(root, root, &mut directories, &mut files, &mut 0)?;
    Ok(CapturedTree {
        root_permissions: metadata.permissions(),
        directories,
        files,
    })
}

fn relative_evidence_path(root: &Path, path: &Path) -> Result<String, RunError> {
    path.strip_prefix(root)
        .map_err(|_| RunError::PathEscape(path.to_path_buf()))?
        .to_str()
        .ok_or_else(|| RunError::UnsupportedWorkspaceEntry(path.to_path_buf()))
        .map(str::to_owned)
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
    fs::set_permissions(destination, metadata.permissions())?;
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

fn confined_evidence_components(
    relative: &Path,
    display_path: &Path,
) -> Result<Vec<OsString>, RunError> {
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
    Ok(components)
}

#[cfg(unix)]
fn open_confined_evidence_root(root: &Path) -> Result<rustix::fd::OwnedFd, RunError> {
    rustix::fs::open(
        root,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::CLOEXEC
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW,
        rustix::fs::Mode::empty(),
    )
    .map_err(|error| match error {
        rustix::io::Errno::LOOP | rustix::io::Errno::NOTDIR => {
            RunError::PathEscape(root.to_path_buf())
        }
        _ => io::Error::from(error).into(),
    })
}

#[cfg(unix)]
fn open_confined_evidence_directory_at(
    root: &rustix::fd::OwnedFd,
    components: Vec<OsString>,
    display_path: &Path,
) -> Result<rustix::fd::OwnedFd, RunError> {
    let mut directory = rustix::io::fcntl_dupfd_cloexec(root, 0).map_err(io::Error::from)?;
    for component in components {
        directory = rustix::fs::openat(
            &directory,
            component,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::CLOEXEC
                | rustix::fs::OFlags::DIRECTORY
                | rustix::fs::OFlags::NOFOLLOW,
            rustix::fs::Mode::empty(),
        )
        .map_err(|error| match error {
            rustix::io::Errno::LOOP | rustix::io::Errno::NOTDIR => {
                RunError::PathEscape(display_path.to_path_buf())
            }
            _ => io::Error::from(error).into(),
        })?;
    }
    Ok(directory)
}

#[cfg(unix)]
fn open_confined_evidence_parent(
    root: &AgentSessionEvidenceRoot,
    relative: &Path,
    display_path: &Path,
) -> Result<(rustix::fd::OwnedFd, OsString), RunError> {
    let mut components = confined_evidence_components(relative, display_path)?;
    let name = components
        .pop()
        .expect("confined evidence path has a final component");
    let directory = open_confined_evidence_directory_at(&root.directory, components, display_path)?;
    Ok((directory, name))
}

#[cfg(unix)]
fn create_confined_evidence_directory(
    root: &AgentSessionEvidenceRoot,
    relative: &Path,
) -> Result<rustix::fd::OwnedFd, RunError> {
    let display_path = root.display_path().join(relative);
    let (parent, name) = open_confined_evidence_parent(root, relative, &display_path)?;
    rustix::fs::mkdirat(
        &parent,
        &name,
        rustix::fs::Mode::RWXU | rustix::fs::Mode::RWXG | rustix::fs::Mode::RWXO,
    )
    .map_err(io::Error::from)?;
    rustix::fs::openat(
        &parent,
        &name,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::CLOEXEC
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW,
        rustix::fs::Mode::empty(),
    )
    .map_err(|error| match error {
        rustix::io::Errno::LOOP | rustix::io::Errno::NOTDIR => RunError::PathEscape(display_path),
        _ => io::Error::from(error).into(),
    })
}

#[cfg(not(unix))]
fn create_confined_evidence_directory(
    root: &AgentSessionEvidenceRoot,
    relative: &Path,
) -> Result<(), RunError> {
    Err(RunError::ConfinedReadUnavailable(
        root.display_path().join(relative),
    ))
}

#[cfg(unix)]
fn open_or_create_confined_directory_at(
    root: &rustix::fd::OwnedFd,
    relative: &Path,
    display_path: &Path,
) -> Result<rustix::fd::OwnedFd, RunError> {
    let components = if relative.as_os_str().is_empty() {
        Vec::new()
    } else {
        confined_evidence_components(relative, display_path)?
    };
    let mut directory = rustix::io::fcntl_dupfd_cloexec(root, 0).map_err(io::Error::from)?;
    for component in components {
        match rustix::fs::mkdirat(
            &directory,
            &component,
            rustix::fs::Mode::RWXU | rustix::fs::Mode::RWXG | rustix::fs::Mode::RWXO,
        ) {
            Ok(()) | Err(rustix::io::Errno::EXIST) => {}
            Err(error) => return Err(io::Error::from(error).into()),
        }
        directory = rustix::fs::openat(
            &directory,
            &component,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::CLOEXEC
                | rustix::fs::OFlags::DIRECTORY
                | rustix::fs::OFlags::NOFOLLOW,
            rustix::fs::Mode::empty(),
        )
        .map_err(|error| match error {
            rustix::io::Errno::LOOP | rustix::io::Errno::NOTDIR => {
                RunError::PathEscape(display_path.to_path_buf())
            }
            _ => io::Error::from(error).into(),
        })?;
    }
    Ok(directory)
}

#[cfg(unix)]
fn confined_fd_metadata(descriptor: &rustix::fd::OwnedFd) -> Result<fs::Metadata, RunError> {
    let duplicate = rustix::io::fcntl_dupfd_cloexec(descriptor, 0).map_err(io::Error::from)?;
    Ok(fs::File::from(duplicate).metadata()?)
}

#[cfg(unix)]
fn set_confined_fd_permissions(
    descriptor: &rustix::fd::OwnedFd,
    permissions: &fs::Permissions,
) -> Result<(), RunError> {
    let duplicate = rustix::io::fcntl_dupfd_cloexec(descriptor, 0).map_err(io::Error::from)?;
    fs::File::from(duplicate).set_permissions(permissions.clone())?;
    Ok(())
}

#[cfg(unix)]
fn write_confined_captured_tree(
    root: &AgentSessionEvidenceRoot,
    relative: &Path,
    snapshot: &CapturedTree,
) -> Result<(), RunError> {
    let display_path = root.display_path().join(relative);
    let destination = create_confined_evidence_directory(root, relative)?;
    for directory_relative in snapshot.directories.keys() {
        let directory_relative = Path::new(directory_relative);
        open_or_create_confined_directory_at(
            &destination,
            directory_relative,
            &display_path.join(directory_relative),
        )?;
    }
    for (file_relative, file) in &snapshot.files {
        let file_relative = Path::new(file_relative);
        let file_display = display_path.join(file_relative);
        let mut components = confined_evidence_components(file_relative, &file_display)?;
        let name = components
            .pop()
            .expect("captured file path has a final component");
        let parent_relative = components.iter().collect::<PathBuf>();
        let parent =
            open_or_create_confined_directory_at(&destination, &parent_relative, &file_display)?;
        let opened = rustix::fs::openat(
            &parent,
            &name,
            rustix::fs::OFlags::WRONLY
                | rustix::fs::OFlags::CREATE
                | rustix::fs::OFlags::EXCL
                | rustix::fs::OFlags::CLOEXEC
                | rustix::fs::OFlags::NOFOLLOW,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )
        .map_err(|error| match error {
            rustix::io::Errno::LOOP | rustix::io::Errno::NOTDIR => {
                RunError::PathEscape(file_display)
            }
            _ => io::Error::from(error).into(),
        })?;
        let opened = fs::File::from(opened);
        (&opened).write_all(&file.contents)?;
        opened.set_permissions(file.permissions.clone())?;
    }
    for (directory_relative, permissions) in snapshot.directories.iter().rev() {
        let directory_relative = Path::new(directory_relative);
        let directory = open_confined_evidence_directory_at(
            &destination,
            confined_evidence_components(
                directory_relative,
                &display_path.join(directory_relative),
            )?,
            &display_path.join(directory_relative),
        )?;
        set_confined_fd_permissions(&directory, permissions)?;
    }
    set_confined_fd_permissions(&destination, &snapshot.root_permissions)
}

#[cfg(not(unix))]
fn write_confined_captured_tree(
    root: &AgentSessionEvidenceRoot,
    relative: &Path,
    _snapshot: &CapturedTree,
) -> Result<(), RunError> {
    Err(RunError::ConfinedReadUnavailable(
        root.display_path().join(relative),
    ))
}

#[cfg(unix)]
#[allow(clippy::too_many_lines)]
fn capture_confined_tree(
    root: &AgentSessionEvidenceRoot,
    relative: &Path,
) -> Result<CapturedTree, RunError> {
    #[allow(clippy::too_many_lines)]
    fn visit(
        directory: &rustix::fd::OwnedFd,
        display_root: &Path,
        relative: &Path,
        directories: &mut BTreeMap<String, fs::Permissions>,
        files: &mut BTreeMap<String, CapturedFile>,
        retained_bytes: &mut u64,
    ) -> Result<(), RunError> {
        let entries = rustix::fs::Dir::read_from(directory).map_err(io::Error::from)?;
        for entry in entries {
            let entry = entry.map_err(io::Error::from)?;
            let bytes = entry.file_name().to_bytes();
            if bytes == b"." || bytes == b".." {
                continue;
            }
            let name = OsString::from_vec(bytes.to_vec());
            let entry_relative = relative.join(&name);
            let entry_display = display_root.join(&entry_relative);
            let stat =
                match rustix::fs::statat(directory, &name, rustix::fs::AtFlags::SYMLINK_NOFOLLOW) {
                    Ok(stat) => stat,
                    Err(rustix::io::Errno::NOENT) => continue,
                    Err(error) => return Err(io::Error::from(error).into()),
                };
            match rustix::fs::FileType::from_raw_mode(stat.st_mode) {
                rustix::fs::FileType::Directory => {
                    let opened = rustix::fs::openat(
                        directory,
                        &name,
                        rustix::fs::OFlags::RDONLY
                            | rustix::fs::OFlags::CLOEXEC
                            | rustix::fs::OFlags::DIRECTORY
                            | rustix::fs::OFlags::NOFOLLOW,
                        rustix::fs::Mode::empty(),
                    )
                    .map_err(|error| match error {
                        rustix::io::Errno::LOOP | rustix::io::Errno::NOTDIR => {
                            RunError::PathEscape(entry_display.clone())
                        }
                        _ => io::Error::from(error).into(),
                    })?;
                    let relative_string = entry_relative
                        .to_str()
                        .ok_or_else(|| RunError::UnsupportedWorkspaceEntry(entry_display.clone()))?
                        .to_owned();
                    directories.insert(
                        relative_string,
                        confined_fd_metadata(&opened)?.permissions(),
                    );
                    visit(
                        &opened,
                        display_root,
                        &entry_relative,
                        directories,
                        files,
                        retained_bytes,
                    )?;
                }
                rustix::fs::FileType::RegularFile => {
                    let opened = rustix::fs::openat(
                        directory,
                        &name,
                        rustix::fs::OFlags::RDONLY
                            | rustix::fs::OFlags::CLOEXEC
                            | rustix::fs::OFlags::NONBLOCK
                            | rustix::fs::OFlags::NOFOLLOW,
                        rustix::fs::Mode::empty(),
                    )
                    .map_err(|error| match error {
                        rustix::io::Errno::LOOP | rustix::io::Errno::NOTDIR => {
                            RunError::PathEscape(entry_display.clone())
                        }
                        _ => io::Error::from(error).into(),
                    })?;
                    let opened = fs::File::from(opened);
                    let metadata = opened.metadata()?;
                    if !metadata.is_file() {
                        return Err(RunError::PathEscape(entry_display));
                    }
                    validate_evidence_file(&entry_display, &metadata, retained_bytes)?;
                    let capacity = usize::try_from(metadata.len()).map_err(|_| {
                        RunError::EvidenceLimit("file size does not fit this platform".to_owned())
                    })?;
                    let mut contents = Vec::with_capacity(capacity);
                    opened
                        .take(MAX_EVIDENCE_FILE_BYTES + 1)
                        .read_to_end(&mut contents)?;
                    if contents.len() as u64 > MAX_EVIDENCE_FILE_BYTES {
                        return Err(RunError::EvidenceLimit(format!(
                            "{} grew beyond the per-file limit while being read",
                            entry_display.display()
                        )));
                    }
                    let relative_string = entry_relative
                        .to_str()
                        .ok_or_else(|| RunError::UnsupportedWorkspaceEntry(entry_display.clone()))?
                        .to_owned();
                    files.insert(
                        relative_string,
                        CapturedFile {
                            contents,
                            permissions: metadata.permissions(),
                        },
                    );
                }
                rustix::fs::FileType::Symlink => {
                    return Err(RunError::PathEscape(entry_display));
                }
                _ => return Err(RunError::UnsupportedWorkspaceEntry(entry_display)),
            }
        }
        Ok(())
    }

    let display_path = root.display_path().join(relative);
    let directory = open_confined_evidence_directory_at(
        &root.directory,
        confined_evidence_components(relative, &display_path)?,
        &display_path,
    )?;
    let root_permissions = confined_fd_metadata(&directory)?.permissions();
    let mut directories = BTreeMap::new();
    let mut files = BTreeMap::new();
    visit(
        &directory,
        &display_path,
        Path::new(""),
        &mut directories,
        &mut files,
        &mut 0,
    )?;
    Ok(CapturedTree {
        root_permissions,
        directories,
        files,
    })
}

#[cfg(not(unix))]
fn capture_confined_tree(
    root: &AgentSessionEvidenceRoot,
    relative: &Path,
) -> Result<CapturedTree, RunError> {
    Err(RunError::ConfinedReadUnavailable(
        root.display_path().join(relative),
    ))
}

#[cfg(unix)]
fn remove_confined_evidence_entry(
    root: &AgentSessionEvidenceRoot,
    relative: &Path,
) -> Result<bool, RunError> {
    fn remove_at(
        directory: &rustix::fd::OwnedFd,
        name: &OsStr,
        display_path: &Path,
    ) -> Result<bool, RunError> {
        let stat = match rustix::fs::statat(directory, name, rustix::fs::AtFlags::SYMLINK_NOFOLLOW)
        {
            Ok(stat) => stat,
            Err(rustix::io::Errno::NOENT) => return Ok(false),
            Err(error) => return Err(io::Error::from(error).into()),
        };
        if rustix::fs::FileType::from_raw_mode(stat.st_mode) == rustix::fs::FileType::Directory {
            let opened = rustix::fs::openat(
                directory,
                name,
                rustix::fs::OFlags::RDONLY
                    | rustix::fs::OFlags::CLOEXEC
                    | rustix::fs::OFlags::DIRECTORY
                    | rustix::fs::OFlags::NOFOLLOW,
                rustix::fs::Mode::empty(),
            )
            .map_err(|error| match error {
                rustix::io::Errno::LOOP | rustix::io::Errno::NOTDIR => {
                    RunError::PathEscape(display_path.to_path_buf())
                }
                _ => io::Error::from(error).into(),
            })?;
            let entries = rustix::fs::Dir::read_from(&opened).map_err(io::Error::from)?;
            let mut names = Vec::new();
            for entry in entries {
                let entry = entry.map_err(io::Error::from)?;
                let bytes = entry.file_name().to_bytes();
                if bytes != b"." && bytes != b".." {
                    names.push(OsString::from_vec(bytes.to_vec()));
                }
            }
            for child in names {
                remove_at(&opened, &child, &display_path.join(&child))?;
            }
            rustix::fs::unlinkat(directory, name, rustix::fs::AtFlags::REMOVEDIR)
                .map_err(io::Error::from)?;
        } else {
            rustix::fs::unlinkat(directory, name, rustix::fs::AtFlags::empty())
                .map_err(io::Error::from)?;
        }
        Ok(true)
    }

    let display_path = root.display_path().join(relative);
    let (directory, name) = open_confined_evidence_parent(root, relative, &display_path)?;
    remove_at(&directory, &name, &display_path)
}

#[cfg(not(unix))]
fn remove_confined_evidence_entry(
    root: &AgentSessionEvidenceRoot,
    relative: &Path,
) -> Result<bool, RunError> {
    Err(RunError::ConfinedReadUnavailable(
        root.display_path().join(relative),
    ))
}

#[cfg(unix)]
fn write_confined_bytes_atomic(
    root: &AgentSessionEvidenceRoot,
    relative: &Path,
    bytes: &[u8],
) -> Result<(), RunError> {
    let display_path = root.display_path().join(relative);
    let (directory, name) = open_confined_evidence_parent(root, relative, &display_path)?;
    match rustix::fs::statat(&directory, &name, rustix::fs::AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat)
            if rustix::fs::FileType::from_raw_mode(stat.st_mode)
                != rustix::fs::FileType::RegularFile =>
        {
            return Err(RunError::PathEscape(display_path));
        }
        Ok(_) | Err(rustix::io::Errno::NOENT) => {}
        Err(error) => return Err(io::Error::from(error).into()),
    }
    let temporary = OsString::from(format!(".agent-lab-{}-{}.tmp", now_ms(), random_suffix()));
    let opened = rustix::fs::openat(
        &directory,
        &temporary,
        rustix::fs::OFlags::WRONLY
            | rustix::fs::OFlags::CREATE
            | rustix::fs::OFlags::EXCL
            | rustix::fs::OFlags::CLOEXEC
            | rustix::fs::OFlags::NOFOLLOW,
        rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
    )
    .map_err(io::Error::from)?;
    let write_result = fs::File::from(opened).write_all(bytes);
    if let Err(error) = write_result {
        let _ = rustix::fs::unlinkat(&directory, &temporary, rustix::fs::AtFlags::empty());
        return Err(error.into());
    }
    if let Err(error) = rustix::fs::renameat(&directory, &temporary, &directory, &name) {
        let _ = rustix::fs::unlinkat(&directory, &temporary, rustix::fs::AtFlags::empty());
        return Err(io::Error::from(error).into());
    }
    Ok(())
}

#[cfg(not(unix))]
fn write_confined_bytes_atomic(
    root: &AgentSessionEvidenceRoot,
    relative: &Path,
    _bytes: &[u8],
) -> Result<(), RunError> {
    Err(RunError::ConfinedReadUnavailable(
        root.display_path().join(relative),
    ))
}

fn write_confined_json_atomic(
    root: &AgentSessionEvidenceRoot,
    relative: &Path,
    value: &JsonValue,
) -> Result<(), RunError> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    write_confined_bytes_atomic(root, relative, &bytes)
}

#[cfg(unix)]
fn append_confined_bytes(
    root: &AgentSessionEvidenceRoot,
    relative: &Path,
    bytes: &[u8],
) -> Result<(), RunError> {
    let display_path = root.display_path().join(relative);
    let (directory, name) = open_confined_evidence_parent(root, relative, &display_path)?;
    let opened = rustix::fs::openat(
        &directory,
        &name,
        rustix::fs::OFlags::WRONLY
            | rustix::fs::OFlags::APPEND
            | rustix::fs::OFlags::CREATE
            | rustix::fs::OFlags::CLOEXEC
            | rustix::fs::OFlags::NONBLOCK
            | rustix::fs::OFlags::NOFOLLOW,
        rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
    )
    .map_err(|error| match error {
        rustix::io::Errno::LOOP | rustix::io::Errno::NOTDIR => {
            RunError::PathEscape(display_path.clone())
        }
        _ => io::Error::from(error).into(),
    })?;
    let mut file = fs::File::from(opened);
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(RunError::PathEscape(display_path));
    }
    reject_multiply_linked_file(&display_path, &metadata)?;
    file.write_all(bytes)?;
    Ok(())
}

#[cfg(not(unix))]
fn append_confined_bytes(
    root: &AgentSessionEvidenceRoot,
    relative: &Path,
    _bytes: &[u8],
) -> Result<(), RunError> {
    Err(RunError::ConfinedReadUnavailable(
        root.display_path().join(relative),
    ))
}

#[cfg(unix)]
fn remove_confined_evidence_file(
    root: &AgentSessionEvidenceRoot,
    relative: &Path,
) -> Result<bool, RunError> {
    let display_path = root.display_path().join(relative);
    let (directory, name) = open_confined_evidence_parent(root, relative, &display_path)?;
    match rustix::fs::unlinkat(&directory, &name, rustix::fs::AtFlags::empty()) {
        Ok(()) => Ok(true),
        Err(rustix::io::Errno::NOENT) => Ok(false),
        Err(rustix::io::Errno::ISDIR | rustix::io::Errno::PERM) => {
            Err(RunError::PathEscape(display_path))
        }
        Err(error) => Err(io::Error::from(error).into()),
    }
}

#[cfg(not(unix))]
fn remove_confined_evidence_file(
    root: &AgentSessionEvidenceRoot,
    relative: &Path,
) -> Result<bool, RunError> {
    Err(RunError::ConfinedReadUnavailable(
        root.display_path().join(relative),
    ))
}

#[cfg(unix)]
fn rename_confined_evidence_file(
    root: &AgentSessionEvidenceRoot,
    source: &Path,
    destination: &Path,
) -> Result<(), RunError> {
    let source_display = root.display_path().join(source);
    let destination_display = root.display_path().join(destination);
    let mut source_components = confined_evidence_components(source, &source_display)?;
    let source_name = source_components
        .pop()
        .expect("confined evidence source has a final component");
    let mut destination_components =
        confined_evidence_components(destination, &destination_display)?;
    let destination_name = destination_components
        .pop()
        .expect("confined evidence destination has a final component");
    if source_components != destination_components {
        return Err(RunError::PathEscape(destination_display));
    }
    let directory =
        open_confined_evidence_directory_at(&root.directory, source_components, &source_display)?;
    rustix::fs::renameat(&directory, &source_name, &directory, &destination_name).map_err(|error| {
        match error {
            rustix::io::Errno::LOOP | rustix::io::Errno::NOTDIR => {
                RunError::PathEscape(destination_display)
            }
            _ => io::Error::from(error).into(),
        }
    })
}

#[cfg(not(unix))]
fn rename_confined_evidence_file(
    root: &AgentSessionEvidenceRoot,
    _source: &Path,
    destination: &Path,
) -> Result<(), RunError> {
    Err(RunError::ConfinedReadUnavailable(
        root.display_path().join(destination),
    ))
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
    let directory = open_confined_evidence_root(root)?;
    read_optional_confined_file_at(&directory, relative, display_path)
}

#[cfg(unix)]
fn read_optional_agent_evidence_file(
    root: &AgentSessionEvidenceRoot,
    relative: &Path,
    display_path: &Path,
) -> Result<Option<Vec<u8>>, RunError> {
    read_optional_confined_file_at(&root.directory, relative, display_path)
}

#[cfg(unix)]
fn read_optional_confined_file_at(
    root: &rustix::fd::OwnedFd,
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
    let mut directory = rustix::io::fcntl_dupfd_cloexec(root, 0).map_err(io::Error::from)?;
    for (index, component) in components.iter().enumerate() {
        let is_file = index + 1 == components.len();
        let mut flags =
            rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::CLOEXEC | rustix::fs::OFlags::NOFOLLOW;
        if is_file {
            flags |= rustix::fs::OFlags::NONBLOCK;
        } else {
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
    _root: &Path,
    _relative: &Path,
    display_path: &Path,
) -> Result<Option<Vec<u8>>, RunError> {
    read_optional_confined_file_without_handle_relative_support(display_path)
}

#[cfg(not(unix))]
fn read_optional_agent_evidence_file(
    _root: &AgentSessionEvidenceRoot,
    _relative: &Path,
    display_path: &Path,
) -> Result<Option<Vec<u8>>, RunError> {
    read_optional_confined_file_without_handle_relative_support(display_path)
}

#[cfg(any(not(unix), test))]
fn read_optional_confined_file_without_handle_relative_support(
    display_path: &Path,
) -> Result<Option<Vec<u8>>, RunError> {
    // Checking a path and reopening it cannot enforce confinement across a
    // concurrent symlink or reparse-point replacement. Until a target has a
    // handle-relative implementation, report missing outputs and fail closed
    // for existing ones.
    match fs::symlink_metadata(display_path) {
        Ok(_) => Err(RunError::ConfinedReadUnavailable(
            display_path.to_path_buf(),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn require_race_free_confined_reads(supported: bool) -> Result<(), RunError> {
    if supported {
        Ok(())
    } else {
        Err(RunError::UnsupportedPlatform)
    }
}

fn redact_transcript(mut transcript: DriverTranscript, secrets: &[Vec<u8>]) -> DriverTranscript {
    redact_assistant_transcript_records(&mut transcript.driver_records, secrets);
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

fn redact_assistant_transcript_records(records: &mut [Vec<u8>], secrets: &[Vec<u8>]) {
    let mut redactors = HashMap::<(String, String), AssistantObservationRedactor<'_>>::new();
    for record in records {
        let Ok(mut value) = serde_json::from_slice::<JsonValue>(record) else {
            continue;
        };
        let event_type = value
            .get("eventType")
            .and_then(JsonValue::as_str)
            .map(str::to_owned);
        if matches!(
            event_type.as_deref(),
            Some(
                agent_lab_driver_protocol::ASSISTANT_DELTA_EVENT
                    | agent_lab_driver_protocol::ASSISTANT_COMPLETED_EVENT
            )
        ) {
            let key = value
                .get("sessionId")
                .and_then(JsonValue::as_str)
                .zip(value.get("turnId").and_then(JsonValue::as_str))
                .map(|(session_id, turn_id)| (session_id.to_owned(), turn_id.to_owned()));
            let sanitized = key.and_then(|key| {
                let observation = TurnObservation::parse(
                    event_type
                        .as_deref()
                        .expect("assistant event type is present"),
                    value.get("payload").unwrap_or(&JsonValue::Null),
                )
                .ok()
                .flatten()?;
                redactors
                    .entry(key)
                    .or_insert_with(|| AssistantObservationRedactor::new(secrets))
                    .redact(observation)
                    .ok()?
                    .into_iter()
                    .rev()
                    .find(|observation| observation.event_type() == event_type.as_deref().unwrap())
            });
            if let Some(payload) = value.get_mut("payload") {
                *payload = sanitized.map_or_else(
                    || {
                        let mut payload = payload.clone();
                        match &mut payload {
                            JsonValue::Object(payload) => {
                                payload.insert(
                                    "text".to_owned(),
                                    JsonValue::String("[REDACTED]".to_owned()),
                                );
                            }
                            _ => payload = json!({ "text": "[REDACTED]" }),
                        }
                        payload
                    },
                    |observation| observation.payload(),
                );
            }
        }
        if value.get("type").and_then(JsonValue::as_str) == Some("turn.finished")
            && let Some(key) = value
                .get("sessionId")
                .and_then(JsonValue::as_str)
                .zip(value.get("turnId").and_then(JsonValue::as_str))
                .map(|(session_id, turn_id)| (session_id.to_owned(), turn_id.to_owned()))
        {
            redactors.remove(&key);
        }
        if let Ok(mut sanitized) = serde_json::to_vec(&value) {
            sanitized.push(b'\n');
            *record = sanitized;
        }
    }
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
    #[error("unknown agent session: {0}")]
    UnknownAgentSession(String),
    #[error("run is not ready for an attached terminal: {0}")]
    RunUnavailable(String),
    #[error("model access is not ready: {0}")]
    ModelAccessUnavailable(String),
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
    #[error("race-free confined file reads are unavailable on this platform: {0}")]
    ConfinedReadUnavailable(PathBuf),
    #[error(
        "the run controller requires race-free confined output reads, which are not implemented for this platform"
    )]
    UnsupportedPlatform,
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
            RunError::UnknownRun(_)
            | RunError::UnknownScenario(_)
            | RunError::UnknownAgentSession(_) => StatusCode::NOT_FOUND,
            RunError::RunUnavailable(_) => StatusCode::CONFLICT,
            RunError::ModelAccessUnavailable(_) => StatusCode::PRECONDITION_FAILED,
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
            progress: None,
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

    fn test_agent_turn(status: AgentTurnStatus) -> AgentTurnSummary {
        AgentTurnSummary {
            id: "turn-1".to_owned(),
            session_id: "session-1".to_owned(),
            prompt: "test prompt".to_owned(),
            input: None,
            source_revision: "sha256:test".to_owned(),
            capability_revisions: BTreeMap::new(),
            status,
            started_at_ms: 1,
            finished_at_ms: None,
            outcome: None,
            error: None,
            human_intervention_at_ms: None,
        }
    }

    fn test_agent_session_state(bundle_dir: &Path) -> AgentSessionState {
        let evidence_root = AgentSessionEvidenceRoot::open(bundle_dir.to_path_buf())
            .expect("evidence root should open");
        let (sender, _) = broadcast::channel(8);
        AgentSessionState {
            summary: Mutex::new(AgentSessionSummary {
                id: "session-1".to_owned(),
                workspace_id: "workspace-1".to_owned(),
                harness_id: "fixture".to_owned(),
                model_profile_id: "test".to_owned(),
                model_id: "fixture/test".to_owned(),
                status: AgentSessionStatus::Running,
                active: true,
                created_at_ms: 1,
                updated_at_ms: 1,
                turn_count: 1,
                error: None,
            }),
            turns: Mutex::new(vec![test_agent_turn(AgentTurnStatus::Running)]),
            events: Mutex::new(Vec::new()),
            sender,
            commands: Mutex::new(None),
            lifecycle_cancel: CancellationToken::new(),
            turn_cancel: Mutex::new(None),
            actor: Mutex::new(AgentActorRegistration {
                complete: true,
                handle: None,
            }),
            actor_registered: Condvar::new(),
            evidence_error: Mutex::new(None),
            evidence_root,
            secret_values: Mutex::new(Vec::new()),
        }
    }

    fn observation_ready_event(sequence: u64) -> RunEvent {
        event(
            sequence,
            "agent.session.ready",
            json!({ "driver": { "features": [TURN_OBSERVATIONS_FEATURE] } }),
        )
    }

    #[test]
    fn agent_progress_projects_portable_and_controller_events_with_provenance() {
        let portable = event(
            7,
            "observation.progress",
            json!({
                "sessionId": "session-1",
                "turnId": "turn-1",
                "event": {
                    "phase": "reasoning",
                    "detail": "  Inspecting\n the catalog  ",
                    "source": "fixture"
                }
            }),
        );
        assert_eq!(
            project_agent_progress(&portable),
            Some(AgentProgressProjection {
                phase: ProgressPhase::Reasoning,
                detail: Some("Inspecting the catalog".to_owned()),
                source: Some("fixture".to_owned()),
                source_event_sequence: 7,
                source_event_type: "observation.progress".to_owned(),
            })
        );

        let capability = event(
            8,
            "mcp.tool.started",
            json!({
                "sessionId": "session-1",
                "turnId": "turn-1",
                "source": "catalog",
                "name": "list"
            }),
        );
        assert_eq!(
            project_agent_progress(&capability),
            Some(AgentProgressProjection {
                phase: ProgressPhase::Acting,
                detail: Some("catalog · list".to_owned()),
                source: Some("mcp".to_owned()),
                source_event_sequence: 8,
                source_event_type: "mcp.tool.started".to_owned(),
            })
        );
    }

    #[test]
    fn opaque_v0_thoughts_remain_raw_evidence_not_progress_detail() {
        let event = event(
            9,
            "v0.task-thinking-v1",
            json!({
                "sessionId": "session-1",
                "turnId": "turn-1",
                "thought": "private\u{001b}]52;c;clipboard payload\u{0007} details"
            }),
        );
        let projection = project_agent_progress(&event).unwrap();
        assert_eq!(projection.phase, ProgressPhase::Reasoning);
        assert_eq!(projection.detail.as_deref(), Some("Model is reasoning"));
        assert!(!projection.detail.unwrap().contains("private"));
    }

    #[test]
    fn repeated_agent_progress_is_deduplicated_until_the_phase_changes() {
        let root = temporary_root("progress-deduplication");
        let state = test_agent_session_state(&root);
        let payload = || {
            json!({
                "sessionId": "session-1",
                "turnId": "turn-1",
                "messageId": "message-1",
                "text": "delta"
            })
        };
        record_agent_event(&state, "observation.assistant.delta", payload()).unwrap();
        record_agent_event(&state, "observation.assistant.delta", payload()).unwrap();
        record_agent_event(
            &state,
            "mcp.tool.started",
            json!({
                "sessionId": "session-1",
                "turnId": "turn-1",
                "source": "catalog",
                "name": "list"
            }),
        )
        .unwrap();
        record_agent_event(&state, "observation.assistant.delta", payload()).unwrap();

        let events = lock(&state.events);
        assert!(events[0].progress.is_some());
        assert!(events[1].progress.is_none());
        assert_eq!(
            events[2].progress.as_ref().map(|progress| progress.phase),
            Some(ProgressPhase::Acting)
        );
        assert_eq!(
            events[3].progress.as_ref().map(|progress| progress.phase),
            Some(ProgressPhase::Responding)
        );
        drop(events);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn recent_driver_progress_supersedes_adjacent_generic_fallbacks() {
        let root = temporary_root("progress-precedence");
        let state = test_agent_session_state(&root);
        record_agent_event(
            &state,
            "observation.progress",
            json!({
                "sessionId": "session-1",
                "turnId": "turn-1",
                "event": {
                    "phase": "responding",
                    "detail": "Receiving model response",
                    "source": "fixture"
                }
            }),
        )
        .unwrap();
        record_agent_event(
            &state,
            "model.message.delta",
            json!({
                "sessionId": "session-1",
                "turnId": "turn-1",
                "event": { "messageDelta": "hello" }
            }),
        )
        .unwrap();
        record_agent_event(
            &state,
            "observation.assistant.delta",
            json!({
                "sessionId": "session-1",
                "turnId": "turn-1",
                "event": { "messageId": "message-1", "text": "hello" }
            }),
        )
        .unwrap();

        let events = lock(&state.events);
        assert!(events[0].progress.is_some());
        assert!(events[1].progress.is_none());
        assert!(events[2].progress.is_none());
        drop(events);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn legacy_run_events_without_progress_still_replay() {
        let event: RunEvent = serde_json::from_value(json!({
            "sequence": 1,
            "atMs": 2,
            "type": "agent.turn.started",
            "payload": { "turnId": "turn-1" }
        }))
        .unwrap();
        assert!(event.progress.is_none());
    }

    #[test]
    fn assistant_projection_rejects_events_after_completion_and_incomplete_messages() {
        let turn = test_agent_turn(AgentTurnStatus::Completed);
        let delta_after_completion = vec![
            observation_ready_event(1),
            event(
                2,
                "observation.assistant.completed",
                json!({
                    "turnId": "turn-1",
                    "messageId": "message-1",
                    "text": "complete",
                }),
            ),
            event(
                3,
                "observation.assistant.delta",
                json!({
                    "turnId": "turn-1",
                    "messageId": "message-1",
                    "text": "late",
                }),
            ),
        ];
        let error = build_agent_turn_presentation_from_events(&delta_after_completion, &turn, &[])
            .unwrap_err();
        assert!(error.to_string().contains("delta arrived after completion"));

        let incomplete_second_message = vec![
            observation_ready_event(1),
            event(
                2,
                "observation.assistant.completed",
                json!({
                    "turnId": "turn-1",
                    "messageId": "message-1",
                    "text": "first",
                }),
            ),
            event(
                3,
                "observation.assistant.delta",
                json!({
                    "turnId": "turn-1",
                    "messageId": "message-2",
                    "text": "unfinished",
                }),
            ),
            event(
                4,
                "agent.turn.finished",
                json!({ "turnId": "turn-1", "workspaceDiff": { "changes": [] } }),
            ),
        ];
        let presentation =
            build_agent_turn_presentation_from_events(&incomplete_second_message, &turn, &[])
                .unwrap();
        assert_eq!(presentation.messages.len(), 2);
        assert!(!presentation.messages[1].complete);
        let error =
            validate_agent_turn_presentation(&turn, &presentation, &incomplete_second_message)
                .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("without an authoritative assistant response")
        );
    }

    #[test]
    fn malformed_assistant_sequence_is_rejected_before_terminal_event_persistence() {
        let root = temporary_root("malformed-agent-terminal-prevalidation");
        let bundle = root.join("agent-session");
        fs::create_dir_all(bundle.join("turns/turn-1")).unwrap();
        let session = test_agent_session_state(&bundle);
        record_agent_event(
            &session,
            "agent.session.ready",
            json!({ "driver": { "features": [TURN_OBSERVATIONS_FEATURE] } }),
        )
        .unwrap();
        record_agent_event(
            &session,
            "observation.assistant.completed",
            json!({
                "turnId": "turn-1",
                "messageId": "message-1",
                "text": "complete",
            }),
        )
        .unwrap();
        record_agent_event(
            &session,
            "observation.assistant.delta",
            json!({
                "turnId": "turn-1",
                "messageId": "message-1",
                "text": "late",
            }),
        )
        .unwrap();

        let workspace = test_run_state(&root.join("run"));
        let error = record_finished_agent_turn_event(
            &session,
            &workspace,
            "turn-1",
            AgentTurnStatus::Completed,
            json!({
                "turnId": "turn-1",
                "outcome": "completed",
                "workspaceDiff": { "changes": [] },
            }),
        )
        .unwrap_err();
        assert!(error.to_string().contains("delta arrived after completion"));
        assert!(
            lock(&session.events)
                .iter()
                .all(|event| event.kind != "agent.turn.finished")
        );
        assert!(
            !fs::read_to_string(bundle.join("events.jsonl"))
                .unwrap()
                .contains("agent.turn.finished")
        );
        assert!(!bundle.join("turns/turn-1/presentation.json").exists());

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn split_secret_is_absent_from_live_raw_and_completed_answer_projections() {
        let secrets = vec![b"credential-token".to_vec()];
        let mut redactor = AssistantObservationRedactor::new(&secrets);
        let mut observations = Vec::new();
        for text in ["safe cred", "ential-", "token tail"] {
            observations.extend(
                redactor
                    .redact(TurnObservation::AssistantDelta(
                        agent_lab_driver_protocol::AssistantDeltaObservation {
                            message_id: "message-1".to_owned(),
                            text: text.to_owned(),
                        },
                    ))
                    .unwrap(),
            );
        }
        observations.extend(
            redactor
                .redact(TurnObservation::AssistantCompleted(
                    agent_lab_driver_protocol::AssistantCompletedObservation {
                        message_id: "message-1".to_owned(),
                        text: "safe credential-token tail".to_owned(),
                    },
                ))
                .unwrap(),
        );
        assert!(observations.iter().all(|observation| {
            !matches!(
                observation,
                TurnObservation::AssistantDelta(delta) if delta.text.is_empty()
            )
        }));

        let mut events = vec![observation_ready_event(1)];
        for (index, observation) in observations.iter().enumerate() {
            events.push(event(
                index as u64 + 2,
                observation.event_type(),
                json!({
                    "turnId": "turn-1",
                    "event": observation.payload(),
                }),
            ));
        }
        events.push(event(
            events.len() as u64 + 1,
            "agent.turn.finished",
            json!({
                "turnId": "turn-1",
                "outcome": "completed",
                "workspaceDiff": { "changes": [] },
            }),
        ));

        let raw_texts = events
            .iter()
            .filter_map(|event| {
                event
                    .payload
                    .get("event")
                    .and_then(|payload| payload.get("text"))
                    .and_then(JsonValue::as_str)
            })
            .collect::<String>();
        assert!(!raw_texts.contains("credential-token"));
        assert!(raw_texts.contains("[REDACTED]"));

        let presentation = build_agent_turn_presentation_from_events(
            &events,
            &test_agent_turn(AgentTurnStatus::Completed),
            &secrets,
        )
        .unwrap();
        assert_eq!(
            presentation.response.as_deref(),
            Some("safe [REDACTED] tail")
        );
        assert!(
            presentation
                .messages
                .iter()
                .all(|message| !message.text.contains("credential-token"))
        );
    }

    #[test]
    fn split_secret_is_flushed_safely_into_a_failed_partial_answer() {
        let root = temporary_root("failed-split-secret-answer");
        let bundle = root.join("agent-session");
        fs::create_dir_all(bundle.join("turns/turn-1")).unwrap();
        let secrets = vec![b"credential-token".to_vec()];
        let session = test_agent_session_state(&bundle);
        *lock(&session.secret_values) = secrets.clone();
        let mut live = session.sender.subscribe();
        record_agent_event(
            &session,
            "agent.session.ready",
            json!({ "driver": { "features": [TURN_OBSERVATIONS_FEATURE] } }),
        )
        .unwrap();
        let mut redactor = AssistantObservationRedactor::new(&secrets);
        for text in ["before credential-", "token after"] {
            for observation in redactor
                .redact(TurnObservation::AssistantDelta(
                    agent_lab_driver_protocol::AssistantDeltaObservation {
                        message_id: "message-1".to_owned(),
                        text: text.to_owned(),
                    },
                ))
                .unwrap()
            {
                record_agent_event(
                    &session,
                    observation.event_type(),
                    json!({
                        "sessionId": "session-1",
                        "turnId": "turn-1",
                        "event": observation.payload(),
                    }),
                )
                .unwrap();
            }
        }
        flush_pending_assistant_deltas(&session, "session-1", "turn-1", &mut redactor).unwrap();
        let workspace = test_run_state(&root.join("run"));
        record_finished_agent_turn_event(
            &session,
            &workspace,
            "turn-1",
            AgentTurnStatus::Failed,
            json!({
                "sessionId": "session-1",
                "turnId": "turn-1",
                "outcome": "failed",
                "workspaceDiff": { "changes": [] },
            }),
        )
        .unwrap();

        let events = lock(&session.events).clone();
        let live_events = std::iter::from_fn(|| live.try_recv().ok()).collect::<Vec<_>>();
        assert_eq!(
            live_events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            events
                .iter()
                .filter(|event| event.kind != "agent.turn.finished")
                .map(|event| event.sequence)
                .collect::<Vec<_>>()
        );
        let raw_texts = events
            .iter()
            .filter_map(|event| {
                event
                    .payload
                    .get("event")
                    .and_then(|payload| payload.get("text"))
                    .and_then(JsonValue::as_str)
            })
            .collect::<String>();
        assert_eq!(raw_texts, "before [REDACTED] after");
        assert!(
            !serde_json::to_string(&live_events)
                .unwrap()
                .contains("credential-token")
        );

        let failed_turn = test_agent_turn(AgentTurnStatus::Failed);
        let presentation =
            load_or_build_agent_turn_presentation(&session, &workspace, &failed_turn).unwrap();
        assert_eq!(
            presentation.response.as_deref(),
            Some("before [REDACTED] after")
        );
        assert_eq!(
            presentation.completeness.assistant_output,
            AgentPresentationCompleteness::Partial
        );
        let durable = fs::read_to_string(bundle.join("events.jsonl")).unwrap()
            + &fs::read_to_string(bundle.join("turns/turn-1/presentation.json")).unwrap();
        assert!(!durable.contains("credential-token"));
        assert!(durable.contains("[REDACTED]"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn assembled_answer_is_redacted_again_after_raw_delta_reconstruction() {
        let secrets = vec![b"credential-token".to_vec()];
        let events = vec![
            observation_ready_event(1),
            event(
                2,
                "observation.assistant.delta",
                json!({
                    "turnId": "turn-1",
                    "messageId": "message-1",
                    "text": "credential-",
                }),
            ),
            event(
                3,
                "observation.assistant.delta",
                json!({
                    "turnId": "turn-1",
                    "messageId": "message-1",
                    "text": "token",
                }),
            ),
            event(
                4,
                "agent.turn.finished",
                json!({
                    "turnId": "turn-1",
                    "outcome": "failed",
                    "workspaceDiff": { "changes": [] },
                }),
            ),
        ];
        let presentation = build_agent_turn_presentation_from_events(
            &events,
            &test_agent_turn(AgentTurnStatus::Failed),
            &secrets,
        )
        .unwrap();
        assert_eq!(presentation.response.as_deref(), Some("[REDACTED]"));
        assert_eq!(presentation.messages[0].text, "[REDACTED]");
    }

    #[test]
    fn retained_terminal_evidence_stabilizes_projection_before_summary_persistence() {
        let root = temporary_root("agent-terminal-projection-race");
        let bundle = root.join("agent-session");
        let presentation_path = bundle.join("turns/turn-1/presentation.json");
        fs::create_dir_all(presentation_path.parent().unwrap()).unwrap();
        let events = vec![
            observation_ready_event(1),
            event(
                2,
                "observation.assistant.completed",
                json!({
                    "turnId": "turn-1",
                    "messageId": "message-1",
                    "text": "complete response",
                }),
            ),
            event(
                3,
                "agent.turn.finished",
                json!({
                    "turnId": "turn-1",
                    "outcome": "completed",
                    "workspaceDiff": { "changes": [] },
                }),
            ),
        ];

        let terminal_turn = test_agent_turn(AgentTurnStatus::Completed);
        let stored =
            build_agent_turn_presentation_from_events(&events, &terminal_turn, &[]).unwrap();
        write_json_atomic(&presentation_path, &serde_json::to_value(&stored).unwrap()).unwrap();

        let session = test_agent_session_state(&bundle);
        *lock(&session.events) = events;
        let not_yet_persisted = test_agent_turn(AgentTurnStatus::Running);
        *lock(&session.turns) = vec![not_yet_persisted.clone()];
        let workspace = test_run_state(&root.join("run"));
        let loaded =
            load_or_build_agent_turn_presentation(&session, &workspace, &not_yet_persisted)
                .unwrap();

        assert_eq!(loaded, stored);
        assert_eq!(
            loaded.completeness.assistant_output,
            AgentPresentationCompleteness::Complete
        );
        assert_eq!(
            loaded.completeness.capability_activity,
            AgentPresentationCompleteness::Complete
        );
        assert_eq!(
            loaded.completeness.native_activity,
            AgentPresentationCompleteness::Complete
        );
        assert_eq!(
            loaded.completeness.workspace_effects,
            AgentPresentationCompleteness::Complete
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn failed_partial_projection_repairs_missing_and_invalid_derived_evidence() {
        let root = temporary_root("failed-partial-agent-projection");
        let bundle = root.join("agent-session");
        fs::create_dir_all(bundle.join("turns/turn-1")).unwrap();
        let session = test_agent_session_state(&bundle);
        record_agent_event(
            &session,
            "agent.session.ready",
            json!({ "driver": { "features": [TURN_OBSERVATIONS_FEATURE] } }),
        )
        .unwrap();
        record_agent_event(
            &session,
            "observation.assistant.delta",
            json!({
                "turnId": "turn-1",
                "messageId": "message-1",
                "text": "# Partial answer\n\n- retained before failure",
            }),
        )
        .unwrap();
        let workspace = test_run_state(&root.join("run"));
        record_finished_agent_turn_event(
            &session,
            &workspace,
            "turn-1",
            AgentTurnStatus::Failed,
            json!({
                "turnId": "turn-1",
                "outcome": "failed",
                "error": "intentional fixture failure",
                "workspaceDiff": { "changes": [] },
            }),
        )
        .unwrap();

        let failed_turn = test_agent_turn(AgentTurnStatus::Failed);
        let loaded =
            load_or_build_agent_turn_presentation(&session, &workspace, &failed_turn).unwrap();
        assert_eq!(
            loaded.response.as_deref(),
            Some("# Partial answer\n\n- retained before failure")
        );
        assert_eq!(loaded.messages.len(), 1);
        assert!(!loaded.messages[0].complete);
        assert_eq!(
            loaded.completeness.assistant_output,
            AgentPresentationCompleteness::Partial
        );
        assert_eq!(
            loaded.completeness.workspace_effects,
            AgentPresentationCompleteness::Partial
        );

        fs::remove_file(bundle.join("turns/turn-1/presentation.json")).unwrap();
        let rebuilt =
            load_or_build_agent_turn_presentation(&session, &workspace, &failed_turn).unwrap();
        assert_eq!(rebuilt, loaded);
        assert!(bundle.join("turns/turn-1/presentation.json").is_file());

        fs::write(
            bundle.join("turns/turn-1/presentation.json"),
            b"{\"schemaVersion\":",
        )
        .unwrap();
        let repaired =
            load_or_build_agent_turn_presentation(&session, &workspace, &failed_turn).unwrap();
        assert_eq!(repaired, loaded);
        let stored: AgentTurnPresentation = serde_json::from_slice(
            &fs::read(bundle.join("turns/turn-1/presentation.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(stored, loaded);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn legacy_agent_session_rebuilds_terminal_projection_before_manifest_upgrade() {
        let root = temporary_root("legacy-agent-projection-replay");
        let workspace = Arc::new(test_run_state(&root.join("run")));
        let bundle = workspace.bundle_dir.join("agent-sessions/session-1");
        fs::create_dir_all(bundle.join("turns/turn-1")).unwrap();

        let summary = AgentSessionSummary {
            id: "session-1".to_owned(),
            workspace_id: "run-events".to_owned(),
            harness_id: "fixture".to_owned(),
            model_profile_id: "test".to_owned(),
            model_id: "fixture/test".to_owned(),
            status: AgentSessionStatus::Closed,
            active: false,
            created_at_ms: 1,
            updated_at_ms: 3,
            turn_count: 1,
            error: None,
        };
        let mut turn = test_agent_turn(AgentTurnStatus::Completed);
        turn.finished_at_ms = Some(3);
        turn.outcome = Some("completed".to_owned());
        let legacy_manifest = json!({
            "summary": summary,
            "turns": [turn],
        });
        write_json_atomic(&bundle.join("manifest.json"), &legacy_manifest).unwrap();

        let events = vec![
            observation_ready_event(1),
            event(
                2,
                "mcp.tool.started",
                json!({
                    "turnId": "turn-1",
                    "source": "catalog",
                    "callId": "call-1",
                    "name": "list",
                    "arguments": { "active": true },
                }),
            ),
            event(
                3,
                "mcp.tool.completed",
                json!({
                    "turnId": "turn-1",
                    "source": "catalog",
                    "callId": "call-1",
                    "name": "list",
                    "isError": false,
                    "result": { "items": ["gamma"] },
                }),
            ),
            event(
                4,
                "observation.assistant.completed",
                json!({
                    "turnId": "turn-1",
                    "messageId": "message-1",
                    "text": "# Replayed legacy answer",
                }),
            ),
            event(
                5,
                "agent.turn.finished",
                json!({
                    "turnId": "turn-1",
                    "outcome": "completed",
                    "workspaceDiff": { "changes": [] },
                }),
            ),
        ];
        let mut event_log = Vec::new();
        for event in &events {
            serde_json::to_writer(&mut event_log, event).unwrap();
            event_log.push(b'\n');
        }
        fs::write(bundle.join("events.jsonl"), event_log).unwrap();
        write_json_atomic(
            &bundle.join("turns/turn-1/presentation.json"),
            &json!({
                "schemaVersion": 1,
                "response": "# Replayed legacy answer",
                "messages": [{
                    "id": "message-1",
                    "text": "# Replayed legacy answer",
                    "complete": true,
                    "sourceEventSequences": [4],
                }],
                "activity": [{
                    "kind": "capability-call",
                    "title": "catalog · list",
                    "detail": "{\"items\":[\"gamma\"]}",
                    "status": "completed",
                    "source": "catalog",
                    "path": null,
                    "sourceEventSequences": [2, 3],
                }],
                "usage": null,
                "completeness": {
                    "assistantOutput": "complete",
                    "capabilityActivity": "complete",
                    "nativeActivity": "complete",
                    "workspaceEffects": "complete",
                    "usage": "unavailable",
                },
                "sourceEventSequences": [1, 2, 3, 4, 5],
                "sourceDigest": "sha256:legacy",
            }),
        )
        .unwrap();

        let runs = HashMap::from([("run-events".to_owned(), Arc::clone(&workspace))]);
        let sessions = load_agent_sessions(&runs);
        let session = sessions.get("session-1").unwrap();
        let loaded_turn = lock(&session.turns)[0].clone();
        let presentation =
            load_or_build_agent_turn_presentation(session, &workspace, &loaded_turn).unwrap();
        assert_eq!(
            presentation.response.as_deref(),
            Some("# Replayed legacy answer")
        );
        assert_eq!(presentation.schema_version, AGENT_TURN_PRESENTATION_VERSION);
        assert_eq!(presentation.activity[0].detail, None);
        assert_eq!(
            presentation.activity[0].arguments,
            Some(json!({ "active": true }))
        );
        assert_eq!(
            presentation.activity[0].result,
            Some(json!({ "items": ["gamma"] }))
        );
        assert!(bundle.join("turns/turn-1/presentation.json").is_file());
        let stored: JsonValue = serde_json::from_slice(
            &fs::read(bundle.join("turns/turn-1/presentation.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(stored["schemaVersion"], AGENT_TURN_PRESENTATION_VERSION);
        assert_eq!(stored["activity"][0]["detail"], JsonValue::Null);
        assert!(stored["activity"][0]["arguments"].is_object());

        let upgraded: AgentSessionManifest =
            serde_json::from_slice(&fs::read(bundle.join("manifest.json")).unwrap()).unwrap();
        assert_eq!(upgraded.version, AGENT_SESSION_MANIFEST_VERSION);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn interrupted_current_session_repairs_terminal_projection_before_a_sequence_gap() {
        let root = temporary_root("interrupted-agent-projection-repair");
        let workspace = Arc::new(test_run_state(&root.join("run")));
        let bundle = workspace.bundle_dir.join("agent-sessions/session-1");
        fs::create_dir_all(bundle.join("turns/turn-1")).unwrap();

        let summary = AgentSessionSummary {
            id: "session-1".to_owned(),
            workspace_id: "run-events".to_owned(),
            harness_id: "fixture".to_owned(),
            model_profile_id: "test".to_owned(),
            model_id: "fixture/test".to_owned(),
            status: AgentSessionStatus::Running,
            active: true,
            created_at_ms: 1,
            updated_at_ms: 2,
            turn_count: 1,
            error: None,
        };
        let manifest = AgentSessionManifest {
            version: AGENT_SESSION_MANIFEST_VERSION,
            summary,
            turns: vec![test_agent_turn(AgentTurnStatus::Running)],
        };
        write_json_atomic(
            &bundle.join("manifest.json"),
            &serde_json::to_value(manifest).unwrap(),
        )
        .unwrap();

        let events = [
            observation_ready_event(1),
            event(
                2,
                "observation.assistant.completed",
                json!({
                    "turnId": "turn-1",
                    "messageId": "message-1",
                    "text": "# Completed before interruption",
                }),
            ),
            event(
                3,
                "agent.turn.finished",
                json!({
                    "turnId": "turn-1",
                    "outcome": "completed",
                    "workspaceDiff": { "changes": [] },
                }),
            ),
            event(
                5,
                "agent.session.closed",
                json!({ "sessionId": "session-1" }),
            ),
        ];
        let mut event_log = Vec::new();
        for event in &events {
            serde_json::to_writer(&mut event_log, event).unwrap();
            event_log.push(b'\n');
        }
        fs::write(bundle.join("events.jsonl"), event_log).unwrap();

        let runs = HashMap::from([("run-events".to_owned(), Arc::clone(&workspace))]);
        let sessions = load_agent_sessions(&runs);
        let session = sessions.get("session-1").unwrap();
        let turn = lock(&session.turns)[0].clone();
        assert_eq!(turn.status, AgentTurnStatus::Completed);
        let presentation =
            load_or_build_agent_turn_presentation(session, &workspace, &turn).unwrap();
        assert_eq!(
            presentation.response.as_deref(),
            Some("# Completed before interruption")
        );
        assert!(bundle.join("turns/turn-1/presentation.json").is_file());
        assert!(bundle.join("events.corrupt.jsonl").is_file());
        assert!(
            lock(&session.events)
                .iter()
                .any(|event| event.kind == "agent.session.replay-incomplete")
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn terminal_turn_commit_failures_recover_without_contradictory_outcomes() {
        for marker in [
            "fail-presentation-write.once",
            "fail-terminal-event-append.once",
        ] {
            let root = temporary_root(marker);
            let workspace = Arc::new(test_run_state(&root.join("run")));
            let bundle = workspace.bundle_dir.join("agent-sessions/session-1");
            let turn_dir = bundle.join("turns/turn-1");
            fs::create_dir_all(&turn_dir).unwrap();
            let state = test_agent_session_state(&bundle);
            lock(&state.summary).workspace_id = "run-events".to_owned();
            persist_agent_session(&state).unwrap();
            record_agent_event(
                &state,
                "agent.session.ready",
                json!({
                    "sessionId": "session-1",
                    "driver": { "features": [TURN_OBSERVATIONS_FEATURE] }
                }),
            )
            .unwrap();
            record_agent_event(
                &state,
                "observation.assistant.completed",
                json!({
                    "sessionId": "session-1",
                    "turnId": "turn-1",
                    "event": { "messageId": "message-1", "text": "# Answer" }
                }),
            )
            .unwrap();
            fs::write(turn_dir.join(marker), []).unwrap();

            let error = record_finished_agent_turn_event(
                &state,
                &workspace,
                "turn-1",
                AgentTurnStatus::Completed,
                json!({
                    "sessionId": "session-1",
                    "turnId": "turn-1",
                    "outcome": "completed",
                    "workspaceDiff": { "changes": [] },
                }),
            )
            .unwrap_err();
            assert!(error.to_string().contains("injected agent"));
            assert_eq!(
                lock(&state.events)
                    .iter()
                    .filter(|event| event.kind == "agent.turn.finished")
                    .count(),
                0
            );

            if marker == "fail-terminal-event-append.once" {
                assert!(turn_dir.join("presentation.pending.json").is_file());
                assert!(
                    repair_terminal_agent_turn_presentations(
                        &state.evidence_root,
                        &lock(&state.turns),
                        &lock(&state.events),
                    )
                    .unwrap()
                );
                assert!(!turn_dir.join("presentation.pending.json").exists());
            }
            remove_evidence_entry(&turn_dir.join("presentation.pending.json")).unwrap();
            let terminal = record_finished_agent_turn_event(
                &state,
                &workspace,
                "turn-1",
                AgentTurnStatus::Failed,
                json!({
                    "sessionId": "session-1",
                    "turnId": "turn-1",
                    "outcome": "failed",
                    "error": "terminal projection commit failed",
                    "workspaceDiff": { "changes": [] },
                }),
            )
            .unwrap();
            update_agent_turn_status(
                &state,
                "turn-1",
                AgentTurnStatus::Failed,
                Some("failed"),
                Some("terminal projection commit failed"),
            )
            .unwrap();
            assert_eq!(terminal.payload["outcome"], "failed");
            assert_eq!(
                lock(&state.events)
                    .iter()
                    .filter(|event| event.kind == "agent.turn.finished")
                    .count(),
                1
            );
            let presentation = turn_dir.join("presentation.json");
            fs::copy(&presentation, turn_dir.join("presentation.pending.json")).unwrap();
            fs::remove_file(&presentation).unwrap();

            let runs = HashMap::from([("run-events".to_owned(), Arc::clone(&workspace))]);
            let sessions = load_agent_sessions(&runs);
            let replayed = sessions.get("session-1").unwrap();
            assert_eq!(
                lock(&replayed.events)
                    .iter()
                    .filter(|event| event.kind == "agent.turn.finished")
                    .count(),
                1
            );
            assert_eq!(lock(&replayed.turns)[0].status, AgentTurnStatus::Failed);
            assert!(presentation.is_file());
            assert!(!turn_dir.join("presentation.pending.json").exists());
            fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn capability_projection_scopes_identical_call_ids_by_source() {
        let turn = test_agent_turn(AgentTurnStatus::Completed);
        let events = vec![
            event(
                1,
                "mcp.tool.started",
                json!({
                    "turnId": "turn-1",
                    "source": "catalog",
                    "callId": "call-1",
                    "name": "list",
                    "arguments": { "active": true, "minimumScore": 3 },
                }),
            ),
            event(
                2,
                "mcp.tool.started",
                json!({
                    "turnId": "turn-1",
                    "source": "analysis",
                    "callId": "call-1",
                    "name": "summarize",
                    "arguments": {},
                }),
            ),
            event(
                3,
                "mcp.tool.completed",
                json!({
                    "turnId": "turn-1",
                    "source": "catalog",
                    "callId": "call-1",
                    "name": "list",
                    "isError": false,
                    "result": { "items": [{ "name": "gamma", "score": 8 }] },
                }),
            ),
            event(
                4,
                "mcp.tool.completed",
                json!({
                    "turnId": "turn-1",
                    "source": "analysis",
                    "callId": "call-1",
                    "name": "summarize",
                    "isError": false,
                    "result": { "active": [] },
                }),
            ),
        ];

        let presentation = build_agent_turn_presentation_from_events(&events, &turn, &[]).unwrap();
        assert_eq!(presentation.activity.len(), 2);
        assert_eq!(presentation.activity[0].source.as_deref(), Some("catalog"));
        assert_eq!(presentation.activity[0].source_event_sequences, [1, 3]);
        assert_eq!(presentation.activity[0].status, "completed");
        assert_eq!(presentation.activity[0].detail, None);
        assert_eq!(presentation.activity[0].operation.as_deref(), Some("list"));
        assert_eq!(presentation.activity[0].call_id.as_deref(), Some("call-1"));
        assert_eq!(
            presentation.activity[0].arguments,
            Some(json!({ "active": true, "minimumScore": 3 }))
        );
        assert_eq!(
            presentation.activity[0].result,
            Some(json!({ "items": [{ "name": "gamma", "score": 8 }] }))
        );
        assert_eq!(presentation.activity[1].source.as_deref(), Some("analysis"));
        assert_eq!(presentation.activity[1].source_event_sequences, [2, 4]);
        assert_eq!(presentation.activity[1].status, "completed");

        let serialized = serde_json::to_value(&presentation.activity[0]).unwrap();
        assert_eq!(serialized["detail"], JsonValue::Null);
        assert_eq!(serialized["arguments"]["active"], true);
        assert_eq!(serialized["result"]["items"][0]["score"], 8);
        assert!(serialized.get("actionId").is_none());
        assert!(serialized.get("changeKind").is_none());
    }

    #[test]
    fn typed_capability_projection_redacts_arguments_and_results() {
        let events = vec![
            event(
                1,
                "mcp.tool.started",
                json!({
                    "turnId": "turn-1",
                    "source": "catalog",
                    "callId": "call-1",
                    "name": "list",
                    "arguments": {
                        "query": "use turn-secret",
                        "authorization": "Bearer turn-secret",
                    },
                }),
            ),
            event(
                2,
                "mcp.tool.completed",
                json!({
                    "turnId": "turn-1",
                    "source": "catalog",
                    "callId": "call-1",
                    "name": "list",
                    "isError": false,
                    "result": {
                        "items": [{ "note": "turn-secret stayed private" }],
                    },
                }),
            ),
        ];

        let presentation = build_agent_turn_presentation_from_events(
            &events,
            &test_agent_turn(AgentTurnStatus::Completed),
            &[b"turn-secret".to_vec()],
        )
        .unwrap();
        let activity = &presentation.activity[0];
        let serialized = serde_json::to_string(activity).unwrap();

        assert!(!serialized.contains("turn-secret"));
        assert_eq!(
            activity.arguments,
            Some(json!({
                "query": "use [REDACTED]",
                "authorization": "[REDACTED]",
            }))
        );
        assert_eq!(
            activity.result,
            Some(json!({
                "items": [{ "note": "[REDACTED] stayed private" }],
            }))
        );
    }

    #[test]
    fn null_capability_fields_round_trip_and_reload_without_projection_drift() {
        let root = temporary_root("null-capability-projection");
        let bundle = root.join("agent-session");
        fs::create_dir_all(bundle.join("turns/turn-1")).unwrap();
        let events = vec![
            event(
                1,
                "mcp.tool.started",
                json!({
                    "turnId": "turn-1",
                    "source": "catalog",
                    "callId": "call-1",
                    "name": "list",
                    "arguments": null,
                }),
            ),
            event(
                2,
                "mcp.tool.completed",
                json!({
                    "turnId": "turn-1",
                    "source": "catalog",
                    "callId": "call-1",
                    "name": "list",
                    "isError": false,
                    "result": null,
                }),
            ),
        ];
        let turn = test_agent_turn(AgentTurnStatus::Running);
        let expected = build_agent_turn_presentation_from_events(&events, &turn, &[]).unwrap();
        let serialized = serde_json::to_value(&expected).unwrap();
        let activity = serialized["activity"][0].as_object().unwrap();
        assert!(!activity.contains_key("arguments"));
        assert!(!activity.contains_key("result"));
        let round_tripped: AgentTurnPresentation =
            serde_json::from_value(serialized.clone()).unwrap();
        assert_eq!(round_tripped, expected);
        write_json_atomic(&bundle.join("turns/turn-1/presentation.json"), &serialized).unwrap();

        let state = test_agent_session_state(&bundle);
        *lock(&state.events) = events;
        let workspace = test_run_state(&root.join("run"));
        let loaded = load_or_build_agent_turn_presentation(&state, &workspace, &turn).unwrap();

        assert_eq!(loaded, expected);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn recovered_agent_manifest_requires_confined_unique_turn_identities() {
        let root = temporary_root("agent-manifest-identity-validation");
        let state = test_agent_session_state(&root);
        let summary = lock(&state.summary).clone();
        let valid = AgentSessionManifest {
            version: AGENT_SESSION_MANIFEST_VERSION,
            summary: summary.clone(),
            turns: vec![test_agent_turn(AgentTurnStatus::Failed)],
        };
        validate_recovered_agent_manifest(&valid).unwrap();

        for id in [
            "",
            ".",
            "..",
            "../outside",
            "/tmp/outside",
            "TURN-1",
            "turn_1",
            "turn\\1",
        ] {
            let mut manifest = AgentSessionManifest {
                version: AGENT_SESSION_MANIFEST_VERSION,
                summary: summary.clone(),
                turns: vec![test_agent_turn(AgentTurnStatus::Failed)],
            };
            manifest.turns[0].id = id.to_owned();
            assert!(
                validate_recovered_agent_manifest(&manifest).is_err(),
                "recovered turn id should be rejected: {id:?}"
            );
        }

        let mut mismatched = valid;
        mismatched.turns[0].session_id = "session-2".to_owned();
        assert!(validate_recovered_agent_manifest(&mismatched).is_err());

        let mut duplicate = mismatched;
        duplicate.turns[0]
            .session_id
            .clone_from(&duplicate.summary.id);
        duplicate.turns.push(duplicate.turns[0].clone());
        assert!(validate_recovered_agent_manifest(&duplicate).is_err());
        drop(state);
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn recovered_turn_repairs_use_no_follow_handle_relative_operations() {
        use std::os::unix::fs::symlink;

        let root = temporary_root("agent-turn-repair-confinement");
        let bundle = root.join("session");
        let outside = root.join("outside");
        fs::create_dir_all(bundle.join("turns")).unwrap();
        let evidence_root = AgentSessionEvidenceRoot::open(bundle.clone()).unwrap();
        fs::create_dir(&outside).unwrap();
        let outside_sentinel = outside.join("sentinel.json");
        fs::write(&outside_sentinel, b"outside-safe").unwrap();
        let turn = test_agent_turn(AgentTurnStatus::Failed);
        let events = vec![event(
            1,
            "agent.turn.finished",
            json!({
                "turnId": "turn-1",
                "outcome": "failed",
                "error": "fixture failure",
                "workspaceDiff": { "changes": [] },
            }),
        )];

        symlink(&outside, bundle.join("turns/turn-1")).unwrap();
        assert!(matches!(
            repair_terminal_agent_turn_presentations(
                &evidence_root,
                std::slice::from_ref(&turn),
                &events
            ),
            Err(RunError::PathEscape(_))
        ));
        assert_eq!(fs::read(&outside_sentinel).unwrap(), b"outside-safe");
        fs::remove_file(bundle.join("turns/turn-1")).unwrap();

        let turn_dir = bundle.join("turns/turn-1");
        fs::create_dir(&turn_dir).unwrap();
        symlink(&outside_sentinel, turn_dir.join("presentation.json")).unwrap();
        assert!(matches!(
            repair_terminal_agent_turn_presentations(
                &evidence_root,
                std::slice::from_ref(&turn),
                &events
            ),
            Err(RunError::PathEscape(_))
        ));
        assert_eq!(fs::read(&outside_sentinel).unwrap(), b"outside-safe");
        fs::remove_file(turn_dir.join("presentation.json")).unwrap();

        symlink(
            &outside_sentinel,
            turn_dir.join("presentation.pending.json"),
        )
        .unwrap();
        fs::hard_link(&outside_sentinel, turn_dir.join("presentation.json.tmp")).unwrap();
        assert!(
            repair_terminal_agent_turn_presentations(
                &evidence_root,
                std::slice::from_ref(&turn),
                &events,
            )
            .unwrap()
        );
        assert!(!turn_dir.join("presentation.pending.json").exists());
        assert!(turn_dir.join("presentation.json").is_file());
        assert_eq!(fs::read(&outside_sentinel).unwrap(), b"outside-safe");
        assert_eq!(
            fs::read(turn_dir.join("presentation.json.tmp")).unwrap(),
            b"outside-safe"
        );

        drop(evidence_root);
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn recovered_session_root_files_are_never_followed() {
        use std::os::unix::fs::symlink;

        let root = temporary_root("agent-session-root-confinement");
        let workspace = Arc::new(test_run_state(&root.join("run")));
        let sessions_dir = workspace.bundle_dir.join("agent-sessions");
        fs::create_dir(&sessions_dir).unwrap();
        let outside_manifest = root.join("outside-manifest.json");
        let outside_events = root.join("outside-events.jsonl");
        fs::write(&outside_events, b"outside-events-safe").unwrap();

        let make_manifest = |id: &str| AgentSessionManifest {
            version: AGENT_SESSION_MANIFEST_VERSION,
            summary: AgentSessionSummary {
                id: id.to_owned(),
                workspace_id: "run-events".to_owned(),
                harness_id: "fixture".to_owned(),
                model_profile_id: "test".to_owned(),
                model_id: "fixture/test".to_owned(),
                status: AgentSessionStatus::Closed,
                active: false,
                created_at_ms: 1,
                updated_at_ms: 1,
                turn_count: 0,
                error: None,
            },
            turns: Vec::new(),
        };
        write_json_atomic(
            &outside_manifest,
            &serde_json::to_value(make_manifest("session-1")).unwrap(),
        )
        .unwrap();
        let manifest_link = sessions_dir.join("session-1");
        fs::create_dir(&manifest_link).unwrap();
        symlink(&outside_manifest, manifest_link.join("manifest.json")).unwrap();
        fs::write(manifest_link.join("events.jsonl"), []).unwrap();

        let events_link = sessions_dir.join("session-2");
        fs::create_dir(&events_link).unwrap();
        write_json_atomic(
            &events_link.join("manifest.json"),
            &serde_json::to_value(make_manifest("session-2")).unwrap(),
        )
        .unwrap();
        symlink(&outside_events, events_link.join("events.jsonl")).unwrap();

        let runs = HashMap::from([("run-events".to_owned(), workspace)]);
        assert!(load_agent_sessions(&runs).is_empty());
        assert_eq!(fs::read(&outside_events).unwrap(), b"outside-events-safe");
        let retained: AgentSessionManifest =
            serde_json::from_slice(&fs::read(&outside_manifest).unwrap()).unwrap();
        assert_eq!(retained.summary.id, "session-1");

        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn recovered_agent_sessions_reject_symlinked_collection() {
        use std::os::unix::fs::symlink;

        let root = temporary_root("agent-session-collection-symlink");
        let workspace = Arc::new(test_run_state(&root.join("run")));
        let outside = root.join("outside");
        let outside_session = outside.join("session-1");
        fs::create_dir_all(outside_session.join("turns")).unwrap();
        let manifest = AgentSessionManifest {
            version: AGENT_SESSION_MANIFEST_VERSION,
            summary: AgentSessionSummary {
                id: "session-1".to_owned(),
                workspace_id: "run-events".to_owned(),
                harness_id: "fixture".to_owned(),
                model_profile_id: "test".to_owned(),
                model_id: "fixture/test".to_owned(),
                status: AgentSessionStatus::Starting,
                active: true,
                created_at_ms: 1,
                updated_at_ms: 1,
                turn_count: 0,
                error: None,
            },
            turns: Vec::new(),
        };
        write_json_atomic(
            &outside_session.join("manifest.json"),
            &serde_json::to_value(&manifest).unwrap(),
        )
        .unwrap();
        fs::write(
            outside_session.join("events.jsonl"),
            b"{\"outside\":\"safe\"}\n",
        )
        .unwrap();
        let manifest_before = fs::read(outside_session.join("manifest.json")).unwrap();
        let events_before = fs::read(outside_session.join("events.jsonl")).unwrap();
        symlink(&outside, workspace.bundle_dir.join("agent-sessions")).unwrap();

        let runs = HashMap::from([("run-events".to_owned(), workspace)]);
        assert!(load_agent_sessions(&runs).is_empty());
        assert_eq!(
            fs::read(outside_session.join("manifest.json")).unwrap(),
            manifest_before
        );
        assert_eq!(
            fs::read(outside_session.join("events.jsonl")).unwrap(),
            events_before
        );

        drop(runs);
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn agent_session_collection_stays_pinned_across_path_replacement() {
        let root = temporary_root("agent-session-pinned-collection");
        let run_dir = root.join("run");
        let visible = run_dir.join("agent-sessions");
        let displaced = run_dir.join("displaced-agent-sessions");
        let replacement = root.join("replacement-agent-sessions");
        fs::create_dir_all(visible.join("session-original")).unwrap();
        fs::create_dir_all(replacement.join("session-replacement")).unwrap();
        fs::write(
            visible.join("session-original/manifest.json"),
            b"original-manifest",
        )
        .unwrap();
        fs::write(
            replacement.join("session-replacement/manifest.json"),
            b"replacement-manifest",
        )
        .unwrap();
        let state = test_run_state(&run_dir);

        assert_eq!(
            state.agent_session_directories.session_names().unwrap(),
            [OsString::from("session-original")]
        );
        fs::rename(&visible, &displaced).unwrap();
        fs::rename(&replacement, &visible).unwrap();

        assert_eq!(
            state.agent_session_directories.session_names().unwrap(),
            [OsString::from("session-original")]
        );
        let original = state
            .agent_session_directories
            .open_session(OsStr::new("session-original"))
            .unwrap();
        assert_eq!(
            read_optional_agent_evidence_file(
                &original,
                Path::new("manifest.json"),
                &visible.join("session-original/manifest.json"),
            )
            .unwrap()
            .unwrap(),
            b"original-manifest"
        );
        let created = state
            .agent_session_directories
            .create_session("session-new")
            .unwrap();
        assert!(displaced.join("session-new/turns").is_dir());
        assert!(!visible.join("session-new").exists());
        assert_eq!(
            fs::read(visible.join("session-replacement/manifest.json")).unwrap(),
            b"replacement-manifest"
        );

        drop(created);
        drop(original);
        drop(state);
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn agent_session_evidence_root_stays_pinned_across_path_replacement() {
        let root = temporary_root("agent-session-pinned-evidence-root");
        let visible = root.join("session");
        let displaced = root.join("displaced-session");
        let replacement = root.join("replacement-session");
        let turn_relative = Path::new("turns/turn-1");
        fs::create_dir_all(visible.join(turn_relative)).unwrap();
        fs::create_dir_all(replacement.join(turn_relative)).unwrap();
        fs::write(visible.join("manifest.json"), b"original-manifest").unwrap();
        fs::write(replacement.join("manifest.json"), b"replacement-manifest").unwrap();
        fs::write(replacement.join("events.jsonl"), b"replacement-events\n").unwrap();
        fs::write(
            replacement.join(turn_relative).join("presentation.json"),
            b"replacement-presentation",
        )
        .unwrap();
        fs::write(replacement.join("remove.me"), b"replacement-remove").unwrap();

        let evidence_root = AgentSessionEvidenceRoot::open(visible.clone()).unwrap();
        fs::rename(&visible, &displaced).unwrap();
        fs::rename(&replacement, &visible).unwrap();

        assert_eq!(
            read_optional_agent_evidence_file(
                &evidence_root,
                Path::new("manifest.json"),
                &visible.join("manifest.json"),
            )
            .unwrap()
            .unwrap(),
            b"original-manifest"
        );
        write_confined_json_atomic(
            &evidence_root,
            Path::new("manifest.json"),
            &json!({ "pinned": true }),
        )
        .unwrap();
        append_confined_bytes(
            &evidence_root,
            Path::new("events.jsonl"),
            b"original-event\n",
        )
        .unwrap();
        let pending = turn_relative.join("presentation.pending.json");
        let presentation = turn_relative.join("presentation.json");
        write_confined_bytes_atomic(&evidence_root, &pending, b"original-presentation").unwrap();
        rename_confined_evidence_file(&evidence_root, &pending, &presentation).unwrap();
        write_confined_bytes_atomic(&evidence_root, Path::new("remove.me"), b"remove-original")
            .unwrap();
        assert!(remove_confined_evidence_file(&evidence_root, Path::new("remove.me")).unwrap());

        assert_eq!(
            serde_json::from_slice::<JsonValue>(
                &fs::read(displaced.join("manifest.json")).unwrap()
            )
            .unwrap(),
            json!({ "pinned": true })
        );
        assert_eq!(
            fs::read(visible.join("manifest.json")).unwrap(),
            b"replacement-manifest"
        );
        assert_eq!(
            fs::read(displaced.join("events.jsonl")).unwrap(),
            b"original-event\n"
        );
        assert_eq!(
            fs::read(visible.join("events.jsonl")).unwrap(),
            b"replacement-events\n"
        );
        assert_eq!(
            fs::read(displaced.join(&presentation)).unwrap(),
            b"original-presentation"
        );
        assert_eq!(
            fs::read(visible.join(&presentation)).unwrap(),
            b"replacement-presentation"
        );
        assert!(!displaced.join("remove.me").exists());
        assert_eq!(
            fs::read(visible.join("remove.me")).unwrap(),
            b"replacement-remove"
        );

        drop(evidence_root);
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn agent_turn_finalization_stays_pinned_across_session_path_replacement() {
        let root = temporary_root("agent-turn-pinned-finalization");
        let visible = root.join("session");
        let displaced = root.join("displaced-session");
        let replacement = root.join("replacement-session");
        let workspace = root.join("workspace");
        let turn_relative = Path::new("turns/turn-1");
        fs::create_dir_all(visible.join(turn_relative).join("initial")).unwrap();
        fs::create_dir_all(replacement.join(turn_relative).join("initial")).unwrap();
        fs::create_dir(&workspace).unwrap();
        fs::write(
            visible.join(turn_relative).join("initial/result.txt"),
            b"before",
        )
        .unwrap();
        fs::write(
            replacement.join(turn_relative).join("initial/result.txt"),
            b"replacement-before",
        )
        .unwrap();
        fs::write(
            replacement.join(turn_relative).join("diff.json"),
            b"replacement-diff",
        )
        .unwrap();
        fs::write(workspace.join("result.txt"), b"after").unwrap();
        fs::write(workspace.join("created.txt"), b"created").unwrap();
        let state = test_agent_session_state(&visible);

        fs::rename(&visible, &displaced).unwrap();
        fs::rename(&replacement, &visible).unwrap();
        let diff = finalize_agent_turn_workspace(&state, "turn-1", &workspace, &[]).unwrap();

        assert_eq!(diff["changes"].as_array().unwrap().len(), 2);
        assert_eq!(
            fs::read(displaced.join(turn_relative).join("final/result.txt")).unwrap(),
            b"after"
        );
        assert_eq!(
            fs::read(displaced.join(turn_relative).join("final/created.txt")).unwrap(),
            b"created"
        );
        assert!(
            serde_json::from_slice::<JsonValue>(
                &fs::read(displaced.join(turn_relative).join("diff.json")).unwrap()
            )
            .unwrap()["changes"]
                .as_array()
                .is_some_and(|changes| changes.len() == 2)
        );
        assert_eq!(
            fs::read(visible.join(turn_relative).join("initial/result.txt")).unwrap(),
            b"replacement-before"
        );
        assert_eq!(
            fs::read(visible.join(turn_relative).join("diff.json")).unwrap(),
            b"replacement-diff"
        );
        assert!(!visible.join(turn_relative).join("final").exists());

        drop(state);
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn confined_evidence_io_rejects_fifos_without_blocking() {
        let root = temporary_root("agent-session-fifo-confinement");
        let evidence_root = AgentSessionEvidenceRoot::open(root.clone()).unwrap();
        for name in ["manifest.json", "events.jsonl"] {
            assert!(
                Command::new("mkfifo")
                    .arg(root.join(name))
                    .status()
                    .unwrap()
                    .success()
            );
        }

        let started = Instant::now();
        assert!(
            read_optional_confined_file(
                &root,
                Path::new("manifest.json"),
                &root.join("manifest.json"),
            )
            .is_err()
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "a confined evidence read blocked on a FIFO"
        );

        let started = Instant::now();
        assert!(
            append_confined_bytes(&evidence_root, Path::new("events.jsonl"), b"event\n").is_err()
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "a confined evidence append blocked on a FIFO"
        );

        drop(evidence_root);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn native_and_workspace_activity_retain_kind_specific_fields() {
        let events = vec![
            event(
                1,
                "observation.native-action",
                json!({
                    "turnId": "turn-1",
                    "actionId": "action-1",
                    "name": "write_file",
                    "status": "completed",
                    "summary": "Wrote ranked.json",
                }),
            ),
            event(
                2,
                "agent.turn.finished",
                json!({
                    "turnId": "turn-1",
                    "outcome": "completed",
                    "workspaceDiff": {
                        "changes": [{
                            "path": "ranked.json",
                            "entryType": "file",
                            "kind": "mode-changed",
                            "beforeMode": "0644",
                            "afterMode": "0755",
                        }],
                    },
                }),
            ),
        ];

        let presentation = build_agent_turn_presentation_from_events(
            &events,
            &test_agent_turn(AgentTurnStatus::Completed),
            &[],
        )
        .unwrap();
        let native = &presentation.activity[0];
        let workspace = &presentation.activity[1];

        assert_eq!(native.action_id.as_deref(), Some("action-1"));
        assert_eq!(native.operation.as_deref(), Some("write_file"));
        assert_eq!(native.detail.as_deref(), Some("Wrote ranked.json"));
        assert_eq!(workspace.path.as_deref(), Some("ranked.json"));
        assert_eq!(workspace.change_kind.as_deref(), Some("mode-changed"));
        assert_eq!(workspace.entry_type.as_deref(), Some("file"));
        assert_eq!(workspace.before_mode.as_deref(), Some("0644"));
        assert_eq!(workspace.after_mode.as_deref(), Some("0755"));
        assert_eq!(workspace.detail.as_deref(), Some("mode 0644 -> 0755"));
    }

    #[test]
    fn corrupt_agent_event_suffix_is_quarantined_without_losing_valid_prefix() {
        let root = temporary_root("agent-event-suffix-recovery");
        let prefix = [
            event(
                1,
                "agent.session.started",
                json!({ "sessionId": "session-1" }),
            ),
            event(
                2,
                "agent.session.ready",
                json!({ "sessionId": "session-1" }),
            ),
        ];
        let mut source = Vec::new();
        for event in &prefix {
            source.extend(serde_json::to_vec(event).unwrap());
            source.push(b'\n');
        }
        source.extend_from_slice(br#"{"sequence":3,"kind":"truncated"#);
        fs::write(root.join("events.jsonl"), &source).unwrap();

        let evidence_root = AgentSessionEvidenceRoot::open(root.clone()).unwrap();
        let recovered = read_agent_events_recovering(&evidence_root).unwrap();
        assert_eq!(recovered.len(), 3);
        for (recovered, expected) in recovered.iter().zip(&prefix) {
            assert_eq!(recovered.sequence, expected.sequence);
            assert_eq!(recovered.at_ms, expected.at_ms);
            assert_eq!(recovered.kind, expected.kind);
            assert_eq!(recovered.payload, expected.payload);
        }
        assert_eq!(recovered[2].sequence, 3);
        assert_eq!(recovered[2].kind, "agent.session.replay-incomplete");
        assert_eq!(
            recovered[2].payload["reason"],
            "a corrupt event-log suffix was quarantined"
        );
        assert_eq!(fs::read(root.join("events.corrupt.jsonl")).unwrap(), source);

        let repaired = read_events(&root.join("events.jsonl")).unwrap();
        assert_eq!(
            serde_json::to_value(&repaired).unwrap(),
            serde_json::to_value(&recovered).unwrap()
        );
        drop(evidence_root);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn full_start_turn_record_limit_accounts_for_prompt_input_and_envelope() {
        let oversized_prompt = "p".repeat(MAX_DRIVER_RECORD_BYTES);
        let error =
            validate_agent_turn_command_size("session-1", "turn-1", &oversized_prompt, None, &[])
                .unwrap_err();
        assert!(error.to_string().contains("driver record limit"));

        let input = json!("i".repeat(MAX_AGENT_TURN_INPUT_BYTES - 128));
        assert!(serde_json::to_vec(&input).unwrap().len() <= MAX_AGENT_TURN_INPUT_BYTES);
        let error = validate_agent_turn_command_size("session-1", "turn-1", "p", Some(&input), &[])
            .unwrap_err();
        assert!(error.to_string().contains("driver record limit"));
    }

    #[cfg(unix)]
    #[test]
    #[allow(clippy::too_many_lines)]
    fn final_turn_evidence_redacts_read_only_secrets_and_reports_mode_only_changes() {
        use std::os::unix::fs::PermissionsExt;

        let root = temporary_root("agent-final-evidence");
        let bundle = root.join("session");
        let initial = bundle.join("turns/turn-1/initial");
        let workspace = root.join("workspace");
        fs::create_dir_all(&initial).unwrap();
        fs::create_dir_all(&workspace).unwrap();
        fs::set_permissions(&initial, fs::Permissions::from_mode(0o755)).unwrap();
        fs::set_permissions(&workspace, fs::Permissions::from_mode(0o700)).unwrap();
        fs::create_dir(initial.join("deleted-empty")).unwrap();
        fs::create_dir(initial.join("mode-directory")).unwrap();
        fs::set_permissions(
            initial.join("mode-directory"),
            fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        fs::create_dir(workspace.join("created-empty")).unwrap();
        fs::set_permissions(
            workspace.join("created-empty"),
            fs::Permissions::from_mode(0o750),
        )
        .unwrap();
        fs::create_dir(workspace.join("mode-directory")).unwrap();
        fs::set_permissions(
            workspace.join("mode-directory"),
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        fs::write(initial.join("mode.txt"), "unchanged").unwrap();
        fs::set_permissions(initial.join("mode.txt"), fs::Permissions::from_mode(0o644)).unwrap();
        fs::write(workspace.join("mode.txt"), "unchanged").unwrap();
        fs::set_permissions(
            workspace.join("mode.txt"),
            fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        fs::write(workspace.join("secret.txt"), "read-only-secret").unwrap();
        fs::set_permissions(
            workspace.join("secret.txt"),
            fs::Permissions::from_mode(0o444),
        )
        .unwrap();
        let state = test_agent_session_state(&bundle);

        finalize_agent_turn_workspace(
            &state,
            "turn-1",
            &workspace,
            &[b"read-only-secret".to_vec()],
        )
        .unwrap();

        let final_secret = bundle.join("turns/turn-1/final/secret.txt");
        assert_eq!(fs::read_to_string(&final_secret).unwrap(), "[REDACTED]");
        assert_eq!(
            fs::metadata(&final_secret).unwrap().permissions().mode() & 0o7777,
            0o444
        );
        let diff = read_optional_json(&bundle.join("turns/turn-1/diff.json"))
            .unwrap()
            .unwrap();
        let mode_change = diff["changes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|change| change["path"] == "mode.txt")
            .unwrap();
        assert_eq!(mode_change["kind"], "mode-changed");
        assert_eq!(mode_change["entryType"], "file");
        assert_eq!(mode_change["beforeMode"], "0644");
        assert_eq!(mode_change["afterMode"], "0755");
        let changes = diff["changes"].as_array().unwrap();
        let change = |path: &str| {
            changes
                .iter()
                .find(|change| change["path"] == path)
                .unwrap()
        };
        assert_eq!(change(".")["kind"], "mode-changed");
        assert_eq!(change(".")["entryType"], "directory");
        assert_eq!(change(".")["beforeMode"], "0755");
        assert_eq!(change(".")["afterMode"], "0700");
        assert_eq!(change("created-empty")["kind"], "created");
        assert_eq!(change("created-empty")["entryType"], "directory");
        assert_eq!(change("created-empty")["afterMode"], "0750");
        assert_eq!(change("deleted-empty")["kind"], "deleted");
        assert_eq!(change("deleted-empty")["entryType"], "directory");
        assert_eq!(change("mode-directory")["kind"], "mode-changed");
        assert_eq!(change("mode-directory")["entryType"], "directory");
        assert_eq!(change("mode-directory")["beforeMode"], "0755");
        assert_eq!(change("mode-directory")["afterMode"], "0700");
        let final_tree = capture_tree(&bundle.join("turns/turn-1/final")).unwrap();
        assert_eq!(
            permission_label(&final_tree.root_permissions),
            permission_label(&fs::metadata(&workspace).unwrap().permissions())
        );
        assert_eq!(
            final_tree
                .directories
                .get("created-empty")
                .map(permission_label)
                .as_deref(),
            Some("0750")
        );
        assert!(
            !serde_json::to_string(&diff)
                .unwrap()
                .contains("read-only-secret")
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    fn interactive_fixture_launch() -> DriverLaunch {
        let script = r#"
sequence=1
printf '%s\n' '{"protocolVersion":1,"sequence":1,"causedBy":null,"type":"driver.ready","driver":{"name":"interactive-fixture","version":"1","revision":null,"features":["streaming","turn-observations-v1"]}}'
while IFS= read -r line; do
  session=$(printf '%s' "$line" | sed -E 's/.*"sessionId":"([^"]+)".*/\1/')
  case "$line" in
    *'"type":"session.open"'*)
      sequence=$((sequence + 1))
      printf '{"protocolVersion":1,"sequence":%s,"causedBy":null,"type":"session.opened","sessionId":"%s","processId":4242}\n' "$sequence" "$session"
      ;;
    *'"type":"turn.start"'*)
      turn=$(printf '%s' "$line" | sed -E 's/.*"turnId":"([^"]+)".*/\1/')
      abort_outcome=aborted
      first='# Fixture answer\n\n**Gamma** leads the catalog.\n\n'
      second='| Item | Score |\n| --- | ---: |\n| `gamma` | **8** |\n| `alpha` | 3 |'
      complete='# Fixture answer\n\n**Gamma** leads the catalog.\n\n| Item | Score |\n| --- | ---: |\n| `gamma` | **8** |\n| `alpha` | 3 |'
      case "$line" in
        *'what did you conclude earlier?'*)
          first='## Prior conclusion\n\n'
          second='`gamma` remained highest at **8**.'
          complete='## Prior conclusion\n\n`gamma` remained highest at **8**.'
          ;;
      esac
	      sequence=$((sequence + 1))
	      printf '{"protocolVersion":1,"sequence":%s,"causedBy":null,"type":"turn.event","sessionId":"%s","turnId":"%s","eventType":"observation.assistant.delta","payload":{"messageId":"message-%s","text":"%s"}}\n' "$sequence" "$session" "$turn" "$turn" "$first"
	      case "$line" in
	        *turn-scoped-failure-with-evidence-error*)
	          sleep 0.2
	          sequence=$((sequence + 1))
	          printf '{"protocolVersion":1,"sequence":%s,"causedBy":null,"type":"driver.failed","scope":"turn","sessionId":"%s","turnId":"%s","code":"fixture_turn_failed","message":"the fixture rejected this turn"}\n' "$sequence" "$session" "$turn"
	          continue
	          ;;
	        *turn-scoped-failure*)
	          sequence=$((sequence + 1))
	          printf '{"protocolVersion":1,"sequence":%s,"causedBy":null,"type":"driver.failed","scope":"turn","sessionId":"%s","turnId":"%s","code":"fixture_turn_failed","message":"the fixture rejected this turn"}\n' "$sequence" "$session" "$turn"
	          continue
	          ;;
	        *wait-for-abort-hostile-partial*)
          abort_outcome=completed
          continue
          ;;
        *wait-for-abort-hostile-complete*|*wait-for-timeout-hostile-complete*)
          abort_outcome=completed
          ;;
        *wait-for-abort*)
          continue
          ;;
      esac
      sequence=$((sequence + 1))
      printf '{"protocolVersion":1,"sequence":%s,"causedBy":null,"type":"turn.event","sessionId":"%s","turnId":"%s","eventType":"observation.assistant.delta","payload":{"messageId":"message-%s","text":"%s"}}\n' "$sequence" "$session" "$turn" "$turn" "$second"
      sequence=$((sequence + 1))
      printf '{"protocolVersion":1,"sequence":%s,"causedBy":null,"type":"turn.event","sessionId":"%s","turnId":"%s","eventType":"observation.assistant.completed","payload":{"messageId":"message-%s","text":"%s"}}\n' "$sequence" "$session" "$turn" "$turn" "$complete"
      case "$line" in
        *wait-for-abort-hostile-complete*|*wait-for-timeout-hostile-complete*) continue ;;
      esac
      sequence=$((sequence + 1))
      printf '{"protocolVersion":1,"sequence":%s,"causedBy":null,"type":"turn.event","sessionId":"%s","turnId":"%s","eventType":"observation.native-action","payload":{"actionId":"inspect-catalog-%s","name":"Inspect catalog","status":"completed","summary":"Compared active item scores."}}\n' "$sequence" "$session" "$turn" "$turn"
      sequence=$((sequence + 1))
      printf '{"protocolVersion":1,"sequence":%s,"causedBy":null,"type":"turn.event","sessionId":"%s","turnId":"%s","eventType":"observation.usage","payload":{"inputTokens":7,"outputTokens":21,"totalTokens":28}}\n' "$sequence" "$session" "$turn"
      sequence=$((sequence + 1))
      printf '{"protocolVersion":1,"sequence":%s,"causedBy":null,"type":"turn.finished","sessionId":"%s","turnId":"%s","outcome":"completed","evidence":{"fixture":true}}\n' "$sequence" "$session" "$turn"
      ;;
    *'"type":"turn.abort"'*)
      turn=$(printf '%s' "$line" | sed -E 's/.*"turnId":"([^"]+)".*/\1/')
      sequence=$((sequence + 1))
      printf '{"protocolVersion":1,"sequence":%s,"causedBy":null,"type":"turn.finished","sessionId":"%s","turnId":"%s","outcome":"%s","evidence":{"fixture":true}}\n' "$sequence" "$session" "$turn" "$abort_outcome"
      ;;
    *'"type":"session.close"'*)
      sequence=$((sequence + 1))
      printf '{"protocolVersion":1,"sequence":%s,"causedBy":null,"type":"session.closed","sessionId":"%s"}\n' "$sequence" "$session"
      exit 0
      ;;
  esac
done
"#;
        let mut launch = DriverLaunch::new("/bin/sh");
        launch.args = vec!["-c".into(), script.into()];
        launch
    }

    #[cfg(unix)]
    async fn start_interactive_fixture(
        label: &str,
        launch: DriverLaunch,
    ) -> (PathBuf, RunController, RunSummary, AgentSessionSummary) {
        let root = temporary_root(label);
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);
        let controller = RunController::new_with_harnesses(
            RunControllerConfig {
                scenarios_dir: scenarios,
                data_dir: data,
                driver: DriverLaunch::new("/bin/false"),
            },
            vec![HarnessProfile {
                id: "fixture".to_owned(),
                display_name: "Fixture".to_owned(),
                launch,
                models: BTreeMap::from([("test".to_owned(), "fixture/test".to_owned())]),
            }],
            BTreeMap::from([("test".to_owned(), "Test".to_owned())]),
        )
        .unwrap();
        let explore = controller
            .prepare(PrepareRunRequest {
                scenario_id: "catalog".to_owned(),
            })
            .await
            .unwrap();
        let session = controller
            .start_agent_session(
                &explore.id,
                StartAgentSessionRequest::default(),
                WorkbenchOrigin::Nushell,
            )
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let observed = controller
                .list_agent_sessions(&explore.id)
                .into_iter()
                .find(|candidate| candidate.id == session.id)
                .unwrap();
            if observed.status == AgentSessionStatus::Ready && observed.active {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "interactive fixture session did not become ready"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        (root, controller, explore, session)
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn turn_scoped_driver_failure_keeps_the_native_session_ready() {
        let (root, controller, explore, session) =
            start_interactive_fixture("turn-scoped-driver-failure", interactive_fixture_launch())
                .await;
        let failed = controller
            .start_agent_turn(
                &explore.id,
                &session.id,
                StartAgentTurnRequest {
                    prompt: "turn-scoped-failure".to_owned(),
                    input: None,
                },
                WorkbenchOrigin::Nushell,
            )
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let detail = controller.agent_session(&explore.id, &session.id).unwrap();
            let turn = detail
                .turns
                .iter()
                .find(|turn| turn.id == failed.id)
                .unwrap();
            if turn.status == AgentTurnStatus::Failed
                && detail.summary.status == AgentSessionStatus::Ready
            {
                assert_eq!(turn.outcome.as_deref(), Some("failed"));
                assert!(
                    turn.error
                        .as_deref()
                        .is_some_and(|error| { error.contains("fixture_turn_failed") })
                );
                let terminal = detail
                    .events
                    .iter()
                    .find(|event| {
                        event.kind == "agent.turn.finished" && event.payload["turnId"] == failed.id
                    })
                    .unwrap();
                assert_eq!(terminal.payload["driverFailure"]["scope"], "turn");
                break;
            }
            assert!(
                Instant::now() < deadline,
                "turn-scoped failure did not terminalize without failing the session"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let recovered = controller
            .start_agent_turn(
                &explore.id,
                &session.id,
                StartAgentTurnRequest {
                    prompt: "the next turn still works".to_owned(),
                    input: None,
                },
                WorkbenchOrigin::Nushell,
            )
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let detail = controller.agent_session(&explore.id, &session.id).unwrap();
            if detail
                .turns
                .iter()
                .any(|turn| turn.id == recovered.id && turn.status == AgentTurnStatus::Completed)
            {
                assert_eq!(detail.summary.status, AgentSessionStatus::Ready);
                assert_eq!(detail.summary.turn_count, 2);
                break;
            }
            assert!(
                Instant::now() < deadline,
                "session did not accept a turn after a turn-scoped failure"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        controller.cancel(&explore.id).unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn turn_scoped_failure_cannot_finalize_incomplete_capability_evidence() {
        let (root, controller, explore, session) = start_interactive_fixture(
            "turn-scoped-failure-evidence-error",
            interactive_fixture_launch(),
        )
        .await;
        let state = controller.agent_session_state(&session.id).unwrap();
        let turn = controller
            .start_agent_turn(
                &explore.id,
                &session.id,
                StartAgentTurnRequest {
                    prompt: "turn-scoped-failure-with-evidence-error".to_owned(),
                    input: None,
                },
                WorkbenchOrigin::Nushell,
            )
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if lock(&state.events).iter().any(|event| {
                event.kind == "observation.assistant.delta" && event.payload["turnId"] == turn.id
            }) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "turn did not begin before the injected evidence failure"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        *lock(&state.evidence_error) =
            Some("injected capability evidence persistence failure".to_owned());

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let detail = controller.agent_session(&explore.id, &session.id).unwrap();
            if detail.summary.status == AgentSessionStatus::Failed {
                assert!(
                    detail
                        .summary
                        .error
                        .as_deref()
                        .is_some_and(|error| error.contains("capability evidence"))
                );
                assert_ne!(
                    detail.turns[0].summary.outcome.as_deref(),
                    Some("completed")
                );
                break;
            }
            assert!(
                Instant::now() < deadline,
                "incomplete capability evidence did not fail the session"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        controller.cancel(&explore.id).unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancelling_a_workspace_joins_session_actors_before_finalization() {
        let (root, controller, explore, session) = start_interactive_fixture(
            "workspace-cancel-joins-agents",
            interactive_fixture_launch(),
        )
        .await;
        let session_state = controller.agent_session_state(&session.id).unwrap();
        let workspace = controller.state(&explore.id).unwrap();

        controller.cancel(&explore.id).unwrap();

        assert!(lock(&session_state.actor).complete);
        assert!(lock(&session_state.actor).handle.is_none());
        assert!(matches!(
            lock(&session_state.summary).status,
            AgentSessionStatus::Closed | AgentSessionStatus::Interrupted
        ));
        assert!(lock(&workspace.active_agent_session_id).is_none());
        assert!(workspace.bundle_dir.join("final").is_dir());
        assert_eq!(lock(&workspace.summary).status, RunStatus::Cancelled);

        lock(&session_state.summary).status = AgentSessionStatus::Ready;
        assert!(matches!(
            controller.activate_agent_session(&explore.id, &session.id, WorkbenchOrigin::Nushell),
            Err(RunError::RunUnavailable(_))
        ));

        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn workspace_cancel_finalizes_after_an_agent_actor_panics() {
        let root = temporary_root("workspace-cancel-agent-panic");
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
        let explore = controller
            .prepare(PrepareRunRequest {
                scenario_id: "catalog".to_owned(),
            })
            .await
            .unwrap();
        let workspace = controller.state(&explore.id).unwrap();
        let session_dir = workspace
            .bundle_dir
            .join("agent-sessions")
            .join("session-panic");
        fs::create_dir_all(session_dir.join("turns")).unwrap();
        let session = Arc::new(test_agent_session_state(&session_dir));
        {
            let mut summary = lock(&session.summary);
            summary.id = "session-panic".to_owned();
            summary.workspace_id.clone_from(&explore.id);
            summary.status = AgentSessionStatus::Ready;
            summary.active = true;
            summary.turn_count = 0;
        }
        lock(&session.turns).clear();
        *lock(&session.actor) = AgentActorRegistration {
            complete: true,
            handle: Some(thread::spawn(|| panic!("injected agent actor panic"))),
        };
        persist_agent_session(&session).unwrap();
        lock(&workspace.agent_sessions)
            .insert("session-panic".to_owned(), Arc::downgrade(&session));
        lock(&controller.inner.agent_sessions)
            .insert("session-panic".to_owned(), Arc::clone(&session));
        *lock(&workspace.active_agent_session_id) = Some("session-panic".to_owned());
        persist_active_agent_session(&workspace, Some("session-panic")).unwrap();

        let error = controller.cancel(&explore.id).unwrap_err();
        assert!(error.to_string().contains("actor panicked"));

        let cancelled = controller.get(&explore.id).unwrap();
        assert_eq!(cancelled.summary.status, RunStatus::Cancelled);
        assert!(
            cancelled
                .summary
                .error
                .as_deref()
                .is_some_and(|error| error.contains("actor panicked"))
        );
        assert_eq!(cancelled.score.unwrap()["cancelled"], true);
        assert!(workspace.bundle_dir.join("final").is_dir());
        assert!(lock(&workspace.capabilities).is_empty());
        assert!(lock(&workspace.active_agent_session_id).is_none());
        assert!(controller.cancel(&explore.id).is_ok());

        drop(controller);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn terminal_input_is_serialized_with_turn_finalization_and_reservation_release() {
        let root = temporary_root("terminal-input-finalization-race");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);
        let controller = RunController::new_with_harnesses(
            RunControllerConfig {
                scenarios_dir: scenarios,
                data_dir: data,
                driver: DriverLaunch::new("/bin/false"),
            },
            Vec::new(),
            BTreeMap::new(),
        )
        .unwrap();
        let workspace = Arc::new(test_run_state(&root.join("workspace-run")));
        lock(&workspace.summary).status = RunStatus::Exploring;
        let session_dir = workspace.bundle_dir.join("agent-sessions/session-1");
        fs::create_dir_all(session_dir.join("turns/turn-1")).unwrap();
        let session = Arc::new(test_agent_session_state(&session_dir));
        lock(&session.summary).workspace_id = "run-events".to_owned();
        *lock(&workspace.active_agent_turn) = Some(AgentTurnAttribution {
            session_id: "session-1".to_owned(),
            turn_id: "turn-1".to_owned(),
        });
        lock(&workspace.agent_sessions).insert("session-1".to_owned(), Arc::downgrade(&session));
        lock(&controller.inner.runs).insert("run-events".to_owned(), Arc::clone(&workspace));
        lock(&controller.inner.agent_sessions).insert("session-1".to_owned(), Arc::clone(&session));

        let (locked_tx, locked_rx) = mpsc::channel();
        let finalizer_workspace = Arc::clone(&workspace);
        let finalizer_session = Arc::clone(&session);
        let finalizer = thread::spawn(move || {
            let mut reservation =
                ActiveAgentTurnGuard::new(&finalizer_workspace, "session-1", "turn-1").unwrap();
            reservation
                .finish(|| {
                    locked_tx.send(()).unwrap();
                    thread::sleep(Duration::from_millis(100));
                    assert!(
                        lock(&finalizer_session.turns)[0]
                            .human_intervention_at_ms
                            .is_none()
                    );
                    persist_agent_turn_terminal_state(
                        &finalizer_session,
                        "turn-1",
                        AgentTurnStatus::Completed,
                        Some("completed"),
                        None,
                    )
                })
                .unwrap();
        });
        locked_rx.recv().unwrap();

        controller.note_terminal_input("run-events").unwrap();
        finalizer.join().unwrap();

        let turn = &lock(&session.turns)[0];
        assert_eq!(turn.status, AgentTurnStatus::Completed);
        assert!(turn.human_intervention_at_ms.is_none());
        assert!(lock(&workspace.active_agent_turn).is_none());
        assert!(
            lock(&session.events)
                .iter()
                .all(|event| event.kind != "agent.turn.human-intervention")
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn activation_event_failure_rolls_back_durable_and_in_memory_selection() {
        let root = temporary_root("agent-activation-rollback");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);
        let controller = RunController::new_with_harnesses(
            RunControllerConfig {
                scenarios_dir: scenarios,
                data_dir: data,
                driver: DriverLaunch::new("/bin/false"),
            },
            Vec::new(),
            BTreeMap::new(),
        )
        .unwrap();
        let workspace = Arc::new(test_run_state(&root.join("workspace-run")));
        lock(&workspace.summary).status = RunStatus::Exploring;
        lock(&controller.inner.runs).insert("run-events".to_owned(), Arc::clone(&workspace));
        record_event(&workspace, "test.ready", json!({})).unwrap();

        let make_session = |id: &str| {
            let bundle = workspace.bundle_dir.join("agent-sessions").join(id);
            fs::create_dir_all(bundle.join("turns")).unwrap();
            let state = Arc::new(test_agent_session_state(&bundle));
            {
                let mut summary = lock(&state.summary);
                summary.id = id.to_owned();
                summary.workspace_id = "run-events".to_owned();
                summary.status = AgentSessionStatus::Ready;
            }
            lock(&state.turns).clear();
            state
        };
        let previous = make_session("session-previous");
        let target = make_session("session-target");
        for state in [&previous, &target] {
            let id = lock(&state.summary).id.clone();
            lock(&workspace.agent_sessions).insert(id.clone(), Arc::downgrade(state));
            lock(&controller.inner.agent_sessions).insert(id, Arc::clone(state));
        }
        *lock(&workspace.active_agent_session_id) = Some("session-previous".to_owned());
        persist_active_agent_session(&workspace, Some("session-previous")).unwrap();

        let event_log = workspace.bundle_dir.join("events.jsonl");
        fs::remove_file(&event_log).unwrap();
        fs::create_dir(&event_log).unwrap();
        assert!(
            controller
                .activate_agent_session("run-events", "session-target", WorkbenchOrigin::Nushell,)
                .is_err()
        );

        assert_eq!(
            lock(&workspace.active_agent_session_id).as_deref(),
            Some("session-previous")
        );
        let durable = read_optional_json(&workspace.bundle_dir.join("active-agent-session.json"))
            .unwrap()
            .unwrap();
        assert_eq!(durable["sessionId"], "session-previous");

        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn closing_a_cold_start_cancels_resolver_and_driver_waits() {
        async fn wait_for_closed(controller: &RunController, workspace_id: &str, session_id: &str) {
            let deadline = Instant::now() + Duration::from_secs(2);
            loop {
                let detail = controller.agent_session(workspace_id, session_id).unwrap();
                let closed_events = detail
                    .events
                    .iter()
                    .filter(|event| event.kind == "agent.session.closed")
                    .count();
                if detail.summary.status == AgentSessionStatus::Closed && closed_events == 1 {
                    assert!(detail.events.iter().any(|event| {
                        event.kind == "agent.session.closed" && event.payload["during"] == "startup"
                    }));
                    return;
                }
                assert!(
                    Instant::now() < deadline,
                    "cold-start session did not close promptly"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        let root = temporary_root("cold-start-cancellation");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);
        let models = BTreeMap::from([("test".to_owned(), "Test".to_owned())]);

        let mut stalled_driver = DriverLaunch::new("/bin/sh");
        stalled_driver.args = vec!["-c".into(), "sleep 30".into()];
        let controller = RunController::new_with_harnesses(
            RunControllerConfig {
                scenarios_dir: scenarios.clone(),
                data_dir: data.clone(),
                driver: DriverLaunch::new("/bin/false"),
            },
            vec![HarnessProfile {
                id: "fixture".to_owned(),
                display_name: "Fixture".to_owned(),
                launch: stalled_driver,
                models: BTreeMap::from([("test".to_owned(), "fixture/test".to_owned())]),
            }],
            models.clone(),
        )
        .unwrap();
        let explore = controller
            .prepare(PrepareRunRequest {
                scenario_id: "catalog".to_owned(),
            })
            .await
            .unwrap();
        let session = controller
            .start_agent_session(
                &explore.id,
                StartAgentSessionRequest::default(),
                WorkbenchOrigin::Nushell,
            )
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let detail = controller.agent_session(&explore.id, &session.id).unwrap();
            if detail
                .events
                .iter()
                .any(|event| event.kind == "agent.session.starting")
            {
                break;
            }
            assert!(Instant::now() < deadline, "driver startup did not begin");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        controller
            .close_agent_session(&explore.id, &session.id)
            .unwrap();
        wait_for_closed(&controller, &explore.id, &session.id).await;
        drop(controller);

        let resolver_started = root.join("resolver-started");
        let mut resolver = DriverLaunch::new("/bin/sh");
        resolver.args = vec![
            "-c".into(),
            "printf started > \"$STARTED\"; sleep 30; printf '%s\\n' '{\"status\":\"ready\",\"source\":\"test\",\"environment\":{\"TOKEN\":\"secret\"}}'"
                .into(),
        ];
        resolver
            .env
            .push(("STARTED".into(), resolver_started.clone().into_os_string()));
        let resolver_data = root.join("resolver-runs");
        fs::create_dir(&resolver_data).unwrap();
        let controller = RunController::new_with_harnesses_and_model_access(
            RunControllerConfig {
                scenarios_dir: scenarios,
                data_dir: resolver_data,
                driver: DriverLaunch::new("/bin/false"),
            },
            vec![HarnessProfile {
                id: "fixture".to_owned(),
                display_name: "Fixture".to_owned(),
                launch: DriverLaunch::new("/bin/false"),
                models: BTreeMap::from([("test".to_owned(), "fixture/test".to_owned())]),
            }],
            models,
            vec![ModelAccessProvider {
                id: "gateway".to_owned(),
                display_name: "Gateway".to_owned(),
                resolver: Some(resolver),
                environment_names: vec!["TOKEN".to_owned()],
                setup_hint: "Connect".to_owned(),
            }],
            BTreeMap::from([("fixture".to_owned(), "gateway".to_owned())]),
        )
        .unwrap();
        let explore = controller
            .prepare(PrepareRunRequest {
                scenario_id: "catalog".to_owned(),
            })
            .await
            .unwrap();
        let session = controller
            .start_agent_session(
                &explore.id,
                StartAgentSessionRequest::default(),
                WorkbenchOrigin::Nushell,
            )
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while !resolver_started.is_file() {
            assert!(
                Instant::now() < deadline,
                "model-access resolver did not begin"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        controller
            .close_agent_session(&explore.id, &session.id)
            .unwrap();
        wait_for_closed(&controller, &explore.id, &session.id).await;
        drop(controller);
        fs::remove_dir_all(root).unwrap();
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
                harness_id: None,
                model_profile_id: None,
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
            selection: Mutex::new(WorkbenchSelection {
                harness_id: None,
                model_profile_id: None,
                comparison_harness_ids: Vec::new(),
            }),
            events: Mutex::new(Vec::new()),
            sender,
            cancel: CancellationToken::new(),
            bundle_dir: root.to_path_buf(),
            agent_session_directories: AgentSessionDirectoryAnchor::open(root.to_path_buf())
                .unwrap(),
            workspace,
            output: "result.json".into(),
            initial_snapshot,
            capabilities: Mutex::new(Vec::new()),
            secret_values: Mutex::new(Vec::new()),
            agent_sessions: Mutex::new(HashMap::new()),
            active_agent_session_id: Mutex::new(None),
            active_agent_turn: Mutex::new(None),
            capability_attributions: Mutex::new(HashMap::new()),
            reusable_explore: false,
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
    fn startup_progress_does_not_extend_the_original_deadline() {
        let script = r#"
i=1
while [ "$i" -le 20 ]; do
  printf '{"protocolVersion":1,"sequence":%s,"causedBy":null,"type":"startup.event","phase":"adapter","status":"running","detail":null}\n' "$i"
  i=$((i + 1))
  sleep 0.02
done
sleep 30
"#;
        let mut driver = DriverProcess::spawn("/bin/sh", ["-c", script]).unwrap();
        let cancel = CancellationToken::new();
        let first = receive_until_deadline(
            &mut driver,
            Instant::now() + Duration::from_secs(1),
            &cancel,
        )
        .expect("fixture should emit startup progress")
        .expect("fixture should not end before startup progress");
        assert!(matches!(first.parsed.body, DriverBody::StartupEvent { .. }));
        let started = Instant::now();
        let deadline = started + Duration::from_millis(80);
        loop {
            match receive_until_deadline(&mut driver, deadline, &cancel) {
                Ok(Some(message)) => {
                    assert!(matches!(
                        message.parsed.body,
                        DriverBody::StartupEvent { .. }
                    ));
                }
                Err(ProcessError::Timeout) => break,
                other => panic!("unexpected startup receive result: {other:?}"),
            }
        }
        assert!(started.elapsed() < Duration::from_millis(500));
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
            "agent.turn.finished",
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
    fn driver_secrets_exclude_routine_process_environment() {
        let mut launch = DriverLaunch::new("driver");
        launch.env.extend([
            ("PATH".into(), "/routine/bin".into()),
            ("HOME".into(), "/routine/home".into()),
            ("PROVIDER_TOKEN".into(), "provider-secret".into()),
        ]);

        assert_eq!(
            driver_secret_values(&launch),
            vec![b"provider-secret".to_vec()]
        );
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

    #[test]
    fn confined_read_fallback_fails_closed_for_existing_outputs() {
        let root = temporary_root("output-existing");
        let workspace = root.join("workspace");
        fs::create_dir(&workspace).unwrap();
        let output = workspace.join("result.json");

        assert_eq!(
            read_optional_confined_file_without_handle_relative_support(&output).unwrap(),
            None
        );
        fs::write(&output, br#"{"safe":true}"#).unwrap();

        assert!(matches!(
            read_optional_confined_file_without_handle_relative_support(&output),
            Err(RunError::ConfinedReadUnavailable(_))
        ));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn run_controller_rejects_platforms_without_race_free_confined_reads() {
        assert!(matches!(
            require_race_free_confined_reads(false),
            Err(RunError::UnsupportedPlatform)
        ));
        assert!(require_race_free_confined_reads(true).is_ok());
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

    #[test]
    fn capability_completion_keeps_the_turn_that_started_the_call() {
        let root = temporary_root("capability-turn-attribution");
        let workspace = Arc::new(test_run_state(&root));
        let session_dir = root.join("agent-sessions").join("session-1");
        fs::create_dir_all(session_dir.join("turns")).unwrap();
        let (sender, _) = broadcast::channel(8);
        let session = Arc::new(AgentSessionState {
            summary: Mutex::new(AgentSessionSummary {
                id: "session-1".to_owned(),
                workspace_id: "run-events".to_owned(),
                harness_id: "fixture".to_owned(),
                model_profile_id: "test".to_owned(),
                model_id: "fixture/test".to_owned(),
                status: AgentSessionStatus::Running,
                active: true,
                created_at_ms: 1,
                updated_at_ms: 1,
                turn_count: 1,
                error: None,
            }),
            turns: Mutex::new(Vec::new()),
            events: Mutex::new(Vec::new()),
            sender,
            commands: Mutex::new(None),
            lifecycle_cancel: CancellationToken::new(),
            turn_cancel: Mutex::new(None),
            actor: Mutex::new(AgentActorRegistration {
                complete: true,
                handle: None,
            }),
            actor_registered: Condvar::new(),
            evidence_error: Mutex::new(None),
            evidence_root: AgentSessionEvidenceRoot::open(session_dir.clone()).unwrap(),
            secret_values: Mutex::new(Vec::new()),
        });
        lock(&workspace.agent_sessions).insert("session-1".to_owned(), Arc::downgrade(&session));
        *lock(&workspace.active_agent_turn) = Some(AgentTurnAttribution {
            session_id: "session-1".to_owned(),
            turn_id: "turn-1".to_owned(),
        });

        let observe = source_observer(workspace.clone(), "catalog", "agent");
        observe(
            "mcp.tool.started",
            json!({
                "callId": "catalog-call-1",
                "name": "list",
                "arguments": {}
            }),
        );
        *lock(&workspace.active_agent_turn) = None;
        observe(
            "mcp.tool.completed",
            json!({
                "callId": "catalog-call-1",
                "name": "list",
                "isError": false,
                "result": { "items": [] }
            }),
        );

        let workspace_events = lock(&workspace.events).clone();
        let session_events = lock(&session.events).clone();
        assert_eq!(workspace_events.len(), 2);
        assert_eq!(session_events.len(), 2);
        for event in workspace_events.iter().chain(&session_events) {
            assert_eq!(event.payload["sessionId"], "session-1");
            assert_eq!(event.payload["turnId"], "turn-1");
            assert_eq!(event.payload["callId"], "catalog-call-1");
        }
        assert!(lock(&workspace.capability_attributions).is_empty());

        drop(observe);
        drop(session);
        drop(workspace);
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
                &StartPreparedRunRequest {
                    model_id: Some("test/model".to_owned()),
                    harness_id: None,
                    model_profile_id: None,
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
        let controller = RunController::new_with_harnesses(
            RunControllerConfig {
                scenarios_dir: scenarios,
                data_dir: data,
                driver: DriverLaunch::new("/driver-must-not-start"),
            },
            vec![HarnessProfile {
                id: "v0".to_owned(),
                display_name: "v0".to_owned(),
                launch: DriverLaunch::new("/driver-must-not-start"),
                models: BTreeMap::from([("haiku".to_owned(), "v0/haiku".to_owned())]),
            }],
            BTreeMap::from([("haiku".to_owned(), "Haiku".to_owned())]),
        )
        .unwrap();
        let prepared = controller
            .prepare(PrepareRunRequest {
                scenario_id: "catalog".to_owned(),
            })
            .await
            .unwrap();
        let before_adapter = controller
            .get(&prepared.id)
            .unwrap()
            .assembly
            .harness
            .adapter;
        let state = controller.state(&prepared.id).unwrap();
        fs::create_dir(state.bundle_dir.join("assembly.json.tmp")).unwrap();

        controller
            .start_prepared(
                &prepared.id,
                &StartPreparedRunRequest {
                    model_id: None,
                    harness_id: Some("v0".to_owned()),
                    model_profile_id: Some("haiku".to_owned()),
                },
            )
            .unwrap_err();

        let detail = controller.get(&prepared.id).unwrap();
        assert_eq!(detail.summary.status, RunStatus::Exploring);
        assert!(detail.summary.model_id.is_empty());
        assert!(detail.summary.harness_id.is_none());
        assert!(detail.summary.model_profile_id.is_none());
        assert_eq!(detail.assembly.harness.adapter, before_adapter);
        assert!(detail.assembly.harness.model_id.is_none());
        let manifest: RunSummary =
            serde_json::from_slice(&fs::read(state.bundle_dir.join("manifest.json")).unwrap())
                .unwrap();
        assert_eq!(manifest.status, RunStatus::Exploring);
        assert!(manifest.model_id.is_empty());
        assert!(manifest.harness_id.is_none());
        assert!(manifest.model_profile_id.is_none());
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
            bundle.join("initial/result.json"),
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
            harness_id: None,
            model_profile_id: None,
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
        assert!(!bundle.join("initial").exists());
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
            harness_id: None,
            model_profile_id: None,
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

    #[tokio::test]
    async fn lagged_evaluation_subscriptions_replay_the_missing_durable_suffix() {
        use futures_util::StreamExt;

        let root = temporary_root("lagged-evaluation-subscribe");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);
        let harness = |id: &str| HarnessProfile {
            id: id.to_owned(),
            display_name: id.to_owned(),
            launch: DriverLaunch::new("/bin/false"),
            models: BTreeMap::from([("haiku".to_owned(), format!("{id}/haiku"))]),
        };
        let controller = RunController::new_with_harnesses(
            RunControllerConfig {
                scenarios_dir: scenarios,
                data_dir: data,
                driver: DriverLaunch::new("/bin/false"),
            },
            vec![harness("v0"), harness("eve")],
            BTreeMap::from([("haiku".to_owned(), "Haiku".to_owned())]),
        )
        .unwrap();
        let explore = controller
            .prepare(PrepareRunRequest {
                scenario_id: "catalog".to_owned(),
            })
            .await
            .unwrap();
        let evaluation = controller
            .start_evaluation(StartEvaluationRequest {
                scenario_id: "catalog".to_owned(),
                model_profile_id: "haiku".to_owned(),
                source_workspace_id: explore.id,
                harness_ids: vec!["v0".to_owned(), "eve".to_owned()],
            })
            .unwrap();
        loop {
            if controller
                .get_evaluation(&evaluation.id)
                .unwrap()
                .summary
                .status
                .is_finished()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let (history, receiver) = controller.subscribe_evaluation(&evaluation.id).unwrap();
        let state = controller.evaluation_state(&evaluation.id).unwrap();
        for index in 0..300 {
            record_evaluation_event(&state, "test.lag", json!({ "index": index })).unwrap();
        }
        let expected = lock(&state.events).len();
        let events =
            crate::evaluation_event_stream(controller.clone(), evaluation.id, history, receiver)
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
    fn harness_registry_rejects_duplicates_and_unknown_model_profiles() {
        let root = temporary_root("harness-registry");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);
        let config = || RunControllerConfig {
            scenarios_dir: scenarios.clone(),
            data_dir: data.clone(),
            driver: DriverLaunch::new("/bin/false"),
        };
        let profile = HarnessProfile {
            id: "same".to_owned(),
            display_name: "Same".to_owned(),
            launch: DriverLaunch::new("/bin/false"),
            models: BTreeMap::from([("missing".to_owned(), "model".to_owned())]),
        };
        assert!(
            RunController::new_with_harnesses(config(), vec![profile.clone()], BTreeMap::new())
                .is_err()
        );
        assert!(
            RunController::new_with_harnesses(
                config(),
                vec![profile.clone(), profile],
                BTreeMap::from([("missing".to_owned(), "Missing".to_owned())]),
            )
            .is_err()
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn workbench_selection_persists_and_compare_records_its_origin() {
        let root = temporary_root("workbench-selection");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);
        let harnesses = || {
            ["v0", "eve"]
                .into_iter()
                .map(|id| HarnessProfile {
                    id: id.to_owned(),
                    display_name: id.to_owned(),
                    launch: DriverLaunch::new("/bin/false"),
                    models: BTreeMap::from([("haiku".to_owned(), format!("{id}/haiku"))]),
                })
                .collect::<Vec<_>>()
        };
        let config = || RunControllerConfig {
            scenarios_dir: scenarios.clone(),
            data_dir: data.clone(),
            driver: DriverLaunch::new("/bin/false"),
        };
        let models = BTreeMap::from([("haiku".to_owned(), "Haiku".to_owned())]);
        let controller =
            RunController::new_with_harnesses(config(), harnesses(), models.clone()).unwrap();
        let explore = controller
            .prepare(PrepareRunRequest {
                scenario_id: "catalog".to_owned(),
            })
            .await
            .unwrap();
        let initial = controller.workbench(&explore.id).unwrap();
        assert_eq!(initial.selection.model_profile_id.as_deref(), Some("haiku"));
        assert_eq!(initial.selection.comparison_harness_ids, ["v0", "eve"]);
        let selected = controller
            .update_workbench_selection(
                &explore.id,
                UpdateWorkbenchSelectionRequest {
                    harness_id: Some("v0".to_owned()),
                    model_profile_id: Some("haiku".to_owned()),
                    comparison_harness_ids: None,
                },
                WorkbenchOrigin::Browser,
            )
            .unwrap();
        assert_eq!(selected.harness_id.as_deref(), Some("v0"));
        let evaluation = controller
            .compare_workbench(
                &explore.id,
                CompareWorkbenchRequest {
                    model_profile_id: None,
                    harness_ids: Some(vec!["eve".to_owned(), "v0".to_owned()]),
                },
                WorkbenchOrigin::Nushell,
            )
            .unwrap();
        assert_eq!(evaluation.harness_ids, ["eve", "v0"]);
        assert!(
            lock(&controller.state(&explore.id).unwrap().events)
                .iter()
                .any(|event| {
                    event.kind == "workbench.evaluation.started"
                        && event.payload["origin"] == "nushell"
                })
        );
        let binding = controller.terminal_binding(&explore.id).unwrap();
        assert!(controller.workbench_grant_allows(&binding.control_token, &explore.id));
        let evidence = fs::read_to_string(
            controller
                .state(&explore.id)
                .unwrap()
                .bundle_dir
                .join("events.jsonl"),
        )
        .unwrap();
        assert!(!evidence.contains(&binding.control_token));
        controller.revoke_workbench_grant(&binding.control_token);
        assert!(!controller.workbench_grant_allows(&binding.control_token, &explore.id));
        let bundle_dir = controller.state(&explore.id).unwrap().bundle_dir.clone();
        drop(controller);

        let reopened =
            RunController::new_with_harnesses(config(), harnesses(), models.clone()).unwrap();
        let persisted = reopened.workbench(&explore.id).unwrap().selection;
        assert_eq!(persisted.harness_id.as_deref(), Some("v0"));
        assert_eq!(persisted.comparison_harness_ids, ["v0", "eve"]);
        drop(reopened);

        fs::write(
            bundle_dir.join("workbench.json"),
            br#"{
  "harnessId": "removed-harness",
  "modelProfileId": "removed-model",
  "comparisonHarnessIds": ["removed-harness"]
}
"#,
        )
        .unwrap();
        let repaired = RunController::new_with_harnesses(
            config(),
            harnesses(),
            BTreeMap::from([("haiku".to_owned(), "Haiku".to_owned())]),
        )
        .unwrap();
        let repaired = repaired.workbench(&explore.id).unwrap().selection;
        assert_eq!(repaired.harness_id.as_deref(), Some("eve"));
        assert_eq!(repaired.model_profile_id.as_deref(), Some("haiku"));
        assert_eq!(repaired.comparison_harness_ids, ["v0", "eve"]);
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_session_start_event_persistence_retains_a_terminal_session() {
        let root = temporary_root("agent-session-start-event-failure");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);
        let controller = RunController::new_with_harnesses(
            RunControllerConfig {
                scenarios_dir: scenarios,
                data_dir: data,
                driver: DriverLaunch::new("/bin/false"),
            },
            vec![HarnessProfile {
                id: "fixture".to_owned(),
                display_name: "Fixture".to_owned(),
                launch: DriverLaunch::new("/bin/false"),
                models: BTreeMap::from([("test".to_owned(), "fixture/test".to_owned())]),
            }],
            BTreeMap::from([("test".to_owned(), "Test".to_owned())]),
        )
        .unwrap();
        let explore = controller
            .prepare(PrepareRunRequest {
                scenario_id: "catalog".to_owned(),
            })
            .await
            .unwrap();
        let workspace = controller.state(&explore.id).unwrap();
        let event_log = workspace.bundle_dir.join("events.jsonl");
        fs::remove_file(&event_log).unwrap();
        fs::create_dir(&event_log).unwrap();

        let error = controller
            .start_agent_session(
                &explore.id,
                StartAgentSessionRequest::default(),
                WorkbenchOrigin::Nushell,
            )
            .unwrap_err();

        assert!(matches!(error, RunError::Io(_)));
        let sessions = controller.list_agent_sessions(&explore.id);
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].status, AgentSessionStatus::Failed);
        assert_eq!(
            sessions[0].error.as_deref(),
            Some("workspace session-start evidence could not be persisted")
        );
        let state = controller.agent_session_state(&sessions[0].id).unwrap();
        assert!(lock(&state.commands).is_none());
        let manifest: AgentSessionManifest = serde_json::from_slice(
            &fs::read(state.evidence_root.display_path().join("manifest.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(manifest.summary.status, AgentSessionStatus::Failed);
        assert!(
            lock(&state.events)
                .iter()
                .any(|event| event.kind == "agent.session.failed")
        );

        drop(workspace);
        drop(controller);
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn interactive_agent_session_reuses_one_native_session_across_turns_and_replays() {
        const MARKDOWN_ANSWER: &str = "# Fixture answer\n\n**Gamma** leads the catalog.\n\n| Item | Score |\n| --- | ---: |\n| `gamma` | **8** |\n| `alpha` | 3 |";
        const PRIOR_MARKDOWN_ANSWER: &str =
            "## Prior conclusion\n\n`gamma` remained highest at **8**.";

        let root = temporary_root("interactive-agent-session");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);
        let mut launch = interactive_fixture_launch();
        launch
            .env
            .push(("AGENT_API_TOKEN".into(), "turn-secret".into()));
        let harnesses = || {
            vec![HarnessProfile {
                id: "fixture".to_owned(),
                display_name: "Fixture".to_owned(),
                launch: launch.clone(),
                models: BTreeMap::from([("test".to_owned(), "fixture/test".to_owned())]),
            }]
        };
        let config = || RunControllerConfig {
            scenarios_dir: scenarios.clone(),
            data_dir: data.clone(),
            driver: DriverLaunch::new("/bin/false"),
        };
        let models = BTreeMap::from([("test".to_owned(), "Test".to_owned())]);
        let controller =
            RunController::new_with_harnesses(config(), harnesses(), models.clone()).unwrap();
        let explore = controller
            .prepare(PrepareRunRequest {
                scenario_id: "catalog".to_owned(),
            })
            .await
            .unwrap();
        let starting_session = controller
            .start_agent_session(
                &explore.id,
                StartAgentSessionRequest {
                    harness_id: None,
                    model_profile_id: None,
                },
                WorkbenchOrigin::Nushell,
            )
            .unwrap();
        assert!(!starting_session.active);
        assert_eq!(starting_session.status, AgentSessionStatus::Starting);
        let deadline = Instant::now() + Duration::from_secs(2);
        let session = loop {
            let observed = controller
                .list_agent_sessions(&explore.id)
                .into_iter()
                .find(|session| session.id == starting_session.id)
                .unwrap();
            if observed.status == AgentSessionStatus::Ready && observed.active {
                break observed;
            }
            assert!(
                Instant::now() < deadline,
                "agent session did not become ready"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        };

        for (prompt, input) in [
            ("first turn turn-secret", json!({ "items": [1, 2] })),
            (
                "what did you conclude earlier?",
                json!({ "continue": true }),
            ),
        ] {
            let turn = controller
                .start_agent_turn(
                    &explore.id,
                    &session.id,
                    StartAgentTurnRequest {
                        prompt: prompt.to_owned(),
                        input: Some(input),
                    },
                    WorkbenchOrigin::Nushell,
                )
                .unwrap();
            let deadline = Instant::now() + Duration::from_secs(2);
            loop {
                let detail = controller.agent_session(&explore.id, &session.id).unwrap();
                let observed = detail.turns.iter().find(|item| item.id == turn.id).unwrap();
                if observed.status == AgentTurnStatus::Completed {
                    break;
                }
                assert!(Instant::now() < deadline, "agent turn did not finish");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        let cancelled = controller
            .start_agent_turn(
                &explore.id,
                &session.id,
                StartAgentTurnRequest {
                    prompt: "wait-for-abort-hostile-partial".to_owned(),
                    input: None,
                },
                WorkbenchOrigin::Nushell,
            )
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let detail = controller.agent_session(&explore.id, &session.id).unwrap();
            let observed = detail
                .turns
                .iter()
                .find(|item| item.id == cancelled.id)
                .unwrap();
            if observed.status == AgentTurnStatus::Running {
                break;
            }
            assert!(Instant::now() < deadline, "agent turn did not start");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        controller.note_terminal_input(&explore.id).unwrap();
        let intervened = controller.agent_session(&explore.id, &session.id).unwrap();
        assert!(
            intervened
                .turns
                .iter()
                .find(|turn| turn.id == cancelled.id)
                .unwrap()
                .human_intervention_at_ms
                .is_some()
        );
        assert!(intervened.events.iter().any(|event| {
            event.kind == "agent.turn.human-intervention"
                && event.payload["turnId"] == cancelled.id
                && event.payload["source"] == "terminal-input"
        }));
        controller
            .cancel_agent_turn(&explore.id, &session.id)
            .unwrap();
        loop {
            let detail = controller.agent_session(&explore.id, &session.id).unwrap();
            let observed = detail
                .turns
                .iter()
                .find(|item| item.id == cancelled.id)
                .unwrap();
            if observed.status == AgentTurnStatus::Cancelled {
                break;
            }
            assert!(Instant::now() < deadline, "agent turn was not cancelled");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        loop {
            let detail = controller.agent_session(&explore.id, &session.id).unwrap();
            if detail
                .events
                .iter()
                .filter(|event| event.kind == "agent.turn.finished")
                .count()
                == 3
                && detail.summary.status == AgentSessionStatus::Ready
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "cancelled turn evidence did not finalize"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let timed_out = controller
            .start_agent_turn(
                &explore.id,
                &session.id,
                StartAgentTurnRequest {
                    prompt: "wait-for-timeout-hostile-complete".to_owned(),
                    input: None,
                },
                WorkbenchOrigin::Nushell,
            )
            .unwrap();
        let timeout_deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let detail = controller.agent_session(&explore.id, &session.id).unwrap();
            let observed = detail
                .turns
                .iter()
                .find(|item| item.id == timed_out.id)
                .unwrap();
            if observed.status == AgentTurnStatus::Failed {
                break;
            }
            assert!(
                Instant::now() < timeout_deadline,
                "hostile completed turn did not preserve controller timeout"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let detail = controller.agent_session(&explore.id, &session.id).unwrap();
        assert_eq!(detail.summary.turn_count, 4);
        assert_eq!(detail.summary.status, AgentSessionStatus::Ready);
        assert_eq!(detail.turns[0].prompt, "first turn [REDACTED]");
        assert_eq!(
            detail.turns[0].presentation.response.as_deref(),
            Some(MARKDOWN_ANSWER)
        );
        assert_eq!(
            detail.turns[0].presentation.messages[0].text,
            MARKDOWN_ANSWER
        );
        assert!(detail.turns[0].presentation.messages[0].complete);
        assert_eq!(
            detail.turns[0].presentation.usage,
            Some(json!({
                "inputTokens": 7,
                "outputTokens": 21,
                "totalTokens": 28,
            }))
        );
        assert!(
            detail.turns[0]
                .presentation
                .activity
                .iter()
                .any(|activity| {
                    activity.kind == "native-action"
                        && activity.title == "Inspect catalog"
                        && activity.status == "completed"
                })
        );
        assert_eq!(
            detail.turns[0].presentation.completeness.assistant_output,
            AgentPresentationCompleteness::Complete
        );
        assert_eq!(
            detail.turns[0].presentation.completeness.usage,
            AgentPresentationCompleteness::Complete
        );
        assert_eq!(
            detail.turns[1].presentation.response.as_deref(),
            Some(PRIOR_MARKDOWN_ANSWER)
        );
        let cancelled_turn = detail
            .turns
            .iter()
            .find(|turn| turn.id == cancelled.id)
            .unwrap();
        assert_eq!(
            cancelled_turn.presentation.response.as_deref(),
            Some("# Fixture answer\n\n**Gamma** leads the catalog.\n\n")
        );
        assert_eq!(
            cancelled_turn.presentation.completeness.assistant_output,
            AgentPresentationCompleteness::Partial
        );
        assert_eq!(cancelled_turn.summary.outcome.as_deref(), Some("cancelled"));
        let timed_out_turn = detail
            .turns
            .iter()
            .find(|turn| turn.id == timed_out.id)
            .unwrap();
        assert_eq!(timed_out_turn.summary.outcome.as_deref(), Some("timed-out"));
        assert_eq!(
            timed_out_turn.presentation.response.as_deref(),
            Some(MARKDOWN_ANSWER)
        );
        for (turn_id, expected_outcome) in
            [(&cancelled.id, "cancelled"), (&timed_out.id, "timed-out")]
        {
            let terminal = detail
                .events
                .iter()
                .find(|event| {
                    event.kind == "agent.turn.finished"
                        && event.payload["turnId"] == turn_id.as_str()
                })
                .unwrap();
            assert_eq!(terminal.payload["outcome"], expected_outcome);
            assert_eq!(terminal.payload["driverOutcome"], "completed");
        }
        assert!(detail.turns[0].source_revision.starts_with("sha256:"));
        assert_eq!(
            detail.turns[0].capability_revisions["catalog"],
            "catalog-v2"
        );
        assert_eq!(
            detail
                .events
                .iter()
                .filter(|event| event.kind == "agent.session.ready")
                .count(),
            1
        );
        assert_eq!(
            detail
                .events
                .iter()
                .filter(|event| event.kind == "agent.turn.finished")
                .count(),
            4
        );
        let session_state = controller.agent_session_state(&session.id).unwrap();
        let bundle = session_state.evidence_root.display_path().to_path_buf();
        let evidence = fs::read_to_string(bundle.join("manifest.json")).unwrap()
            + &fs::read_to_string(bundle.join("events.jsonl")).unwrap();
        assert!(!evidence.contains("turn-secret"));
        for turn in &detail.turns {
            let turn_dir = bundle.join("turns").join(&turn.id);
            assert!(turn_dir.join("initial").is_dir());
            assert!(turn_dir.join("final").is_dir());
            assert!(turn_dir.join("diff.json").is_file());
            assert!(turn_dir.join("presentation.json").is_file());
        }
        let second = controller
            .start_agent_session(
                &explore.id,
                StartAgentSessionRequest {
                    harness_id: None,
                    model_profile_id: None,
                },
                WorkbenchOrigin::Nushell,
            )
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let observed = controller
                .list_agent_sessions(&explore.id)
                .into_iter()
                .find(|session| session.id == second.id)
                .unwrap();
            if observed.status == AgentSessionStatus::Ready && observed.active {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "second agent session did not become ready"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(controller.list_agent_sessions(&explore.id).len(), 2);
        assert!(
            controller
                .agent_session(&explore.id, &second.id)
                .unwrap()
                .summary
                .active
        );
        controller
            .activate_agent_session(&explore.id, &session.id, WorkbenchOrigin::Nushell)
            .unwrap();
        assert!(
            controller
                .agent_session(&explore.id, &session.id)
                .unwrap()
                .summary
                .active
        );
        assert_eq!(
            controller
                .agent_session(&explore.id, &session.id)
                .unwrap()
                .turns
                .len(),
            4
        );
        controller
            .close_agent_session(&explore.id, &second.id)
            .unwrap();
        controller
            .close_agent_session(&explore.id, &session.id)
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if controller
                .agent_session(&explore.id, &session.id)
                .unwrap()
                .summary
                .status
                == AgentSessionStatus::Closed
            {
                break;
            }
            assert!(Instant::now() < deadline, "agent session did not close");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        loop {
            if controller
                .agent_session(&explore.id, &second.id)
                .unwrap()
                .summary
                .status
                == AgentSessionStatus::Closed
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "second agent session did not close"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        drop(controller);

        let reopened =
            RunController::new_with_harnesses(config(), harnesses(), models.clone()).unwrap();
        assert!(
            reopened
                .workbench(&explore.id)
                .unwrap()
                .replay_agent_session
                .is_none()
        );
        let replayed = reopened.agent_session(&explore.id, &session.id).unwrap();
        assert_eq!(replayed.summary.status, AgentSessionStatus::Closed);
        assert_eq!(replayed.turns.len(), 4);
        assert_eq!(replayed.events.len(), detail.events.len() + 1);
        assert_eq!(
            replayed
                .turns
                .iter()
                .map(|turn| &turn.presentation)
                .collect::<Vec<_>>(),
            detail
                .turns
                .iter()
                .map(|turn| &turn.presentation)
                .collect::<Vec<_>>()
        );
        drop(reopened);
        let presentation_path = bundle
            .join("turns")
            .join(&detail.turns[0].id)
            .join("presentation.json");
        let mut tampered = read_optional_json(&presentation_path).unwrap().unwrap();
        tampered["response"] = json!("tampered response");
        fs::write(
            &presentation_path,
            serde_json::to_vec_pretty(&tampered).unwrap(),
        )
        .unwrap();
        let reopened = RunController::new_with_harnesses(config(), harnesses(), models).unwrap();
        assert_eq!(
            reopened
                .agent_session(&explore.id, &session.id)
                .unwrap()
                .turns[0]
                .presentation
                .response
                .as_deref(),
            Some(MARKDOWN_ANSWER)
        );
        let repaired: AgentTurnPresentation =
            serde_json::from_slice(&fs::read(&presentation_path).unwrap()).unwrap();
        assert_eq!(repaired.response.as_deref(), Some(MARKDOWN_ANSWER));
        drop(reopened);
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn interrupted_agent_session_becomes_the_workbench_replay_selection() {
        let root = temporary_root("interrupted-agent-replay-selection");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);
        let harnesses = || {
            vec![HarnessProfile {
                id: "fixture".to_owned(),
                display_name: "Fixture".to_owned(),
                launch: interactive_fixture_launch(),
                models: BTreeMap::from([("test".to_owned(), "fixture/test".to_owned())]),
            }]
        };
        let config = || RunControllerConfig {
            scenarios_dir: scenarios.clone(),
            data_dir: data.clone(),
            driver: DriverLaunch::new("/bin/false"),
        };
        let models = BTreeMap::from([("test".to_owned(), "Test".to_owned())]);
        let controller =
            RunController::new_with_harnesses(config(), harnesses(), models.clone()).unwrap();
        let explore = controller
            .prepare(PrepareRunRequest {
                scenario_id: "catalog".to_owned(),
            })
            .await
            .unwrap();
        let session = controller
            .start_agent_session(
                &explore.id,
                StartAgentSessionRequest::default(),
                WorkbenchOrigin::Nushell,
            )
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let observed = controller
                .list_agent_sessions(&explore.id)
                .into_iter()
                .find(|candidate| candidate.id == session.id)
                .unwrap();
            if observed.status == AgentSessionStatus::Ready && observed.active {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "agent session did not become ready"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let turn = controller
            .start_agent_turn(
                &explore.id,
                &session.id,
                StartAgentTurnRequest {
                    prompt: "replay this answer".to_owned(),
                    input: None,
                },
                WorkbenchOrigin::Nushell,
            )
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let detail = controller.agent_session(&explore.id, &session.id).unwrap();
            if detail.turns.iter().any(|candidate| {
                candidate.id == turn.id && candidate.status == AgentTurnStatus::Completed
            }) {
                break;
            }
            assert!(Instant::now() < deadline, "agent turn did not finish");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let expected = controller
            .agent_session(&explore.id, &session.id)
            .unwrap()
            .turns[0]
            .presentation
            .clone();
        let session_state = controller.agent_session_state(&session.id).unwrap();
        let bundle = session_state.evidence_root.display_path().to_path_buf();
        drop(controller);

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let manifest: AgentSessionManifest =
                serde_json::from_slice(&fs::read(bundle.join("manifest.json")).unwrap()).unwrap();
            if manifest.summary.status == AgentSessionStatus::Interrupted {
                break;
            }
            assert!(Instant::now() < deadline, "agent session did not stop");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let reopened = RunController::new_with_harnesses(config(), harnesses(), models).unwrap();
        let workbench = reopened.workbench(&explore.id).unwrap();
        assert!(workbench.active_agent_session.is_none());
        let replay = workbench.replay_agent_session.unwrap();
        assert_eq!(replay.id, session.id);
        assert_eq!(replay.status, AgentSessionStatus::Interrupted);
        assert!(!replay.active);
        assert_eq!(
            reopened
                .agent_session(&explore.id, &session.id)
                .unwrap()
                .turns[0]
                .presentation,
            expected
        );

        drop(reopened);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn missing_model_access_is_workbench_state_not_a_failed_evaluation() {
        let root = temporary_root("model-access-missing");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);
        let harness = |id: &str| HarnessProfile {
            id: id.to_owned(),
            display_name: id.to_owned(),
            launch: DriverLaunch::new("/bin/false"),
            models: BTreeMap::from([("haiku".to_owned(), format!("{id}/haiku"))]),
        };
        let controller = RunController::new_with_harnesses_and_model_access(
            RunControllerConfig {
                scenarios_dir: scenarios,
                data_dir: data,
                driver: DriverLaunch::new("/bin/false"),
            },
            vec![harness("v0"), harness("eve")],
            BTreeMap::from([("haiku".to_owned(), "Haiku".to_owned())]),
            vec![ModelAccessProvider {
                id: "gateway".to_owned(),
                display_name: "Gateway".to_owned(),
                resolver: None,
                environment_names: vec!["TOKEN".to_owned()],
                setup_hint: "Connect the gateway".to_owned(),
            }],
            BTreeMap::from([
                ("v0".to_owned(), "gateway".to_owned()),
                ("eve".to_owned(), "gateway".to_owned()),
            ]),
        )
        .unwrap();
        let explore = controller
            .prepare(PrepareRunRequest {
                scenario_id: "catalog".to_owned(),
            })
            .await
            .unwrap();
        let workbench = controller.workbench(&explore.id).unwrap();
        assert_eq!(workbench.model_access.len(), 1);
        assert_eq!(
            workbench.model_access[0].status,
            ModelAccessStatus::NeedsSetup
        );
        let error = controller
            .compare_workbench(
                &explore.id,
                CompareWorkbenchRequest::default(),
                WorkbenchOrigin::Browser,
            )
            .unwrap_err();
        assert!(matches!(error, RunError::ModelAccessUnavailable(_)));
        assert!(controller.list_evaluations().is_empty());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn resolverless_model_access_reads_allowlisted_ambient_environment() {
        let provider = ModelAccessProvider {
            id: "ambient".to_owned(),
            display_name: "Ambient".to_owned(),
            resolver: None,
            environment_names: vec!["PATH".to_owned()],
            setup_hint: "Provide PATH".to_owned(),
        };

        let resolution = resolve_model_access(&provider, true).unwrap();
        assert_eq!(resolution.status, ModelAccessStatus::Ready);
        assert!(
            resolution
                .environment
                .get("PATH")
                .is_some_and(|path| !path.is_empty())
        );
    }

    #[cfg(unix)]
    #[test]
    fn model_access_messages_are_redacted_against_resolver_environment() {
        use std::os::unix::fs::PermissionsExt;

        let root = temporary_root("model-access-message-redaction");
        let resolver_path = root.join("resolver.sh");
        fs::write(
            &resolver_path,
            "#!/bin/sh\nprintf '{\"status\":\"needs-setup\",\"message\":\"credential %s rejected\"}\\n' \"$TOKEN\"\n",
        )
        .unwrap();
        fs::set_permissions(&resolver_path, fs::Permissions::from_mode(0o700)).unwrap();
        let mut resolver = DriverLaunch::new(resolver_path);
        resolver.clear_env = true;
        resolver
            .env
            .push(("TOKEN".into(), "secret-model-token".into()));
        let provider = ModelAccessProvider {
            id: "gateway".to_owned(),
            display_name: "Gateway".to_owned(),
            resolver: Some(resolver),
            environment_names: vec!["TOKEN".to_owned()],
            setup_hint: "Connect the gateway".to_owned(),
        };

        let resolution = resolve_model_access(&provider, false).unwrap();
        let message = resolution.message.unwrap();
        assert!(!message.contains("secret-model-token"));
        assert!(message.contains("[REDACTED]"));
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn model_access_secrets_are_injected_only_into_the_resolved_launch() {
        use std::os::unix::fs::PermissionsExt;

        let root = temporary_root("model-access-ready");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);
        let resolver_path = root.join("resolver.sh");
        fs::write(
            &resolver_path,
            r#"#!/bin/sh
if [ "$1" = "resolve" ]; then
  printf '%s\n' '{"status":"ready","source":"test","environment":{"TOKEN":"secret-model-token"}}'
else
  printf '%s\n' '{"status":"ready","source":"test"}'
fi
"#,
        )
        .unwrap();
        fs::set_permissions(&resolver_path, fs::Permissions::from_mode(0o700)).unwrap();
        let harness = HarnessProfile {
            id: "v0".to_owned(),
            display_name: "v0".to_owned(),
            launch: DriverLaunch::new("/bin/false"),
            models: BTreeMap::from([("haiku".to_owned(), "v0/haiku".to_owned())]),
        };
        let controller = RunController::new_with_harnesses_and_model_access(
            RunControllerConfig {
                scenarios_dir: scenarios,
                data_dir: data,
                driver: DriverLaunch::new("/bin/false"),
            },
            vec![harness.clone()],
            BTreeMap::from([("haiku".to_owned(), "Haiku".to_owned())]),
            vec![ModelAccessProvider {
                id: "gateway".to_owned(),
                display_name: "Gateway".to_owned(),
                resolver: Some(DriverLaunch::new(resolver_path)),
                environment_names: vec!["TOKEN".to_owned()],
                setup_hint: "Connect the gateway".to_owned(),
            }],
            BTreeMap::from([("v0".to_owned(), "gateway".to_owned())]),
        )
        .unwrap();
        let launch = controller.resolve_harness_driver(&harness).unwrap();
        assert!(
            launch
                .env
                .iter()
                .any(|(name, value)| name == "TOKEN" && value == "secret-model-token")
        );
        let snapshot = controller.model_access(&WorkbenchSelection {
            harness_id: Some("v0".to_owned()),
            model_profile_id: Some("haiku".to_owned()),
            comparison_harness_ids: Vec::new(),
        });
        let serialized = serde_json::to_string(&snapshot).unwrap();
        assert_eq!(snapshot[0].status, ModelAccessStatus::Ready);
        assert!(!serialized.contains("secret-model-token"));
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn evaluation_preflight_probes_without_resolving_credentials() {
        use std::os::unix::fs::PermissionsExt;

        let root = temporary_root("model-access-preflight");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);
        let mode_log = root.join("resolver-modes.log");
        let resolver_path = root.join("resolver.sh");
        fs::write(
            &resolver_path,
            format!(
                r#"#!/bin/sh
printf '%s\n' "$1" >> '{}'
if [ "$1" = "resolve" ]; then
  printf '%s\n' '{{"status":"ready","source":"test","environment":{{"TOKEN":"secret-model-token"}}}}'
else
  printf '%s\n' '{{"status":"ready","source":"test"}}'
fi
"#,
                mode_log.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&resolver_path, fs::Permissions::from_mode(0o700)).unwrap();
        let harness = |id: &str| HarnessProfile {
            id: id.to_owned(),
            display_name: id.to_owned(),
            launch: DriverLaunch::new("/bin/false"),
            models: BTreeMap::from([("haiku".to_owned(), format!("{id}/haiku"))]),
        };
        let provider = ModelAccessProvider {
            id: "gateway".to_owned(),
            display_name: "Gateway".to_owned(),
            resolver: Some(DriverLaunch::new(resolver_path)),
            environment_names: vec!["TOKEN".to_owned()],
            setup_hint: "Connect the gateway".to_owned(),
        };
        let controller = RunController::new_with_harnesses_and_model_access(
            RunControllerConfig {
                scenarios_dir: scenarios,
                data_dir: data,
                driver: DriverLaunch::new("/bin/false"),
            },
            vec![harness("v0"), harness("eve")],
            BTreeMap::from([("haiku".to_owned(), "Haiku".to_owned())]),
            vec![provider],
            BTreeMap::from([
                ("v0".to_owned(), "gateway".to_owned()),
                ("eve".to_owned(), "gateway".to_owned()),
            ]),
        )
        .unwrap();

        let error = controller
            .start_evaluation(StartEvaluationRequest {
                scenario_id: "catalog".to_owned(),
                model_profile_id: "haiku".to_owned(),
                source_workspace_id: "missing-workspace".to_owned(),
                harness_ids: vec!["v0".to_owned(), "eve".to_owned()],
            })
            .unwrap_err();
        assert!(matches!(error, RunError::UnknownRun(_)));
        let modes = fs::read_to_string(mode_log).unwrap();
        assert_eq!(modes.lines().collect::<Vec<_>>(), ["probe", "probe"]);
        assert!(controller.list_evaluations().is_empty());
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn invalid_prepared_runs_do_not_resolve_launch_credentials() {
        use std::os::unix::fs::PermissionsExt;

        let root = temporary_root("model-access-invalid-run");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);
        let mode_log = root.join("resolver-modes.log");
        let resolver_path = root.join("resolver.sh");
        fs::write(
            &resolver_path,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$1\" >> '{}'\nprintf '%s\\n' '{{\"status\":\"ready\",\"source\":\"test\",\"environment\":{{\"TOKEN\":\"secret\"}}}}'\n",
                mode_log.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&resolver_path, fs::Permissions::from_mode(0o700)).unwrap();
        let harness = HarnessProfile {
            id: "v0".to_owned(),
            display_name: "v0".to_owned(),
            launch: DriverLaunch::new("/bin/false"),
            models: BTreeMap::from([("haiku".to_owned(), "v0/haiku".to_owned())]),
        };
        let controller = RunController::new_with_harnesses_and_model_access(
            RunControllerConfig {
                scenarios_dir: scenarios,
                data_dir: data,
                driver: DriverLaunch::new("/bin/false"),
            },
            vec![harness],
            BTreeMap::from([("haiku".to_owned(), "Haiku".to_owned())]),
            vec![ModelAccessProvider {
                id: "gateway".to_owned(),
                display_name: "Gateway".to_owned(),
                resolver: Some(DriverLaunch::new(resolver_path)),
                environment_names: vec!["TOKEN".to_owned()],
                setup_hint: "Connect the gateway".to_owned(),
            }],
            BTreeMap::from([("v0".to_owned(), "gateway".to_owned())]),
        )
        .unwrap();

        let error = controller
            .start_prepared(
                "missing-run",
                &StartPreparedRunRequest {
                    model_id: None,
                    harness_id: Some("v0".to_owned()),
                    model_profile_id: Some("haiku".to_owned()),
                },
            )
            .unwrap_err();
        assert!(matches!(error, RunError::UnknownRun(_)));
        assert!(!mode_log.exists());

        let prepared = controller
            .prepare(PrepareRunRequest {
                scenario_id: "catalog".to_owned(),
            })
            .await
            .unwrap();
        controller.cancel(&prepared.id).unwrap();
        let error = controller
            .start_prepared(
                &prepared.id,
                &StartPreparedRunRequest {
                    model_id: None,
                    harness_id: Some("v0".to_owned()),
                    model_profile_id: Some("haiku".to_owned()),
                },
            )
            .unwrap_err();
        assert!(matches!(error, RunError::RunUnavailable(_)));
        assert!(!mode_log.exists());

        drop(controller);
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn model_access_resolvers_are_bounded_by_a_timeout() {
        use std::os::unix::fs::PermissionsExt;

        let root = temporary_root("model-access-timeout");
        let resolver_path = root.join("resolver.sh");
        fs::write(&resolver_path, "#!/bin/sh\nsleep 30\n").unwrap();
        fs::set_permissions(&resolver_path, fs::Permissions::from_mode(0o700)).unwrap();
        let provider = ModelAccessProvider {
            id: "gateway".to_owned(),
            display_name: "Gateway".to_owned(),
            resolver: Some(DriverLaunch::new(resolver_path)),
            environment_names: vec!["TOKEN".to_owned()],
            setup_hint: "Connect the gateway".to_owned(),
        };

        let started = Instant::now();
        let error = resolve_model_access_with_timeout(&provider, false, Duration::from_millis(50))
            .unwrap_err();
        assert!(matches!(
            error,
            RunError::ModelAccessUnavailable(message) if message.contains("timed out")
        ));
        assert!(started.elapsed() < Duration::from_secs(2));
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn evaluation_snapshots_once_and_runs_second_arm_after_first_failure() {
        let root = temporary_root("evaluation");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);
        let harness = |id: &str| HarnessProfile {
            id: id.to_owned(),
            display_name: id.to_owned(),
            launch: DriverLaunch::new("/bin/false"),
            models: BTreeMap::from([("haiku".to_owned(), format!("{id}/haiku"))]),
        };
        let controller = RunController::new_with_harnesses(
            RunControllerConfig {
                scenarios_dir: scenarios.clone(),
                data_dir: data.clone(),
                driver: DriverLaunch::new("/bin/false"),
            },
            vec![harness("v0"), harness("eve")],
            BTreeMap::from([("haiku".to_owned(), "Haiku".to_owned())]),
        )
        .unwrap();
        let explore = controller
            .prepare(PrepareRunRequest {
                scenario_id: "catalog".to_owned(),
            })
            .await
            .unwrap();
        fs::write(
            controller
                .workspace(&explore.id)
                .unwrap()
                .join("before.txt"),
            "before",
        )
        .unwrap();
        let evaluation = controller
            .start_evaluation(StartEvaluationRequest {
                scenario_id: "catalog".to_owned(),
                model_profile_id: "haiku".to_owned(),
                source_workspace_id: explore.id.clone(),
                harness_ids: vec!["v0".to_owned(), "eve".to_owned()],
            })
            .unwrap();
        fs::write(
            controller.workspace(&explore.id).unwrap().join("after.txt"),
            "after",
        )
        .unwrap();

        let detail = loop {
            let detail = controller.get_evaluation(&evaluation.id).unwrap();
            if detail.summary.status.is_finished() {
                break detail;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert_eq!(detail.summary.status, EvaluationStatus::Failed);
        assert_eq!(detail.summary.arms.len(), 2);
        for arm in &detail.summary.arms {
            assert_eq!(arm.status, "failed");
            let workspace = controller
                .workspace(arm.run_id.as_deref().unwrap())
                .unwrap();
            assert!(workspace.join("before.txt").is_file());
            assert!(!workspace.join("after.txt").exists());
        }
        let comparison = detail.comparison.as_ref().unwrap();
        assert_eq!(comparison["outputsMatch"], false);
        assert_eq!(comparison["artifactComparison"], "missing");
        drop(controller);
        let reopened = RunController::new_with_harnesses(
            RunControllerConfig {
                scenarios_dir: scenarios,
                data_dir: data,
                driver: DriverLaunch::new("/bin/false"),
            },
            vec![harness("v0"), harness("eve")],
            BTreeMap::from([("haiku".to_owned(), "Haiku".to_owned())]),
        )
        .unwrap();
        let replay = reopened.get_evaluation(&evaluation.id).unwrap();
        assert_eq!(replay.summary.status, EvaluationStatus::Failed);
        assert_eq!(replay.comparison, detail.comparison);
        drop(reopened);
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn evaluation_runs_later_arms_after_a_launch_credential_failure() {
        use std::os::unix::fs::PermissionsExt;

        let root = temporary_root("evaluation-start-failure");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);
        let resolver_path = root.join("resolver.sh");
        fs::write(
            &resolver_path,
            r#"#!/bin/sh
if [ "$1" = "probe" ]; then
  printf '%s\n' '{"status":"ready","source":"test"}'
else
  printf '%s\n' '{"status":"needs-setup","message":"launch credential unavailable"}'
fi
"#,
        )
        .unwrap();
        fs::set_permissions(&resolver_path, fs::Permissions::from_mode(0o700)).unwrap();
        let harness = |id: &str| HarnessProfile {
            id: id.to_owned(),
            display_name: id.to_owned(),
            launch: DriverLaunch::new("/bin/false"),
            models: BTreeMap::from([("haiku".to_owned(), format!("{id}/haiku"))]),
        };
        let controller = RunController::new_with_harnesses_and_model_access(
            RunControllerConfig {
                scenarios_dir: scenarios,
                data_dir: data,
                driver: DriverLaunch::new("/bin/false"),
            },
            vec![harness("v0"), harness("eve")],
            BTreeMap::from([("haiku".to_owned(), "Haiku".to_owned())]),
            vec![ModelAccessProvider {
                id: "gateway".to_owned(),
                display_name: "Gateway".to_owned(),
                resolver: Some(DriverLaunch::new(resolver_path)),
                environment_names: vec!["TOKEN".to_owned()],
                setup_hint: "Connect the gateway".to_owned(),
            }],
            BTreeMap::from([("v0".to_owned(), "gateway".to_owned())]),
        )
        .unwrap();
        let explore = controller
            .prepare(PrepareRunRequest {
                scenario_id: "catalog".to_owned(),
            })
            .await
            .unwrap();
        let evaluation = controller
            .start_evaluation(StartEvaluationRequest {
                scenario_id: "catalog".to_owned(),
                model_profile_id: "haiku".to_owned(),
                source_workspace_id: explore.id,
                harness_ids: vec!["v0".to_owned(), "eve".to_owned()],
            })
            .unwrap();

        let detail = loop {
            let detail = controller.get_evaluation(&evaluation.id).unwrap();
            if detail.summary.status.is_finished() {
                break detail;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert_eq!(detail.summary.status, EvaluationStatus::Failed);
        assert_eq!(detail.summary.arms[0].status, "failed");
        assert_eq!(detail.summary.arms[1].status, "failed");
        for arm in &detail.summary.arms {
            let run_id = arm.run_id.as_deref().unwrap();
            assert!(controller.get(run_id).unwrap().summary.status.is_finished());
        }
        assert!(detail.events.iter().any(|event| {
            event.kind == "evaluation.arm.finished"
                && event.payload["harnessId"] == "v0"
                && event.payload["error"]
                    .as_str()
                    .is_some_and(|error| error.contains("launch credential unavailable"))
        }));

        drop(controller);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn evaluation_arms_are_not_reused_as_explore_workspaces_after_restart() {
        let root = temporary_root("evaluation-arm-explore-isolation");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);
        let config = || RunControllerConfig {
            scenarios_dir: scenarios.clone(),
            data_dir: data.clone(),
            driver: DriverLaunch::new("/bin/false"),
        };
        let controller = RunController::new(config()).unwrap();
        let arm = controller
            .prepare_snapshot_run(
                "catalog",
                &scenarios.join("catalog/workspace"),
                "revision-test",
            )
            .await
            .unwrap();
        assert_eq!(arm.status, RunStatus::Exploring);
        drop(controller);

        let reopened = RunController::new(config()).unwrap();
        let explore = reopened
            .prepare(PrepareRunRequest {
                scenario_id: "catalog".to_owned(),
            })
            .await
            .unwrap();
        assert_ne!(explore.id, arm.id);
        assert!(reopened.state(&explore.id).unwrap().reusable_explore);
        assert!(!reopened.state(&arm.id).unwrap().reusable_explore);

        drop(reopened);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn evaluation_rejects_oversized_sources_before_creating_a_bundle() {
        let root = temporary_root("oversized-evaluation-source");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);
        let harness = |id: &str| HarnessProfile {
            id: id.to_owned(),
            display_name: id.to_owned(),
            launch: DriverLaunch::new("/bin/false"),
            models: BTreeMap::from([("haiku".to_owned(), format!("{id}/haiku"))]),
        };
        let controller = RunController::new_with_harnesses(
            RunControllerConfig {
                scenarios_dir: scenarios,
                data_dir: data.clone(),
                driver: DriverLaunch::new("/bin/false"),
            },
            vec![harness("v0"), harness("eve")],
            BTreeMap::from([("haiku".to_owned(), "Haiku".to_owned())]),
        )
        .unwrap();
        let explore = controller
            .prepare(PrepareRunRequest {
                scenario_id: "catalog".to_owned(),
            })
            .await
            .unwrap();
        fs::File::create(
            controller
                .workspace(&explore.id)
                .unwrap()
                .join("oversized.bin"),
        )
        .unwrap()
        .set_len(MAX_EVIDENCE_FILE_BYTES + 1)
        .unwrap();

        assert!(matches!(
            controller.start_evaluation(StartEvaluationRequest {
                scenario_id: "catalog".to_owned(),
                model_profile_id: "haiku".to_owned(),
                source_workspace_id: explore.id,
                harness_ids: vec!["v0".to_owned(), "eve".to_owned()],
            }),
            Err(RunError::EvidenceLimit(_))
        ));
        assert!(controller.list_evaluations().is_empty());
        assert_eq!(fs::read_dir(data.join("evaluations")).unwrap().count(), 0);

        drop(controller);
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn evaluation_snapshots_preserve_executable_files_and_empty_directories() {
        use std::os::unix::fs::PermissionsExt;

        let root = temporary_root("evaluation-snapshot-metadata");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);
        let harness = |id: &str| HarnessProfile {
            id: id.to_owned(),
            display_name: id.to_owned(),
            launch: DriverLaunch::new("/bin/false"),
            models: BTreeMap::from([("haiku".to_owned(), format!("{id}/haiku"))]),
        };
        let controller = RunController::new_with_harnesses(
            RunControllerConfig {
                scenarios_dir: scenarios,
                data_dir: data.clone(),
                driver: DriverLaunch::new("/bin/false"),
            },
            vec![harness("v0"), harness("eve")],
            BTreeMap::from([("haiku".to_owned(), "Haiku".to_owned())]),
        )
        .unwrap();
        let explore = controller
            .prepare(PrepareRunRequest {
                scenario_id: "catalog".to_owned(),
            })
            .await
            .unwrap();
        let workspace = controller.workspace(&explore.id).unwrap();
        let empty = workspace.join("empty");
        fs::create_dir(&empty).unwrap();
        fs::set_permissions(&empty, fs::Permissions::from_mode(0o750)).unwrap();
        let helper = workspace.join("helper.sh");
        fs::write(&helper, "#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&helper, fs::Permissions::from_mode(0o751)).unwrap();
        let evaluation = controller
            .start_evaluation(StartEvaluationRequest {
                scenario_id: "catalog".to_owned(),
                model_profile_id: "haiku".to_owned(),
                source_workspace_id: explore.id,
                harness_ids: vec!["v0".to_owned(), "eve".to_owned()],
            })
            .unwrap();
        let captured = data.join("evaluations").join(&evaluation.id).join("source");
        assert!(captured.join("empty").is_dir());
        assert_eq!(
            fs::metadata(captured.join("empty"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o750
        );
        assert_eq!(
            fs::metadata(captured.join("helper.sh"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o751
        );
        let detail = loop {
            let detail = controller.get_evaluation(&evaluation.id).unwrap();
            if detail.summary.status.is_finished() {
                break detail;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        for arm in detail.summary.arms {
            let arm_workspace = controller
                .workspace(arm.run_id.as_deref().unwrap())
                .unwrap();
            assert!(arm_workspace.join("empty").is_dir());
            assert_eq!(
                fs::metadata(arm_workspace.join("empty"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o750
            );
            assert_eq!(
                fs::metadata(arm_workspace.join("helper.sh"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o751
            );
        }

        drop(controller);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn evaluation_storage_is_isolated_by_the_selected_data_root() {
        let root = temporary_root("evaluation-data-root-isolation");
        let scenarios = root.join("scenarios");
        let first_data = root.join("first-runs");
        let second_data = root.join("second-runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&first_data).unwrap();
        fs::create_dir(&second_data).unwrap();
        write_scenario(&scenarios);
        let harness = |id: &str| HarnessProfile {
            id: id.to_owned(),
            display_name: id.to_owned(),
            launch: DriverLaunch::new("/bin/false"),
            models: BTreeMap::from([("haiku".to_owned(), format!("{id}/haiku"))]),
        };
        let controller = |data_dir: PathBuf| {
            RunController::new_with_harnesses(
                RunControllerConfig {
                    scenarios_dir: scenarios.clone(),
                    data_dir,
                    driver: DriverLaunch::new("/bin/false"),
                },
                vec![harness("v0"), harness("eve")],
                BTreeMap::from([("haiku".to_owned(), "Haiku".to_owned())]),
            )
            .unwrap()
        };
        let first = controller(first_data.clone());
        let explore = first
            .prepare(PrepareRunRequest {
                scenario_id: "catalog".to_owned(),
            })
            .await
            .unwrap();
        let evaluation = first
            .start_evaluation(StartEvaluationRequest {
                scenario_id: "catalog".to_owned(),
                model_profile_id: "haiku".to_owned(),
                source_workspace_id: explore.id,
                harness_ids: vec!["v0".to_owned(), "eve".to_owned()],
            })
            .unwrap();
        let second = controller(second_data.clone());

        assert_eq!(first.list_evaluations().len(), 1);
        assert!(second.list_evaluations().is_empty());
        assert!(first_data.join("evaluations").is_dir());
        assert!(second_data.join("evaluations").is_dir());
        assert!(!root.join("evaluations").exists());
        loop {
            if first
                .get_evaluation(&evaluation.id)
                .unwrap()
                .summary
                .status
                .is_finished()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        drop(first);
        drop(second);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn cancelling_a_queued_evaluation_cancels_both_arms() {
        let root = temporary_root("evaluation-cancel");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);
        let harness = |id: &str| HarnessProfile {
            id: id.to_owned(),
            display_name: id.to_owned(),
            launch: DriverLaunch::new("/bin/false"),
            models: BTreeMap::from([("haiku".to_owned(), format!("{id}/haiku"))]),
        };
        let controller = RunController::new_with_harnesses(
            RunControllerConfig {
                scenarios_dir: scenarios,
                data_dir: data,
                driver: DriverLaunch::new("/bin/false"),
            },
            vec![harness("v0"), harness("eve")],
            BTreeMap::from([("haiku".to_owned(), "Haiku".to_owned())]),
        )
        .unwrap();
        let explore = controller
            .prepare(PrepareRunRequest {
                scenario_id: "catalog".to_owned(),
            })
            .await
            .unwrap();
        let evaluation = controller
            .start_evaluation(StartEvaluationRequest {
                scenario_id: "catalog".to_owned(),
                model_profile_id: "haiku".to_owned(),
                source_workspace_id: explore.id,
                harness_ids: vec!["v0".to_owned(), "eve".to_owned()],
            })
            .unwrap();
        controller.cancel_evaluation(&evaluation.id).unwrap();

        let detail = loop {
            let detail = controller.get_evaluation(&evaluation.id).unwrap();
            if detail.summary.status.is_finished() {
                break detail;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert_eq!(detail.summary.status, EvaluationStatus::Cancelled);
        assert!(
            detail
                .summary
                .arms
                .iter()
                .all(|arm| arm.status == "cancelled")
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn malformed_event_evidence_fails_only_that_replayed_evaluation() {
        let root = temporary_root("malformed-evaluation-replay");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        let evaluations = data.join("evaluations");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        fs::create_dir(&evaluations).unwrap();
        write_scenario(&scenarios);

        let id = "evaluation-run-malformed";
        let bundle = evaluations.join(id);
        fs::create_dir_all(bundle.join("source")).unwrap();
        let summary = EvaluationSummary {
            id: id.to_owned(),
            scenario_id: "catalog".to_owned(),
            model_profile_id: "haiku".to_owned(),
            source_workspace_id: "run-explore".to_owned(),
            source_revision: "revision-1".to_owned(),
            harness_ids: vec!["v0".to_owned(), "eve".to_owned()],
            arms: vec![
                EvaluationArmSummary {
                    harness_id: "v0".to_owned(),
                    run_id: Some("run-v0".to_owned()),
                    status: "passed".to_owned(),
                },
                EvaluationArmSummary {
                    harness_id: "eve".to_owned(),
                    run_id: Some("run-eve".to_owned()),
                    status: "passed".to_owned(),
                },
            ],
            status: EvaluationStatus::Passed,
            started_at_ms: 1,
            finished_at_ms: Some(2),
        };
        let manifest = serde_json::to_vec(&summary).unwrap();
        let malformed_events = br#"{"sequence":1}{"sequence":2}\n"#;
        let comparison = br#"{"version":2,"outputsMatch":true}"#;
        fs::write(bundle.join("manifest.json"), &manifest).unwrap();
        fs::write(bundle.join("events.jsonl"), malformed_events).unwrap();
        fs::write(bundle.join("comparison.json"), comparison).unwrap();

        let controller = RunController::new(RunControllerConfig {
            scenarios_dir: scenarios,
            data_dir: data,
            driver: DriverLaunch::new("/driver-is-not-needed-for-replay"),
        })
        .unwrap();
        let detail = controller.get_evaluation(id).unwrap();
        assert_eq!(detail.summary.status, EvaluationStatus::Failed);
        assert_eq!(detail.events.len(), 1);
        assert_eq!(detail.events[0].kind, "evaluation.finished");
        assert_eq!(detail.events[0].payload["recovered"], true);
        assert!(detail.comparison.is_none());
        assert_eq!(fs::read(bundle.join("manifest.json")).unwrap(), manifest);
        assert_eq!(
            fs::read(bundle.join("events.jsonl")).unwrap(),
            malformed_events
        );
        assert_eq!(
            fs::read(bundle.join("comparison.json")).unwrap(),
            comparison
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn malformed_evaluation_metadata_does_not_block_valid_replay() {
        let root = temporary_root("malformed-evaluation-metadata");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        let evaluations = data.join("evaluations");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        fs::create_dir(&evaluations).unwrap();
        write_scenario(&scenarios);

        let valid_id = "evaluation-run-valid";
        let valid_bundle = evaluations.join(valid_id);
        fs::create_dir_all(valid_bundle.join("source")).unwrap();
        let summary = EvaluationSummary {
            id: valid_id.to_owned(),
            scenario_id: "catalog".to_owned(),
            model_profile_id: "haiku".to_owned(),
            source_workspace_id: "run-explore".to_owned(),
            source_revision: "revision-1".to_owned(),
            harness_ids: vec!["v0".to_owned(), "eve".to_owned()],
            arms: Vec::new(),
            status: EvaluationStatus::Passed,
            started_at_ms: 1,
            finished_at_ms: Some(2),
        };
        write_json_atomic(
            &valid_bundle.join("manifest.json"),
            &serde_json::to_value(&summary).unwrap(),
        )
        .unwrap();
        fs::write(valid_bundle.join("events.jsonl"), []).unwrap();

        let malformed_bundle = evaluations.join("evaluation-run-malformed");
        fs::create_dir_all(malformed_bundle.join("source")).unwrap();
        fs::write(malformed_bundle.join("manifest.json"), b"{not-json").unwrap();

        let controller = RunController::new(RunControllerConfig {
            scenarios_dir: scenarios,
            data_dir: data,
            driver: DriverLaunch::new("/driver-is-not-needed-for-replay"),
        })
        .unwrap();
        let replayed = controller.get_evaluation(valid_id).unwrap().summary;
        assert_eq!(replayed.id, summary.id);
        assert_eq!(replayed.status, EvaluationStatus::Passed);
        assert!(matches!(
            controller.get_evaluation("evaluation-run-malformed"),
            Err(RunError::InvalidRequest(message)) if message.contains("unknown evaluation")
        ));

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
            harness_id: None,
            model_profile_id: None,
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
                progress: None,
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
                progress: None,
            },
            RunEvent {
                sequence: 3,
                at_ms: 2,
                kind: "run.finished".to_owned(),
                payload: json!({ "status": "passed" }),
                progress: None,
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
            harness_id: None,
            model_profile_id: None,
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
            harness_id: None,
            model_profile_id: None,
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

    #[tokio::test]
    async fn malformed_bundle_metadata_does_not_block_valid_replay() {
        let root = temporary_root("malformed-metadata");
        let scenarios = root.join("scenarios");
        let data = root.join("runs");
        fs::create_dir(&scenarios).unwrap();
        fs::create_dir(&data).unwrap();
        write_scenario(&scenarios);

        let config = || RunControllerConfig {
            scenarios_dir: scenarios.clone(),
            data_dir: data.clone(),
            driver: DriverLaunch::new("/driver-is-not-needed-for-replay"),
        };
        let controller = RunController::new(config()).unwrap();
        let valid = controller
            .prepare(PrepareRunRequest {
                scenario_id: "catalog".to_owned(),
            })
            .await
            .unwrap();
        drop(controller);

        let malformed_manifest = data.join("run-malformed-manifest");
        fs::create_dir(&malformed_manifest).unwrap();
        fs::write(malformed_manifest.join("manifest.json"), b"{malformed").unwrap();

        let malformed_assembly = data.join("run-malformed-assembly");
        fs::create_dir(&malformed_assembly).unwrap();
        let summary = RunSummary {
            id: "run-malformed-assembly".to_owned(),
            scenario_id: "catalog".to_owned(),
            scenario_title: "Catalog".to_owned(),
            model_id: "test/model".to_owned(),
            harness_id: None,
            model_profile_id: None,
            status: RunStatus::Passed,
            started_at_ms: 1,
            finished_at_ms: Some(2),
            event_count: 0,
            error: None,
        };
        write_json_atomic(
            &malformed_assembly.join("manifest.json"),
            &serde_json::to_value(summary).unwrap(),
        )
        .unwrap();
        fs::write(malformed_assembly.join("events.jsonl"), []).unwrap();
        fs::write(malformed_assembly.join("assembly.json"), b"{malformed").unwrap();

        let replayed = RunController::new(config()).unwrap();
        assert!(replayed.state(&valid.id).is_ok());
        assert!(replayed.state("run-malformed-manifest").is_err());
        assert!(replayed.state("run-malformed-assembly").is_err());
        assert_eq!(lock(&replayed.inner.runs).len(), 1);

        drop(replayed);
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
            harness_id: None,
            model_profile_id: None,
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
    #[allow(clippy::too_many_lines)]
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
                harness_id: None,
                model_profile_id: None,
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
            selection: Mutex::new(WorkbenchSelection {
                harness_id: None,
                model_profile_id: None,
                comparison_harness_ids: Vec::new(),
            }),
            events: Mutex::new(Vec::new()),
            sender,
            cancel: CancellationToken::new(),
            bundle_dir: root.clone(),
            agent_session_directories: AgentSessionDirectoryAnchor::open(root.clone()).unwrap(),
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
            agent_sessions: Mutex::new(HashMap::new()),
            active_agent_session_id: Mutex::new(None),
            active_agent_turn: Mutex::new(None),
            capability_attributions: Mutex::new(HashMap::new()),
            reusable_explore: false,
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
    fn startup_events_are_redacted_before_recording() {
        let root = temporary_root("startup-event-redaction");
        fs::create_dir(root.join("initial")).unwrap();
        let state = test_run_state(&root);
        lock(&state.secret_values).push(b"provider-secret".to_vec());

        record_startup_event(
            &state,
            "phase-provider-secret",
            "status-provider-secret",
            Some("detail provider-secret"),
        )
        .unwrap();

        let serialized = serde_json::to_string(&lock(&state.events).clone()).unwrap();
        assert!(!serialized.contains("provider-secret"));
        assert!(serialized.contains("[REDACTED]"));
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

    #[cfg(target_os = "linux")]
    #[test]
    fn non_utf8_workspace_paths_are_rejected_before_snapshotting() {
        use std::os::unix::ffi::OsStringExt;

        let root = temporary_root("non-utf8-evidence");
        fs::create_dir(root.join("initial")).unwrap();
        let state = test_run_state(&root);
        let invalid_name = std::ffi::OsString::from_vec(vec![b'f', b'i', b'l', b'e', 0xff]);
        fs::write(state.workspace.join(invalid_name), b"content").unwrap();

        assert!(matches!(
            finalize_workspace(&state),
            Err(RunError::UnsupportedWorkspaceEntry(_))
        ));
        assert!(!root.join("final").exists());
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
            harness_id: None,
            model_profile_id: None,
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
        assert_eq!(review.steps[0].title, "Driver protocol ready");
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
            harness_id: None,
            model_profile_id: None,
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
            harness_id: None,
            model_profile_id: None,
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
    fn causal_review_projects_eve_steps_native_actions_and_reported_usage() {
        let summary = RunSummary {
            id: "run-eve-review".to_owned(),
            scenario_id: "catalog".to_owned(),
            scenario_title: "Catalog".to_owned(),
            model_id: "claude-haiku-4-5-20251001".to_owned(),
            harness_id: Some("eve".to_owned()),
            model_profile_id: Some("haiku-4-5".to_owned()),
            status: RunStatus::Passed,
            started_at_ms: 10,
            finished_at_ms: Some(30),
            event_count: 8,
            error: None,
        };
        let events = vec![
            event(1, "driver.session-opened", JsonValue::Null),
            event(
                2,
                "model.step.started",
                json!({ "data": { "stepIndex": 0 } }),
            ),
            event(
                3,
                "model.message.delta",
                json!({ "data": { "messageDelta": "I will write the result." } }),
            ),
            event(
                4,
                "model.step.completed",
                json!({
                    "data": {
                        "usage": {
                            "inputTokens": 100,
                            "outputTokens": 20,
                            "cacheReadTokens": 80,
                            "cacheWriteTokens": 10
                        }
                    }
                }),
            ),
            event(
                5,
                "harness.action.result",
                json!({
                    "data": {
                        "status": "completed",
                        "result": {
                            "callId": "write-1",
                            "toolName": "write_file",
                            "output": { "path": "/workspace/result.json" }
                        }
                    }
                }),
            ),
            event(
                6,
                "mcp.tool.completed",
                json!({ "source": "catalog", "name": "list", "actor": "agent", "isError": false }),
            ),
            event(
                7,
                "workspace.finalized",
                json!({ "changes": [{ "path": "result.json", "kind": "created" }] }),
            ),
            event(
                8,
                "run.finished",
                json!({ "status": "passed", "score": {} }),
            ),
        ];

        let review = build_review(&summary, &events);
        assert_eq!(review.metrics.model_turns, 1);
        assert_eq!(review.metrics.capability_calls, 1);
        assert_eq!(review.metrics.native_actions, 1);
        assert_eq!(review.steps[0].title, "Model step 1");
        assert_eq!(
            review.steps[0].detail.as_deref(),
            Some("I will write the result.")
        );
        assert_eq!(review.steps[1].title, "Wrote /workspace/result.json");
        let (usage, cache) = reported_usage(&events);
        assert_eq!(usage, json!({ "inputTokens": 100, "outputTokens": 20 }));
        assert_eq!(cache, json!({ "readTokens": 80, "writeTokens": 10 }));
    }

    #[test]
    fn causal_review_explains_model_provider_failures() {
        let summary = RunSummary {
            id: "run-provider-failure".to_owned(),
            scenario_id: "catalog".to_owned(),
            scenario_title: "Catalog".to_owned(),
            model_id: "test/model".to_owned(),
            harness_id: None,
            model_profile_id: None,
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
            harness_id: None,
            model_profile_id: None,
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

    #[test]
    fn transcript_redaction_prevents_split_assistant_secrets_from_reconstruction() {
        let driver_records = [
            json!({
                "protocolVersion": 1,
                "sequence": 1,
                "causedBy": null,
                "type": "turn.event",
                "sessionId": "session-1",
                "turnId": "turn-1",
                "eventType": "observation.assistant.delta",
                "payload": {
                    "messageId": "message-1",
                    "text": "credential-",
                },
            }),
            json!({
                "protocolVersion": 1,
                "sequence": 2,
                "causedBy": null,
                "type": "turn.event",
                "sessionId": "session-1",
                "turnId": "turn-1",
                "eventType": "observation.assistant.delta",
                "payload": {
                    "messageId": "message-1",
                    "text": "token tail",
                },
            }),
            json!({
                "protocolVersion": 1,
                "sequence": 3,
                "causedBy": null,
                "type": "turn.event",
                "sessionId": "session-1",
                "turnId": "turn-1",
                "eventType": "observation.assistant.completed",
                "payload": {
                    "messageId": "message-1",
                    "text": "credential-token tail",
                },
            }),
            json!({
                "protocolVersion": 1,
                "sequence": 4,
                "causedBy": null,
                "type": "turn.finished",
                "sessionId": "session-1",
                "turnId": "turn-1",
                "outcome": "completed",
                "evidence": {},
            }),
        ]
        .into_iter()
        .map(|value| serde_json::to_vec(&value).unwrap())
        .collect();
        let transcript = DriverTranscript {
            controller_records: Vec::new(),
            driver_records,
            driver_stderr: Vec::new(),
        };

        let redacted = redact_transcript(transcript, &[b"credential-token".to_vec()]);
        let records = redacted
            .driver_records
            .iter()
            .map(|record| serde_json::from_slice::<JsonValue>(record).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            records
                .iter()
                .map(|record| record["sequence"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            [1, 2, 3, 4]
        );
        assert!(
            records
                .iter()
                .filter(|record| record["type"] == "turn.event")
                .all(|record| {
                    record["sessionId"] == "session-1"
                        && record["turnId"] == "turn-1"
                        && record["payload"]["messageId"] == "message-1"
                })
        );
        let assistant_text = records
            .iter()
            .filter_map(|record| record["payload"]["text"].as_str())
            .collect::<String>();
        assert!(!assistant_text.contains("credential-token"));
        assert!(assistant_text.contains("[REDACTED]"));
        let serialized = redacted.driver_records.concat();
        assert!(
            !serialized
                .windows(b"credential-token".len())
                .any(|window| window == b"credential-token")
        );
    }
}
