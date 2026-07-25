//! Local browser gateway for Agent Lab terminal sessions.

mod runs;

use std::{
    collections::VecDeque,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use axum::{
    Json, Router,
    extract::{
        Path as AxumPath, Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode, Uri, header},
    response::{IntoResponse, Response, Sse, sse::Event},
    routing::{get, patch, post},
};
use futures_util::{SinkExt, StreamExt};
use portable_pty::{Child, CommandBuilder, MasterPty, NativePtySystem, PtySize, PtySystem};
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tower_http::{
    services::{ServeDir, ServeFile},
    set_header::SetResponseHeaderLayer,
};

pub use runs::{
    AgentAssistantMessage, AgentPresentationCompleteness, AgentPresentationCompletenessSummary,
    AgentSessionDetail, AgentSessionStatus, AgentSessionSummary, AgentTurnActivity,
    AgentTurnCompletionIndex, AgentTurnCompletionRef, AgentTurnDetail, AgentTurnPresentation,
    AgentTurnStatus, AgentTurnSummary, CompareWorkbenchRequest, EvaluationDetail, EvaluationStatus,
    EvaluationSummary, HarnessMetadata, HarnessProfile, ModelAccessProvider, ModelAccessSnapshot,
    ModelAccessStatus, ModelProfileMetadata, PrepareRunRequest, RunController, RunControllerConfig,
    RunDetail, RunError, RunEvent, RunStatus, RunSummary, ScenarioManifest,
    StartAgentSessionRequest, StartAgentTurnRequest, StartEvaluationRequest,
    StartPreparedRunRequest, StartRunRequest, TerminalBinding, TerminalCapabilityBinding,
    UpdateWorkbenchSelectionRequest, WorkbenchOrigin, WorkbenchSelection, WorkbenchSnapshot,
};

const DEFAULT_COLS: u16 = 100;
const DEFAULT_ROWS: u16 = 30;
const AUTH_PROTOCOL_PREFIX: &str = "agent-lab.auth.";
const EVENT_STREAM_EPOCH_HEADER: HeaderName =
    HeaderName::from_static("x-agent-lab-event-stream-epoch");

/// A source capable of opening a terminal session for the web surface.
pub trait SessionProvider: Send + Sync + 'static {
    /// Human-readable provider name sent to the browser as session evidence.
    fn name(&self) -> &'static str;

    /// Open one new terminal session at the requested dimensions.
    ///
    /// # Errors
    ///
    /// Returns an error when the provider cannot create its bounded session.
    fn open(
        &self,
        size: TerminalSize,
        run_id: Option<&str>,
    ) -> Result<Box<dyn BrowserSession>, GatewayError>;
}

/// A bidirectional terminal session independent of its process or transport.
pub trait BrowserSession: Send + Sync + 'static {
    /// Transfer ownership of the session's output reader to the gateway.
    ///
    /// # Errors
    ///
    /// Returns an error if the reader was already taken or is unavailable.
    fn take_reader(&self) -> Result<Box<dyn Read + Send>, GatewayError>;

    /// Write browser input or a terminal response into the session.
    ///
    /// # Errors
    ///
    /// Returns an error when the session can no longer accept input.
    fn write(&self, bytes: &[u8]) -> Result<(), GatewayError>;

    /// Record browser-observed human input separately from terminal protocol
    /// replies carried over the same PTY byte stream.
    ///
    /// # Errors
    ///
    /// Returns an error when the session cannot persist the attribution.
    fn note_human_input(&self) -> Result<(), GatewayError> {
        Ok(())
    }

    /// Resize the session's terminal viewport.
    ///
    /// # Errors
    ///
    /// Returns an error when the session cannot apply the requested size.
    fn resize(&self, size: TerminalSize) -> Result<(), GatewayError>;

    /// Terminate the bounded session and wait for its child resources.
    fn terminate(&self);
}

/// The bounded fixture provider used by the first browser steel thread.
#[derive(Debug, Clone)]
pub struct FixtureSessionProvider {
    shell: PathBuf,
    cwd: PathBuf,
}

impl FixtureSessionProvider {
    /// Create a provider that launches the existing visual shell binary.
    #[must_use]
    pub fn new(shell: impl Into<PathBuf>, cwd: impl Into<PathBuf>) -> Self {
        Self {
            shell: shell.into(),
            cwd: cwd.into(),
        }
    }
}

impl SessionProvider for FixtureSessionProvider {
    fn name(&self) -> &'static str {
        "fixture"
    }

    fn open(
        &self,
        size: TerminalSize,
        _run_id: Option<&str>,
    ) -> Result<Box<dyn BrowserSession>, GatewayError> {
        Ok(Box::new(PtyTerminalSession::spawn(
            &self.shell,
            &self.cwd,
            size,
            &["--fixture".to_owned()],
            &[],
        )?))
    }
}

/// A Nushell provider that can attach to the workspace and MCP source owned by a run.
#[derive(Clone)]
pub struct RunSessionProvider {
    shell: PathBuf,
    runs: RunController,
    origin: String,
}

impl RunSessionProvider {
    #[must_use]
    pub fn new(shell: impl Into<PathBuf>, runs: RunController, origin: String) -> Self {
        Self {
            shell: shell.into(),
            runs,
            origin,
        }
    }
}

impl SessionProvider for RunSessionProvider {
    fn name(&self) -> &'static str {
        "nushell"
    }

    fn open(
        &self,
        size: TerminalSize,
        run_id: Option<&str>,
    ) -> Result<Box<dyn BrowserSession>, GatewayError> {
        let run_id = run_id.ok_or(GatewayError::RunRequired)?;
        let binding = self.runs.terminal_binding(run_id)?;
        let mut args = Vec::new();
        let mut environment = Vec::new();
        for (index, source) in binding.sources.into_iter().enumerate() {
            let token_env = format!("AGENT_LAB_MCP_TOKEN_{index}");
            args.extend([
                "--mcp-http".to_owned(),
                source.id,
                source.url,
                token_env.clone(),
            ]);
            environment.push((token_env, source.token));
        }
        let control_env = "AGENT_LAB_WORKBENCH_TOKEN".to_owned();
        args.extend([
            "--workbench".to_owned(),
            self.origin.clone(),
            run_id.to_owned(),
            control_env.clone(),
        ]);
        environment.push((control_env, binding.control_token.clone()));
        let session = match PtyTerminalSession::spawn(
            &self.shell,
            &binding.workspace,
            size,
            &args,
            &environment,
        ) {
            Ok(session) => session,
            Err(error) => {
                self.runs.revoke_workbench_grant(&binding.control_token);
                return Err(error);
            }
        };
        Ok(Box::new(GrantedTerminalSession {
            inner: session,
            runs: self.runs.clone(),
            workspace_id: run_id.to_owned(),
            token: binding.control_token,
        }))
    }
}

struct GrantedTerminalSession {
    inner: PtyTerminalSession,
    runs: RunController,
    workspace_id: String,
    token: String,
}

impl BrowserSession for GrantedTerminalSession {
    fn take_reader(&self) -> Result<Box<dyn Read + Send>, GatewayError> {
        self.inner.take_reader()
    }

    fn write(&self, bytes: &[u8]) -> Result<(), GatewayError> {
        self.inner.write(bytes)
    }

    fn note_human_input(&self) -> Result<(), GatewayError> {
        self.runs.note_terminal_input(&self.workspace_id)?;
        Ok(())
    }

    fn resize(&self, size: TerminalSize) -> Result<(), GatewayError> {
        self.inner.resize(size)
    }

    fn terminate(&self) {
        self.inner.terminate();
        self.runs.revoke_workbench_grant(&self.token);
    }
}

impl Drop for GrantedTerminalSession {
    fn drop(&mut self) {
        self.runs.revoke_workbench_grant(&self.token);
    }
}

/// Dimensions shared by the browser terminal and the child PTY.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
pub struct TerminalSize {
    /// Terminal columns.
    pub cols: u16,
    /// Terminal rows.
    pub rows: u16,
}

impl TerminalSize {
    fn validated(self) -> Result<Self, GatewayError> {
        if self.cols == 0 || self.rows == 0 || self.cols > 500 || self.rows > 500 {
            return Err(GatewayError::InvalidSize);
        }
        Ok(self)
    }
}

impl Default for TerminalSize {
    fn default() -> Self {
        Self {
            cols: DEFAULT_COLS,
            rows: DEFAULT_ROWS,
        }
    }
}

struct PtyTerminalSession {
    master: Mutex<Box<dyn MasterPty + Send>>,
    child: Mutex<Box<dyn Child + Send + Sync>>,
    reader: Mutex<Option<Box<dyn Read + Send>>>,
    writer: Mutex<Box<dyn Write + Send>>,
}

impl PtyTerminalSession {
    fn spawn(
        shell: &Path,
        cwd: &Path,
        size: TerminalSize,
        args: &[String],
        environment: &[(String, String)],
    ) -> Result<Self, GatewayError> {
        if !shell.is_file() {
            return Err(GatewayError::ShellNotFound(shell.to_path_buf()));
        }

        let pair = NativePtySystem::default().openpty(size.into())?;
        let mut command = CommandBuilder::new(shell);
        for argument in args {
            command.arg(argument);
        }
        for (name, value) in environment {
            command.env(name, value);
        }
        command.cwd(cwd);
        command.env("TERM", "xterm-256color");
        let child = pair.slave.spawn_command(command)?;
        drop(pair.slave);

        let reader = pair.master.try_clone_reader()?;
        let writer = pair.master.take_writer()?;

        Ok(Self {
            master: Mutex::new(pair.master),
            child: Mutex::new(child),
            reader: Mutex::new(Some(reader)),
            writer: Mutex::new(writer),
        })
    }
}

impl BrowserSession for PtyTerminalSession {
    fn take_reader(&self) -> Result<Box<dyn Read + Send>, GatewayError> {
        self.reader
            .lock()
            .map_err(|_| GatewayError::SessionUnavailable)?
            .take()
            .ok_or(GatewayError::SessionUnavailable)
    }

    fn write(&self, bytes: &[u8]) -> Result<(), GatewayError> {
        let mut writer = self
            .writer
            .lock()
            .map_err(|_| GatewayError::SessionUnavailable)?;
        writer.write_all(bytes)?;
        writer.flush()?;
        Ok(())
    }

    fn resize(&self, size: TerminalSize) -> Result<(), GatewayError> {
        self.master
            .lock()
            .map_err(|_| GatewayError::SessionUnavailable)?
            .resize(size.into())
            .map_err(GatewayError::from)
    }

    fn terminate(&self) {
        if let Ok(mut child) = self.child.lock() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for PtyTerminalSession {
    fn drop(&mut self) {
        self.terminate();
    }
}

impl From<TerminalSize> for PtySize {
    fn from(size: TerminalSize) -> Self {
        Self {
            rows: size.rows,
            cols: size.cols,
            pixel_width: 0,
            pixel_height: 0,
        }
    }
}

/// Configuration for the loopback browser server.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Directory containing the `SvelteKit` static build.
    pub assets: PathBuf,
    /// Exact browser origin allowed to open sessions.
    pub origin: String,
    /// Per-process bearer token required by the WebSocket upgrade.
    pub token: String,
    /// Model identifiers exposed by the configured agent harness.
    pub models: Vec<String>,
    shutdown: CancellationToken,
}

impl ServerConfig {
    /// Create configuration for a server that is already bound to loopback.
    #[must_use]
    pub fn new(assets: impl Into<PathBuf>, origin: String) -> Self {
        Self {
            assets: assets.into(),
            origin,
            token: generate_token(),
            models: Vec::new(),
            shutdown: CancellationToken::new(),
        }
    }

    /// Publish the models supported by this server's configured harness.
    #[must_use]
    pub fn with_models(mut self, models: Vec<String>) -> Self {
        self.models = models;
        self
    }

    /// Signal all long-lived browser sessions to stop during server shutdown.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

#[derive(Clone)]
struct AppState {
    config: ServerConfig,
    provider: Arc<dyn SessionProvider>,
    runs: Option<RunController>,
    event_stream_epoch: HeaderValue,
}

/// Build the HTTP application around one bounded session provider.
pub fn app(config: ServerConfig, provider: Arc<dyn SessionProvider>) -> Router {
    app_with_runs(config, provider, None)
}

/// Build the browser application with the optional scenario run controller.
pub fn app_with_runs(
    config: ServerConfig,
    provider: Arc<dyn SessionProvider>,
    runs: Option<RunController>,
) -> Router {
    let index = config.assets.join("index.html");
    let assets = ServeDir::new(&config.assets).not_found_service(ServeFile::new(index));
    let state = AppState {
        config,
        provider,
        runs,
        event_stream_epoch: HeaderValue::from_str(&generate_token())
            .unwrap_or_else(|_| HeaderValue::from_static("event-stream-epoch")),
    };

    Router::new()
        .route("/api/session-token", get(session_token))
        .route("/api/terminal", get(upgrade_terminal))
        .route("/api/models", get(list_models))
        .route("/api/harnesses", get(list_harnesses))
        .route("/api/model-profiles", get(list_model_profiles))
        .route("/api/scenarios", get(list_scenarios))
        .route("/api/explore", post(prepare_run))
        .route("/api/runs", get(list_runs).post(start_run))
        .route("/api/runs/{id}", get(get_run))
        .route("/api/runs/{id}/start", post(start_prepared_run))
        .route("/api/runs/{id}/cancel", post(cancel_run))
        .route("/api/runs/{id}/events", get(run_events))
        .route("/api/workbench/{id}", get(get_workbench))
        .route(
            "/api/workbench/{id}/selection",
            patch(update_workbench_selection),
        )
        .route("/api/workbench/{id}/compare", post(compare_workbench))
        .route(
            "/api/workbench/{workspace_id}/agent-sessions",
            get(list_agent_sessions).post(start_agent_session),
        )
        .route(
            "/api/workbench/{workspace_id}/agent-sessions/{session_id}",
            get(get_agent_session),
        )
        .route(
            "/api/workbench/{workspace_id}/agent-sessions/{session_id}/activate",
            post(activate_agent_session),
        )
        .route(
            "/api/workbench/{workspace_id}/agent-sessions/{session_id}/turns",
            post(start_agent_turn),
        )
        .route(
            "/api/workbench/{workspace_id}/agent-sessions/{session_id}/cancel",
            post(cancel_agent_turn),
        )
        .route(
            "/api/workbench/{workspace_id}/agent-sessions/{session_id}/close",
            post(close_agent_session),
        )
        .route(
            "/api/workbench/{workspace_id}/agent-sessions/{session_id}/events",
            get(agent_session_events),
        )
        .route(
            "/api/workbench/{workspace_id}/evaluations/{evaluation_id}",
            get(get_workbench_evaluation),
        )
        .route(
            "/api/workbench/{workspace_id}/evaluations/{evaluation_id}/events",
            get(workbench_evaluation_events),
        )
        .route(
            "/api/workbench/{workspace_id}/evaluations/{evaluation_id}/cancel",
            post(cancel_workbench_evaluation),
        )
        .route(
            "/api/evaluations",
            get(list_evaluations).post(start_evaluation),
        )
        .route("/api/evaluations/{id}", get(get_evaluation))
        .route("/api/evaluations/{id}/cancel", post(cancel_evaluation))
        .route("/api/evaluations/{id}/events", get(evaluation_events))
        .fallback_service(assets)
        .layer(SetResponseHeaderLayer::if_not_present(
            header::X_FRAME_OPTIONS,
            HeaderValue::from_static("DENY"),
        ))
        .layer(SetResponseHeaderLayer::if_not_present(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        ))
        .with_state(state)
}

async fn list_models(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if !run_request_is_authorized(&state, &headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    Json(state.config.models.clone()).into_response()
}

async fn list_harnesses(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if !run_request_is_authorized(&state, &headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    Json(
        state
            .runs
            .as_ref()
            .map_or_else(Vec::new, RunController::harnesses),
    )
    .into_response()
}

async fn list_model_profiles(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if !run_request_is_authorized(&state, &headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    Json(
        state
            .runs
            .as_ref()
            .map_or_else(Vec::new, RunController::model_profiles),
    )
    .into_response()
}

async fn list_scenarios(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if !run_request_is_authorized(&state, &headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    Json(
        state
            .runs
            .as_ref()
            .map_or_else(Vec::new, RunController::scenarios),
    )
    .into_response()
}

async fn list_runs(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if !run_request_is_authorized(&state, &headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    Json(
        state
            .runs
            .as_ref()
            .map_or_else(Vec::new, RunController::list),
    )
    .into_response()
}

async fn start_run(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<StartRunRequest>,
) -> Response {
    let Some(runs) = authorized_runs(&state, &headers) else {
        return StatusCode::FORBIDDEN.into_response();
    };
    if request
        .model_id
        .as_deref()
        .is_some_and(|model| !model_is_available(&state.config, model))
    {
        return unavailable_model_response(request.model_id.as_deref().unwrap_or_default());
    }
    match runs.start(request).await {
        Ok(summary) => (StatusCode::CREATED, Json(summary)).into_response(),
        Err(error) => run_error_response(error),
    }
}

async fn prepare_run(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<PrepareExploreRequest>,
) -> Response {
    let Some(runs) = authorized_runs(&state, &headers) else {
        return StatusCode::FORBIDDEN.into_response();
    };
    match runs
        .prepare_from_workspace(
            PrepareRunRequest {
                scenario_id: request.scenario_id,
            },
            request.source_workspace_id.as_deref(),
        )
        .await
    {
        Ok(summary) => (StatusCode::CREATED, Json(summary)).into_response(),
        Err(error) => run_error_response(error),
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PrepareExploreRequest {
    scenario_id: String,
    #[serde(default)]
    source_workspace_id: Option<String>,
}

async fn start_prepared_run(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<StartPreparedRunRequest>,
) -> Response {
    let Some(runs) = authorized_runs(&state, &headers) else {
        return StatusCode::FORBIDDEN.into_response();
    };
    if request
        .model_id
        .as_deref()
        .is_some_and(|model| !model_is_available(&state.config, model))
    {
        return unavailable_model_response(request.model_id.as_deref().unwrap_or_default());
    }
    match runs.start_prepared(&id, &request) {
        Ok(summary) => Json(summary).into_response(),
        Err(error) => run_error_response(error),
    }
}

fn model_is_available(config: &ServerConfig, model_id: &str) -> bool {
    config.models.is_empty() || config.models.iter().any(|model| model == model_id)
}

fn unavailable_model_response(model_id: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({
            "error": format!("model is not available from this harness: {model_id}")
        })),
    )
        .into_response()
}

async fn get_run(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<String>,
) -> Response {
    let Some(runs) = authorized_runs(&state, &headers) else {
        return StatusCode::FORBIDDEN.into_response();
    };
    match runs.get(&id) {
        Ok(run) => Json(run).into_response(),
        Err(error) => run_error_response(error),
    }
}

async fn cancel_run(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<String>,
) -> Response {
    let Some(runs) = authorized_runs(&state, &headers) else {
        return StatusCode::FORBIDDEN.into_response();
    };
    match runs.cancel(&id) {
        Ok(()) => StatusCode::ACCEPTED.into_response(),
        Err(error) => run_error_response(error),
    }
}

async fn run_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<String>,
) -> Response {
    let Some(runs) = authorized_runs(&state, &headers) else {
        return StatusCode::FORBIDDEN.into_response();
    };
    let Ok((history, receiver)) = runs.subscribe(&id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let event_stream_epoch = state.event_stream_epoch.clone();
    let stream = run_event_stream(runs, id, history, receiver)
        .map(|event| {
            Event::default()
                .id(event.sequence.to_string())
                .event(&event.kind)
                .json_data(event)
        })
        .take_until(state.config.shutdown.cancelled_owned());
    let response = Sse::new(stream)
        .keep_alive(axum::response::sse::KeepAlive::default())
        .into_response();
    with_event_stream_epoch(response, event_stream_epoch)
}

fn with_event_stream_epoch(mut response: Response, epoch: HeaderValue) -> Response {
    response
        .headers_mut()
        .insert(EVENT_STREAM_EPOCH_HEADER, epoch);
    response
}

fn run_event_stream(
    runs: RunController,
    id: String,
    history: Vec<RunEvent>,
    receiver: tokio::sync::broadcast::Receiver<RunEvent>,
) -> impl futures_util::Stream<Item = RunEvent> {
    futures_util::stream::unfold(
        (VecDeque::from(history), receiver, runs, id, 0_u64),
        |(mut pending, mut receiver, runs, id, mut last_sequence)| async move {
            loop {
                if !pending.is_empty() {
                    let Ok(current) = runs.events_after(&id, last_sequence) else {
                        return None;
                    };
                    pending = VecDeque::from(current);
                }
                if let Some(event) = pending.pop_front() {
                    if event_is_after_history(&event, last_sequence) {
                        last_sequence = event.sequence;
                        return Some((event, (pending, receiver, runs, id, last_sequence)));
                    }
                    continue;
                }
                match receiver.recv().await {
                    Ok(event) => pending.push_back(event),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let Ok(events) = runs.events_after(&id, last_sequence) else {
                            return None;
                        };
                        pending.extend(events);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                }
            }
        },
    )
}

fn event_is_after_history(event: &RunEvent, live_after_sequence: u64) -> bool {
    event.sequence > live_after_sequence
}

async fn list_evaluations(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if !run_request_is_authorized(&state, &headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    Json(
        state
            .runs
            .as_ref()
            .map_or_else(Vec::new, RunController::list_evaluations),
    )
    .into_response()
}

async fn get_workbench(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<String>,
) -> Response {
    let Some((runs, _origin)) = authorized_workbench(&state, &headers, &id) else {
        return StatusCode::FORBIDDEN.into_response();
    };
    match runs.workbench(&id) {
        Ok(workbench) => Json(workbench).into_response(),
        Err(error) => run_error_response(error),
    }
}

async fn update_workbench_selection(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<UpdateWorkbenchSelectionRequest>,
) -> Response {
    let Some((runs, origin)) = authorized_workbench(&state, &headers, &id) else {
        return StatusCode::FORBIDDEN.into_response();
    };
    match runs.update_workbench_selection(&id, request, origin) {
        Ok(selection) => Json(selection).into_response(),
        Err(error) => run_error_response(error),
    }
}

async fn compare_workbench(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<CompareWorkbenchRequest>,
) -> Response {
    let Some((runs, origin)) = authorized_workbench(&state, &headers, &id) else {
        return StatusCode::FORBIDDEN.into_response();
    };
    match runs.compare_workbench(&id, request, origin) {
        Ok(evaluation) => (StatusCode::CREATED, Json(evaluation)).into_response(),
        Err(error) => run_error_response(error),
    }
}

async fn list_agent_sessions(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(workspace_id): AxumPath<String>,
) -> Response {
    let Some((runs, _origin)) = authorized_workbench(&state, &headers, &workspace_id) else {
        return StatusCode::FORBIDDEN.into_response();
    };
    if let Err(error) = runs.ensure_exploring_workspace(&workspace_id) {
        return run_error_response(error);
    }
    Json(runs.list_agent_sessions(&workspace_id)).into_response()
}

async fn start_agent_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(workspace_id): AxumPath<String>,
    Json(request): Json<StartAgentSessionRequest>,
) -> Response {
    let Some((runs, origin)) = authorized_workbench(&state, &headers, &workspace_id) else {
        return StatusCode::FORBIDDEN.into_response();
    };
    match tokio::task::spawn_blocking(move || {
        runs.start_agent_session(&workspace_id, request, origin)
    })
    .await
    {
        Ok(Ok(session)) => (StatusCode::CREATED, Json(session)).into_response(),
        Ok(Err(error)) => run_error_response(error),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": format!("agent session task failed: {error}") })),
        )
            .into_response(),
    }
}

async fn get_agent_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath((workspace_id, session_id)): AxumPath<(String, String)>,
) -> Response {
    let Some((runs, _origin)) = authorized_workbench(&state, &headers, &workspace_id) else {
        return StatusCode::FORBIDDEN.into_response();
    };
    match runs.agent_session(&workspace_id, &session_id) {
        Ok(session) => Json(session).into_response(),
        Err(error) => run_error_response(error),
    }
}

async fn activate_agent_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath((workspace_id, session_id)): AxumPath<(String, String)>,
) -> Response {
    let Some((runs, origin)) = authorized_workbench(&state, &headers, &workspace_id) else {
        return StatusCode::FORBIDDEN.into_response();
    };
    match runs.activate_agent_session(&workspace_id, &session_id, origin) {
        Ok(session) => Json(session).into_response(),
        Err(error) => run_error_response(error),
    }
}

async fn start_agent_turn(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath((workspace_id, session_id)): AxumPath<(String, String)>,
    Json(request): Json<StartAgentTurnRequest>,
) -> Response {
    let Some((runs, origin)) = authorized_workbench(&state, &headers, &workspace_id) else {
        return StatusCode::FORBIDDEN.into_response();
    };
    match tokio::task::spawn_blocking(move || {
        runs.start_agent_turn(&workspace_id, &session_id, request, origin)
    })
    .await
    {
        Ok(Ok(turn)) => (StatusCode::ACCEPTED, Json(turn)).into_response(),
        Ok(Err(error)) => run_error_response(error),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": format!("agent turn task failed: {error}") })),
        )
            .into_response(),
    }
}

async fn cancel_agent_turn(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath((workspace_id, session_id)): AxumPath<(String, String)>,
) -> Response {
    let Some((runs, _origin)) = authorized_workbench(&state, &headers, &workspace_id) else {
        return StatusCode::FORBIDDEN.into_response();
    };
    match runs.cancel_agent_turn(&workspace_id, &session_id) {
        Ok(()) => StatusCode::ACCEPTED.into_response(),
        Err(error) => run_error_response(error),
    }
}

async fn close_agent_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath((workspace_id, session_id)): AxumPath<(String, String)>,
) -> Response {
    let Some((runs, _origin)) = authorized_workbench(&state, &headers, &workspace_id) else {
        return StatusCode::FORBIDDEN.into_response();
    };
    match runs.close_agent_session(&workspace_id, &session_id) {
        Ok(session) => (StatusCode::ACCEPTED, Json(session)).into_response(),
        Err(error) => run_error_response(error),
    }
}

async fn agent_session_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath((workspace_id, session_id)): AxumPath<(String, String)>,
) -> Response {
    let Some((runs, _origin)) = authorized_workbench(&state, &headers, &workspace_id) else {
        return StatusCode::FORBIDDEN.into_response();
    };
    let Ok((history, receiver)) = runs.subscribe_agent_session(&workspace_id, &session_id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let stream = agent_session_event_stream(runs, session_id, history, receiver)
        .map(|event| {
            Event::default()
                .id(event.sequence.to_string())
                .event(&event.kind)
                .json_data(event)
        })
        .take_until(state.config.shutdown.cancelled_owned());
    Sse::new(stream)
        .keep_alive(axum::response::sse::KeepAlive::default())
        .into_response()
}

fn agent_session_event_stream(
    runs: RunController,
    id: String,
    history: Vec<RunEvent>,
    receiver: tokio::sync::broadcast::Receiver<RunEvent>,
) -> impl futures_util::Stream<Item = RunEvent> {
    futures_util::stream::unfold(
        (VecDeque::from(history), receiver, runs, id, 0_u64),
        |(mut pending, mut receiver, runs, id, mut last_sequence)| async move {
            loop {
                if !pending.is_empty() {
                    let Ok(current) = runs.agent_session_events_after(&id, last_sequence) else {
                        return None;
                    };
                    pending = VecDeque::from(current);
                }
                if let Some(event) = pending.pop_front() {
                    if event_is_after_history(&event, last_sequence) {
                        last_sequence = event.sequence;
                        return Some((event, (pending, receiver, runs, id, last_sequence)));
                    }
                    continue;
                }
                match receiver.recv().await {
                    Ok(event) => pending.push_back(event),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let Ok(events) = runs.agent_session_events_after(&id, last_sequence) else {
                            return None;
                        };
                        pending.extend(events);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                }
            }
        },
    )
}

async fn get_workbench_evaluation(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath((workspace_id, evaluation_id)): AxumPath<(String, String)>,
) -> Response {
    let Some((runs, _origin)) = authorized_workbench(&state, &headers, &workspace_id) else {
        return StatusCode::FORBIDDEN.into_response();
    };
    let evaluation_id = (evaluation_id != "latest").then_some(evaluation_id.as_str());
    match runs.workbench_evaluation(&workspace_id, evaluation_id) {
        Ok(evaluation) => Json(evaluation).into_response(),
        Err(error) => run_error_response(error),
    }
}

async fn workbench_evaluation_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath((workspace_id, evaluation_id)): AxumPath<(String, String)>,
) -> Response {
    let Some((runs, _origin)) = authorized_workbench(&state, &headers, &workspace_id) else {
        return StatusCode::FORBIDDEN.into_response();
    };
    let Ok(detail) = runs.workbench_evaluation(&workspace_id, Some(&evaluation_id)) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Ok((history, receiver)) = runs.subscribe_evaluation(&detail.summary.id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    evaluation_event_response(
        runs,
        detail.summary.id,
        history,
        receiver,
        state.config.shutdown,
    )
}

async fn cancel_workbench_evaluation(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath((workspace_id, evaluation_id)): AxumPath<(String, String)>,
) -> Response {
    let Some((runs, _origin)) = authorized_workbench(&state, &headers, &workspace_id) else {
        return StatusCode::FORBIDDEN.into_response();
    };
    if runs
        .workbench_evaluation(&workspace_id, Some(&evaluation_id))
        .is_err()
    {
        return StatusCode::NOT_FOUND.into_response();
    }
    match runs.cancel_evaluation(&evaluation_id) {
        Ok(()) => StatusCode::ACCEPTED.into_response(),
        Err(error) => run_error_response(error),
    }
}

async fn start_evaluation(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<StartEvaluationRequest>,
) -> Response {
    let Some(runs) = authorized_runs(&state, &headers) else {
        return StatusCode::FORBIDDEN.into_response();
    };
    match runs.start_evaluation(request) {
        Ok(summary) => (StatusCode::CREATED, Json(summary)).into_response(),
        Err(error) => run_error_response(error),
    }
}

async fn get_evaluation(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<String>,
) -> Response {
    let Some(runs) = authorized_runs(&state, &headers) else {
        return StatusCode::FORBIDDEN.into_response();
    };
    match runs.get_evaluation(&id) {
        Ok(evaluation) => Json(evaluation).into_response(),
        Err(error) => run_error_response(error),
    }
}

async fn cancel_evaluation(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<String>,
) -> Response {
    let Some(runs) = authorized_runs(&state, &headers) else {
        return StatusCode::FORBIDDEN.into_response();
    };
    match runs.cancel_evaluation(&id) {
        Ok(()) => StatusCode::ACCEPTED.into_response(),
        Err(error) => run_error_response(error),
    }
}

async fn evaluation_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<String>,
) -> Response {
    let Some(runs) = authorized_runs(&state, &headers) else {
        return StatusCode::FORBIDDEN.into_response();
    };
    let Ok((history, receiver)) = runs.subscribe_evaluation(&id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    evaluation_event_response(runs, id, history, receiver, state.config.shutdown)
}

fn evaluation_event_response(
    runs: RunController,
    id: String,
    history: Vec<RunEvent>,
    receiver: tokio::sync::broadcast::Receiver<RunEvent>,
    shutdown: CancellationToken,
) -> Response {
    let stream = evaluation_event_stream(runs, id, history, receiver)
        .map(|event| {
            Event::default()
                .id(event.sequence.to_string())
                .event(&event.kind)
                .json_data(event)
        })
        .take_until(shutdown.cancelled_owned());
    Sse::new(stream)
        .keep_alive(axum::response::sse::KeepAlive::default())
        .into_response()
}

fn evaluation_event_stream(
    runs: RunController,
    id: String,
    history: Vec<RunEvent>,
    receiver: tokio::sync::broadcast::Receiver<RunEvent>,
) -> impl futures_util::Stream<Item = RunEvent> {
    futures_util::stream::unfold(
        (VecDeque::from(history), receiver, runs, id, 0_u64, false),
        |(mut pending, mut receiver, runs, id, mut last_sequence, finished)| async move {
            if finished {
                return None;
            }
            loop {
                if !pending.is_empty() {
                    let Ok(current) = runs.evaluation_events_after(&id, last_sequence) else {
                        return None;
                    };
                    pending = VecDeque::from(current);
                }
                if let Some(event) = pending.pop_front() {
                    if event_is_after_history(&event, last_sequence) {
                        last_sequence = event.sequence;
                        let finished = evaluation_event_is_terminal(&event);
                        return Some((
                            event,
                            (pending, receiver, runs, id, last_sequence, finished),
                        ));
                    }
                    continue;
                }
                match receiver.recv().await {
                    Ok(event) => pending.push_back(event),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let Ok(events) = runs.evaluation_events_after(&id, last_sequence) else {
                            return None;
                        };
                        pending.extend(events);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                }
            }
        },
    )
}

fn evaluation_event_is_terminal(event: &RunEvent) -> bool {
    matches!(
        event.kind.as_str(),
        "evaluation.finished" | "evaluation.unavailable"
    )
}

fn authorized_workbench(
    state: &AppState,
    headers: &HeaderMap,
    workspace_id: &str,
) -> Option<(RunController, WorkbenchOrigin)> {
    if let Some(runs) = authorized_runs(state, headers) {
        return Some((runs, WorkbenchOrigin::Browser));
    }
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))?;
    let runs = state.runs.clone()?;
    runs.workbench_grant_allows(bearer, workspace_id)
        .then_some((runs, WorkbenchOrigin::Nushell))
}

fn authorized_runs(state: &AppState, headers: &HeaderMap) -> Option<RunController> {
    run_request_is_authorized(state, headers)
        .then(|| state.runs.clone())
        .flatten()
}

fn run_request_is_authorized(state: &AppState, headers: &HeaderMap) -> bool {
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    bearer == Some(state.config.token.as_str())
        && request_is_same_origin(headers, &state.config.origin, true)
}

fn run_error_response(error: RunError) -> Response {
    let (status, message): (StatusCode, String) = error.into();
    (status, Json(serde_json::json!({ "error": message }))).into_response()
}

#[derive(Serialize)]
struct TokenResponse<'a> {
    token: &'a str,
}

async fn session_token(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if !request_is_same_origin(&headers, &state.config.origin, true) {
        return StatusCode::FORBIDDEN.into_response();
    }

    let mut response = Json(TokenResponse {
        token: &state.config.token,
    })
    .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, max-age=0"),
    );
    response
}

#[derive(Debug, Deserialize)]
struct TerminalQuery {
    #[serde(default = "default_cols")]
    cols: u16,
    #[serde(default = "default_rows")]
    rows: u16,
    #[serde(rename = "runId")]
    run_id: Option<String>,
}

const fn default_cols() -> u16 {
    DEFAULT_COLS
}

const fn default_rows() -> u16 {
    DEFAULT_ROWS
}

async fn upgrade_terminal(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<TerminalQuery>,
    upgrade: WebSocketUpgrade,
) -> Response {
    let auth_protocol = format!("{AUTH_PROTOCOL_PREFIX}{}", state.config.token);
    if !terminal_request_is_authorized(&headers, &auth_protocol, &state.config) {
        return StatusCode::FORBIDDEN.into_response();
    }

    let Ok(size) = (TerminalSize {
        cols: query.cols,
        rows: query.rows,
    })
    .validated() else {
        return StatusCode::BAD_REQUEST.into_response();
    };

    upgrade
        .protocols([auth_protocol])
        .on_upgrade(move |socket| {
            serve_terminal(
                socket,
                state.provider,
                size,
                query.run_id,
                state.config.shutdown,
            )
        })
}

fn terminal_request_is_authorized(
    headers: &HeaderMap,
    auth_protocol: &str,
    config: &ServerConfig,
) -> bool {
    headers
        .get(header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|protocol| protocol.trim() == auth_protocol)
        })
        && request_is_same_origin(headers, &config.origin, false)
}

fn request_is_same_origin(headers: &HeaderMap, expected: &str, allow_referer: bool) -> bool {
    let Ok(expected_uri) = expected.parse::<Uri>() else {
        return false;
    };
    let Some(expected_scheme) = expected_uri.scheme_str() else {
        return false;
    };
    let Some(request_origin) = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .map(|host| format!("{expected_scheme}://{host}"))
    else {
        return false;
    };
    if !origin_reaches_bound_listener(&request_origin, expected) {
        return false;
    }

    match headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
    {
        Some(origin) => origin == request_origin,
        None if allow_referer => headers
            .get(header::REFERER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<Uri>().ok())
            .is_some_and(|uri| {
                uri.scheme_str()
                    .zip(uri.authority())
                    .is_some_and(|(scheme, authority)| {
                        request_origin == format!("{scheme}://{authority}")
                    })
            }),
        None => false,
    }
}

fn origin_reaches_bound_listener(request_origin: &str, expected: &str) -> bool {
    if request_origin == expected {
        return true;
    }

    let Ok(request_uri) = request_origin.parse::<Uri>() else {
        return false;
    };
    let Ok(expected_uri) = expected.parse::<Uri>() else {
        return false;
    };
    let Some(request_authority) = request_uri.authority() else {
        return false;
    };
    let Some(expected_authority) = expected_uri.authority() else {
        return false;
    };

    request_uri.scheme_str() == Some("http")
        && expected_uri.scheme_str() == Some("http")
        && request_authority.port_u16() == expected_authority.port_u16()
        && matches!(
            (request_authority.host(), expected_authority.host()),
            ("localhost", "127.0.0.1") | ("127.0.0.1", "localhost")
        )
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientControl {
    Resize { cols: u16, rows: u16 },
    HumanInput,
}

/// Structured lifecycle evidence carried separately from binary PTY frames.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum SessionEvent<'a> {
    /// A provider opened a new bounded session.
    Started {
        provider: &'a str,
        cols: u16,
        rows: u16,
    },
    /// The browser and the underlying session accepted new terminal dimensions.
    Resized { cols: u16, rows: u16 },
    /// The session's output stream reached EOF.
    Exited,
    /// The provider could not open or initialize the session.
    Error { message: &'a str },
}

fn session_event_message(event: &SessionEvent<'_>) -> Message {
    Message::Text(
        serde_json::to_string(event)
            .expect("session event serialization cannot fail")
            .into(),
    )
}

fn spawn_session_reader(
    mut reader: Box<dyn Read + Send>,
) -> (
    mpsc::Receiver<Result<Vec<u8>, std::io::Error>>,
    tokio::task::JoinHandle<()>,
) {
    let (output_tx, output_rx) = mpsc::channel(32);
    let task = tokio::task::spawn_blocking(move || {
        let mut buffer = [0_u8; 8192];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => break,
                Err(error) if is_pty_eof(&error) => break,
                Err(error) => {
                    tracing::debug!(%error, "terminal reader stopped");
                    let _ = output_tx.blocking_send(Err(error));
                    break;
                }
                Ok(read) => {
                    if output_tx
                        .blocking_send(Ok(buffer[..read].to_vec()))
                        .is_err()
                    {
                        break;
                    }
                }
            }
        }
    });
    (output_rx, task)
}

fn is_pty_eof(error: &std::io::Error) -> bool {
    #[cfg(unix)]
    {
        error.raw_os_error() == Some(libc::EIO)
    }

    #[cfg(not(unix))]
    {
        let _ = error;
        false
    }
}

fn spawn_session_writer(
    session: Arc<dyn BrowserSession>,
) -> (
    mpsc::Sender<Vec<u8>>,
    tokio::task::JoinHandle<Result<(), GatewayError>>,
) {
    let (input_tx, mut input_rx) = mpsc::channel::<Vec<u8>>(32);
    let task = tokio::task::spawn_blocking(move || {
        while let Some(bytes) = input_rx.blocking_recv() {
            session.write(&bytes)?;
        }
        Ok(())
    });
    (input_tx, task)
}

async fn open_session(
    provider: Arc<dyn SessionProvider>,
    initial_size: TerminalSize,
    run_id: Option<String>,
) -> Result<Box<dyn BrowserSession>, String> {
    match tokio::task::spawn_blocking(move || provider.open(initial_size, run_id.as_deref())).await
    {
        Ok(result) => result.map_err(|error| error.to_string()),
        Err(error) => Err(format!("session provider task failed: {error}")),
    }
}

#[allow(clippy::too_many_lines)]
async fn serve_terminal(
    socket: WebSocket,
    provider: Arc<dyn SessionProvider>,
    initial_size: TerminalSize,
    run_id: Option<String>,
    shutdown: CancellationToken,
) {
    let session = match open_session(Arc::clone(&provider), initial_size, run_id).await {
        Ok(session) => session,
        Err(message) => {
            send_open_error(socket, &message).await;
            return;
        }
    };

    let (mut socket_tx, mut socket_rx) = socket.split();
    let started = session_event_message(&SessionEvent::Started {
        provider: provider.name(),
        cols: initial_size.cols,
        rows: initial_size.rows,
    });
    if socket_tx.send(started).await.is_err() {
        session.terminate();
        return;
    }

    let reader = match session.take_reader() {
        Ok(reader) => reader,
        Err(error) => {
            session.terminate();
            let message = error.to_string();
            let event = session_event_message(&SessionEvent::Error { message: &message });
            let _ = socket_tx.send(event).await;
            return;
        }
    };
    let session: Arc<dyn BrowserSession> = Arc::from(session);
    let (mut pty_rx, read_task) = spawn_session_reader(reader);
    let (input_tx, mut write_task) = spawn_session_writer(Arc::clone(&session));

    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            result = &mut write_task => {
                if let Ok(Err(error)) = result {
                    tracing::debug!(%error, "terminal writer stopped");
                }
                break;
            },
            output = pty_rx.recv() => match output {
                Some(Ok(bytes)) => {
                    if socket_tx.send(Message::Binary(bytes.into())).await.is_err() {
                        break;
                    }
                }
                Some(Err(error)) => {
                    let message = error.to_string();
                    let event = session_event_message(&SessionEvent::Error { message: &message });
                    let _ = socket_tx.send(event).await;
                    break;
                }
                None => {
                    let _ = socket_tx
                        .send(session_event_message(&SessionEvent::Exited))
                        .await;
                    break;
                }
            },
            incoming = socket_rx.next() => match incoming {
                Some(Ok(Message::Binary(bytes))) => {
                    if input_tx.send(bytes.to_vec()).await.is_err() {
                        break;
                    }
                }
                Some(Ok(Message::Text(text))) => {
                    let Ok(control) = serde_json::from_str(&text) else {
                        continue;
                    };
                    match control {
                        ClientControl::Resize { cols, rows } => {
                            let Ok(size) = (TerminalSize { cols, rows }).validated() else {
                                continue;
                            };
                            if session.resize(size).is_err() {
                                break;
                            }
                            let resized = session_event_message(&SessionEvent::Resized {
                                cols: size.cols,
                                rows: size.rows,
                            });
                            if socket_tx.send(resized).await.is_err() {
                                break;
                            }
                        }
                        ClientControl::HumanInput => {
                            if session.note_human_input().is_err() {
                                break;
                            }
                        }
                    }
                }
                Some(Ok(Message::Close(_)) | Err(_)) | None => break,
                Some(Ok(Message::Ping(data))) => {
                    if socket_tx.send(Message::Pong(data)).await.is_err() {
                        break;
                    }
                }
                Some(Ok(Message::Pong(_))) => {}
            }
        }
    }

    drop(input_tx);
    session.terminate();
    read_task.abort();
    write_task.abort();
}

async fn send_open_error(mut socket: WebSocket, message: &str) {
    let event = session_event_message(&SessionEvent::Error { message });
    let _ = socket.send(event).await;
    let _ = socket.close().await;
}

fn generate_token() -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = rand::rng().random::<[u8; 32]>();
    let mut token = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        token.push(HEX[usize::from(byte >> 4)] as char);
        token.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    token
}

/// Errors produced while opening or operating a terminal session.
#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    /// The configured shell executable does not exist.
    #[error("visual shell binary not found at {0}")]
    ShellNotFound(PathBuf),
    /// The requested terminal dimensions were unsafe or nonsensical.
    #[error("terminal size must be between 1 and 500 rows and columns")]
    InvalidSize,
    /// A session resource was already consumed or became unavailable.
    #[error("terminal session became unavailable")]
    SessionUnavailable,
    /// Agent Lab terminals are always attached to one prepared scenario workspace.
    #[error("a prepared scenario run is required for this terminal")]
    RunRequired,
    /// Operating-system PTY failure.
    #[error("PTY operation failed: {0}")]
    Pty(#[from] anyhow::Error),
    /// Terminal I/O failure.
    #[error("terminal I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// The requested run could not provide its workspace or capability source.
    #[error(transparent)]
    Run(#[from] RunError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    struct EioReader;

    #[cfg(unix)]
    impl Read for EioReader {
        fn read(&mut self, _buffer: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::from_raw_os_error(libc::EIO))
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pty_eio_is_treated_as_eof() {
        let (mut output, task) = spawn_session_reader(Box::new(EioReader));

        assert!(output.recv().await.is_none());
        task.await.unwrap();
    }

    #[tokio::test]
    async fn evaluation_stream_ends_after_each_terminal_event() {
        let root =
            std::env::temp_dir().join(format!("agent-lab-evaluation-stream-{}", generate_token()));
        let scenarios_dir = root.join("scenarios");
        let data_dir = root.join("data");
        std::fs::create_dir_all(scenarios_dir.join("fixture/workspace")).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::write(
            scenarios_dir.join("fixture.toml"),
            r#"
version = 1
id = "fixture"
title = "Fixture"
description = "test"
question = "Does the event stream stop after a terminal event?"
seed = "fixture/workspace"
prompt = "finish"
output = "result.json"

[limits]
maxDurationMs = 1000
maxCommandCount = 1
maxOrchestratorInvocations = 1
maxToolInvocations = 1

[assertions]
activeNames = []
totalScore = 0
"#,
        )
        .unwrap();
        let harness = |id: &str| HarnessProfile {
            id: id.to_owned(),
            display_name: id.to_owned(),
            launch: agent_lab_driver_protocol::DriverLaunch::new("/bin/false"),
            models: std::collections::BTreeMap::from([("test".to_owned(), format!("{id}/test"))]),
        };
        let runs = RunController::new_with_harnesses(
            RunControllerConfig {
                scenarios_dir,
                data_dir,
                driver: agent_lab_driver_protocol::DriverLaunch::new(
                    std::env::current_exe().unwrap(),
                ),
            },
            vec![harness("fixture-a"), harness("fixture-b")],
            std::collections::BTreeMap::from([("test".to_owned(), "Test".to_owned())]),
        )
        .unwrap();
        let workspace = runs
            .prepare(PrepareRunRequest {
                scenario_id: "fixture".to_owned(),
            })
            .await
            .unwrap();
        let evaluation = runs
            .start_evaluation(StartEvaluationRequest {
                scenario_id: "fixture".to_owned(),
                model_profile_id: "test".to_owned(),
                source_workspace_id: workspace.id,
                harness_ids: vec!["fixture-a".to_owned(), "fixture-b".to_owned()],
            })
            .unwrap();
        let (history, receiver) = runs.subscribe_evaluation(&evaluation.id).unwrap();
        let stream = evaluation_event_stream(runs.clone(), evaluation.id, history, receiver)
            .collect::<Vec<_>>();
        let events = tokio::time::timeout(std::time::Duration::from_secs(5), stream)
            .await
            .expect("evaluation stream should reach a terminal event");

        assert_eq!(
            events.last().map(|event| event.kind.as_str()),
            Some("evaluation.finished")
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| evaluation_event_is_terminal(event))
                .count(),
            1
        );
        assert!(evaluation_event_is_terminal(&RunEvent {
            sequence: 1,
            at_ms: 1,
            kind: "evaluation.unavailable".to_owned(),
            payload: serde_json::Value::Null,
            progress: None,
        }));

        drop(runs);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn evidence_streams_revalidate_unpolled_history_before_yielding() {
        let root =
            std::env::temp_dir().join(format!("agent-lab-history-recheck-{}", generate_token()));
        let scenarios_dir = root.join("scenarios");
        let data_dir = root.join("data");
        std::fs::create_dir_all(scenarios_dir.join("fixture/workspace")).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::write(
            scenarios_dir.join("fixture.toml"),
            r#"
version = 1
id = "fixture"
title = "Fixture"
description = "test"
question = "Does the event stream revalidate history?"
seed = "fixture/workspace"
prompt = "finish"
output = "result.json"

[limits]
maxDurationMs = 1000
maxCommandCount = 1
maxOrchestratorInvocations = 1
maxToolInvocations = 1

[assertions]
activeNames = []
totalScore = 0
"#,
        )
        .unwrap();
        let runs = RunController::new(RunControllerConfig {
            scenarios_dir,
            data_dir,
            driver: agent_lab_driver_protocol::DriverLaunch::new(std::env::current_exe().unwrap()),
        })
        .unwrap();
        let prepared = runs
            .prepare(PrepareRunRequest {
                scenario_id: "fixture".to_owned(),
            })
            .await
            .unwrap();
        let stale = RunEvent {
            sequence: 1,
            at_ms: 1,
            kind: "stale".to_owned(),
            payload: serde_json::json!({ "credential": "late-credential" }),
            progress: None,
        };

        {
            let (_sender, receiver) = tokio::sync::broadcast::channel(4);
            let stream = run_event_stream(
                runs.clone(),
                prepared.id.clone(),
                vec![stale.clone()],
                receiver,
            );
            futures_util::pin_mut!(stream);
            let event = stream.next().await.expect("authoritative run event");
            assert!(
                !serde_json::to_string(&event)
                    .unwrap()
                    .contains("late-credential")
            );
        }
        {
            let (_sender, receiver) = tokio::sync::broadcast::channel(4);
            let stream = agent_session_event_stream(
                runs.clone(),
                "removed-session".to_owned(),
                vec![stale.clone()],
                receiver,
            );
            futures_util::pin_mut!(stream);
            assert!(stream.next().await.is_none());
        }
        {
            let (_sender, receiver) = tokio::sync::broadcast::channel(4);
            let stream = evaluation_event_stream(
                runs.clone(),
                "removed-evaluation".to_owned(),
                vec![stale],
                receiver,
            );
            futures_util::pin_mut!(stream);
            assert!(stream.next().await.is_none());
        }

        drop(runs);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn evidence_streams_revalidate_non_lagged_live_events_before_yielding() {
        let root =
            std::env::temp_dir().join(format!("agent-lab-live-recheck-{}", generate_token()));
        let scenarios_dir = root.join("scenarios");
        let data_dir = root.join("data");
        std::fs::create_dir_all(scenarios_dir.join("fixture/workspace")).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::write(
            scenarios_dir.join("fixture.toml"),
            r#"
version = 1
id = "fixture"
title = "Fixture"
description = "test"
question = "Does the event stream revalidate live events?"
seed = "fixture/workspace"
prompt = "finish"
output = "result.json"

[limits]
maxDurationMs = 1000
maxCommandCount = 1
maxOrchestratorInvocations = 1
maxToolInvocations = 1

[assertions]
activeNames = []
totalScore = 0
"#,
        )
        .unwrap();
        let runs = RunController::new(RunControllerConfig {
            scenarios_dir,
            data_dir,
            driver: agent_lab_driver_protocol::DriverLaunch::new(std::env::current_exe().unwrap()),
        })
        .unwrap();
        let prepared = runs
            .prepare(PrepareRunRequest {
                scenario_id: "fixture".to_owned(),
            })
            .await
            .unwrap();
        let stale = RunEvent {
            sequence: 99,
            at_ms: 1,
            kind: "stale".to_owned(),
            payload: serde_json::json!({ "credential": "late-credential" }),
            progress: None,
        };

        {
            let (sender, receiver) = tokio::sync::broadcast::channel(4);
            sender.send(stale.clone()).unwrap();
            let stream = run_event_stream(runs.clone(), prepared.id.clone(), Vec::new(), receiver);
            futures_util::pin_mut!(stream);
            let event = stream.next().await.expect("authoritative run event");
            assert!(
                !serde_json::to_string(&event)
                    .unwrap()
                    .contains("late-credential")
            );
        }
        {
            let (sender, receiver) = tokio::sync::broadcast::channel(4);
            sender.send(stale.clone()).unwrap();
            let stream = agent_session_event_stream(
                runs.clone(),
                "removed-session".to_owned(),
                Vec::new(),
                receiver,
            );
            futures_util::pin_mut!(stream);
            assert!(stream.next().await.is_none());
        }
        {
            let (sender, receiver) = tokio::sync::broadcast::channel(4);
            sender.send(stale).unwrap();
            let stream = evaluation_event_stream(
                runs.clone(),
                "removed-evaluation".to_owned(),
                Vec::new(),
                receiver,
            );
            futures_util::pin_mut!(stream);
            assert!(stream.next().await.is_none());
        }

        drop(runs);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn terminal_sizes_are_bounded() {
        assert!(TerminalSize { cols: 80, rows: 24 }.validated().is_ok());
        assert!(TerminalSize { cols: 0, rows: 24 }.validated().is_err());
        assert!(
            TerminalSize {
                cols: 80,
                rows: 501
            }
            .validated()
            .is_err()
        );
    }

    #[test]
    fn human_input_uses_a_control_frame_distinct_from_pty_bytes() {
        let control = serde_json::from_str::<ClientControl>(r#"{"type":"human_input"}"#)
            .expect("human input control should parse");
        assert!(matches!(control, ClientControl::HumanInput));
    }

    #[test]
    fn live_run_events_skip_sequences_already_present_in_history() {
        let duplicate = RunEvent {
            sequence: 7,
            at_ms: 1,
            kind: "duplicate".to_owned(),
            payload: serde_json::Value::Null,
            progress: None,
        };
        let next = RunEvent {
            sequence: 8,
            at_ms: 2,
            kind: "next".to_owned(),
            payload: serde_json::Value::Null,
            progress: None,
        };
        assert!(!event_is_after_history(&duplicate, 7));
        assert!(event_is_after_history(&next, 7));
    }

    #[test]
    fn run_event_responses_identify_the_server_epoch() {
        let response = with_event_stream_epoch(
            StatusCode::OK.into_response(),
            HeaderValue::from_static("boot-epoch-1"),
        );

        assert_eq!(
            response.headers().get(EVENT_STREAM_EPOCH_HEADER),
            Some(&HeaderValue::from_static("boot-epoch-1"))
        );
    }

    #[tokio::test]
    async fn fixture_only_app_metadata_routes_remain_bootable_without_runs() {
        let config = ServerConfig {
            assets: PathBuf::new(),
            origin: "http://127.0.0.1:4100".to_owned(),
            token: "process-secret".to_owned(),
            models: Vec::new(),
            shutdown: CancellationToken::new(),
        };
        let state = AppState {
            config,
            provider: Arc::new(FixtureSessionProvider::new("/fixture-shell", "/workspace")),
            runs: None,
            event_stream_epoch: HeaderValue::from_static("fixture-event-stream-epoch"),
        };
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("127.0.0.1:4100"));
        headers.insert(
            header::REFERER,
            HeaderValue::from_static("http://127.0.0.1:4100/"),
        );
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer process-secret"),
        );

        assert_eq!(
            list_models(State(state.clone()), headers.clone())
                .await
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            list_scenarios(State(state.clone()), headers.clone())
                .await
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            list_runs(State(state.clone()), headers.clone())
                .await
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            list_harnesses(State(state.clone()), headers.clone())
                .await
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            list_model_profiles(State(state.clone()), headers.clone())
                .await
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            list_evaluations(State(state), headers).await.status(),
            StatusCode::OK
        );
    }

    #[test]
    fn same_origin_requires_the_bound_host_and_websocket_origin() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("127.0.0.1:4100"));
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("http://127.0.0.1:4100"),
        );
        assert!(request_is_same_origin(
            &headers,
            "http://127.0.0.1:4100",
            true
        ));

        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("https://attacker.example"),
        );
        assert!(!request_is_same_origin(
            &headers,
            "http://127.0.0.1:4100",
            true
        ));
    }

    #[test]
    fn same_origin_token_fetch_accepts_the_bound_referer() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("127.0.0.1:4100"));
        headers.insert(
            header::REFERER,
            HeaderValue::from_static("http://127.0.0.1:4100/workbench"),
        );

        assert!(request_is_same_origin(
            &headers,
            "http://127.0.0.1:4100",
            true
        ));
        assert!(!request_is_same_origin(
            &headers,
            "http://127.0.0.1:4100",
            false
        ));

        headers.insert(
            header::REFERER,
            HeaderValue::from_static("https://attacker.example/workbench"),
        );
        assert!(!request_is_same_origin(
            &headers,
            "http://127.0.0.1:4100",
            true
        ));
    }

    #[test]
    fn same_origin_accepts_the_localhost_alias_for_the_bound_loopback_listener() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("localhost:4100"));
        headers.insert(
            header::REFERER,
            HeaderValue::from_static("http://localhost:4100/workbench"),
        );

        assert!(request_is_same_origin(
            &headers,
            "http://127.0.0.1:4100",
            true
        ));

        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("http://localhost:4100"),
        );
        assert!(request_is_same_origin(
            &headers,
            "http://127.0.0.1:4100",
            false
        ));
    }

    #[test]
    fn same_origin_rejects_non_loopback_aliases_and_mismatched_ports() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::HOST,
            HeaderValue::from_static("attacker.example:4100"),
        );
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("http://attacker.example:4100"),
        );
        assert!(!request_is_same_origin(
            &headers,
            "http://127.0.0.1:4100",
            false
        ));

        headers.insert(header::HOST, HeaderValue::from_static("localhost:4200"));
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("http://localhost:4200"),
        );
        assert!(!request_is_same_origin(
            &headers,
            "http://127.0.0.1:4100",
            false
        ));
    }

    #[test]
    fn same_origin_accepts_an_explicit_https_proxy_origin() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::HOST,
            HeaderValue::from_static("workbench.agent-lab.localhost"),
        );
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("https://workbench.agent-lab.localhost"),
        );
        assert!(request_is_same_origin(
            &headers,
            "https://workbench.agent-lab.localhost",
            false
        ));

        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("https://attacker.example"),
        );
        assert!(!request_is_same_origin(
            &headers,
            "https://workbench.agent-lab.localhost",
            false
        ));
    }

    #[test]
    fn configured_model_catalog_rejects_free_form_ids() {
        let config = ServerConfig::new(PathBuf::new(), "http://127.0.0.1:4100".to_owned())
            .with_models(vec!["supported/model".to_owned()]);

        assert!(model_is_available(&config, "supported/model"));
        assert!(!model_is_available(&config, "invented/model"));
    }

    #[test]
    fn terminal_upgrade_requires_the_process_token() {
        let config = ServerConfig {
            assets: PathBuf::new(),
            origin: "http://127.0.0.1:4100".to_owned(),
            token: "process-secret".to_owned(),
            models: Vec::new(),
            shutdown: CancellationToken::new(),
        };
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("127.0.0.1:4100"));
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("http://127.0.0.1:4100"),
        );
        let auth_protocol = format!("{AUTH_PROTOCOL_PREFIX}{}", config.token);
        headers.insert(
            header::SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_static("agent-lab.auth.wrong-token"),
        );

        assert!(!terminal_request_is_authorized(
            &headers,
            &auth_protocol,
            &config
        ));
        headers.insert(
            header::SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_str(&format!("unused, {auth_protocol}")).unwrap(),
        );
        assert!(terminal_request_is_authorized(
            &headers,
            &auth_protocol,
            &config
        ));
    }
}
