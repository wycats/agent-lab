use std::{
    io::{BufRead, BufReader},
    sync::{Arc, mpsc},
    thread,
    time::{Duration, Instant},
};

use agent_lab_driver_protocol::{
    ASSISTANT_COMPLETED_EVENT, ASSISTANT_DELTA_EVENT, TurnObservation,
};
use reqwest::blocking::Client;
use serde_json::{Map, Value as JsonValue, json};
use thiserror::Error;

// A workbench snapshot can probe the single-run harness and a two-harness comparison, with each
// distinct model-access provider bounded to 10 seconds by the controller.
const WORKBENCH_REQUEST_TIMEOUT: Duration = Duration::from_secs(90);
const AGENT_SESSION_START_TIMEOUT: Duration = Duration::from_secs(165);

#[derive(Debug, Error)]
pub enum WorkbenchError {
    #[error("workbench request failed: {0}")]
    Request(String),
    #[error("workbench response was malformed: {0}")]
    Malformed(String),
    #[error("workbench request cancelled: {0}")]
    Cancelled(String),
}

#[derive(Clone)]
pub struct WorkbenchBridge {
    inner: Arc<WorkbenchConfig>,
    requests: mpsc::Sender<JsonRequest>,
}

struct WorkbenchConfig {
    client: Client,
    origin: String,
    workspace_id: String,
    token: String,
}

struct JsonRequest {
    method: reqwest::Method,
    path: String,
    body: Option<JsonValue>,
    reply: mpsc::Sender<Result<JsonValue, WorkbenchError>>,
}

pub struct ComparisonStream {
    pub evaluation: JsonValue,
    pub receiver: mpsc::Receiver<Result<JsonValue, WorkbenchError>>,
}

pub struct ValidationStream {
    pub attempt: JsonValue,
    pub receiver: mpsc::Receiver<Result<JsonValue, WorkbenchError>>,
}

pub struct ProposalStream {
    pub proposal: JsonValue,
    pub receiver: mpsc::Receiver<Result<JsonValue, WorkbenchError>>,
}

pub struct AgentTurnStream {
    pub session: JsonValue,
    pub turn: JsonValue,
    pub receiver: mpsc::Receiver<Result<AgentTurnOutput, WorkbenchError>>,
}

#[derive(Debug, PartialEq)]
pub enum AgentTurnOutput {
    Progress(JsonValue),
    AssistantDelta { message_id: String, text: String },
    AssistantCompleted { message_id: String, text: String },
    Raw(JsonValue),
    Finished { outcome: String },
}

impl WorkbenchBridge {
    /// Create a bridge to one controller-owned Explore workspace.
    ///
    /// # Errors
    ///
    /// Returns an error when the HTTP client or background request worker
    /// cannot be created.
    pub fn new(origin: &str, workspace_id: String, token: String) -> Result<Self, WorkbenchError> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .build()
            .map_err(|error| WorkbenchError::Request(error.to_string()))?;
        let inner = Arc::new(WorkbenchConfig {
            client,
            origin: origin.trim_end_matches('/').to_owned(),
            workspace_id,
            token,
        });
        let (requests, receiver) = mpsc::channel::<JsonRequest>();
        let worker = inner.clone();
        thread::Builder::new()
            .name("agent-lab-workbench-bridge".to_owned())
            .spawn(move || {
                for request in receiver {
                    let result =
                        execute_json_request(&worker, request.method, &request.path, request.body);
                    let _ = request.reply.send(result);
                }
            })
            .map_err(|error| WorkbenchError::Request(error.to_string()))?;
        Ok(Self { inner, requests })
    }

    /// Read the current workbench assembly and shared selection.
    ///
    /// # Errors
    ///
    /// Returns an error when the controller request fails or returns malformed
    /// JSON.
    pub fn assembly(&self) -> Result<JsonValue, WorkbenchError> {
        self.get_json(&format!("/api/workbench/{}", self.inner.workspace_id))
    }

    /// Read a durable evaluation belonging to this workbench.
    ///
    /// # Errors
    ///
    /// Returns an error when the evaluation cannot be loaded or decoded.
    pub fn evaluation(&self, id: Option<&str>) -> Result<JsonValue, WorkbenchError> {
        let id = id.unwrap_or("latest");
        self.get_json(&format!(
            "/api/workbench/{}/evaluations/{id}",
            self.inner.workspace_id
        ))
    }

    /// List the persistent agent sessions attached to this workspace.
    ///
    /// # Errors
    ///
    /// Returns an error when the controller request fails or is malformed.
    pub fn agent_sessions(&self) -> Result<JsonValue, WorkbenchError> {
        self.get_json(&format!(
            "/api/workbench/{}/agent-sessions",
            self.inner.workspace_id
        ))
    }

    /// List evaluation drafts attached to this workspace.
    ///
    /// # Errors
    ///
    /// Returns an error when the controller request fails or is malformed.
    pub fn evaluation_drafts(&self) -> Result<JsonValue, WorkbenchError> {
        self.get_json(&format!(
            "/api/workbench/{}/evaluation-drafts",
            self.inner.workspace_id
        ))
    }

    /// Return one evaluation draft attached to this workspace.
    ///
    /// # Errors
    ///
    /// Returns an error when the controller request fails or is malformed.
    pub fn evaluation_draft(&self, draft_id: &str) -> Result<JsonValue, WorkbenchError> {
        self.get_json(&format!(
            "/api/workbench/{}/evaluation-drafts/{draft_id}",
            self.inner.workspace_id
        ))
    }

    /// Create an evaluation draft from a stable turn span.
    ///
    /// # Errors
    ///
    /// Returns an error when the controller rejects the span or the response is malformed.
    pub fn create_evaluation_draft(
        &self,
        session_id: Option<&str>,
        from_turn_id: &str,
        through_turn_id: &str,
    ) -> Result<JsonValue, WorkbenchError> {
        self.post_json(
            &format!(
                "/api/workbench/{}/evaluation-drafts",
                self.inner.workspace_id
            ),
            &json!({
                "sessionId": session_id,
                "fromTurnId": from_turn_id,
                "throughTurnId": through_turn_id,
            }),
        )
    }

    /// Start a read-only proposal session and stream attributable progress into one editable draft.
    ///
    /// # Errors
    ///
    /// Returns an error when the controller rejects the source session/span or streaming cannot
    /// begin.
    pub fn propose_evaluation(
        &self,
        session_id: Option<&str>,
        from_turn_id: Option<&str>,
        through_turn_id: Option<&str>,
    ) -> Result<ProposalStream, WorkbenchError> {
        let proposal = self.post_json(
            &format!(
                "/api/workbench/{}/evaluation-proposals",
                self.inner.workspace_id
            ),
            &json!({
                "sessionId": session_id,
                "fromTurnId": from_turn_id,
                "throughTurnId": through_turn_id,
            }),
        )?;
        let proposal_id = proposal
            .get("id")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| WorkbenchError::Malformed("proposal response has no id".to_owned()))?
            .to_owned();
        let (sender, receiver) = mpsc::channel();
        let bridge = self.clone();
        thread::Builder::new()
            .name("agent-lab-proposal-stream".to_owned())
            .spawn(move || {
                if let Err(error) = bridge.stream_evaluation_proposal(&proposal_id, &sender) {
                    let _ = sender.send(Err(WorkbenchError::Request(format!(
                        "{error}; proposal {proposal_id} remains available"
                    ))));
                }
            })
            .map_err(|error| WorkbenchError::Request(error.to_string()))?;
        Ok(ProposalStream { proposal, receiver })
    }

    /// Request proposal cancellation without waiting for the controller's terminal response.
    ///
    /// # Errors
    ///
    /// Returns an error when the detached request worker cannot be started.
    pub fn cancel_evaluation_proposal(&self, proposal_id: &str) -> Result<(), WorkbenchError> {
        self.request_json_detached(
            reqwest::Method::POST,
            &format!(
                "/api/workbench/{}/evaluation-proposals/{proposal_id}/cancel",
                self.inner.workspace_id
            ),
            Some(json!({})),
        )
        .map(|_| ())
    }

    /// Submit an optimistic edit and create a new immutable revision when material.
    ///
    /// # Errors
    ///
    /// Returns an error when the base revision is stale or the controller request fails.
    pub fn update_evaluation_draft(
        &self,
        draft_id: &str,
        value: &JsonValue,
    ) -> Result<JsonValue, WorkbenchError> {
        self.patch_json(
            &format!(
                "/api/workbench/{}/evaluation-drafts/{draft_id}",
                self.inner.workspace_id
            ),
            value,
        )
    }

    /// Start validation and stream either its projection or raw events.
    ///
    /// # Errors
    ///
    /// Returns an error when validation cannot start or its response is malformed.
    pub fn validate_evaluation_draft(
        &self,
        draft_id: &str,
        revision_id: Option<&str>,
        raw: bool,
    ) -> Result<ValidationStream, WorkbenchError> {
        let attempt = self.post_json(
            &format!(
                "/api/workbench/{}/evaluation-drafts/{draft_id}/validate",
                self.inner.workspace_id
            ),
            &json!({ "revisionId": revision_id }),
        )?;
        let validation_id = attempt
            .get("id")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| WorkbenchError::Malformed("validation response has no id".to_owned()))?
            .to_owned();
        let (sender, receiver) = mpsc::channel();
        let bridge = self.clone();
        let draft_id = draft_id.to_owned();
        thread::Builder::new()
            .name("agent-lab-validation-stream".to_owned())
            .spawn(move || {
                if let Err(error) =
                    bridge.stream_evaluation_validation(&draft_id, &validation_id, raw, &sender)
                {
                    let _ = sender.send(Err(WorkbenchError::Request(format!(
                        "{error}; validation {validation_id} remains available on draft {draft_id}"
                    ))));
                }
            })
            .map_err(|error| WorkbenchError::Request(error.to_string()))?;
        Ok(ValidationStream { attempt, receiver })
    }

    /// Retain a draft revision and promote it when that exact revision has passed.
    ///
    /// # Errors
    ///
    /// Returns an error when the controller rejects the revision or request.
    pub fn save_evaluation_draft(
        &self,
        draft_id: &str,
        revision_id: Option<&str>,
        name: Option<&str>,
    ) -> Result<JsonValue, WorkbenchError> {
        self.post_json(
            &format!(
                "/api/workbench/{}/evaluation-drafts/{draft_id}/save",
                self.inner.workspace_id
            ),
            &json!({ "revisionId": revision_id, "name": name }),
        )
    }

    /// List saved runnable evaluation definitions for this workspace.
    ///
    /// # Errors
    ///
    /// Returns an error when the controller request fails or is malformed.
    pub fn evaluation_definitions(&self) -> Result<JsonValue, WorkbenchError> {
        self.get_json(&format!(
            "/api/workbench/{}/evaluation-definitions",
            self.inner.workspace_id
        ))
    }

    /// Return one saved evaluation definition.
    ///
    /// # Errors
    ///
    /// Returns an error when the controller request fails or is malformed.
    pub fn evaluation_definition(&self, definition_id: &str) -> Result<JsonValue, WorkbenchError> {
        self.get_json(&format!(
            "/api/workbench/{}/evaluation-definitions/{definition_id}",
            self.inner.workspace_id
        ))
    }

    /// Start a paired run from an immutable evaluation definition.
    ///
    /// # Errors
    ///
    /// Returns an error when the definition, harnesses, model, or response is invalid.
    pub fn run_evaluation_definition(
        &self,
        definition_id: &str,
        harness_ids: &[String],
        model_profile_id: Option<String>,
    ) -> Result<ComparisonStream, WorkbenchError> {
        let mut body = Map::new();
        if !harness_ids.is_empty() {
            body.insert("harnessIds".to_owned(), json!(harness_ids));
        }
        if let Some(model_profile_id) = model_profile_id {
            body.insert("modelProfileId".to_owned(), json!(model_profile_id));
        }
        let evaluation = self.post_json(
            &format!(
                "/api/workbench/{}/evaluation-definitions/{definition_id}/run",
                self.inner.workspace_id
            ),
            &JsonValue::Object(body),
        )?;
        let evaluation_id = evaluation
            .get("id")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| {
                WorkbenchError::Malformed("definition run response has no id".to_owned())
            })?
            .to_owned();
        let (sender, receiver) = mpsc::channel();
        let bridge = self.clone();
        let initial = evaluation.clone();
        thread::Builder::new()
            .name("agent-lab-definition-evaluation-stream".to_owned())
            .spawn(move || {
                if sender
                    .send(Ok(milestone(
                        "comparison-created",
                        &evaluation_id,
                        &initial,
                    )))
                    .is_err()
                {
                    return;
                }
                if let Err(error) = bridge.stream_evaluation(&evaluation_id, false, &sender) {
                    let _ = sender.send(Err(WorkbenchError::Request(format!(
                        "{error}; evaluation {evaluation_id} continues and can be reopened"
                    ))));
                }
            })
            .map_err(|error| WorkbenchError::Request(error.to_string()))?;
        Ok(ComparisonStream {
            evaluation,
            receiver,
        })
    }

    /// Read one persistent agent session.
    ///
    /// # Errors
    ///
    /// Returns an error when the session cannot be loaded or decoded.
    pub fn agent_session(&self, session_id: &str) -> Result<JsonValue, WorkbenchError> {
        self.get_json(&format!(
            "/api/workbench/{}/agent-sessions/{session_id}",
            self.inner.workspace_id
        ))
    }

    /// Create and activate a harness-native session from the shared selection.
    ///
    /// # Errors
    ///
    /// Returns an error when the controller cannot start the selected session.
    pub fn start_agent_session(
        &self,
        harness_id: Option<&str>,
        model_profile_id: Option<&str>,
    ) -> Result<JsonValue, WorkbenchError> {
        self.post_json(
            &format!("/api/workbench/{}/agent-sessions", self.inner.workspace_id),
            &json!({
                "harnessId": harness_id,
                "modelProfileId": model_profile_id,
            }),
        )
    }

    /// Create an agent session without making a slow model-access preflight
    /// uninterruptible from the shell.
    ///
    /// When interrupted before the controller returns a session id, cleanup
    /// waits off-thread for the durable session response and closes that
    /// session as soon as it becomes addressable.
    ///
    /// # Errors
    ///
    /// Returns an error when startup fails or the caller interrupts it.
    pub fn start_agent_session_interruptible<F>(
        &self,
        harness_id: Option<&str>,
        model_profile_id: Option<&str>,
        mut interrupted: F,
    ) -> Result<JsonValue, WorkbenchError>
    where
        F: FnMut() -> bool,
    {
        let response = self.request_json_detached(
            reqwest::Method::POST,
            &format!("/api/workbench/{}/agent-sessions", self.inner.workspace_id),
            Some(json!({
                "harnessId": harness_id,
                "modelProfileId": model_profile_id,
            })),
        )?;
        loop {
            if interrupted() {
                let bridge = self.clone();
                thread::Builder::new()
                    .name("agent-lab-agent-start-cancel".to_owned())
                    .spawn(move || {
                        if let Ok(Ok(session)) = response.recv()
                            && let Some(session_id) = session.get("id").and_then(JsonValue::as_str)
                        {
                            let _ = bridge.close_agent_session(session_id);
                        }
                    })
                    .map_err(|error| WorkbenchError::Request(error.to_string()))?;
                return Err(WorkbenchError::Cancelled(
                    "session startup was interrupted; any durable session created by the request \
                     is being closed and remains available through `agent sessions`"
                        .to_owned(),
                ));
            }
            match response.recv_timeout(Duration::from_millis(100)) {
                Ok(result) => return result,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(WorkbenchError::Request(
                        "agent session request worker stopped".to_owned(),
                    ));
                }
            }
        }
    }

    /// Wait until one newly-created native session is ready and active.
    ///
    /// # Errors
    ///
    /// Returns an error when startup fails, activation is deferred, or the
    /// bounded startup deadline expires.
    pub fn wait_for_agent_session_ready<F, C>(
        &self,
        session_id: &str,
        on_progress: &mut F,
        interrupted: &mut C,
    ) -> Result<JsonValue, WorkbenchError>
    where
        F: FnMut(&JsonValue),
        C: FnMut() -> bool,
    {
        let deadline = Instant::now() + AGENT_SESSION_START_TIMEOUT;
        let mut ready_inactive_since = None;
        let mut latest_sequence = 0_u64;
        loop {
            if interrupted() {
                self.close_agent_session_detached(session_id);
                return Err(WorkbenchError::Cancelled(format!(
                    "agent session {session_id} startup was interrupted; it is being closed and \
                     remains available through `agent sessions`"
                )));
            }
            let detail = self.agent_session(session_id)?;
            if interrupted() {
                self.close_agent_session_detached(session_id);
                return Err(WorkbenchError::Cancelled(format!(
                    "agent session {session_id} startup was interrupted; it is being closed and \
                     remains available through `agent sessions`"
                )));
            }
            if let Some(events) = detail.get("events").and_then(JsonValue::as_array) {
                for event in events {
                    let sequence = event
                        .get("sequence")
                        .and_then(JsonValue::as_u64)
                        .unwrap_or(0);
                    if sequence <= latest_sequence {
                        continue;
                    }
                    latest_sequence = latest_sequence.max(sequence);
                    if let Some(progress) = event.get("progress") {
                        on_progress(progress);
                    }
                }
            }
            let session = detail.get("summary").cloned().ok_or_else(|| {
                WorkbenchError::Malformed(format!(
                    "new agent session {session_id} has no summary during startup"
                ))
            })?;
            let status = session
                .get("status")
                .and_then(JsonValue::as_str)
                .unwrap_or("unknown");
            let active = session.get("active").and_then(JsonValue::as_bool) == Some(true);
            if status == "ready" && active {
                return Ok(session);
            }
            if status == "ready" {
                let since = ready_inactive_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= Duration::from_secs(1) {
                    return Err(WorkbenchError::Request(format!(
                        "agent session {session_id} opened but could not become active; finish the active turn, then run `agent switch {session_id}`"
                    )));
                }
            } else {
                ready_inactive_since = None;
            }
            if matches!(status, "failed" | "closed" | "interrupted" | "closing") {
                let detail = session
                    .get("error")
                    .and_then(JsonValue::as_str)
                    .unwrap_or("session startup did not complete");
                return Err(WorkbenchError::Request(format!(
                    "agent session {session_id} entered {status}: {detail}"
                )));
            }
            if Instant::now() >= deadline {
                return Err(WorkbenchError::Request(format!(
                    "agent session {session_id} did not become ready within {} seconds",
                    AGENT_SESSION_START_TIMEOUT.as_secs()
                )));
            }
            on_progress(&JsonValue::Null);
            thread::sleep(Duration::from_millis(250));
        }
    }

    /// Make one existing ready session the active shell session.
    ///
    /// # Errors
    ///
    /// Returns an error when the session cannot be activated.
    pub fn activate_agent_session(&self, session_id: &str) -> Result<JsonValue, WorkbenchError> {
        self.post_json(
            &format!(
                "/api/workbench/{}/agent-sessions/{session_id}/activate",
                self.inner.workspace_id
            ),
            &json!({}),
        )
    }

    /// Start a turn in an existing session and stream its attributable events.
    ///
    /// # Errors
    ///
    /// Returns an error when the turn cannot be created or its event worker
    /// cannot be started.
    pub fn start_agent_turn(
        &self,
        session: JsonValue,
        prompt: &str,
        input: Option<&JsonValue>,
        raw: bool,
        include_startup: bool,
        include_progress: bool,
    ) -> Result<AgentTurnStream, WorkbenchError> {
        let session_id = session
            .get("id")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| WorkbenchError::Malformed("agent session has no id".to_owned()))?
            .to_owned();
        let turn = self.post_json(
            &format!(
                "/api/workbench/{}/agent-sessions/{session_id}/turns",
                self.inner.workspace_id
            ),
            &json!({ "prompt": prompt, "input": input }),
        )?;
        let turn_id = turn
            .get("id")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| WorkbenchError::Malformed("agent turn has no id".to_owned()))?
            .to_owned();
        let (sender, receiver) = mpsc::channel();
        let bridge = self.clone();
        thread::Builder::new()
            .name("agent-lab-agent-turn-stream".to_owned())
            .spawn(move || {
                if let Err(error) = bridge.stream_agent_turn(
                    &session_id,
                    &turn_id,
                    raw,
                    include_startup,
                    include_progress,
                    &sender,
                ) {
                    let _ = sender.send(Err(WorkbenchError::Request(format!(
                        "{error}; session {session_id} and turn {turn_id} can be reopened"
                    ))));
                }
            })
            .map_err(|error| WorkbenchError::Request(error.to_string()))?;
        Ok(AgentTurnStream {
            session,
            turn,
            receiver,
        })
    }

    /// Cancel the active turn for one workspace-owned agent session.
    ///
    /// # Errors
    ///
    /// Returns an error when the controller rejects the request or cannot be
    /// reached.
    pub fn cancel_agent_turn(&self, session_id: &str) -> Result<(), WorkbenchError> {
        self.post_json(
            &format!(
                "/api/workbench/{}/agent-sessions/{session_id}/cancel",
                self.inner.workspace_id
            ),
            &json!({}),
        )?;
        Ok(())
    }

    pub fn cancel_agent_turn_detached(&self, session_id: &str) {
        let bridge = self.clone();
        let session_id = session_id.to_owned();
        let _ = thread::Builder::new()
            .name("agent-lab-agent-cancel".to_owned())
            .spawn(move || {
                let _ = bridge.cancel_agent_turn(&session_id);
            });
    }

    /// Close a ready interactive session and return its closing state.
    ///
    /// # Errors
    ///
    /// Returns an error when the session cannot enter its closing transition.
    pub fn close_agent_session(&self, session_id: &str) -> Result<JsonValue, WorkbenchError> {
        self.post_json(
            &format!(
                "/api/workbench/{}/agent-sessions/{session_id}/close",
                self.inner.workspace_id
            ),
            &json!({}),
        )
    }

    pub fn close_agent_session_detached(&self, session_id: &str) {
        let bridge = self.clone();
        let session_id = session_id.to_owned();
        let _ = thread::Builder::new()
            .name("agent-lab-agent-close".to_owned())
            .spawn(move || {
                let _ = bridge.close_agent_session(&session_id);
            });
    }

    /// Start a comparison and optionally stream its controller events.
    ///
    /// # Errors
    ///
    /// Returns an error when the comparison cannot be created or its stream
    /// worker cannot be started.
    pub fn compare(
        &self,
        harness_ids: &[String],
        model_profile_id: Option<String>,
        raw: bool,
        stream: bool,
    ) -> Result<ComparisonStream, WorkbenchError> {
        let mut body = Map::new();
        if !harness_ids.is_empty() {
            body.insert("harnessIds".to_owned(), json!(harness_ids));
        }
        if let Some(model_profile_id) = model_profile_id {
            body.insert("modelProfileId".to_owned(), json!(model_profile_id));
        }
        let evaluation = self.post_json(
            &format!("/api/workbench/{}/compare", self.inner.workspace_id),
            &JsonValue::Object(body),
        )?;
        let evaluation_id = evaluation
            .get("id")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| WorkbenchError::Malformed("comparison response has no id".to_owned()))?
            .to_owned();
        let (sender, receiver) = mpsc::channel();
        if !stream {
            return Ok(ComparisonStream {
                evaluation,
                receiver,
            });
        }
        let bridge = self.clone();
        let initial = evaluation.clone();
        thread::Builder::new()
            .name("agent-lab-workbench-stream".to_owned())
            .spawn(move || {
                if sender
                    .send(Ok(milestone(
                        "comparison-created",
                        &evaluation_id,
                        &initial,
                    )))
                    .is_err()
                {
                    return;
                }
                if let Err(error) = bridge.stream_evaluation(&evaluation_id, raw, &sender) {
                    let _ = sender.send(Err(WorkbenchError::Request(format!(
                        "{error}; evaluation {evaluation_id} continues and can be reopened"
                    ))));
                }
            })
            .map_err(|error| WorkbenchError::Request(error.to_string()))?;
        Ok(ComparisonStream {
            evaluation,
            receiver,
        })
    }

    pub fn cancel(&self, evaluation_id: &str) {
        let bridge = self.clone();
        let evaluation_id = evaluation_id.to_owned();
        let _ = thread::Builder::new()
            .name("agent-lab-workbench-cancel".to_owned())
            .spawn(move || {
                let _ = bridge
                    .request(
                        reqwest::Method::POST,
                        &format!(
                            "/api/workbench/{}/evaluations/{evaluation_id}/cancel",
                            bridge.inner.workspace_id
                        ),
                    )
                    .timeout(Duration::from_secs(10))
                    .send();
            });
    }

    pub fn cancel_evaluation_validation(&self, draft_id: &str, validation_id: &str) {
        let bridge = self.clone();
        let draft_id = draft_id.to_owned();
        let validation_id = validation_id.to_owned();
        let _ = thread::Builder::new()
            .name("agent-lab-validation-cancel".to_owned())
            .spawn(move || {
                let _ = bridge
                    .request(
                        reqwest::Method::POST,
                        &format!(
                            "/api/workbench/{}/evaluation-drafts/{draft_id}/validations/{validation_id}/cancel",
                            bridge.inner.workspace_id
                        ),
                    )
                    .timeout(Duration::from_secs(10))
                    .send();
            });
    }

    fn stream_agent_turn(
        &self,
        session_id: &str,
        turn_id: &str,
        raw: bool,
        include_startup: bool,
        include_progress: bool,
        sender: &mpsc::Sender<Result<AgentTurnOutput, WorkbenchError>>,
    ) -> Result<(), WorkbenchError> {
        let response = self
            .request(
                reqwest::Method::GET,
                &format!(
                    "/api/workbench/{}/agent-sessions/{session_id}/events",
                    self.inner.workspace_id
                ),
            )
            .send()
            .and_then(reqwest::blocking::Response::error_for_status)
            .map_err(|error| WorkbenchError::Request(error.to_string()))?;
        for line in BufReader::new(response).lines() {
            let line = line.map_err(|error| WorkbenchError::Request(error.to_string()))?;
            let Some(data) = line.strip_prefix("data:") else {
                continue;
            };
            let event: JsonValue = serde_json::from_str(data.trim())
                .map_err(|error| WorkbenchError::Malformed(error.to_string()))?;
            let kind = event.get("type").and_then(JsonValue::as_str).unwrap_or("");
            let payload = event.get("payload").unwrap_or(&JsonValue::Null);
            let belongs_to_turn =
                payload.get("turnId").and_then(JsonValue::as_str) == Some(turn_id);
            let session_lifecycle = include_startup
                && matches!(
                    kind,
                    "agent.session.starting" | "startup.event" | "agent.session.ready"
                );
            let session_failed = kind == "agent.session.failed";
            if !belongs_to_turn && !session_lifecycle && !session_failed {
                continue;
            }
            if raw {
                if sender
                    .send(Ok(AgentTurnOutput::Raw(agent_raw_envelope(
                        session_id, turn_id, &event,
                    ))))
                    .is_err()
                {
                    return Ok(());
                }
            } else {
                if include_progress
                    && let Some(progress) = event.get("progress")
                    && sender
                        .send(Ok(AgentTurnOutput::Progress(progress.clone())))
                        .is_err()
                {
                    return Ok(());
                }
                if let Some(output) = assistant_output(&event)?
                    && sender.send(Ok(output)).is_err()
                {
                    return Ok(());
                }
            }
            if kind == "agent.turn.finished" {
                if !raw {
                    let outcome = payload
                        .get("outcome")
                        .and_then(JsonValue::as_str)
                        .unwrap_or("unknown")
                        .to_owned();
                    let _ = sender.send(Ok(AgentTurnOutput::Finished { outcome }));
                }
                return Ok(());
            }
            if session_failed {
                let detail = payload
                    .get("message")
                    .and_then(JsonValue::as_str)
                    .unwrap_or("agent session failed");
                let _ = sender.send(Err(WorkbenchError::Request(format!(
                    "{detail}; session {session_id} and turn {turn_id} can be reopened"
                ))));
                return Ok(());
            }
        }
        Err(WorkbenchError::Request(
            "agent event stream ended before the turn completed".to_owned(),
        ))
    }

    fn stream_evaluation(
        &self,
        evaluation_id: &str,
        raw: bool,
        sender: &mpsc::Sender<Result<JsonValue, WorkbenchError>>,
    ) -> Result<(), WorkbenchError> {
        let response = self
            .request(
                reqwest::Method::GET,
                &format!(
                    "/api/workbench/{}/evaluations/{evaluation_id}/events",
                    self.inner.workspace_id
                ),
            )
            .send()
            .and_then(reqwest::blocking::Response::error_for_status)
            .map_err(|error| WorkbenchError::Request(error.to_string()))?;
        let mut finished = false;
        let mut unavailable = false;
        for line in BufReader::new(response).lines() {
            let line = line.map_err(|error| WorkbenchError::Request(error.to_string()))?;
            let Some(data) = line.strip_prefix("data:") else {
                continue;
            };
            let event: JsonValue = serde_json::from_str(data.trim())
                .map_err(|error| WorkbenchError::Malformed(error.to_string()))?;
            let kind = event.get("type").and_then(JsonValue::as_str).unwrap_or("");
            if raw {
                if sender
                    .send(Ok(raw_envelope(evaluation_id, &event)))
                    .is_err()
                {
                    return Ok(());
                }
            } else if let Some(projected) = project_milestone(evaluation_id, &event)
                && sender.send(Ok(projected)).is_err()
            {
                return Ok(());
            }
            if kind == "evaluation.finished" || kind == "evaluation.unavailable" {
                finished = true;
                unavailable = kind == "evaluation.unavailable";
                break;
            }
        }
        if !finished {
            return Err(WorkbenchError::Request(
                "evaluation event stream ended before completion".to_owned(),
            ));
        }
        if !unavailable {
            let detail = self.evaluation(Some(evaluation_id))?;
            let _ = sender.send(Ok(milestone("comparison-finished", evaluation_id, &detail)));
        }
        Ok(())
    }

    fn stream_evaluation_validation(
        &self,
        draft_id: &str,
        validation_id: &str,
        raw: bool,
        sender: &mpsc::Sender<Result<JsonValue, WorkbenchError>>,
    ) -> Result<(), WorkbenchError> {
        let response = self
            .request(
                reqwest::Method::GET,
                &format!(
                    "/api/workbench/{}/evaluation-drafts/{draft_id}/events",
                    self.inner.workspace_id
                ),
            )
            .send()
            .and_then(reqwest::blocking::Response::error_for_status)
            .map_err(|error| WorkbenchError::Request(error.to_string()))?;
        for line in BufReader::new(response).lines() {
            let line = line.map_err(|error| WorkbenchError::Request(error.to_string()))?;
            let Some(data) = line.strip_prefix("data:") else {
                continue;
            };
            let event: JsonValue = serde_json::from_str(data.trim())
                .map_err(|error| WorkbenchError::Malformed(error.to_string()))?;
            let kind = event.get("type").and_then(JsonValue::as_str).unwrap_or("");
            let payload = event.get("payload").unwrap_or(&JsonValue::Null);
            if payload.get("validationId").and_then(JsonValue::as_str) != Some(validation_id)
                && payload.get("id").and_then(JsonValue::as_str) != Some(validation_id)
            {
                continue;
            }
            if raw
                && sender
                    .send(Ok(json!({
                        "type": "validation-event",
                        "draftId": draft_id,
                        "validationId": validation_id,
                        "event": event,
                    })))
                    .is_err()
            {
                return Ok(());
            }
            if kind == "evaluation-validation.finished" {
                if !raw {
                    let draft = self.evaluation_draft(draft_id)?;
                    let attempt = draft
                        .get("validations")
                        .and_then(JsonValue::as_array)
                        .and_then(|attempts| {
                            attempts.iter().find(|attempt| {
                                attempt.get("id").and_then(JsonValue::as_str) == Some(validation_id)
                            })
                        })
                        .cloned()
                        .ok_or_else(|| {
                            WorkbenchError::Malformed(format!(
                                "draft {draft_id} has no completed validation {validation_id}"
                            ))
                        })?;
                    let _ = sender.send(Ok(json!({
                        "type": "validation-attempt",
                        "attempt": attempt,
                    })));
                }
                return Ok(());
            }
        }
        Err(WorkbenchError::Request(
            "evaluation draft event stream ended before validation completion".to_owned(),
        ))
    }

    fn stream_evaluation_proposal(
        &self,
        proposal_id: &str,
        sender: &mpsc::Sender<Result<JsonValue, WorkbenchError>>,
    ) -> Result<(), WorkbenchError> {
        let response = self
            .request(
                reqwest::Method::GET,
                &format!(
                    "/api/workbench/{}/evaluation-proposals/{proposal_id}/events",
                    self.inner.workspace_id
                ),
            )
            .send()
            .and_then(reqwest::blocking::Response::error_for_status)
            .map_err(|error| WorkbenchError::Request(error.to_string()))?;
        for line in BufReader::new(response).lines() {
            let line = line.map_err(|error| WorkbenchError::Request(error.to_string()))?;
            let Some(data) = line.strip_prefix("data:") else {
                continue;
            };
            let event: JsonValue = serde_json::from_str(data.trim())
                .map_err(|error| WorkbenchError::Malformed(error.to_string()))?;
            let kind = event.get("type").and_then(JsonValue::as_str).unwrap_or("");
            let payload = event.get("payload").unwrap_or(&JsonValue::Null);
            let projected = match kind {
                "evaluation-proposal.created" => Some(json!({
                    "type": "proposal-created",
                    "proposalId": proposal_id,
                })),
                "evaluation-proposal.session.ready" => Some(json!({
                    "type": "proposal-progress",
                    "proposalId": proposal_id,
                    "phase": "ready",
                    "detail": "Proposal agent ready",
                })),
                "observation.progress" => Some(json!({
                    "type": "proposal-progress",
                    "proposalId": proposal_id,
                    "phase": payload.pointer("/event/phase"),
                    "detail": payload.pointer("/event/detail"),
                })),
                "evaluation-proposal.finished" => {
                    let proposal = self.get_json(&format!(
                        "/api/workbench/{}/evaluation-proposals/{proposal_id}",
                        self.inner.workspace_id
                    ))?;
                    let draft = proposal
                        .pointer("/summary/draftId")
                        .and_then(JsonValue::as_str)
                        .map(|draft_id| self.evaluation_draft(draft_id))
                        .transpose()?;
                    Some(json!({
                        "type": "proposal-finished",
                        "proposal": proposal,
                        "draft": draft,
                    }))
                }
                _ => None,
            };
            if let Some(projected) = projected
                && sender.send(Ok(projected)).is_err()
            {
                return Ok(());
            }
            if kind == "evaluation-proposal.finished" {
                return Ok(());
            }
        }
        Err(WorkbenchError::Request(
            "evaluation proposal event stream ended before completion".to_owned(),
        ))
    }

    fn get_json(&self, path: &str) -> Result<JsonValue, WorkbenchError> {
        self.request_json(reqwest::Method::GET, path, None)
    }

    fn post_json(&self, path: &str, body: &JsonValue) -> Result<JsonValue, WorkbenchError> {
        self.request_json(reqwest::Method::POST, path, Some(body.clone()))
    }

    fn patch_json(&self, path: &str, body: &JsonValue) -> Result<JsonValue, WorkbenchError> {
        self.request_json(reqwest::Method::PATCH, path, Some(body.clone()))
    }

    fn request_json(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<JsonValue>,
    ) -> Result<JsonValue, WorkbenchError> {
        let (reply, response) = mpsc::channel();
        self.requests
            .send(JsonRequest {
                method,
                path: path.to_owned(),
                body,
                reply,
            })
            .map_err(|_| WorkbenchError::Request("workbench bridge stopped".to_owned()))?;
        response
            .recv()
            .map_err(|_| WorkbenchError::Request("workbench bridge stopped".to_owned()))?
    }

    fn request_json_detached(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<JsonValue>,
    ) -> Result<mpsc::Receiver<Result<JsonValue, WorkbenchError>>, WorkbenchError> {
        let worker = self.inner.clone();
        let path = path.to_owned();
        let (reply, response) = mpsc::channel();
        thread::Builder::new()
            .name("agent-lab-workbench-request".to_owned())
            .spawn(move || {
                let result = execute_json_request(&worker, method, &path, body);
                let _ = reply.send(result);
            })
            .map_err(|error| WorkbenchError::Request(error.to_string()))?;
        Ok(response)
    }

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::blocking::RequestBuilder {
        self.inner
            .client
            .request(method, format!("{}{}", self.inner.origin, path))
            .bearer_auth(&self.inner.token)
    }
}

fn execute_json_request(
    worker: &WorkbenchConfig,
    method: reqwest::Method,
    path: &str,
    body: Option<JsonValue>,
) -> Result<JsonValue, WorkbenchError> {
    let mut builder = worker
        .client
        .request(method, format!("{}{}", worker.origin, path))
        .bearer_auth(&worker.token)
        .timeout(WORKBENCH_REQUEST_TIMEOUT);
    if let Some(body) = body {
        builder = builder.json(&body);
    }
    builder
        .send()
        .and_then(reqwest::blocking::Response::error_for_status)
        .map_err(|error| WorkbenchError::Request(error.to_string()))
        .and_then(|response| {
            response
                .bytes()
                .map_err(|error| WorkbenchError::Request(error.to_string()))
                .and_then(|body| {
                    if body.is_empty() {
                        Ok(JsonValue::Null)
                    } else {
                        serde_json::from_slice(&body)
                            .map_err(|error| WorkbenchError::Malformed(error.to_string()))
                    }
                })
        })
}

fn milestone(phase: &str, evaluation_id: &str, data: &JsonValue) -> JsonValue {
    json!({
        "phase": phase,
        "evaluationId": evaluation_id,
        "harness": JsonValue::Null,
        "status": data.pointer("/summary/status").or_else(|| data.get("status")),
        "runId": JsonValue::Null,
        "data": data,
    })
}

fn assistant_output(event: &JsonValue) -> Result<Option<AgentTurnOutput>, WorkbenchError> {
    let Some(event_type @ (ASSISTANT_DELTA_EVENT | ASSISTANT_COMPLETED_EVENT)) =
        event.get("type").and_then(JsonValue::as_str)
    else {
        return Ok(None);
    };
    let payload = event.get("payload").unwrap_or(&JsonValue::Null);
    let observation = payload.get("event").unwrap_or(payload);
    match TurnObservation::parse(event_type, observation)
        .map_err(|error| WorkbenchError::Malformed(error.to_string()))?
    {
        Some(TurnObservation::AssistantDelta(delta)) => Ok(Some(AgentTurnOutput::AssistantDelta {
            message_id: delta.message_id,
            text: delta.text,
        })),
        Some(TurnObservation::AssistantCompleted(completed)) => {
            Ok(Some(AgentTurnOutput::AssistantCompleted {
                message_id: completed.message_id,
                text: completed.text,
            }))
        }
        _ => Err(WorkbenchError::Malformed(
            "assistant output parsed as another observation kind".to_owned(),
        )),
    }
}

fn agent_raw_envelope(session_id: &str, turn_id: &str, event: &JsonValue) -> JsonValue {
    json!({
        "source": "agent-session",
        "sessionId": session_id,
        "turnId": turn_id,
        "sequence": event.get("sequence"),
        "timestamp": event.get("atMs"),
        "kind": event.get("type"),
        "payload": event.get("payload"),
    })
}

fn project_milestone(evaluation_id: &str, event: &JsonValue) -> Option<JsonValue> {
    let kind = event.get("type")?.as_str()?;
    let phase = match kind {
        "evaluation.status" => "comparison-running",
        "evaluation.arm.started" => "arm-started",
        "evaluation.arm.progress" => "arm-progress",
        "evaluation.arm.finished" => "arm-finished",
        "evaluation.finished" => "comparison-status",
        "evaluation.unavailable" => "comparison-unavailable",
        _ => return None,
    };
    let payload = event.get("payload").cloned().unwrap_or(JsonValue::Null);
    Some(json!({
        "phase": phase,
        "evaluationId": evaluation_id,
        "harness": payload.get("harnessId"),
        "status": payload.get("status"),
        "runId": payload.get("runId"),
        "data": payload,
    }))
}

fn raw_envelope(evaluation_id: &str, event: &JsonValue) -> JsonValue {
    let payload = event.get("payload").cloned().unwrap_or(JsonValue::Null);
    if event.get("type").and_then(JsonValue::as_str) == Some("evaluation.arm.event") {
        let run_event = &payload["event"];
        return json!({
            "source": "run",
            "evaluationId": evaluation_id,
            "harness": payload.get("harnessId"),
            "run": payload.get("runId"),
            "sequence": run_event.get("sequence"),
            "timestamp": run_event.get("atMs"),
            "kind": run_event.get("type"),
            "payload": run_event.get("payload"),
        });
    }
    json!({
        "source": "evaluation",
        "evaluationId": evaluation_id,
        "harness": payload.get("harnessId"),
        "run": payload.get("runId"),
        "sequence": event.get("sequence"),
        "timestamp": event.get("atMs"),
        "kind": event.get("type"),
        "payload": payload,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        io::{BufRead, BufReader, Read, Write},
        net::{TcpListener, TcpStream},
    };

    use super::*;

    fn read_http_request(stream: &TcpStream) -> String {
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut request_line = String::new();
        reader.read_line(&mut request_line).unwrap();
        let mut content_length = 0_usize;
        loop {
            let mut header = String::new();
            reader.read_line(&mut header).unwrap();
            if header == "\r\n" {
                break;
            }
            if let Some(length) = header
                .to_ascii_lowercase()
                .strip_prefix("content-length:")
                .and_then(|value| value.trim().parse().ok())
            {
                content_length = length;
            }
        }
        let mut body = vec![0; content_length];
        reader.read_exact(&mut body).unwrap();
        request_line
    }

    fn write_json_response(stream: &mut TcpStream, status: &str, body: &str) {
        write!(
            stream,
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
        stream.flush().unwrap();
    }

    fn write_sse_response(stream: &mut TcpStream, body: &str) {
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
        stream.flush().unwrap();
    }

    #[test]
    fn interrupted_session_creation_returns_before_a_delayed_response_and_closes_it_later() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let (closed_sender, closed_receiver) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut start, _) = listener.accept().unwrap();
            assert_eq!(
                read_http_request(&start),
                "POST /api/workbench/workspace-1/agent-sessions HTTP/1.1\r\n"
            );
            thread::sleep(Duration::from_millis(400));
            write_json_response(
                &mut start,
                "201 Created",
                r#"{"id":"agent-session-delayed","status":"starting"}"#,
            );

            let (mut close, _) = listener.accept().unwrap();
            assert_eq!(
                read_http_request(&close),
                "POST /api/workbench/workspace-1/agent-sessions/agent-session-delayed/close HTTP/1.1\r\n"
            );
            write_json_response(
                &mut close,
                "202 Accepted",
                r#"{"id":"agent-session-delayed","status":"closing"}"#,
            );
            closed_sender.send(()).unwrap();
        });
        let bridge =
            WorkbenchBridge::new(&origin, "workspace-1".to_owned(), "token".to_owned()).unwrap();
        let started = Instant::now();

        let error = bridge
            .start_agent_session_interruptible(None, None, || {
                started.elapsed() >= Duration::from_millis(25)
            })
            .unwrap_err();

        assert!(matches!(error, WorkbenchError::Cancelled(_)));
        assert!(
            started.elapsed() < Duration::from_millis(300),
            "Ctrl-C should not wait for a delayed session-creation response"
        );
        closed_receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("the eventually-created durable session should be closed");
        server.join().unwrap();
    }

    #[test]
    fn json_requests_outlive_the_controller_model_access_preflight() {
        assert!(WORKBENCH_REQUEST_TIMEOUT > Duration::from_secs(30));
    }

    #[test]
    fn raw_arm_events_use_the_stable_source_labelled_envelope() {
        let event = json!({
            "sequence": 4,
            "atMs": 20,
            "type": "evaluation.arm.event",
            "payload": {
                "harnessId": "eve",
                "runId": "run-1",
                "event": {
                    "sequence": 9,
                    "atMs": 21,
                    "type": "tool.completed",
                    "payload": { "name": "catalog list" }
                }
            }
        });

        assert_eq!(
            raw_envelope("evaluation-1", &event),
            json!({
                "source": "run",
                "evaluationId": "evaluation-1",
                "harness": "eve",
                "run": "run-1",
                "sequence": 9,
                "timestamp": 21,
                "kind": "tool.completed",
                "payload": { "name": "catalog list" }
            })
        );
    }

    #[test]
    fn raw_progress_events_remain_source_labelled_evaluation_events() {
        let event = json!({
            "sequence": 12,
            "atMs": 30,
            "type": "evaluation.arm.progress",
            "payload": {
                "harnessId": "v0",
                "runId": "run-2",
                "status": "running",
                "step": { "kind": "capability" }
            }
        });

        assert_eq!(
            raw_envelope("evaluation-2", &event),
            json!({
                "source": "evaluation",
                "evaluationId": "evaluation-2",
                "harness": "v0",
                "run": "run-2",
                "sequence": 12,
                "timestamp": 30,
                "kind": "evaluation.arm.progress",
                "payload": {
                    "harnessId": "v0",
                    "runId": "run-2",
                    "status": "running",
                    "step": { "kind": "capability" }
                }
            })
        );
    }

    #[test]
    fn assistant_output_reads_the_controller_wrapped_observation() {
        let event = json!({
            "sequence": 8,
            "atMs": 31,
            "type": "observation.assistant.delta",
            "payload": {
                "sessionId": "agent-session-1",
                "turnId": "agent-turn-1",
                "event": { "messageId": "message-1", "text": "hello" }
            }
        });

        assert_eq!(
            assistant_output(&event).unwrap(),
            Some(AgentTurnOutput::AssistantDelta {
                message_id: "message-1".to_owned(),
                text: "hello".to_owned(),
            })
        );
    }

    #[test]
    fn assistant_output_preserves_completed_message_identity() {
        let event = json!({
            "sequence": 9,
            "atMs": 32,
            "type": "observation.assistant.completed",
            "payload": {
                "sessionId": "agent-session-1",
                "turnId": "agent-turn-1",
                "event": { "messageId": "message-2", "text": "world" }
            }
        });

        assert_eq!(
            assistant_output(&event).unwrap(),
            Some(AgentTurnOutput::AssistantCompleted {
                message_id: "message-2".to_owned(),
                text: "world".to_owned(),
            })
        );
    }

    #[test]
    fn non_answer_events_do_not_enter_the_default_projection() {
        let event = json!({
            "type": "observation.capability.completed",
            "payload": { "event": { "name": "catalog.list" } }
        });

        assert_eq!(assistant_output(&event).unwrap(), None);
    }

    #[test]
    fn malformed_assistant_deltas_fail_instead_of_disappearing() {
        let event = json!({
            "type": "observation.assistant.delta",
            "payload": { "event": { "text": 42 } }
        });

        assert!(assistant_output(&event).is_err());
    }

    #[test]
    fn raw_agent_events_use_one_consistent_envelope() {
        let event = json!({
            "sequence": 8,
            "atMs": 31,
            "type": "observation.assistant.delta",
            "payload": {
                "sessionId": "agent-session-1",
                "turnId": "agent-turn-1",
                "event": { "text": "hello" }
            }
        });

        assert_eq!(
            agent_raw_envelope("agent-session-1", "agent-turn-1", &event),
            json!({
                "source": "agent-session",
                "sessionId": "agent-session-1",
                "turnId": "agent-turn-1",
                "sequence": 8,
                "timestamp": 31,
                "kind": "observation.assistant.delta",
                "payload": {
                    "sessionId": "agent-session-1",
                    "turnId": "agent-turn-1",
                    "event": { "text": "hello" }
                }
            })
        );
    }

    #[test]
    fn unavailable_evaluation_is_a_terminal_inspectable_milestone() {
        let event = json!({
            "sequence": 12,
            "atMs": 33,
            "type": "evaluation.unavailable",
            "payload": {
                "evaluationId": "evaluation-2",
                "reason": "protected-evidence",
                "message": "Evaluation evidence is no longer available."
            }
        });

        assert_eq!(
            project_milestone("evaluation-2", &event),
            Some(json!({
                "phase": "comparison-unavailable",
                "evaluationId": "evaluation-2",
                "harness": null,
                "status": null,
                "runId": null,
                "data": {
                    "evaluationId": "evaluation-2",
                    "reason": "protected-evidence",
                    "message": "Evaluation evidence is no longer available."
                }
            }))
        );
    }

    #[test]
    fn unavailable_evaluation_stream_finishes_without_fetching_removed_detail() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let server = thread::spawn(move || {
            let (mut events, _) = listener.accept().unwrap();
            assert_eq!(
                read_http_request(&events),
                "GET /api/workbench/workspace-1/evaluations/evaluation-2/events HTTP/1.1\r\n"
            );
            let event = json!({
                "sequence": 12,
                "atMs": 33,
                "type": "evaluation.unavailable",
                "payload": {
                    "evaluationId": "evaluation-2",
                    "reason": "protected-evidence",
                    "message": "Evaluation evidence is no longer available."
                }
            });
            write_sse_response(&mut events, &format!("data: {event}\n\n"));
        });
        let bridge =
            WorkbenchBridge::new(&origin, "workspace-1".to_owned(), "token".to_owned()).unwrap();
        let (sender, receiver) = mpsc::channel();

        bridge
            .stream_evaluation("evaluation-2", false, &sender)
            .unwrap();
        drop(sender);
        let output = receiver.into_iter().collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(
            output,
            vec![json!({
                "phase": "comparison-unavailable",
                "evaluationId": "evaluation-2",
                "harness": null,
                "status": null,
                "runId": null,
                "data": {
                    "evaluationId": "evaluation-2",
                    "reason": "protected-evidence",
                    "message": "Evaluation evidence is no longer available."
                }
            })]
        );
        server.join().unwrap();
    }

    #[test]
    fn proposal_stream_projects_progress_and_returns_the_editable_draft() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let server = thread::spawn(move || {
            let (mut start, _) = listener.accept().unwrap();
            assert_eq!(
                read_http_request(&start),
                "POST /api/workbench/workspace-1/evaluation-proposals HTTP/1.1\r\n"
            );
            write_json_response(
                &mut start,
                "201 Created",
                r#"{"id":"proposal-1","status":"queued"}"#,
            );

            let (mut events, _) = listener.accept().unwrap();
            assert_eq!(
                read_http_request(&events),
                "GET /api/workbench/workspace-1/evaluation-proposals/proposal-1/events HTTP/1.1\r\n"
            );
            let progress = json!({
                "sequence": 1,
                "atMs": 10,
                "type": "observation.progress",
                "payload": {
                    "event": {
                        "phase": "reasoning",
                        "detail": "Choosing a reusable task"
                    }
                }
            });
            let finished = json!({
                "sequence": 2,
                "atMs": 11,
                "type": "evaluation-proposal.finished",
                "payload": {
                    "proposalId": "proposal-1",
                    "draftId": "draft-1",
                    "status": "complete"
                }
            });
            write_sse_response(
                &mut events,
                &format!("data: {progress}\n\ndata: {finished}\n\n"),
            );

            let (mut detail, _) = listener.accept().unwrap();
            assert_eq!(
                read_http_request(&detail),
                "GET /api/workbench/workspace-1/evaluation-proposals/proposal-1 HTTP/1.1\r\n"
            );
            write_json_response(
                &mut detail,
                "200 OK",
                r#"{"summary":{"id":"proposal-1","draftId":"draft-1","status":"complete"}}"#,
            );

            let (mut draft, _) = listener.accept().unwrap();
            assert_eq!(
                read_http_request(&draft),
                "GET /api/workbench/workspace-1/evaluation-drafts/draft-1 HTTP/1.1\r\n"
            );
            write_json_response(
                &mut draft,
                "200 OK",
                r#"{"summary":{"id":"draft-1","currentRevisionId":"revision-1"},"revisions":[],"validations":[],"events":[]}"#,
            );
        });
        let bridge =
            WorkbenchBridge::new(&origin, "workspace-1".to_owned(), "token".to_owned()).unwrap();
        let stream = bridge
            .propose_evaluation(
                Some("agent-session-1"),
                Some("agent-turn-1"),
                Some("agent-turn-1"),
            )
            .unwrap();
        let output = stream
            .receiver
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(
            output[0],
            json!({
                "type": "proposal-progress",
                "proposalId": "proposal-1",
                "phase": "reasoning",
                "detail": "Choosing a reusable task",
            })
        );
        assert_eq!(output[1]["type"], "proposal-finished");
        assert_eq!(output[1]["draft"]["summary"]["id"], "draft-1");
        server.join().unwrap();
    }

    #[test]
    fn proposal_cancellation_returns_before_a_delayed_controller_response() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let server = thread::spawn(move || {
            let (mut request, _) = listener.accept().unwrap();
            assert_eq!(
                read_http_request(&request),
                "POST /api/workbench/workspace-1/evaluation-proposals/proposal-1/cancel HTTP/1.1\r\n"
            );
            thread::sleep(Duration::from_millis(300));
            write_json_response(&mut request, "200 OK", "null");
        });
        let bridge =
            WorkbenchBridge::new(&origin, "workspace-1".to_owned(), "token".to_owned()).unwrap();
        let started = Instant::now();

        bridge.cancel_evaluation_proposal("proposal-1").unwrap();

        assert!(
            started.elapsed() < Duration::from_millis(100),
            "proposal cancellation must not synchronously wait for the controller"
        );
        server.join().unwrap();
    }
}
