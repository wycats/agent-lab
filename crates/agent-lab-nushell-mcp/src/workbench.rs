use std::{
    io::{BufRead, BufReader},
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};

use reqwest::blocking::Client;
use serde_json::{Map, Value as JsonValue, json};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum WorkbenchError {
    #[error("workbench request failed: {0}")]
    Request(String),
    #[error("workbench response was malformed: {0}")]
    Malformed(String),
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
                    let mut builder = worker
                        .client
                        .request(request.method, format!("{}{}", worker.origin, request.path))
                        .bearer_auth(&worker.token)
                        .timeout(Duration::from_secs(10));
                    if let Some(body) = request.body {
                        builder = builder.json(&body);
                    }
                    let result = builder
                        .send()
                        .and_then(reqwest::blocking::Response::error_for_status)
                        .map_err(|error| WorkbenchError::Request(error.to_string()))
                        .and_then(|response| {
                            response
                                .json()
                                .map_err(|error| WorkbenchError::Malformed(error.to_string()))
                        });
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
            if kind == "evaluation.finished" {
                finished = true;
                break;
            }
        }
        if !finished {
            return Err(WorkbenchError::Request(
                "evaluation event stream ended before completion".to_owned(),
            ));
        }
        let detail = self.evaluation(Some(evaluation_id))?;
        let _ = sender.send(Ok(milestone("comparison-finished", evaluation_id, &detail)));
        Ok(())
    }

    fn get_json(&self, path: &str) -> Result<JsonValue, WorkbenchError> {
        self.request_json(reqwest::Method::GET, path, None)
    }

    fn post_json(&self, path: &str, body: &JsonValue) -> Result<JsonValue, WorkbenchError> {
        self.request_json(reqwest::Method::POST, path, Some(body.clone()))
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

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::blocking::RequestBuilder {
        self.inner
            .client
            .request(method, format!("{}{}", self.inner.origin, path))
            .bearer_auth(&self.inner.token)
    }
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

fn project_milestone(evaluation_id: &str, event: &JsonValue) -> Option<JsonValue> {
    let kind = event.get("type")?.as_str()?;
    let phase = match kind {
        "evaluation.status" => "comparison-running",
        "evaluation.arm.started" => "arm-started",
        "evaluation.arm.progress" => "arm-progress",
        "evaluation.arm.finished" => "arm-finished",
        "evaluation.finished" => "comparison-status",
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
    use super::*;

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
}
