use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use thiserror::Error;

pub const PROTOCOL_VERSION: u32 = 1;
pub const TURN_OBSERVATIONS_FEATURE: &str = "turn-observations-v1";
pub const ASSISTANT_DELTA_EVENT: &str = "observation.assistant.delta";
pub const ASSISTANT_COMPLETED_EVENT: &str = "observation.assistant.completed";
pub const NATIVE_ACTION_EVENT: &str = "observation.native-action";
pub const PROGRESS_EVENT: &str = "observation.progress";
pub const USAGE_EVENT: &str = "observation.usage";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ControllerCommand {
    pub protocol_version: u32,
    pub message_id: String,
    #[serde(flatten)]
    pub body: CommandBody,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum CommandBody {
    #[serde(rename = "session.open", rename_all = "camelCase")]
    OpenSession {
        session_id: String,
        config: JsonValue,
        limits: JsonValue,
    },
    #[serde(rename = "turn.start", rename_all = "camelCase")]
    StartTurn {
        session_id: String,
        turn_id: String,
        task: JsonValue,
        capability_sources: JsonValue,
    },
    #[serde(rename = "turn.abort", rename_all = "camelCase")]
    AbortTurn {
        session_id: String,
        turn_id: String,
        reason: Option<String>,
    },
    #[serde(rename = "session.close", rename_all = "camelCase")]
    CloseSession { session_id: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DriverMessage {
    pub protocol_version: u32,
    pub sequence: u64,
    pub caused_by: Option<String>,
    #[serde(flatten)]
    pub body: DriverBody,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DriverBody {
    #[serde(rename = "startup.event", rename_all = "camelCase")]
    StartupEvent {
        phase: String,
        status: String,
        detail: Option<String>,
    },
    #[serde(rename = "driver.ready")]
    Ready { driver: DriverDescriptor },
    #[serde(rename = "session.opened", rename_all = "camelCase")]
    SessionOpened { session_id: String, process_id: u32 },
    #[serde(rename = "turn.event", rename_all = "camelCase")]
    TurnEvent {
        session_id: String,
        turn_id: String,
        event_type: String,
        payload: JsonValue,
    },
    #[serde(rename = "turn.finished", rename_all = "camelCase")]
    TurnFinished {
        session_id: String,
        turn_id: String,
        outcome: String,
        evidence: JsonValue,
    },
    #[serde(rename = "session.closed", rename_all = "camelCase")]
    SessionClosed { session_id: String },
    #[serde(rename = "driver.failed", rename_all = "camelCase")]
    Failed {
        scope: DriverFailureScope,
        session_id: Option<String>,
        turn_id: Option<String>,
        code: String,
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DriverDescriptor {
    pub name: String,
    pub version: String,
    pub revision: Option<String>,
    pub features: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DriverFailureScope {
    Driver,
    Protocol,
    Session,
    Turn,
}

/// A portable, driver-authored projection of one native turn observation.
///
/// Drivers that advertise [`TURN_OBSERVATIONS_FEATURE`] may emit these values
/// through the existing opaque `turn.event` message. Native events remain
/// valid and should be retained alongside this projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnObservation {
    AssistantDelta(AssistantDeltaObservation),
    AssistantCompleted(AssistantCompletedObservation),
    NativeAction(NativeActionObservation),
    Progress(ProgressObservation),
    Usage(UsageObservation),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AssistantDeltaObservation {
    pub message_id: String,
    pub text: String,
}

/// The authoritative complete visible assistant text for one message.
///
/// Consumers use this value for durable presentation and replay. When a driver
/// also streams deltas for the same message, their concatenation must equal
/// this text so terminal and structured consumers observe one answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AssistantCompletedObservation {
    pub message_id: String,
    pub text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NativeActionStatus {
    Started,
    Completed,
    Failed,
    Cancelled,
}

impl NativeActionStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NativeActionObservation {
    pub action_id: String,
    pub name: String,
    pub status: NativeActionStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

/// A coarse, display-safe phase supplied by the harness.
///
/// The detail is intended to make observable work legible. Harnesses retain
/// their native events alongside this portable projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProgressObservation {
    pub phase: ProgressPhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProgressPhase {
    Starting,
    Preparing,
    Reasoning,
    Responding,
    Acting,
    Waiting,
    Finalizing,
}

impl ProgressPhase {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Preparing => "preparing",
            Self::Reasoning => "reasoning",
            Self::Responding => "responding",
            Self::Acting => "acting",
            Self::Waiting => "waiting",
            Self::Finalizing => "finalizing",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UsageObservation {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_read_input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_creation_input_tokens: Option<u64>,
}

#[derive(Debug, Error)]
pub enum TurnObservationError {
    #[error("malformed {event_type} payload: {source}")]
    Malformed {
        event_type: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("invalid {event_type} payload: {message}")]
    Invalid { event_type: String, message: String },
}

impl TurnObservation {
    #[must_use]
    pub const fn event_type(&self) -> &'static str {
        match self {
            Self::AssistantDelta(_) => ASSISTANT_DELTA_EVENT,
            Self::AssistantCompleted(_) => ASSISTANT_COMPLETED_EVENT,
            Self::NativeAction(_) => NATIVE_ACTION_EVENT,
            Self::Progress(_) => PROGRESS_EVENT,
            Self::Usage(_) => USAGE_EVENT,
        }
    }

    /// Encode this observation as the payload of an existing `turn.event`.
    #[must_use]
    pub fn payload(&self) -> JsonValue {
        match self {
            Self::AssistantDelta(value) => {
                json!({ "messageId": value.message_id, "text": value.text })
            }
            Self::AssistantCompleted(value) => {
                json!({ "messageId": value.message_id, "text": value.text })
            }
            Self::NativeAction(value) => {
                let mut payload = JsonMap::from_iter([
                    ("actionId".to_owned(), json!(value.action_id)),
                    ("name".to_owned(), json!(value.name)),
                    ("status".to_owned(), json!(value.status.as_str())),
                ]);
                if let Some(summary) = &value.summary {
                    payload.insert("summary".to_owned(), json!(summary));
                }
                JsonValue::Object(payload)
            }
            Self::Progress(value) => {
                let mut payload =
                    JsonMap::from_iter([("phase".to_owned(), json!(value.phase.as_str()))]);
                if let Some(detail) = &value.detail {
                    payload.insert("detail".to_owned(), json!(detail));
                }
                if let Some(source) = &value.source {
                    payload.insert("source".to_owned(), json!(source));
                }
                JsonValue::Object(payload)
            }
            Self::Usage(value) => {
                let mut payload = JsonMap::new();
                insert_metric(&mut payload, "inputTokens", value.input_tokens);
                insert_metric(&mut payload, "outputTokens", value.output_tokens);
                insert_metric(&mut payload, "totalTokens", value.total_tokens);
                insert_metric(
                    &mut payload,
                    "cacheReadInputTokens",
                    value.cache_read_input_tokens,
                );
                insert_metric(
                    &mut payload,
                    "cacheCreationInputTokens",
                    value.cache_creation_input_tokens,
                );
                JsonValue::Object(payload)
            }
        }
    }

    /// Encode this observation through the existing opaque event wire shape.
    #[must_use]
    pub fn into_driver_body(
        self,
        session_id: impl Into<String>,
        turn_id: impl Into<String>,
    ) -> DriverBody {
        DriverBody::TurnEvent {
            session_id: session_id.into(),
            turn_id: turn_id.into(),
            event_type: self.event_type().to_owned(),
            payload: self.payload(),
        }
    }

    /// Parse a recognized portable event while leaving every other event
    /// opaque and compatible with protocol version 1.
    ///
    /// # Errors
    ///
    /// Returns an error when a recognized event has a malformed or invalid
    /// payload. Unknown event types return `Ok(None)`.
    pub fn parse(
        event_type: &str,
        payload: &JsonValue,
    ) -> Result<Option<Self>, TurnObservationError> {
        let observation = match event_type {
            ASSISTANT_DELTA_EVENT => Self::AssistantDelta(parse_payload(event_type, payload)?),
            ASSISTANT_COMPLETED_EVENT => {
                Self::AssistantCompleted(parse_payload(event_type, payload)?)
            }
            NATIVE_ACTION_EVENT => Self::NativeAction(parse_payload(event_type, payload)?),
            PROGRESS_EVENT => Self::Progress(parse_payload(event_type, payload)?),
            USAGE_EVENT => Self::Usage(parse_payload(event_type, payload)?),
            _ => return Ok(None),
        };
        observation.validate()?;
        Ok(Some(observation))
    }

    fn validate(&self) -> Result<(), TurnObservationError> {
        match self {
            Self::AssistantDelta(value) => {
                require_non_empty(self.event_type(), "messageId", &value.message_id)?;
                require_non_empty(self.event_type(), "text", &value.text)
            }
            Self::AssistantCompleted(value) => {
                require_non_empty(self.event_type(), "messageId", &value.message_id)?;
                require_non_empty(self.event_type(), "text", &value.text)
            }
            Self::NativeAction(value) => {
                require_non_empty(self.event_type(), "actionId", &value.action_id)?;
                require_non_empty(self.event_type(), "name", &value.name)?;
                if let Some(summary) = &value.summary {
                    require_non_empty(self.event_type(), "summary", summary)?;
                }
                Ok(())
            }
            Self::Progress(value) => {
                if let Some(detail) = &value.detail {
                    require_non_empty(self.event_type(), "detail", detail)?;
                }
                if let Some(source) = &value.source {
                    require_non_empty(self.event_type(), "source", source)?;
                }
                Ok(())
            }
            Self::Usage(value) => {
                if [
                    value.input_tokens,
                    value.output_tokens,
                    value.total_tokens,
                    value.cache_read_input_tokens,
                    value.cache_creation_input_tokens,
                ]
                .iter()
                .all(Option::is_none)
                {
                    return Err(TurnObservationError::Invalid {
                        event_type: self.event_type().to_owned(),
                        message: "at least one token metric is required".to_owned(),
                    });
                }
                Ok(())
            }
        }
    }
}

fn insert_metric(payload: &mut JsonMap<String, JsonValue>, name: &str, value: Option<u64>) {
    if let Some(value) = value {
        payload.insert(name.to_owned(), JsonValue::from(value));
    }
}

fn parse_payload<T: for<'de> Deserialize<'de>>(
    event_type: &str,
    payload: &JsonValue,
) -> Result<T, TurnObservationError> {
    serde_json::from_value(payload.clone()).map_err(|source| TurnObservationError::Malformed {
        event_type: event_type.to_owned(),
        source,
    })
}

fn require_non_empty(
    event_type: &str,
    field: &str,
    value: &str,
) -> Result<(), TurnObservationError> {
    if value.trim().is_empty() {
        Err(TurnObservationError::Invalid {
            event_type: event_type.to_owned(),
            message: format!("{field} must not be empty"),
        })
    } else {
        Ok(())
    }
}
