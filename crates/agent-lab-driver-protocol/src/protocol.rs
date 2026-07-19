use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

pub const PROTOCOL_VERSION: u32 = 1;

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
