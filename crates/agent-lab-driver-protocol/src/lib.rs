//! Experimental external agent-driver process protocol.
//!
//! This crate exists to generate evidence for steel thread 0002. Its message
//! names and Rust types are not stable Agent Lab contracts.

mod assistant_text;
mod evidence;
mod process;
mod protocol;

pub use assistant_text::{
    AssistantTextPart, AssistantTextPartKind, answer_after_leading_thinking, split_assistant_text,
};
pub use evidence::{
    CanonicalProjection, CanonicalizationPolicy, DriverEvidenceBundle, EVIDENCE_SCHEMA_VERSION,
    EvidenceError, EvidenceManifest,
};
pub use process::{
    DriverLaunch, DriverProcess, DriverTranscript, MAX_DRIVER_RECORD_BYTES,
    MAX_DRIVER_STDERR_BYTES, MAX_DRIVER_TRANSCRIPT_BYTES, ProcessError, RawDriverMessage,
};
pub use protocol::{
    ASSISTANT_COMPLETED_EVENT, ASSISTANT_DELTA_EVENT, AssistantCompletedObservation,
    AssistantDeltaObservation, CommandBody, ControllerCommand, DriverBody, DriverDescriptor,
    DriverFailureScope, DriverMessage, NATIVE_ACTION_EVENT, NativeActionObservation,
    NativeActionStatus, PROGRESS_EVENT, PROTOCOL_VERSION, ProgressObservation, ProgressPhase,
    TURN_OBSERVATIONS_FEATURE, TurnObservation, TurnObservationError, USAGE_EVENT,
    UsageObservation,
};
