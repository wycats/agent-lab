//! Experimental external agent-driver process protocol.
//!
//! This crate exists to generate evidence for steel thread 0002. Its message
//! names and Rust types are not stable Agent Lab contracts.

mod evidence;
mod process;
mod protocol;

pub use evidence::{
    CanonicalProjection, CanonicalizationPolicy, DriverEvidenceBundle, EVIDENCE_SCHEMA_VERSION,
    EvidenceError, EvidenceManifest,
};
pub use process::{DriverLaunch, DriverProcess, DriverTranscript, ProcessError, RawDriverMessage};
pub use protocol::{
    CommandBody, ControllerCommand, DriverBody, DriverDescriptor, DriverFailureScope,
    DriverMessage, PROTOCOL_VERSION,
};
