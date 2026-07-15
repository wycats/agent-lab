//! Experimental external agent-driver process protocol.
//!
//! This crate exists to generate evidence for steel thread 0002. Its message
//! names and Rust types are not stable Agent Lab contracts.

mod evidence;
mod process;
mod protocol;

pub use evidence::{CanonicalProjection, CanonicalizationPolicy, DriverEvidenceBundle};
pub use process::{DriverProcess, DriverTranscript, ProcessError, RawDriverMessage};
pub use protocol::{
    CommandBody, ControllerCommand, DriverBody, DriverDescriptor, DriverFailureScope,
    DriverMessage, PROTOCOL_VERSION,
};
