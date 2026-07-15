//! Experimental embedded Nushell plus MCP steel thread.
//!
//! The types in this crate are evidence-producing probes, not stable Agent Lab
//! contracts. They intentionally keep the first Nushell and MCP integration in
//! one crate until the working path reveals boundaries worth extracting.

#![allow(
    clippy::result_large_err,
    reason = "Nushell's public command and evaluation APIs return ShellError by value"
)]

mod bridge;
mod host;
mod value;

pub use bridge::{BridgeError, LifecycleEvent, McpBridge};
pub use host::{HostError, NushellHost};
