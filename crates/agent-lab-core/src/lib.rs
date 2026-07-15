//! Core contracts for Agent Lab.
//!
//! The bootstrap intentionally contains no runtime abstraction. The first
//! steel thread will introduce only the contracts required by an embedded
//! Nushell session operating a modern MCP connection.

/// The public name of the laboratory.
pub const NAME: &str = "agent-lab";

#[cfg(test)]
mod tests {
    use super::NAME;

    #[test]
    fn identity_is_stable() {
        assert_eq!(NAME, "agent-lab");
    }
}
