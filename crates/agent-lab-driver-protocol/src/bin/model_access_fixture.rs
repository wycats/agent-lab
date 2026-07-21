use std::process::ExitCode;

use serde_json::json;

fn main() -> ExitCode {
    let mode = std::env::args().nth(1);
    let response = match mode.as_deref() {
        Some("probe") => json!({
            "status": "ready",
            "source": "deterministic fixture",
        }),
        Some("resolve") => json!({
            "status": "ready",
            "source": "deterministic fixture",
            "environment": {
                "AGENT_LAB_FIXTURE_MODEL_TOKEN": "fixture-model-secret",
            },
        }),
        _ => {
            eprintln!("usage: agent-lab-model-access-fixture <probe|resolve>");
            return ExitCode::from(2);
        }
    };
    println!("{response}");
    ExitCode::SUCCESS
}
