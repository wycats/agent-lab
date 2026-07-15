use std::{env, path::PathBuf, process::ExitCode};

use agent_lab_nushell_mcp::{McpBridge, NushellHost};

struct Source {
    namespace: String,
    executable: PathBuf,
    args: Vec<String>,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("agent-lab shell: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    let source = parse_source(&args)?;
    let mut host = NushellHost::new();
    if let Some(source) = source {
        let bridge = McpBridge::connect(source.executable, source.args)?;
        host.attach(source.namespace, bridge)?;
    }
    host.run_interactive()?;
    Ok(())
}

fn parse_source(args: &[String]) -> Result<Option<Source>, String> {
    match args {
        [] => Ok(None),
        [flag] if flag == "--fixture" => {
            let executable = env::current_exe()
                .map_err(|error| error.to_string())?
                .with_file_name(format!("agent-lab-mcp-fixture{}", env::consts::EXE_SUFFIX));
            Ok(Some(Source {
                namespace: "fixture".to_owned(),
                executable,
                args: Vec::new(),
            }))
        }
        [flag, namespace, executable, rest @ ..] if flag == "--mcp" => Ok(Some(Source {
            namespace: namespace.clone(),
            executable: executable.into(),
            args: rest.to_vec(),
        })),
        _ => Err(usage()),
    }
}

fn usage() -> String {
    "usage: agent-lab-nushell-mcp-shell [--fixture | --mcp NAMESPACE EXECUTABLE [ARG ...]]"
        .to_owned()
}
