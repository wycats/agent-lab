use std::{env, path::PathBuf, process::ExitCode};

use agent_lab_nushell_mcp::{McpBridge, NushellHost};

struct Source {
    namespace: String,
    transport: SourceTransport,
}

enum SourceTransport {
    Child {
        executable: PathBuf,
        args: Vec<String>,
    },
    Http {
        url: String,
        token: String,
    },
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
    let sources = parse_sources(&args)?;
    let mut host = NushellHost::new();
    for source in sources {
        let bridge = match source.transport {
            SourceTransport::Child { executable, args } => McpBridge::connect(executable, args)?,
            SourceTransport::Http { url, token } => McpBridge::connect_http(url, token)?,
        };
        host.attach(source.namespace, bridge)?;
    }
    host.run_interactive()?;
    Ok(())
}

fn parse_sources(args: &[String]) -> Result<Vec<Source>, String> {
    match args {
        [] => Ok(Vec::new()),
        [flag] if flag == "--fixture" => {
            let executable = env::current_exe()
                .map_err(|error| error.to_string())?
                .with_file_name(format!("agent-lab-mcp-fixture{}", env::consts::EXE_SUFFIX));
            Ok(vec![Source {
                namespace: "fixture".to_owned(),
                transport: SourceTransport::Child {
                    executable,
                    args: Vec::new(),
                },
            }])
        }
        [flag, namespace, executable, rest @ ..] if flag == "--mcp" => Ok(vec![Source {
            namespace: namespace.clone(),
            transport: SourceTransport::Child {
                executable: executable.into(),
                args: rest.to_vec(),
            },
        }]),
        _ => parse_http_sources(args),
    }
}

fn parse_http_sources(args: &[String]) -> Result<Vec<Source>, String> {
    if args.is_empty() || !args.len().is_multiple_of(4) {
        return Err(usage());
    }
    args.chunks_exact(4)
        .map(|chunk| match chunk {
            [flag, namespace, url, token_env] if flag == "--mcp-http" => Ok(Source {
                namespace: namespace.clone(),
                transport: SourceTransport::Http {
                    url: url.clone(),
                    token: env::var(token_env).map_err(|_| {
                        format!("{token_env} is required for --mcp-http {namespace}")
                    })?,
                },
            }),
            _ => Err(usage()),
        })
        .collect()
}

fn usage() -> String {
    "usage: agent-lab-nushell-mcp-shell [--fixture | --mcp NAMESPACE EXECUTABLE [ARG ...] | [--mcp-http NAMESPACE URL TOKEN_ENV]...]".to_owned()
}
