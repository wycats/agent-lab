use std::{env, net::Ipv4Addr, path::PathBuf, sync::Arc};

use agent_lab_web::{FixtureSessionProvider, ServerConfig, app};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let options = Options::parse(env::args().skip(1))?;
    if !options.assets.join("index.html").is_file() {
        return Err(format!(
            "web assets not found at {}; run `pnpm web:build` first",
            options.assets.display()
        )
        .into());
    }
    if !options.shell.is_file() {
        return Err(format!(
            "visual shell not found at {}; build the agent-lab-nushell-mcp binaries first",
            options.shell.display()
        )
        .into());
    }
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, options.port)).await?;
    let address = listener.local_addr()?;
    let origin = format!("http://{address}");
    let config = ServerConfig::new(options.assets, origin.clone());
    let provider = Arc::new(FixtureSessionProvider::new(
        options.shell,
        env::current_dir()?,
    ));

    println!("Agent Lab web surface: {origin}");
    println!("Local Nushell + fixture MCP sessions; press Ctrl-C to stop.");

    axum::serve(listener, app(config, provider))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

#[derive(Debug)]
struct Options {
    port: u16,
    assets: PathBuf,
    shell: PathBuf,
}

impl Options {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut port = 0;
        let mut assets = PathBuf::from("apps/web/build");
        let shell = default_shell_path()?;
        let mut args = args;
        while let Some(argument) = args.next() {
            match argument.as_str() {
                "--port" => {
                    port = args
                        .next()
                        .ok_or_else(usage)?
                        .parse()
                        .map_err(|_| usage())?;
                }
                "--assets" => assets = args.next().ok_or_else(usage)?.into(),
                _ => return Err(usage()),
            }
        }
        Ok(Self {
            port,
            assets,
            shell,
        })
    }
}

fn default_shell_path() -> Result<PathBuf, String> {
    let executable = env::current_exe().map_err(|error| error.to_string())?;
    Ok(executable.with_file_name(format!(
        "agent-lab-nushell-mcp-shell{}",
        env::consts::EXE_SUFFIX
    )))
}

fn usage() -> String {
    "usage: agent-lab-web [--port PORT] [--assets DIRECTORY]".to_owned()
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::warn!(%error, "failed to install Ctrl-C handler");
    }
}
