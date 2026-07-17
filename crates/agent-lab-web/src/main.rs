use std::{env, ffi::OsString, net::Ipv4Addr, path::PathBuf, sync::Arc};

use agent_lab_driver_protocol::DriverLaunch;
use agent_lab_web::{
    RunController, RunControllerConfig, RunSessionProvider, ServerConfig, app_with_runs,
};
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
    let shutdown = config.clone();
    let mut driver = DriverLaunch::new(options.driver);
    driver.args = options.driver_args;
    driver.cwd = options.driver_cwd;
    driver.clear_env = true;
    for name in ["PATH", "HOME", "TMPDIR", "SHELL"]
        .into_iter()
        .chain(options.driver_env.iter().map(String::as_str))
    {
        if let Some(value) = env::var_os(name) {
            driver.env.push((OsString::from(name), value));
        }
    }
    let runs = RunController::new(RunControllerConfig {
        scenarios_dir: options.scenarios,
        data_dir: options.data,
        driver,
    })?;
    let provider = Arc::new(RunSessionProvider::new(options.shell, runs.clone()));

    println!("Agent Lab web surface: {origin}");
    println!("Local Nushell and scenario runs; press Ctrl-C to stop.");

    axum::serve(listener, app_with_runs(config, provider, Some(runs)))
        .with_graceful_shutdown(shutdown_signal(shutdown))
        .await?;
    Ok(())
}

#[derive(Debug)]
struct Options {
    port: u16,
    assets: PathBuf,
    shell: PathBuf,
    scenarios: PathBuf,
    data: PathBuf,
    driver: PathBuf,
    driver_args: Vec<OsString>,
    driver_cwd: Option<PathBuf>,
    driver_env: Vec<String>,
}

impl Options {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut port = 0;
        let mut assets = PathBuf::from("apps/web/build");
        let shell = default_shell_path()?;
        let mut scenarios = PathBuf::from("scenarios");
        let mut data = PathBuf::from(".agent-lab/runs");
        let mut driver = default_driver_path()?;
        let mut driver_args = Vec::new();
        let mut driver_cwd = None;
        let mut driver_env = Vec::new();
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
                "--scenarios" => scenarios = args.next().ok_or_else(usage)?.into(),
                "--data" => data = args.next().ok_or_else(usage)?.into(),
                "--driver" => driver = args.next().ok_or_else(usage)?.into(),
                "--driver-arg" => driver_args.push(args.next().ok_or_else(usage)?.into()),
                "--driver-cwd" => driver_cwd = Some(args.next().ok_or_else(usage)?.into()),
                "--driver-env" => driver_env.push(args.next().ok_or_else(usage)?),
                _ => return Err(usage()),
            }
        }
        Ok(Self {
            port,
            assets,
            shell,
            scenarios,
            data,
            driver,
            driver_args,
            driver_cwd,
            driver_env,
        })
    }
}

fn default_driver_path() -> Result<PathBuf, String> {
    let executable = env::current_exe().map_err(|error| error.to_string())?;
    Ok(executable.with_file_name(format!(
        "agent-lab-driver-fixture{}",
        env::consts::EXE_SUFFIX
    )))
}

fn default_shell_path() -> Result<PathBuf, String> {
    let executable = env::current_exe().map_err(|error| error.to_string())?;
    Ok(executable.with_file_name(format!(
        "agent-lab-nushell-mcp-shell{}",
        env::consts::EXE_SUFFIX
    )))
}

fn usage() -> String {
    "usage: agent-lab-web [--port PORT] [--assets DIRECTORY] [--scenarios DIRECTORY] [--data DIRECTORY] [--driver EXECUTABLE] [--driver-arg ARG]... [--driver-cwd DIRECTORY] [--driver-env NAME]...".to_owned()
}

async fn shutdown_signal(config: ServerConfig) {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::warn!(%error, "failed to install Ctrl-C handler");
    }
    config.shutdown();
}
