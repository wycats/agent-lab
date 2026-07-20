use std::{
    collections::{BTreeMap, HashSet},
    env,
    ffi::OsString,
    fs,
    net::Ipv4Addr,
    path::{Path, PathBuf},
    sync::Arc,
};

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
    let config = ServerConfig::new(options.assets, origin.clone()).with_models(options.models);
    let shutdown = config.clone();
    let mut driver = DriverLaunch::new(options.driver);
    driver.args = options.driver_args;
    driver.cwd = options.driver_cwd;
    driver.clear_env = true;
    let mut driver_env = BTreeMap::new();
    for name in ["PATH", "HOME", "TMPDIR", "SHELL"] {
        if let Some(value) = env::var_os(name) {
            driver_env.insert(OsString::from(name), value);
        }
    }
    if let Some(path) = &options.driver_env_file {
        for (name, value) in load_driver_env_file(path, &options.driver_env)? {
            driver_env.insert(name, value);
        }
    }
    for name in &options.driver_env {
        if let Some(value) = env::var_os(name) {
            driver_env.insert(OsString::from(name), value);
        }
    }
    driver.env.extend(driver_env);
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
    driver_env_file: Option<PathBuf>,
    models: Vec<String>,
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
        let mut driver_env_file = None;
        let mut models = Vec::new();
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
                "--driver-env-file" => {
                    driver_env_file = Some(args.next().ok_or_else(usage)?.into());
                }
                "--model" => models.push(args.next().ok_or_else(usage)?),
                _ => return Err(usage()),
            }
        }
        if models.is_empty() {
            models.push("fixture/model".to_owned());
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
            driver_env_file,
            models,
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
    "usage: agent-lab-web [--port PORT] [--assets DIRECTORY] [--scenarios DIRECTORY] [--data DIRECTORY] [--driver EXECUTABLE] [--driver-arg ARG]... [--driver-cwd DIRECTORY] [--driver-env NAME]... [--driver-env-file FILE] [--model MODEL]...".to_owned()
}

fn load_driver_env_file(
    path: &Path,
    allowlist: &[String],
) -> Result<Vec<(OsString, OsString)>, String> {
    let contents = fs::read_to_string(path).map_err(|error| {
        format!(
            "could not read driver environment file {}: {error}",
            path.display()
        )
    })?;
    let allowlist = allowlist.iter().map(String::as_str).collect::<HashSet<_>>();
    let mut values = BTreeMap::new();
    for (index, raw_line) in contents.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line);
        let Some((name, raw_value)) = line.split_once('=') else {
            continue;
        };
        let name = name.trim();
        if !allowlist.contains(name) {
            continue;
        }
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
        {
            return Err(format!(
                "invalid allowlisted name on line {} of {}",
                index + 1,
                path.display()
            ));
        }
        let value = parse_env_value(raw_value.trim()).map_err(|message| {
            format!(
                "invalid value for {name} on line {} of {}: {message}",
                index + 1,
                path.display()
            )
        })?;
        values.insert(OsString::from(name), OsString::from(value));
    }
    Ok(values.into_iter().collect())
}

fn parse_env_value(value: &str) -> Result<String, &'static str> {
    if let Some(value) = value.strip_prefix('"') {
        let value = value.strip_suffix('"').ok_or("unterminated double quote")?;
        let mut parsed = String::with_capacity(value.len());
        let mut chars = value.chars();
        while let Some(character) = chars.next() {
            if character != '\\' {
                parsed.push(character);
                continue;
            }
            let escaped = chars.next().ok_or("trailing escape")?;
            parsed.push(match escaped {
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                '\\' => '\\',
                '"' => '"',
                _ => return Err("unsupported escape"),
            });
        }
        return Ok(parsed);
    }
    if let Some(value) = value.strip_prefix('\'') {
        return value
            .strip_suffix('\'')
            .map(str::to_owned)
            .ok_or("unterminated single quote");
    }
    Ok(value.to_owned())
}

async fn shutdown_signal(config: ServerConfig) {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::warn!(%error, "failed to install Ctrl-C handler");
    }
    config.shutdown();
}

#[cfg(test)]
mod tests {
    use super::{load_driver_env_file, parse_env_value};
    use std::{ffi::OsString, fs};

    #[test]
    fn driver_env_file_reads_only_explicitly_allowlisted_names() {
        let root = std::env::temp_dir().join(format!(
            "agent-lab-env-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        fs::create_dir_all(&root).unwrap();
        let path = root.join(".env.local");
        fs::write(
            &path,
            "IGNORED=outside-boundary\nTOKEN=\"secret\\nvalue\"\nexport API_KEY='literal'\n",
        )
        .unwrap();

        let values =
            load_driver_env_file(&path, &["TOKEN".to_owned(), "API_KEY".to_owned()]).unwrap();

        assert_eq!(
            values,
            vec![
                (OsString::from("API_KEY"), OsString::from("literal")),
                (OsString::from("TOKEN"), OsString::from("secret\nvalue")),
            ]
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn env_value_errors_do_not_include_the_value() {
        assert_eq!(
            parse_env_value("\"sensitive\\q\""),
            Err("unsupported escape")
        );
    }
}
