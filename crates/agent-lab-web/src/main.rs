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
    HarnessProfile, ModelAccessProvider, RunController, RunControllerConfig, RunSessionProvider,
    ServerConfig, app_with_runs,
};
use serde::Deserialize;
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
    let controller_config = RunControllerConfig {
        scenarios_dir: options.scenarios,
        data_dir: options.data,
        driver,
    };
    let runs = if let Some(path) = options.harness_config {
        let (harnesses, model_profiles, model_access, harness_model_access) =
            load_harness_config(&path, options.driver_env_file.as_deref())?;
        RunController::new_with_harnesses_and_model_access(
            controller_config,
            harnesses,
            model_profiles,
            model_access,
            harness_model_access,
        )?
    } else {
        RunController::new(controller_config)?
    };
    let provider = Arc::new(RunSessionProvider::new(
        options.shell,
        runs.clone(),
        origin.clone(),
    ));

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
    harness_config: Option<PathBuf>,
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
        let mut harness_config = None;
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
                "--harness-config" => harness_config = Some(args.next().ok_or_else(usage)?.into()),
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
            harness_config,
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
    "usage: agent-lab-web [--port PORT] [--assets DIRECTORY] [--scenarios DIRECTORY] [--data DIRECTORY] [--driver EXECUTABLE] [--driver-arg ARG]... [--driver-cwd DIRECTORY] [--driver-env NAME]... [--driver-env-file FILE] [--model MODEL]... [--harness-config FILE]".to_owned()
}

#[derive(Debug, Deserialize)]
struct LocalHarnessConfig {
    #[serde(default)]
    model_profiles: BTreeMap<String, LocalModelProfile>,
    #[serde(default)]
    model_access: BTreeMap<String, LocalModelAccess>,
    harnesses: Vec<LocalHarness>,
}

#[derive(Debug, Deserialize)]
struct LocalModelProfile {
    display_name: String,
}

#[derive(Debug, Deserialize)]
struct LocalModelAccess {
    display_name: String,
    command: PathBuf,
    #[serde(default)]
    arguments: Vec<String>,
    cwd: Option<PathBuf>,
    #[serde(default)]
    environment_allowlist: Vec<String>,
    setup_hint: String,
}

#[derive(Debug, Deserialize)]
struct LocalHarness {
    id: String,
    display_name: String,
    command: PathBuf,
    #[serde(default)]
    arguments: Vec<String>,
    cwd: Option<PathBuf>,
    #[serde(default)]
    environment_allowlist: Vec<String>,
    model_access: Option<String>,
    models: BTreeMap<String, String>,
}

type LoadedHarnessConfig = (
    Vec<HarnessProfile>,
    BTreeMap<String, String>,
    Vec<ModelAccessProvider>,
    BTreeMap<String, String>,
);

fn load_harness_config(
    path: &Path,
    environment_file: Option<&Path>,
) -> Result<LoadedHarnessConfig, String> {
    let contents = fs::read_to_string(path)
        .map_err(|error| format!("could not read harness config {}: {error}", path.display()))?;
    let parsed: LocalHarnessConfig = toml::from_str(&contents)
        .map_err(|error| format!("invalid harness config {}: {error}", path.display()))?;
    let base = path.parent().unwrap_or_else(|| Path::new("."));
    let mut access_profiles = Vec::new();
    for (id, access) in parsed.model_access {
        validate_environment_names(&access.environment_allowlist, &format!("model access {id}"))?;
        let command = resolve_config_path(base, &access.command);
        validate_launch_paths(
            &command,
            access.cwd.as_deref(),
            base,
            &format!("model access {id}"),
        )?;
        let mut resolver = DriverLaunch::new(command);
        resolver.args = access.arguments.into_iter().map(OsString::from).collect();
        resolver.cwd = access
            .cwd
            .as_ref()
            .map(|cwd| resolve_config_path(base, cwd));
        resolver.clear_env = true;
        for name in ["PATH", "HOME", "TMPDIR", "SHELL"]
            .into_iter()
            .chain(access.environment_allowlist.iter().map(String::as_str))
        {
            if let Some(value) = env::var_os(name) {
                resolver.env.push((OsString::from(name), value));
            }
        }
        if let Some(environment_file) = environment_file {
            resolver.env.extend(load_driver_env_file(
                environment_file,
                &access.environment_allowlist,
            )?);
        }
        access_profiles.push(ModelAccessProvider {
            id,
            display_name: access.display_name,
            resolver: Some(resolver),
            environment_names: access.environment_allowlist,
            setup_hint: access.setup_hint,
        });
    }
    let mut profiles = Vec::new();
    let mut harness_model_access = BTreeMap::new();
    for harness in parsed.harnesses {
        validate_environment_names(
            &harness.environment_allowlist,
            &format!("harness {}", harness.id),
        )?;
        if let Some(provider_id) = &harness.model_access {
            harness_model_access.insert(harness.id.clone(), provider_id.clone());
        }
        let command = resolve_config_path(base, &harness.command);
        validate_launch_paths(
            &command,
            harness.cwd.as_deref(),
            base,
            &format!("harness {}", harness.id),
        )?;
        let mut launch = DriverLaunch::new(command);
        launch.args = harness.arguments.into_iter().map(OsString::from).collect();
        launch.cwd = harness
            .cwd
            .as_ref()
            .map(|cwd| resolve_config_path(base, cwd));
        launch.clear_env = true;
        for name in ["PATH", "HOME", "TMPDIR", "SHELL"]
            .into_iter()
            .chain(harness.environment_allowlist.iter().map(String::as_str))
        {
            if let Some(value) = env::var_os(name) {
                launch.env.push((OsString::from(name), value));
            }
        }
        if let Some(environment_file) = environment_file {
            launch.env.extend(load_driver_env_file(
                environment_file,
                &harness.environment_allowlist,
            )?);
        }
        profiles.push(HarnessProfile {
            id: harness.id,
            display_name: harness.display_name,
            launch,
            models: harness.models,
        });
    }
    Ok((
        profiles,
        parsed
            .model_profiles
            .into_iter()
            .map(|(id, profile)| (id, profile.display_name))
            .collect(),
        access_profiles,
        harness_model_access,
    ))
}

fn resolve_config_path(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_owned()
    } else {
        base.join(path)
    }
}

fn validate_environment_names(names: &[String], owner: &str) -> Result<(), String> {
    let mut unique = HashSet::new();
    for name in names {
        let mut bytes = name.bytes();
        let valid = bytes
            .next()
            .is_some_and(|byte| byte == b'_' || byte.is_ascii_alphabetic())
            && bytes.all(|byte| byte == b'_' || byte.is_ascii_alphanumeric());
        if !valid {
            return Err(format!(
                "{owner} contains an unsafe environment name: {name}"
            ));
        }
        if !unique.insert(name) {
            return Err(format!("{owner} repeats environment name: {name}"));
        }
    }
    Ok(())
}

fn validate_launch_paths(
    command: &Path,
    cwd: Option<&Path>,
    base: &Path,
    owner: &str,
) -> Result<(), String> {
    if !command.is_file() {
        return Err(format!(
            "{owner} executable does not exist or is not a file: {}",
            command.display()
        ));
    }
    if let Some(cwd) = cwd {
        let cwd = resolve_config_path(base, cwd);
        if !cwd.is_dir() {
            return Err(format!(
                "{owner} working directory does not exist or is not a directory: {}",
                cwd.display()
            ));
        }
    }
    Ok(())
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
        if validate_environment_names(&[name.to_owned()], "driver environment file").is_err() {
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
    use super::{load_driver_env_file, load_harness_config, parse_env_value};
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

    #[test]
    fn harness_config_rejects_unsafe_environment_names_and_missing_executables() {
        let root = std::env::temp_dir().join(format!(
            "agent-lab-harness-config-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        fs::create_dir_all(&root).unwrap();
        let executable = std::env::current_exe().unwrap();
        let config = root.join("harnesses.toml");
        fs::write(
            &config,
            format!(
                r#"
[model_profiles.fixture]
display_name = "Fixture"

[[harnesses]]
id = "fixture"
display_name = "Fixture"
command = {executable:?}
environment_allowlist = ["TOKEN-NAME"]

[harnesses.models]
fixture = "fixture/model"
"#
            ),
        )
        .unwrap();
        let error = load_harness_config(&config, None).unwrap_err();
        assert!(error.contains("unsafe environment name"));

        fs::write(
            &config,
            r#"
[model_profiles.fixture]
display_name = "Fixture"

[[harnesses]]
id = "fixture"
display_name = "Fixture"
command = "missing-driver"

[harnesses.models]
fixture = "fixture/model"
"#,
        )
        .unwrap();
        let error = load_harness_config(&config, None).unwrap_err();
        assert!(error.contains("executable does not exist"));
        fs::remove_dir_all(root).unwrap();
    }
}
