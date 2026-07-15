use std::{env, error::Error, io, time::Duration};

use agent_lab_driver_protocol::{
    CommandBody, ControllerCommand, DriverBody, DriverProcess, PROTOCOL_VERSION,
};
use serde_json::{Value as JsonValue, json};

const TIMEOUT: Duration = Duration::from_secs(30);

fn command(message_id: &str, body: CommandBody) -> ControllerCommand {
    ControllerCommand {
        protocol_version: PROTOCOL_VERSION,
        message_id: message_id.to_owned(),
        body,
    }
}

fn json_env(name: &str, default: JsonValue) -> Result<JsonValue, Box<dyn Error>> {
    match env::var(name) {
        Ok(value) => Ok(serde_json::from_str(&value)?),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args_os().skip(1);
    let executable = args.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: agent-lab-driver-probe <executable> [args...]",
        )
    })?;
    let mut driver = DriverProcess::spawn(executable, args)?;

    let ready = driver.receive(TIMEOUT)?;
    let DriverBody::Ready { driver: descriptor } = ready.parsed.body else {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "expected driver.ready").into());
    };

    driver.send(&command(
        "probe-open",
        CommandBody::OpenSession {
            session_id: "probe-session".to_owned(),
            config: json_env("AGENT_LAB_DRIVER_CONFIG_JSON", json!({}))?,
            limits: json_env("AGENT_LAB_DRIVER_LIMITS_JSON", json!({}))?,
        },
    ))?;
    let opened = driver.receive(TIMEOUT)?;
    let DriverBody::SessionOpened { process_id, .. } = opened.parsed.body else {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "expected session.opened").into());
    };
    if process_id != driver.process_id() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "driver-reported process ID does not match the child process",
        )
        .into());
    }

    driver.send(&command(
        "probe-turn",
        CommandBody::StartTurn {
            session_id: "probe-session".to_owned(),
            turn_id: "probe-turn".to_owned(),
            task: json_env("AGENT_LAB_DRIVER_TASK_JSON", json!({}))?,
            capability_sources: json_env("AGENT_LAB_CAPABILITY_SOURCES_JSON", json!([]))?,
        },
    ))?;

    let mut event_types = Vec::new();
    let (outcome, evidence) = loop {
        let message = driver.receive(TIMEOUT)?;
        match message.parsed.body {
            DriverBody::TurnEvent { event_type, .. } => event_types.push(event_type),
            DriverBody::TurnFinished {
                outcome, evidence, ..
            } => break (outcome, evidence),
            DriverBody::Failed { code, message, .. } => {
                return Err(io::Error::other(format!("driver failed: {code}: {message}")).into());
            }
            _ => {}
        }
    };

    driver.send(&command(
        "probe-close",
        CommandBody::CloseSession {
            session_id: "probe-session".to_owned(),
        },
    ))?;
    let closed = driver.receive(TIMEOUT)?;
    if !matches!(closed.parsed.body, DriverBody::SessionClosed { .. }) {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "expected session.closed").into());
    }
    let exit_code = driver.wait_for_exit(TIMEOUT)?;
    let transcript = driver.transcript();

    serde_json::to_writer_pretty(
        io::stdout(),
        &json!({
            "driver": descriptor,
            "processId": process_id,
            "eventTypes": event_types,
            "outcome": outcome,
            "evidence": evidence,
            "exitCode": exit_code,
            "controllerRecordCount": transcript.controller_records.len(),
            "driverRecordCount": transcript.driver_records.len(),
            "stderrBytes": transcript.driver_stderr.len(),
        }),
    )?;
    println!();

    Ok(())
}
