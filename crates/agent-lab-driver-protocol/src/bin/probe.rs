use std::{env, error::Error, io, path::PathBuf, time::Duration};

use agent_lab_driver_protocol::{
    CanonicalizationPolicy, CommandBody, ControllerCommand, DriverBody, DriverDescriptor,
    DriverEvidenceBundle, DriverProcess, PROTOCOL_VERSION,
};
use serde_json::{Value as JsonValue, json};

const TIMEOUT: Duration = Duration::from_secs(30);
const PROBE_SESSION_ID: &str = "probe-session";
const PROBE_TURN_ID: &str = "probe-turn";

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

fn ensure_clean_exit(exit_code: Option<i32>) -> Result<(), io::Error> {
    if exit_code == Some(0) {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "driver exited unsuccessfully with code {exit_code:?}"
        )))
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
    let (descriptor, process_id) = open_probe_session(&mut driver)?;
    let (event_types, outcome, turn_evidence) = run_probe_turn(&mut driver)?;
    let exit_code = close_probe_session(&mut driver)?;
    let transcript = driver.transcript();
    let evidence_dir = env::var_os("AGENT_LAB_EVIDENCE_DIR").map(PathBuf::from);
    if let Some(directory) = &evidence_dir {
        let policy = serde_json::from_value::<CanonicalizationPolicy>(json_env(
            "AGENT_LAB_CANONICAL_POLICY_JSON",
            json!({ "name": "identity-v1", "removedObjectKeys": [] }),
        )?)?;
        DriverEvidenceBundle::new(
            env::var("AGENT_LAB_CONTROLLER_REVISION").ok(),
            descriptor.clone(),
            process_id,
            transcript.clone(),
            policy,
        )?
        .write_to_dir(directory)?;
    }

    serde_json::to_writer_pretty(
        io::stdout(),
        &json!({
            "driver": descriptor,
            "processId": process_id,
            "eventTypes": event_types,
            "outcome": outcome,
            "evidence": turn_evidence,
            "evidenceDir": evidence_dir,
            "exitCode": exit_code,
            "controllerRecordCount": transcript.controller_records.len(),
            "driverRecordCount": transcript.driver_records.len(),
            "stderrBytes": transcript.driver_stderr.len(),
        }),
    )?;
    println!();

    Ok(())
}

fn open_probe_session(
    driver: &mut DriverProcess,
) -> Result<(DriverDescriptor, u32), Box<dyn Error>> {
    let descriptor = loop {
        match driver.receive(TIMEOUT)?.parsed.body {
            DriverBody::StartupEvent { .. } => {}
            DriverBody::Ready { driver } => break driver,
            body => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("expected startup.event or driver.ready, received {body:?}"),
                )
                .into());
            }
        }
    };

    driver.send(&command(
        "probe-open",
        CommandBody::OpenSession {
            session_id: PROBE_SESSION_ID.to_owned(),
            config: json_env("AGENT_LAB_DRIVER_CONFIG_JSON", json!({}))?,
            limits: json_env("AGENT_LAB_DRIVER_LIMITS_JSON", json!({}))?,
        },
    ))?;
    let (session_id, process_id) = loop {
        match driver.receive(TIMEOUT)?.parsed.body {
            DriverBody::StartupEvent { .. } => {}
            DriverBody::SessionOpened {
                session_id,
                process_id,
            } => break (session_id, process_id),
            body => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("expected startup.event or session.opened, received {body:?}"),
                )
                .into());
            }
        }
    };
    if session_id != PROBE_SESSION_ID {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("opened unexpected session {session_id}"),
        )
        .into());
    }
    if process_id != driver.process_id() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "driver-reported process ID does not match the child process",
        )
        .into());
    }
    Ok((descriptor, process_id))
}

fn run_probe_turn(
    driver: &mut DriverProcess,
) -> Result<(Vec<String>, String, JsonValue), Box<dyn Error>> {
    driver.send(&command(
        "probe-turn",
        CommandBody::StartTurn {
            session_id: PROBE_SESSION_ID.to_owned(),
            turn_id: PROBE_TURN_ID.to_owned(),
            task: json_env("AGENT_LAB_DRIVER_TASK_JSON", json!({}))?,
            capability_sources: json_env("AGENT_LAB_CAPABILITY_SOURCES_JSON", json!([]))?,
        },
    ))?;

    let mut event_types = Vec::new();
    let (outcome, evidence) = loop {
        let message = driver.receive(TIMEOUT)?;
        match message.parsed.body {
            DriverBody::StartupEvent { .. } => {}
            DriverBody::TurnEvent {
                session_id,
                turn_id,
                event_type,
                ..
            } => {
                ensure_turn_identity(&session_id, &turn_id)?;
                event_types.push(event_type);
            }
            DriverBody::TurnFinished {
                session_id,
                turn_id,
                outcome,
                evidence,
            } => {
                ensure_turn_identity(&session_id, &turn_id)?;
                break (outcome, evidence);
            }
            DriverBody::Failed { code, message, .. } => {
                return Err(io::Error::other(format!("driver failed: {code}: {message}")).into());
            }
            body => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unexpected driver message during turn: {body:?}"),
                )
                .into());
            }
        }
    };
    Ok((event_types, outcome, evidence))
}

fn close_probe_session(driver: &mut DriverProcess) -> Result<Option<i32>, Box<dyn Error>> {
    driver.send(&command(
        "probe-close",
        CommandBody::CloseSession {
            session_id: PROBE_SESSION_ID.to_owned(),
        },
    ))?;
    let closed = driver.receive(TIMEOUT)?;
    if !matches!(
        closed.parsed.body,
        DriverBody::SessionClosed { ref session_id } if session_id == PROBE_SESSION_ID
    ) {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "expected session.closed").into());
    }
    let exit_code = driver.wait_for_exit(TIMEOUT)?;
    ensure_clean_exit(exit_code)?;
    Ok(exit_code)
}

fn ensure_turn_identity(session_id: &str, turn_id: &str) -> Result<(), io::Error> {
    if session_id == PROBE_SESSION_ID && turn_id == PROBE_TURN_ID {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("received event for unexpected session/turn {session_id}/{turn_id}"),
        ))
    }
}
