use std::io::{self, BufRead, BufWriter, Write};

use agent_lab_driver_protocol::{
    CommandBody, ControllerCommand, DriverBody, DriverDescriptor, DriverFailureScope,
    DriverMessage, PROTOCOL_VERSION,
};
use serde_json::{Value as JsonValue, json};

struct Fixture {
    sequence: u64,
    caused_by: Option<String>,
    session_id: Option<String>,
    active_turn: Option<String>,
}

impl Fixture {
    fn new() -> Self {
        Self {
            sequence: 0,
            caused_by: None,
            session_id: None,
            active_turn: None,
        }
    }

    fn emit(&mut self, output: &mut impl Write, body: DriverBody) -> io::Result<()> {
        self.sequence += 1;
        write_message(
            output,
            &DriverMessage {
                protocol_version: PROTOCOL_VERSION,
                sequence: self.sequence,
                caused_by: self.caused_by.clone(),
                body,
            },
        )
    }

    fn fail(
        &mut self,
        output: &mut impl Write,
        scope: DriverFailureScope,
        turn_id: Option<String>,
        code: &str,
        message: &str,
    ) -> io::Result<()> {
        self.emit(
            output,
            DriverBody::Failed {
                scope,
                session_id: self.session_id.clone(),
                turn_id,
                code: code.to_owned(),
                message: message.to_owned(),
            },
        )
    }

    fn handle(&mut self, output: &mut impl Write, body: CommandBody) -> io::Result<bool> {
        match body {
            CommandBody::OpenSession { session_id, .. } => {
                self.open_session(output, session_id)?;
                Ok(false)
            }
            CommandBody::StartTurn {
                session_id,
                turn_id,
                task,
                capability_sources,
            } => {
                self.start_turn(output, session_id, turn_id, &task, capability_sources)?;
                Ok(false)
            }
            CommandBody::AbortTurn {
                session_id,
                turn_id,
                reason,
            } => {
                self.abort_turn(output, session_id, turn_id, reason.as_ref())?;
                Ok(false)
            }
            CommandBody::CloseSession { session_id } => self.close_session(output, session_id),
        }
    }

    fn open_session(&mut self, output: &mut impl Write, session_id: String) -> io::Result<()> {
        if self.session_id.is_some() {
            return self.fail(
                output,
                DriverFailureScope::Session,
                None,
                "session-already-open",
                "fixture supports one session",
            );
        }
        self.session_id = Some(session_id.clone());
        self.emit(
            output,
            DriverBody::SessionOpened {
                session_id,
                process_id: std::process::id(),
            },
        )
    }

    fn start_turn(
        &mut self,
        output: &mut impl Write,
        session_id: String,
        turn_id: String,
        task: &JsonValue,
        capability_sources: JsonValue,
    ) -> io::Result<()> {
        if self.session_id.as_deref() != Some(&session_id) {
            return self.fail(
                output,
                DriverFailureScope::Session,
                Some(turn_id),
                "unknown-session",
                "turn session does not match the open session",
            );
        }
        match task.get("mode").and_then(JsonValue::as_str) {
            Some("malformed-output") => {
                output.write_all(b"{not-json}\n")?;
                output.flush()
            }
            Some("unsupported-version") => {
                self.sequence += 1;
                write_message(
                    output,
                    &DriverMessage {
                        protocol_version: PROTOCOL_VERSION + 1,
                        sequence: self.sequence,
                        caused_by: self.caused_by.clone(),
                        body: fixture_event(session_id, turn_id, "fixture.bad-version"),
                    },
                )
            }
            Some("repeat-sequence") => write_message(
                output,
                &DriverMessage {
                    protocol_version: PROTOCOL_VERSION,
                    sequence: self.sequence,
                    caused_by: self.caused_by.clone(),
                    body: fixture_event(session_id, turn_id, "fixture.repeated-sequence"),
                },
            ),
            Some("exit") => std::process::exit(17),
            Some("fail") => self.fail(
                output,
                DriverFailureScope::Turn,
                Some(turn_id),
                "fixture-failure",
                "intentional fixture turn failure",
            ),
            Some("wait-for-abort") => {
                self.active_turn = Some(turn_id.clone());
                self.emit(
                    output,
                    DriverBody::TurnEvent {
                        session_id,
                        turn_id,
                        event_type: "fixture.waiting".to_owned(),
                        payload: json!({ "cancellable": true }),
                    },
                )
            }
            _ => self.complete_turn(output, session_id, turn_id, task, capability_sources),
        }
    }

    fn complete_turn(
        &mut self,
        output: &mut impl Write,
        session_id: String,
        turn_id: String,
        task: &JsonValue,
        capability_sources: JsonValue,
    ) -> io::Result<()> {
        self.emit(
            output,
            DriverBody::TurnEvent {
                session_id: session_id.clone(),
                turn_id: turn_id.clone(),
                event_type: "fixture.started".to_owned(),
                payload: json!({ "task": task }),
            },
        )?;
        self.emit(
            output,
            DriverBody::TurnEvent {
                session_id: session_id.clone(),
                turn_id: turn_id.clone(),
                event_type: "fixture.capabilities".to_owned(),
                payload: capability_sources,
            },
        )?;
        self.emit(
            output,
            DriverBody::TurnFinished {
                session_id,
                turn_id,
                outcome: "completed".to_owned(),
                evidence: json!({ "fixture": true }),
            },
        )
    }

    fn abort_turn(
        &mut self,
        output: &mut impl Write,
        session_id: String,
        turn_id: String,
        reason: Option<&String>,
    ) -> io::Result<()> {
        if self.session_id.as_deref() != Some(&session_id)
            || self.active_turn.as_deref() != Some(&turn_id)
        {
            return self.fail(
                output,
                DriverFailureScope::Turn,
                Some(turn_id),
                "turn-not-active",
                "cannot abort an inactive turn",
            );
        }
        self.emit(
            output,
            DriverBody::TurnEvent {
                session_id: session_id.clone(),
                turn_id: turn_id.clone(),
                event_type: "fixture.aborted".to_owned(),
                payload: json!({ "reason": reason }),
            },
        )?;
        self.emit(
            output,
            DriverBody::TurnFinished {
                session_id,
                turn_id,
                outcome: "aborted".to_owned(),
                evidence: json!({ "fixture": true }),
            },
        )?;
        self.active_turn = None;
        Ok(())
    }

    fn close_session(&mut self, output: &mut impl Write, session_id: String) -> io::Result<bool> {
        if self.session_id.as_deref() != Some(&session_id) {
            self.fail(
                output,
                DriverFailureScope::Session,
                None,
                "unknown-session",
                "close session does not match the open session",
            )?;
            return Ok(false);
        }
        self.emit(output, DriverBody::SessionClosed { session_id })?;
        Ok(true)
    }
}

fn fixture_event(session_id: String, turn_id: String, event_type: &str) -> DriverBody {
    DriverBody::TurnEvent {
        session_id,
        turn_id,
        event_type: event_type.to_owned(),
        payload: JsonValue::Null,
    }
}

fn write_message(output: &mut impl Write, message: &DriverMessage) -> io::Result<()> {
    serde_json::to_writer(&mut *output, message)?;
    output.write_all(b"\n")?;
    output.flush()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stdin = io::stdin();
    let mut output = BufWriter::new(io::stdout());
    let mut fixture = Fixture::new();
    fixture.emit(
        &mut output,
        DriverBody::Ready {
            driver: DriverDescriptor {
                name: "agent-lab-fixture".to_owned(),
                version: env!("CARGO_PKG_VERSION").to_owned(),
                revision: None,
                features: vec![
                    "streaming".to_owned(),
                    "cancellation".to_owned(),
                    "raw-evidence".to_owned(),
                ],
            },
        },
    )?;

    for line in stdin.lock().lines() {
        let line = line?;
        fixture.caused_by = None;
        let command = match serde_json::from_str::<ControllerCommand>(&line) {
            Ok(command) => command,
            Err(error) => {
                fixture.fail(
                    &mut output,
                    DriverFailureScope::Protocol,
                    None,
                    "invalid-command",
                    &error.to_string(),
                )?;
                continue;
            }
        };
        fixture.caused_by = Some(command.message_id.clone());
        if command.protocol_version != PROTOCOL_VERSION {
            fixture.fail(
                &mut output,
                DriverFailureScope::Protocol,
                None,
                "unsupported-version",
                &format!("received {}", command.protocol_version),
            )?;
            continue;
        }

        if fixture.handle(&mut output, command.body)? {
            break;
        }
    }

    Ok(())
}
