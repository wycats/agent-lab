use std::{
    fs,
    io::{self, BufRead, BufWriter, Write},
    path::PathBuf,
};

use agent_lab_driver_protocol::{
    CommandBody, ControllerCommand, DriverBody, DriverDescriptor, DriverFailureScope,
    DriverMessage, MAX_DRIVER_RECORD_BYTES, MAX_DRIVER_STDERR_BYTES, PROTOCOL_VERSION,
};
use rmcp::{
    ClientHandler, ServiceExt,
    model::CallToolRequestParams,
    transport::{
        StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use serde_json::{Map, Value as JsonValue, json};

#[derive(Clone)]
struct FixtureMcpClient;

impl ClientHandler for FixtureMcpClient {}

struct Fixture {
    sequence: u64,
    caused_by: Option<String>,
    session_id: Option<String>,
    active_turn: Option<String>,
    workspace_root: Option<PathBuf>,
}

impl Fixture {
    fn new() -> Self {
        Self {
            sequence: 0,
            caused_by: None,
            session_id: None,
            active_turn: None,
            workspace_root: None,
        }
    }

    fn startup(&mut self, output: &mut impl Write) -> io::Result<()> {
        self.emit(
            output,
            DriverBody::StartupEvent {
                phase: "adapter-load".to_owned(),
                status: "completed".to_owned(),
                detail: Some("Fixture adapter loaded".to_owned()),
            },
        )
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
        session_id: Option<String>,
        turn_id: Option<String>,
        code: &str,
        message: &str,
    ) -> io::Result<()> {
        self.emit(
            output,
            DriverBody::Failed {
                scope,
                session_id,
                turn_id,
                code: code.to_owned(),
                message: message.to_owned(),
            },
        )
    }

    fn handle(&mut self, output: &mut impl Write, body: CommandBody) -> io::Result<bool> {
        match body {
            CommandBody::OpenSession {
                session_id, config, ..
            } => {
                self.open_session(output, session_id, &config)?;
                Ok(false)
            }
            CommandBody::StartTurn {
                session_id,
                turn_id,
                task,
                capability_sources,
            } => {
                self.start_turn(output, session_id, turn_id, &task, &capability_sources)?;
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
            CommandBody::CloseSession { session_id } => self.close_session(output, &session_id),
        }
    }

    fn open_session(
        &mut self,
        output: &mut impl Write,
        session_id: String,
        config: &JsonValue,
    ) -> io::Result<()> {
        if self.session_id.is_some() {
            return self.fail(
                output,
                DriverFailureScope::Session,
                Some(session_id),
                None,
                "session-already-open",
                "fixture supports one session",
            );
        }
        self.session_id = Some(session_id.clone());
        self.workspace_root = config
            .get("workspaceRoot")
            .and_then(JsonValue::as_str)
            .map(PathBuf::from);
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
        capability_sources: &JsonValue,
    ) -> io::Result<()> {
        if self.session_id.as_deref() != Some(&session_id) {
            return self.fail(
                output,
                DriverFailureScope::Session,
                Some(session_id),
                None,
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
            Some("wrong-turn-finished") => self.emit(
                output,
                DriverBody::TurnFinished {
                    session_id,
                    turn_id: "stale-turn".to_owned(),
                    outcome: "completed".to_owned(),
                    evidence: json!({ "fixture": true }),
                },
            ),
            Some("unexpected-ready") => self.emit(
                output,
                DriverBody::Ready {
                    driver: DriverDescriptor {
                        name: "unexpected-ready".to_owned(),
                        version: "1".to_owned(),
                        revision: None,
                        features: vec![],
                    },
                },
            ),
            Some("fail") => self.fail(
                output,
                DriverFailureScope::Turn,
                Some(session_id),
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
        capability_sources: &JsonValue,
    ) -> io::Result<()> {
        let scenario_mode = task.get("mode").and_then(JsonValue::as_str) == Some("real");
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
                payload: (*capability_sources).clone(),
            },
        )?;
        if scenario_mode {
            self.complete_catalog_scenario(output, &session_id, &turn_id, capability_sources)?;
        }
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

    fn complete_catalog_scenario(
        &mut self,
        output: &mut impl Write,
        session_id: &str,
        turn_id: &str,
        capability_sources: &JsonValue,
    ) -> io::Result<()> {
        let result = call_catalog_capabilities(capability_sources)?;

        if let Some(workspace_root) = &self.workspace_root {
            fs::write(
                workspace_root.join("result.json"),
                serde_json::to_vec_pretty(&result).map_err(io::Error::other)?,
            )?;
            self.emit(
                output,
                DriverBody::TurnEvent {
                    session_id: session_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    event_type: "workspace.changed".to_owned(),
                    payload: json!({ "path": "result.json", "kind": "created" }),
                },
            )?;
        }
        Ok(())
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
                Some(session_id),
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

    fn close_session(&mut self, output: &mut impl Write, session_id: &str) -> io::Result<bool> {
        if self.session_id.as_deref() != Some(session_id) {
            self.fail(
                output,
                DriverFailureScope::Session,
                Some(session_id.to_owned()),
                None,
                "unknown-session",
                "close session does not match the open session",
            )?;
            return Ok(false);
        }
        if self.active_turn.is_some() {
            self.fail(
                output,
                DriverFailureScope::Session,
                Some(session_id.to_owned()),
                None,
                "turn-active",
                "cannot close a session while a turn is active",
            )?;
            return Ok(false);
        }
        self.emit(
            output,
            DriverBody::SessionClosed {
                session_id: session_id.to_owned(),
            },
        )?;
        let trailing_count = std::env::var("AGENT_LAB_FIXTURE_TRAILING_STDOUT_COUNT")
            .ok()
            .and_then(|count| count.parse().ok())
            .unwrap_or_else(|| {
                usize::from(std::env::var_os("AGENT_LAB_FIXTURE_TRAILING_STDOUT").is_some())
            });
        for index in 0..trailing_count {
            self.emit(
                output,
                DriverBody::TurnEvent {
                    session_id: session_id.to_owned(),
                    turn_id: "after-close".to_owned(),
                    event_type: "fixture.trailing-stdout".to_owned(),
                    payload: json!({ "index": index }),
                },
            )?;
        }
        if std::env::var_os("AGENT_LAB_FIXTURE_TRAILING_MALFORMED_STDOUT").is_some() {
            output.write_all(b"{not-json}\n")?;
            output.flush()?;
        }
        if std::env::var_os("AGENT_LAB_FIXTURE_TRAILING_STDERR").is_some() {
            eprintln!("fixture trailing stderr");
        }
        Ok(true)
    }
}

fn call_catalog_capabilities(capability_sources: &JsonValue) -> io::Result<JsonValue> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(io::Error::other)?;
    runtime.block_on(async {
        let catalog = call_capability(capability_sources, "catalog", "list", Map::new()).await?;
        let items = catalog
            .get("items")
            .and_then(JsonValue::as_array)
            .cloned()
            .ok_or_else(|| io::Error::other("catalog fixture result omitted items"))?;
        call_capability(
            capability_sources,
            "analysis",
            "summarize",
            json!({ "items": items })
                .as_object()
                .cloned()
                .unwrap_or_default(),
        )
        .await
    })
}

async fn call_capability(
    capability_sources: &JsonValue,
    source_id: &str,
    tool: &str,
    arguments: Map<String, JsonValue>,
) -> io::Result<JsonValue> {
    let source = capability_sources
        .as_array()
        .and_then(|sources| {
            sources
                .iter()
                .find(|source| source.get("id").and_then(JsonValue::as_str) == Some(source_id))
        })
        .ok_or_else(|| io::Error::other(format!("fixture source is unavailable: {source_id}")))?;
    let transport = source
        .get("transport")
        .ok_or_else(|| io::Error::other(format!("fixture source has no transport: {source_id}")))?;
    let url = transport
        .get("url")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| io::Error::other(format!("fixture source has no URL: {source_id}")))?;
    let token = transport
        .pointer("/headers/Authorization")
        .and_then(JsonValue::as_str)
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or_else(|| {
            io::Error::other(format!("fixture source has no bearer token: {source_id}"))
        })?;
    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(url).auth_header(token),
    );
    let service = FixtureMcpClient
        .serve(transport)
        .await
        .map_err(io::Error::other)?;
    let result = service
        .peer()
        .call_tool(CallToolRequestParams::new(tool.to_owned()).with_arguments(arguments))
        .await
        .map_err(io::Error::other)?;
    let structured = result.structured_content.ok_or_else(|| {
        io::Error::other(format!("fixture tool returned no structured data: {tool}"))
    })?;
    service.cancel().await.map_err(io::Error::other)?;
    Ok(structured)
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

fn start_fixture(output: &mut impl Write, fixture: &mut Fixture) -> io::Result<bool> {
    fixture.startup(output)?;
    fixture.emit(
        output,
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
    if std::env::var_os("AGENT_LAB_FIXTURE_EXIT_AFTER_READY").is_some() {
        return Ok(true);
    }
    if std::env::var_os("AGENT_LAB_FIXTURE_MALFORMED_AFTER_READY").is_some() {
        output.write_all(b"{not-json}\n")?;
        output.flush()?;
        return Ok(true);
    }
    Ok(false)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os("AGENT_LAB_FIXTURE_OVERSIZED_STDOUT").is_some() {
        let mut output = io::stdout().lock();
        output.write_all(&vec![b'x'; MAX_DRIVER_RECORD_BYTES + 1])?;
        output.flush()?;
        return Ok(());
    }
    if std::env::var_os("AGENT_LAB_FIXTURE_OVERSIZED_STDERR").is_some() {
        let mut error = io::stderr().lock();
        error.write_all(&vec![b'x'; MAX_DRIVER_STDERR_BYTES + 1])?;
        error.flush()?;
        std::thread::sleep(std::time::Duration::from_secs(30));
        return Ok(());
    }
    if std::env::var_os("AGENT_LAB_FIXTURE_LARGE_TRANSCRIPT").is_some() {
        let mut output = BufWriter::new(io::stdout());
        for sequence in 1..=10 {
            write_message(
                &mut output,
                &DriverMessage {
                    protocol_version: PROTOCOL_VERSION,
                    sequence,
                    caused_by: None,
                    body: DriverBody::Ready {
                        driver: DriverDescriptor {
                            name: "large-transcript".to_owned(),
                            version: "1".to_owned(),
                            revision: None,
                            features: vec!["x".repeat(MAX_DRIVER_RECORD_BYTES - 1024)],
                        },
                    },
                },
            )?;
        }
        return Ok(());
    }
    let stdin = io::stdin();
    let mut output = BufWriter::new(io::stdout());
    let mut fixture = Fixture::new();
    if start_fixture(&mut output, &mut fixture)? {
        return Ok(());
    }

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
                None,
                "unsupported-version",
                &format!("received {}", command.protocol_version),
            )?;
            continue;
        }

        if fixture.handle(&mut output, command.body)?
            && std::env::var_os("AGENT_LAB_FIXTURE_WAIT_FOR_STDIN_EOF_AFTER_CLOSE").is_none()
        {
            break;
        }
    }

    if let Some(code) = std::env::var("AGENT_LAB_FIXTURE_EXIT_CODE_AFTER_CLOSE")
        .ok()
        .and_then(|code| code.parse().ok())
    {
        std::process::exit(code);
    }

    Ok(())
}
