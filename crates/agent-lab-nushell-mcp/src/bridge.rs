use std::{
    path::Path,
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

#[allow(deprecated)]
use rmcp::model::LoggingMessageNotificationParam;
use rmcp::{
    ClientHandler, ServiceExt,
    model::{
        CallToolRequestParams, CallToolResult, Meta, NumberOrString, ProgressNotificationParam,
        ProgressToken, Tool,
    },
    service::NotificationContext,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use thiserror::Error;
use tokio::{
    process::Command,
    sync::{mpsc as tokio_mpsc, oneshot},
};

static NEXT_RUNTIME_ID: AtomicU64 = AtomicU64::new(1);
const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LifecycleEvent {
    pub sequence: u64,
    pub kind: String,
    pub payload: JsonValue,
}

#[derive(Debug, Error)]
pub enum BridgeError {
    #[error("MCP bridge startup failed: {0}")]
    Startup(String),
    #[error("MCP bridge startup timed out after {timeout:?}")]
    StartupTimeout { timeout: Duration },
    #[error("MCP bridge request channel closed")]
    ChannelClosed,
    #[error("MCP request failed: {0}")]
    Request(String),
}

#[derive(Clone)]
pub struct McpBridge {
    inner: Arc<BridgeInner>,
}

struct BridgeInner {
    sender: tokio_mpsc::UnboundedSender<Request>,
    events: Arc<EventLog>,
    discovery_stale: Arc<AtomicBool>,
    runtime_id: u64,
}

enum Request {
    ListTools(mpsc::SyncSender<Result<Vec<Tool>, String>>),
    CallTool {
        name: String,
        arguments: serde_json::Map<String, JsonValue>,
        response: mpsc::SyncSender<Result<CallToolResult, String>>,
    },
    Shutdown,
}

#[derive(Default)]
struct EventLog {
    state: Mutex<EventLogState>,
}

#[derive(Default)]
struct EventLogState {
    next_sequence: u64,
    events: Vec<LifecycleEvent>,
}

impl EventLog {
    fn record(&self, kind: &str, payload: JsonValue) {
        let mut state = self.state.lock().expect("event log lock poisoned");
        state.next_sequence += 1;
        let sequence = state.next_sequence;
        state.events.push(LifecycleEvent {
            sequence,
            kind: kind.to_owned(),
            payload,
        });
    }

    fn snapshot(&self) -> Vec<LifecycleEvent> {
        self.state
            .lock()
            .expect("event log lock poisoned")
            .events
            .clone()
    }
}

#[derive(Clone)]
struct RecordingClient {
    events: Arc<EventLog>,
    discovery_stale: Arc<AtomicBool>,
}

impl ClientHandler for RecordingClient {
    fn on_progress(
        &self,
        params: ProgressNotificationParam,
        _context: NotificationContext<rmcp::RoleClient>,
    ) -> impl Future<Output = ()> + Send + '_ {
        self.events.record(
            "mcp.progress",
            serde_json::to_value(params).expect("progress notifications serialize"),
        );
        std::future::ready(())
    }

    #[allow(deprecated)]
    fn on_logging_message(
        &self,
        params: LoggingMessageNotificationParam,
        _context: NotificationContext<rmcp::RoleClient>,
    ) -> impl Future<Output = ()> + Send + '_ {
        self.events.record(
            "mcp.log",
            serde_json::to_value(params).expect("log notifications serialize"),
        );
        std::future::ready(())
    }

    fn on_tool_list_changed(
        &self,
        _context: NotificationContext<rmcp::RoleClient>,
    ) -> impl Future<Output = ()> + Send + '_ {
        self.discovery_stale.store(true, Ordering::SeqCst);
        self.events.record("mcp.tools.changed", JsonValue::Null);
        std::future::ready(())
    }
}

impl McpBridge {
    /// Start a child MCP server and a persistent async runtime that owns its session.
    ///
    /// # Errors
    ///
    /// Returns an error when the runtime thread, child transport, MCP
    /// initialization, or initial discovery cannot start.
    pub fn connect(
        executable: impl AsRef<Path>,
        args: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, BridgeError> {
        Self::connect_with_timeout(
            executable.as_ref().to_owned(),
            args.into_iter().map(Into::into).collect(),
            STARTUP_TIMEOUT,
        )
    }

    fn connect_with_timeout(
        executable: std::path::PathBuf,
        args: Vec<String>,
        startup_timeout: Duration,
    ) -> Result<Self, BridgeError> {
        let runtime_id = NEXT_RUNTIME_ID.fetch_add(1, Ordering::SeqCst);
        let (sender, receiver) = tokio_mpsc::unbounded_channel();
        let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
        let (startup_cancel_sender, startup_cancel_receiver) = oneshot::channel();
        let events = Arc::new(EventLog::default());
        let discovery_stale = Arc::new(AtomicBool::new(false));
        let handler = RecordingClient {
            events: events.clone(),
            discovery_stale: discovery_stale.clone(),
        };
        thread::Builder::new()
            .name(format!("agent-lab-mcp-{runtime_id}"))
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build();
                let result = match runtime {
                    Ok(runtime) => runtime.block_on(run_actor(
                        executable,
                        args,
                        handler,
                        receiver,
                        ready_sender.clone(),
                        startup_cancel_receiver,
                    )),
                    Err(error) => Err(error.to_string()),
                };

                if let Err(error) = result {
                    let _ = ready_sender.send(Err(error));
                }
            })
            .map_err(|error| BridgeError::Startup(error.to_string()))?;

        match ready_receiver.recv_timeout(startup_timeout) {
            Ok(result) => result.map_err(BridgeError::Startup)?,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let _ = startup_cancel_sender.send(());
                return Err(BridgeError::StartupTimeout {
                    timeout: startup_timeout,
                });
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(BridgeError::ChannelClosed);
            }
        }

        events.record("bridge.ready", json!({ "runtimeId": runtime_id }));

        Ok(Self {
            inner: Arc::new(BridgeInner {
                sender,
                events,
                discovery_stale,
                runtime_id,
            }),
        })
    }

    #[must_use]
    pub fn runtime_id(&self) -> u64 {
        self.inner.runtime_id
    }

    #[must_use]
    pub fn discovery_is_stale(&self) -> bool {
        self.inner.discovery_stale.load(Ordering::SeqCst)
    }

    pub(crate) fn mark_discovery_fresh(&self) {
        self.inner.discovery_stale.store(false, Ordering::SeqCst);
    }

    #[must_use]
    pub fn events(&self) -> Vec<LifecycleEvent> {
        self.inner.events.snapshot()
    }

    /// Return all tools currently exposed by the live MCP session.
    ///
    /// This method waits synchronously for the background actor. It is safe to
    /// call from a Tokio context, but it blocks the caller's thread.
    ///
    /// # Errors
    ///
    /// Returns an error when the actor has stopped or MCP discovery fails.
    pub fn list_tools(&self) -> Result<Vec<Tool>, BridgeError> {
        let (sender, receiver) = mpsc::sync_channel(1);
        self.inner
            .sender
            .send(Request::ListTools(sender))
            .map_err(|_| BridgeError::ChannelClosed)?;
        let tools = receiver
            .recv()
            .map_err(|_| BridgeError::ChannelClosed)?
            .map_err(BridgeError::Request)?;
        self.inner
            .events
            .record("mcp.tools.listed", json!({ "count": tools.len() }));
        Ok(tools)
    }

    /// Invoke one tool through the live MCP session.
    ///
    /// This method waits synchronously for the background actor. It is safe to
    /// call from a Tokio context, but it blocks the caller's thread.
    ///
    /// # Errors
    ///
    /// Returns an error when the actor has stopped or the server returns a
    /// protocol-level failure. Tool-level failures remain successful MCP
    /// responses with `is_error` set.
    pub fn call_tool(
        &self,
        name: impl Into<String>,
        arguments: serde_json::Map<String, JsonValue>,
    ) -> Result<CallToolResult, BridgeError> {
        let name = name.into();
        let (sender, receiver) = mpsc::sync_channel(1);
        self.inner
            .events
            .record("mcp.tool.started", json!({ "name": name }));
        self.inner
            .sender
            .send(Request::CallTool {
                name: name.clone(),
                arguments,
                response: sender,
            })
            .map_err(|_| BridgeError::ChannelClosed)?;
        match receiver.recv().map_err(|_| BridgeError::ChannelClosed)? {
            Ok(result) => {
                self.inner.events.record(
                    "mcp.tool.completed",
                    json!({ "name": name, "isError": result.is_error.unwrap_or(false) }),
                );
                Ok(result)
            }
            Err(error) => {
                self.inner.events.record(
                    "mcp.tool.protocol_failed",
                    json!({ "name": name, "error": error }),
                );
                Err(BridgeError::Request(error))
            }
        }
    }
}

impl Drop for BridgeInner {
    fn drop(&mut self) {
        let _ = self.sender.send(Request::Shutdown);
    }
}

async fn run_actor(
    executable: std::path::PathBuf,
    args: Vec<String>,
    handler: RecordingClient,
    mut receiver: tokio_mpsc::UnboundedReceiver<Request>,
    ready_sender: mpsc::SyncSender<Result<(), String>>,
    mut startup_cancel: oneshot::Receiver<()>,
) -> Result<(), String> {
    let mut command = Command::new(executable);
    command
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    let mut child = command.spawn().map_err(|error| error.to_string())?;
    let child_stdout = child
        .stdout
        .take()
        .ok_or_else(|| "MCP child stdout was not piped".to_owned())?;
    let child_stdin = child
        .stdin
        .take()
        .ok_or_else(|| "MCP child stdin was not piped".to_owned())?;
    let startup_result = {
        let startup = async {
            let service = handler
                .serve((child_stdout, child_stdin))
                .await
                .map_err(|error| error.to_string())?;
            service
                .peer()
                .list_all_tools()
                .await
                .map_err(|error| error.to_string())?;
            Ok::<_, String>(service)
        };
        tokio::pin!(startup);
        tokio::select! {
            result = &mut startup => Some(result),
            _ = &mut startup_cancel => None,
        }
    };
    let mut service = if let Some(result) = startup_result {
        result?
    } else {
        terminate_child(&mut child).await?;
        return Err("bridge startup cancelled".to_owned());
    };
    ready_sender
        .send(Ok(()))
        .map_err(|_| "bridge ready receiver closed".to_owned())?;

    let mut progress_token = 0_i64;
    while let Some(request) = receiver.recv().await {
        match request {
            Request::ListTools(response) => {
                let result = service
                    .peer()
                    .list_all_tools()
                    .await
                    .map_err(|error| error.to_string());
                let _ = response.send(result);
            }
            Request::CallTool {
                name,
                arguments,
                response,
            } => {
                progress_token += 1;
                let mut params = CallToolRequestParams::new(name).with_arguments(arguments);
                params.meta = Some(Meta::with_progress_token(ProgressToken(
                    NumberOrString::Number(progress_token),
                )));
                let result = service
                    .peer()
                    .call_tool(params)
                    .await
                    .map_err(|error| error.to_string());
                let _ = response.send(result);
            }
            Request::Shutdown => break,
        }
    }

    let _ = service.close().await;
    wait_for_child_exit(&mut child).await?;
    Ok(())
}

async fn terminate_child(child: &mut tokio::process::Child) -> Result<(), String> {
    match child.try_wait().map_err(|error| error.to_string())? {
        Some(_) => Ok(()),
        None => child.kill().await.map_err(|error| error.to_string()),
    }
}

async fn wait_for_child_exit(child: &mut tokio::process::Child) -> Result<(), String> {
    match tokio::time::timeout(Duration::from_secs(3), child.wait()).await {
        Ok(result) => result.map(|_| ()).map_err(|error| error.to_string()),
        Err(_) => terminate_child(child).await,
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::{
        fs,
        process::{Command, Stdio},
        sync::Arc,
        thread,
        time::Instant,
    };

    use serde_json::json;

    use super::{BridgeError, EventLog, McpBridge};

    #[test]
    fn startup_timeout_stops_the_in_flight_child() {
        let marker = std::env::temp_dir().join(format!(
            "agent-lab-mcp-timeout-{}-{}",
            std::process::id(),
            super::NEXT_RUNTIME_ID.load(std::sync::atomic::Ordering::SeqCst)
        ));
        let script = r#"echo $$ > "$1"; exec sleep 30"#;
        let error = McpBridge::connect_with_timeout(
            "/bin/sh".into(),
            vec![
                "-c".to_owned(),
                script.to_owned(),
                "agent-lab-timeout".to_owned(),
                marker.to_string_lossy().into_owned(),
            ],
            std::time::Duration::from_millis(500),
        )
        .err()
        .expect("hung startup should time out");
        assert!(matches!(error, BridgeError::StartupTimeout { .. }));

        let deadline = Instant::now() + std::time::Duration::from_secs(2);
        let pid = loop {
            if let Ok(pid) = fs::read_to_string(&marker) {
                break pid.trim().to_owned();
            }
            assert!(Instant::now() < deadline, "child should record its pid");
            thread::sleep(std::time::Duration::from_millis(10));
        };
        while Command::new("kill")
            .args(["-0", &pid])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
        {
            assert!(Instant::now() < deadline, "timed-out child should stop");
            thread::sleep(std::time::Duration::from_millis(10));
        }
        let _ = fs::remove_file(marker);
    }

    #[test]
    fn concurrent_event_records_remain_in_sequence_order() {
        let events = Arc::new(EventLog::default());
        let threads = (0..8)
            .map(|thread_id| {
                let events = events.clone();
                thread::spawn(move || {
                    for event_id in 0..100 {
                        events.record("test", json!({ "thread": thread_id, "event": event_id }));
                    }
                })
            })
            .collect::<Vec<_>>();
        for thread in threads {
            thread.join().expect("event writer should finish");
        }

        let snapshot = events.snapshot();
        assert_eq!(snapshot.len(), 800);
        assert!(
            snapshot
                .iter()
                .enumerate()
                .all(|(index, event)| event.sequence == index as u64 + 1)
        );
    }
}
