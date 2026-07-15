use std::{
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    thread,
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
    transport::TokioChildProcess,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use thiserror::Error;
use tokio::{process::Command, sync::mpsc as tokio_mpsc};

static NEXT_RUNTIME_ID: AtomicU64 = AtomicU64::new(1);

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
    sender: tokio_mpsc::Sender<Request>,
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
    next_sequence: AtomicU64,
    events: Mutex<Vec<LifecycleEvent>>,
}

impl EventLog {
    fn record(&self, kind: &str, payload: JsonValue) {
        let sequence = self.next_sequence.fetch_add(1, Ordering::SeqCst) + 1;
        self.events
            .lock()
            .expect("event log lock poisoned")
            .push(LifecycleEvent {
                sequence,
                kind: kind.to_owned(),
                payload,
            });
    }

    fn snapshot(&self) -> Vec<LifecycleEvent> {
        self.events.lock().expect("event log lock poisoned").clone()
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
        let runtime_id = NEXT_RUNTIME_ID.fetch_add(1, Ordering::SeqCst);
        let (sender, receiver) = tokio_mpsc::channel(32);
        let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
        let events = Arc::new(EventLog::default());
        let discovery_stale = Arc::new(AtomicBool::new(false));
        let handler = RecordingClient {
            events: events.clone(),
            discovery_stale: discovery_stale.clone(),
        };
        let executable = executable.as_ref().to_owned();
        let args = args.into_iter().map(Into::into).collect::<Vec<_>>();

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
                    )),
                    Err(error) => Err(error.to_string()),
                };

                if let Err(error) = result {
                    let _ = ready_sender.send(Err(error));
                }
            })
            .map_err(|error| BridgeError::Startup(error.to_string()))?;

        ready_receiver
            .recv()
            .map_err(|_| BridgeError::ChannelClosed)?
            .map_err(BridgeError::Startup)?;

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

    #[must_use]
    pub fn events(&self) -> Vec<LifecycleEvent> {
        self.inner.events.snapshot()
    }

    /// Return all tools currently exposed by the live MCP session.
    ///
    /// # Errors
    ///
    /// Returns an error when the actor has stopped or MCP discovery fails.
    pub fn list_tools(&self) -> Result<Vec<Tool>, BridgeError> {
        let (sender, receiver) = mpsc::sync_channel(1);
        self.inner
            .sender
            .blocking_send(Request::ListTools(sender))
            .map_err(|_| BridgeError::ChannelClosed)?;
        let tools = receiver
            .recv()
            .map_err(|_| BridgeError::ChannelClosed)?
            .map_err(BridgeError::Request)?;
        self.inner.discovery_stale.store(false, Ordering::SeqCst);
        self.inner
            .events
            .record("mcp.tools.listed", json!({ "count": tools.len() }));
        Ok(tools)
    }

    /// Invoke one tool through the live MCP session.
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
            .blocking_send(Request::CallTool {
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
        let _ = self.sender.blocking_send(Request::Shutdown);
    }
}

async fn run_actor(
    executable: std::path::PathBuf,
    args: Vec<String>,
    handler: RecordingClient,
    mut receiver: tokio_mpsc::Receiver<Request>,
    ready_sender: mpsc::SyncSender<Result<(), String>>,
) -> Result<(), String> {
    let mut command = Command::new(executable);
    command.args(args).kill_on_drop(true);
    let transport = TokioChildProcess::new(command).map_err(|error| error.to_string())?;
    let mut service = handler
        .serve(transport)
        .await
        .map_err(|error| error.to_string())?;
    service
        .peer()
        .list_all_tools()
        .await
        .map_err(|error| error.to_string())?;
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
    Ok(())
}
