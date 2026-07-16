//! Local browser gateway for Agent Lab terminal sessions.

use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use axum::{
    Json, Router,
    extract::{
        Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, HeaderValue, StatusCode, Uri, header},
    response::{IntoResponse, Response},
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use portable_pty::{Child, CommandBuilder, MasterPty, NativePtySystem, PtySize, PtySystem};
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tower_http::{
    services::{ServeDir, ServeFile},
    set_header::SetResponseHeaderLayer,
};

const DEFAULT_COLS: u16 = 100;
const DEFAULT_ROWS: u16 = 30;
const AUTH_PROTOCOL_PREFIX: &str = "agent-lab.auth.";

/// A source capable of opening a terminal session for the web surface.
pub trait SessionProvider: Send + Sync + 'static {
    /// Human-readable provider name sent to the browser as session evidence.
    fn name(&self) -> &'static str;

    /// Open one new terminal session at the requested dimensions.
    ///
    /// # Errors
    ///
    /// Returns an error when the provider cannot create its bounded session.
    fn open(&self, size: TerminalSize) -> Result<Box<dyn BrowserSession>, GatewayError>;
}

/// A bidirectional terminal session independent of its process or transport.
pub trait BrowserSession: Send + Sync + 'static {
    /// Transfer ownership of the session's output reader to the gateway.
    ///
    /// # Errors
    ///
    /// Returns an error if the reader was already taken or is unavailable.
    fn take_reader(&self) -> Result<Box<dyn Read + Send>, GatewayError>;

    /// Write browser input or a terminal response into the session.
    ///
    /// # Errors
    ///
    /// Returns an error when the session can no longer accept input.
    fn write(&self, bytes: &[u8]) -> Result<(), GatewayError>;

    /// Resize the session's terminal viewport.
    ///
    /// # Errors
    ///
    /// Returns an error when the session cannot apply the requested size.
    fn resize(&self, size: TerminalSize) -> Result<(), GatewayError>;

    /// Terminate the bounded session and wait for its child resources.
    fn terminate(&self);
}

/// The bounded fixture provider used by the first browser steel thread.
#[derive(Debug, Clone)]
pub struct FixtureSessionProvider {
    shell: PathBuf,
}

impl FixtureSessionProvider {
    /// Create a provider that launches the existing visual shell binary.
    #[must_use]
    pub fn new(shell: impl Into<PathBuf>) -> Self {
        Self {
            shell: shell.into(),
        }
    }
}

impl SessionProvider for FixtureSessionProvider {
    fn name(&self) -> &'static str {
        "fixture"
    }

    fn open(&self, size: TerminalSize) -> Result<Box<dyn BrowserSession>, GatewayError> {
        Ok(Box::new(PtyTerminalSession::spawn(&self.shell, size)?))
    }
}

/// Dimensions shared by the browser terminal and the child PTY.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
pub struct TerminalSize {
    /// Terminal columns.
    pub cols: u16,
    /// Terminal rows.
    pub rows: u16,
}

impl TerminalSize {
    fn validated(self) -> Result<Self, GatewayError> {
        if self.cols == 0 || self.rows == 0 || self.cols > 500 || self.rows > 500 {
            return Err(GatewayError::InvalidSize);
        }
        Ok(self)
    }
}

impl Default for TerminalSize {
    fn default() -> Self {
        Self {
            cols: DEFAULT_COLS,
            rows: DEFAULT_ROWS,
        }
    }
}

struct PtyTerminalSession {
    master: Mutex<Box<dyn MasterPty + Send>>,
    child: Mutex<Box<dyn Child + Send + Sync>>,
    reader: Mutex<Option<Box<dyn Read + Send>>>,
    writer: Mutex<Box<dyn Write + Send>>,
}

impl PtyTerminalSession {
    fn spawn(shell: &Path, size: TerminalSize) -> Result<Self, GatewayError> {
        if !shell.is_file() {
            return Err(GatewayError::ShellNotFound(shell.to_path_buf()));
        }

        let pair = NativePtySystem::default().openpty(size.into())?;
        let mut command = CommandBuilder::new(shell);
        command.arg("--fixture");
        let child = pair.slave.spawn_command(command)?;
        drop(pair.slave);

        let reader = pair.master.try_clone_reader()?;
        let writer = pair.master.take_writer()?;

        Ok(Self {
            master: Mutex::new(pair.master),
            child: Mutex::new(child),
            reader: Mutex::new(Some(reader)),
            writer: Mutex::new(writer),
        })
    }
}

impl BrowserSession for PtyTerminalSession {
    fn take_reader(&self) -> Result<Box<dyn Read + Send>, GatewayError> {
        self.reader
            .lock()
            .map_err(|_| GatewayError::SessionUnavailable)?
            .take()
            .ok_or(GatewayError::SessionUnavailable)
    }

    fn write(&self, bytes: &[u8]) -> Result<(), GatewayError> {
        let mut writer = self
            .writer
            .lock()
            .map_err(|_| GatewayError::SessionUnavailable)?;
        writer.write_all(bytes)?;
        writer.flush()?;
        Ok(())
    }

    fn resize(&self, size: TerminalSize) -> Result<(), GatewayError> {
        self.master
            .lock()
            .map_err(|_| GatewayError::SessionUnavailable)?
            .resize(size.into())
            .map_err(GatewayError::from)
    }

    fn terminate(&self) {
        if let Ok(mut child) = self.child.lock() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for PtyTerminalSession {
    fn drop(&mut self) {
        self.terminate();
    }
}

impl From<TerminalSize> for PtySize {
    fn from(size: TerminalSize) -> Self {
        Self {
            rows: size.rows,
            cols: size.cols,
            pixel_width: 0,
            pixel_height: 0,
        }
    }
}

/// Configuration for the loopback browser server.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Directory containing the `SvelteKit` static build.
    pub assets: PathBuf,
    /// Exact browser origin allowed to open sessions.
    pub origin: String,
    /// Per-process bearer token required by the WebSocket upgrade.
    pub token: String,
}

impl ServerConfig {
    /// Create configuration for a server that is already bound to loopback.
    #[must_use]
    pub fn new(assets: impl Into<PathBuf>, origin: String) -> Self {
        Self {
            assets: assets.into(),
            origin,
            token: generate_token(),
        }
    }
}

#[derive(Clone)]
struct AppState {
    config: ServerConfig,
    provider: Arc<dyn SessionProvider>,
}

/// Build the HTTP application around one bounded session provider.
pub fn app(config: ServerConfig, provider: Arc<dyn SessionProvider>) -> Router {
    let index = config.assets.join("index.html");
    let assets = ServeDir::new(&config.assets).not_found_service(ServeFile::new(index));
    let state = AppState { config, provider };

    Router::new()
        .route("/api/session-token", get(session_token))
        .route("/api/terminal", get(upgrade_terminal))
        .fallback_service(assets)
        .layer(SetResponseHeaderLayer::if_not_present(
            header::X_FRAME_OPTIONS,
            HeaderValue::from_static("DENY"),
        ))
        .layer(SetResponseHeaderLayer::if_not_present(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        ))
        .with_state(state)
}

#[derive(Serialize)]
struct TokenResponse<'a> {
    token: &'a str,
}

async fn session_token(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if !request_is_same_origin(&headers, &state.config.origin, true) {
        return StatusCode::FORBIDDEN.into_response();
    }

    let mut response = Json(TokenResponse {
        token: &state.config.token,
    })
    .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, max-age=0"),
    );
    response
}

#[derive(Debug, Deserialize)]
struct TerminalQuery {
    #[serde(default = "default_cols")]
    cols: u16,
    #[serde(default = "default_rows")]
    rows: u16,
}

const fn default_cols() -> u16 {
    DEFAULT_COLS
}

const fn default_rows() -> u16 {
    DEFAULT_ROWS
}

async fn upgrade_terminal(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<TerminalQuery>,
    upgrade: WebSocketUpgrade,
) -> Response {
    let auth_protocol = format!("{AUTH_PROTOCOL_PREFIX}{}", state.config.token);
    if !terminal_request_is_authorized(&headers, &auth_protocol, &state.config) {
        return StatusCode::FORBIDDEN.into_response();
    }

    let Ok(size) = (TerminalSize {
        cols: query.cols,
        rows: query.rows,
    })
    .validated() else {
        return StatusCode::BAD_REQUEST.into_response();
    };

    upgrade
        .protocols([auth_protocol])
        .on_upgrade(move |socket| serve_terminal(socket, state.provider, size))
}

fn terminal_request_is_authorized(
    headers: &HeaderMap,
    auth_protocol: &str,
    config: &ServerConfig,
) -> bool {
    headers
        .get(header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|protocol| protocol.trim() == auth_protocol)
        })
        && request_is_same_origin(headers, &config.origin, false)
}

fn request_is_same_origin(headers: &HeaderMap, expected: &str, allow_referer: bool) -> bool {
    let host_matches = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|host| expected == format!("http://{host}"));
    if !host_matches {
        return false;
    }

    match headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
    {
        Some(origin) => origin == expected,
        None if allow_referer => headers
            .get(header::REFERER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<Uri>().ok())
            .is_some_and(|uri| {
                uri.scheme_str()
                    .zip(uri.authority())
                    .is_some_and(|(scheme, authority)| {
                        expected == format!("{scheme}://{authority}")
                    })
            }),
        None => false,
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientControl {
    Resize { cols: u16, rows: u16 },
}

/// Structured lifecycle evidence carried separately from binary PTY frames.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum SessionEvent<'a> {
    /// A provider opened a new bounded session.
    Started {
        provider: &'a str,
        cols: u16,
        rows: u16,
    },
    /// The browser and the underlying session accepted new terminal dimensions.
    Resized { cols: u16, rows: u16 },
    /// The session's output stream reached EOF.
    Exited,
    /// The provider could not open or initialize the session.
    Error { message: &'a str },
}

fn session_event_message(event: &SessionEvent<'_>) -> Message {
    Message::Text(
        serde_json::to_string(event)
            .expect("session event serialization cannot fail")
            .into(),
    )
}

fn spawn_session_reader(
    mut reader: Box<dyn Read + Send>,
) -> (
    mpsc::Receiver<Result<Vec<u8>, std::io::Error>>,
    tokio::task::JoinHandle<()>,
) {
    let (output_tx, output_rx) = mpsc::channel(32);
    let task = tokio::task::spawn_blocking(move || {
        let mut buffer = [0_u8; 8192];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => break,
                Err(error) => {
                    tracing::debug!(%error, "terminal reader stopped");
                    let _ = output_tx.blocking_send(Err(error));
                    break;
                }
                Ok(read) => {
                    if output_tx
                        .blocking_send(Ok(buffer[..read].to_vec()))
                        .is_err()
                    {
                        break;
                    }
                }
            }
        }
    });
    (output_rx, task)
}

fn spawn_session_writer(
    session: Arc<dyn BrowserSession>,
) -> (
    mpsc::Sender<Vec<u8>>,
    tokio::task::JoinHandle<Result<(), GatewayError>>,
) {
    let (input_tx, mut input_rx) = mpsc::channel::<Vec<u8>>(32);
    let task = tokio::task::spawn_blocking(move || {
        while let Some(bytes) = input_rx.blocking_recv() {
            session.write(&bytes)?;
        }
        Ok(())
    });
    (input_tx, task)
}

async fn open_session(
    provider: Arc<dyn SessionProvider>,
    initial_size: TerminalSize,
) -> Result<Box<dyn BrowserSession>, String> {
    match tokio::task::spawn_blocking(move || provider.open(initial_size)).await {
        Ok(result) => result.map_err(|error| error.to_string()),
        Err(error) => Err(format!("session provider task failed: {error}")),
    }
}

async fn serve_terminal(
    socket: WebSocket,
    provider: Arc<dyn SessionProvider>,
    initial_size: TerminalSize,
) {
    let session = match open_session(Arc::clone(&provider), initial_size).await {
        Ok(session) => session,
        Err(message) => {
            send_open_error(socket, &message).await;
            return;
        }
    };

    let (mut socket_tx, mut socket_rx) = socket.split();
    let started = session_event_message(&SessionEvent::Started {
        provider: provider.name(),
        cols: initial_size.cols,
        rows: initial_size.rows,
    });
    if socket_tx.send(started).await.is_err() {
        session.terminate();
        return;
    }

    let reader = match session.take_reader() {
        Ok(reader) => reader,
        Err(error) => {
            session.terminate();
            let message = error.to_string();
            let event = session_event_message(&SessionEvent::Error { message: &message });
            let _ = socket_tx.send(event).await;
            return;
        }
    };
    let session: Arc<dyn BrowserSession> = Arc::from(session);
    let (mut pty_rx, read_task) = spawn_session_reader(reader);
    let (input_tx, mut write_task) = spawn_session_writer(Arc::clone(&session));

    loop {
        tokio::select! {
            result = &mut write_task => {
                if let Ok(Err(error)) = result {
                    tracing::debug!(%error, "terminal writer stopped");
                }
                break;
            },
            output = pty_rx.recv() => match output {
                Some(Ok(bytes)) => {
                    if socket_tx.send(Message::Binary(bytes.into())).await.is_err() {
                        break;
                    }
                }
                Some(Err(error)) => {
                    let message = error.to_string();
                    let event = session_event_message(&SessionEvent::Error { message: &message });
                    let _ = socket_tx.send(event).await;
                    break;
                }
                None => {
                    let _ = socket_tx
                        .send(session_event_message(&SessionEvent::Exited))
                        .await;
                    break;
                }
            },
            incoming = socket_rx.next() => match incoming {
                Some(Ok(Message::Binary(bytes))) => {
                    if input_tx.send(bytes.to_vec()).await.is_err() {
                        break;
                    }
                }
                Some(Ok(Message::Text(text))) => {
                    let Ok(ClientControl::Resize { cols, rows }) = serde_json::from_str(&text) else {
                        continue;
                    };
                    let Ok(size) = (TerminalSize { cols, rows }).validated() else {
                        continue;
                    };
                    if session.resize(size).is_err() {
                        break;
                    }
                    let resized = session_event_message(&SessionEvent::Resized {
                        cols: size.cols,
                        rows: size.rows,
                    });
                    if socket_tx.send(resized).await.is_err() {
                        break;
                    }
                }
                Some(Ok(Message::Close(_)) | Err(_)) | None => break,
                Some(Ok(Message::Ping(data))) => {
                    if socket_tx.send(Message::Pong(data)).await.is_err() {
                        break;
                    }
                }
                Some(Ok(Message::Pong(_))) => {}
            }
        }
    }

    drop(input_tx);
    session.terminate();
    read_task.abort();
    write_task.abort();
}

async fn send_open_error(mut socket: WebSocket, message: &str) {
    let event = session_event_message(&SessionEvent::Error { message });
    let _ = socket.send(event).await;
    let _ = socket.close().await;
}

fn generate_token() -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = rand::rng().random::<[u8; 32]>();
    let mut token = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        token.push(HEX[usize::from(byte >> 4)] as char);
        token.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    token
}

/// Errors produced while opening or operating a terminal session.
#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    /// The configured shell executable does not exist.
    #[error("visual shell binary not found at {0}")]
    ShellNotFound(PathBuf),
    /// The requested terminal dimensions were unsafe or nonsensical.
    #[error("terminal size must be between 1 and 500 rows and columns")]
    InvalidSize,
    /// A session resource was already consumed or became unavailable.
    #[error("terminal session became unavailable")]
    SessionUnavailable,
    /// Operating-system PTY failure.
    #[error("PTY operation failed: {0}")]
    Pty(#[from] anyhow::Error),
    /// Terminal I/O failure.
    #[error("terminal I/O failed: {0}")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_sizes_are_bounded() {
        assert!(TerminalSize { cols: 80, rows: 24 }.validated().is_ok());
        assert!(TerminalSize { cols: 0, rows: 24 }.validated().is_err());
        assert!(
            TerminalSize {
                cols: 80,
                rows: 501
            }
            .validated()
            .is_err()
        );
    }

    #[test]
    fn same_origin_requires_the_bound_host_and_websocket_origin() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("127.0.0.1:4100"));
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("http://127.0.0.1:4100"),
        );
        assert!(request_is_same_origin(
            &headers,
            "http://127.0.0.1:4100",
            true
        ));

        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("https://attacker.example"),
        );
        assert!(!request_is_same_origin(
            &headers,
            "http://127.0.0.1:4100",
            true
        ));
    }

    #[test]
    fn same_origin_token_fetch_accepts_the_bound_referer() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("127.0.0.1:4100"));
        headers.insert(
            header::REFERER,
            HeaderValue::from_static("http://127.0.0.1:4100/workbench"),
        );

        assert!(request_is_same_origin(
            &headers,
            "http://127.0.0.1:4100",
            true
        ));
        assert!(!request_is_same_origin(
            &headers,
            "http://127.0.0.1:4100",
            false
        ));

        headers.insert(
            header::REFERER,
            HeaderValue::from_static("https://attacker.example/workbench"),
        );
        assert!(!request_is_same_origin(
            &headers,
            "http://127.0.0.1:4100",
            true
        ));
    }

    #[test]
    fn terminal_upgrade_requires_the_process_token() {
        let config = ServerConfig {
            assets: PathBuf::new(),
            origin: "http://127.0.0.1:4100".to_owned(),
            token: "process-secret".to_owned(),
        };
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("127.0.0.1:4100"));
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("http://127.0.0.1:4100"),
        );
        let auth_protocol = format!("{AUTH_PROTOCOL_PREFIX}{}", config.token);
        headers.insert(
            header::SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_static("agent-lab.auth.wrong-token"),
        );

        assert!(!terminal_request_is_authorized(
            &headers,
            &auth_protocol,
            &config
        ));
        headers.insert(
            header::SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_str(&format!("unused, {auth_protocol}")).unwrap(),
        );
        assert!(terminal_request_is_authorized(
            &headers,
            &auth_protocol,
            &config
        ));
    }
}
