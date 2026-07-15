use std::{
    ffi::OsStr,
    io::{BufRead, BufReader, Read, Write},
    path::Path,
    process::{Child, ChildStdin, Command, Stdio},
    sync::{Arc, Mutex, mpsc},
    thread,
    time::{Duration, Instant},
};

use thiserror::Error;

use crate::{ControllerCommand, DriverMessage, PROTOCOL_VERSION};

#[derive(Debug, Clone, PartialEq)]
pub struct RawDriverMessage {
    pub raw: Vec<u8>,
    pub parsed: DriverMessage,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DriverTranscript {
    pub controller_records: Vec<Vec<u8>>,
    pub driver_records: Vec<Vec<u8>>,
    pub driver_stderr: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum ProcessError {
    #[error("failed to spawn driver: {0}")]
    Spawn(String),
    #[error("failed to encode controller command: {0}")]
    Encode(#[from] serde_json::Error),
    #[error("failed to write controller command: {0}")]
    Write(String),
    #[error("driver output reader failed: {0}")]
    Read(String),
    #[error("driver emitted malformed JSON: {message}; raw={raw:?}")]
    MalformedOutput { raw: Vec<u8>, message: String },
    #[error("driver emitted protocol version {actual}; expected {expected}")]
    UnsupportedVersion { expected: u32, actual: u32 },
    #[error("driver sequence was not contiguous: expected={expected}, actual={actual}")]
    UnexpectedSequence { expected: u64, actual: u64 },
    #[error("timed out waiting for driver output")]
    Timeout,
    #[error("driver stdout closed unexpectedly with exit code {code:?}")]
    UnexpectedExit { code: Option<i32> },
    #[error("driver output reader stopped unexpectedly")]
    ReaderStopped,
    #[error("timed out waiting for driver process to exit")]
    ExitTimeout,
}

enum ReaderItem {
    Line(Vec<u8>),
    Error(String),
    Eof,
}

pub struct DriverProcess {
    child: Child,
    stdin: ChildStdin,
    output: mpsc::Receiver<ReaderItem>,
    stderr: Arc<Mutex<Vec<u8>>>,
    sent: Vec<Vec<u8>>,
    received: Vec<Vec<u8>>,
    last_sequence: u64,
}

impl DriverProcess {
    /// Spawn a driver with piped stdio.
    ///
    /// # Errors
    ///
    /// Returns an error when the process or any required pipe cannot be opened.
    pub fn spawn(
        executable: impl AsRef<Path>,
        args: impl IntoIterator<Item = impl AsRef<OsStr>>,
    ) -> Result<Self, ProcessError> {
        let mut child = Command::new(executable.as_ref())
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| ProcessError::Spawn(error.to_string()))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| ProcessError::Spawn("driver stdin was not piped".to_owned()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| ProcessError::Spawn("driver stdout was not piped".to_owned()))?;
        let child_stderr = child
            .stderr
            .take()
            .ok_or_else(|| ProcessError::Spawn("driver stderr was not piped".to_owned()))?;

        let (sender, output) = mpsc::channel();
        thread::Builder::new()
            .name("agent-lab-driver-stdout".to_owned())
            .spawn(move || read_stdout(stdout, &sender))
            .map_err(|error| ProcessError::Spawn(error.to_string()))?;

        let stderr = Arc::new(Mutex::new(Vec::new()));
        let stderr_writer = stderr.clone();
        thread::Builder::new()
            .name("agent-lab-driver-stderr".to_owned())
            .spawn(move || {
                let mut reader = BufReader::new(child_stderr);
                let mut chunk = [0_u8; 4096];
                while let Ok(count) = reader.read(&mut chunk) {
                    if count == 0 {
                        break;
                    }
                    stderr_writer
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .extend_from_slice(&chunk[..count]);
                }
            })
            .map_err(|error| ProcessError::Spawn(error.to_string()))?;

        Ok(Self {
            child,
            stdin,
            output,
            stderr,
            sent: Vec::new(),
            received: Vec::new(),
            last_sequence: 0,
        })
    }

    #[must_use]
    pub fn process_id(&self) -> u32 {
        self.child.id()
    }

    #[must_use]
    pub fn sent_records(&self) -> &[Vec<u8>] {
        &self.sent
    }

    #[must_use]
    pub fn stderr(&self) -> Vec<u8> {
        self.stderr
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    #[must_use]
    pub fn transcript(&self) -> DriverTranscript {
        DriverTranscript {
            controller_records: self.sent.clone(),
            driver_records: self.received.clone(),
            driver_stderr: self.stderr(),
        }
    }

    /// Send one controller command as a complete JSON Lines record.
    ///
    /// # Errors
    ///
    /// Returns an error when serialization or the driver stdin pipe fails.
    pub fn send(&mut self, command: &ControllerCommand) -> Result<(), ProcessError> {
        let mut raw = serde_json::to_vec(command)?;
        raw.push(b'\n');
        self.stdin
            .write_all(&raw)
            .and_then(|()| self.stdin.flush())
            .map_err(|error| ProcessError::Write(error.to_string()))?;
        self.sent.push(raw);
        Ok(())
    }

    /// Receive and validate one driver message while retaining the exact bytes.
    ///
    /// # Errors
    ///
    /// Returns distinct errors for timeout, reader failure, malformed output,
    /// protocol mismatch, sequence violation, and unexpected process exit.
    pub fn receive(&mut self, timeout: Duration) -> Result<RawDriverMessage, ProcessError> {
        match self.output.recv_timeout(timeout) {
            Ok(ReaderItem::Line(raw)) => {
                self.received.push(raw.clone());
                let parsed = serde_json::from_slice::<DriverMessage>(&raw).map_err(|error| {
                    ProcessError::MalformedOutput {
                        raw: raw.clone(),
                        message: error.to_string(),
                    }
                })?;
                if parsed.protocol_version != PROTOCOL_VERSION {
                    return Err(ProcessError::UnsupportedVersion {
                        expected: PROTOCOL_VERSION,
                        actual: parsed.protocol_version,
                    });
                }
                let expected_sequence = self.last_sequence + 1;
                if parsed.sequence != expected_sequence {
                    return Err(ProcessError::UnexpectedSequence {
                        expected: expected_sequence,
                        actual: parsed.sequence,
                    });
                }
                self.last_sequence = parsed.sequence;
                Ok(RawDriverMessage { raw, parsed })
            }
            Ok(ReaderItem::Error(error)) => Err(ProcessError::Read(error)),
            Ok(ReaderItem::Eof) => {
                let status = self
                    .child
                    .wait()
                    .map_err(|error| ProcessError::Read(error.to_string()))?;
                Err(ProcessError::UnexpectedExit {
                    code: status.code(),
                })
            }
            Err(mpsc::RecvTimeoutError::Timeout) => Err(ProcessError::Timeout),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(ProcessError::ReaderStopped),
        }
    }

    /// Wait for a driver that has performed a protocol-level clean shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error when polling the child fails or the deadline expires.
    pub fn wait_for_exit(&mut self, timeout: Duration) -> Result<Option<i32>, ProcessError> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self
                .child
                .try_wait()
                .map_err(|error| ProcessError::Read(error.to_string()))?
            {
                return Ok(status.code());
            }
            if Instant::now() >= deadline {
                return Err(ProcessError::ExitTimeout);
            }
            thread::sleep(Duration::from_millis(5));
        }
    }
}

impl Drop for DriverProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn read_stdout(stdout: impl Read, sender: &mpsc::Sender<ReaderItem>) {
    let mut reader = BufReader::new(stdout);
    loop {
        let mut raw = Vec::new();
        match reader.read_until(b'\n', &mut raw) {
            Ok(0) => {
                let _ = sender.send(ReaderItem::Eof);
                return;
            }
            Ok(_) => {
                if sender.send(ReaderItem::Line(raw)).is_err() {
                    return;
                }
            }
            Err(error) => {
                let _ = sender.send(ReaderItem::Error(error.to_string()));
                return;
            }
        }
    }
}
