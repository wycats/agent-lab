use std::{
    ffi::{OsStr, OsString},
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    process::{ChildStdin, Command, Stdio},
    sync::{Arc, Mutex, mpsc},
    thread,
    time::{Duration, Instant},
};

#[cfg(windows)]
use process_wrap::std::JobObject;
#[cfg(unix)]
use process_wrap::std::ProcessGroup;
use process_wrap::std::{ChildWrapper, CommandWrap};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{ControllerCommand, DriverMessage, PROTOCOL_VERSION};

#[derive(Debug, Clone, PartialEq)]
pub struct RawDriverMessage {
    pub raw: Vec<u8>,
    pub parsed: DriverMessage,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
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

/// Explicit process launch configuration supplied by the Agent Lab host.
#[derive(Debug, Clone)]
pub struct DriverLaunch {
    pub executable: PathBuf,
    pub args: Vec<OsString>,
    pub cwd: Option<PathBuf>,
    pub env: Vec<(OsString, OsString)>,
    pub clear_env: bool,
}

impl DriverLaunch {
    #[must_use]
    pub fn new(executable: impl Into<PathBuf>) -> Self {
        Self {
            executable: executable.into(),
            args: Vec::new(),
            cwd: None,
            env: Vec::new(),
            clear_env: false,
        }
    }
}

pub struct DriverProcess {
    child: Box<dyn ChildWrapper>,
    stdin: ChildStdin,
    output: mpsc::Receiver<ReaderItem>,
    stderr: Arc<Mutex<Vec<u8>>>,
    stdout_reader: Option<thread::JoinHandle<Result<(), String>>>,
    stderr_reader: Option<thread::JoinHandle<Result<(), String>>>,
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
        let mut launch = DriverLaunch::new(executable.as_ref());
        launch.args = args
            .into_iter()
            .map(|arg| arg.as_ref().to_owned())
            .collect();
        Self::spawn_with(launch)
    }

    /// Spawn a driver with an explicit working directory and environment.
    ///
    /// # Errors
    ///
    /// Returns an error when the process or any required pipe cannot be opened.
    pub fn spawn_with(launch: DriverLaunch) -> Result<Self, ProcessError> {
        let mut command = Command::new(&launch.executable);
        command.args(&launch.args);
        if let Some(cwd) = &launch.cwd {
            command.current_dir(cwd);
        }
        if launch.clear_env {
            command.env_clear();
        }
        command.envs(launch.env);
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut command = CommandWrap::from(command);
        #[cfg(unix)]
        command.wrap(ProcessGroup::leader());
        #[cfg(windows)]
        command.wrap(JobObject);
        let mut child = command
            .spawn()
            .map_err(|error| ProcessError::Spawn(error.to_string()))?;
        let stdin = child
            .stdin()
            .take()
            .ok_or_else(|| ProcessError::Spawn("driver stdin was not piped".to_owned()))?;
        let stdout = child
            .stdout()
            .take()
            .ok_or_else(|| ProcessError::Spawn("driver stdout was not piped".to_owned()))?;
        let child_stderr = child
            .stderr()
            .take()
            .ok_or_else(|| ProcessError::Spawn("driver stderr was not piped".to_owned()))?;

        let (sender, output) = mpsc::channel();
        let stdout_reader = match thread::Builder::new()
            .name("agent-lab-driver-stdout".to_owned())
            .spawn(move || read_stdout(stdout, &sender))
        {
            Ok(reader) => reader,
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(ProcessError::Spawn(error.to_string()));
            }
        };

        let stderr = Arc::new(Mutex::new(Vec::new()));
        let stderr_writer = stderr.clone();
        let stderr_reader = match thread::Builder::new()
            .name("agent-lab-driver-stderr".to_owned())
            .spawn(move || read_stderr(child_stderr, &stderr_writer))
        {
            Ok(reader) => reader,
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = stdout_reader.join();
                return Err(ProcessError::Spawn(error.to_string()));
            }
        };

        Ok(Self {
            child,
            stdin,
            output,
            stderr,
            stdout_reader: Some(stdout_reader),
            stderr_reader: Some(stderr_reader),
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
        let deadline = Instant::now() + timeout;
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
            Ok(ReaderItem::Eof) => self.unexpected_exit_before(deadline),
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
                self.join_readers()?;
                return Ok(status.code());
            }
            if Instant::now() >= deadline {
                return Err(ProcessError::ExitTimeout);
            }
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn join_readers(&mut self) -> Result<(), ProcessError> {
        let stdout = join_reader(self.stdout_reader.take(), "stdout");
        let stderr = join_reader(self.stderr_reader.take(), "stderr");
        stdout.and(stderr)
    }

    fn unexpected_exit_before(
        &mut self,
        deadline: Instant,
    ) -> Result<RawDriverMessage, ProcessError> {
        loop {
            if let Some(status) = self
                .child
                .try_wait()
                .map_err(|error| ProcessError::Read(error.to_string()))?
            {
                self.join_readers()?;
                return Err(ProcessError::UnexpectedExit {
                    code: status.code(),
                });
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(ProcessError::Timeout);
            }
            thread::sleep((deadline - now).min(Duration::from_millis(5)));
        }
    }
}

impl Drop for DriverProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        let _ = self.join_readers();
    }
}

fn join_reader(
    reader: Option<thread::JoinHandle<Result<(), String>>>,
    stream: &str,
) -> Result<(), ProcessError> {
    match reader.map(thread::JoinHandle::join) {
        None | Some(Ok(Ok(()))) => Ok(()),
        Some(Ok(Err(error))) => Err(ProcessError::Read(format!("driver {stream}: {error}"))),
        Some(Err(_)) => Err(ProcessError::ReaderStopped),
    }
}

fn read_stdout(stdout: impl Read, sender: &mpsc::Sender<ReaderItem>) -> Result<(), String> {
    let mut reader = BufReader::new(stdout);
    loop {
        let mut raw = Vec::new();
        match reader.read_until(b'\n', &mut raw) {
            Ok(0) => {
                let _ = sender.send(ReaderItem::Eof);
                return Ok(());
            }
            Ok(_) => {
                if sender.send(ReaderItem::Line(raw)).is_err() {
                    return Ok(());
                }
            }
            Err(error) => {
                let _ = sender.send(ReaderItem::Error(error.to_string()));
                return Err(error.to_string());
            }
        }
    }
}

fn read_stderr(stderr: impl Read, destination: &Mutex<Vec<u8>>) -> Result<(), String> {
    let mut reader = BufReader::new(stderr);
    let mut chunk = [0_u8; 4096];
    loop {
        let count = reader.read(&mut chunk).map_err(|error| error.to_string())?;
        if count == 0 {
            return Ok(());
        }
        destination
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend_from_slice(&chunk[..count]);
    }
}
