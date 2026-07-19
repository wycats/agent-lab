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

pub const MAX_DRIVER_RECORD_BYTES: usize = 1024 * 1024;
const OUTPUT_QUEUE_CAPACITY: usize = 32;

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
    #[error("driver emitted a JSON Lines record without a trailing newline: raw={raw:?}")]
    UnterminatedOutput { raw: Vec<u8> },
    #[error("driver output record exceeded the {limit}-byte limit")]
    OutputLimitExceeded { limit: usize },
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
    OutputLimitExceeded,
    Eof,
}

enum ReaderCompletion {
    Stdout,
    Stderr,
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
    reader_completion: mpsc::Receiver<ReaderCompletion>,
    stderr: Arc<Mutex<Vec<u8>>>,
    stdout_reader: Option<thread::JoinHandle<Result<(), String>>>,
    stderr_reader: Option<thread::JoinHandle<Result<(), String>>>,
    sent: Vec<Vec<u8>>,
    received: Vec<Vec<u8>>,
    last_sequence: u64,
    stdout_eof: bool,
    stdout_complete: bool,
    stderr_complete: bool,
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

        let (sender, output) = mpsc::sync_channel(OUTPUT_QUEUE_CAPACITY);
        let (completion_sender, reader_completion) = mpsc::channel();
        let stdout_completion = completion_sender.clone();
        let stdout_reader = match thread::Builder::new()
            .name("agent-lab-driver-stdout".to_owned())
            .spawn(move || {
                let result = read_stdout(stdout, &sender);
                let _ = stdout_completion.send(ReaderCompletion::Stdout);
                result
            }) {
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
            .spawn(move || {
                let result = read_stderr(child_stderr, &stderr_writer);
                let _ = completion_sender.send(ReaderCompletion::Stderr);
                result
            }) {
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
            reader_completion,
            stderr,
            stdout_reader: Some(stdout_reader),
            stderr_reader: Some(stderr_reader),
            sent: Vec::new(),
            received: Vec::new(),
            last_sequence: 0,
            stdout_eof: false,
            stdout_complete: false,
            stderr_complete: false,
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
                if !raw.ends_with(b"\n") {
                    return Err(ProcessError::UnterminatedOutput { raw });
                }
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
            Ok(ReaderItem::OutputLimitExceeded) => Err(ProcessError::OutputLimitExceeded {
                limit: MAX_DRIVER_RECORD_BYTES,
            }),
            Ok(ReaderItem::Eof) => {
                self.stdout_eof = true;
                self.unexpected_exit_before(deadline)
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
                self.finish_after_exit(deadline, true)?;
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

    fn finish_after_exit(
        &mut self,
        deadline: Instant,
        exit_timeout: bool,
    ) -> Result<(), ProcessError> {
        // The driver may have exited while descendants still own its inherited
        // pipes. Terminating the process group/job makes reader completion part
        // of the same bounded lifecycle.
        let _ = self.child.start_kill();
        let pending = self.drain_stdout_before(deadline, exit_timeout);
        self.wait_for_readers_before(deadline, exit_timeout)?;
        let joined = self.join_readers();
        match pending {
            Err(error) => Err(error),
            Ok(()) => joined,
        }
    }

    fn drain_stdout_before(
        &mut self,
        deadline: Instant,
        exit_timeout: bool,
    ) -> Result<(), ProcessError> {
        let mut pending = None;
        while !self.stdout_eof {
            let remaining = remaining_before(deadline, exit_timeout)?;
            match self.output.recv_timeout(remaining) {
                Ok(ReaderItem::Line(raw)) => {
                    self.received.push(raw.clone());
                    if !raw.ends_with(b"\n") && pending.is_none() {
                        pending = Some(ProcessError::UnterminatedOutput { raw });
                    }
                }
                Ok(ReaderItem::Error(error)) => {
                    pending.get_or_insert(ProcessError::Read(error));
                }
                Ok(ReaderItem::OutputLimitExceeded) => {
                    pending.get_or_insert(ProcessError::OutputLimitExceeded {
                        limit: MAX_DRIVER_RECORD_BYTES,
                    });
                }
                Ok(ReaderItem::Eof) => self.stdout_eof = true,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    return Err(deadline_error(exit_timeout));
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    if pending.is_none() {
                        pending = Some(ProcessError::ReaderStopped);
                    }
                    break;
                }
            }
        }
        pending.map_or(Ok(()), Err)
    }

    fn wait_for_readers_before(
        &mut self,
        deadline: Instant,
        exit_timeout: bool,
    ) -> Result<(), ProcessError> {
        while !(self.stdout_complete && self.stderr_complete) {
            let remaining = remaining_before(deadline, exit_timeout)?;
            match self.reader_completion.recv_timeout(remaining) {
                Ok(ReaderCompletion::Stdout) => self.stdout_complete = true,
                Ok(ReaderCompletion::Stderr) => self.stderr_complete = true,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    return Err(deadline_error(exit_timeout));
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(ProcessError::ReaderStopped);
                }
            }
        }
        Ok(())
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
                self.finish_after_exit(deadline, false)?;
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
        if self.stdout_reader.is_some() || self.stderr_reader.is_some() {
            let _ = self.child.start_kill();
            let deadline = Instant::now() + Duration::from_millis(100);
            while Instant::now() < deadline {
                match self.child.try_wait() {
                    Ok(Some(_)) | Err(_) => break,
                    Ok(None) => thread::sleep(Duration::from_millis(1)),
                }
            }
            // Join handles are intentionally detached here. The public wait
            // path bounds and joins them; Drop must never block on an escaped
            // descendant that kept an inherited pipe open.
            self.stdout_reader.take();
            self.stderr_reader.take();
        }
    }
}

fn remaining_before(deadline: Instant, exit_timeout: bool) -> Result<Duration, ProcessError> {
    let now = Instant::now();
    if now >= deadline {
        Err(deadline_error(exit_timeout))
    } else {
        Ok(deadline - now)
    }
}

fn deadline_error(exit_timeout: bool) -> ProcessError {
    if exit_timeout {
        ProcessError::ExitTimeout
    } else {
        ProcessError::Timeout
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

fn read_stdout(stdout: impl Read, sender: &mpsc::SyncSender<ReaderItem>) -> Result<(), String> {
    let mut reader = BufReader::new(stdout);
    loop {
        let mut raw = Vec::new();
        let mut bounded = (&mut reader).take((MAX_DRIVER_RECORD_BYTES + 1) as u64);
        match bounded.read_until(b'\n', &mut raw) {
            Ok(0) => {
                let _ = sender.send(ReaderItem::Eof);
                return Ok(());
            }
            Ok(_) if raw.len() > MAX_DRIVER_RECORD_BYTES => {
                let _ = sender.send(ReaderItem::OutputLimitExceeded);
                return Err(format!(
                    "driver output record exceeded the {MAX_DRIVER_RECORD_BYTES}-byte limit"
                ));
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
