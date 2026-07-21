use std::{
    collections::{BTreeMap, BTreeSet},
    fs, io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use thiserror::Error;

use crate::{
    CommandBody, ControllerCommand, DriverBody, DriverDescriptor, DriverFailureScope,
    DriverMessage, DriverTranscript, PROTOCOL_VERSION,
};

pub const EVIDENCE_SCHEMA_VERSION: u32 = 1;

static STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CanonicalizationPolicy {
    pub name: String,
    pub removed_object_keys: BTreeSet<String>,
}

impl CanonicalizationPolicy {
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        removed_object_keys: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            name: name.into(),
            removed_object_keys: removed_object_keys.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CanonicalProjection {
    pub policy: CanonicalizationPolicy,
    pub driver_records: Vec<JsonValue>,
}

/// Small index for a durable evidence directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EvidenceManifest {
    pub schema_version: u32,
    pub controller_revision: Option<String>,
    pub driver: DriverDescriptor,
    pub process_id: u32,
    pub canonicalization: CanonicalizationPolicy,
    pub controller_record_count: usize,
    pub driver_record_count: usize,
    pub stderr_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DriverEvidenceBundle {
    pub controller_revision: Option<String>,
    pub driver: DriverDescriptor,
    pub process_id: u32,
    pub transcript: DriverTranscript,
    pub canonical: CanonicalProjection,
}

impl DriverEvidenceBundle {
    /// Construct a bundle that preserves raw records and derives a named
    /// canonical comparison projection from them.
    ///
    /// # Errors
    ///
    /// Returns an error if retained records or their declared identities are
    /// invalid or inconsistent.
    pub fn new(
        controller_revision: Option<String>,
        driver: DriverDescriptor,
        process_id: u32,
        transcript: DriverTranscript,
        policy: CanonicalizationPolicy,
    ) -> Result<Self, EvidenceError> {
        let canonical = canonicalize_driver_records(&transcript, policy)?;
        let bundle = Self {
            controller_revision,
            driver,
            process_id,
            transcript,
            canonical,
        };
        validate_bundle(&bundle)?;
        Ok(bundle)
    }

    #[must_use]
    pub fn manifest(&self) -> EvidenceManifest {
        EvidenceManifest {
            schema_version: EVIDENCE_SCHEMA_VERSION,
            controller_revision: self.controller_revision.clone(),
            driver: self.driver.clone(),
            process_id: self.process_id,
            canonicalization: self.canonical.policy.clone(),
            controller_record_count: self.transcript.controller_records.len(),
            driver_record_count: self.transcript.driver_records.len(),
            stderr_bytes: self.transcript.driver_stderr.len(),
        }
    }

    /// Atomically finalize an inspectable evidence directory.
    ///
    /// The directory contains `manifest.json`, exact controller and driver
    /// JSON Lines transcripts, driver stderr, and the named canonical
    /// projection. The target must not already exist.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid retained records, an inconsistent
    /// canonical projection, filesystem failures, or an existing target.
    pub fn write_to_dir(&self, target: impl AsRef<Path>) -> Result<(), EvidenceError> {
        validate_bundle(self)?;
        let target = target.as_ref();
        if target.exists() {
            return Err(EvidenceError::AlreadyExists(target.to_path_buf()));
        }
        let parent = target.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        let name = target
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                EvidenceError::InvalidBundle("evidence target needs a file name".to_owned())
            })?;
        let staging = parent.join(format!(
            ".{name}.tmp-{}-{}",
            std::process::id(),
            STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&staging)?;

        let result = (|| -> Result<(), EvidenceError> {
            write_json(&staging.join("manifest.json"), &self.manifest())?;
            write_records(
                &staging.join("controller.jsonl"),
                &self.transcript.controller_records,
            )?;
            write_records(
                &staging.join("driver.jsonl"),
                &self.transcript.driver_records,
            )?;
            fs::write(
                staging.join("driver.stderr.log"),
                &self.transcript.driver_stderr,
            )?;
            write_json(&staging.join("canonical.json"), &self.canonical)?;
            finalize_no_replace(&staging, target)?;
            Ok(())
        })();

        if result.is_err() {
            let _ = fs::remove_dir_all(&staging);
        }
        result
    }

    /// Reopen and verify a durable evidence directory without running a driver.
    ///
    /// # Errors
    ///
    /// Returns an error when files are missing, records are malformed, counts
    /// disagree, or the stored canonical projection cannot be reproduced.
    pub fn read_from_dir(root: impl AsRef<Path>) -> Result<Self, EvidenceError> {
        let root = root.as_ref();
        let manifest: EvidenceManifest = read_json(&root.join("manifest.json"))?;
        if manifest.schema_version != EVIDENCE_SCHEMA_VERSION {
            return Err(EvidenceError::InvalidBundle(format!(
                "unsupported evidence schema version {}",
                manifest.schema_version
            )));
        }
        let transcript = DriverTranscript {
            controller_records: read_records(&root.join("controller.jsonl"))?,
            driver_records: read_records(&root.join("driver.jsonl"))?,
            driver_stderr: fs::read(root.join("driver.stderr.log"))?,
        };
        let canonical: CanonicalProjection = read_json(&root.join("canonical.json"))?;
        let bundle = Self {
            controller_revision: manifest.controller_revision.clone(),
            driver: manifest.driver.clone(),
            process_id: manifest.process_id,
            transcript,
            canonical,
        };
        if bundle.manifest() != manifest {
            return Err(EvidenceError::InvalidBundle(
                "manifest counts or identities do not match retained evidence".to_owned(),
            ));
        }
        validate_bundle(&bundle)?;
        Ok(bundle)
    }
}

#[derive(Debug, Error)]
pub enum EvidenceError {
    #[error("evidence target already exists: {0}")]
    AlreadyExists(PathBuf),
    #[error("invalid evidence bundle: {0}")]
    InvalidBundle(String),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

fn validate_bundle(bundle: &DriverEvidenceBundle) -> Result<(), EvidenceError> {
    let controller_records =
        validate_records::<ControllerCommand>("controller", &bundle.transcript.controller_records)?;
    let lifecycle = validate_controller_records(&controller_records)?;
    let driver_records =
        validate_records::<DriverMessage>("driver", &bundle.transcript.driver_records)?;
    validate_driver_records(bundle, &driver_records, &lifecycle)?;
    let expected =
        canonicalize_driver_records(&bundle.transcript, bundle.canonical.policy.clone())?;
    if expected != bundle.canonical {
        return Err(EvidenceError::InvalidBundle(
            "canonical projection does not match the retained driver records".to_owned(),
        ));
    }
    Ok(())
}

struct ControllerLifecycle {
    commands: BTreeMap<String, CommandIdentity>,
    sessions: BTreeSet<String>,
    turns: BTreeSet<(String, String)>,
}

impl ControllerLifecycle {
    fn retain_command(
        &mut self,
        record: &ControllerCommand,
        record_number: usize,
    ) -> Result<(), EvidenceError> {
        if record.protocol_version != PROTOCOL_VERSION {
            return Err(EvidenceError::InvalidBundle(format!(
                "controller record {record_number} has protocol version {}; expected {PROTOCOL_VERSION}",
                record.protocol_version
            )));
        }
        if self
            .commands
            .insert(
                record.message_id.clone(),
                CommandIdentity::from_body(&record.body),
            )
            .is_some()
        {
            return Err(EvidenceError::InvalidBundle(format!(
                "controller record {record_number} repeats message ID {}",
                record.message_id
            )));
        }
        Ok(())
    }
}

enum CommandIdentity {
    OpenSession { session_id: String },
    StartTurn { session_id: String, turn_id: String },
    AbortTurn { session_id: String, turn_id: String },
    CloseSession { session_id: String },
}

impl CommandIdentity {
    fn from_body(body: &CommandBody) -> Self {
        match body {
            CommandBody::OpenSession { session_id, .. } => Self::OpenSession {
                session_id: session_id.clone(),
            },
            CommandBody::StartTurn {
                session_id,
                turn_id,
                ..
            } => Self::StartTurn {
                session_id: session_id.clone(),
                turn_id: turn_id.clone(),
            },
            CommandBody::AbortTurn {
                session_id,
                turn_id,
                ..
            } => Self::AbortTurn {
                session_id: session_id.clone(),
                turn_id: turn_id.clone(),
            },
            CommandBody::CloseSession { session_id } => Self::CloseSession {
                session_id: session_id.clone(),
            },
        }
    }

    fn session_id(&self) -> &str {
        match self {
            Self::OpenSession { session_id }
            | Self::StartTurn { session_id, .. }
            | Self::AbortTurn { session_id, .. }
            | Self::CloseSession { session_id } => session_id,
        }
    }

    fn matches_turn(&self, expected_session: &str, expected_turn: &str) -> bool {
        matches!(
            self,
            Self::StartTurn {
                session_id,
                turn_id,
            } | Self::AbortTurn {
                session_id,
                turn_id,
            } if session_id == expected_session && turn_id == expected_turn
        )
    }
}

fn command_causes_driver_body(command: &CommandIdentity, body: &DriverBody) -> bool {
    match body {
        DriverBody::StartupEvent { .. } => true,
        DriverBody::Ready { .. } => false,
        DriverBody::SessionOpened { session_id, .. } => matches!(
            command,
            CommandIdentity::OpenSession {
                session_id: expected,
            } if expected == session_id
        ),
        DriverBody::TurnEvent {
            session_id,
            turn_id,
            ..
        }
        | DriverBody::TurnFinished {
            session_id,
            turn_id,
            ..
        } => command.matches_turn(session_id, turn_id),
        DriverBody::SessionClosed { session_id } => matches!(
            command,
            CommandIdentity::CloseSession {
                session_id: expected,
            } if expected == session_id
        ),
        DriverBody::Failed {
            scope,
            session_id,
            turn_id,
            ..
        } => match scope {
            DriverFailureScope::Driver | DriverFailureScope::Protocol => true,
            DriverFailureScope::Session => session_id
                .as_deref()
                .is_some_and(|session_id| command.session_id() == session_id),
            DriverFailureScope::Turn => session_id.as_deref().is_some_and(|session_id| {
                turn_id
                    .as_deref()
                    .is_some_and(|turn_id| command.matches_turn(session_id, turn_id))
            }),
        },
    }
}

fn validate_controller_records(
    records: &[ControllerCommand],
) -> Result<ControllerLifecycle, EvidenceError> {
    let mut lifecycle = ControllerLifecycle {
        commands: BTreeMap::new(),
        sessions: BTreeSet::new(),
        turns: BTreeSet::new(),
    };
    let mut closed_sessions = BTreeSet::new();
    for (index, record) in records.iter().enumerate() {
        lifecycle.retain_command(record, index + 1)?;
        match &record.body {
            CommandBody::OpenSession { session_id, .. } => {
                if !lifecycle.sessions.insert(session_id.clone()) {
                    return Err(EvidenceError::InvalidBundle(format!(
                        "controller record {} opens duplicate session {session_id}",
                        index + 1
                    )));
                }
            }
            CommandBody::StartTurn {
                session_id,
                turn_id,
                ..
            } => {
                if !lifecycle.sessions.contains(session_id) {
                    return Err(EvidenceError::InvalidBundle(format!(
                        "controller record {} starts a turn for unopened session {session_id}",
                        index + 1
                    )));
                }
                if closed_sessions.contains(session_id) {
                    return Err(EvidenceError::InvalidBundle(format!(
                        "controller record {} starts a turn after session {session_id} closed",
                        index + 1
                    )));
                }
                if !lifecycle
                    .turns
                    .insert((session_id.clone(), turn_id.clone()))
                {
                    return Err(EvidenceError::InvalidBundle(format!(
                        "controller record {} starts duplicate turn {session_id}/{turn_id}",
                        index + 1
                    )));
                }
            }
            CommandBody::AbortTurn {
                session_id,
                turn_id,
                ..
            } => {
                if closed_sessions.contains(session_id) {
                    return Err(EvidenceError::InvalidBundle(format!(
                        "controller record {} aborts a turn after session {session_id} closed",
                        index + 1
                    )));
                }
                if !lifecycle
                    .turns
                    .contains(&(session_id.clone(), turn_id.clone()))
                {
                    return Err(EvidenceError::InvalidBundle(format!(
                        "controller record {} aborts unknown turn {session_id}/{turn_id}",
                        index + 1
                    )));
                }
            }
            CommandBody::CloseSession { session_id } => {
                if !lifecycle.sessions.contains(session_id) {
                    return Err(EvidenceError::InvalidBundle(format!(
                        "controller record {} references unopened session {session_id}",
                        index + 1
                    )));
                }
                if !closed_sessions.insert(session_id.clone()) {
                    return Err(EvidenceError::InvalidBundle(format!(
                        "controller record {} closes duplicate session {session_id}",
                        index + 1
                    )));
                }
            }
        }
    }
    if lifecycle.sessions != closed_sessions {
        return Err(EvidenceError::InvalidBundle(
            "controller transcript does not close every opened session".to_owned(),
        ));
    }
    Ok(lifecycle)
}

fn validate_records<T>(kind: &str, records: &[Vec<u8>]) -> Result<Vec<T>, EvidenceError>
where
    T: for<'de> Deserialize<'de>,
{
    let mut parsed = Vec::with_capacity(records.len());
    for (index, record) in records.iter().enumerate() {
        if !record.ends_with(b"\n") {
            return Err(EvidenceError::InvalidBundle(format!(
                "{kind} record {} is not newline terminated",
                index + 1
            )));
        }
        parsed.push(serde_json::from_slice::<T>(record).map_err(|error| {
            EvidenceError::InvalidBundle(format!("{kind} record {} is invalid: {error}", index + 1))
        })?);
    }
    Ok(parsed)
}

fn validate_driver_records(
    bundle: &DriverEvidenceBundle,
    records: &[DriverMessage],
    lifecycle: &ControllerLifecycle,
) -> Result<(), EvidenceError> {
    let mut state = DriverLifecycleState::default();
    for (index, record) in records.iter().enumerate() {
        let record_number = index + 1;
        if record.protocol_version != PROTOCOL_VERSION {
            return Err(EvidenceError::InvalidBundle(format!(
                "driver record {record_number} has protocol version {}; expected {PROTOCOL_VERSION}",
                record.protocol_version
            )));
        }
        let expected_sequence = u64::try_from(record_number).map_err(|error| {
            EvidenceError::InvalidBundle(format!("driver record count exceeds u64: {error}"))
        })?;
        if record.sequence != expected_sequence {
            return Err(EvidenceError::InvalidBundle(format!(
                "driver record {record_number} has sequence {}; expected {expected_sequence}",
                record.sequence
            )));
        }
        if let Some(caused_by) = &record.caused_by {
            let command = lifecycle.commands.get(caused_by).ok_or_else(|| {
                EvidenceError::InvalidBundle(format!(
                    "driver record {record_number} references unknown controller message {caused_by}"
                ))
            })?;
            if !command_causes_driver_body(command, &record.body) {
                return Err(EvidenceError::InvalidBundle(format!(
                    "driver record {record_number} has a causal command that does not match its lifecycle identity"
                )));
            }
        }
        state.validate_body(bundle, lifecycle, record_number, &record.body)?;
    }
    state.finish(lifecycle, records)
}

#[derive(Default)]
struct DriverLifecycleState {
    saw_ready: bool,
    terminal_turns: BTreeSet<(String, String)>,
    opened_sessions: BTreeSet<String>,
    closed_sessions: BTreeSet<String>,
}

impl DriverLifecycleState {
    fn validate_body(
        &mut self,
        bundle: &DriverEvidenceBundle,
        lifecycle: &ControllerLifecycle,
        record_number: usize,
        body: &DriverBody,
    ) -> Result<(), EvidenceError> {
        if !self.saw_ready
            && !matches!(
                body,
                DriverBody::StartupEvent { .. } | DriverBody::Ready { .. }
            )
        {
            return Err(EvidenceError::InvalidBundle(format!(
                "driver record {record_number} occurs before driver.ready"
            )));
        }
        match body {
            DriverBody::StartupEvent { phase, status, .. } => {
                if phase.trim().is_empty() || status.trim().is_empty() {
                    return Err(EvidenceError::InvalidBundle(format!(
                        "driver record {record_number} has an empty startup phase or status"
                    )));
                }
            }
            DriverBody::Ready { driver } => {
                if self.saw_ready {
                    return Err(EvidenceError::InvalidBundle(format!(
                        "driver record {record_number} repeats driver.ready"
                    )));
                }
                self.saw_ready = true;
                if driver != &bundle.driver {
                    return Err(EvidenceError::InvalidBundle(format!(
                        "driver record {record_number} descriptor does not match the manifest"
                    )));
                }
            }
            DriverBody::SessionOpened {
                session_id,
                process_id,
            } => {
                if !lifecycle.sessions.contains(session_id) {
                    return Err(EvidenceError::InvalidBundle(format!(
                        "driver record {record_number} opened unexpected session {session_id}"
                    )));
                }
                if *process_id != bundle.process_id {
                    return Err(EvidenceError::InvalidBundle(format!(
                        "driver record {record_number} process ID {process_id} does not match manifest process ID {}",
                        bundle.process_id
                    )));
                }
                if !self.opened_sessions.insert(session_id.clone()) {
                    return Err(EvidenceError::InvalidBundle(format!(
                        "driver record {record_number} opened duplicate session {session_id}"
                    )));
                }
            }
            DriverBody::TurnEvent {
                session_id,
                turn_id,
                ..
            } => self.validate_turn_event(lifecycle, record_number, session_id, turn_id)?,
            DriverBody::TurnFinished {
                session_id,
                turn_id,
                ..
            } => self.validate_turn_finished(lifecycle, record_number, session_id, turn_id)?,
            DriverBody::SessionClosed { session_id } => {
                if !self.opened_sessions.contains(session_id) {
                    return Err(EvidenceError::InvalidBundle(format!(
                        "driver record {record_number} closed unopened session {session_id}"
                    )));
                }
                if lifecycle.turns.iter().any(|turn| {
                    turn.0.as_str() == session_id.as_str() && !self.terminal_turns.contains(turn)
                }) {
                    return Err(EvidenceError::InvalidBundle(format!(
                        "driver record {record_number} closed session {session_id} with an unfinished turn"
                    )));
                }
                if !self.closed_sessions.insert(session_id.clone()) {
                    return Err(EvidenceError::InvalidBundle(format!(
                        "driver record {record_number} closed duplicate session {session_id}"
                    )));
                }
            }
            DriverBody::Failed {
                scope,
                session_id,
                turn_id,
                ..
            } => self.validate_failure_identity(
                lifecycle,
                record_number,
                *scope,
                session_id.as_deref(),
                turn_id.as_deref(),
            )?,
        }
        Ok(())
    }

    fn validate_turn_event(
        &self,
        lifecycle: &ControllerLifecycle,
        record_number: usize,
        session_id: &str,
        turn_id: &str,
    ) -> Result<(), EvidenceError> {
        self.validate_turn_reference(lifecycle, record_number, session_id, turn_id)?;
        if self
            .terminal_turns
            .contains(&(session_id.to_owned(), turn_id.to_owned()))
        {
            Err(EvidenceError::InvalidBundle(format!(
                "driver record {record_number} emits an event after turn {session_id}/{turn_id} became terminal"
            )))
        } else {
            Ok(())
        }
    }

    fn validate_turn_finished(
        &mut self,
        lifecycle: &ControllerLifecycle,
        record_number: usize,
        session_id: &str,
        turn_id: &str,
    ) -> Result<(), EvidenceError> {
        self.validate_turn_reference(lifecycle, record_number, session_id, turn_id)?;
        if self
            .terminal_turns
            .insert((session_id.to_owned(), turn_id.to_owned()))
        {
            Ok(())
        } else {
            Err(EvidenceError::InvalidBundle(format!(
                "driver record {record_number} finishes already-terminal turn {session_id}/{turn_id}"
            )))
        }
    }

    fn validate_turn_reference(
        &self,
        lifecycle: &ControllerLifecycle,
        record_number: usize,
        session_id: &str,
        turn_id: &str,
    ) -> Result<(), EvidenceError> {
        validate_open_turn_reference(
            lifecycle,
            &self.opened_sessions,
            &self.closed_sessions,
            record_number,
            session_id,
            turn_id,
        )
    }

    fn validate_failure_identity(
        &mut self,
        lifecycle: &ControllerLifecycle,
        record_number: usize,
        scope: DriverFailureScope,
        session_id: Option<&str>,
        turn_id: Option<&str>,
    ) -> Result<(), EvidenceError> {
        let valid = match (scope, session_id, turn_id) {
            (DriverFailureScope::Driver | DriverFailureScope::Protocol, None, None) => true,
            (DriverFailureScope::Session, Some(session_id), None) => {
                self.opened_sessions.contains(session_id)
                    && !self.closed_sessions.contains(session_id)
                    && lifecycle.sessions.contains(session_id)
            }
            (DriverFailureScope::Turn, Some(session_id), Some(turn_id)) => {
                let turn = (session_id.to_owned(), turn_id.to_owned());
                let valid = self.opened_sessions.contains(session_id)
                    && !self.closed_sessions.contains(session_id)
                    && lifecycle.turns.contains(&turn)
                    && !self.terminal_turns.contains(&turn);
                if valid {
                    self.terminal_turns.insert(turn);
                }
                valid
            }
            _ => false,
        };
        if valid {
            Ok(())
        } else {
            Err(EvidenceError::InvalidBundle(format!(
                "driver record {record_number} has invalid {scope:?} failure identity session={session_id:?} turn={turn_id:?}"
            )))
        }
    }

    fn finish(
        &self,
        lifecycle: &ControllerLifecycle,
        records: &[DriverMessage],
    ) -> Result<(), EvidenceError> {
        if !self.saw_ready {
            return Err(EvidenceError::InvalidBundle(
                "driver transcript does not contain driver.ready".to_owned(),
            ));
        }
        if !matches!(
            records.last().map(|record| &record.body),
            Some(DriverBody::SessionClosed { .. })
        ) {
            return Err(EvidenceError::InvalidBundle(
                "driver transcript does not end with session.closed".to_owned(),
            ));
        }
        if let Some((session_id, turn_id)) = lifecycle.turns.difference(&self.terminal_turns).next()
        {
            return Err(EvidenceError::InvalidBundle(format!(
                "turn {session_id}/{turn_id} did not reach a terminal driver record"
            )));
        }
        if lifecycle.sessions != self.opened_sessions || lifecycle.sessions != self.closed_sessions
        {
            return Err(EvidenceError::InvalidBundle(
                "driver transcript does not open and close every requested session".to_owned(),
            ));
        }
        Ok(())
    }
}

#[cfg(any(
    target_os = "android",
    target_os = "linux",
    target_os = "macos",
    target_os = "ios",
    target_os = "tvos",
    target_os = "visionos",
    target_os = "watchos",
))]
fn finalize_no_replace(staging: &Path, target: &Path) -> Result<(), EvidenceError> {
    use rustix::{
        fs::{CWD, RenameFlags, renameat_with},
        io::Errno,
    };

    match renameat_with(CWD, staging, CWD, target, RenameFlags::NOREPLACE) {
        Ok(()) => Ok(()),
        Err(Errno::EXIST) => Err(EvidenceError::AlreadyExists(target.to_path_buf())),
        Err(error) => Err(EvidenceError::Io(error.into())),
    }
}

#[cfg(windows)]
fn finalize_no_replace(staging: &Path, target: &Path) -> Result<(), EvidenceError> {
    match fs::rename(staging, target) {
        Ok(()) => Ok(()),
        Err(_) if target.exists() => Err(EvidenceError::AlreadyExists(target.to_path_buf())),
        Err(error) => Err(EvidenceError::Io(error)),
    }
}

#[cfg(not(any(
    windows,
    target_os = "android",
    target_os = "linux",
    target_os = "macos",
    target_os = "ios",
    target_os = "tvos",
    target_os = "visionos",
    target_os = "watchos",
)))]
fn finalize_no_replace(staging: &Path, target: &Path) -> Result<(), EvidenceError> {
    let _ = staging;
    Err(EvidenceError::Io(io::Error::new(
        io::ErrorKind::Unsupported,
        format!(
            "atomic no-replace evidence finalization is unsupported for {}",
            target.display()
        ),
    )))
}

fn validate_open_turn_reference(
    lifecycle: &ControllerLifecycle,
    opened_sessions: &BTreeSet<String>,
    closed_sessions: &BTreeSet<String>,
    record_number: usize,
    session_id: &str,
    turn_id: &str,
) -> Result<(), EvidenceError> {
    if opened_sessions.contains(session_id)
        && !closed_sessions.contains(session_id)
        && lifecycle
            .turns
            .contains(&(session_id.to_owned(), turn_id.to_owned()))
    {
        Ok(())
    } else {
        Err(EvidenceError::InvalidBundle(format!(
            "driver record {record_number} references unexpected turn {session_id}/{turn_id}"
        )))
    }
}

fn canonicalize_driver_records(
    transcript: &DriverTranscript,
    policy: CanonicalizationPolicy,
) -> Result<CanonicalProjection, serde_json::Error> {
    let driver_records = transcript
        .driver_records
        .iter()
        .map(|record| serde_json::from_slice(record))
        .collect::<Result<Vec<JsonValue>, _>>()?
        .into_iter()
        .map(|record| canonicalize_value(record, &policy.removed_object_keys))
        .collect();

    Ok(CanonicalProjection {
        policy,
        driver_records,
    })
}

fn canonicalize_value(value: JsonValue, removed_keys: &BTreeSet<String>) -> JsonValue {
    match value {
        JsonValue::Array(values) => JsonValue::Array(
            values
                .into_iter()
                .map(|value| canonicalize_value(value, removed_keys))
                .collect(),
        ),
        JsonValue::Object(values) => JsonValue::Object(
            values
                .into_iter()
                .filter(|(key, _)| !removed_keys.contains(key))
                .map(|(key, value)| (key, canonicalize_value(value, removed_keys)))
                .collect(),
        ),
        value => value,
    }
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<(), EvidenceError> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    fs::write(path, bytes)?;
    Ok(())
}

fn read_json<T>(path: &Path) -> Result<T, EvidenceError>
where
    T: for<'de> Deserialize<'de>,
{
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

fn write_records(path: &Path, records: &[Vec<u8>]) -> Result<(), EvidenceError> {
    let bytes = records.iter().flatten().copied().collect::<Vec<_>>();
    fs::write(path, bytes)?;
    Ok(())
}

fn read_records(path: &Path) -> Result<Vec<Vec<u8>>, EvidenceError> {
    let bytes = fs::read(path)?;
    Ok(bytes
        .split_inclusive(|byte| *byte == b'\n')
        .filter(|record| !record.is_empty())
        .map(<[u8]>::to_vec)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finalization_does_not_replace_an_existing_empty_directory() {
        let root = std::env::temp_dir().join(format!(
            "agent-lab-evidence-no-replace-{}-{}",
            std::process::id(),
            STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let staging = root.join("staging");
        let target = root.join("target");
        fs::create_dir_all(&staging).unwrap();
        fs::write(staging.join("payload"), b"staged").unwrap();
        fs::create_dir(&target).unwrap();

        assert!(matches!(
            finalize_no_replace(&staging, &target),
            Err(EvidenceError::AlreadyExists(path)) if path == target
        ));
        assert_eq!(fs::read_dir(&target).unwrap().count(), 0);
        assert_eq!(fs::read(staging.join("payload")).unwrap(), b"staged");

        fs::remove_dir_all(root).unwrap();
    }
}
