use std::{
    collections::BTreeSet,
    fs, io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use thiserror::Error;

use crate::{
    ControllerCommand, DriverBody, DriverDescriptor, DriverMessage, DriverTranscript,
    PROTOCOL_VERSION,
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
            fs::rename(&staging, target)?;
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
    validate_controller_records(&controller_records)?;
    let driver_records =
        validate_records::<DriverMessage>("driver", &bundle.transcript.driver_records)?;
    validate_driver_records(bundle, &driver_records)?;
    let expected =
        canonicalize_driver_records(&bundle.transcript, bundle.canonical.policy.clone())?;
    if expected != bundle.canonical {
        return Err(EvidenceError::InvalidBundle(
            "canonical projection does not match the retained driver records".to_owned(),
        ));
    }
    Ok(())
}

fn validate_controller_records(records: &[ControllerCommand]) -> Result<(), EvidenceError> {
    for (index, record) in records.iter().enumerate() {
        if record.protocol_version != PROTOCOL_VERSION {
            return Err(EvidenceError::InvalidBundle(format!(
                "controller record {} has protocol version {}; expected {PROTOCOL_VERSION}",
                index + 1,
                record.protocol_version
            )));
        }
    }
    Ok(())
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
) -> Result<(), EvidenceError> {
    let mut saw_ready = false;
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
        match &record.body {
            DriverBody::Ready { driver } => {
                saw_ready = true;
                if driver != &bundle.driver {
                    return Err(EvidenceError::InvalidBundle(format!(
                        "driver record {record_number} descriptor does not match the manifest"
                    )));
                }
            }
            DriverBody::SessionOpened { process_id, .. } if *process_id != bundle.process_id => {
                return Err(EvidenceError::InvalidBundle(format!(
                    "driver record {record_number} process ID {process_id} does not match manifest process ID {}",
                    bundle.process_id
                )));
            }
            _ => {}
        }
    }
    if !saw_ready {
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
    Ok(())
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
