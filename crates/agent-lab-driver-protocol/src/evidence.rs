use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::{DriverDescriptor, DriverTranscript};

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
    /// Returns an error if a retained driver record is not valid JSON.
    pub fn new(
        controller_revision: Option<String>,
        driver: DriverDescriptor,
        process_id: u32,
        transcript: DriverTranscript,
        policy: CanonicalizationPolicy,
    ) -> Result<Self, serde_json::Error> {
        let canonical = canonicalize_driver_records(&transcript, policy)?;
        Ok(Self {
            controller_revision,
            driver,
            process_id,
            transcript,
            canonical,
        })
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
