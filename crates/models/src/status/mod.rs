pub mod activation;
pub mod capture;
pub mod catalog_test;
pub mod collection;
pub mod connector;
pub mod materialization;
pub mod publications;
pub mod summary;

use crate::{datetime_schema, is_false, option_datetime_schema, CatalogType, Id};
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub use self::summary::{StatusSummaryType, Summary};

/// Response type for the status endpoint
#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct StatusResponse {
    /// The name of the live spec
    pub catalog_name: String,
    /// The id of the live spec
    pub live_spec_id: Id,
    /// The type of the live spec
    pub spec_type: Option<CatalogType>,
    /// A brief summary of the status
    pub summary: Summary,
    /// Whether the shards are disabled. Only pertinent to tasks. Omitted if false.
    #[serde(default, skip_serializing_if = "is_false")]
    pub disabled: bool,
    /// The id of the last successful publication that modified the spec.
    pub last_pub_id: Id,
    /// The id of the last successful publication of the spec, regardless of
    /// whether the spec was modified. This value can be compared against the
    /// value of `/status/activations/last_activated` in order to determine
    /// whether the most recent build has been activated in the data plane.
    pub last_build_id: Id,
    /// The status of the connector, if present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connector_status: Option<connector::ConnectorStatus>,
    /// Time at which the controller is next scheduled to run. Or null if there
    /// is no run scheduled.
    #[schemars(schema_with = "option_datetime_schema")]
    pub controller_next_run: Option<DateTime<Utc>>,
    /// Time of the last publication that affected the live spec.
    #[schemars(schema_with = "datetime_schema")]
    pub live_spec_updated_at: DateTime<Utc>,
    /// Time of the last controller run for this spec.
    #[schemars(schema_with = "datetime_schema")]
    pub controller_updated_at: DateTime<Utc>,
    /// The controller status json.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub controller_status: Option<ControllerStatus>,
    /// Error from the most recent controller run, or `null` if the run was
    /// successful.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub controller_error: Option<String>,
    /// The number of consecutive failures of the controller. Resets to 0 after
    /// any successful run.
    #[serde(default, skip_serializing_if = "crate::is_i32_zero")]
    pub controller_failures: i32,
}

/// Represents the internal state of a controller.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
#[serde(tag = "type")]
pub enum ControllerStatus {
    Capture(capture::CaptureStatus),
    Collection(collection::CollectionStatus),
    Materialization(materialization::MaterializationStatus),
    Test(catalog_test::TestStatus),
    #[serde(other, untagged)]
    Uninitialized,
}

impl Default for ControllerStatus {
    fn default() -> Self {
        ControllerStatus::Uninitialized
    }
}

// Status types are serialized as plain json columns.
crate::sqlx_json::sqlx_json!(ControllerStatus);

impl ControllerStatus {
    pub fn catalog_type(&self) -> Option<CatalogType> {
        match self {
            ControllerStatus::Capture(_) => Some(CatalogType::Capture),
            ControllerStatus::Collection(_) => Some(CatalogType::Collection),
            ControllerStatus::Materialization(_) => Some(CatalogType::Materialization),
            ControllerStatus::Test(_) => Some(CatalogType::Test),
            ControllerStatus::Uninitialized => None,
        }
    }

    pub fn is_uninitialized(&self) -> bool {
        matches!(self, ControllerStatus::Uninitialized)
    }

    /// Returns the activation status, if this status is for a capture, collection, or materialization.
    pub fn activation_status(&self) -> Option<&activation::ActivationStatus> {
        match self {
            ControllerStatus::Capture(c) => Some(&c.activation),
            ControllerStatus::Collection(c) => Some(&c.activation),
            ControllerStatus::Materialization(c) => Some(&c.activation),
            _ => None,
        }
    }

    pub fn publication_status(&self) -> Option<&publications::PublicationStatus> {
        match self {
            ControllerStatus::Capture(c) => Some(&c.publications),
            ControllerStatus::Collection(c) => Some(&c.publications),
            ControllerStatus::Materialization(c) => Some(&c.publications),
            ControllerStatus::Test(s) => Some(&s.publications),
            ControllerStatus::Uninitialized => None,
        }
    }

    pub fn as_capture_mut(&mut self) -> anyhow::Result<&mut capture::CaptureStatus> {
        if self.is_uninitialized() {
            *self = ControllerStatus::Capture(Default::default());
        }
        match self {
            ControllerStatus::Capture(c) => Ok(c),
            _ => anyhow::bail!("expected capture status"),
        }
    }

    pub fn as_collection_mut(&mut self) -> anyhow::Result<&mut collection::CollectionStatus> {
        if self.is_uninitialized() {
            *self = ControllerStatus::Collection(Default::default());
        }
        match self {
            ControllerStatus::Collection(c) => Ok(c),
            _ => anyhow::bail!("expected collection status"),
        }
    }

    pub fn as_materialization_mut(
        &mut self,
    ) -> anyhow::Result<&mut materialization::MaterializationStatus> {
        if self.is_uninitialized() {
            *self = ControllerStatus::Materialization(Default::default());
        }
        match self {
            ControllerStatus::Materialization(m) => Ok(m),
            _ => anyhow::bail!("expected materialization status"),
        }
    }

    pub fn as_test_mut(&mut self) -> anyhow::Result<&mut catalog_test::TestStatus> {
        if self.is_uninitialized() {
            *self = ControllerStatus::Test(Default::default());
        }
        match self {
            ControllerStatus::Test(t) => Ok(t),
            _ => anyhow::bail!("expected test status"),
        }
    }

    pub fn unwrap_capture(&self) -> &capture::CaptureStatus {
        match self {
            ControllerStatus::Capture(c) => c,
            _ => panic!("expected capture status"),
        }
    }

    pub fn unwrap_collection(&self) -> &collection::CollectionStatus {
        match self {
            ControllerStatus::Collection(c) => c,
            _ => panic!("expected collection status"),
        }
    }

    pub fn unwrap_materialization(&self) -> &materialization::MaterializationStatus {
        match self {
            ControllerStatus::Materialization(m) => m,
            _ => panic!("expected materialization status"),
        }
    }

    pub fn unwrap_test(&self) -> &catalog_test::TestStatus {
        match self {
            ControllerStatus::Test(t) => t,
            _ => panic!("expected test status"),
        }
    }
}

/// Identifies the specific task shard that is the source of an event. This
/// matches the shape of the `shard` field in an `ops.Log` message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ShardRef {
    /// The name of the task
    pub name: String,
    /// The key range of the task as a hex string. Together with rClockBegin, this
    /// uniquely identifies a specific task shard.
    pub key_begin: String,
    /// The rClock range of the task as a hex string. Together with keyBegin, this
    /// uniquely identifies a specific task shard.
    pub r_clock_begin: String,
    /// The id of the build that the shard was running when the event was
    /// generated. This can be compared against the `last_build_id` of the live
    /// spec to determine whether the event happened with the most rececnt
    /// version of the published spec (it did if the `last_build_id` is the
    /// same).
    pub build: Id,
}

#[cfg(test)]
mod test {
    use std::collections::{BTreeSet, VecDeque};

    use chrono::{TimeZone, Utc};

    use super::*;
    use crate::draft_error::Error;
    use crate::publications::{AffectedConsumer, IncompatibleCollection, JobStatus, RejectedField};
    use crate::status::activation::ActivationStatus;
    use crate::status::materialization::{MaterializationStatus, SourceCaptureStatus};
    use crate::status::publications::{PublicationInfo, PublicationStatus};
    use crate::Id;

    #[test]
    fn test_status_round_trip_serde() {
        let mut add_bindings = BTreeSet::new();
        add_bindings.insert(crate::Collection::new("snails/shells"));

        let pub_status = PublicationInfo {
            id: Id::new([4, 3, 2, 1, 1, 2, 3, 4]),
            created: Some(Utc.with_ymd_and_hms(2024, 5, 30, 9, 10, 11).unwrap()),
            completed: Some(Utc.with_ymd_and_hms(2024, 5, 30, 9, 10, 11).unwrap()),
            detail: Some("some detail".to_string()),
            result: Some(JobStatus::build_failed(vec![IncompatibleCollection {
                collection: "snails/water".to_string(),
                requires_recreation: Vec::new(),
                affected_materializations: vec![AffectedConsumer {
                    name: "snails/materialize".to_string(),
                    fields: vec![RejectedField {
                        field: "a_field".to_string(),
                        reason: "do not like".to_string(),
                    }],
                    resource_path: vec!["water".to_string()],
                }],
            }])),
            errors: vec![Error {
                catalog_name: "snails/shells".to_string(),
                scope: Some("flow://materializations/snails/shells".to_string()),
                detail: "a_field simply cannot be tolerated".to_string(),
            }],
            count: 1,
            is_touch: false,
        };
        let mut history = VecDeque::new();
        history.push_front(pub_status);

        let status = ControllerStatus::Materialization(MaterializationStatus {
            activation: ActivationStatus {
                last_activated: Id::new([1, 2, 3, 4, 4, 3, 2, 1]),
                last_activated_at: Some("2024-01-02T03:04:05.06Z".parse().unwrap()),
                last_failure: None,
                recent_failure_count: 3,
                next_retry: Some("2025-01-02T03:04:05.06Z".parse().unwrap()),
            },
            source_capture: Some(SourceCaptureStatus {
                up_to_date: false,
                add_bindings,
            }),
            publications: PublicationStatus {
                max_observed_pub_id: Id::new([1, 2, 3, 4, 5, 6, 7, 8]),
                history,
                dependency_hash: Some("abc12345".to_string()),
            },
        });

        let as_json = serde_json::to_string_pretty(&status).expect("failed to serialize status");
        let round_tripped: ControllerStatus =
            serde_json::from_str(&as_json).expect("failed to deserialize status");

        #[derive(Debug)]
        #[allow(unused)]
        struct StatusSnapshot {
            starting: ControllerStatus,
            json: String,
            parsed: ControllerStatus,
        }

        insta::assert_debug_snapshot!(
            "materialization-status-round-trip",
            StatusSnapshot {
                starting: status,
                json: as_json,
                parsed: round_tripped,
            }
        );
    }

    #[test]
    fn test_status_json_schema() {
        let settings = schemars::gen::SchemaSettings::draft2019_09();
        let generator = schemars::gen::SchemaGenerator::new(settings);
        let schema_obj = generator.into_root_schema_for::<ControllerStatus>();
        let schema = serde_json::to_value(&schema_obj).unwrap();
        insta::assert_json_snapshot!(schema);
    }
}
