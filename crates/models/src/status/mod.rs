pub mod capture;
pub mod catalog_test;
pub mod collection;
pub mod materialization;
pub mod publications;

use crate::{datetime_schema, is_false, option_datetime_schema, CatalogType, Id};
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Response type for the status endpoint
#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct StatusResponse {
    /// The name of the live spec
    pub catalog_name: String,
    /// The id of the live spec
    pub live_spec_id: Id,
    /// The type of the live spec
    pub spec_type: Option<CatalogType>,
    /// Whether the shards are disabled. Only pertinent to tasks. Omitted if false.
    #[serde(default, skip_serializing_if = "is_false")]
    pub disabled: bool,
    /// The id of the last successful publication that modified the spec.
    pub last_pub_id: Id,
    /// The id of the last successful publication of the spec, regardless of
    /// whether the spec was modified. This value can be compared against the
    /// value of `/controller_status/activations/last_activated` in order to
    /// determine whether the most recent build has been activated in the data
    /// plane.
    pub last_build_id: Id,
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
    pub status: Status,
    /// Error from the most recent controller run, or `null` if the run was
    /// successful.
    pub controller_error: Option<String>,
    /// The number of consecutive failures of the controller. Resets to 0 after
    /// any successful run.
    pub controller_failures: i32,
}

/// Represents the internal state of a controller.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
#[serde(tag = "type")]
pub enum Status {
    Capture(capture::CaptureStatus),
    Collection(collection::CollectionStatus),
    Materialization(materialization::MaterializationStatus),
    Test(catalog_test::TestStatus),
    #[serde(other, untagged)]
    Uninitialized,
}

// Status types are serialized as plain json columns.
crate::sqlx_json::sqlx_json!(Status);

impl Status {
    pub fn catalog_type(&self) -> Option<CatalogType> {
        match self {
            Status::Capture(_) => Some(CatalogType::Capture),
            Status::Collection(_) => Some(CatalogType::Collection),
            Status::Materialization(_) => Some(CatalogType::Materialization),
            Status::Test(_) => Some(CatalogType::Test),
            Status::Uninitialized => None,
        }
    }

    pub fn is_uninitialized(&self) -> bool {
        matches!(self, Status::Uninitialized)
    }

    pub fn as_capture_mut(&mut self) -> anyhow::Result<&mut capture::CaptureStatus> {
        if self.is_uninitialized() {
            *self = Status::Capture(Default::default());
        }
        match self {
            Status::Capture(c) => Ok(c),
            _ => anyhow::bail!("expected capture status"),
        }
    }

    pub fn as_collection_mut(&mut self) -> anyhow::Result<&mut collection::CollectionStatus> {
        if self.is_uninitialized() {
            *self = Status::Collection(Default::default());
        }
        match self {
            Status::Collection(c) => Ok(c),
            _ => anyhow::bail!("expected collection status"),
        }
    }

    pub fn as_materialization_mut(
        &mut self,
    ) -> anyhow::Result<&mut materialization::MaterializationStatus> {
        if self.is_uninitialized() {
            *self = Status::Materialization(Default::default());
        }
        match self {
            Status::Materialization(m) => Ok(m),
            _ => anyhow::bail!("expected materialization status"),
        }
    }

    pub fn as_test_mut(&mut self) -> anyhow::Result<&mut catalog_test::TestStatus> {
        if self.is_uninitialized() {
            *self = Status::Test(Default::default());
        }
        match self {
            Status::Test(t) => Ok(t),
            _ => anyhow::bail!("expected test status"),
        }
    }

    pub fn unwrap_capture(&self) -> &capture::CaptureStatus {
        match self {
            Status::Capture(c) => c,
            _ => panic!("expected capture status"),
        }
    }

    pub fn unwrap_collection(&self) -> &collection::CollectionStatus {
        match self {
            Status::Collection(c) => c,
            _ => panic!("expected collection status"),
        }
    }

    pub fn unwrap_materialization(&self) -> &materialization::MaterializationStatus {
        match self {
            Status::Materialization(m) => m,
            _ => panic!("expected materialization status"),
        }
    }

    pub fn unwrap_test(&self) -> &catalog_test::TestStatus {
        match self {
            Status::Test(t) => t,
            _ => panic!("expected test status"),
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::{BTreeSet, VecDeque};

    use chrono::{TimeZone, Utc};

    use super::*;
    use crate::draft_error::Error;
    use crate::publications::{AffectedConsumer, IncompatibleCollection, JobStatus, RejectedField};
    use crate::status::materialization::{MaterializationStatus, SourceCaptureStatus};
    use crate::status::publications::{ActivationStatus, PublicationInfo, PublicationStatus};
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

        let status = Status::Materialization(MaterializationStatus {
            activation: ActivationStatus {
                last_activated: Id::new([1, 2, 3, 4, 4, 3, 2, 1]),
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
        let round_tripped: Status =
            serde_json::from_str(&as_json).expect("failed to deserialize status");

        #[derive(Debug)]
        #[allow(unused)]
        struct StatusSnapshot {
            starting: Status,
            json: String,
            parsed: Status,
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
        let schema_obj = generator.into_root_schema_for::<Status>();
        let schema = serde_json::to_value(&schema_obj).unwrap();
        insta::assert_json_snapshot!(schema);
    }
}
