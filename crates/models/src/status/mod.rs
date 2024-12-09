pub mod capture;
pub mod catalog_test;
pub mod collection;
pub mod materialization;
pub mod publications;

use crate::CatalogType;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Represents the internal state of a controller.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
#[serde(tag = "type")]
pub enum Status {
    Capture(capture::CaptureStatus),
    Collection(collection::CollectionStatus),
    Materialization(materialization::MaterializationStatus),
    Test(catalog_test::TestStatus),
    #[schemars(skip)]
    #[serde(other, untagged)]
    Uninitialized,
}

impl Status {
    pub fn json_schema() -> schemars::schema::RootSchema {
        let settings = schemars::gen::SchemaSettings::draft2019_09();
        //settings.option_add_null_type = false;
        //settings.inline_subschemas = true;
        let generator = schemars::gen::SchemaGenerator::new(settings);
        generator.into_root_schema_for::<Status>()
    }

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
        let schema = serde_json::to_value(Status::json_schema()).unwrap();
        insta::assert_json_snapshot!(schema);
    }
}
