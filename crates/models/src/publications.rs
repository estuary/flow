use crate::Id;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// JobStatus is the possible outcomes of a handled publication.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum JobStatus {
    Queued,
    BuildFailed {
        #[serde(
            default,
            skip_serializing_if = "Vec::is_empty",
            rename = "incompatible_collections"
        )]
        incompatible_collections: Vec<IncompatibleCollection>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        evolution_id: Option<Id>,
    },
    TestFailed,
    PublishFailed,
    Success,
    /// Returned when there are no draft specs (after pruning unbound
    /// collections). There will not be any `draft_errors` in this case, because
    /// there's no `catalog_name` to associate with an error. And it may not be
    /// desirable to treat this as an error, depending on the scenario.
    EmptyDraft,
    /// One or more expected `last_pub_id`s did not match the actual `last_pub_id`, indicating that specs
    /// have been changed since the draft was created.
    ExpectPubIdMismatch {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        failures: Vec<LockFailure>,
    },
    /// Optimistic locking failure for one or more specs in the publication. This case should
    /// typically be retried by the publisher.
    BuildIdLockFailure {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        failures: Vec<LockFailure>,
    },
    /// The publication used the deprecated background flag, which is no longer supported.
    DeprecatedBackground,
}

impl JobStatus {
    pub fn is_success(&self) -> bool {
        // TODO(phil): should EmptyDraft also be considered successful?
        // This question is not relevent today, but will become important
        // once we implement auto-discovery in controllers.
        match self {
            JobStatus::Success { .. } => true,
            _ => false,
        }
    }

    pub fn incompatible_collections(&self) -> Option<&[IncompatibleCollection]> {
        match self {
            JobStatus::BuildFailed {
                incompatible_collections,
                ..
            } if !incompatible_collections.is_empty() => Some(incompatible_collections.as_slice()),
            _ => None,
        }
    }

    pub fn has_incompatible_collections(&self) -> bool {
        self.incompatible_collections().is_some()
    }

    pub fn is_empty_draft(&self) -> bool {
        matches!(self, JobStatus::EmptyDraft)
    }

    pub fn build_failed(incompatible_collections: Vec<IncompatibleCollection>) -> JobStatus {
        JobStatus::BuildFailed {
            incompatible_collections,
            evolution_id: None,
        }
    }
}

/// Represents an optimistic lock failure when trying to update live specs.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, JsonSchema)]
pub struct LockFailure {
    pub catalog_name: String,
    pub expected: Id,
    pub actual: Option<Id>,
}

/// Reasons why a draft collection spec would need to be published under a new name.
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ReCreateReason {
    /// The collection key in the draft differs from that of the live spec.
    KeyChange,
    /// One or more collection partition fields in the draft differs from that of the live spec.
    PartitionChange,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct IncompatibleCollection {
    pub collection: String,
    /// Reasons why the collection would need to be re-created in order for a publication of the draft spec to succeed.
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        alias = "requiresRecreation"
    )]
    pub requires_recreation: Vec<ReCreateReason>,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        alias = "affectedMaterializations"
    )]
    pub affected_materializations: Vec<AffectedConsumer>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, JsonSchema)]
pub struct AffectedConsumer {
    pub name: String,
    pub fields: Vec<RejectedField>,
    /// Identifies the specific binding that is affected. This can be used to differentiate
    /// in cases there are multiple bindings with the same source.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resource_path: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, JsonSchema)]
pub struct RejectedField {
    pub field: String,
    pub reason: String,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_publication_job_status_serde() {
        let starting = JobStatus::build_failed(vec![IncompatibleCollection {
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
        }]);

        let as_json = serde_json::to_string_pretty(&starting).expect("failed to serialize");
        let parsed =
            serde_json::from_str::<'_, JobStatus>(&as_json).expect("failed to deserialize");
        assert_eq!(
            starting, parsed,
            "unequal status after round-trip, json:\n{as_json}"
        );
    }

    #[test]
    fn test_status_serde_backward_compatibility() {
        let old_json = r##"{
          "type": "buildFailed",
          "incompatible_collections": [
            {
              "collection": "acmeCo/foo",
              "affectedMaterializations": [
                {
                  "name": "acmeCo/postgres",
                  "fields": [
                    {
                      "field": "some_date",
                      "reason": "Field 'some_date' is already being materialized as endpoint type 'TIMESTAMP WITH TIME ZONE' but endpoint type 'DATE' is required by its schema '{ type: [null, string], format: date }'"
                    }
                  ]
                }
              ]
            }
          ]
        }"##;

        let result: JobStatus =
            serde_json::from_str(old_json).expect("old status json failed to deserialize");
        insta::assert_debug_snapshot!(result, @r###"
        BuildFailed {
            incompatible_collections: [
                IncompatibleCollection {
                    collection: "acmeCo/foo",
                    requires_recreation: [],
                    affected_materializations: [
                        AffectedConsumer {
                            name: "acmeCo/postgres",
                            fields: [
                                RejectedField {
                                    field: "some_date",
                                    reason: "Field 'some_date' is already being materialized as endpoint type 'TIMESTAMP WITH TIME ZONE' but endpoint type 'DATE' is required by its schema '{ type: [null, string], format: date }'",
                                },
                            ],
                            resource_path: [],
                        },
                    ],
                },
            ],
            evolution_id: None,
        }
        "###);

        let old_json = r##"{"type":"buildFailed","incompatible_collections":[{"collection":"acmeCo/bar","requiresRecreation":["keyChange"]}]}"##;

        let result: JobStatus =
            serde_json::from_str(old_json).expect("old status json failed to deserialize");
        insta::assert_debug_snapshot!(result, @r###"
        BuildFailed {
            incompatible_collections: [
                IncompatibleCollection {
                    collection: "acmeCo/bar",
                    requires_recreation: [
                        KeyChange,
                    ],
                    affected_materializations: [],
                },
            ],
            evolution_id: None,
        }
        "###);
    }
}
