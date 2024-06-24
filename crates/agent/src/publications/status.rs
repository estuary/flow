use models::Id;
use proto_flow::materialize::response::validated::constraint::Type as ConstraintType;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tables::BuiltRow;

/// JobStatus is the possible outcomes of a handled publication.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum JobStatus {
    Queued,
    BuildFailed {
        #[serde(
            default,
            skip_serializing_if = "Vec::is_empty",
            alias = "incompatible_collections"
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

    pub fn has_incompatible_collections(&self) -> bool {
        matches!(self, JobStatus::BuildFailed { incompatible_collections, .. } if !incompatible_collections.is_empty())
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
    pub expect_pub_id: models::Id,
    pub last_pub_id: Option<models::Id>,
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
#[serde(rename_all = "camelCase")]
pub struct IncompatibleCollection {
    pub collection: String,
    /// Reasons why the collection would need to be re-created in order for a publication of the draft spec to succeed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requires_recreation: Vec<ReCreateReason>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub affected_materializations: Vec<AffectedConsumer>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, JsonSchema)]
pub struct AffectedConsumer {
    pub name: String,
    pub fields: Vec<RejectedField>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, JsonSchema)]
pub struct RejectedField {
    pub field: String,
    pub reason: String,
}

pub fn get_incompatible_collections(output: &tables::Validations) -> Vec<IncompatibleCollection> {
    // We'll collect a map of collection names to lists of materializations that have rejected the proposed collection changes.
    let mut naughty_collections = BTreeMap::new();

    // Look at materialization validation responses for any collections that have been rejected due to unsatisfiable constraints.
    for mat in output.built_materializations.iter() {
        let Some(validated) = mat.validated() else {
            continue;
        };
        let Some(model) = mat.model() else {
            continue;
        };
        for (i, binding) in validated.bindings.iter().enumerate() {
            let naughty_fields: Vec<RejectedField> = binding
                .constraints
                .iter()
                .filter(|(_, constraint)| constraint.r#type == ConstraintType::Unsatisfiable as i32)
                .map(|(field, constraint)| RejectedField {
                    field: field.clone(),
                    reason: constraint.reason.clone(),
                })
                .collect();
            if !naughty_fields.is_empty() {
                let collection_name = model.bindings[i].source.collection().to_string();
                let affected_consumers = naughty_collections
                    .entry(collection_name)
                    .or_insert_with(|| Vec::new());
                affected_consumers.push(AffectedConsumer {
                    name: mat.catalog_name().to_string(),
                    fields: naughty_fields,
                });
            }
        }
    }

    naughty_collections
        .into_iter()
        .map(
            |(collection, affected_materializations)| IncompatibleCollection {
                collection,
                affected_materializations,
                requires_recreation: Vec::new(),
            },
        )
        .collect()
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
}
