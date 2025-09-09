use crate::Id;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The highest-level status of a publication.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum StatusType {
    /// The publication has not yet been completed.
    Queued,
    /// There was a failure to build or validate the drafted specs. This could
    /// be due to a mistake in the drafted specs, or due to a failure to
    /// validate the proposed changes with an external system connected to one
    /// of the connected captures or materializations.
    BuildFailed,
    /// Publication failed due to the failure of one or more tests.
    TestFailed,
    /// Something went wrong with the publication process. These errors can
    /// typically be retried by the client.
    PublishFailed,
    /// The publication was successful. All drafted specs are now committed as
    /// the live specs. Note that activation of the published specs in the data
    /// plane happens asynchronously, after the publication is committed.
    /// Therefore, it may take some time for the published changes to be
    /// reflected in running tasks.
    Success,
    /// Returned when there are no draft specs (after pruning unbound
    /// collections). There will not be any `draft_errors` in this case, because
    /// there's no `catalog_name` to associate with an error. And it may not be
    /// desirable to treat this as an error, depending on the scenario.
    EmptyDraft,
    /// One or more expected `last_pub_id`s did not match the actual `last_pub_id`, indicating that specs
    /// have been changed since the draft was created.
    ExpectPubIdMismatch,
    /// Optimistic locking failure for one or more specs in the publication. This case should
    /// typically be retried by the publisher.
    BuildIdLockFailure,
    /// The publication used the deprecated background flag, which is no longer supported.
    DeprecatedBackground,
}

/// The status of a publication.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct JobStatus {
    pub r#type: StatusType,
    #[serde(alias = "failures", default, skip_serializing_if = "Vec::is_empty")]
    pub lock_failures: Vec<LockFailure>,
}

impl From<StatusType> for JobStatus {
    fn from(status_type: StatusType) -> Self {
        JobStatus {
            r#type: status_type,
            lock_failures: Vec::new(),
        }
    }
}

impl JobStatus {
    pub fn is_success(&self) -> bool {
        match self.r#type {
            StatusType::Success { .. } => true,
            _ => false,
        }
    }

    pub fn is_empty_draft(&self) -> bool {
        matches!(self.r#type, StatusType::EmptyDraft)
    }

    pub fn build_id_lock_failure(lock_failures: Vec<LockFailure>) -> Self {
        JobStatus {
            r#type: StatusType::BuildIdLockFailure,
            lock_failures,
        }
    }
}

/// Represents an optimistic lock failure when trying to update live specs.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, JsonSchema)]
pub struct LockFailure {
    /// The name of the spec that failed the optimistic concurrency check.
    pub catalog_name: String,
    /// The expected id (either `last_pub_id` or `last_build_id`) that was not
    /// matched.
    pub expected: Id,
    /// The actual id that was found.
    pub actual: Option<Id>,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_publication_job_status_serde() {
        let starting: JobStatus = StatusType::BuildFailed.into();

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
        JobStatus {
            type: BuildFailed,
            lock_failures: [],
        }
        "###);

        let old_json = r##"{"type":"buildFailed","incompatible_collections":[{"collection":"acmeCo/bar","requiresRecreation":["keyChange"]}]}"##;

        let result: JobStatus =
            serde_json::from_str(old_json).expect("old status json failed to deserialize");
        insta::assert_debug_snapshot!(result, @r###"
        JobStatus {
            type: BuildFailed,
            lock_failures: [],
        }
        "###);
    }
}
