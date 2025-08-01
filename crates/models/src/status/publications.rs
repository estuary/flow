use crate::draft_error;
use crate::publications;
use crate::Id;
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

/// Summary of a publication that was attempted by a controller.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, JsonSchema)]
#[cfg_attr(feature = "async-graphql", derive(async_graphql::SimpleObject))]
pub struct PublicationInfo {
    /// The id of the publication, which will match the `last_pub_id` of the
    /// spec after a successful publication, at least until the next publication.
    pub id: Id,
    /// Time at which the publication was initiated
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "crate::datetime_schema")]
    pub created: Option<DateTime<Utc>>,
    /// Time at which the publication was completed
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "crate::datetime_schema")]
    pub completed: Option<DateTime<Utc>>,
    /// A brief description of the reason for the publication
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// The final result of the publication
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<publications::JobStatus>,
    /// Errors will be non-empty for publications that were not successful
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<draft_error::Error>,
    /// A touch publication is a publication that does not modify the spec, but
    /// only updates the `built_spec` and `last_build_id` fields. They are most
    /// commonly performed in response to changes in the spec's dependencies.
    /// Touch publications will never be combined with non-touch publications in
    /// the history.
    #[serde(default, skip_serializing_if = "is_false")]
    pub is_touch: bool,
    /// A publication info may represent multiple publications of the same spec.
    /// If the publications have similar outcomes, then multiple publications
    /// can be condensed into a single entry in the history. If this is done,
    /// then the `count` field will be greater than 1. This field is omitted if
    /// the count is 1.
    #[serde(default = "default_count", skip_serializing_if = "is_one")]
    #[schemars(schema_with = "count_schema")]
    pub count: u32,
}

/// Used for publication info serde
fn is_false(b: &bool) -> bool {
    !*b
}

/// Used for publication info serde
fn default_count() -> u32 {
    1
}

/// Used for publication info serde
fn is_one(i: &u32) -> bool {
    *i == 1
}

fn count_schema(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    serde_json::from_value(serde_json::json!({
        "type": "integer",
        "minimum": 1,
    }))
    .unwrap()
}

impl PublicationInfo {
    pub fn is_success(&self) -> bool {
        self.result.as_ref().is_some_and(|s| s.is_success())
    }

    /// Tries to reduce `other` into `self` if the two should be combined in the
    /// history. If `other` cannot be reduced into `self`, then it is returned
    /// unmodified.
    ///
    /// Combining events in the history is a way to cram more information into a
    /// smaller summary, and it helps avoid having repeated publications (like
    /// touch publications, which can number in the hundreds) quickly push out
    /// relevant prior events. But it's important that we _only_ combine
    /// publication entries in the specific cases where we know it won't cause
    /// confusion. Two publications should be combined in the history only if
    /// their final `job_status`es are identical (e.g. both `{"type":
    /// "buildFailed"}`). And then only in one of these cases:
    /// - they are both touch publications
    /// - If they are both _unsuccessful_ non-touch publications (i.e. we never
    ///   combine successful publications that have modified the spec)
    pub fn try_reduce(&mut self, other: PublicationInfo) -> Option<PublicationInfo> {
        if (self.is_touch != other.is_touch)
            || (self.result != other.result)
            || (!self.is_touch && self.is_success())
        {
            return Some(other);
        }
        self.id = other.id;
        self.count += other.count;
        self.completed = other.completed;
        self.errors = other.errors;
        self.detail = other.detail;
        None
    }
}

/// Information on the publications performed by the controller.
/// This does not include any information on user-initiated publications.
#[derive(Debug, Serialize, Deserialize, PartialEq, JsonSchema)]
#[cfg_attr(feature = "async-graphql", derive(async_graphql::SimpleObject))]
pub struct PublicationStatus {
    /// Hash of all of the dependencies of this spec at the time of the last
    /// observation. This is compared against the `dependency_hash` of the live
    /// spec in order to determine whether any of the spec's dependencies have
    /// changed since it was last published. If they have, then the controller
    /// will initiate a touch publication of the spec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dependency_hash: Option<String>,
    /// The publication id at which the controller has last notified dependent
    /// specs. A publication of the controlled spec will cause the controller to
    /// notify the controllers of all dependent specs. When it does so, it sets
    /// `max_observed_pub_id` to the current `last_pub_id`, so that it can avoid
    /// notifying dependent controllers unnecessarily.
    #[serde(default = "Id::zero", skip_serializing_if = "Id::is_zero")]
    pub max_observed_pub_id: Id,
    /// A limited history of publications performed by this controller
    pub history: VecDeque<PublicationInfo>,
}

impl Clone for PublicationStatus {
    fn clone(&self) -> Self {
        PublicationStatus {
            max_observed_pub_id: self.max_observed_pub_id,
            history: self.history.clone(),
            dependency_hash: self.dependency_hash.clone(),
        }
    }
}

impl Default for PublicationStatus {
    fn default() -> Self {
        PublicationStatus {
            dependency_hash: None,
            max_observed_pub_id: Id::zero(),
            history: VecDeque::new(),
        }
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_publication_history_folding() {
        let touch_success = PublicationInfo {
            id: crate::Id::new([1, 1, 1, 1, 1, 1, 1, 1]),
            created: Some("2024-11-11T11:11:11Z".parse().unwrap()),
            completed: Some("2024-11-22T17:01:01Z".parse().unwrap()),
            detail: Some("touch success".to_string()),
            result: Some(publications::JobStatus::Success),
            errors: Vec::new(),
            is_touch: true,
            count: 1,
        };

        // Sucessful touch publications should be combined
        let other_touch_success = PublicationInfo {
            id: crate::Id::new([2, 1, 1, 1, 1, 1, 1, 1]),
            completed: Some("2024-11-22T22:22:22Z".parse().unwrap()),
            created: Some("2024-11-11T11:00:00Z".parse().unwrap()),
            detail: Some("other touch success".to_string()),
            ..touch_success.clone()
        };
        let mut reduced = touch_success.clone();
        assert!(reduced.try_reduce(other_touch_success).is_none());
        assert_eq!(
            reduced,
            PublicationInfo {
                id: crate::Id::new([2, 1, 1, 1, 1, 1, 1, 1]),
                completed: Some("2024-11-22T22:22:22Z".parse().unwrap()),
                created: Some("2024-11-11T11:11:11Z".parse().unwrap()),
                detail: Some("other touch success".to_string()),
                result: Some(publications::JobStatus::Success),
                errors: Vec::new(),
                is_touch: true,
                count: 2,
            }
        );

        let reg_success = PublicationInfo {
            id: crate::Id::new([3, 1, 1, 1, 1, 1, 1, 1]),
            completed: Some("2024-11-23T23:33:33Z".parse().unwrap()),
            created: Some("2024-11-11T11:11:11Z".parse().unwrap()),
            detail: Some("non-touch success".to_string()),
            result: Some(publications::JobStatus::Success),
            errors: Vec::new(),
            is_touch: false,
            count: 1,
        };
        // Touch success and regular success should not be combined
        let mut touch_subject = touch_success.clone();
        assert!(touch_subject.try_reduce(reg_success.clone()).is_some());

        // Successful non-touch publications should never be combined because we
        // want to preserve the history of modifications to the model.
        let mut reg_subject = reg_success.clone();
        assert!(reg_subject
            .try_reduce(PublicationInfo {
                id: crate::Id::new([4, 1, 1, 1, 1, 1, 1, 1]),
                ..reg_success.clone()
            })
            .is_some(),);

        let reg_fail = PublicationInfo {
            id: crate::Id::new([5, 1, 1, 1, 1, 1, 1, 1]),
            completed: Some("2024-12-01T01:55:55Z".parse().unwrap()),
            created: Some("2024-11-11T11:11:11Z".parse().unwrap()),
            detail: Some("reg failure".to_string()),
            result: Some(publications::JobStatus::BuildFailed {
                incompatible_collections: Vec::new(),
                evolution_id: None,
            }),
            errors: vec![draft_error::Error {
                catalog_name: "acmeCo/fail-thing".to_string(),
                scope: None,
                detail: "schmeetail".to_string(),
            }],
            is_touch: false,
            count: 1,
        };

        // A publication with the same unsuccessful status should be combined,
        // and the detail and error should be that of the most recent
        // publication.
        let same_reg_fail = PublicationInfo {
            id: crate::Id::new([5, 1, 1, 1, 1, 1, 1, 1]),
            completed: Some("2024-12-01T01:55:55Z".parse().unwrap()),
            detail: Some("same but different reg failure".to_string()),
            errors: vec![draft_error::Error {
                catalog_name: "acmeCo/fail-thing".to_string(),
                scope: None,
                detail: "a different error".to_string(),
            }],
            ..reg_fail.clone()
        };
        let mut reduced = reg_fail.clone();
        assert!(reduced.try_reduce(same_reg_fail.clone()).is_none());
        assert_eq!(
            reduced,
            PublicationInfo {
                id: crate::Id::new([5, 1, 1, 1, 1, 1, 1, 1]),
                completed: Some("2024-12-01T01:55:55Z".parse().unwrap()),
                created: Some("2024-11-11T11:11:11Z".parse().unwrap()),
                detail: Some("same but different reg failure".to_string()),
                errors: vec![draft_error::Error {
                    catalog_name: "acmeCo/fail-thing".to_string(),
                    scope: None,
                    detail: "a different error".to_string(),
                }],
                result: Some(publications::JobStatus::BuildFailed {
                    incompatible_collections: Vec::new(),
                    evolution_id: None
                }),
                is_touch: false,
                count: 2,
            }
        );

        // A publication with a different status should not be combined
        let diff_reg_fail = PublicationInfo {
            result: Some(publications::JobStatus::BuildFailed {
                incompatible_collections: vec![publications::IncompatibleCollection {
                    collection: "acmeCo/anvils".to_string(),
                    requires_recreation: Vec::new(),
                    affected_materializations: Vec::new(),
                }],
                evolution_id: None,
            }),
            ..same_reg_fail.clone()
        };
        let mut reg_subject = reg_fail.clone();
        assert!(reg_subject.try_reduce(diff_reg_fail).is_some());
    }
}
