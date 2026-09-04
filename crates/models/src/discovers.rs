use std::collections::BTreeMap;

use crate::{Id, ResourcePath};

/// Represents a capture binding that was added, removed, or modified by a
/// discover.
#[derive(Debug, PartialEq, Clone)]
pub struct Changed {
    /// The name of the target collection for the binding.
    pub target: crate::Collection,
    /// Whether the binding is disabled.
    pub disable: bool,
    /// Optional reason describing a non-obvious change that was made.
    pub reason: Option<String>,
}
/// Represents a set of changes resulting from a discover.
pub type Changes = BTreeMap<ResourcePath, Changed>;

/// JobStatus is the possible outcomes of a handled discover operation.
///
/// It is stored verbatim in `discovers.job_status`, which PostgREST clients
/// read directly, so the camelCase `type` tags are a wire contract.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum JobStatus {
    Queued,
    WrongProtocol,
    TagFailed,
    ImageForbidden,
    PullFailed,
    DiscoverFailed,
    MergeFailed,
    Success {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        publication_id: Option<Id>,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        specs_unchanged: bool,
    },
    DeprecatedBackground,
    NoDataPlane,
    NotAuthorized,
}

impl JobStatus {
    pub fn is_success(&self) -> bool {
        matches!(self, JobStatus::Success { .. })
    }
}

/// Current outcome of a discover operation.
///
/// This is the unit-variant projection of [`JobStatus`], which
/// `async_graphql::Enum` requires and which `JobStatus::Success` (carrying
/// data) cannot satisfy. Its serde names are the same camelCase tags stored in
/// `discovers.job_status`, so a stored `type` parses into it directly; only the
/// GraphQL representation is `SCREAMING_SNAKE_CASE`. Variants are ordered for
/// the published schema rather than in `JobStatus` order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[cfg_attr(
    feature = "async-graphql",
    derive(async_graphql::Enum),
    graphql(rename_items = "SCREAMING_SNAKE_CASE")
)]
#[serde(rename_all = "camelCase")]
pub enum DiscoverStatusType {
    /// The discover has not yet been processed.
    Queued,
    /// Discovered bindings were merged into the draft.
    Success,
    /// The connector's discovery RPC failed.
    DiscoverFailed,
    /// Discovered specs could not be merged with the draft's existing specs.
    MergeFailed,
    /// The connector image could not be pulled.
    PullFailed,
    /// The connector tag has not been successfully processed.
    TagFailed,
    /// The connector is not a capture connector.
    WrongProtocol,
    /// The connector image is not allowed.
    ImageForbidden,
    /// The data plane does not exist or the caller cannot use it.
    NoDataPlane,
    /// The caller does not hold `SpecEdit` on the capture name.
    NotAuthorized,
    /// The discover used the deprecated background flag.
    DeprecatedBackground,
}

impl From<&JobStatus> for DiscoverStatusType {
    fn from(status: &JobStatus) -> Self {
        match status {
            JobStatus::Queued => DiscoverStatusType::Queued,
            JobStatus::Success { .. } => DiscoverStatusType::Success,
            JobStatus::DiscoverFailed => DiscoverStatusType::DiscoverFailed,
            JobStatus::MergeFailed => DiscoverStatusType::MergeFailed,
            JobStatus::PullFailed => DiscoverStatusType::PullFailed,
            JobStatus::TagFailed => DiscoverStatusType::TagFailed,
            JobStatus::WrongProtocol => DiscoverStatusType::WrongProtocol,
            JobStatus::ImageForbidden => DiscoverStatusType::ImageForbidden,
            JobStatus::NoDataPlane => DiscoverStatusType::NoDataPlane,
            JobStatus::NotAuthorized => DiscoverStatusType::NotAuthorized,
            JobStatus::DeprecatedBackground => DiscoverStatusType::DeprecatedBackground,
        }
    }
}

#[cfg(test)]
mod test {
    use super::{DiscoverStatusType, JobStatus};

    // `discovers.job_status` is a UI-facing contract: pin every wire tag, and
    // pin that each tag parses into the DiscoverStatusType that
    // `From<&JobStatus>` yields, since the two are meant to be interchangeable
    // views of the stored `type`. The exhaustive `From` match is the
    // compile-time guard for a new JobStatus variant; this list is the wire
    // guard and must be extended alongside it.
    #[test]
    fn test_job_status_wire_tags() {
        let all = vec![
            JobStatus::Queued,
            JobStatus::WrongProtocol,
            JobStatus::TagFailed,
            JobStatus::ImageForbidden,
            JobStatus::PullFailed,
            JobStatus::DiscoverFailed,
            JobStatus::MergeFailed,
            JobStatus::Success {
                publication_id: None,
                specs_unchanged: false,
            },
            JobStatus::Success {
                publication_id: Some(crate::Id::new([1, 2, 3, 4, 5, 6, 7, 8])),
                specs_unchanged: true,
            },
            JobStatus::DeprecatedBackground,
            JobStatus::NoDataPlane,
            JobStatus::NotAuthorized,
        ];
        let serialized = serde_json::to_value(&all).unwrap();
        insta::assert_json_snapshot!("job-status-wire-tags", serialized);

        // Every tag round-trips to a variant that serializes identically.
        let round: Vec<JobStatus> = serde_json::from_value(serialized.clone()).unwrap();
        assert_eq!(serialized, serde_json::to_value(&round).unwrap());

        let mapping: Vec<(String, DiscoverStatusType)> = all
            .iter()
            .map(|status| {
                let tag = serde_json::to_value(status).unwrap()["type"].clone();
                let parsed: DiscoverStatusType = serde_json::from_value(tag.clone()).unwrap();
                assert_eq!(DiscoverStatusType::from(status), parsed, "tag {tag}");
                (tag.as_str().unwrap().to_string(), parsed)
            })
            .collect();
        insta::assert_debug_snapshot!("job-status-to-discover-status-type", mapping);
    }
}
