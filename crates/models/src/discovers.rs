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

#[cfg(test)]
mod test {
    use super::JobStatus;

    // `discovers.job_status` is a UI-facing contract: pin every wire tag.
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
    }
}
