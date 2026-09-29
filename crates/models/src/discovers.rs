use std::collections::BTreeMap;

use crate::ResourcePath;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
#[cfg_attr(
    feature = "async-graphql",
    derive(async_graphql::Enum),
    graphql(name = "DiscoverStatus", rename_items = "SCREAMING_SNAKE_CASE")
)]
pub enum JobStatus {
    /// The discover is queued or in progress.
    Queued,
    Success,
    WrongProtocol,
    TagFailed,
    ImageForbidden,
    DiscoverFailed,
    NoDataPlane,
    NotAuthorized,
    // Legacy status values retained for compatibility.
    MergeFailed,
    DeprecatedBackground,
    PullFailed,
}

impl JobStatus {
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Success)
    }
}

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

#[cfg(test)]
mod test {
    use super::JobStatus;

    #[test]
    fn job_status_wire_format() {
        for (status, tag) in [
            (JobStatus::Queued, "queued"),
            (JobStatus::Success, "success"),
            (JobStatus::WrongProtocol, "wrongProtocol"),
            (JobStatus::TagFailed, "tagFailed"),
            (JobStatus::ImageForbidden, "imageForbidden"),
            (JobStatus::DiscoverFailed, "discoverFailed"),
            (JobStatus::NoDataPlane, "noDataPlane"),
            (JobStatus::NotAuthorized, "notAuthorized"),
            (JobStatus::MergeFailed, "mergeFailed"),
            (JobStatus::DeprecatedBackground, "deprecatedBackground"),
            (JobStatus::PullFailed, "pullFailed"),
        ] {
            let json = serde_json::json!({ "type": tag });
            assert_eq!(serde_json::to_value(status).unwrap(), json);
            assert_eq!(serde_json::from_value::<JobStatus>(json).unwrap(), status);
        }

        // Legacy success records can include publication fields.
        assert_eq!(
            serde_json::from_value::<JobStatus>(serde_json::json!({
                "type": "success",
                "publication_id": "0123456789abcdef",
                "specs_unchanged": true
            }))
            .unwrap(),
            JobStatus::Success
        );
        assert!(
            serde_json::from_value::<JobStatus>(serde_json::json!({ "type": "unknown" })).is_err()
        );
    }
}
