use crate::publications::LockFailure;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A generic error that can be associated with a particular draft spec for a given operation.
#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Clone, JsonSchema)]
#[cfg_attr(feature = "async-graphql", derive(async_graphql::SimpleObject))]
pub struct Error {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub catalog_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    pub detail: String,
}

impl From<LockFailure> for Error {
    fn from(err: LockFailure) -> Self {
        let detail = format!(
            "the expectPubId of spec {:?} {:?} did not match that of the live spec {:?}",
            err.catalog_name, err.expected, err.actual
        );
        Error {
            catalog_name: err.catalog_name,
            detail,
            scope: None,
        }
    }
}
