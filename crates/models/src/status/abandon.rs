use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Status of the abandonment evaluation for a task.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, JsonSchema)]
#[cfg_attr(feature = "async-graphql", derive(async_graphql::SimpleObject))]
pub struct AbandonStatus {
    /// When this spec was last checked for abandonment
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "crate::option_datetime_schema")]
    pub last_evaluated: Option<DateTime<Utc>>,
}
