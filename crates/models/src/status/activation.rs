use crate::Id;
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::ShardRef;

/// Status of the task shards running in the data-plane. This records information about
/// the activations of builds in the data-plane, including any subsequent re-activations
/// due to shard failures.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct ActivationStatus {
    /// The build id that was last activated in the data plane.
    /// If this is less than the `last_build_id` of the controlled spec,
    /// then an activation is still pending.
    #[serde(default = "Id::zero", skip_serializing_if = "Id::is_zero")]
    pub last_activated: Id,
    /// The time at which the last data plane activation was performed.
    /// This could have been in order to activate a recent publication,
    /// or in response to a shard failure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "crate::option_datetime_schema")]
    pub last_activated_at: Option<DateTime<Utc>>,

    /// The most recent shard failure to have been observed. The presence of a failure here
    /// does not necessarily mean that the shard is currently in a failed state, as it may
    /// have been re-activated since the failure occurred.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure: Option<ShardFailure>,

    /// Count of shard failures that have been observed over the last 24 hours for the currently activated
    /// build. This resets to 0 when a newly published build is activated.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub recent_failure_count: u32,

    /// The next time at which failed task shards will be re-activated. If this is present, then
    /// there has been at least one observed shard failure, which the controller has not yet handled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "crate::option_datetime_schema")]
    pub next_retry: Option<DateTime<Utc>>,
}

fn is_zero(i: &u32) -> bool {
    *i == 0
}

/// The shape of a connector status, which matches that of an ops::Log.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ShardFailure {
    /// The specific shard that failed
    pub shard: ShardRef,
    /// The time at which the failure occurred
    #[schemars(schema_with = "crate::datetime_schema")]
    pub ts: DateTime<Utc>,
    /// The message is meant to be presented to users, and may use Markdown formatting.
    pub message: String,
    /// Arbitrary JSON that can be used to communicate additional details. The
    /// specific fields and their meanings are up to the connector, except for
    /// the flow `/events` fields: `eventType`, `eventTarget`, and `error`, which
    /// are restricted to string values.
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub fields: serde_json::Map<String, serde_json::Value>,
}

crate::sqlx_json::sqlx_json!(ShardFailure);

impl Default for ActivationStatus {
    fn default() -> Self {
        Self {
            last_activated: Id::zero(),
            last_activated_at: None,
            last_failure: None,
            recent_failure_count: 0,
            next_retry: None,
        }
    }
}
