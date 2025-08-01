use crate::{status::ShardRef, Id};
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The shape of a connector status, which matches that of an ops::Log.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "async-graphql", derive(async_graphql::SimpleObject))]
#[serde(rename_all = "camelCase")]
pub struct ConnectorStatus {
    /// The shard that last updated the status
    pub shard: ShardRef,
    /// The time at which the status was last updated
    #[schemars(schema_with = "crate::datetime_schema")]
    pub ts: DateTime<Utc>,
    /// The message is meant to be presented to users, and may use Markdown formatting.
    pub message: String,
    /// Arbitrary JSON that can be used to communicate additional details. The
    /// specific fields and their meanings are entirely up to the connector.
    #[serde(default)]
    #[cfg_attr(feature = "async-graphql", graphql(skip))]
    pub fields: serde_json::Map<String, serde_json::Value>,
}

impl ConnectorStatus {
    /// Returns true if this connector status comes from the same build, and was
    /// written after the `last_activation_ts`. If both of those conditions are
    /// met, then we can assume that the status information is up to date and
    /// relevant, barring any bugs in the connector itself.
    pub fn is_current(&self, last_build_id: Id, last_activation_ts: DateTime<Utc>) -> bool {
        self.shard.build == last_build_id && self.ts > last_activation_ts
    }
}

crate::sqlx_json::sqlx_json!(ConnectorStatus);

/// The shape of a config update event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "async-graphql", derive(async_graphql::SimpleObject))]
#[serde(rename_all = "camelCase")]
pub struct ConfigUpdate {
    /// The specific shard that emitted the config update event.
    pub shard: ShardRef,
    /// The time at which the config update was emitted.
    #[schemars(schema_with = "crate::datetime_schema")]
    pub ts: DateTime<Utc>,
    /// The message is meant to be presented to users, and may use Markdown formatting.
    pub message: String,
    /// Arbitrary JSON that can be used to communicate additional details. The
    /// specific fields and their meanings are up to the connector, except for
    /// the flow `/events` fields: `eventType`, `eventTarget`, and `error`, which
    /// are restricted to string values and `config` which is restricted to the
    /// the updated config.
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    #[cfg_attr(feature = "async-graphql", graphql(skip))]
    pub fields: serde_json::Map<String, serde_json::Value>,
}

crate::sqlx_json::sqlx_json!(ConfigUpdate);
