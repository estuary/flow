use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// A ShardTemplate configures how shards process a catalog task.
#[derive(Serialize, Deserialize, Debug, Default, JsonSchema, Clone, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = ShardTemplate::example())]
pub struct ShardTemplate {
    /// # Disable processing of the task's shards.
    #[serde(default, skip_serializing_if = "super::is_false")]
    pub disable: bool,
    /// # Minimum duration of task transactions.
    /// This duration lower-bounds the amount of time during which a transaction
    /// must process documents before it must flush and commit.
    /// It may run for more time if additional documents are available.
    /// The default value is zero seconds.
    /// Larger values may result in more data reduction, at the cost of
    /// more latency.
    /// EXPERIMENTAL: this field MAY be removed.
    #[serde(
        default,
        with = "humantime_serde",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(schema_with = "super::duration_schema")]
    pub min_txn_duration: Option<std::time::Duration>,
    /// # Maximum duration of task transactions.
    /// This duration upper-bounds the amount of time during which a transaction
    /// may process documents before it must flush and commit.
    /// It may run for less time if there aren't additional ready documents for
    /// it to process.
    /// If not set, the maximum duration defaults to five minutes for materializations,
    /// and one second for captures and derivations.
    /// EXPERIMENTAL: this field MAY be removed.
    #[serde(
        default,
        with = "humantime_serde",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(schema_with = "super::duration_schema")]
    pub max_txn_duration: Option<std::time::Duration>,
    /// # Number of hot standbys to keep for each task shard.
    /// Hot standbys of a shard actively replicate the shard's state to another
    /// machine, and are able to be quickly promoted to take over processing for
    /// the shard should its current primary fail.
    /// If not set, then no hot standbys are maintained.
    /// EXPERIMENTAL: this field MAY be removed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "u32")]
    pub hot_standbys: Option<u32>,
    /// # Size of the ring buffer used to sequence documents for exactly-once semantics.
    /// The ring buffer is a performance optimization only:
    /// catalog tasks will replay portions of journals as
    /// needed when messages aren't available in the buffer.
    /// It can remain small if upstream task transactions are small,
    /// but larger transactions will achieve better performance with a
    /// larger ring.
    /// If not set, a reasonable default (currently 65,536) is used.
    /// EXPERIMENTAL: this field is LIKELY to be removed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "u32")]
    pub ring_buffer_size: Option<u32>,
    /// # Size of the reader channel used for decoded documents.
    /// Larger values are recommended for tasks having more than one
    /// shard split and long, bursty transaction durations.
    /// If not set, a reasonable default (currently 4,096) is used.
    /// EXPERIMENTAL: this field is LIKELY to be removed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "u32")]
    pub read_channel_size: Option<u32>,
    /// # Log level of this tasks's shards.
    /// Log levels may currently be "error", "warn", "info", "debug", or "trace".
    /// If not set, the effective log level is "info".
    // NOTE(johnny): We're not making this an enum because it's likely
    // we'll introduce a modular logging capability.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "String")]
    pub log_level: Option<String>,
}

impl ShardTemplate {
    pub fn example() -> Self {
        Self {
            max_txn_duration: Some(Duration::from_secs(30)),
            hot_standbys: Some(1),
            ..Default::default()
        }
    }
    pub fn is_empty(&self) -> bool {
        let ShardTemplate {
            disable,
            min_txn_duration: o1,
            max_txn_duration: o2,
            hot_standbys: o3,
            ring_buffer_size: o4,
            read_channel_size: o5,
            log_level: o6,
        } = self;

        !disable
            && o1.is_none()
            && o2.is_none()
            && o3.is_none()
            && o4.is_none()
            && o5.is_none()
            && o6.is_none()
    }
}
