use super::Collection;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};
use std::collections::BTreeMap;

// Note that OnIncompatibleSchemaChange is currently only used with materializations.
// It theoretically could be useful for derivations, but that's being left for future work.
/// Determines how to handle incompatible schema changes for a given binding.
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(example = OnIncompatibleSchemaChange::example())]
pub enum OnIncompatibleSchemaChange {
    /// Fail the publication of the incompatible schema change. This prevents any schema change
    /// from being applied if it is incompatible with the existing schema, as determined by the
    /// connector.
    Abort,
    /// Increment the backfill counter of the binding, causing it to start over from the beginning.
    Backfill,
    /// Disable the binding, which will be effectively excluded from the task until it is re-enabled.
    DisableBinding,
    /// Disable the entire task, preventing it from running until it is re-enabled.
    DisableTask,
}

impl Default for OnIncompatibleSchemaChange {
    fn default() -> Self {
        OnIncompatibleSchemaChange::Backfill
    }
}

impl OnIncompatibleSchemaChange {
    pub fn example() -> Self {
        OnIncompatibleSchemaChange::Backfill
    }

    pub fn is_default(&self) -> bool {
        self == &OnIncompatibleSchemaChange::default()
    }
}

/// A source collection and details of how it's read.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(untagged, deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = Source::example())]
pub enum Source {
    Source(FullSource),
    Collection(Collection),
}

/// A source collection and details of how it's read.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = FullSource::example())]
pub struct FullSource {
    /// # Name of the collection to be read.
    pub name: Collection,
    /// # Selector over partition of the source collection to read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "PartitionSelector", example = PartitionSelector::example())]
    pub partitions: Option<PartitionSelector>,
    /// # Lower bound date-time for documents which should be processed.
    /// Source collection documents published before this date-time are filtered.
    /// `notBefore` is *only* a filter. Updating its value will not cause Flow
    /// to re-process documents that have already been read.
    /// Optional. Default is to process all documents.
    #[serde(
        with = "time::serde::rfc3339::option",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(schema_with = "super::option_datetime_schema")]
    pub not_before: Option<time::OffsetDateTime>,
    /// # Upper bound date-time for documents which should be processed.
    /// Source collection documents published after this date-time are filtered.
    /// `notAfter` is *only* a filter. Updating its value will not cause Flow
    /// to re-process documents that have already been read.
    /// Optional. Default is to process all documents.
    #[serde(
        with = "time::serde::rfc3339::option",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(schema_with = "super::option_datetime_schema")]
    pub not_after: Option<time::OffsetDateTime>,
}

impl FullSource {
    pub fn example() -> Self {
        Self {
            name: Collection::new("source/collection"),
            partitions: None,
            not_before: None,
            not_after: None,
        }
    }
}

impl Source {
    pub fn example() -> Self {
        Self::Collection(Collection::new("source/collection"))
    }
    pub fn collection(&self) -> &Collection {
        match self {
            Self::Collection(name) => name,
            Self::Source(FullSource { name, .. }) => name,
        }
    }

    pub fn set_collection(&mut self, new_collection: Collection) {
        match self {
            Self::Collection(name) => *name = new_collection,
            Self::Source(FullSource { name, .. }) => *name = new_collection,
        }
    }
}

impl Into<FullSource> for Source {
    fn into(self) -> FullSource {
        match self {
            Self::Collection(name) => FullSource {
                name,
                partitions: None,
                not_before: None,
                not_after: None,
            },
            Self::Source(source) => source,
        }
    }
}

/// Partition selectors identify a desired subset of the
/// available logical partitions of a collection.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = PartitionSelector::example())]
pub struct PartitionSelector {
    /// Partition field names and corresponding values which must be matched
    /// from the Source collection. Only documents having one of the specified
    /// values across all specified partition names will be matched. For example,
    ///   source: [App, Web]
    ///   region: [APAC]
    /// would mean only documents of 'App' or 'Web' source and also occurring
    /// in the 'APAC' region will be processed.
    #[serde(default)]
    pub include: BTreeMap<String, Vec<serde_json::Value>>,
    /// Partition field names and values which are excluded from the source
    /// collection. Any documents matching *any one* of the partition values
    /// will be excluded.
    #[serde(default)]
    pub exclude: BTreeMap<String, Vec<serde_json::Value>>,
}

impl PartitionSelector {
    pub fn example() -> Self {
        from_value(json!({
            "include": {
                "a_partition": ["A", "B"],
            },
            "exclude": {
                "other_partition": [32, 64],
            }
        }))
        .unwrap()
    }
}
