use super::Collection;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};
use std::collections::BTreeMap;

/// A source collection and details of how it's read.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(untagged, deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "Source::example")]
pub enum Source {
    Source(FullSource),
    Collection(Collection),
}

/// A source collection and details of how it's read.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "FullSource::example")]
pub struct FullSource {
    /// # Name of the collection to be read.
    pub name: Collection,
    /// # Selector over partition of the source collection to read.
    #[schemars(example = "PartitionSelector::example")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partitions: Option<PartitionSelector>,
    // TODO(johnny): Add `not_before`, `not_after` ?
}

impl FullSource {
    pub fn example() -> Self {
        Self {
            name: Collection::new("source/collection"),
            partitions: None,
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
}

impl Into<FullSource> for Source {
    fn into(self) -> FullSource {
        match self {
            Self::Collection(name) => FullSource {
                name,
                partitions: None,
            },
            Self::Source(source) => source,
        }
    }
}

/// Partition selectors identify a desired subset of the
/// available logical partitions of a collection.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "PartitionSelector::example")]
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
