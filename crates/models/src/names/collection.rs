use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};
use std::collections::BTreeMap;

/// Partition selectors identify a desired subset of the
/// available logical partitions of a collection.
#[derive(Serialize, Deserialize, Debug, JsonSchema, Clone)]
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
