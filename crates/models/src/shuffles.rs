use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};
use std::collections::BTreeMap;

use super::CompositeKey;

/// A Shuffle specifies how a shuffling key is to be extracted from
/// collection documents.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "Shuffle::example")]
pub enum Shuffle {
    /// Shuffle by extracting the given fields.
    Key(CompositeKey),
    /// Invoke the lambda for each source document,
    /// and shuffle on its returned key.
    Lambda(Lambda),
}

impl Shuffle {
    pub fn example() -> Self {
        Self::Key(CompositeKey::example())
    }
}

/// Lambdas are user functions which are invoked by the Flow runtime to
/// process and transform source collection documents into derived collections.
/// Flow supports multiple lambda run-times, with a current focus on TypeScript
/// and remote HTTP APIs.
///
/// TypeScript lambdas are invoked within on-demand run-times, which are
/// automatically started and scaled by Flow's task distribution in order
/// to best co-locate data and processing, as well as to manage fail-over.
///
/// Remote lambdas may be called from many Flow tasks, and are up to the
/// API provider to provision and scale.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "Lambda::example_typescript")]
#[schemars(example = "Lambda::example_remote")]
#[schemars(example = "Lambda::example_sqlite")]
pub enum Lambda {
    Typescript,
    Remote(String),
    Sql(String),
}

impl Lambda {
    pub fn example_typescript() -> Self {
        Self::Typescript
    }
    pub fn example_remote() -> Self {
        Self::Remote("http://example/api".to_string())
    }
    pub fn example_sqlite() -> Self {
        Self::Sql("SELECT foo, bar FROM source;".to_string())
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
