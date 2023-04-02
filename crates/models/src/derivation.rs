use super::{
    CompositeKey, ConnectorConfig, DeriveUsingSqlite, DeriveUsingTypescript, RawValue,
    ShardTemplate, Source, Transform,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};
use std::time::Duration;

/// Derive specifies how a collection is derived from other collections.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Derivation {
    /// # The selected runtime for this derivation.
    pub using: DeriveUsing,
    /// # Transforms which make up this derivation.
    pub transforms: Vec<TransformDef>,
    /// # Key component types of the shuffle keys used by derivation lambdas.
    /// Typically you omit this and Flow infers it from your transform shuffle keys.
    /// In some circumstances, Flow may require that you explicitly tell it of
    /// your shuffled key types.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub shuffle_key_types: Vec<ShuffleType>,
    /// # Template for shards of this derivation task.
    #[serde(default, skip_serializing_if = "ShardTemplate::is_empty")]
    pub shards: ShardTemplate,
}

/// A derivation runtime implementation.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum DeriveUsing {
    /// # A Connector.
    Connector(ConnectorConfig),
    /// # A SQLite derivation.
    Sqlite(DeriveUsingSqlite),
    /// # A TypeScript derivation.
    Typescript(DeriveUsingTypescript),
}

/// A Transform reads and shuffles documents of a source collection,
/// and processes each document through either one or both of a register
/// "update" lambda and a derived document "publish" lambda.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "TransformDef::example")]
pub struct TransformDef {
    /// # Name of this transformation.
    /// The names of transforms within a derivation must be unique and stable.
    pub name: Transform,
    /// # Source collection read by this transform.
    pub source: Source,
    /// # Shuffle by which source documents are mapped to processing shards.
    /// If empty, the key of the source collection is used.
    #[schemars(example = "Shuffle::example")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shuffle: Option<Shuffle>,
    /// # Priority applied to documents processed by this transform.
    /// When all transforms are of equal priority, Flow processes documents
    /// according to their associated publishing time, as encoded in the
    /// document UUID.
    ///
    /// However, when one transform has a higher priority than others,
    /// then *all* ready documents are processed through the transform
    /// before *any* documents of other transforms are processed.
    #[serde(default, skip_serializing_if = "TransformDef::priority_is_zero")]
    pub priority: u32,
    /// # Delay applied to documents processed by this transform.
    /// Delays are applied as an adjustment to the UUID clock encoded within each
    /// document, which is then used to impose a relative ordering of all documents
    /// read by this derivation. This means that read delays are applied in a
    /// consistent way, even when back-filling over historical documents. When caught
    /// up and tailing the source collection, delays also "gate" documents such that
    /// they aren't processed until the current wall-time reflects the delay.
    #[schemars(schema_with = "super::duration_schema")]
    #[serde(
        default,
        with = "humantime_serde",
        skip_serializing_if = "Option::is_none"
    )]
    pub read_delay: Option<Duration>,
    /// # Lambda applied to the sourced documents of this transform.
    /// Lambdas may be provided inline,
    /// or as a relative URL to a file containing the lambda.
    #[serde(default, skip_serializing_if = "RawValue::is_null")]
    pub lambda: RawValue,
}

/// A Shuffle specifies how a shuffling key is to be extracted from
/// collection documents.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "Shuffle::example")]
pub enum Shuffle {
    /// # Key which identifies fields of sourced documents to extract and shuffle upon.
    Key(CompositeKey),
    /// # Lambda which extracts a shuffle key from the sourced documents of this transform.
    /// Lambdas may be provided inline, or as a relative URL to a file containing the lambda.
    Lambda(RawValue),
}

impl Shuffle {
    pub fn example() -> Self {
        Self::Key(CompositeKey::example())
    }
}

/// Type of a shuffled key component.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum ShuffleType {
    Boolean,
    Integer,
    String,
}

impl TransformDef {
    fn example() -> Self {
        from_value(json!({
            "name": "my-transform",
            "source": "some/source/collection",
        }))
        .unwrap()
    }
    fn priority_is_zero(p: &u32) -> bool {
        *p == 0
    }
}
