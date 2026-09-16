use super::{
    CompositeKey, ConnectorConfig, DeriveUsingPython, DeriveUsingSqlite, DeriveUsingTypescript,
    JsonPointer, LocalConfig, RawValue, Secret, ShardTemplate, Source, Transform,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};
use std::collections::BTreeMap;
use std::time::Duration;

/// Derive specifies how a collection is derived from other collections.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Derivation {
    /// # The selected runtime for this derivation.
    pub using: DeriveUsing,
    /// # Secrets which are merged into the `using` configuration.
    /// Maps the name of a secret to a JSON pointer within the endpoint
    /// configuration. On task start, the resolved value is applied into
    /// the specified location as an RFC 7396 JSON merge patch.
    ///
    /// A configuration using `secrets` may not also be sealed with `sops`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub secrets: BTreeMap<Secret, JsonPointer>,
    /// # Transforms which make up this derivation.
    pub transforms: Vec<TransformDef>,
    /// # Key component types of the shuffle keys used by derivation lambdas.
    /// Typically you omit this and Flow infers it from your transform shuffle keys.
    /// In some circumstances, Flow may require that you explicitly tell it of
    /// your shuffled key types.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub shuffle_key_types: Vec<ShuffleType>,
    /// # Salt used for redacting sensitive fields in derived documents.
    /// When provided, this base64-encoded salt is used instead of a generated one.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::serde_opt_bytes"
    )]
    #[schemars(with = "String")]
    pub redact_salt: Option<bytes::Bytes>,
    /// # Template for shards of this derivation task.
    #[serde(default, skip_serializing_if = "ShardTemplate::is_empty")]
    pub shards: ShardTemplate,
}

/// A derivation runtime implementation.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum DeriveUsing {
    /// # A Connector.
    Connector(ConnectorConfig),
    /// # A SQLite derivation.
    Sqlite(DeriveUsingSqlite),
    /// # A TypeScript derivation.
    Typescript(DeriveUsingTypescript),
    /// # A Python derivation.
    Python(DeriveUsingPython),
    /// # A local command (development only).
    Local(LocalConfig),
}

/// A Transform reads and shuffles documents of a source collection,
/// and processes each document through either one or both of a register
/// "update" lambda and a derived document "publish" lambda.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = TransformDef::example())]
pub struct TransformDef {
    /// # Name of this transformation.
    /// The names of transforms within a derivation must be unique and stable.
    pub name: Transform,
    /// # Source collection read by this transform.
    pub source: Source,
    /// # Shuffle by which source documents are mapped to processing shards.
    pub shuffle: Shuffle,
    /// # Priority applied to documents processed by this transform.
    /// When all transforms are of equal priority, Flow processes documents
    /// according to their associated publishing time, as encoded in the
    /// document UUID.
    ///
    /// However, when one transform has a higher priority than others,
    /// then *all* ready documents are processed through the transform
    /// before *any* documents of other transforms are processed.
    ///
    /// Priority may be negative, which de-prioritizes a transform with
    /// respect to transforms having the default priority of zero.
    #[serde(default, skip_serializing_if = "TransformDef::priority_is_zero")]
    pub priority: i32,
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
    /// # Whether to disable this transform.
    /// Disabled transforms are completely ignored at runtime and are not validated.
    #[serde(default, skip_serializing_if = "super::is_false")]
    pub disable: bool,
    /// # Backfill counter for this transform.
    /// Every increment of this counter will result in a new backfill of this
    /// transform. Specifically, the transform's lambda will be re-invoked for
    /// every applicable document of its source collection.
    ///
    /// Note that a backfill does *not* truncate the derived collection,
    /// and documents published by a backfilled transform will coexist with
    /// (and be ordered after) any documents which were published as part
    /// of a preceding backfill.
    #[serde(default, skip_serializing_if = "super::is_u32_zero")]
    pub backfill: u32,
}

/// A Shuffle specifies how a shuffling key is to be extracted from
/// collection documents.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = Shuffle::example())]
pub enum Shuffle {
    /// # A Document may be shuffled to any task shard.
    /// Use 'any' if your transformation does not rely on internal task state,
    /// or if your derivation is not intended to scale beyond a single shard.
    Any,
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
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
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
            "shuffle": "any",
        }))
        .unwrap()
    }

    fn priority_is_zero(p: &i32) -> bool {
        *p == 0
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn secrets_stanza_round_trips() {
        let fixture = json!({
            "using": {"connector": {"image": "an/image", "config": {}}},
            "secrets": {
                "acmeCo/widgets/password": "/credentials/password",
                "acmeCo/widgets/whole-config": "",
            },
            "transforms": [],
        });
        let model: Derivation = serde_json::from_value(fixture.clone()).unwrap();

        // Secret names are unique and canonicalized into lexicographic order.
        assert_eq!(
            model
                .secrets
                .keys()
                .map(AsRef::as_ref)
                .collect::<Vec<&str>>(),
            ["acmeCo/widgets/password", "acmeCo/widgets/whole-config"],
        );
        assert_eq!(serde_json::to_value(&model).unwrap(), fixture);

        // An empty stanza is omitted entirely.
        let model = Derivation {
            secrets: BTreeMap::new(),
            ..model
        };
        assert!(
            serde_json::to_value(&model)
                .unwrap()
                .get("secrets")
                .is_none()
        );
    }
}
