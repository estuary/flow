use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json, Value};
use std::collections::BTreeMap;
use std::time::Duration;

use super::{Collection, Lambda, PartitionSelector, Schema, ShardTemplate, Shuffle, Transform};

/// A derivation specifies how a collection is derived from other
/// collections. A collection without a derivation is a "captured"
/// collection, into which documents are directly ingested.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Derivation {
    /// # Register configuration of this derivation.
    #[serde(default)]
    pub register: Register,
    /// # Transforms which make up this derivation.
    #[schemars(schema_with = "transforms_schema")]
    pub transform: BTreeMap<Transform, TransformDef>,
    /// # Template for shards of this derivation task.
    #[serde(default)]
    pub shards: ShardTemplate,
}

/// Registers are the internal states of a derivation, which can be read and
/// updated by all of its transformations. They're an important building
/// block for joins, aggregations, and other complex stateful workflows.
///
/// Registers are implemented using JSON-Schemas, often ones with reduction
/// annotations. When reading source documents, every distinct shuffle key
/// by which the source collection is read is mapped to a corresponding
/// register value (or, if no shuffle key is defined, the source collection's
/// key is used instead).
///
/// Then, an "update" lambda of the transformation produces updates which
/// are reduced into the register, and a "publish" lambda reads the current
/// (and previous, if updated) register value.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Register {
    /// # Schema which validates and reduces register documents.
    pub schema: Schema,
    /// # Initial value of a keyed register which has never been updated.
    /// If not specified, the default is "null".
    #[serde(default = "value_null")]
    pub initial: Value,
}

fn value_null() -> Value {
    Value::Null
}

impl Default for Register {
    fn default() -> Self {
        Register {
            schema: Schema::Bool(true),
            initial: Value::Null,
        }
    }
}

/// A Transform reads and shuffles documents of a source collection,
/// and processes each document through either one or both of a register
/// "update" lambda and a derived document "publish" lambda.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "TransformDef::example")]
pub struct TransformDef {
    /// # Source collection read by this transform.
    pub source: TransformSource,
    /// # Priority applied to documents processed by this transform.
    /// When all transforms are of equal priority, Flow processes documents
    /// according to their associated publishing time, as encoded in the
    /// document UUID.
    ///
    /// However, when one transform has a higher priority than others,
    /// then *all* ready documents are processed through the transform
    /// before *any* documents of other transforms are processed.
    #[serde(default)]
    pub priority: u32,
    /// # Delay applied to documents processed by this transform.
    /// Delays are applied as an adjustment to the UUID clock encoded within each
    /// document, which is then used to impose a relative ordering of all documents
    /// read by this derivation. This means that read delays are applied in a
    /// consistent way, even when back-filling over historical documents. When caught
    /// up and tailing the source collection, delays also "gate" documents such that
    /// they aren't processed until the current wall-time reflects the delay.
    #[serde(default, with = "humantime_serde")]
    #[schemars(schema_with = "super::duration_schema")]
    pub read_delay: Option<Duration>,
    /// # Shuffle by which source documents are mapped to registers.
    /// If empty, the key of the source collection is used.
    #[serde(default)]
    #[schemars(example = "Shuffle::example")]
    pub shuffle: Option<Shuffle>,
    /// # Update that maps a source document into register updates.
    #[serde(default)]
    #[schemars(example = "Update::example")]
    pub update: Option<Update>,
    /// # Publish that maps a source document and registers into derived documents of the collection.
    #[serde(default)]
    #[schemars(example = "Publish::example")]
    pub publish: Option<Publish>,
}

impl TransformDef {
    fn example() -> Self {
        from_value(json!({
            "source": TransformSource::example(),
            "publish": Publish::example(),
            "update": null,
        }))
        .unwrap()
    }
}

/// Update lambdas take a source document and transform it into one or more
/// register updates, which are then reduced into the associated register by
/// the runtime. For example these register updates might update counters,
/// or update the state of a "join" window.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "Update::example")]
pub struct Update {
    /// # Lambda invoked by the update.
    pub lambda: Lambda,
}

impl Update {
    fn example() -> Self {
        from_value(json!({
            "lambda": Lambda::example_typescript(),
        }))
        .unwrap()
    }
}

/// Publish lambdas take a source document, a current register and
/// (if there is also an "update" lambda) a previous register, and transform
/// them into one or more documents to be published into a derived collection.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "Publish::example")]
pub struct Publish {
    /// # Lambda invoked by the publish.
    pub lambda: Lambda,
}

impl Publish {
    fn example() -> Self {
        from_value(json!({
            "lambda": Lambda::example_typescript(),
        }))
        .unwrap()
    }
}

/// TransformSource defines a transformation source collection and how it's read.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "TransformSource::example")]
pub struct TransformSource {
    /// # Name of the collection to be read.
    pub name: Collection,
    /// # Optional JSON-Schema to validate against the source collection.
    /// All data in the source collection is already validated against the
    /// schema of that collection, so providing a source schema is only used
    /// for _additional_ validation beyond that.
    ///
    /// This is useful in building "Extract Load Transform" patterns,
    /// where a collection is captured with minimal schema applied (perhaps
    /// because it comes from an uncontrolled third party), and is then
    /// progressively verified as collections are derived.
    /// If None, the principal schema of the collection is used instead.
    #[serde(default)]
    #[schemars(example = "Schema::example_relative")]
    pub schema: Option<Schema>,
    /// # Selector over partition of the source collection to read.
    #[serde(default)]
    #[schemars(example = "PartitionSelector::example")]
    pub partitions: Option<PartitionSelector>,
}

impl TransformSource {
    fn example() -> Self {
        Self {
            name: Collection::new("source/collection"),
            schema: None,
            partitions: None,
        }
    }
}

fn transforms_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    let schema = Transform::json_schema(gen);
    gen.definitions_mut()
        .insert(Transform::schema_name(), schema);

    let schema = TransformDef::json_schema(gen);
    gen.definitions_mut()
        .insert(TransformDef::schema_name(), schema);

    from_value(json!({
        "type": "object",
        "patternProperties": {
            Transform::schema_pattern(): {
                "$ref": format!("#/definitions/{}", TransformDef::schema_name()),
            },
        },
        "additionalProperties": false,
        "example": [{"nameOfTransform": TransformDef::example()}],
    }))
    .unwrap()
}
