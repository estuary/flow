use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};

mod collection;
mod journal;
mod reference;
mod schema_support;
mod shards;

pub use collection::PartitionSelector;
pub use journal::{
    BucketType, CompressionCodec, FragmentTemplate, JournalTemplate, StorageMapping, Store,
};
pub use reference::{
    Capture, Collection, CompositeKey, Field, JsonPointer, Materialization, Object, Prefix, Rule,
    Test, Transform,
};
pub use shards::ShardTemplate;

fn duration_schema(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    from_value(json!({
        "type": ["string", "null"],
        "pattern": "^\\d+(s|m|h)$"
    }))
    .unwrap()
}

fn is_false(b: &bool) -> bool {
    !*b
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
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "Lambda::example_typescript")]
#[schemars(example = "Lambda::example_remote")]
pub enum Lambda {
    Typescript,
    Remote(String),
}
