use serde_json::{from_value, json};

pub mod build;
pub mod collate;
pub mod tables;

mod captures;
mod catalogs;
mod collections;
mod config;
mod derivations;
mod journals;
mod labels;
mod materializations;
mod references;
mod resources;
mod schemas;
mod shards;
mod shuffles;
mod tests;

pub use captures::{CaptureBinding, CaptureDef, CaptureEndpoint};
pub use catalogs::Catalog;
pub use collections::{CollectionDef, Projection};
pub use config::{Config, ConnectorConfig, RawConnectorConfig};
pub use derivations::{Derivation, Publish, Register, TransformDef, TransformSource, Update};
pub use journals::{
    BucketType, CompressionCodec, FragmentTemplate, JournalTemplate, StorageDef, Store,
};
pub use materializations::{
    MaterializationBinding, MaterializationDef, MaterializationEndpoint, MaterializationFields,
    SqliteConfig,
};
pub use references::{
    Capture, Collection, CompositeKey, Field, JsonPointer, Materialization, PartitionField, Prefix,
    RelativeUrl, Test, Transform,
};
pub use resources::{ContentFormat, ContentType, Import, ResourceDef};
pub use schemas::Schema;
pub use shards::ShardTemplate;
pub use shuffles::{Lambda, PartitionSelector, Shuffle};
pub use tests::{TestDocuments, TestStep, TestStepIngest, TestStepVerify};

/// Object is an alias for a JSON object.
pub type Object = serde_json::Map<String, serde_json::Value>;

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
