use serde_json::{from_value, json};

pub mod collate;

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

pub use crate::labels::{Label, LabelSelector, LabelSet};
pub use captures::{CaptureBinding, CaptureDef, CaptureEndpoint};
pub use catalogs::Catalog;
pub use collections::{CollectionDef, Projection};
pub use config::{Config, ConnectorConfig, PortSpec};
pub use derivations::{
    Derivation, Publish, Register, TransformDef, TransformSource, TypescriptModule, Update,
};
pub use journals::{
    BucketType, CompressionCodec, FragmentTemplate, JournalTemplate, StorageDef, Store,
};
pub use materializations::{
    MaterializationBinding, MaterializationDef, MaterializationEndpoint, MaterializationFields,
    SqliteConfig,
};
pub use references::{
    Capture, Collection, CompositeKey, Field, JsonPointer, Materialization, PartitionField,
    PortName, Prefix, RelativeUrl, Test, Transform,
};
pub use resources::{ContentType, Import, ResourceDef};
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
