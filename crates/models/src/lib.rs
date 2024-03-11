pub mod collate;

mod captures;
mod catalogs;
mod collections;
mod connector;
mod derivation;
mod derive_sqlite;
mod derive_typescript;
mod journals;
mod labels;
mod materializations;
mod raw_value;
mod references;
mod schemas;
mod shards;
mod source;
mod tests;

pub use crate::labels::{Label, LabelSelector, LabelSet};
pub use captures::{AutoDiscover, CaptureBinding, CaptureDef, CaptureEndpoint};
pub use catalogs::Catalog;
pub use collections::{CollectionDef, Projection};
pub use connector::{ConnectorConfig, LocalConfig};
pub use derivation::{Derivation, DeriveUsing, Shuffle, ShuffleType, TransformDef};
pub use derive_sqlite::DeriveUsingSqlite;
pub use derive_typescript::DeriveUsingTypescript;
pub use journals::{
    CompressionCodec, CustomStore, FragmentTemplate, JournalTemplate, S3StorageConfig, StorageDef,
    Store, AZURE_CONTAINER_RE, AZURE_STORAGE_ACCOUNT_RE, GCS_BUCKET_RE, S3_BUCKET_RE,
};
pub use materializations::{
    MaterializationBinding, MaterializationDef, MaterializationEndpoint, MaterializationFields,
    SqliteConfig,
};
pub use raw_value::RawValue;
pub use references::{
    Capture, Collection, CompositeKey, Field, JsonPointer, Materialization, PartitionField, Prefix,
    RelativeUrl, StorageEndpoint, Test, Transform, CATALOG_PREFIX_RE, TOKEN_RE,
};
pub use schemas::Schema;
pub use shards::ShardTemplate;
pub use source::{FullSource, PartitionSelector, Source};
pub use tests::{TestDocuments, TestStep, TestStepIngest, TestStepVerify};

fn duration_schema(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    serde_json::from_value(serde_json::json!({
        "type": ["string", "null"],
        "pattern": "^\\d+(s|m|h)$"
    }))
    .unwrap()
}

fn option_datetime_schema(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    serde_json::from_value(serde_json::json!({
        "type": ["string", "null"],
        "format": "date-time",
    }))
    .unwrap()
}

fn is_false(b: &bool) -> bool {
    !*b
}

fn is_u32_zero(u: &u32) -> bool {
    *u == 0
}
