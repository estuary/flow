pub mod collate;

mod captures;
mod catalogs;
mod collections;
mod connector;
mod derivation;
mod derive_sqlite;
mod derive_typescript;
mod id;
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
pub use id::{Id, IdGenerator};
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
use serde::{Deserialize, Serialize};
pub use shards::ShardTemplate;
pub use source::{FullSource, OnIncompatibleSchemaChange, PartitionSelector, Source};
pub use tests::{TestDef, TestDocuments, TestStep, TestStepIngest, TestStepVerify};

/// ModelDef is the common trait of top-level Flow specifications.
pub trait ModelDef:
    Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + std::fmt::Debug
{
    // Source collections read by this specification.
    fn sources(&self) -> impl Iterator<Item = &Source>;
    // Target collections written to by this specification.
    fn targets(&self) -> impl Iterator<Item = &Collection>;

    fn catalog_type(&self) -> CatalogType;
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CatalogType {
    Capture,
    Collection,
    Materialization,
    Test,
}

impl std::str::FromStr for CatalogType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "capture" => Ok(CatalogType::Capture),
            "collection" => Ok(CatalogType::Collection),
            "materialization" => Ok(CatalogType::Materialization),
            "test" => Ok(CatalogType::Test),
            _ => Err(()),
        }
    }
}

impl std::fmt::Display for CatalogType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(self.as_ref())
    }
}

impl std::convert::AsRef<str> for CatalogType {
    fn as_ref(&self) -> &str {
        // These strings match what's used by serde, and also match the definitions in the database.
        match *self {
            CatalogType::Capture => "capture",
            CatalogType::Collection => "collection",
            CatalogType::Materialization => "materialization",
            CatalogType::Test => "test",
        }
    }
}

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
