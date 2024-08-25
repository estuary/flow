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

use std::collections::BTreeSet;

pub use crate::labels::{Label, LabelSelector, LabelSet};
pub use captures::{AutoDiscover, CaptureBinding, CaptureDef, CaptureEndpoint};
pub use catalogs::Catalog;
pub use collections::{CollectionDef, Projection};
pub use connector::{split_image_tag, ConnectorConfig, LocalConfig};
pub use derivation::{Derivation, DeriveUsing, Shuffle, ShuffleType, TransformDef};
pub use derive_sqlite::DeriveUsingSqlite;
pub use derive_typescript::DeriveUsingTypescript;
pub use id::{Id, IdGenerator};
pub use journals::{
    AzureStorageConfig, CompressionCodec, CustomStore, FragmentTemplate, GcsBucketAndPrefix,
    JournalTemplate, S3StorageConfig, StorageDef, Store, AZURE_CONTAINER_RE,
    AZURE_STORAGE_ACCOUNT_RE, GCS_BUCKET_RE, S3_BUCKET_RE,
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
    /// Source collections read by this specification.
    /// Disabled bindings must be excluded from the iterator.
    fn sources(&self) -> impl Iterator<Item = &Source>;
    /// Target collections written to by this specification.
    /// Disabled bindings must be excluded from the iterator.
    fn targets(&self) -> impl Iterator<Item = &Collection>;

    /// Returns the `sources` of this spec as an owned set.
    fn reads_from(&self) -> BTreeSet<Collection> {
        self.sources().map(|s| s.collection().clone()).collect()
    }

    /// Returns the `targets` of this spec as an owned set.
    fn writes_to(&self) -> BTreeSet<Collection> {
        self.targets().cloned().collect()
    }

    fn catalog_type(&self) -> CatalogType;

    /// Returns true if the task shards are enabled.
    fn is_enabled(&self) -> bool;

    /// The full connector image name used by this specificiation, including the tag.
    fn connector_image(&self) -> Option<&str>;

    /// If this spec is a materialization, returns the value of `source_capture`.
    /// This function is admittedly a little smelly, but it's included in the trait
    /// so that we can generically get all the dependencies of each spec.
    fn materialization_source_capture(&self) -> Option<Capture> {
        None
    }

    /// Returns all the dependencies of the spec as a set of strings.
    fn all_dependencies(&self) -> BTreeSet<String> {
        let mut deps: BTreeSet<String> = self.reads_from().into_iter().map(|c| c.into()).collect();
        deps.extend(self.writes_to().into_iter().map(|c| c.into()));
        deps.extend(
            self.materialization_source_capture()
                .into_iter()
                .map(|c| c.into()),
        );
        deps
    }
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AnySpec {
    Capture(CaptureDef),
    Collection(CollectionDef),
    Materialization(MaterializationDef),
    Test(TestDef),
}

impl AnySpec {
    pub fn to_raw_value(&self) -> crate::RawValue {
        match self {
            Self::Capture(model) => {
                crate::RawValue::from_string(serde_json::to_string(model).unwrap()).unwrap()
            }
            Self::Collection(model) => {
                crate::RawValue::from_string(serde_json::to_string(model).unwrap()).unwrap()
            }
            Self::Materialization(model) => {
                crate::RawValue::from_string(serde_json::to_string(model).unwrap()).unwrap()
            }
            Self::Test(model) => {
                crate::RawValue::from_string(serde_json::to_string(model).unwrap()).unwrap()
            }
        }
    }

    pub fn deserialize(catalog_type: CatalogType, json: &str) -> serde_json::Result<AnySpec> {
        match catalog_type {
            CatalogType::Capture => Ok(AnySpec::Capture(serde_json::from_str(json)?)),
            CatalogType::Collection => Ok(AnySpec::Collection(serde_json::from_str(json)?)),
            CatalogType::Materialization => {
                Ok(AnySpec::Materialization(serde_json::from_str(json)?))
            }
            CatalogType::Test => Ok(AnySpec::Test(serde_json::from_str(json)?)),
        }
    }

    pub fn catalog_type(&self) -> CatalogType {
        match self {
            AnySpec::Capture(_) => CatalogType::Capture,
            AnySpec::Collection(_) => CatalogType::Collection,
            AnySpec::Materialization(_) => CatalogType::Materialization,
            AnySpec::Test(_) => CatalogType::Test,
        }
    }

    pub fn as_capture(&self) -> Option<&CaptureDef> {
        if let AnySpec::Capture(cap) = self {
            Some(cap)
        } else {
            None
        }
    }

    pub fn as_collection(&self) -> Option<&CollectionDef> {
        if let AnySpec::Collection(col) = self {
            Some(col)
        } else {
            None
        }
    }

    pub fn as_materialization(&self) -> Option<&MaterializationDef> {
        if let AnySpec::Materialization(mat) = self {
            Some(mat)
        } else {
            None
        }
    }

    pub fn as_test(&self) -> Option<&TestDef> {
        if let AnySpec::Test(test) = self {
            Some(test)
        } else {
            None
        }
    }
}

impl From<CaptureDef> for AnySpec {
    fn from(value: CaptureDef) -> Self {
        AnySpec::Capture(value)
    }
}
impl From<CollectionDef> for AnySpec {
    fn from(value: CollectionDef) -> Self {
        AnySpec::Collection(value)
    }
}
impl From<MaterializationDef> for AnySpec {
    fn from(value: MaterializationDef) -> Self {
        AnySpec::Materialization(value)
    }
}
impl From<TestDef> for AnySpec {
    fn from(value: TestDef) -> Self {
        AnySpec::Test(value)
    }
}

impl ModelDef for AnySpec {
    fn reads_from(&self) -> BTreeSet<Collection> {
        match self {
            AnySpec::Capture(_) => BTreeSet::new(),
            AnySpec::Collection(c) => c.reads_from(),
            AnySpec::Materialization(m) => m.reads_from(),
            AnySpec::Test(t) => t.reads_from(),
        }
    }

    fn writes_to(&self) -> BTreeSet<Collection> {
        match self {
            AnySpec::Capture(c) => c.writes_to(),
            AnySpec::Collection(_) => BTreeSet::new(),
            AnySpec::Materialization(_) => BTreeSet::new(),
            AnySpec::Test(t) => t.writes_to(),
        }
    }

    fn is_enabled(&self) -> bool {
        match self {
            AnySpec::Capture(c) => c.is_enabled(),
            AnySpec::Collection(c) => c.is_enabled(),
            AnySpec::Materialization(m) => m.is_enabled(),
            AnySpec::Test(t) => t.is_enabled(),
        }
    }

    fn materialization_source_capture(&self) -> Option<Capture> {
        match self {
            AnySpec::Materialization(m) => m.materialization_source_capture(),
            _ => None,
        }
    }

    fn connector_image(&self) -> Option<&str> {
        match self {
            AnySpec::Capture(c) => c.connector_image(),
            AnySpec::Collection(c) => c.connector_image(),
            AnySpec::Materialization(m) => m.connector_image(),
            AnySpec::Test(t) => t.connector_image(),
        }
    }

    fn catalog_type(&self) -> CatalogType {
        match self {
            AnySpec::Capture(_) => CatalogType::Capture,
            AnySpec::Collection(_) => CatalogType::Collection,
            AnySpec::Materialization(_) => CatalogType::Materialization,
            AnySpec::Test(_) => CatalogType::Test,
        }
    }

    // AnySpec does not implement `sources` or `targets` because the borrowed return type
    // can't be abstracted over. This is gross, but it'll probably be better to refactor
    // how `ModelDef` surfaces dependencies as opposed to doing backbends to make these
    // functions work.
    fn sources(&self) -> impl Iterator<Item = &Source> {
        unimplemented!("AnySpec does not implement sources()");
        // This is silly, but it keeps the compiler happy
        #[allow(unreachable_code)]
        std::iter::empty()
    }

    fn targets(&self) -> impl Iterator<Item = &Collection> {
        unimplemented!("AnySpec does not implement targets()");
        #[allow(unreachable_code)]
        std::iter::empty()
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
