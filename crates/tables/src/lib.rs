#[macro_use]
mod macros;
mod behaviors;
mod ext;

use std::str::FromStr;

use macros::*;

#[cfg(feature = "persist")]
pub use macros::{load_tables, persist_tables, SqlTableObj};
#[cfg(feature = "persist")]
use prost::Message;

// Re-exports for users of this crate.
pub use ext::{AnySpec, SpecExt};
pub use itertools::EitherOrBoth;
pub use macros::{Row, Table};

mod draft;
pub use draft::DraftRow;

tables!(
    table Fetches (row Fetch, sql "fetches") {
        // Import depth of this fetch.
        key depth: u32,
        // Fetched resource Url.
        key resource: url::Url,
    }

    table Resources (row Resource, sql "resources") {
        // Url of this resource.
        key resource: url::Url,
        // Content-type of this resource.
        val content_type: proto_flow::flow::ContentType,
        // Byte content of this resource.
        val content: bytes::Bytes,
        // Document content of this resource.
        val content_dom: models::RawValue,
    }

    table Imports (row Import, sql "imports") {
        // Scope is the referring resource and fragment location which caused the import.
        key scope: url::Url,
        // Resource which is imported. Never has a fragment.
        key to_resource: url::Url,
    }

    table StorageMappings (row StorageMapping, sql "storage_mappings") {
        // Catalog prefix to which this storage mapping applies.
        key catalog_prefix: models::Prefix,
        // Scope of the storage mapping.
        val scope: url::Url,
        // Stores for journal fragments under this prefix.
        val stores: Vec<models::Store>,
    }

    table DraftCollections (row DraftCollection, sql "draft_collections") {
        // Name of this collection.
        key catalog_name: models::Collection,
        // Scope of the draft collection.
        val scope: url::Url,
        // Expected last publication ID which built this collection.
        val expect_build_id: Option<models::Id>,
        // Specification of this collection, or None if the collection is being deleted.
        val spec: Option<models::CollectionDef>,
    }

    table DraftCaptures (row DraftCapture, sql "draft_captures") {
        // Name of this capture.
        key catalog_name: models::Capture,
        // Scope of the draft capture.
        val scope: url::Url,
        // Expected last publication ID which built this capture.
        val expect_build_id: Option<models::Id>,
        // Specification of this capture, or None if the capture is being deleted.
        val spec: Option<models::CaptureDef>,
    }

    table DraftMaterializations (row DraftMaterialization, sql "draft_materializations") {
        // Name of this materialization.
        key catalog_name: models::Materialization,
        // Scope of the draft materialization.
        val scope: url::Url,
        // Expected last publication ID which built this materialization.
        val expect_build_id: Option<models::Id>,
        // Specification of this materialization, or None if the materialization is being deleted.
        val spec: Option<models::MaterializationDef>,
    }

    table DraftTests (row DraftTest, sql "draft_tests") {
        // Name of the test.
        key catalog_name: models::Test,
        // Scope of the draft test.
        val scope: url::Url,
        // Expected last publication ID which built this materialization.
        val expect_build_id: Option<models::Id>,
        // Specification of the test.
        val spec: Option<models::TestDef>,
    }

    table LiveCollections (row #[derive(Clone)] LiveCollection, sql "live_collections") {
        // Name of this collection.
        key catalog_name: models::Collection,
        // Id of the live specification within the control plane.
        val live_spec_id: models::Id,
        // Id of the last publication which built this collection.
        val last_build_id: models::Id,
        // User specification of the collection.
        val spec: models::CollectionDef,
        // Last built specification of the collection.
        val built_spec: proto_flow::flow::CollectionSpec,
        // Inferred schema MD5 applied upon the last build of this collection.
        val inferred_schema_md5: Option<String>,
    }

    table LiveCaptures (row #[derive(Clone)] LiveCapture, sql "live_captures") {
        // Name of this capture.
        key catalog_name: models::Capture,
        // Id of the live specification within the control plane.
        val live_spec_id: models::Id,
        // Id of the last publication which built this capture.
        val last_build_id: models::Id,
        // User specification of the collection.
        val spec: models::CaptureDef,
        // Last built specification of the collection.
        val built_spec: proto_flow::flow::CaptureSpec,
    }

    table LiveMaterializations (row #[derive(Clone)] LiveMaterialization, sql "live_materializations") {
        // Name of this materialization.
        key catalog_name: models::Materialization,
        // Id of the live specification within the control plane.
        val live_spec_id: models::Id,
        // Id of the last publication which built this materialization.
        val last_build_id: models::Id,
        // User specification of the materialization.
        val spec: models::MaterializationDef,
        // Last built specification of the collection.
        val built_spec: proto_flow::flow::MaterializationSpec,
    }

    table LiveTests (row #[derive(Clone)] LiveTest, sql "live_tests") {
        // Name of this materialization.
        key catalog_name: models::Test,
        // Id of the live specification within the control plane.
        val live_spec_id: models::Id,
        // Id of the last publication which built this test.
        val last_build_id: models::Id,
        // User specification of the test.
        val spec: models::TestDef,
        // Last built specification of the test.
        val built_spec: proto_flow::flow::TestSpec,
    }

    table InferredSchemas (row #[derive(Clone)] InferredSchema, sql "inferred_schemas") {
        // Collection which this inferred schema reflects.
        key collection_name: models::Collection,
        // Inferred schema of the collection.
        val schema: models::Schema,
        // MD5 content sum of `schema`.
        val md5: String,
    }

    table BuiltCaptures (row BuiltCapture, sql "built_captures") {
        // Name of this capture.
        key catalog_name: models::Capture,
        // Scope of this built capture.
        val scope: url::Url,
        // Validated response which was used to build this spec.
        val validated: proto_flow::capture::response::Validated,
        // Built specification for this capture.
        val spec: proto_flow::flow::CaptureSpec,
    }

    table BuiltCollections (row BuiltCollection, sql "built_collections") {
        // Name of this collection.
        key catalog_name: models::Collection,
        // Scope of this built collection.
        val scope: url::Url,
        // Validated response which was used to build this spec.
        val validated: Option<proto_flow::derive::response::Validated>,
        // Built specification for this collection.
        val spec: proto_flow::flow::CollectionSpec,
        // The md5 sum of the inferred schema at the time that this collection
        // was built. Note that this may be present even if the collection does
        // not actually use the inferred schema. And it may also be missing,
        // even if the collection _does_ use schema inference, for "remote"
        // collections that were resolve dynamically during the build.
        val inferred_schema_md5: Option<String>,
    }

    table BuiltMaterializations (row BuiltMaterialization, sql "built_materializations") {
        // Name of this materialization.
        key catalog_name: models::Materialization,
        // Scope of this built materialization.
        val scope: url::Url,
        // Validated response which was used to build this spec.
        val validated: proto_flow::materialize::response::Validated,
        // Built specification for this materialization.
        val spec: proto_flow::flow::MaterializationSpec,
    }

    table BuiltTests (row BuiltTest, sql "built_tests") {
        // Name of the test case.
        key catalog_name: models::Test,
        // Scope of this built test.
        val scope: url::Url,
        // Built specification for this test case.
        val spec: proto_flow::flow::TestSpec,
    }

    table Errors (row Error, sql "errors") {
        // Scope of this error.
        val scope: url::Url,
        // Error content.
        val error: anyhow::Error,
    }

    table Meta (row Build, sql "meta") {
        val build_config: proto_flow::flow::build_api::Config,
    }
);

pub fn parse_synthetic_scope(url: &url::Url) -> anyhow::Result<(models::CatalogType, String)> {
    if url.scheme() != "flow" {
        return Err(anyhow::anyhow!("expected flow:// URL, got {}", url));
    }
    let Some(host) = url.host_str() else {
        return Err(anyhow::anyhow!(
            "expected flow:// URL with host, got {}",
            url
        ));
    };
    let catalog_type = models::CatalogType::from_str(host)
        .map_err(|_| anyhow::anyhow!("invalid CatalogType {host:?}"))?;
    let catalog_name = url.path().trim_start_matches('/').to_string();
    Ok((catalog_type, catalog_name))
}

/// Generate a synthetic scope URL for a given catalog type and name, for when a meaningful scope
/// URL is otherwise not avaialble.
pub fn synthetic_scope(
    catalog_type: models::CatalogType,
    catalog_name: impl AsRef<str>,
) -> url::Url {
    let url_str = format!("flow://{}/", catalog_type.as_ref());
    let mut url = url::Url::parse(&url_str).unwrap();
    // using set_path for the catalog name ensures that the name gets properly escaped so that the URL is
    // guaranteed to be valid, even if the catalog_name is not.
    url.set_path(catalog_name.as_ref());
    url
}

/// DraftCatalog are tables which are populated by catalog loads of the `sources` crate.
#[derive(Default)]
pub struct DraftCatalog {
    pub captures: DraftCaptures,
    pub collections: DraftCollections,
    pub materializations: DraftMaterializations,
    pub tests: DraftTests,
    pub errors: Errors,
    pub fetches: Fetches,
    pub imports: Imports,
    pub resources: Resources,
    pub storage_mappings: StorageMappings,
}

impl DraftCatalog {
    pub fn to_models_catalog(&self) -> models::Catalog {
        models::Catalog {
            captures: self
                .captures
                .iter()
                .filter_map(|r| r.spec.clone().map(|s| (r.catalog_name.clone(), s)))
                .collect(),
            collections: self
                .collections
                .iter()
                .filter_map(|r| r.spec.clone().map(|s| (r.catalog_name.clone(), s)))
                .collect(),
            materializations: self
                .materializations
                .iter()
                .filter_map(|r| r.spec.clone().map(|s| (r.catalog_name.clone(), s)))
                .collect(),
            tests: self
                .tests
                .iter()
                .filter_map(|r| r.spec.clone().map(|s| (r.catalog_name.clone(), s)))
                .collect(),
            ..Default::default()
        }
    }
}

impl std::fmt::Debug for DraftCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = &mut f.debug_struct("DraftCatalog");

        fn field<'a, 'b, 'c, T: Row>(
            s: &'c mut std::fmt::DebugStruct<'a, 'b>,
            name: &str,
            value: &Table<T>,
        ) -> &'c mut std::fmt::DebugStruct<'a, 'b> {
            if !value.is_empty() {
                s.field(name, value);
            }
            s
        }

        s = field(s, "captures", &self.captures);
        s = field(s, "collections", &self.collections);
        s = field(s, "materializations", &self.materializations);
        s = field(s, "tests", &self.tests);
        s = field(s, "errors", &self.errors);
        s = field(s, "fetches", &self.fetches);
        s = field(s, "imports", &self.imports);
        s = field(s, "resources", &self.resources);
        s = field(s, "storage_mappings", &self.storage_mappings);
        s.finish()
    }
}

impl From<models::Catalog> for DraftCatalog {
    fn from(value: models::Catalog) -> Self {
        Self {
            captures: value
                .captures
                .into_iter()
                .map(|(name, spec)| DraftCapture {
                    scope: synthetic_scope(models::CatalogType::Capture, &name),
                    catalog_name: name,
                    spec: Some(spec),
                    expect_build_id: None,
                })
                .collect(),
            collections: value
                .collections
                .into_iter()
                .map(|(name, spec)| DraftCollection {
                    scope: synthetic_scope(models::CatalogType::Collection, &name),
                    catalog_name: name,
                    spec: Some(spec),
                    expect_build_id: None,
                })
                .collect(),
            materializations: value
                .materializations
                .into_iter()
                .map(|(name, spec)| DraftMaterialization {
                    scope: synthetic_scope(models::CatalogType::Materialization, &name),
                    catalog_name: name,
                    spec: Some(spec),
                    expect_build_id: None,
                })
                .collect(),
            tests: value
                .tests
                .into_iter()
                .map(|(name, spec)| DraftTest {
                    scope: synthetic_scope(models::CatalogType::Test, &name),
                    catalog_name: name,
                    spec: Some(spec),
                    expect_build_id: None,
                })
                .collect(),
            ..Default::default()
        }
    }
}

impl From<LiveCatalog> for DraftCatalog {
    fn from(live: LiveCatalog) -> Self {
        Self {
            captures: live.captures.into_iter().map(DraftCapture::from).collect(),
            collections: live
                .collections
                .into_iter()
                .map(DraftCollection::from)
                .collect(),
            materializations: live
                .materializations
                .into_iter()
                .map(DraftMaterialization::from)
                .collect(),
            tests: live.tests.into_iter().map(DraftTest::from).collect(),
            ..Default::default()
        }
    }
}

// LiveCatalog are tables which are populated from the Estuary control plane.
#[derive(Default, Debug)]
pub struct LiveCatalog {
    pub captures: LiveCaptures,
    pub collections: LiveCollections,
    pub materializations: LiveMaterializations,
    pub tests: LiveTests,
    pub inferred_schemas: InferredSchemas,
}

impl LiveCatalog {
    pub fn is_empty(&self) -> bool {
        self.captures.is_empty()
            && self.collections.is_empty()
            && self.inferred_schemas.is_empty()
            && self.materializations.is_empty()
            && self.tests.is_empty()
    }
}

impl LiveCatalog {
    pub fn all_spec_names(&self) -> impl Iterator<Item = &str> {
        self.captures
            .iter()
            .map(|c| c.catalog_name.as_str())
            .chain(self.collections.iter().map(|c| c.catalog_name.as_str()))
            .chain(
                self.materializations
                    .iter()
                    .map(|c| c.catalog_name.as_str()),
            )
            .chain(self.tests.iter().map(|c| c.catalog_name.as_str()))
    }
}

/// Validations are tables populated by catalog validations of the `validation` crate.
#[derive(Default, Debug)]
pub struct Validations {
    pub built_captures: BuiltCaptures,
    pub built_collections: BuiltCollections,
    pub built_materializations: BuiltMaterializations,
    pub built_tests: BuiltTests,
    pub errors: Errors,
}

#[cfg(feature = "persist")]
impl DraftCatalog {
    pub fn into_result(mut self) -> Result<Self, Errors> {
        match std::mem::take(&mut self.errors) {
            errors if errors.is_empty() => Ok(self),
            errors => Err(errors),
        }
    }

    // Access all tables as an array of dynamic TableObj instances.
    pub fn as_tables(&self) -> Vec<&dyn SqlTableObj> {
        // This de-structure ensures we can't fail to update as tables change.
        let Self {
            captures,
            collections,
            errors,
            fetches,
            imports,
            materializations,
            resources,
            storage_mappings,
            tests,
        } = self;

        vec![
            captures,
            collections,
            errors,
            fetches,
            imports,
            materializations,
            resources,
            storage_mappings,
            tests,
        ]
    }

    // Access all tables as an array of mutable dynamic SqlTableObj instances.
    pub fn as_tables_mut(&mut self) -> Vec<&mut dyn SqlTableObj> {
        let Self {
            captures,
            collections,
            errors,
            fetches,
            imports,
            materializations,
            resources,
            storage_mappings,
            tests,
        } = self;

        vec![
            captures,
            collections,
            errors,
            fetches,
            imports,
            materializations,
            resources,
            storage_mappings,
            tests,
        ]
    }
}

#[cfg(feature = "persist")]
impl Validations {
    pub fn into_result(mut self) -> Result<Self, Errors> {
        match std::mem::take(&mut self.errors) {
            errors if errors.is_empty() => Ok(self),
            errors => Err(errors),
        }
    }

    // Access all tables as an array of dynamic TableObj instances.
    pub fn as_tables(&self) -> Vec<&dyn SqlTableObj> {
        // This de-structure ensures we can't fail to update as tables change.
        let Self {
            built_captures,
            built_collections,
            built_materializations,
            built_tests,
            errors,
        } = self;

        vec![
            built_captures,
            built_collections,
            built_materializations,
            built_tests,
            errors,
        ]
    }

    // Access all tables as an array of mutable dynamic SqlTableObj instances.
    pub fn as_tables_mut(&mut self) -> Vec<&mut dyn SqlTableObj> {
        let Self {
            built_captures,
            built_collections,
            built_materializations,
            built_tests,
            errors,
        } = self;

        vec![
            built_captures,
            built_collections,
            built_materializations,
            built_tests,
            errors,
        ]
    }
}

// macros::TableColumn implementations for table columns.

primitive_sql_types!(
    String => "TEXT",
    url::Url => "TEXT",
    bool => "BOOLEAN",
    u32 => "INTEGER",
);

// primitive_sql_types generates SqlColumn but not Column implementations.
impl Column for String {
    fn column_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self)
    }
}
impl Column for url::Url {
    fn column_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
impl Column for bool {
    fn column_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Match SQLite encoding of booleans.
        if *self {
            f.write_str("1")
        } else {
            f.write_str("0")
        }
    }
}
impl Column for u32 {}

string_wrapper_types!(
    models::Capture,
    models::Collection,
    models::Materialization,
    models::Prefix,
    models::Test,
);

json_sql_types!(
    Vec<models::Store>,
    models::CaptureDef,
    models::CollectionDef,
    models::Id,
    models::MaterializationDef,
    models::RawValue,
    models::Schema,
    models::TestDef,
    proto_flow::flow::ContentType,
);

proto_sql_types!(
    proto_flow::capture::response::Validated,
    proto_flow::derive::response::Validated,
    proto_flow::flow::CaptureSpec,
    proto_flow::flow::CollectionSpec,
    proto_flow::flow::MaterializationSpec,
    proto_flow::flow::TestSpec,
    proto_flow::flow::build_api::Config,
    proto_flow::materialize::response::Validated,
);

// Additional bespoke column implementations for types that require extra help.
impl Column for anyhow::Error {
    fn column_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#}", self)
    }
}

#[cfg(feature = "persist")]
impl SqlColumn for anyhow::Error {
    fn sql_type() -> &'static str {
        "TEXT"
    }
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(format!("{:#}", self).into())
    }
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        Ok(anyhow::anyhow!(String::column_result(value)?))
    }
}

impl Column for bytes::Bytes {
    fn column_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const ELIDE: &str = ".. binary ..";
        <str as std::fmt::Debug>::fmt(ELIDE, f)
    }
}

#[cfg(feature = "persist")]
impl SqlColumn for bytes::Bytes {
    fn sql_type() -> &'static str {
        "BLOB"
    }
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(self.as_ref().into())
    }
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        use rusqlite::types::FromSql;
        Ok(<Vec<u8>>::column_result(value)?.into())
    }
}

#[cfg(test)]
mod test {
    use super::macros::*;

    tables!(
        table Foos (row Foo, sql "foos") {
            val f1: u32,
        }

        table Bars (row Bar, sql "bars") {
            key b1: u32,
            val b2: u32,
        }

        table Quibs (row Quib, sql "quibs") {
            key q1: u32,
            key q2: u32,
        }
    );

    #[test]
    fn test_insert_indexing() {
        let mut tbl = Foos::new();
        tbl.insert_row(1);
        tbl.insert_row(0);
        tbl.insert_row(2);
        tbl.insert_row(1);
        tbl.insert_row(0);
        tbl.insert_row(1);

        // When key is empty, the initial ordering is preserved.
        assert_eq!(
            tbl.iter().map(|r| r.f1).collect::<Vec<_>>(),
            vec![1, 0, 2, 1, 0, 1]
        );

        // Table ordered by a single column.
        let mut tbl = Bars::new();
        tbl.insert_row(10, 90);
        tbl.insert_row(0, 78);
        tbl.insert_row(20, 56);
        tbl.insert_row(10, 34);
        tbl.insert_row(0, 12);
        tbl.insert_row(10, 90);

        // Ordered with respect to key, but not to the extra columns.
        assert_eq!(
            tbl.iter().map(|r| (r.b1, r.b2)).collect::<Vec<_>>(),
            vec![(0, 78), (0, 12), (10, 90), (10, 34), (10, 90), (20, 56)]
        );

        let joined: Vec<usize> = tbl
            .inner_join(
                [(0u32, 1usize), (0, 2), (10, 3), (15, 4), (20, 5), (21, 6)].into_iter(),
                |_bar, _k, v| Some(v),
            )
            .collect();

        assert_eq!(joined, vec![1, 2, 3, 5]);

        // Table ordered on a composite key.
        let mut tbl = Quibs::new();
        tbl.insert_row(10, 90);
        tbl.insert_row(0, 78);
        tbl.insert_row(20, 56);
        tbl.insert_row(10, 34);
        tbl.insert_row(0, 12);
        tbl.insert_row(10, 90);

        // Fully ordered by the composite key (both columns).
        assert_eq!(
            tbl.iter().map(|r| (r.q1, r.q2)).collect::<Vec<_>>(),
            vec![(0, 12), (0, 78), (10, 34), (10, 90), (10, 90), (20, 56)]
        );

        let joined: Vec<usize> = tbl
            .inner_join(
                [
                    ((0u32, 10u32), 1usize),
                    ((0, 78), 2),
                    ((0, 90), 3),
                    ((10, 34), 4),
                    ((10, 90), 5),
                    ((21, 0), 6),
                ]
                .into_iter(),
                |_quib, _k, v| Some(v),
            )
            .collect();

        assert_eq!(joined, vec![2, 4, 5]);
    }
}
