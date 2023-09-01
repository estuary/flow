#[macro_use]
mod macros;
use macros::*;

pub use macros::Table;

#[cfg(feature = "persist")]
pub use macros::{load_tables, persist_tables, SqlTableObj};
#[cfg(feature = "persist")]
use prost::Message;

tables!(
    table Fetches (row Fetch, order_by [depth resource], sql "fetches") {
        // Import depth of this fetch.
        depth: u32,
        // Fetched resource Url.
        resource: url::Url,
    }

    table Resources (row Resource, order_by [resource], sql "resources") {
        // Url of this resource.
        resource: url::Url,
        // Content-type of this resource.
        content_type: proto_flow::flow::ContentType,
        // Byte content of this resource.
        content: bytes::Bytes,
        // Document content of this resource.
        content_dom: models::RawValue,
    }

    table Imports (row Import, order_by [scope to_resource], sql "imports") {
        // Scope is the referring resource and fragment location which caused the import.
        scope: url::Url,
        // Resource which is imported. Never has a fragment.
        to_resource: url::Url,
    }

    table StorageMappings (row StorageMapping, order_by [prefix], sql "storage_mappings") {
        scope: url::Url,
        // Catalog prefix to which this storage mapping applies.
        prefix: models::Prefix,
        // Stores for journal fragments under this prefix.
        stores: Vec<models::Store>,
    }

    table Collections (row Collection, order_by [collection], sql "collections") {
        scope: url::Url,
        // Name of this collection.
        collection: models::Collection,
        // Specification of this collection.
        spec: models::CollectionDef,
    }

    table Captures (row Capture, order_by [capture], sql "captures") {
        scope: url::Url,
        // Name of this capture.
        capture: models::Capture,
        // Capture specification.
        spec: models::CaptureDef,
    }

    table Materializations (row Materialization, order_by [materialization], sql "materializations") {
        scope: url::Url,
        // Name of this materialization.
        materialization: models::Materialization,
        // Materialization specification.
        spec: models::MaterializationDef,
    }

    table Tests (row Test, order_by [test], sql "tests") {
        scope: url::Url,
        // Name of the test.
        test: models::Test,
        // Specification of the test.
        spec: Vec<models::TestStep>,
    }

    table BuiltCaptures (row BuiltCapture, order_by [capture], sql "built_captures") {
        scope: url::Url,
        // Name of this capture.
        capture: String,
        // Validated response which was used to build this spec.
        validated: proto_flow::capture::response::Validated,
        // Built specification for this capture.
        spec: proto_flow::flow::CaptureSpec,
    }

    table BuiltCollections (row BuiltCollection, order_by [collection], sql "built_collections") {
        scope: url::Url,
        // Name of this collection.
        collection: models::Collection,
        // Validated response which was used to build this spec.
        validated: Option<proto_flow::derive::response::Validated>,
        // Built specification for this collection.
        spec: proto_flow::flow::CollectionSpec,
    }

    table BuiltMaterializations (row BuiltMaterialization, order_by [materialization], sql "built_materializations") {
        scope: url::Url,
        // Name of this materialization.
        materialization: String,
        // Validated response which was used to build this spec.
        validated: proto_flow::materialize::response::Validated,
        // Built specification for this materialization.
        spec: proto_flow::flow::MaterializationSpec,
    }

    table BuiltTests (row BuiltTest, order_by [test], sql "built_tests") {
        scope: url::Url,
        // Name of the test case.
        test: models::Test,
        // Built specification for this test case.
        spec: proto_flow::flow::TestSpec,
    }

    table Errors (row Error, order_by [], sql "errors") {
        scope: url::Url,
        error: anyhow::Error,
    }

    table Meta (row Build, order_by [], sql "meta") {
        build_config: proto_flow::flow::build_api::Config,
    }
);

/// Sources are tables which are populated by catalog loads of the `sources` crate.
#[derive(Default, Debug)]
pub struct Sources {
    pub captures: Captures,
    pub collections: Collections,
    pub errors: Errors,
    pub fetches: Fetches,
    pub imports: Imports,
    pub materializations: Materializations,
    pub resources: Resources,
    pub storage_mappings: StorageMappings,
    pub tests: Tests,
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
impl Sources {
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
    Vec<models::TestStep>,
    models::CaptureDef,
    models::CollectionDef,
    models::MaterializationDef,
    models::RawValue,
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

// Modules that extend tables with additional implementations.
mod behaviors;

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
        table Foos (row Foo, order_by [], sql "foos") {
            f1: u32,
        }

        table Bars (row Bar, order_by [b1], sql "bars") {
            b1: u32,
            b2: u32,
        }

        table Quibs (row Quib, order_by [q1 q2], sql "quibs") {
            q1: u32,
            q2: u32,
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

        // When order_by is empty, the initial ordering is preserved.
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

        // Ordered with respect to order_by, but not to the extra columns.
        assert_eq!(
            tbl.iter().map(|r| (r.b1, r.b2)).collect::<Vec<_>>(),
            vec![(0, 78), (0, 12), (10, 90), (10, 34), (10, 90), (20, 56)]
        );

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
    }
}
