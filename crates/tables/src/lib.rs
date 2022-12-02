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
        // Document dontent of this resource, or 'null' if not a document.
        content_dom: Box<serde_json::value::RawValue>,
    }

    table Imports (row Import, order_by [from_resource to_resource], sql "imports") {
        scope: url::Url,
        // Resource which does the importing.
        from_resource: url::Url,
        // Resource which is imported.
        to_resource: url::Url,
    }

    table NPMDependencies (row NPMDependency, order_by [derivation package], sql "npm_dependencies") {
        scope: url::Url,
        // Derivation to which this NPM package dependency belongs.
        derivation: models::Collection,
        // NPM package name.
        package: String,
        // NPM package semver.
        version: String,
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
        // Schema against which collection documents are validated and reduced on write.
        write_schema: url::Url,
        // Schema against which collection documents are validated and reduced on read.
        read_schema: url::Url,
    }

    table Projections (row Projection, order_by [collection field], sql "projections") {
        scope: url::Url,
        // Collection to which this projection belongs.
        collection: models::Collection,
        // Field of this projection.
        field: models::Field,
        // Specification of this projection.
        spec: models::Projection,
    }

    table Derivations (row Derivation, order_by [derivation], sql "derivations") {
        scope: url::Url,
        // Collection which this derivation derives.
        derivation: models::Collection,
        // Derivation specification.
        spec: models::Derivation,
        // JSON Schema against which derivation register documents are validated,
        // and which provides document annotations.
        register_schema: url::Url,
        // Typescript module implementing lambdas of the derivation.
        typescript_module: Option<url::Url>,
    }

    table Transforms (row Transform, order_by [derivation transform], sql "transforms") {
        scope: url::Url,
        // Derivation to which this transform belongs.
        derivation: models::Collection,
        // Name of this transform, scoped to the owning derivation.
        transform: models::Transform,
        // Specification of this transform.
        spec: models::TransformDef,
    }

    table Captures (row Capture, order_by [capture], sql "captures") {
        scope: url::Url,
        // Name of this capture.
        capture: models::Capture,
        // Capture specification.
        spec: models::CaptureDef,
        // Endpoint configuration of the capture.
        endpoint_config: Option<url::Url>,
    }

    table CaptureBindings (row CaptureBinding, order_by [capture capture_index], sql "capture_bindings") {
        scope: url::Url,
        // Capture to which this binding belongs.
        capture: models::Capture,
        // Index of this binding within the Capture.
        capture_index: u32,
        // Specification of the capture binding.
        spec: models::CaptureBinding,
    }

    table Materializations (row Materialization, order_by [materialization], sql "materializations") {
        scope: url::Url,
        // Name of this materialization.
        materialization: models::Materialization,
        // Materialization specification.
        spec: models::MaterializationDef,
        // Endpoint configuration of the materialization.
        endpoint_config: Option<url::Url>,
    }

    table MaterializationBindings (row MaterializationBinding, order_by [materialization materialization_index], sql "materialization_bindings") {
        scope: url::Url,
        // Materialization to which this binding belongs.
        materialization: models::Materialization,
        // Index of this binding within the Materialization.
        materialization_index: u32,
        // Specification of the materialization binding.
        spec: models::MaterializationBinding,
    }

    table TestSteps (row TestStep, order_by [test step_index], sql "test_steps") {
        scope: url::Url,
        // Name of the owning test case.
        test: models::Test,
        // Enumerated index of this test step.
        step_index: u32,
        // Specification of the test step.
        spec: models::TestStep,
        // Documents ingested or verified by this step.
        documents: url::Url,
    }

    table SchemaDocs (row SchemaDoc, order_by [schema], sql "schema_docs") {
        schema: url::Url,
        // JSON document model of the schema.
        dom: serde_json::Value,
    }

    table Inferences (row Inference, order_by [schema location], sql "inferences") {
        // URL of the schema which is inferred, inclusive of any fragment pointer.
        schema: url::Url,
        // A location within a document verified by this schema,
        // relative to the schema's root.
        location: models::JsonPointer,
        // Inference at this schema location.
        spec: proto_flow::flow::Inference,
    }

    table BuiltCaptures (row BuiltCapture, order_by [capture], sql "built_captures") {
        scope: url::Url,
        // Name of this capture.
        capture: String,
        // Built specification for this capture.
        spec: proto_flow::flow::CaptureSpec,
    }

    table BuiltCollections (row BuiltCollection, order_by [collection], sql "built_collections") {
        scope: url::Url,
        // Name of this collection.
        collection: models::Collection,
        // Built specification for this collection.
        spec: proto_flow::flow::CollectionSpec,
    }

    table BuiltMaterializations (row BuiltMaterialization, order_by [materialization], sql "built_materializations") {
        scope: url::Url,
        // Name of this materialization.
        materialization: String,
        // Built specification for this materialization.
        spec: proto_flow::flow::MaterializationSpec,
    }

    table BuiltDerivations (row BuiltDerivation, order_by [derivation], sql "built_derivations") {
        scope: url::Url,
        // Name of this derivation.
        derivation: models::Collection,
        // Built specification for this derivation.
        spec: proto_flow::flow::DerivationSpec,
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
    pub capture_bindings: CaptureBindings,
    pub captures: Captures,
    pub collections: Collections,
    pub derivations: Derivations,
    pub errors: Errors,
    pub fetches: Fetches,
    pub imports: Imports,
    pub materialization_bindings: MaterializationBindings,
    pub materializations: Materializations,
    pub npm_dependencies: NPMDependencies,
    pub projections: Projections,
    pub resources: Resources,
    pub schema_docs: SchemaDocs,
    pub storage_mappings: StorageMappings,
    pub test_steps: TestSteps,
    pub transforms: Transforms,
}

/// Validations are tables populated by catalog validations of the `validation` crate.
#[derive(Default, Debug)]
pub struct Validations {
    pub built_captures: BuiltCaptures,
    pub built_collections: BuiltCollections,
    pub built_derivations: BuiltDerivations,
    pub built_materializations: BuiltMaterializations,
    pub built_tests: BuiltTests,
    pub errors: Errors,
    pub inferences: Inferences,
}

/// All combines Sources and Validations:
///  * errors of the respective tables are combined.
///  * Validations::implicit_projections is folded into Sources::projections.
#[derive(Default, Debug)]
pub struct All {
    pub built_captures: BuiltCaptures,
    pub built_collections: BuiltCollections,
    pub built_derivations: BuiltDerivations,
    pub built_materializations: BuiltMaterializations,
    pub built_tests: BuiltTests,
    pub capture_bindings: CaptureBindings,
    pub captures: Captures,
    pub collections: Collections,
    pub derivations: Derivations,
    pub errors: Errors,
    pub fetches: Fetches,
    pub imports: Imports,
    pub inferences: Inferences,
    pub materialization_bindings: MaterializationBindings,
    pub materializations: Materializations,
    pub meta: Meta,
    pub npm_dependencies: NPMDependencies,
    pub projections: Projections,
    pub resources: Resources,
    pub schema_docs: SchemaDocs,
    pub storage_mappings: StorageMappings,
    pub test_steps: TestSteps,
    pub transforms: Transforms,
}

#[cfg(feature = "persist")]
impl All {
    // Access all tables as an array of dynamic TableObj instances.
    pub fn as_tables(&self) -> Vec<&dyn SqlTableObj> {
        // This de-structure ensures we can't fail to update as tables change.
        let Self {
            built_captures,
            built_collections,
            built_derivations,
            built_materializations,
            built_tests,
            capture_bindings,
            captures,
            collections,
            derivations,
            errors,
            fetches,
            imports,
            inferences,
            materialization_bindings,
            materializations,
            meta,
            npm_dependencies,
            projections,
            resources,
            schema_docs,
            storage_mappings,
            test_steps,
            transforms,
        } = self;

        vec![
            built_captures,
            built_collections,
            built_derivations,
            built_materializations,
            built_tests,
            capture_bindings,
            captures,
            collections,
            derivations,
            errors,
            fetches,
            imports,
            inferences,
            materialization_bindings,
            materializations,
            meta,
            npm_dependencies,
            projections,
            resources,
            schema_docs,
            storage_mappings,
            test_steps,
            transforms,
        ]
    }

    // Access all tables as an array of mutable dynamic SqlTableObj instances.
    pub fn as_tables_mut(&mut self) -> Vec<&mut dyn SqlTableObj> {
        let Self {
            built_captures,
            built_collections,
            built_derivations,
            built_materializations,
            built_tests,
            capture_bindings,
            captures,
            collections,
            derivations,
            errors,
            fetches,
            imports,
            inferences,
            materialization_bindings,
            materializations,
            meta,
            npm_dependencies,
            projections,
            resources,
            schema_docs,
            storage_mappings,
            test_steps,
            transforms,
        } = self;

        vec![
            built_captures,
            built_collections,
            built_derivations,
            built_materializations,
            built_tests,
            capture_bindings,
            captures,
            collections,
            derivations,
            errors,
            fetches,
            imports,
            inferences,
            materialization_bindings,
            materializations,
            meta,
            npm_dependencies,
            projections,
            resources,
            schema_docs,
            storage_mappings,
            test_steps,
            transforms,
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
    models::Field,
    models::JsonPointer,
    models::Materialization,
    models::Prefix,
    models::Test,
    models::Transform,
);

json_sql_types!(
    Box<serde_json::value::RawValue>,
    Vec<models::Store>,
    models::CaptureBinding,
    models::CaptureDef,
    models::CollectionDef,
    models::Derivation,
    models::MaterializationBinding,
    models::MaterializationDef,
    models::Projection,
    models::TestStep,
    models::TransformDef,
    proto_flow::flow::ContentType,
    serde_json::Value,
);

proto_sql_types!(
    proto_flow::flow::CaptureSpec,
    proto_flow::flow::CollectionSpec,
    proto_flow::flow::DerivationSpec,
    proto_flow::flow::Inference,
    proto_flow::flow::MaterializationSpec,
    proto_flow::flow::TestSpec,
    proto_flow::flow::build_api::Config,
);

// Modules that extend tables with additional implementations.
mod behaviors;

// Additional bespoke column implementations for types that require extra help.
impl Column for anyhow::Error {
    fn column_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[cfg(feature = "persist")]
impl SqlColumn for anyhow::Error {
    fn sql_type() -> &'static str {
        "TEXT"
    }
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(format!("{:?}", self).into())
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
    fn test_indexing() {
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
