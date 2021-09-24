use crate::names;
use prost::Message;
use std::collections::BTreeMap;

#[macro_use]
mod macros;
use macros::*;

pub use macros::{load_tables, persist_tables, Table, TableObj, TableRow};

tables!(
    table Fetches (row Fetch, order_by [depth resource], sql "fetches") {
        depth: u32,
        resource: url::Url,
    }

    table Resources (row Resource, order_by [resource], sql "resources") {
        resource: url::Url,
        content_type: protocol::flow::ContentType,
        content: bytes::Bytes,
    }

    table Imports (row Import, order_by [from_resource to_resource], sql "imports") {
        scope: url::Url,
        // Resource which does the importing.
        from_resource: url::Url,
        // Resource which is imported.
        to_resource: url::Url,
    }

    table NPMDependencies (row NPMDependency, order_by [package version], sql "npm_dependencies") {
        scope: url::Url,
        // NPM package name.
        package: String,
        // NPM package semver.
        version: String,
    }

    table JournalRules (row JournalRule, order_by [rule], sql "journal_rules") {
        scope: url::Url,
        // Name of this rule, which also encodes its priority as
        // lexicographic order determines evaluation and application order.
        rule: names::Rule,
        // Rule selector and patch template.
        spec: protocol::flow::journal_rules::Rule,
    }

    table Collections (row Collection, order_by [collection], sql "collections") {
        scope: url::Url,
        collection: names::Collection,
        // JSON Schema against which all collection documents are validated,
        // and which provides document annotations.
        schema: url::Url,
        // JSON pointers which define the composite key of the collection.
        key: names::CompositeKey,
    }

    table Projections (row Projection, order_by [collection field], sql "projections") {
        scope: url::Url,
        collection: names::Collection,
        field: String,
        location: names::JsonPointer,
        // Is this projection a logically partitioned field?
        partition: bool,
        // Was this projection provided by the user, or inferred
        // from the collection schema ?
        user_provided: bool,
    }

    table Derivations (row Derivation, order_by [derivation], sql "derivations") {
        scope: url::Url,
        derivation: names::Collection,
        // JSON Schema against which register values are validated,
        // and which provides document annotations.
        register_schema: url::Url,
        // JSON value taken by registers which have never before been updated.
        register_initial: serde_json::Value,
    }

    table Transforms (row Transform, order_by [derivation transform], sql "transforms") {
        scope: url::Url,
        derivation: names::Collection,
        // Read priority applied to documents processed by this transform.
        // Ready documents of higher priority are processed before those
        // of lower priority.
        priority: u32,
        // Publish that maps source documents and registers into derived documents.
        publish_lambda: Option<names::Lambda>,
        // Relative time delay applied to documents processed by this transform.
        read_delay_seconds: Option<u32>,
        // JSON pointers which define the composite shuffle key of the transform.
        shuffle_key: Option<names::CompositeKey>,
        // Computed shuffle of this transform. If set, shuffle_hash and shuffle_key
        // must not be (and vice versa).
        shuffle_lambda: Option<names::Lambda>,
        // Collection which is read by this transform.
        source_collection: names::Collection,
        // Selector over logical partitions of the source collection.
        source_partitions: Option<names::PartitionSelector>,
        // Optional alternative JSON schema against which source documents are
        // validated prior to transformation. If None, the collection's schema
        // is used instead.
        source_schema: Option<url::Url>,
        // Name of this transform, scoped to the owning derivation.
        transform: names::Transform,
        // Update that maps source documents into register updates.
        update_lambda: Option<names::Lambda>,
    }

    table Captures (row Capture, order_by [capture], sql "captures") {
        scope: url::Url,
        // Name of this capture.
        capture: names::Capture,
        // Enumerated type of the endpoint, used to select an appropriate driver.
        endpoint_type: protocol::flow::EndpointType,
        // JSON object which configures the endpoint with respect to its driver.
        endpoint_spec: serde_json::Value,
        // Interval between invocations of the capture.
        interval_seconds: u32,
    }

    table CaptureBindings (row CaptureBinding, order_by [capture capture_index], sql "capture_bindings") {
        scope: url::Url,
        // Capture to which this binding belongs.
        capture: names::Capture,
        // Index of this binding within the Capture.
        capture_index: u32,
        // JSON object which specifies the endpoint resource to be captured.
        resource_spec: serde_json::Value,
        // Collection into which documents are captured.
        collection: names::Collection,
    }

    table Materializations (row Materialization, order_by [materialization], sql "materializations") {
        scope: url::Url,
        // Name of this materialization.
        materialization: names::Materialization,
        // Enumerated type of the endpoint, used to select an appropriate driver.
        endpoint_type: protocol::flow::EndpointType,
        // JSON object which configures the endpoint with respect to its driver.
        endpoint_spec: serde_json::Value,
    }

    table MaterializationBindings (row MaterializationBinding, order_by [materialization materialization_index], sql "materialization_bindings") {
        scope: url::Url,
        // Materialization to which this binding belongs.
        materialization: names::Materialization,
        // Index of this binding within the Materialization.
        materialization_index: u32,
        // JSON object which specifies the endpoint resource to be materialized.
        resource_spec: serde_json::Value,
        // Collection from which documents are materialized.
        collection: names::Collection,
        // Fields which must not be included in the materialization.
        fields_exclude: Vec<String>,
        // Fields which must be included in the materialization,
        // and driver-specific field configuration.
        fields_include: BTreeMap<String, names::Object>,
        // Should recommended fields be selected ?
        fields_recommended: bool,
        // Selector over logical partitions of the source collection.
        source_partitions: Option<names::PartitionSelector>,
    }

    table TestSteps (row TestStep, order_by [test step_index], sql "test_steps") {
        scope: url::Url,
        // Collection ingested or verified by this step.
        collection: names::Collection,
        // Documents ingested or verified by this step.
        documents: Vec<serde_json::Value>,
        // When verifying, selector over logical partitions of the collection.
        partitions: Option<names::PartitionSelector>,
        // Enumerated index of this test step.
        step_index: u32,
        // Step type (e.x., ingest or verify).
        step_type: protocol::flow::test_spec::step::Type,
        // Name of the owning test case.
        test: names::Test,
    }

    table SchemaDocs (row SchemaDoc, order_by [schema], sql "schema_docs") {
        schema: url::Url,
        // JSON document model of the schema.
        dom: serde_json::Value,
    }

    table NamedSchemas (row NamedSchema, order_by [anchor_name], sql "named_schemas") {
        // Scope is the canonical non-anchor URI of this schema.
        scope: url::Url,
        // Anchor is the alternative anchor'd URI.
        anchor: url::Url,
        // Name portion of the anchor.
        anchor_name: String,
    }

    table Inferences (row Inference, order_by [schema location], sql "inferences") {
        // URL of the schema which is inferred, inclusive of any fragment pointer.
        schema: url::Url,
        // A location within a document verified by this schema,
        // relative to the schema's root.
        location: names::JsonPointer,
        // Inference at this schema location.
        spec: protocol::flow::Inference,
    }

    table BuiltCaptures (row BuiltCapture, order_by [capture], sql "built_captures") {
        scope: url::Url,
        // Name of this capture.
        capture: String,
        // Built specification for this capture.
        spec: protocol::flow::CaptureSpec,
    }

    table BuiltCollections (row BuiltCollection, order_by [collection], sql "built_collections") {
        scope: url::Url,
        // Name of this collection.
        collection: names::Collection,
        // Built specification for this collection.
        spec: protocol::flow::CollectionSpec,
    }

    table BuiltMaterializations (row BuiltMaterialization, order_by [materialization], sql "built_materializations") {
        scope: url::Url,
        // Name of this materialization.
        materialization: String,
        // Built specification for this materialization.
        spec: protocol::flow::MaterializationSpec,
    }

    table BuiltDerivations (row BuiltDerivation, order_by [derivation], sql "built_derivations") {
        scope: url::Url,
        // Name of this derivation.
        derivation: names::Collection,
        // Built specification for this derivation.
        spec: protocol::flow::DerivationSpec,
    }

    table BuiltTests (row BuiltTest, order_by [test], sql "built_tests") {
        // Name of the test case.
        test: names::Test,
        // Built specification for this test case.
        spec: protocol::flow::TestSpec,
    }

    table Errors (row Error, order_by [], sql "errors") {
        scope: url::Url,
        error: anyhow::Error,
    }

    table Meta (row Build, order_by [], sql "meta") {
        build_uuid: uuid::Uuid,
        build_config: protocol::flow::build_api::Config,
    }
);

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
    pub journal_rules: JournalRules,
    pub materialization_bindings: MaterializationBindings,
    pub materializations: Materializations,
    pub meta: Meta,
    pub named_schemas: NamedSchemas,
    pub npm_dependencies: NPMDependencies,
    pub projections: Projections,
    pub resources: Resources,
    pub schema_docs: SchemaDocs,
    pub test_steps: TestSteps,
    pub transforms: Transforms,
}

impl All {
    // Access all tables as an array of dynamic TableObj instances.
    pub fn as_tables(&self) -> Vec<&dyn TableObj> {
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
            journal_rules,
            materialization_bindings,
            materializations,
            meta,
            named_schemas,
            npm_dependencies,
            projections,
            resources,
            schema_docs,
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
            journal_rules,
            materialization_bindings,
            materializations,
            meta,
            named_schemas,
            npm_dependencies,
            projections,
            resources,
            schema_docs,
            test_steps,
            transforms,
        ]
    }

    // Access all tables as an array of mutable dynamic TableObj instances.
    pub fn as_tables_mut(&mut self) -> Vec<&mut dyn TableObj> {
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
            journal_rules,
            materialization_bindings,
            materializations,
            meta,
            named_schemas,
            npm_dependencies,
            projections,
            resources,
            schema_docs,
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
            journal_rules,
            materialization_bindings,
            materializations,
            meta,
            named_schemas,
            npm_dependencies,
            projections,
            resources,
            schema_docs,
            test_steps,
            transforms,
        ]
    }
}

// macros::SQLType implementations for table columns.

primitive_sql_types!(
    String => "TEXT",
    url::Url => "TEXT",
    bool => "BOOLEAN",
    u32 => "INTEGER",
);

string_wrapper_types!(
    names::Capture,
    names::Collection,
    names::JsonPointer,
    names::Materialization,
    names::Rule,
    names::Test,
    names::Transform,
);

json_sql_types!(
    BTreeMap<String, names::Object>,
    Vec<String>,
    Vec<serde_json::Value>,
    names::CompositeKey,
    names::Lambda,
    names::PartitionSelector,
    protocol::flow::ContentType,
    protocol::flow::EndpointType,
    protocol::flow::test_spec::step::Type,
    serde_json::Value,
    uuid::Uuid,
);

proto_sql_types!(
    protocol::flow::CaptureSpec,
    protocol::flow::CollectionSpec,
    protocol::flow::DerivationSpec,
    protocol::flow::Inference,
    protocol::flow::MaterializationSpec,
    protocol::flow::TestSpec,
    protocol::flow::TransformSpec,
    protocol::flow::build_api::Config,
    protocol::flow::journal_rules::Rule,
    protocol::flow::shard_rules::Rule,
);

// Modules that extend tables with additional implementations.
mod behaviors;

// Additional bespoke SQLType implementations for types that require extra help.
impl SQLType for anyhow::Error {
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

impl SQLType for bytes::Bytes {
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
    use super::{macros::*, TableObj};

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
