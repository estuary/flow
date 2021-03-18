use crate::names;
use prost::Message;
use std::collections::BTreeMap;

#[macro_use]
mod macros;
use macros::*;

pub use macros::{load_tables, persist_tables, Table, TableObj, TableRow};

tables!(
    table Fetches (row Fetch, sql "fetches") {
        resource: url::Url,
    }

    table Resources (row Resource, sql "resources") {
        resource: url::Url,
        content_type: protocol::flow::ContentType,
        content: bytes::Bytes,
    }

    table Imports (row Import, sql "imports") {
        scope: url::Url,
        // Resource which does the importing.
        from_resource: url::Url,
        // Resource which is imported.
        to_resource: url::Url,
    }

    table NPMDependencies (row NPMDependency, sql "npm_dependencies") {
        scope: url::Url,
        // NPM package name.
        package: String,
        // NPM package semver.
        version: String,
    }

    table JournalRules (row JournalRule, sql "journal_rules") {
        scope: url::Url,
        // Name of this rule, which also encodes its priority as
        // lexicographic order determines evaluation and application order.
        rule: names::Rule,
        // Rule selector and patch template.
        spec: protocol::flow::journal_rules::Rule,
    }

    table ShardRules (row ShardRule, sql "shard_rules") {
        scope: url::Url,
        // Name of this rule, which also encodes its priority as
        // lexicographic order determines evaluation and application order.
        rule: names::Rule,
        // Rule selector and patch template.
        spec: protocol::flow::shard_rules::Rule,
    }

    table Collections (row Collection, sql "collections") {
        scope: url::Url,
        collection: names::Collection,
        // JSON Schema against which all collection documents are validated,
        // and which provides document annotations.
        schema: url::Url,
        // JSON pointers which define the composite key of the collection.
        key: names::CompositeKey,
    }

    table Projections (row Projection, sql "projections") {
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

    table Derivations (row Derivation, sql "derivations") {
        scope: url::Url,
        derivation: names::Collection,
        // JSON Schema against which register values are validated,
        // and which provides document annotations.
        register_schema: url::Url,
        // JSON value taken by registers which have never before been updated.
        register_initial: serde_json::Value,
    }

    table Transforms (row Transform, sql "transforms") {
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
        // If true, a register update which reduces to an invalid register value
        // should silently roll back the register, rather than failing processing.
        rollback_on_register_conflict: bool,
        // Hash function applied to shuffled keys.
        shuffle_hash: protocol::flow::shuffle::Hash,
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

    table Endpoints (row Endpoint, sql "endpoints") {
        scope: url::Url,
        // Name of this endpoint.
        endpoint: names::Endpoint,
        // Enumerated type of the endpoint, used to select an appropriate driver.
        endpoint_type: protocol::flow::EndpointType,
        // JSON object which partially configures the endpoint.
        base_config: serde_json::Value,
    }

    table Captures (row Capture, sql "captures") {
        scope: url::Url,
        // Collection into which documents are captured.
        collection: names::Collection,
        // Endpoint from which documents are to be captured.
        endpoint: names::Endpoint,
        // JSON object which merges into the endpoint's base_config,
        // to fully configure this capture with respect to the endpoint driver.
        patch_config: serde_json::Value,
    }

    table Materializations (row Materialization, sql "materializations") {
        scope: url::Url,
        // Collection from which documents are materialized.
        collection: names::Collection,
        // Endpoint into which documents are materialized.
        endpoint: names::Endpoint,
        // Fields which must not be included in the materialization.
        fields_exclude: Vec<String>,
        // Fields which must be included in the materialization,
        // and driver-specific field configuration.
        fields_include: BTreeMap<String, names::Object>,
        // Should recommended fields be selected ?
        fields_recommended: bool,
        // JSON object which merges into the endpoint's base_config,
        // to fully configure this materialization with respect to the
        // endpoint driver.
        patch_config: serde_json::Value,
    }

    table TestSteps (row TestStep, sql "test_steps") {
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

    table SchemaDocs (row SchemaDoc, sql "schema_docs") {
        schema: url::Url,
        // JSON document model of the schema.
        dom: serde_json::Value,
    }

    table NamedSchemas (row NamedSchema, sql "named_schemas") {
        // Scope is the canonical non-anchor URI of this schema.
        scope: url::Url,
        // Anchor is the alternative anchor'd URI.
        anchor: url::Url,
        // Name portion of the anchor.
        anchor_name: String,
    }

    table Inferences (row Inference, sql "inferences") {
        // URL of the schema which is inferred, inclusive of any fragment pointer.
        schema: url::Url,
        // A location within a document verified by this schema,
        // relative to the schema's root.
        location: names::JsonPointer,
        // Inference at this schema location.
        spec: protocol::flow::Inference,
    }

    table BuiltCaptures (row BuiltCapture, sql "built_captures") {
        scope: url::Url,
        // Name of this capture.
        capture: String,
        // Built specification for this capture.
        spec: protocol::flow::CaptureSpec,
    }

    table BuiltCollections (row BuiltCollection, sql "built_collections") {
        scope: url::Url,
        // Name of this collection.
        collection: names::Collection,
        // Built specification for this collection.
        spec: protocol::flow::CollectionSpec,
    }

    table BuiltMaterializations (row BuiltMaterialization, sql "built_materializations") {
        scope: url::Url,
        // Name of this materialization.
        materialization: String,
        // Collection from which documents are materialized.
        collection: names::Collection,
        // Enumerated type of the endpoint, used to select an appropriate driver.
        endpoint_type: protocol::flow::EndpointType,
        // Built specification for this materialization.
        spec: protocol::flow::MaterializationSpec,
    }

    table BuiltDerivations (row BuiltDerivation, sql "built_derivations") {
        scope: url::Url,
        // Name of this derivation.
        derivation: names::Collection,
        // Built specification for this derivation.
        spec: protocol::flow::DerivationSpec,
    }

    table BuiltTests (row BuiltTest, sql "built_tests") {
        // Name of the test case.
        test: names::Test,
        // Built specification for this test case.
        spec: protocol::flow::TestSpec,
    }

    table Errors (row Error, sql "errors") {
        scope: url::Url,
        error: anyhow::Error,
    }
);

#[derive(Default, Debug)]
pub struct All {
    pub built_captures: BuiltCaptures,
    pub built_collections: BuiltCollections,
    pub built_derivations: BuiltDerivations,
    pub built_materializations: BuiltMaterializations,
    pub built_tests: BuiltTests,
    pub captures: Captures,
    pub collections: Collections,
    pub derivations: Derivations,
    pub endpoints: Endpoints,
    pub errors: Errors,
    pub fetches: Fetches,
    pub imports: Imports,
    pub inferences: Inferences,
    pub journal_rules: JournalRules,
    pub shard_rules: ShardRules,
    pub materializations: Materializations,
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
            captures,
            collections,
            derivations,
            endpoints,
            errors,
            fetches,
            imports,
            inferences,
            journal_rules,
            materializations,
            named_schemas,
            npm_dependencies,
            projections,
            resources,
            schema_docs,
            shard_rules,
            test_steps,
            transforms,
        } = self;

        vec![
            built_captures,
            built_collections,
            built_derivations,
            built_materializations,
            built_tests,
            captures,
            collections,
            derivations,
            endpoints,
            errors,
            fetches,
            imports,
            inferences,
            journal_rules,
            materializations,
            named_schemas,
            npm_dependencies,
            projections,
            resources,
            schema_docs,
            shard_rules,
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
            captures,
            collections,
            derivations,
            endpoints,
            errors,
            fetches,
            imports,
            inferences,
            journal_rules,
            materializations,
            named_schemas,
            npm_dependencies,
            projections,
            resources,
            schema_docs,
            shard_rules,
            test_steps,
            transforms,
        } = self;

        vec![
            built_captures,
            built_collections,
            built_derivations,
            built_materializations,
            built_tests,
            captures,
            collections,
            derivations,
            endpoints,
            errors,
            fetches,
            imports,
            inferences,
            journal_rules,
            materializations,
            named_schemas,
            npm_dependencies,
            projections,
            resources,
            schema_docs,
            shard_rules,
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
    names::Collection,
    names::Endpoint,
    names::JsonPointer,
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
    protocol::flow::shuffle::Hash,
    protocol::flow::test_spec::step::Type,
    serde_json::Value,
);

proto_sql_types!(
    protocol::flow::CaptureSpec,
    protocol::flow::CollectionSpec,
    protocol::flow::DerivationSpec,
    protocol::flow::Inference,
    protocol::flow::MaterializationSpec,
    protocol::flow::TestSpec,
    protocol::flow::TransformSpec,
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
