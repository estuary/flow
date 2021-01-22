use crate::source;
use crate::validation;

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
        content_type: source::ContentType,
        content: Vec<u8>,
    }

    table Imports (row Import, sql "imports") {
        scope: url::Url,
        from_resource: url::Url,
        to_resource: url::Url,
    }

    table NodeJSDependencies (row NodeJSDependency, sql "node_dependencies") {
        scope: url::Url,
        package: String,
        version: String,
    }

    table Collections (row Collection, sql "collections") {
        scope: url::Url,
        collection: source::CollectionName,
        schema: url::Url,
        key: source::CompositeKey,
        store_endpoint: source::EndpointName,
        store_patch_config: serde_json::Value,
    }

    table Projections (row Projection, sql "projections") {
        scope: url::Url,
        collection: source::CollectionName,
        field: String,
        location: source::JsonPointer,
        partition: bool,
        user_provided: bool,
    }

    table Derivations (row Derivation, sql "derivations") {
        scope: url::Url,
        derivation: source::CollectionName,
        register_schema: url::Url,
        register_initial: serde_json::Value,
    }

    table Transforms (row Transform, sql "transforms") {
        scope: url::Url,
        transform: source::TransformName,
        derivation: source::CollectionName,
        source_collection: source::CollectionName,
        source_partitions: Option<source::PartitionSelector>,
        source_schema: Option<url::Url>,
        shuffle_key: Option<source::CompositeKey>,
        shuffle_lambda: Option<source::Lambda>,
        shuffle_hash: source::ShuffleHash,
        read_delay_seconds: Option<u32>,
        priority: u32,
        update_lambda: Option<source::Lambda>,
        publish_lambda: Option<source::Lambda>,
    }

    table Endpoints (row Endpoint, sql "endpoints") {
        scope: url::Url,
        endpoint: source::EndpointName,
        endpoint_type: source::EndpointType,
        base_config: serde_json::Value,
    }

    table Captures (row Capture, sql "captures") {
        scope: url::Url,
        capture: source::CaptureName,
        collection: source::CollectionName,
        allow_push: bool,
        endpoint: Option<source::EndpointName>,
        patch_config: serde_json::Value,
    }

    table Materializations (row Materialization, sql "materializations") {
        scope: url::Url,
        materialization: source::MaterializationName,
        collection: source::CollectionName,
        endpoint: source::EndpointName,
        patch_config: serde_json::Value,
        field_selector: source::MaterializationFields,
    }

    table TestSteps (row TestStep, sql "test_steps") {
        scope: url::Url,
        test: source::TestName,
        step_index: u32,
        step: source::TestStep,
    }

    table SchemaDocs (row SchemaDoc, sql "schema_docs") {
        schema: url::Url,
        dom: serde_json::Value,
    }

    table Inferences (row Inference, sql "inferences") {
        schema: url::Url,
        location: source::JsonPointer,
        spec: protocol::flow::Inference,
    }

    table BuiltCollections (row BuiltCollection, sql "built_collections") {
        scope: url::Url,
        collection: source::CollectionName,
        spec: protocol::flow::CollectionSpec,
    }

    table BuiltMaterializations (row BuiltMaterialization, sql "built_materializations") {
        scope: url::Url,
        materialization: source::MaterializationName,
        collection: source::CollectionName,
        endpoint_config: serde_json::Value,
        field_selection: protocol::materialize::FieldSelection,
    }

    table Errors (row Error, sql "errors") {
        scope: url::Url,
        error: anyhow::Error,
    }
);

// macros::SQLType implementations for table columns.

primitive_sql_types!(
    String => "TEXT",
    url::Url => "TEXT",
    Vec<u8> => "BLOB",
    bool => "BOOLEAN",
    u32 => "INTEGER",
);

string_wrapper_types!(
    source::CaptureName,
    source::CollectionName,
    source::EndpointName,
    source::JsonPointer,
    source::MaterializationName,
    source::TestName,
    source::TransformName,
);

json_sql_types!(
    protocol::flow::CollectionSpec,
    protocol::flow::Inference,
    protocol::flow::shuffle::Hash,
    protocol::materialize::FieldSelection,
    serde_json::Value,
    source::CompositeKey,
    source::ContentType,
    source::EndpointType,
    source::Lambda,
    source::MaterializationFields,
    source::PartitionSelector,
    source::TestStep,
);

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

impl Errors {
    pub fn push_validation(&mut self, scope: &url::Url, err: validation::Error) {
        self.push_row(scope, anyhow::anyhow!(err))
    }
}
