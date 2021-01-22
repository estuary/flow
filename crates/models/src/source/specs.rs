use super::wrappers::*;

use schemars::{schema, JsonSchema};
use serde::{Deserialize, Serialize};
use serde_json::{from_value as from_json_value, json, Value};
use std::collections::BTreeMap;
use std::time::Duration;

/// Object is an alias for a JSON object.
pub type Object = serde_json::Map<String, Value>;

/// Ordered JSON-Pointers which define how a composite key may be extracted from
/// a collection document.
#[derive(Serialize, Deserialize, Debug, JsonSchema, Clone, Eq, PartialEq)]
#[schemars(example = "CompositeKey::example")]
pub struct CompositeKey(Vec<JsonPointer>);

impl CompositeKey {
    pub fn example() -> Self {
        CompositeKey(vec![JsonPointer::example()])
    }
}

impl std::ops::Deref for CompositeKey {
    type Target = Vec<JsonPointer>;

    fn deref(&self) -> &Vec<JsonPointer> {
        &self.0
    }
}

/// Each catalog source defines a portion of a Flow Catalog, by defining
/// collections, derivations, tests, and materializations of the Catalog.
/// Catalog sources may reference and import other sources, in order to
/// collections and other entities that source defines.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Catalog {
    /// # JSON-Schema against which the Catalog is validated.
    #[serde(default, rename = "$schema")]
    pub _schema: Option<String>,
    /// # Import other Flow catalog sources.
    /// By importing another Flow catalog source, the collections, schemas, and derivations
    /// it defines become usable within this Catalog source. Each import is an absolute URI,
    /// or a URI which is relative to this source location.
    #[serde(default)]
    #[schemars(example = "Catalog::example_import")]
    pub import: Vec<RelativeUrl>,
    /// # NPM package dependencies of the Catalog.
    /// Dependencies are included when building the catalog's build NodeJS
    /// package, as {"package-name": "version"}. I.e. {"moment": "^2.24"}.
    ///
    /// Version strings can take any form understood by NPM.
    /// See https://docs.npmjs.com/files/package.json#dependencies
    #[serde(default)]
    #[schemars(default = "Catalog::default_node_dependencies")]
    pub node_dependencies: BTreeMap<String, String>,
    /// # Collections defined by this Catalog.
    #[serde(default)]
    #[schemars(example = "Catalog::example_collections")]
    pub collections: BTreeMap<CollectionName, Collection>,
    /// # Named Endpoints of this Catalog.
    #[serde(default)]
    pub endpoints: BTreeMap<EndpointName, Endpoint>,
    /// # Materializations of this Catalog.
    #[serde(default)]
    pub materializations: BTreeMap<MaterializationName, Materialization>,
    /// # Captures of this Catalog.
    #[serde(default)]
    pub captures: BTreeMap<CaptureName, Capture>,
    // Tests of the catalog, indexed by name.
    #[serde(default)]
    #[schemars(default = "Catalog::default_test")]
    #[schemars(example = "Catalog::example_test")]
    pub tests: BTreeMap<TestName, Vec<TestStep>>,
}

impl Catalog {
    fn default_node_dependencies() -> BTreeMap<String, String> {
        from_json_value(json!({"a-npm-package": "^1.2.3"})).unwrap()
    }
    fn default_test() -> Value {
        json!({"Test that fob quips ipsum": []})
    }
    fn example_import() -> Vec<RelativeUrl> {
        vec![
            RelativeUrl::example_relative(),
            RelativeUrl::example_absolute(),
        ]
    }
    fn example_collections() -> Vec<Collection> {
        vec![Collection::example()]
    }
    fn example_test() -> Value {
        json!({
            "Test that fob quips ipsum": [
                TestStep::example_ingest(),
                TestStep::example_verify(),
            ]
        })
    }
}

/// Collection describes a set of related documents, where each adheres to a
/// common schema and grouping key. Collections are append-only: once a document
/// is added to a collection, it is never removed. However, it may be replaced
/// or updated (either in whole, or in part) by a future document sharing its
/// key. Each new document of a given key is "reduced" into existing documents
/// of the key. By default, this reduction is achieved by completely replacing
/// the previous document, but much richer reduction behaviors can be specified
/// through the use of annotated reduction strategies of the collection schema.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "Collection::example")]
pub struct Collection {
    /// # Schema against which collection documents are validated and reduced.
    #[schemars(example = "Schema::example_relative")]
    pub schema: Schema,
    /// # Composite key of this collection.
    pub key: CompositeKey,
    /// # Fragment storage endpoint of this collection.
    /// This must be a compatible file storage endpoint, such as S3 or GCS.
    pub store: EndpointRef,
    /// # Projections and logical partitions of this collection.
    #[serde(default)]
    #[schemars(default = "Projections::example")]
    pub projections: Projections,
    /// # Derivation which builds this collection from others.
    pub derivation: Option<Derivation>,
}

impl Collection {
    fn example() -> Self {
        from_json_value(json!({
            "name": CollectionName::example(),
            "schema": RelativeUrl::example_relative(),
            "key": CompositeKey::example(),
        }))
        .unwrap()
    }
}

/// Projections are named locations within a collection document which
/// may be used for logical partitioning or directly exposed to databases
/// into which collections are materialized.
#[derive(Serialize, Deserialize, Debug, Default, JsonSchema)]
#[schemars(example = "Projections::example")]
pub struct Projections(BTreeMap<String, Projection>);

impl Projections {
    pub fn iter(&self) -> impl Iterator<Item = (&String, &Projection)> {
        self.0.iter()
    }

    fn example() -> Self {
        from_json_value(json!({
            "a_field": JsonPointer::example(),
            "a_partition": {
                "location": JsonPointer::example(),
                "partition": true,
            }
        }))
        .unwrap()
    }
}

/// A projection representation that allows projections to be specified either
/// as a simple JSON Pointer, or as an object with separate properties for
/// the location and partition indicator.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(untagged, deny_unknown_fields, rename_all = "camelCase")]
pub enum Projection {
    Pointer(JsonPointer),
    Object {
        /// # Location of this projection.
        location: JsonPointer,
        /// # Is this projection a logical partition?
        #[serde(default)]
        partition: bool,
    },
}
impl Projection {
    /// Location of the projected field within the document.
    pub fn location(&self) -> &JsonPointer {
        match self {
            Projection::Pointer(location) => location,
            Projection::Object { location, .. } => location,
        }
    }

    /// Is this projection a logical partition?
    pub fn is_partition(&self) -> bool {
        match self {
            Projection::Pointer(_) => false,
            Projection::Object { partition, .. } => *partition,
        }
    }
}

/// Registers are the internal states of a derivation, which can be read and
/// updated by all of its transformations. They're an important building
/// block for joins, aggregations, and other complex stateful workflows.
///
/// Registers are implemented using JSON-Schemas, often ones with reduction
/// annotations. When reading source documents, every distinct shuffle key
/// by which the source collection is read is mapped to a corresponding
/// register value (or, if no shuffle key is defined, the source collection's
/// key is used instead).
///
/// Then, an "update" lambda of the transformation produces updates which
/// are reduced into the register, and a "publish" lambda reads the current
/// (and previous, if updated) register value.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Register {
    /// # Schema which validates and reduces register documents.
    pub schema: Schema,
    /// # Initial value of a keyed register which has never been updated.
    /// If not specified, the default is "null".
    #[serde(default = "value_null")]
    pub initial: Value,
}

fn value_null() -> Value {
    Value::Null
}

impl Default for Register {
    fn default() -> Self {
        Register {
            schema: Schema::Bool(true),
            initial: Value::Null,
        }
    }
}

/// A derivation specifies how a collection is derived from other
/// collections. A collection without a derivation is a "captured"
/// collection, into which documents are directly ingested.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "Derivation::example")]
pub struct Derivation {
    /// # Register configuration of this derivation.
    #[serde(default)]
    pub register: Register,
    /// # Transforms which make up this derivation.
    #[schemars(default = "Derivation::default_transform")]
    pub transform: BTreeMap<TransformName, Transform>,
}

impl Derivation {
    fn example() -> Self {
        from_json_value(json!({
            "transform": {
                "nameOfTransform": Transform::example(),
            }
        }))
        .unwrap()
    }
    fn default_transform() -> Value {
        json!({"nameOfTransform": {"source": {"name": "a/source/collection"}}})
    }
}

/// Lambdas are user functions which are invoked by the Flow runtime to
/// process and transform source collection documents into derived collections.
/// Flow supports multiple lambda run-times, with a current focus on TypeScript
/// and remote HTTP APIs.
///
/// TypeScript lambdas are invoked within on-demand run-times, which are
/// automatically started and scaled by Flow's task distribution in order
/// to best co-locate data and processing, as well as to manage fail-over.
///
/// Remote lambdas may be called from many Flow tasks, and are up to the
/// API provider to provision and scale.
///
/// (Note that Sqlite lambdas are not implemented yet).
///
/// Lambdas are invoked from a few contexts:
///
/// "Update" lambdas take a source document and transform it into one or more
/// register updates, which are then reduced into the associated register by
/// the runtime. For example these register updates might update counters,
/// or update the state of a "join" window.
///
/// "Publish" lambdas take a source document, a current register and
/// (if there is also an "update" lambda) a previous register, and transform
/// them into one or more documents to be published into a derived collection.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "Lambda::example_nodejs_publish")]
#[schemars(example = "Lambda::example_nodejs_update")]
#[schemars(example = "Lambda::example_remote")]
pub enum Lambda {
    NodeJS(String),
    Sqlite(String),
    SqliteFile(RelativeUrl),
    Remote(String),
}

impl Lambda {
    fn example_nodejs_publish() -> Self {
        from_json_value(json!({
            "nodeJS": "return doPublish(source, register);"
        }))
        .unwrap()
    }
    fn example_nodejs_update() -> Self {
        from_json_value(json!({
            "nodeJS": "return doUpdate(source);"
        }))
        .unwrap()
    }
    fn example_remote() -> Self {
        from_json_value(json!({
            "remote": "http://example/api"
        }))
        .unwrap()
    }
}

/// A Shuffle specifies how a shuffling key is to be extracted from
/// collection documents.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "Transform::example")]
pub enum Shuffle {
    /// Shuffle by extracting the given fields.
    Key(CompositeKey),
    /// Shuffle by taking the MD5 hash of the given fields.
    /// Note this is **not** a cryptographicly strong hash,
    /// but is well suited for mapping large keys into shorter ones,
    /// or for better distributing hot-spots in a key space.
    MD5(CompositeKey),
    /// Invoke the lambda for each source document,
    /// and shuffle on its returned key.
    Lambda(Lambda),
}

/// A Transform reads and shuffles documents of a source collection,
/// and processes each document through either one or both of a register
/// "update" lambda and a derived document "publish" lambda.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "Transform::example")]
pub struct Transform {
    /// # Source collection read by this transform.
    pub source: TransformSource,
    /// # Priority applied to documents processed by this transform.
    /// When all transforms are of equal priority, Flow processes documents
    /// according to their associated publishing time, as encoded in the
    /// document UUID.
    ///
    /// However, when one transform has a higher priority than others,
    /// then *all* ready documents are processed through the transform
    /// before *any* documents of other transforms are processed.
    #[serde(default)]
    pub priority: u32,
    /// # Delay applied to documents processed by this transform.
    /// Delays are applied as an adjustment to the UUID clock encoded within each
    /// document, which is then used to impose a relative ordering of all documents
    /// read by this derivation. This means that read delays are applied in a
    /// consistent way, even when back-filling over historical documents. When caught
    /// up and tailing the source collection, delays also "gate" documents such that
    /// they aren't processed until the current wall-time reflects the delay.
    #[serde(default, with = "humantime_serde")]
    #[schemars(schema_with = "duration_schema")]
    pub read_delay: Option<Duration>,
    /// # Shuffle by which source documents are mapped to registers.
    /// If empty, the key of the source collection is used.
    #[serde(default)]
    #[schemars(default = "CompositeKey::example")]
    pub shuffle: Option<Shuffle>,
    /// # Update lambda that produces register updates from source documents.
    #[serde(default)]
    #[schemars(default = "Lambda::example_nodejs_update")]
    pub update: Option<Lambda>,
    /// # Publish lambda that produces documents to publish into the collection.
    #[serde(default)]
    #[schemars(default = "Lambda::example_nodejs_publish")]
    pub publish: Option<Lambda>,
}

impl Transform {
    fn example() -> Self {
        from_json_value(json!({
            "source": TransformSource::example(),
            "publish": Lambda::example_nodejs_publish(),
            "update": Lambda::example_nodejs_update(),
        }))
        .unwrap()
    }
}

/// SourcePartitions is optional partitions of a read source collection.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "TransformSource::example")]
pub struct TransformSource {
    /// # Name of the collection to be materialized.
    pub name: CollectionName,
    /// # Optional JSON-Schema to validate against the source collection.
    /// All data in the source collection is already validated against the
    /// schema of that collection, so providing a source schema is only used
    /// for _additional_ validation beyond that.
    ///
    /// This is useful in building "Extract Load Transform" patterns,
    /// where a collection is captured with minimal schema applied (perhaps
    /// because it comes from an uncontrolled third party), and is then
    /// progressively verified as collections are derived.
    /// If None, the principal schema of the collection is used instead.
    #[serde(default)]
    #[schemars(default = "Schema::example_relative")]
    pub schema: Option<Schema>,
    /// # Selector over partition of the source collection to read.
    #[serde(default)]
    #[schemars(default = "PartitionSelector::example")]
    pub partitions: Option<PartitionSelector>,
}

impl TransformSource {
    fn example() -> Self {
        Self {
            name: CollectionName::new("source/collection"),
            schema: None,
            partitions: None,
        }
    }
}

/// Partition selectors identify a desired subset of the
/// available logical partitions of a collection.
#[derive(Serialize, Deserialize, Debug, JsonSchema, Clone)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "PartitionSelector::example")]
pub struct PartitionSelector {
    /// Partition field names and corresponding values which must be matched
    /// from the Source collection. Only documents having one of the specified
    /// values across all specified partition names will be matched. For example,
    ///   source: [App, Web]
    ///   region: [APAC]
    /// would mean only documents of 'App' or 'Web' source and also occurring
    /// in the 'APAC' region will be processed.
    #[serde(default)]
    pub include: BTreeMap<String, Vec<Value>>,
    /// Partition field names and values which are excluded from the source
    /// collection. Any documents matching *any one* of the partition values
    /// will be excluded.
    #[serde(default)]
    pub exclude: BTreeMap<String, Vec<Value>>,
}

impl PartitionSelector {
    fn example() -> Self {
        from_json_value(json!({
            "include": {
                "a_partition": ["A", "B"],
            },
            "exclude": {
                "other_partition": [32, 64],
            }
        }))
        .unwrap()
    }
}

/// A schema is a draft 2019-09 JSON Schema which validates Flow documents.
/// Schemas also provide annotations at document locations, such as reduction
/// strategies for combining one document into another.
///
/// Schemas may be defined inline to the catalog, or given as a relative
/// or absolute URI. URIs may optionally include a JSON fragment pointer that
/// locates a specific sub-schema therein.
///
/// I.e, "schemas/marketing.yaml#/$defs/campaign" would reference the schema
/// at location {"$defs": {"campaign": ...}} within ./schemas/marketing.yaml.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(untagged)]
#[schemars(example = "Schema::example_absolute")]
#[schemars(example = "Schema::example_relative")]
#[schemars(example = "Schema::example_inline_basic")]
#[schemars(example = "Schema::example_inline_counter")]
pub enum Schema {
    Url(RelativeUrl),
    Object(Object),
    Bool(bool),
}

impl Schema {
    fn example_absolute() -> Self {
        from_json_value(json!("http://example/schema#/$defs/subPath")).unwrap()
    }
    fn example_relative() -> Self {
        from_json_value(json!("../path/to/schema#/$defs/subPath")).unwrap()
    }
    fn example_inline_basic() -> Self {
        from_json_value(json!({
            "type": "object",
            "properties": {
                "foo": { "type": "integer" },
                "bar": { "const": 42 }
            }
        }))
        .unwrap()
    }
    fn example_inline_counter() -> Self {
        from_json_value(json!({
            "type": "object",
            "reduce": {"strategy": "merge"},
            "properties": {
                "foo_count": {
                    "type": "integer",
                    "reduce": {"strategy": "sum"},
                }
            }
        }))
        .unwrap()
    }
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(example = "TestStep::example_ingest")]
#[schemars(example = "TestStep::example_verify")]
pub enum TestStep {
    /// Ingest document fixtures into a collection.
    Ingest(TestStepIngest),
    /// Verify the contents of a collection match a set of document fixtures.
    Verify(TestStepVerify),
}

impl TestStep {
    fn example_ingest() -> Self {
        TestStep::Ingest(TestStepIngest::example())
    }
    fn example_verify() -> Self {
        TestStep::Verify(TestStepVerify::example())
    }
}

/// An ingestion test step ingests document fixtures into the named
/// collection.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(example = "TestStepIngest::example")]
pub struct TestStepIngest {
    /// # Name of the collection into which the test will ingest.
    pub collection: CollectionName,
    /// # Documents to ingest.
    /// Each document must conform to the collection's schema.
    pub documents: Vec<Value>,
}

impl TestStepIngest {
    fn example() -> Self {
        from_json_value(json!({
            "collection": CollectionName::example(),
            "documents": [
                {"example": "document"},
                {"another": "document"},
            ]
        }))
        .unwrap()
    }
}

/// A verification test step verifies that the contents of the named
/// collection match the expected fixtures, after fully processing all
/// preceding ingestion test steps.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(example = "TestStepVerify::example")]
pub struct TestStepVerify {
    /// # Collection into which the test will ingest.
    pub collection: CollectionName,
    /// # Documents to verify.
    /// Each document may contain only a portion of the matched document's
    /// properties, and any properties present in the actual document but
    /// not in this document fixture are ignored. All other values must
    /// match or the test will fail.
    pub documents: Vec<Value>,
    /// # Selector over partitions to verify.
    #[serde(default)]
    #[schemars(default = "PartitionSelector::example")]
    pub partitions: Option<PartitionSelector>,
}

impl TestStepVerify {
    fn example() -> Self {
        from_json_value(json!({
            "collection": CollectionName::example(),
            "documents": [
                {"expected": "document"},
            ],
        }))
        .unwrap()
    }
}

/// Connection configuration that's used for connecting to a variety of sql databases.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct SqlTargetConnection {
    /// # The connection URI for the target database
    pub uri: String,
}

/// An Endpoint is an external system from which a Flow collection may be captured,
/// or to which a Flow collection may be materialized.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum Endpoint {
    /// # A PostgreSQL database.
    Postgres(PostgresConfig),
    /// # A SQLite database.
    Sqlite(SqliteConfig),
    /// # An S3 bucket and prefix.
    S3(BucketConfig),
    /// # A GCS bucket and prefix.
    GS(BucketConfig),
    /// # A remote implementation of an endpoint gRPC driver.
    Remote(RemoteDriverConfig),
}

impl Endpoint {
    pub fn endpoint_type(&self) -> EndpointType {
        match self {
            Endpoint::Postgres(_) => EndpointType::Postgres,
            Endpoint::Sqlite(_) => EndpointType::Sqlite,
            Endpoint::S3(_) => EndpointType::S3,
            Endpoint::GS(_) => EndpointType::GS,
            Endpoint::Remote(_) => EndpointType::Remote,
        }
    }

    pub fn base_config(&self) -> Value {
        match self {
            Endpoint::Postgres(cfg) => serde_json::to_value(cfg),
            Endpoint::Sqlite(cfg) => serde_json::to_value(cfg),
            Endpoint::S3(cfg) => serde_json::to_value(cfg),
            Endpoint::GS(cfg) => serde_json::to_value(cfg),
            Endpoint::Remote(cfg) => serde_json::to_value(cfg),
        }
        .unwrap()
    }
}

/// PostgreSQL endpoint configuration.
/// Compare to https://pkg.go.dev/github.com/lib/pq#hdr-Connection_String_Parameters
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct PostgresConfig {
    /// Preserve and pass-through all configuration.
    /// Some fields are explicit below, to benefit from JSON-Schema generation.
    #[serde(flatten)]
    pub extra: Object,

    /// # Host address of the database.
    pub host: String,
    /// # Port of the database (default: 5432).
    pub port: Option<u16>,
    /// # Connection user.
    pub user: String,
    /// # Connection password.
    pub password: String,
    /// # Logical database (default: $user).
    pub dbname: Option<String>,
}

/// Sqlite endpoint configuration.
/// Compare to https://github.com/mattn/go-sqlite3#connection-string
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct SqliteConfig {
    /// Preserve and pass-through all configuration.
    /// Some fields are explicit below, to benefit from JSON-Schema generation.
    #[serde(flatten)]
    pub extra: Object,

    /// # Filesystem path of the database.
    pub path: String,
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct BucketConfig {
    /// Preserve and pass-through all configuration.
    /// Some fields are explicit below, to benefit from JSON-Schema generation.
    #[serde(flatten)]
    pub extra: Object,
    /// # Bucket name.
    pub bucket: String,
    /// # File prefix within the bucket.
    #[serde(default)]
    pub prefix: String,
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct RemoteDriverConfig {
    /// Preserve and pass-through all configuration.
    /// Some fields are explicit below, to benefit from JSON-Schema generation.
    #[serde(flatten)]
    pub extra: Object,
    /// # gRPC address of the driver.
    pub grpc_driver_address: String,
}

/// A Materialization binds a Flow collection with an external system & target
/// (e.x, a SQL table) into which the collection is to be continuously materialized.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Materialization {
    /// # Source collection to materialize.
    pub source: MaterializationSource,
    /// # Endpoint to materialize into.
    pub endpoint: EndpointRef,
    /// # Selected projections for this materialization.
    #[serde(default)]
    pub fields: MaterializationFields,
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(example = "MaterializationSource::example")]
pub struct MaterializationSource {
    /// # Name of the collection to be materialized.
    pub name: CollectionName,
}

impl MaterializationSource {
    fn example() -> Self {
        Self {
            name: CollectionName::new("source/collection"),
        }
    }
}

/// A reference to an endpoint, with optional additional configuration.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(example = "EndpointRef::example")]
pub struct EndpointRef {
    /// # Name of the endpoint to use.
    pub name: EndpointName,
    /// # Additional endpoint configuration.
    /// Configuration is merged into that of the endpoint.
    #[serde(default)]
    pub config: Object,
}

impl EndpointRef {
    fn example() -> Self {
        Self {
            name: EndpointName::example(),
            config: vec![("table".to_string(), json!("a_sql_table"))]
                .into_iter()
                .collect(),
        }
    }
}

/// MaterializationFields defines a selection of projections to materialize,
/// as well as optional per-projection, driver-specific configuration.
#[derive(Serialize, Deserialize, Debug, JsonSchema, Clone)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "MaterializationFields::example")]
pub struct MaterializationFields {
    /// # Fields to include.
    /// This supplements any recommended fields, where enabled.
    /// Values are passed through to the driver, e.x. for customization
    /// of the driver's schema generation or runtime behavior with respect
    /// to the field.
    #[serde(default)]
    pub include: BTreeMap<String, Object>,
    /// # Fields to exclude.
    /// This removes from recommended projections, where enabled.
    #[serde(default)]
    pub exclude: Vec<String>,
    /// # Should recommended projections for the endpoint be used?
    pub recommended: bool,
}

impl Default for MaterializationFields {
    fn default() -> Self {
        MaterializationFields {
            include: BTreeMap::new(),
            exclude: Vec::new(),
            recommended: true,
        }
    }
}

impl MaterializationFields {
    fn example() -> Self {
        MaterializationFields {
            include: vec![("added".to_string(), Object::new())]
                .into_iter()
                .collect(),
            exclude: vec!["removed".to_string()],
            recommended: true,
        }
    }
}

/// List of Captures, each binding a source in an external system (e.g. cloud storage prefix)
/// to a captured collection. Captures may provided for any collection defined either within
/// the current file, or a file that's imported by it. Multiple Captures may be defined per
/// collection, and may be defined in different files.
/// A Capture binds a source of data to a target collection. The result of this binding
/// is a process that will continuously add data to the collection as it becomes available from the
/// source.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Capture {
    /// # Target collection to capture into.
    pub target: CaptureTarget,
    #[serde(flatten)]
    pub inner: CaptureType,
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(example = "CaptureTarget::example")]
pub struct CaptureTarget {
    /// # Name of the collection to be read.
    pub name: CollectionName,
}

impl CaptureTarget {
    fn example() -> Self {
        Self {
            name: CollectionName::new("target/collection"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum CaptureType {
    Endpoint(EndpointRef),
    // TODO(johnny): I'm expecting we'll introduce more behavior
    // configuration here, but I don't know what it is yet.
    PushAPI(Object),
}

fn duration_schema(_: &mut schemars::gen::SchemaGenerator) -> schema::Schema {
    from_json_value(json!({
        "type": ["string", "null"],
        "pattern": "^\\d+(s|m|h)$"
    }))
    .unwrap()
}
