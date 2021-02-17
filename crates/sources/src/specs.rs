use super::proto_serde;
use models::names;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::time::Duration;

/// Each catalog source defines a portion of a Flow Catalog, by defining
/// collections, derivations, tests, and materializations of the Catalog.
/// Catalog sources may reference and import other sources, in order to
/// collections and other entities that source defines.
#[derive(Serialize, Deserialize, JsonSchema)]
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
    pub npm_dependencies: BTreeMap<String, String>,
    /// # Journal rules of the Catalog.
    /// Rules which template and modify the JournalSpecs managed by Flow.
    /// Each rule may specify a label selector, which must be matched for the rule to apply.
    /// Rules are evaluated in ascending global lexical order of their rule name.
    #[serde(default)]
    pub journal_rules: BTreeMap<names::Rule, proto_serde::JournalRule>,
    /// # Collections defined by this Catalog.
    #[serde(default)]
    #[schemars(example = "Catalog::example_collections")]
    pub collections: BTreeMap<names::Collection, CollectionDef>,
    /// # Named Endpoints of this Catalog.
    #[serde(default)]
    pub endpoints: BTreeMap<names::Endpoint, EndpointDef>,
    /// # Materializations of this Catalog.
    #[serde(default)]
    pub materializations: BTreeMap<names::Materialization, MaterializationDef>,
    /// # Captures of this Catalog.
    #[serde(default)]
    pub captures: BTreeMap<names::Capture, CaptureDef>,
    // Tests of the catalog, indexed by name.
    #[serde(default)]
    #[schemars(default = "Catalog::default_test")]
    #[schemars(example = "Catalog::example_test")]
    pub tests: BTreeMap<names::Test, Vec<TestStep>>,
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
#[schemars(example = "CollectionDef::example")]
pub struct CollectionDef {
    /// # Schema against which collection documents are validated and reduced.
    #[schemars(example = "Schema::example_relative")]
    pub schema: Schema,
    /// # Composite key of this collection.
    pub key: names::CompositeKey,
    /// # Projections and logical partitions of this collection.
    #[serde(default)]
    #[schemars(default = "Projections::example")]
    pub projections: Projections,
    /// # Derivation which builds this collection from others.
    pub derivation: Option<Derivation>,
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
}

/// A projection representation that allows projections to be specified either
/// as a simple JSON Pointer, or as an object with separate properties for
/// the location and partition indicator.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(untagged, deny_unknown_fields, rename_all = "camelCase")]
pub enum Projection {
    Pointer(names::JsonPointer),
    Object {
        /// # Location of this projection.
        location: names::JsonPointer,
        /// # Is this projection a logical partition?
        #[serde(default)]
        partition: bool,
    },
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
    pub transform: BTreeMap<names::Transform, Transform>,
}

/// A Shuffle specifies how a shuffling key is to be extracted from
/// collection documents.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "Transform::example")]
pub enum Shuffle {
    /// Shuffle by extracting the given fields.
    Key(names::CompositeKey),
    /// Shuffle by taking the MD5 hash of the given fields.
    /// Note this is **not** a cryptographicly strong hash,
    /// but is well suited for mapping large keys into shorter ones,
    /// or for better distributing hot-spots in a key space.
    Md5(names::CompositeKey),
    /// Invoke the lambda for each source document,
    /// and shuffle on its returned key.
    Lambda(names::Lambda),
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
    #[schemars(schema_with = "Transform::read_delay_schema")]
    pub read_delay: Option<Duration>,
    /// # Shuffle by which source documents are mapped to registers.
    /// If empty, the key of the source collection is used.
    #[serde(default)]
    #[schemars(default = "names::CompositeKey::example")]
    pub shuffle: Option<Shuffle>,
    /// # Update that maps a source document into register updates.
    #[serde(default)]
    #[schemars(default = "Update::example")]
    pub update: Option<Update>,
    /// # Publish that maps a source document and registers into derived documents of the collection.
    #[serde(default)]
    #[schemars(default = "Publish::example")]
    pub publish: Option<Publish>,
}

/// SourcePartitions is optional partitions of a read source collection.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "TransformSource::example")]
pub struct TransformSource {
    /// # Name of the collection to be materialized.
    pub name: names::Collection,
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
    #[schemars(default = "names::PartitionSelector::example")]
    pub partitions: Option<names::PartitionSelector>,
}

/// Publish lambdas take a source document, a current register and
/// (if there is also an "update" lambda) a previous register, and transform
/// them into one or more documents to be published into a derived collection.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "Publish::example")]
pub struct Publish {
    /// # Lambda invoked by the publish.
    pub lambda: names::Lambda,
}

/// Update lambdas take a source document and transform it into one or more
/// register updates, which are then reduced into the associated register by
/// the runtime. For example these register updates might update counters,
/// or update the state of a "join" window.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "Update::example")]
pub struct Update {
    /// # Lambda invoked by the update.
    pub lambda: names::Lambda,
    /// # Should lambda documents which produce register conflicts be ignored?
    /// Documents returned by an update lambda are checked against the register
    /// schema before being reduced into the register, and again after the register
    /// value has been fully reduced with the update. Ordinarily a failure of
    /// either check will halt processing of a derivation.
    ///
    /// Optionally, the runtime can be asked to "roll back" the reduced register
    /// value if an update causes it to violate its schema, effectively ignoring
    /// the document returned by the update lambda -- for example, because a value
    /// which must be positive would become negative with the update.
    ///
    /// This can be paired with a publish lambda which checks whether the register
    /// was updated to implement transactional workflows with complex constraint
    /// checks (for example, in a workflow which joins product inventory updates
    /// with attempted purchases).
    #[serde(default)]
    pub rollback_on_conflict: bool,
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
    Object(names::Object),
    Bool(bool),
}

/// An Endpoint is an external system from which a Flow collection may be captured,
/// or to which a Flow collection may be materialized.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum EndpointDef {
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

impl EndpointDef {
    pub fn endpoint_type(&self) -> protocol::flow::EndpointType {
        use protocol::flow::EndpointType;

        match self {
            EndpointDef::Postgres(_) => EndpointType::Postgresql,
            EndpointDef::Sqlite(_) => EndpointType::Sqlite,
            EndpointDef::S3(_) => EndpointType::S3,
            EndpointDef::GS(_) => EndpointType::Gs,
            EndpointDef::Remote(_) => EndpointType::Remote,
        }
    }

    pub fn base_config(&self) -> Value {
        match self {
            EndpointDef::Postgres(cfg) => serde_json::to_value(cfg),
            EndpointDef::Sqlite(cfg) => serde_json::to_value(cfg),
            EndpointDef::S3(cfg) => serde_json::to_value(cfg),
            EndpointDef::GS(cfg) => serde_json::to_value(cfg),
            EndpointDef::Remote(cfg) => serde_json::to_value(cfg),
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
    pub extra: names::Object,

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
    pub extra: names::Object,

    /// # Filesystem path of the database.
    pub path: String,
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct BucketConfig {
    /// Preserve and pass-through all configuration.
    /// Some fields are explicit below, to benefit from JSON-Schema generation.
    #[serde(flatten)]
    pub extra: names::Object,
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
    pub extra: names::Object,
    /// # gRPC address of the driver.
    pub grpc_driver_address: String,
}

/// A Materialization binds a Flow collection with an external system & target
/// (e.x, a SQL table) into which the collection is to be continuously materialized.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MaterializationDef {
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
    pub name: names::Collection,
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
    pub include: BTreeMap<String, names::Object>,
    /// # Fields to exclude.
    /// This removes from recommended projections, where enabled.
    #[serde(default)]
    pub exclude: Vec<String>,
    /// # Should recommended projections for the endpoint be used?
    pub recommended: bool,
}

impl Default for MaterializationFields {
    fn default() -> Self {
        Self {
            include: BTreeMap::new(),
            exclude: Vec::new(),
            recommended: true,
        }
    }
}

/// A reference to an endpoint, with optional additional configuration.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(example = "EndpointRef::example")]
pub struct EndpointRef {
    /// # Name of the endpoint to use.
    pub name: names::Endpoint,
    /// # Additional endpoint configuration.
    /// Configuration is merged into that of the endpoint.
    #[serde(default)]
    pub config: names::Object,
}

/// List of Captures, each binding a source in an external system (e.g. cloud storage prefix)
/// to a captured collection. Captures may provided for any collection defined either within
/// the current file, or a file that's imported by it. Multiple Captures may be defined per
/// collection, and may be defined in different files.
/// A Capture binds a source of data to a target collection. The result of this binding
/// is a process that will continuously add data to the collection as it becomes available from the
/// source.
// TODO(johnny): this should be serde(deny_unknown_fields), but this plays poorly
//  with JsonSchema generation, and I can't think of a good way to fix it right now.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CaptureDef {
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
    pub name: names::Collection,
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum CaptureType {
    Endpoint(EndpointRef),
    // TODO(johnny): I'm expecting we'll introduce more behavior
    // configuration here, but I don't know what it is yet.
    PushAPI(names::Object),
}

/// A URL identifying a resource, which may be a relative local path
/// with respect to the current resource (i.e, ../path/to/flow.yaml),
/// or may be an external absolute URL (i.e., http://example/flow.yaml).
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[schemars(example = "RelativeUrl::example_relative")]
#[schemars(example = "RelativeUrl::example_absolute")]
pub struct RelativeUrl(pub String);

impl std::ops::Deref for RelativeUrl {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "TestStep::example_ingest")]
#[schemars(example = "TestStep::example_verify")]
pub enum TestStep {
    /// Ingest document fixtures into a collection.
    Ingest(TestStepIngest),
    /// Verify the contents of a collection match a set of document fixtures.
    Verify(TestStepVerify),
}

/// An ingestion test step ingests document fixtures into the named
/// collection.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "TestStepIngest::example")]
pub struct TestStepIngest {
    /// # Name of the collection into which the test will ingest.
    pub collection: names::Collection,
    /// # Documents to ingest.
    /// Each document must conform to the collection's schema.
    pub documents: Vec<Value>,
}

/// A verification test step verifies that the contents of the named
/// collection match the expected fixtures, after fully processing all
/// preceding ingestion test steps.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "TestStepVerify::example")]
pub struct TestStepVerify {
    /// # Collection into which the test will ingest.
    pub collection: names::Collection,
    /// # Documents to verify.
    /// Each document may contain only a portion of the matched document's
    /// properties, and any properties present in the actual document but
    /// not in this document fixture are ignored. All other values must
    /// match or the test will fail.
    pub documents: Vec<Value>,
    /// # Selector over partitions to verify.
    #[serde(default)]
    #[schemars(default = "names::PartitionSelector::example")]
    pub partitions: Option<names::PartitionSelector>,
}
