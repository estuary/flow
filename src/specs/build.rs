use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::time::Duration;

/// Catalog is a YAML specification against which Estuary catalog input files are parsed.
#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Catalog {
    /// Additional Estuary inputs which should be processed.
    /// Derived collections must import the catalog sources of all source
    /// collections they reference.
    #[serde(default)]
    pub import: Vec<String>,
    /// Dependencies to include when building the catalog's build NodeJS
    /// package, as {"package-name": "version"}. I.e. {"moment": "^2.24"}.
    ///
    /// Version strings can take any form understood by NodeJS.
    /// See https://docs.npmjs.com/files/package.json#dependencies
    #[serde(default)]
    pub node_dependencies: BTreeMap<String, String>,
    /// Definitions of captured and derived collections.
    #[serde(default)]
    pub collections: Vec<Collection>,
}

/// Collection specifies an Estuary document Collection.
#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Collection {
    /// Canonical name of this Collection. I.e. "marketing/campaigns".
    pub name: String,
    /// This collections JSON-Schema, which all documents must validate against.
    /// The schema may be provided as a relative or absolute URL, or written inline.
    /// I.e, "schemas/marketing.yaml#/$defs/campaign" would reference the schema
    /// at location {"$defs": {"campaign": ...}} within ./schemas/marketing.yaml.
    pub schema: Schema,
    /// Composite key of this Collection, as an array of JSON-Pointers.
    pub key: Vec<String>,
    /// Relative URL of YAML or JSON files containing example "fixtures" of
    /// collection documents. Fixtures are used to test the catalog:
    /// - Fixtures of captured collections are validated against the collection
    ///   schema.
    /// - Derived collections process and transform fixture documents of their
    ///   source collections, and then validated them against their own fixtures.
    #[serde(default)]
    pub fixtures: Vec<String>,
    /// Projections are named locations within a collection document which
    /// may be used for logical partitioning or directly exposed to databases
    /// into which collections are materialized.
    #[serde(default)]
    pub projections: Projections,
    /// A derivation specifies how this collection is derived from other
    /// collections (as opposed to being a captured collection into which
    /// documents are directly written).
    pub derivation: Option<Derivation>,
    /// Materializations project a view of the current state into an external system like a
    /// database or key/value store. These states will be kept up to date automatically as
    /// documents are processed in the collection. The keys used here are arbitrary identifiers
    /// that will be used to uniquely identify each materialization, and the values are any valid
    /// Materialization object.
    #[serde(default)]
    pub materializations: BTreeMap<String, Materialization>,
}

fn default_projections_enabled() -> bool {
    true
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct DefaultProjectionSpec {
    #[serde(default = "default_projections_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub max_depth: Option<u8>,
}
impl Default for DefaultProjectionSpec {
    fn default() -> Self {
        DefaultProjectionSpec {
            enabled: default_projections_enabled(),
            max_depth: None,
        }
    }
}

/// Determines how "default" projections are generated. This allows for multiple representations in
/// an attempt to make it as ergonomic as possible. Just disabling default projections altogether
/// only requires a `defaults: false`, while more advanced options use a more future-proofed
/// object representation.
#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum DefaultProjections {
    Enabled(bool),
    Spec(DefaultProjectionSpec),
}

impl DefaultProjections {
    pub fn get_config(&self) -> DefaultProjectionSpec {
        match self {
            DefaultProjections::Spec(s) => *s,
            DefaultProjections::Enabled(true) => DefaultProjectionSpec::default(),
            DefaultProjections::Enabled(false) => DefaultProjectionSpec {
                enabled: false,
                max_depth: None,
            },
        }
    }
}

impl Default for DefaultProjections {
    fn default() -> Self {
        DefaultProjections::Enabled(true)
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Projections {
    #[serde(default)]
    pub defaults: DefaultProjections,
    #[serde(default)]
    pub fields: BTreeMap<String, Projection>,
}

impl Projections {
    pub fn iter<'a>(&'a self) -> impl Iterator<Item = ProjectionSpec<'a>> {
        self.fields
            .iter()
            .map(|(field, projection)| ProjectionSpec {
                field,
                location: &projection.location(),
                partition: projection.is_partition(),
            })
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged, deny_unknown_fields, rename_all = "camelCase")]
pub enum Projection {
    SimpleLocation(String),
    Object(FullProjection),
}
impl Projection {
    fn location(&self) -> &str {
        match self {
            Projection::SimpleLocation(loc) => loc.as_str(),
            Projection::Object(obj) => obj.location.as_str(),
        }
    }

    fn is_partition(&self) -> bool {
        match self {
            Projection::SimpleLocation(_) => false,
            Projection::Object(obj) => obj.partition,
        }
    }
}

/// A Projection is a named location within a document.
#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FullProjection {
    /// Location of the projected field within the document, as a JSON-Pointer.
    pub location: String,
    /// Is this projection a logical partition?
    #[serde(default)]
    pub partition: bool,
}

/// An intermediate representation of a Projection that is used for both explicit projections
/// defined in the catalog spec, as well as default projections that are generated automatically.
#[derive(Debug)]
pub struct ProjectionSpec<'a> {
    /// Name of this projection, which is used as the field or column name in
    /// tabular databases or stores to which this collection is materialized.
    pub field: &'a str,
    /// Location of the projected field within the document, as a JSON-Pointer.
    pub location: &'a str,
    /// Is this projection a logical partition?
    pub partition: bool,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Register {
    /// The schema of this register, which all register instances must validate against.
    /// Reduction annotations from the schema are used to reduce registers into a single,
    /// current value for each key.
    pub schema: Schema,
    /// The initial value of a register which hasn't been written to yet.
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

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Derivation {
    /// A derivation "register" is an place to store arbitrary internal state
    /// which is shared between transforms of the derivation, and available to
    /// lambdas alongside the source document which is being processed.
    /// Derivations may have an arbitrary number of registers, where each register
    /// is keyed on the shuffle ID of the source document.
    #[serde(default)]
    pub register: Register,
    /// Lambdas to invoke when an instance of a distributed processor is started,
    /// and before any messages are processed. This is an opportunity to initialize
    /// SQL tables or other state. Note that bootstrap lambdas will be invoked for
    /// each processor, every time that processor is re-assigned to a new host
    /// (which may happen at any time).
    #[serde(default)]
    pub bootstrap: Vec<Lambda>,
    /// Transforms of source collections which produce the derived collection.
    pub transform: BTreeMap<String, Transform>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum Lambda {
    /// Typescript / JavaScript expression.
    NodeJS(String),
    /// SQLite lambda expression.
    Sqlite(String),
    /// Relative URL of a file which contains a SQLite lambda expression.
    SqliteFile(String),
    /// Remote endpoint URL of a compatible lambda function.
    Remote(String),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Transform {
    /// Source collection read by this transform.
    pub source: Source,
    /// Delay applied to documents read by this transform. Delays are applied
    /// as an adjustment to the UUID clock encoded within each document, which
    /// is then used to impose a relative ordering of all documents read by this
    /// derivation. This means that read delays are applied in a consistent way,
    /// even when back-filling over historical documents. When caught up and
    /// tailing the source collection, delays also "gate" documents such that
    /// they aren't processed until the current wall-time reflects the delay.
    #[serde(default, with = "humantime_serde")]
    pub read_delay: Option<Duration>,
    /// Shuffle key by which source collection messages are mapped to a
    /// derivation register, as an array of JSON-Pointers. If empty, the key of
    /// the source collection is used.
    // TODO(johnny): It was a whoops to hoist the key back here.
    // We want this to be able to represent a hash function to use also.
    #[serde(default)]
    pub shuffle: Option<Vec<String>>,
    /// An "update" lambda takes a source document and associated register,
    /// produces documents to be reduced back into the register
    /// according to its schema.
    #[serde(default)]
    pub update: Option<Lambda>,
    /// A "publish" lambda takes a source document and associated register,
    /// and produces or more documents to be published into the derived collection.
    /// If the transform has both "update" and "publish" lambdas, the "update"
    /// lambda is run first, its output is reduced into the register,
    /// and then the "publish" lambda is invoked with the result.
    #[serde(default)]
    pub publish: Option<Lambda>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Source {
    /// Name of the source collection.
    pub name: String,
    /// Optional JSON-Schema to validate against the source collection. All data in the source
    /// collection is already validated against the schema of that collection, so providing a
    /// schema here is only used for _additional_ validation beyond that.
    /// This is useful in building "Extract Load Transform" patterns,
    /// where a collection is captured with minimal schema applied (perhaps
    /// because it comes from an uncontrolled third party), and is then
    /// progressively verified as collections are derived.
    /// If None, the principal schema of the collection is used instead.
    #[serde(default)]
    pub schema: Option<Schema>,
    /// Partition selector over partitions of the source collection to be read.
    #[serde(default)]
    pub partitions: PartitionSelector,
}

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
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

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Fixture {
    pub document: Value,
    pub key: Vec<Value>,
    #[serde(default)]
    pub projections: BTreeMap<String, Value>,
}

/// Used for collection schemas and transform source schemas, to allow flexibility in how they can
/// be represented. The main distinction we're concerned with is whether the schema is provided
/// inline or as a URI pointing to an external schema resource.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(untagged)]
pub enum Schema {
    /// Schema was provided as a URI that is expected to resolve to a JSON schema.
    Url(String),
    /// Schema provided directly inline as a JSON object.
    Object(BTreeMap<String, Value>),
    /// Schema provided directly inline as a boolean. This is only ever really useful if the value
    /// is the literal `true`, which permits all JSON data. A value of `false` would reject all
    /// data.
    Bool(bool),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SqlTargetConnection {
    pub uri: String,
    pub table: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", deny_unknown_fields, rename_all = "camelCase")]
pub enum Materialization {
    Postgres {
        #[serde(flatten)]
        connection: SqlTargetConnection,
    },
    Sqlite {
        #[serde(flatten)]
        connection: SqlTargetConnection,
    },
}
