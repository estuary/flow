use serde::{Deserialize, Serialize};
use serde_json;

/// Source is a YAML specification against which Estuary catalog input files are parsed.
#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Source {
    /// Additional Estuary inputs which should be processed.
    /// Derived collections must import the catalog sources of all source
    /// collections they reference.
    #[serde(default)]
    pub import: Vec<String>,
    /// Definitions of captured and derived collections.
    #[serde(default)]
    pub collections: Vec<Collection>,
    ///// Definitions of collection materializations.
    //#[serde(default)]
    //pub materializations: Vec<Materialization>,
}

/// Collection specifies an Estuary document Collection.
#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Collection {
    /// Canonical name of this Collection. I.e. "marketing/campaigns".
    pub name: String,
    /// Relative URL of this collection's JSON-Schema, with respect to path of
    /// the YAML specification which included it.
    /// I.e, "schemas/marketing.yaml#/$defs/campaign" would reference the schema
    /// at location {"$defs": {"campaign": ...}} within ./schemas/marketing.yaml.
    pub schema: String,
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
    pub projections: Vec<Projection>,
    /// A derivation specifies how this collection is derived from other
    /// collections (as opposed to being a captured collection into which
    /// documents are directly written).
    pub derivation: Option<Derivation>,
}

/// A Projection is a named location within a document.
#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Projection {
    /// Name of this projection, which is used as the field or column name in
    /// tabular databases or stores to which this collection is materialized.
    pub field: String,
    /// Location of the projected field within the document, as a JSON-Pointer.
    pub location: String,
    /// Is this projection a logical partition?
    #[serde(default)]
    pub partition: bool,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Derivation {
    /// Number of distributed processors of this derivation. Derivations which
    /// use no state may freely change this value. If state is used, beware that
    /// changing parallelism also alters the correspondence of specific shuffle
    /// keys and the processor to which they are shuffled.
    pub parallelism: Option<u8>,
    /// Lambdas to invoke when an instance of a distributed processor is started,
    /// and before any messages are processed. This is an opportunity to initialize
    /// SQL tables or other state. Note that bootstrap lambdas will be invoked for
    /// each processor, every time that processor is re-assigned to a new host
    /// (which may happen at any time).
    #[serde(default)]
    pub bootstrap: Vec<Lambda>,
    /// Transforms of source collections which produce the derived collection.
    pub transform: Vec<Transform>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum Lambda {
    /// Typescript lambda expression.
    Typescript(String),
    /// Relative URL of a file which contains a Typescript lambda expression.
    TypescriptFile(String),
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
    pub source: String,
    /// Optional relative URL of a JSON-Schema to verify against source collection
    /// documents. This is useful in building "Extract Load Transform" patterns,
    /// where a collection is captured with minimal schema applied (perhaps
    /// because it comes from an uncontrolled third party), and is then
    /// progressively verified as collections are derived.
    pub source_schema: Option<String>,
    /// Shuffle applied to source collection messages in their mapping to a
    /// specific parallel processor of the derived collection. By default,
    /// messages are shuffled on the source collection key to a single
    /// processor.
    #[serde(default)]
    pub shuffle: Shuffle,
    /// Lambda to invoke to transform a source collection document into a derived
    /// collection document.
    pub lambda: Lambda,
}

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Shuffle {
    /// Composite key on which to shuffle documents of the source collection,
    /// as an array of JSON-Pointers.
    pub key: Option<Vec<String>>,
    /// Number of processors to which this source message is sent, after ranking
    /// on the shuffle key. Default is 1. If set, the "choose" parameter must be
    /// unset or zero.
    pub broadcast: Option<u16>,
    /// Number of processors from which a single processor is randomly selected,
    /// after ranking on the shuffle key. Default is unset. If set, the "broadcast"
    /// parameter must be unset or zero.
    pub choose: Option<u16>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Fixture {
    pub document: serde_json::Value,
    pub key: Vec<serde_json::Value>,
    #[serde(default)]
    pub projections: serde_json::Map<String, serde_json::Value>,
}

/*
#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Materialization {
    pub collection: String,
    pub target: Target,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", deny_unknown_fields, rename_all = "camelCase")]
pub enum Target {
    Postgres { endpoint: String, table: String },
    Elastic { endpoint: String, index: String },
}
*/
