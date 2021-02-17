/// Slice represents a contiguous slice of bytes within an associated Arena.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Slice {
    #[prost(uint32, tag = "1")]
    pub begin: u32,
    #[prost(uint32, tag = "2")]
    pub end: u32,
}
/// UUIDParts is a deconstructed, RFC 4122 v1 variant Universally Unique
/// Identifier as used by Gazette.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UuidParts {
    /// Producer is the unique node identifier portion of a v1 UUID, as the high
    /// 48 bits of |producer_and_flags|. The MSB must be 1 to mark this producer
    /// as "multicast" and not an actual MAC address (as per RFC 4122).
    ///
    /// Bits 49-54 must be zero.
    ///
    /// The low 10 bits are the 10 least-significant bits of the v1 UUID clock
    /// sequence, used by Gazette to represent flags over message transaction
    /// semantics.
    #[prost(fixed64, tag = "1")]
    pub producer_and_flags: u64,
    /// Clock is a v1 UUID 60-bit timestamp (60 MSBs), followed by 4 bits of
    /// sequence counter.
    #[prost(fixed64, tag = "2")]
    pub clock: u64,
}
/// LambdaSpec describes a Flow transformation lambda and how to invoke it.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LambdaSpec {
    /// If non-empty, this is a TypeScript lambda and the field is its invocation path.
    /// E.x. 'some/derivation/andTransform/Update'.
    #[prost(string, tag = "1")]
    pub typescript: ::prost::alloc::string::String,
    /// If non-empty, this is a remote lambda and the field is its invocation URL.
    /// E.x. 'https://my/external/api'.
    #[prost(string, tag = "2")]
    pub remote: ::prost::alloc::string::String,
}
/// Shuffle is a description of a document shuffle, where each document
/// is mapped into:
///  * An extracted, packed composite key (a "shuffle key").
///  * A rotated Clock value (an "r-clock").
/// The packed key and r-clock can then be compared to individual reader
/// RangeSpec's.
///
/// Shuffle is a complete description of how a group of related readers
/// (e.x. a set of shards collectively processing a single derivation or
/// materialization) are performing their read. It contains all (and only!)
/// stable descriptions of the read's behavior, and is a primary structure
/// across both the shuffle server and client implementations.
///
/// Shuffles are also compared using deep equality in order to identify and
/// group related reads, placing all reads having equal Shuffles into common
/// "read rings" which consolidate their underlying journal reads.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Shuffle {
    /// Group to which this shuffle belongs. It's used to suffix all journal
    /// reads undertaken by this shuffle, and must be stable. Examples:
    ///  `derive/{derivation}/{transform}`
    ///  `materialize/{materialization}`
    #[prost(string, tag = "1")]
    pub group_name: ::prost::alloc::string::String,
    /// Source collection read by this transform.
    #[prost(string, tag = "2")]
    pub source_collection: ::prost::alloc::string::String,
    /// Selector of partitions of the collection which this transform reads.
    #[prost(message, optional, tag = "3")]
    pub source_partitions: ::core::option::Option<super::protocol::LabelSelector>,
    /// JSON pointer locating the UUID of each source document.
    #[prost(string, tag = "4")]
    pub source_uuid_ptr: ::prost::alloc::string::String,
    /// Composite key over which shuffling occurs, specified as one or more
    /// JSON-Pointers indicating a message location to extract.
    #[prost(string, repeated, tag = "5")]
    pub shuffle_key_ptr: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// uses_source_key is true if shuffle_key_ptr is the source's native key,
    /// and false if it's some other key. When shuffling using the source's key,
    /// we can minimize data movement by assigning a shard coordinator for each
    /// journal such that the shard's key range overlap that of the journal.
    #[prost(bool, tag = "6")]
    pub uses_source_key: bool,
    /// Computed shuffle lambda. If non-nil, then shuffle_key_ptr MUST be empty
    /// and uses_source_key MUST be false.
    #[prost(message, optional, tag = "7")]
    pub shuffle_lambda: ::core::option::Option<LambdaSpec>,
    /// Schema against which shuffled documents are to be validated.
    #[prost(string, tag = "8")]
    pub source_schema_uri: ::prost::alloc::string::String,
    /// uses_source_schema is true iff source_schema_uri is the source collection's
    /// schema, and false if it's a source schema specific to this transform.
    #[prost(bool, tag = "9")]
    pub uses_source_schema: bool,
    /// filter_r_clocks is true if the shuffle coordinator should filter documents
    /// sent to each subscriber based on its covered r-clock ranges and the
    /// individual document clocks. If false, the subscriber's r-clock range is
    /// ignored and all documents which match the key range are sent.
    ///
    /// filter_r_clocks is set 'true' when reading on behalf of transforms having
    /// a "publish" but not an "update" lambda, as such documents have no
    /// side-effects on the reader's state store, and would not be published anyway
    /// for falling outside of the reader's r-clock range.
    #[prost(bool, tag = "10")]
    pub filter_r_clocks: bool,
    #[prost(enumeration = "shuffle::Hash", tag = "11")]
    pub hash: i32,
    /// Number of seconds for which documents of this collection are delayed
    /// while reading, relative to other documents (when back-filling) and the
    /// present wall-clock time (when tailing).
    #[prost(uint32, tag = "12")]
    pub read_delay_seconds: u32,
    /// Priority of this shuffle, with respect to other related Shuffle reads
    /// (e.x. Shuffles of a different transformation within the same derivation).
    /// Higher values imply higher priority.
    #[prost(uint32, tag = "13")]
    pub priority: u32,
}
/// Nested message and enum types in `Shuffle`.
pub mod shuffle {
    /// Optional hash applied to extracted, packed shuffle keys. Hashes can:
    /// * Mitigate shard skew which might otherwise occur due to key locality
    ///   (many co-occurring updates to "nearby" keys).
    /// * Give predictable storage sizes for keys which are otherwise unbounded.
    /// * Allow for joins over sensitive fields, which should not be stored
    ///   in-the-clear at rest where possible.
    /// Either cryptographic or non-cryptographic functions may be appropriate
    /// depending on the use case.
    #[derive(serde::Deserialize, serde::Serialize)]
    #[serde(deny_unknown_fields)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Hash {
        /// None performs no hash, returning the original key.
        None = 0,
        /// MD5 returns the MD5 digest of the original key. It is not a safe
        /// cryptographic hash, but is well-known and fast, with good distribution
        /// properties.
        Md5 = 1,
    }
}
/// JournalShuffle is a Shuffle of a Journal by a Coordinator shard.
/// They're compared using deep equality in order to consolidate groups of
/// related logical reads into a single physical read of the journal.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct JournalShuffle {
    /// Journal to be shuffled.
    #[prost(string, tag = "1")]
    pub journal: ::prost::alloc::string::String,
    /// Coordinator is the Shard ID which is responsible for reads of this journal.
    #[prost(string, tag = "2")]
    pub coordinator: ::prost::alloc::string::String,
    /// Shuffle of this JournalShuffle.
    #[prost(message, optional, tag = "3")]
    pub shuffle: ::core::option::Option<Shuffle>,
    /// Is this a reply of the journal's content?
    /// We separate ongoing vs replayed reads of a journal's content into
    /// distinct rings, so that ongoing reads cannot deadlock a replay read.
    ///
    /// If we didn't do this, a shard might issue a replay read while
    /// *also* having a full recv queue of its ongoing read. Then, the
    /// the server would on sending yet another ongoing read, such that
    /// it's unable to service the replay read that would ultimately
    /// unblock the shard / allow it to drain new ongoing reads.
    #[prost(bool, tag = "4")]
    pub replay: bool,
}
/// Projection is a mapping between a document location, specified as a
/// JSON-Pointer, and a corresponding field string in a flattened
/// (i.e. tabular or SQL) namespace which aliases it.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Projection {
    /// Document location of this projection, as a JSON-Pointer.
    #[prost(string, tag = "1")]
    pub ptr: ::prost::alloc::string::String,
    /// Field is the flattened, tabular alias of this projection.
    #[prost(string, tag = "2")]
    pub field: ::prost::alloc::string::String,
    /// Was this projection user provided ?
    #[prost(bool, tag = "3")]
    pub user_provided: bool,
    /// Does this projection constitute a logical partitioning of the collection?
    #[prost(bool, tag = "4")]
    pub is_partition_key: bool,
    /// Does this location form (part of) the collection key?
    #[prost(bool, tag = "5")]
    pub is_primary_key: bool,
    /// Inference of this projection.
    #[prost(message, optional, tag = "6")]
    pub inference: ::core::option::Option<Inference>,
}
/// Inference details type information which is statically known
/// about a given document location.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Inference {
    /// The possible types for this location.
    /// Subset of ["null", "boolean", "object", "array", "integer", "numeric", "string"].
    #[prost(string, repeated, tag = "1")]
    pub types: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Whether the projection must always exist (either as a location within)
    /// the source document, or as a null-able column in the database.
    #[prost(bool, tag = "2")]
    pub must_exist: bool,
    #[prost(message, optional, tag = "3")]
    pub string: ::core::option::Option<inference::String>,
    /// The title from the schema, if provided
    #[prost(string, tag = "4")]
    pub title: ::prost::alloc::string::String,
    /// The description from the schema, if provided
    #[prost(string, tag = "5")]
    pub description: ::prost::alloc::string::String,
}
/// Nested message and enum types in `Inference`.
pub mod inference {
    /// String type-specific inferences.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct String {
        /// Annotated Content-Type when the projection is of "string" type.
        #[prost(string, tag = "3")]
        pub content_type: ::prost::alloc::string::String,
        /// Annotated format when the projection is of "string" type.
        #[prost(string, tag = "4")]
        pub format: ::prost::alloc::string::String,
        /// Whether the value is base64-encoded when the projection is of "string" type.
        #[prost(bool, tag = "5")]
        pub is_base64: bool,
        /// Maximum length when the projection is of "string" type. Zero for no limit.
        #[prost(uint32, tag = "6")]
        pub max_length: u32,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CollectionSpec {
    /// Name of this collection.
    #[prost(string, tag = "1")]
    pub collection: ::prost::alloc::string::String,
    /// Schema against which collection documents are validated,
    /// and which provides reduction annotations.
    #[prost(string, tag = "2")]
    pub schema_uri: ::prost::alloc::string::String,
    /// Composite key of the collection, as JSON-Pointers.
    #[prost(string, repeated, tag = "3")]
    pub key_ptrs: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// JSON pointer locating the UUID of each collection document.
    #[prost(string, tag = "4")]
    pub uuid_ptr: ::prost::alloc::string::String,
    /// Logical partition fields of this collection.
    #[prost(string, repeated, tag = "5")]
    pub partition_fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Logical projections of this collection
    #[prost(message, repeated, tag = "6")]
    pub projections: ::prost::alloc::vec::Vec<Projection>,
    /// JSON-encoded document template for creating Gazette consumer
    /// transaction acknowledgements of writes into this collection.
    #[prost(string, tag = "7")]
    pub ack_json_template: ::prost::alloc::string::String,
}
/// TransformSpec describes a specific transform of a derivation.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransformSpec {
    /// Derivation this transform belongs to.
    #[prost(string, tag = "1")]
    pub derivation: ::prost::alloc::string::String,
    /// Name of this transform, scoped to it's derivation.
    #[prost(string, tag = "2")]
    pub transform: ::prost::alloc::string::String,
    /// Shuffle applied to source documents for this transform.
    #[prost(message, optional, tag = "3")]
    pub shuffle: ::core::option::Option<Shuffle>,
    /// Update lambda of this transform, if any.
    #[prost(message, optional, tag = "4")]
    pub update_lambda: ::core::option::Option<LambdaSpec>,
    /// If an applied update causes the register to be invalid against its
    /// schema, should the document roll back instead of failing processing?
    #[prost(bool, tag = "5")]
    pub rollback_on_register_conflict: bool,
    /// Publish lambda of this transform, if any.
    #[prost(message, optional, tag = "6")]
    pub publish_lambda: ::core::option::Option<LambdaSpec>,
}
/// DerivationSpec describes a collection, and it's means of derivation.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DerivationSpec {
    /// Derivations are collections.
    #[prost(message, optional, tag = "1")]
    pub collection: ::core::option::Option<CollectionSpec>,
    /// Schema against which derivation registers are validated,
    /// and which provides reduction annotations.
    #[prost(string, tag = "2")]
    pub register_schema_uri: ::prost::alloc::string::String,
    /// JSON-encoded initial value of novel document registers.
    #[prost(string, tag = "3")]
    pub register_initial_json: ::prost::alloc::string::String,
    /// Transforms of this derivation.
    #[prost(message, repeated, tag = "4")]
    pub transforms: ::prost::alloc::vec::Vec<TransformSpec>,
}
/// FieldSelection is a selection of a collection's projection fields.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FieldSelection {
    /// Fields for each key component of the collection. Keys have the same length
    /// as the collection's key, and the document pointer of the field at each index
    /// matches the pointer identified by the corresponding index of the collection key.
    #[prost(string, repeated, tag = "1")]
    pub keys: ::std::vec::Vec<std::string::String>,
    /// All other selected fields, other than those in keys and the document field.
    /// Entries are in ascending sorted order, and may be empty.
    #[prost(string, repeated, tag = "2")]
    pub values: ::std::vec::Vec<std::string::String>,
    /// Field having a document pointer located at the document root.
    #[prost(string, tag = "3")]
    pub document: std::string::String,
    /// Additional configuration, keyed by fields included in |keys|, |values|, or |document|.
    /// Values are arbitrary JSON-encoded objects.
    #[prost(map = "string, string", tag = "4")]
    pub field_config: ::std::collections::HashMap<std::string::String, std::string::String>,
}
/// MaterializationSpec describes a collection and its materialization to an endpoint.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MaterializationSpec {
    /// Name of this materialization.
    #[prost(string, tag = "1")]
    pub materialization: std::string::String,
    /// Collection to be materialized.
    #[prost(message, optional, tag = "2")]
    pub collection: ::std::option::Option<CollectionSpec>,
    /// Type of the materialization's endpoint.
    #[prost(enumeration = "EndpointType", tag = "3")]
    pub endpoint_type: i32,
    /// JSON-encoded object which configures this materialization with
    /// respect to the endpoint type driver.
    #[prost(string, tag = "4")]
    pub endpoint_config: std::string::String,
    /// Resolved fields selected for materialization.
    #[prost(message, optional, tag = "5")]
    pub field_selection: ::std::option::Option<FieldSelection>,
}
/// RangeSpec describes the ranges of shuffle keys and r-clocks which a reader
/// is responsible for.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RangeSpec {
    /// Byte [begin, end) exclusive range of keys to be shuffled to this reader.
    #[prost(bytes = "vec", tag = "2")]
    pub key_begin: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub key_end: ::prost::alloc::vec::Vec<u8>,
    /// Rotated [begin, end) exclusive ranges of Clocks to be shuffled to this
    /// reader.
    #[prost(uint64, tag = "4")]
    pub r_clock_begin: u64,
    #[prost(uint64, tag = "5")]
    pub r_clock_end: u64,
}
/// JournalRules are an ordered sequence of Rules which specify a
/// condition -- as a label selector -- and, if matched, a template
/// to apply to the base JournalSpec.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct JournalRules {
    #[prost(message, repeated, tag = "1")]
    pub rules: ::prost::alloc::vec::Vec<journal_rules::Rule>,
}
/// Nested message and enum types in `JournalRules`.
pub mod journal_rules {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Rule {
        /// Name of the rule.
        #[prost(string, tag = "1")]
        pub rule: ::prost::alloc::string::String,
        /// Label selector which must pass for the template to be applied.
        #[prost(message, optional, tag = "2")]
        pub selector: ::core::option::Option<super::super::protocol::LabelSelector>,
        /// Template to union into the base JournalSpec.
        #[prost(message, optional, tag = "3")]
        pub template: ::core::option::Option<super::super::protocol::JournalSpec>,
    }
}
/// ShardRules are an ordered sequence of Rules which specify a
/// condition -- as a label selector -- and, if matched, a template
/// to apply to the base ShardSpec.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardRules {
    #[prost(message, repeated, tag = "1")]
    pub rules: ::prost::alloc::vec::Vec<shard_rules::Rule>,
}
/// Nested message and enum types in `ShardRules`.
pub mod shard_rules {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Rule {
        /// Label selector which must pass for the template to be applied.
        #[prost(message, optional, tag = "1")]
        pub selector: ::core::option::Option<super::super::protocol::LabelSelector>,
        /// Template to union into the base JournalSpec.
        #[prost(message, optional, tag = "2")]
        pub template: ::core::option::Option<super::super::consumer::ShardSpec>,
    }
}
/// SchemaBundle is a bundle of JSON schemas and their base URI.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SchemaBundle {
    /// Schemas of the bundle. Keys are the absolute URIs of the schema,
    /// and values are JSON-encoded schema documents.
    #[prost(map = "string, string", tag = "1")]
    pub bundle:
        ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
}
/// ShuffleRequest is the request message of a Shuffle RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleRequest {
    /// Journal to be shuffled, routed to a coordinator.
    #[prost(message, optional, tag = "1")]
    pub shuffle: ::core::option::Option<JournalShuffle>,
    /// Resolution header of the |shuffle.coordinator| shard.
    #[prost(message, optional, tag = "2")]
    pub resolution: ::core::option::Option<super::protocol::Header>,
    /// Ranges of responsibility which are unique to this reader,
    /// against which document shuffle outcomes are matched to determine
    /// read eligibility.
    #[prost(message, optional, tag = "3")]
    pub range: ::core::option::Option<RangeSpec>,
    /// Offset to begin reading the journal from.
    #[prost(int64, tag = "4")]
    pub offset: i64,
    /// Offset to stop reading the journal at, or zero if unbounded.
    #[prost(int64, tag = "5")]
    pub end_offset: i64,
}
/// ShuffleResponse is the streamed response message of a Shuffle RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleResponse {
    /// Status of the Shuffle RPC.
    #[prost(enumeration = "super::consumer::Status", tag = "1")]
    pub status: i32,
    /// Header of the response.
    #[prost(message, optional, tag = "2")]
    pub header: ::core::option::Option<super::protocol::Header>,
    /// Terminal error encountered while serving this ShuffleRequest. A terminal
    /// error is only sent if a future ShuffleRequest of this same configuration
    /// and offset will fail in the exact same way, and operator intervention is
    /// required to properly recover. Such errors are returned so that the caller
    /// can also abort with a useful, contextual error message.
    ///
    /// Examples of terminal errors include the requested journal not existing,
    /// or data corruption. Errors *not* returned as |terminal_error| include
    /// network errors, process failures, and other conditions which can be
    /// retried.
    #[prost(string, tag = "3")]
    pub terminal_error: ::prost::alloc::string::String,
    /// Offset which was read through to produce this ShuffleResponse.
    #[prost(int64, tag = "4")]
    pub read_through: i64,
    /// WriteHead of the journal as reported by the broker, as of the creation of
    /// this ShuffleResponse.
    #[prost(int64, tag = "5")]
    pub write_head: i64,
    /// Memory arena of this message.
    #[prost(bytes = "vec", tag = "6")]
    pub arena: ::prost::alloc::vec::Vec<u8>,
    /// Shuffled documents, each encoded in the 'application/json'
    /// media-type.
    #[prost(message, repeated, tag = "7")]
    pub docs_json: ::prost::alloc::vec::Vec<Slice>,
    /// The begin offset of each document within the requested journal.
    #[prost(int64, repeated, packed = "false", tag = "8")]
    pub begin: ::prost::alloc::vec::Vec<i64>,
    /// The end offset of each document within the journal.
    #[prost(int64, repeated, packed = "false", tag = "9")]
    pub end: ::prost::alloc::vec::Vec<i64>,
    /// UUIDParts of each document.
    #[prost(message, repeated, tag = "10")]
    pub uuid_parts: ::prost::alloc::vec::Vec<UuidParts>,
    /// Packed, embedded encoding of the shuffle key into a byte string.
    /// If the Shuffle specified a Hash to use, it's applied as well.
    #[prost(message, repeated, tag = "11")]
    pub packed_key: ::prost::alloc::vec::Vec<Slice>,
}
/// Schema API takes SchemaBundle as its request message with code 1,
/// and sends back instances of BuiltIndex with code 1.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SchemaApi {}
/// Nested message and enum types in `SchemaAPI`.
pub mod schema_api {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct BuiltIndex {
        #[prost(fixed64, tag = "1")]
        pub schema_index_memptr: u64,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExtractApi {}
/// Nested message and enum types in `ExtractAPI`.
pub mod extract_api {
    /// TODO - pass schema_index_memptr & Shuffle spec here, migrating this
    /// towards a ShuffleAPI which takes over some of the responsibility of
    /// current Go implementation.
    ///
    /// Have ExtractAPI do schema validation so DeriveAPI doesn't have to.
    /// When we introduce computed lambdas, it will be within this API
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Config {
        /// JSON pointer of the document UUID to extract.
        /// If empty, UUIDParts are not extracted.
        #[prost(string, tag = "1")]
        pub uuid_ptr: ::prost::alloc::string::String,
        /// Field JSON pointers to extract from documents and return.
        /// If empty, no fields are extracted.
        #[prost(string, repeated, tag = "2")]
        pub field_ptrs: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CombineApi {}
/// Nested message and enum types in `CombineAPI`.
pub mod combine_api {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Config {
        /// Memory address of a shared SchemaIndex, which must exist for
        /// the complete lifetime of this API's use.
        #[prost(fixed64, tag = "1")]
        pub schema_index_memptr: u64,
        /// Schema against which documents are to be validated,
        /// and which provides reduction annotations.
        #[prost(string, tag = "2")]
        pub schema_uri: ::prost::alloc::string::String,
        /// Composite key used to group documents to be combined, specified as one or
        /// more JSON-Pointers indicating a message location to extract.
        /// If empty, all request documents are combined into a single response
        /// document.
        #[prost(string, repeated, tag = "3")]
        pub key_ptr: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
        /// Field JSON pointers to be extracted from combined documents and returned.
        /// If empty, no fields are extracted.
        #[prost(string, repeated, tag = "4")]
        pub field_ptrs: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
        /// JSON-Pointer at which a placeholder UUID should be inserted into
        /// returned documents. If empty, no placeholder is inserted.
        #[prost(string, tag = "5")]
        pub uuid_placeholder_ptr: ::prost::alloc::string::String,
    }
}
/// DeriveAPI is a meta-message which name spaces messages of the Derive API bridge.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeriveApi {}
/// Nested message and enum types in `DeriveAPI`.
pub mod derive_api {
    /// Config configures an instance of the derive service.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Config {
        /// Memory address of a shared SchemaIndex, which must exist for
        /// the complete lifetime of this API's use.
        #[prost(fixed64, tag = "1")]
        pub schema_index_memptr: u64,
        /// Derivation to derive.
        #[prost(message, optional, tag = "2")]
        pub derivation: ::core::option::Option<super::DerivationSpec>,
        /// Memory address of an RocksDB Environment to use (as a *rocksdb_env_t).
        /// Ownership of the environment is transferred with this message.
        #[prost(fixed64, tag = "3")]
        pub rocksdb_env_memptr: u64,
        /// Local directory for ephemeral processing state.
        #[prost(string, tag = "4")]
        pub local_dir: ::prost::alloc::string::String,
        /// Unix Domain Socket at which the JavaScript transformer may be reached.
        #[prost(string, tag = "5")]
        pub typescript_uds_path: ::prost::alloc::string::String,
    }
    /// DocHeader precedes a JSON-encoded document.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct DocHeader {
        /// UUID of this document.
        #[prost(message, optional, tag = "1")]
        pub uuid: ::core::option::Option<super::UuidParts>,
        /// FDB packed shuffle key of the document.
        #[prost(bytes = "vec", tag = "2")]
        pub packed_key: ::prost::alloc::vec::Vec<u8>,
        /// Index of the transformation under which this document is being
        /// processed, within the configured DerivationSpec.
        #[prost(uint32, tag = "3")]
        pub transform_index: u32,
    }
    /// Prepare a commit of the transaction.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Prepare {
        /// Checkpoint to commit.
        #[prost(message, optional, tag = "1")]
        pub checkpoint: ::core::option::Option<super::super::consumer::Checkpoint>,
    }
}
/// IngestRequest describes documents to ingest into collections.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IngestRequest {
    #[prost(message, repeated, tag = "1")]
    pub collections: ::prost::alloc::vec::Vec<ingest_request::Collection>,
}
/// Nested message and enum types in `IngestRequest`.
pub mod ingest_request {
    /// Collection describes an ingest into a collection.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Collection {
        /// Name of the collection into which to ingest.
        #[prost(string, tag = "1")]
        pub name: ::prost::alloc::string::String,
        /// Newline-separated JSON documents to ingest.
        #[prost(bytes = "vec", tag = "2")]
        pub docs_json_lines: ::prost::alloc::vec::Vec<u8>,
    }
}
/// IngestResponse is the result of an Ingest RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IngestResponse {
    /// Journals appended to by this ingestion, and their maximum offset on commit.
    #[prost(map = "string, int64", tag = "1")]
    pub journal_write_heads: ::std::collections::HashMap<::prost::alloc::string::String, i64>,
    /// Etcd header which describes current journal partitions.
    #[prost(message, optional, tag = "2")]
    pub journal_etcd: ::core::option::Option<super::protocol::header::Etcd>,
}
/// AdvanceTimeRequest is a testing-only request to modify effective test time.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AdvanceTimeRequest {
    /// Number of seconds to add to the current clock delta.
    #[prost(uint64, tag = "1")]
    pub add_clock_delta_seconds: u64,
}
/// AdvanceTimeRequest is a testing-only response to modify effective test time.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AdvanceTimeResponse {
    /// Current effective delta from wall-clock time, in seconds.
    #[prost(uint64, tag = "1")]
    pub clock_delta_seconds: u64,
}
/// ClearRegistersRequest is a testing-only request to remove all registers of a shard.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ClearRegistersRequest {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<super::protocol::Header>,
    #[prost(string, tag = "2")]
    pub shard_id: ::prost::alloc::string::String,
}
/// ClearRegistersResponse is a testing-only response to remove all registers of a shard.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ClearRegistersResponse {
    #[prost(enumeration = "super::consumer::Status", tag = "1")]
    pub status: i32,
    #[prost(message, optional, tag = "2")]
    pub header: ::core::option::Option<super::protocol::Header>,
}
/// EndpointType enumerates the endpoint types understood by Flow.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub enum EndpointType {
    /// Remote is an arbitrary gRPC materialization protocol server.
    Remote = 0,
    Postgresql = 1,
    Sqlite = 2,
    S3 = 3,
    Gs = 4,
}
/// ContentType enumerates the content types understood by Flow.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub enum ContentType {
    CatalogSpec = 0,
    JsonSchema = 1,
    TypescriptModule = 2,
    NpmPackage = 3,
}
#[doc = r" Generated client implementations."]
pub mod shuffler_client {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    pub struct ShufflerClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ShufflerClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> ShufflerClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + HttpBody + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = tonic::client::Grpc::with_interceptor(inner, interceptor);
            Self { inner }
        }
        pub async fn shuffle(
            &mut self,
            request: impl tonic::IntoRequest<super::ShuffleRequest>,
        ) -> Result<tonic::Response<tonic::codec::Streaming<super::ShuffleResponse>>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/flow.Shuffler/Shuffle");
            self.inner
                .server_streaming(request.into_request(), path, codec)
                .await
        }
    }
    impl<T: Clone> Clone for ShufflerClient<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }
    impl<T> std::fmt::Debug for ShufflerClient<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "ShufflerClient {{ ... }}")
        }
    }
}
#[doc = r" Generated client implementations."]
pub mod ingester_client {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = " Ingester offers transactional ingest of documents into collections."]
    pub struct IngesterClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl IngesterClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> IngesterClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + HttpBody + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = tonic::client::Grpc::with_interceptor(inner, interceptor);
            Self { inner }
        }
        pub async fn ingest(
            &mut self,
            request: impl tonic::IntoRequest<super::IngestRequest>,
        ) -> Result<tonic::Response<super::IngestResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/flow.Ingester/Ingest");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
    impl<T: Clone> Clone for IngesterClient<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }
    impl<T> std::fmt::Debug for IngesterClient<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "IngesterClient {{ ... }}")
        }
    }
}
#[doc = r" Generated client implementations."]
pub mod testing_client {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    pub struct TestingClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl TestingClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> TestingClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + HttpBody + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = tonic::client::Grpc::with_interceptor(inner, interceptor);
            Self { inner }
        }
        pub async fn advance_time(
            &mut self,
            request: impl tonic::IntoRequest<super::AdvanceTimeRequest>,
        ) -> Result<tonic::Response<super::AdvanceTimeResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/flow.Testing/AdvanceTime");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn clear_registers(
            &mut self,
            request: impl tonic::IntoRequest<super::ClearRegistersRequest>,
        ) -> Result<tonic::Response<super::ClearRegistersResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/flow.Testing/ClearRegisters");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
    impl<T: Clone> Clone for TestingClient<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }
    impl<T> std::fmt::Debug for TestingClient<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestingClient {{ ... }}")
        }
    }
}
#[doc = r" Generated server implementations."]
pub mod shuffler_server {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with ShufflerServer."]
    #[async_trait]
    pub trait Shuffler: Send + Sync + 'static {
        #[doc = "Server streaming response type for the Shuffle method."]
        type ShuffleStream: Stream<Item = Result<super::ShuffleResponse, tonic::Status>>
            + Send
            + Sync
            + 'static;
        async fn shuffle(
            &self,
            request: tonic::Request<super::ShuffleRequest>,
        ) -> Result<tonic::Response<Self::ShuffleStream>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct ShufflerServer<T: Shuffler> {
        inner: _Inner<T>,
    }
    struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
    impl<T: Shuffler> ShufflerServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, None);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, Some(interceptor.into()));
            Self { inner }
        }
    }
    impl<T, B> Service<http::Request<B>> for ShufflerServer<T>
    where
        T: Shuffler,
        B: HttpBody + Send + Sync + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/flow.Shuffler/Shuffle" => {
                    #[allow(non_camel_case_types)]
                    struct ShuffleSvc<T: Shuffler>(pub Arc<T>);
                    impl<T: Shuffler> tonic::server::ServerStreamingService<super::ShuffleRequest> for ShuffleSvc<T> {
                        type Response = super::ShuffleResponse;
                        type ResponseStream = T::ShuffleStream;
                        type Future =
                            BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ShuffleRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).shuffle(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1;
                        let inner = inner.0;
                        let method = ShuffleSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(tonic::body::BoxBody::empty())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: Shuffler> Clone for ShufflerServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self { inner }
        }
    }
    impl<T: Shuffler> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone(), self.1.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Shuffler> tonic::transport::NamedService for ShufflerServer<T> {
        const NAME: &'static str = "flow.Shuffler";
    }
}
#[doc = r" Generated server implementations."]
pub mod ingester_server {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with IngesterServer."]
    #[async_trait]
    pub trait Ingester: Send + Sync + 'static {
        async fn ingest(
            &self,
            request: tonic::Request<super::IngestRequest>,
        ) -> Result<tonic::Response<super::IngestResponse>, tonic::Status>;
    }
    #[doc = " Ingester offers transactional ingest of documents into collections."]
    #[derive(Debug)]
    pub struct IngesterServer<T: Ingester> {
        inner: _Inner<T>,
    }
    struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
    impl<T: Ingester> IngesterServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, None);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, Some(interceptor.into()));
            Self { inner }
        }
    }
    impl<T, B> Service<http::Request<B>> for IngesterServer<T>
    where
        T: Ingester,
        B: HttpBody + Send + Sync + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/flow.Ingester/Ingest" => {
                    #[allow(non_camel_case_types)]
                    struct IngestSvc<T: Ingester>(pub Arc<T>);
                    impl<T: Ingester> tonic::server::UnaryService<super::IngestRequest> for IngestSvc<T> {
                        type Response = super::IngestResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::IngestRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).ingest(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = IngestSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(tonic::body::BoxBody::empty())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: Ingester> Clone for IngesterServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self { inner }
        }
    }
    impl<T: Ingester> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone(), self.1.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Ingester> tonic::transport::NamedService for IngesterServer<T> {
        const NAME: &'static str = "flow.Ingester";
    }
}
#[doc = r" Generated server implementations."]
pub mod testing_server {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with TestingServer."]
    #[async_trait]
    pub trait Testing: Send + Sync + 'static {
        async fn advance_time(
            &self,
            request: tonic::Request<super::AdvanceTimeRequest>,
        ) -> Result<tonic::Response<super::AdvanceTimeResponse>, tonic::Status>;
        async fn clear_registers(
            &self,
            request: tonic::Request<super::ClearRegistersRequest>,
        ) -> Result<tonic::Response<super::ClearRegistersResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct TestingServer<T: Testing> {
        inner: _Inner<T>,
    }
    struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
    impl<T: Testing> TestingServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, None);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, Some(interceptor.into()));
            Self { inner }
        }
    }
    impl<T, B> Service<http::Request<B>> for TestingServer<T>
    where
        T: Testing,
        B: HttpBody + Send + Sync + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/flow.Testing/AdvanceTime" => {
                    #[allow(non_camel_case_types)]
                    struct AdvanceTimeSvc<T: Testing>(pub Arc<T>);
                    impl<T: Testing> tonic::server::UnaryService<super::AdvanceTimeRequest> for AdvanceTimeSvc<T> {
                        type Response = super::AdvanceTimeResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AdvanceTimeRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).advance_time(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = AdvanceTimeSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/flow.Testing/ClearRegisters" => {
                    #[allow(non_camel_case_types)]
                    struct ClearRegistersSvc<T: Testing>(pub Arc<T>);
                    impl<T: Testing> tonic::server::UnaryService<super::ClearRegistersRequest>
                        for ClearRegistersSvc<T>
                    {
                        type Response = super::ClearRegistersResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ClearRegistersRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).clear_registers(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = ClearRegistersSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(tonic::body::BoxBody::empty())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: Testing> Clone for TestingServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self { inner }
        }
    }
    impl<T: Testing> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone(), self.1.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Testing> tonic::transport::NamedService for TestingServer<T> {
        const NAME: &'static str = "flow.Testing";
    }
}
