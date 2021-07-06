/// Slice represents a contiguous slice of bytes within an associated Arena.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Slice {
    #[prost(uint32, tag="1")]
    pub begin: u32,
    #[prost(uint32, tag="2")]
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
    #[prost(fixed64, tag="1")]
    pub producer_and_flags: u64,
    /// Clock is a v1 UUID 60-bit timestamp (60 MSBs), followed by 4 bits of
    /// sequence counter.
    #[prost(fixed64, tag="2")]
    pub clock: u64,
}
/// LambdaSpec describes a Flow transformation lambda and how to invoke it.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LambdaSpec {
    /// If non-empty, this is a TypeScript lambda and the field is its invocation
    /// path. E.x. 'some/derivation/andTransform/Update'.
    #[prost(string, tag="1")]
    pub typescript: ::prost::alloc::string::String,
    /// If non-empty, this is a remote lambda and the field is its invocation URL.
    /// E.x. 'https://my/external/api'.
    #[prost(string, tag="2")]
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
    #[prost(string, tag="1")]
    pub group_name: ::prost::alloc::string::String,
    /// Source collection read by this transform.
    #[prost(string, tag="2")]
    pub source_collection: ::prost::alloc::string::String,
    /// Selector of partitions of the collection which this transform reads.
    #[prost(message, optional, tag="3")]
    pub source_partitions: ::core::option::Option<super::protocol::LabelSelector>,
    /// JSON pointer locating the UUID of each source document.
    #[prost(string, tag="4")]
    pub source_uuid_ptr: ::prost::alloc::string::String,
    /// Composite key over which shuffling occurs, specified as one or more
    /// JSON-Pointers indicating a message location to extract.
    #[prost(string, repeated, tag="5")]
    pub shuffle_key_ptr: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// uses_source_key is true if shuffle_key_ptr is the source's native key,
    /// and false if it's some other key. When shuffling using the source's key,
    /// we can minimize data movement by assigning a shard coordinator for each
    /// journal such that the shard's key range overlap that of the journal.
    #[prost(bool, tag="6")]
    pub uses_source_key: bool,
    /// Computed shuffle lambda. If non-nil, then shuffle_key_ptr MUST be empty
    /// and uses_source_key MUST be false.
    #[prost(message, optional, tag="7")]
    pub shuffle_lambda: ::core::option::Option<LambdaSpec>,
    /// Schema against which shuffled documents are to be validated.
    #[prost(string, tag="8")]
    pub source_schema_uri: ::prost::alloc::string::String,
    /// uses_source_schema is true iff source_schema_uri is the source collection's
    /// schema, and false if it's a source schema specific to this transform.
    #[prost(bool, tag="9")]
    pub uses_source_schema: bool,
    /// Validate the schema of documents at time of shuffled read.
    /// We always validate documents, but there's a choice whether we validate
    /// within the shuffle server (true) or later within the shuffle client
    /// (false).
    /// - Derivations: true, as the derivation runtime can then by-pass
    ///   a round of JSON parsing and validation.
    /// - Materializations: false, as the materialization runtime immediately
    ///   combines over the document --  which requires parsing & validation
    ///   anyway.
    #[prost(bool, tag="10")]
    pub validate_schema_at_read: bool,
    /// filter_r_clocks is true if the shuffle coordinator should filter documents
    /// sent to each subscriber based on its covered r-clock ranges and the
    /// individual document clocks. If false, the subscriber's r-clock range is
    /// ignored and all documents which match the key range are sent.
    ///
    /// filter_r_clocks is set 'true' when reading on behalf of transforms having
    /// a "publish" but not an "update" lambda, as such documents have no
    /// side-effects on the reader's state store, and would not be published anyway
    /// for falling outside of the reader's r-clock range.
    #[prost(bool, tag="11")]
    pub filter_r_clocks: bool,
    /// Number of seconds for which documents of this collection are delayed
    /// while reading, relative to other documents (when back-filling) and the
    /// present wall-clock time (when tailing).
    #[prost(uint32, tag="12")]
    pub read_delay_seconds: u32,
    /// Priority of this shuffle, with respect to other related Shuffle reads
    /// (e.x. Shuffles of a different transformation within the same derivation).
    /// Higher values imply higher priority.
    #[prost(uint32, tag="13")]
    pub priority: u32,
}
/// JournalShuffle is a Shuffle of a Journal by a Coordinator shard.
/// They're compared using deep equality in order to consolidate groups of
/// related logical reads into a single physical read of the journal.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct JournalShuffle {
    /// Journal to be shuffled.
    #[prost(string, tag="1")]
    pub journal: ::prost::alloc::string::String,
    /// Coordinator is the Shard ID which is responsible for reads of this journal.
    #[prost(string, tag="2")]
    pub coordinator: ::prost::alloc::string::String,
    /// Shuffle of this JournalShuffle.
    #[prost(message, optional, tag="3")]
    pub shuffle: ::core::option::Option<Shuffle>,
    /// Is this a reply of the journal's content? We separate ongoing vs replayed
    /// reads of a journal's content into distinct rings.
    #[prost(bool, tag="4")]
    pub replay: bool,
    /// Catalog commons for resolution of catalog resources like schema URIs.
    #[prost(string, tag="5")]
    pub commons_id: ::prost::alloc::string::String,
    /// Etcd modfication revision of the |commons_id| CatalogCommons. As a
    /// CatalogCommons is write-once, this is also its creation revision.
    #[prost(int64, tag="6")]
    pub commons_revision: i64,
}
/// Projection is a mapping between a document location, specified as a
/// JSON-Pointer, and a corresponding field string in a flattened
/// (i.e. tabular or SQL) namespace which aliases it.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Projection {
    /// Document location of this projection, as a JSON-Pointer.
    #[prost(string, tag="1")]
    pub ptr: ::prost::alloc::string::String,
    /// Field is the flattened, tabular alias of this projection.
    #[prost(string, tag="2")]
    pub field: ::prost::alloc::string::String,
    /// Was this projection user provided ?
    #[prost(bool, tag="3")]
    pub user_provided: bool,
    /// Does this projection constitute a logical partitioning of the collection?
    #[prost(bool, tag="4")]
    pub is_partition_key: bool,
    /// Does this location form (part of) the collection key?
    #[prost(bool, tag="5")]
    pub is_primary_key: bool,
    /// Inference of this projection.
    #[prost(message, optional, tag="6")]
    pub inference: ::core::option::Option<Inference>,
}
/// Inference details type information which is statically known
/// about a given document location.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Inference {
    /// The possible types for this location.
    /// Subset of ["null", "boolean", "object", "array", "integer", "numeric",
    /// "string"].
    #[prost(string, repeated, tag="1")]
    pub types: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Whether the projection must always exist (either as a location within)
    /// the source document, or as a null-able column in the database.
    #[prost(bool, tag="2")]
    pub must_exist: bool,
    #[prost(message, optional, tag="3")]
    pub string: ::core::option::Option<inference::String>,
    /// The title from the schema, if provided
    #[prost(string, tag="4")]
    pub title: ::prost::alloc::string::String,
    /// The description from the schema, if provided
    #[prost(string, tag="5")]
    pub description: ::prost::alloc::string::String,
}
/// Nested message and enum types in `Inference`.
pub mod inference {
    /// String type-specific inferences, or nil iff types doesn't include "string".
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct String {
        /// Annotated Content-Type when the projection is of "string" type.
        #[prost(string, tag="3")]
        pub content_type: ::prost::alloc::string::String,
        /// Annotated format when the projection is of "string" type.
        #[prost(string, tag="4")]
        pub format: ::prost::alloc::string::String,
        /// Whether the value is base64-encoded when the projection is of "string"
        /// type.
        #[prost(bool, tag="5")]
        pub is_base64: bool,
        /// Maximum length when the projection is of "string" type. Zero for no
        /// limit.
        #[prost(uint32, tag="6")]
        pub max_length: u32,
    }
}
/// Next tag: 9.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CollectionSpec {
    /// Name of this collection.
    #[prost(string, tag="1")]
    pub collection: ::prost::alloc::string::String,
    /// Schema against which collection documents are validated,
    /// and which provides reduction annotations.
    #[prost(string, tag="2")]
    pub schema_uri: ::prost::alloc::string::String,
    /// Schema document of the collection, in a bundled and stand-alone form.
    /// All external references within the document have been bundled as
    /// included internal definitions.
    #[prost(string, tag="8")]
    pub schema_json: ::prost::alloc::string::String,
    /// Composite key of the collection, as JSON-Pointers.
    #[prost(string, repeated, tag="3")]
    pub key_ptrs: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// JSON pointer locating the UUID of each collection document.
    #[prost(string, tag="4")]
    pub uuid_ptr: ::prost::alloc::string::String,
    /// Logical partition fields of this collection.
    #[prost(string, repeated, tag="5")]
    pub partition_fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Logical projections of this collection
    #[prost(message, repeated, tag="6")]
    pub projections: ::prost::alloc::vec::Vec<Projection>,
    /// JSON-encoded document template for creating Gazette consumer
    /// transaction acknowledgements of writes into this collection.
    #[prost(string, tag="7")]
    pub ack_json_template: ::prost::alloc::string::String,
}
/// TransformSpec describes a specific transform of a derivation.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransformSpec {
    /// Derivation this transform belongs to.
    #[prost(string, tag="1")]
    pub derivation: ::prost::alloc::string::String,
    /// Name of this transform, scoped to it's derivation.
    #[prost(string, tag="2")]
    pub transform: ::prost::alloc::string::String,
    /// Shuffle applied to source documents for this transform.
    #[prost(message, optional, tag="3")]
    pub shuffle: ::core::option::Option<Shuffle>,
    /// Update lambda of this transform, if any.
    #[prost(message, optional, tag="4")]
    pub update_lambda: ::core::option::Option<LambdaSpec>,
    /// Publish lambda of this transform, if any.
    #[prost(message, optional, tag="5")]
    pub publish_lambda: ::core::option::Option<LambdaSpec>,
}
/// DerivationSpec describes a collection, and it's means of derivation.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DerivationSpec {
    /// Derivations are collections.
    #[prost(message, optional, tag="1")]
    pub collection: ::core::option::Option<CollectionSpec>,
    /// Schema against which derivation registers are validated,
    /// and which provides reduction annotations.
    #[prost(string, tag="2")]
    pub register_schema_uri: ::prost::alloc::string::String,
    /// JSON-encoded initial value of novel document registers.
    #[prost(string, tag="3")]
    pub register_initial_json: ::prost::alloc::string::String,
    /// Transforms of this derivation.
    #[prost(message, repeated, tag="4")]
    pub transforms: ::prost::alloc::vec::Vec<TransformSpec>,
}
/// FieldSelection is a selection of a collection's projection fields.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FieldSelection {
    /// Fields for each key component of the collection. Included key fields appear
    /// in the collection's key component order, and a given key pointer will be
    /// included at most once.
    #[prost(string, repeated, tag="1")]
    pub keys: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// All other selected fields, other than those in keys and the document field.
    /// Entries are in ascending sorted order, and may be empty.
    #[prost(string, repeated, tag="2")]
    pub values: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Field having a document pointer located at the document root.
    #[prost(string, tag="3")]
    pub document: ::prost::alloc::string::String,
    /// Additional configuration, keyed by fields included in |keys|, |values|, or
    /// |document|. Values are arbitrary JSON-encoded objects.
    #[prost(map="string, string", tag="4")]
    pub field_config_json: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
}
/// CaptureSpec describes a collection and its capture from an endpoint.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CaptureSpec {
    /// Name of this capture.
    #[prost(string, tag="1")]
    pub capture: ::prost::alloc::string::String,
    /// Type of the captures's endpoint.
    #[prost(enumeration="EndpointType", tag="2")]
    pub endpoint_type: i32,
    /// JSON-encoded object which specifies this capture with
    /// respect to the endpoint type driver.
    #[prost(string, tag="3")]
    pub endpoint_spec_json: ::prost::alloc::string::String,
    #[prost(message, repeated, tag="4")]
    pub bindings: ::prost::alloc::vec::Vec<capture_spec::Binding>,
    /// Minimum interval of time between successive invocations of the capture.
    #[prost(uint32, tag="5")]
    pub interval_seconds: u32,
}
/// Nested message and enum types in `CaptureSpec`.
pub mod capture_spec {
    /// Bindings of endpoint resources and collections into which they're captured.
    /// Bindings are ordered and unique on the bound collection name.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Binding {
        /// JSON-encoded object which specifies the endpoint resource to be captured.
        #[prost(string, tag="1")]
        pub resource_spec_json: ::prost::alloc::string::String,
        /// Driver-supplied path components which fully qualify the
        /// subresource being materialized.
        #[prost(string, repeated, tag="2")]
        pub resource_path: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
        /// Collection to be captured into.
        #[prost(message, optional, tag="3")]
        pub collection: ::core::option::Option<super::CollectionSpec>,
    }
}
/// MaterializationSpec describes a collection and its materialization to an
/// endpoint.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MaterializationSpec {
    /// Name of this materialization.
    #[prost(string, tag="1")]
    pub materialization: ::prost::alloc::string::String,
    /// Type of the materialization's endpoint.
    #[prost(enumeration="EndpointType", tag="2")]
    pub endpoint_type: i32,
    /// JSON-encoded object which specifies this materialization with
    /// respect to the endpoint type driver.
    #[prost(string, tag="3")]
    pub endpoint_spec_json: ::prost::alloc::string::String,
    #[prost(message, repeated, tag="4")]
    pub bindings: ::prost::alloc::vec::Vec<materialization_spec::Binding>,
}
/// Nested message and enum types in `MaterializationSpec`.
pub mod materialization_spec {
    /// Bindings of endpoint resources and collections from which they're materialized.
    /// Bindings are ordered and unique on the bound collection name.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Binding {
        /// JSON-encoded object which specifies the endpoint resource to be materialized.
        #[prost(string, tag="1")]
        pub resource_spec_json: ::prost::alloc::string::String,
        /// Driver-supplied path components which fully qualify the
        /// subresource being materialized.
        #[prost(string, repeated, tag="2")]
        pub resource_path: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
        /// Collection to be materialized.
        #[prost(message, optional, tag="3")]
        pub collection: ::core::option::Option<super::CollectionSpec>,
        /// Resolved fields selected for materialization.
        #[prost(message, optional, tag="4")]
        pub field_selection: ::core::option::Option<super::FieldSelection>,
        /// Materialize delta updates of documents rather than full reductions.
        #[prost(bool, tag="5")]
        pub delta_updates: bool,
        /// Shuffle applied to collection documents for this materialization binding.
        #[prost(message, optional, tag="6")]
        pub shuffle: ::core::option::Option<super::Shuffle>,
    }
}
/// TestSpec describes a catalog test.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TestSpec {
    /// Name of this test.
    #[prost(string, tag="1")]
    pub test: ::prost::alloc::string::String,
    #[prost(message, repeated, tag="2")]
    pub steps: ::prost::alloc::vec::Vec<test_spec::Step>,
}
/// Nested message and enum types in `TestSpec`.
pub mod test_spec {
    /// Steps of the test.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Step {
        #[prost(enumeration="step::Type", tag="1")]
        pub step_type: i32,
        /// Index of this step within the test.
        #[prost(uint32, tag="2")]
        pub step_index: u32,
        /// Scope of the test definition location.
        #[prost(string, tag="3")]
        pub step_scope: ::prost::alloc::string::String,
        /// Collection ingested or verified by this step.
        #[prost(string, tag="4")]
        pub collection: ::prost::alloc::string::String,
        /// Schema of this collection.
        #[prost(string, tag="5")]
        pub collection_schema_uri: ::prost::alloc::string::String,
        /// Grouped key pointers of the collection.
        #[prost(string, repeated, tag="6")]
        pub collection_key_ptr: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
        /// JSON pointer locating the UUID of each collection document.
        #[prost(string, tag="7")]
        pub collection_uuid_ptr: ::prost::alloc::string::String,
        /// Newline-separated JSON documents to ingest.
        #[prost(string, tag="8")]
        pub docs_json_lines: ::prost::alloc::string::String,
        /// When verifying, selector over logical partitions of the collection.
        #[prost(message, optional, tag="9")]
        pub partitions: ::core::option::Option<super::super::protocol::LabelSelector>,
    }
    /// Nested message and enum types in `Step`.
    pub mod step {
        /// Type of this step.
        #[derive(serde::Deserialize, serde::Serialize)] #[serde(deny_unknown_fields)]
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
        #[repr(i32)]
        pub enum Type {
            Ingest = 0,
            Verify = 1,
        }
    }
}
/// RangeSpec describes the ranges of shuffle keys and r-clocks which a reader
/// is responsible for.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RangeSpec {
    /// [begin, end] inclusive range of keys to be shuffled to this reader.
    /// Ranges are with respect to a 32-bit hash of a packed document key.
    ///
    /// The choice of hash function is important: while it need not be
    /// cryptographically secure, it must demonstrate a strong "avalanche effect"
    /// (ideally meeting the strict avalanche criterion), to ensure that small
    /// pertubations of input keys are equally likely to independently affect
    /// hash output bits. Particularly the higest bits of the hash result,
    /// which dominate the selection of a shuffled shard.
    ///
    /// At present, Flow uses the high 32 bits of a HighWayHash 64-bit
    /// checksum, using a fixed 32-byte key.
    #[prost(fixed32, tag="2")]
    pub key_begin: u32,
    #[prost(fixed32, tag="3")]
    pub key_end: u32,
    /// Rotated [begin, end] inclusive ranges of Clocks.
    #[prost(fixed32, tag="4")]
    pub r_clock_begin: u32,
    #[prost(fixed32, tag="5")]
    pub r_clock_end: u32,
}
/// JournalRules are an ordered sequence of Rules which specify a
/// condition -- as a label selector -- and, if matched, a template
/// to apply to the base JournalSpec.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct JournalRules {
    #[prost(message, repeated, tag="1")]
    pub rules: ::prost::alloc::vec::Vec<journal_rules::Rule>,
}
/// Nested message and enum types in `JournalRules`.
pub mod journal_rules {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Rule {
        /// Name of the rule.
        #[prost(string, tag="1")]
        pub rule: ::prost::alloc::string::String,
        /// Label selector which must pass for the template to be applied.
        #[prost(message, optional, tag="2")]
        pub selector: ::core::option::Option<super::super::protocol::LabelSelector>,
        /// Template to union into the base JournalSpec.
        #[prost(message, optional, tag="3")]
        pub template: ::core::option::Option<super::super::protocol::JournalSpec>,
    }
}
/// ShardRules are an ordered sequence of Rules which specify a
/// condition -- as a label selector -- and, if matched, a template
/// to apply to the base ShardSpec.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardRules {
    #[prost(message, repeated, tag="1")]
    pub rules: ::prost::alloc::vec::Vec<shard_rules::Rule>,
}
/// Nested message and enum types in `ShardRules`.
pub mod shard_rules {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Rule {
        /// Name of the rule.
        #[prost(string, tag="1")]
        pub rule: ::prost::alloc::string::String,
        /// Label selector which must pass for the template to be applied.
        #[prost(message, optional, tag="2")]
        pub selector: ::core::option::Option<super::super::protocol::LabelSelector>,
        /// Template to union into the base ShardSpec.
        #[prost(message, optional, tag="3")]
        pub template: ::core::option::Option<super::super::consumer::ShardSpec>,
    }
}
/// SchemaBundle is a bundle of JSON schemas and their base URI.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SchemaBundle {
    /// Schemas of the bundle. Keys are the absolute URIs of the schema,
    /// and values are JSON-encoded schema documents.
    #[prost(map="string, string", tag="1")]
    pub bundle: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
}
/// ShuffleRequest is the request message of a Shuffle RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleRequest {
    /// Journal to be shuffled, routed to a coordinator.
    #[prost(message, optional, tag="1")]
    pub shuffle: ::core::option::Option<JournalShuffle>,
    /// Resolution header of the |shuffle.coordinator| shard.
    #[prost(message, optional, tag="2")]
    pub resolution: ::core::option::Option<super::protocol::Header>,
    /// Ranges of responsibility which are unique to this reader,
    /// against which document shuffle outcomes are matched to determine
    /// read eligibility.
    #[prost(message, optional, tag="3")]
    pub range: ::core::option::Option<RangeSpec>,
    /// Offset to begin reading the journal from.
    #[prost(int64, tag="4")]
    pub offset: i64,
    /// Offset to stop reading the journal at, or zero if unbounded.
    #[prost(int64, tag="5")]
    pub end_offset: i64,
}
/// ShuffleResponse is the streamed response message of a Shuffle RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleResponse {
    /// Status of the Shuffle RPC.
    #[prost(enumeration="super::consumer::Status", tag="1")]
    pub status: i32,
    /// Header of the response.
    #[prost(message, optional, tag="2")]
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
    #[prost(string, tag="3")]
    pub terminal_error: ::prost::alloc::string::String,
    /// Offset which was read through to produce this ShuffleResponse.
    #[prost(int64, tag="4")]
    pub read_through: i64,
    /// WriteHead of the journal as reported by the broker, as of the creation of
    /// this ShuffleResponse.
    #[prost(int64, tag="5")]
    pub write_head: i64,
    /// Memory arena of this message.
    #[prost(bytes="vec", tag="6")]
    pub arena: ::prost::alloc::vec::Vec<u8>,
    /// Shuffled documents, each encoded in the 'application/json'
    /// media-type.
    #[prost(message, repeated, tag="7")]
    pub docs_json: ::prost::alloc::vec::Vec<Slice>,
    /// The journal offsets of each document within the requested journal.
    /// For a document at index i, its offsets are [ offsets[2*i], offsets[2*i+1]
    /// ).
    #[prost(int64, repeated, packed="false", tag="8")]
    pub offsets: ::prost::alloc::vec::Vec<i64>,
    /// UUIDParts of each document.
    #[prost(message, repeated, tag="9")]
    pub uuid_parts: ::prost::alloc::vec::Vec<UuidParts>,
    /// Packed, embedded encoding of the shuffle key into a byte string.
    /// If the Shuffle specified a Hash to use, it's applied as well.
    #[prost(message, repeated, tag="10")]
    pub packed_key: ::prost::alloc::vec::Vec<Slice>,
}
/// SplitRequest is the request message of a Split RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SplitRequest {
    /// Shard to split.
    #[prost(string, tag="1")]
    pub shard: ::prost::alloc::string::String,
    /// Split on key.
    #[prost(bool, tag="2")]
    pub split_on_key: bool,
    /// Split on r-clock.
    #[prost(bool, tag="3")]
    pub split_on_rclock: bool,
}
/// SplitResponse is the response message of a Split RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SplitResponse {
    /// Status of the Shuffle RPC.
    #[prost(enumeration="super::consumer::Status", tag="1")]
    pub status: i32,
    /// Header of the response.
    #[prost(message, optional, tag="2")]
    pub header: ::core::option::Option<super::protocol::Header>,
    /// Original (parent) shard RangeSpec.
    #[prost(message, optional, tag="3")]
    pub parent_range: ::core::option::Option<RangeSpec>,
    /// Future left-hand child RangeSpec.
    #[prost(message, optional, tag="4")]
    pub lhs_range: ::core::option::Option<RangeSpec>,
    /// Future Right-hand child RangeSpec.
    #[prost(message, optional, tag="5")]
    pub rhs_range: ::core::option::Option<RangeSpec>,
}
/// CatalogTask is a self-contained, long lived specification executed
/// by the Flow runtime. Tasks have stable names which coexist in a shared
/// global namespace, with a specification that evolves over time.
///
/// A CatalogTask is associated with a CatalogCommons, which provides all
/// resources required by the current specification that may be shared
/// with other CatalogTasks.
///
/// Tags 1-10 are available for future use.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CatalogTask {
    /// Catalog commons used by this task.
    #[prost(string, tag="10")]
    pub commons_id: ::prost::alloc::string::String,
    /// A capture of a data source into a collection.
    /// These don't do anything quite yet.
    #[prost(message, optional, tag="11")]
    pub capture: ::core::option::Option<CaptureSpec>,
    /// An ingested collection.
    #[prost(message, optional, tag="12")]
    pub ingestion: ::core::option::Option<CollectionSpec>,
    /// A derived collection.
    #[prost(message, optional, tag="13")]
    pub derivation: ::core::option::Option<DerivationSpec>,
    /// A materialization of a collection.
    #[prost(message, optional, tag="14")]
    pub materialization: ::core::option::Option<MaterializationSpec>,
}
/// CatalogCommons describes a "commons" of shared resources utilized by multiple
/// CatalogTasks. It's indexed and referenced on its |commons_id|, which is an
/// opaque and unique identifier. A commons is garbage-collected when it's
/// no longer referred to by any CatalogTasks.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CatalogCommons {
    /// ID of this commons.
    #[prost(string, tag="1")]
    pub commons_id: ::prost::alloc::string::String,
    // Tags 2-9 are available for future use.

    /// Journal rules applied to create and update JournalSpecs.
    #[prost(message, optional, tag="10")]
    pub journal_rules: ::core::option::Option<JournalRules>,
    /// Shard rules applied to create and update ShardSpecs.
    #[prost(message, optional, tag="11")]
    pub shard_rules: ::core::option::Option<ShardRules>,
    /// Schema definitions, against which registers and sourced or derived
    /// documents are validated.
    #[prost(message, optional, tag="12")]
    pub schemas: ::core::option::Option<SchemaBundle>,
    /// Unix domain socket on which a local TypeScript runtime is already
    /// listening. This is set by `flowctl test` and `flowctl develop`, and is
    /// empty otherwise.
    #[prost(string, tag="13")]
    pub typescript_local_socket: ::prost::alloc::string::String,
    /// TypeScript NPM package, as a stand-alone gzipped tarball with bundled
    /// dependencies. At present we expect only etcd:// schemes with no host, and
    /// map paths to fetched Etcd values. This is a handy short term representation
    /// that will evolve over time. Empty if |typescript_local_socket| is set.
    #[prost(string, tag="14")]
    pub typescript_package_url: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SchemaApi {
}
/// Nested message and enum types in `SchemaAPI`.
pub mod schema_api {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct BuiltIndex {
        #[prost(fixed64, tag="1")]
        pub schema_index_memptr: u64,
    }
    /// Code labels message codes passed over the CGO bridge.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Code {
        Invalid = 0,
        /// Take a request SchemaBundle and respond with a BuiltIndex. (Go <-> Rust).
        BuildIndex = 1,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExtractApi {
}
/// Nested message and enum types in `ExtractAPI`.
pub mod extract_api {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Config {
        /// JSON pointer of the document UUID to extract.
        #[prost(string, tag="1")]
        pub uuid_ptr: ::prost::alloc::string::String,
        /// URI of schema to validate non-ACK documents against.
        /// If empty, schema validation is not performed.
        #[prost(string, tag="2")]
        pub schema_uri: ::prost::alloc::string::String,
        /// Memory address of the accosiated SchemaIndex, which must exist for
        /// the remainder of this API's usage.
        #[prost(fixed64, tag="3")]
        pub schema_index_memptr: u64,
        /// Field JSON pointers to extract from documents and return as packed
        /// tuples.
        #[prost(string, repeated, tag="4")]
        pub field_ptrs: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    }
    /// Code labels message codes passed over the CGO bridge.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Code {
        Invalid = 0,
        /// Configure or re-configure the extractor (Go -> Rust).
        Configure = 1,
        /// Extract from a document (Go -> Rust).
        Extract = 2,
        /// UUID extracted from a document (Rust -> Go).
        ExtractedUuid = 3,
        /// Fields extracted from a document (Rust -> Go).
        ExtractedFields = 4,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CombineApi {
}
/// Nested message and enum types in `CombineAPI`.
pub mod combine_api {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Config {
        /// Memory address of a shared SchemaIndex, which must exist for
        /// the complete lifetime of this API's use.
        #[prost(fixed64, tag="1")]
        pub schema_index_memptr: u64,
        /// Schema against which documents are to be validated,
        /// and which provides reduction annotations.
        #[prost(string, tag="2")]
        pub schema_uri: ::prost::alloc::string::String,
        /// Composite key used to group documents to be combined, specified as one or
        /// more JSON-Pointers indicating a message location to extract.
        /// If empty, all request documents are combined into a single response
        /// document.
        #[prost(string, repeated, tag="3")]
        pub key_ptr: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
        /// Field JSON pointers to be extracted from combined documents and returned.
        /// If empty, no fields are extracted.
        #[prost(string, repeated, tag="4")]
        pub field_ptrs: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
        /// JSON-Pointer at which a placeholder UUID should be inserted into
        /// returned documents. If empty, no placeholder is inserted.
        #[prost(string, tag="5")]
        pub uuid_placeholder_ptr: ::prost::alloc::string::String,
    }
    /// Code labels message codes passed over the CGO bridge.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Code {
        Invalid = 0,
        /// Configure or re-configure the combiner (Go -> Rust).
        /// A combiner may be configured only on first initialization,
        /// or immediately after having drained.
        Configure = 1,
        /// Reduce a left-hand side document (Go -> Rust).
        ReduceLeft = 2,
        /// Combine a right-hand side document (Go -> Rust).
        CombineRight = 3,
        /// Drain the combiner (Go -> Rust).
        Drain = 4,
        /// Next drained document is partially combined (Rust -> Go).
        DrainedCombinedDocument = 5,
        /// Next drained document is fully reduced (Rust -> Go).
        DrainedReducedDocument = 6,
        /// Next drained key (follows drained document; Rust -> Go).
        DrainedKey = 7,
        /// Next drained fields (follows key; Rust -> Go).
        DrainedFields = 8,
    }
}
/// DeriveAPI is a meta-message which name spaces messages of the Derive API
/// bridge.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeriveApi {
}
/// Nested message and enum types in `DeriveAPI`.
pub mod derive_api {
    /// Open the registers database.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Open {
        /// Memory address of an RocksDB Environment to use (as a *rocksdb_env_t).
        /// Ownership of the environment is transferred with this message.
        #[prost(fixed64, tag="1")]
        pub rocksdb_env_memptr: u64,
        /// Local directory for ephemeral processing state.
        #[prost(string, tag="2")]
        pub local_dir: ::prost::alloc::string::String,
    }
    /// Config configures the derived DerivationSpec and its associated schema
    /// index.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Config {
        /// Derivation to derive.
        #[prost(message, optional, tag="1")]
        pub derivation: ::core::option::Option<super::DerivationSpec>,
        /// Memory address of a associated SchemaIndex, which must exist for
        /// the complete lifetime of this API's use.
        #[prost(fixed64, tag="2")]
        pub schema_index_memptr: u64,
    }
    /// DocHeader precedes a JSON-encoded document.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct DocHeader {
        /// UUID of this document.
        #[prost(message, optional, tag="1")]
        pub uuid: ::core::option::Option<super::UuidParts>,
        /// FDB packed shuffle key of the document.
        #[prost(bytes="vec", tag="2")]
        pub packed_key: ::prost::alloc::vec::Vec<u8>,
        /// Index of the transformation under which this document is being
        /// processed, within the configured DerivationSpec.
        #[prost(uint32, tag="3")]
        pub transform_index: u32,
    }
    /// Invoke a lambda, using Rust-owned memory buffers of invocation content.
    /// Memory will remain pinned until the trampoline task completion.
    /// |sources_length| will never be zero. If |registers_length| is zero,
    /// this invocation is of the update lambda. Otherwise, it's the publish
    /// lambda.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Invoke {
        /// Index of the transformation to be invoked within DerivationSpec.
        #[prost(uint32, tag="1")]
        pub transform_index: u32,
        /// Memory pointer and length of comma-separated source documents.
        #[prost(fixed64, tag="2")]
        pub sources_memptr: u64,
        #[prost(uint64, tag="3")]
        pub sources_length: u64,
        /// Memory pointer and length of comma-separated register documents.
        #[prost(fixed64, tag="4")]
        pub registers_memptr: u64,
        #[prost(uint64, tag="5")]
        pub registers_length: u64,
    }
    /// Prepare a commit of the transaction.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Prepare {
        /// Checkpoint to commit.
        #[prost(message, optional, tag="1")]
        pub checkpoint: ::core::option::Option<super::super::consumer::Checkpoint>,
    }
    /// Codes passed over the CGO bridge.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Code {
        Invalid = 0,
        /// Open the registers database (Go -> Rust).
        Open = 1,
        /// Restore the last checkpoint from an opened database (Go <-> Rust).
        RestoreCheckpoint = 2,
        /// Configure or re-configure the derive API (Go -> Rust).
        Configure = 3,
        /// Begin a new transaction (Go -> Rust).
        BeginTransaction = 4,
        /// Next drained document is partially combined (Rust -> Go).
        /// Must match CombineAPI.Code.
        DrainedCombinedDocument = 5,
        /// Next drained document is fully reduced (Rust -> Go).
        /// Must match CombineAPI.Code.
        DrainedReducedDocument = 6,
        /// Next drained key (follows drained document; Rust -> Go).
        /// Must match CombineAPI.Code.
        DrainedKey = 7,
        /// Next drained fields (follows key; Rust -> Go).
        /// Must match CombineAPI.Code.
        DrainedFields = 8,
        /// Next source document header (Go -> Rust).
        NextDocumentHeader = 9,
        /// Next source document body (Go -> Rust).
        NextDocumentBody = 10,
        /// Trampoline task start or completion (Rust <-> Go).
        Trampoline = 11,
        /// Trampoline sub-type: invoke transform lambda.
        TrampolineInvoke = 12,
        /// Flush transaction (Go -> Rust).
        FlushTransaction = 13,
        /// Prepare transaction to commit (Go -> Rust).
        PrepareToCommit = 14,
        /// Clear registers values (test support only; Go -> Rust).
        ClearRegisters = 15,
    }
}
/// BuildAPI is a meta-message which name spaces messages of the Build API
/// bridge.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BuildApi {
}
/// Nested message and enum types in `BuildAPI`.
pub mod build_api {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Config {
        /// Path to the base build directory.
        #[prost(string, tag="1")]
        pub directory: ::prost::alloc::string::String,
        /// Root catalog source specification. This may be either a local path
        /// relative to the current working directory, or an absolute URL.
        #[prost(string, tag="2")]
        pub source: ::prost::alloc::string::String,
        /// Content type of the source.
        #[prost(enumeration="super::ContentType", tag="3")]
        pub source_type: i32,
        /// Path of the catalog database to write.
        #[prost(string, tag="4")]
        pub catalog_path: ::prost::alloc::string::String,
        /// Optional supplemental journal rules to add, beyond those already in the
        /// catalog. This is used to add development & testing overrides.
        #[prost(message, optional, tag="5")]
        pub extra_journal_rules: ::core::option::Option<super::JournalRules>,
        /// Optional supplemental shard rules to add, beyond those already in the
        /// catalog. This is used to add development & testing overrides.
        #[prost(message, optional, tag="6")]
        pub extra_shard_rules: ::core::option::Option<super::ShardRules>,
        /// Should the TypeScript package be generated?
        #[prost(bool, tag="7")]
        pub typescript_generate: bool,
        /// Should the TypeScript package be built? Implies generation.
        #[prost(bool, tag="8")]
        pub typescript_compile: bool,
        /// Should the TypeScript package be packaged into the catalog?
        /// Implies generation and compilation.
        #[prost(bool, tag="9")]
        pub typescript_package: bool,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Fetch {
        #[prost(string, tag="1")]
        pub resource_url: ::prost::alloc::string::String,
        #[prost(enumeration="super::ContentType", tag="2")]
        pub content_type: i32,
    }
    /// Code labels message codes passed over the CGO bridge.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Code {
        /// Begin a build with a Config (Go -> Rust).
        Begin = 0,
        /// Poll the build after completing one or more trampoline tasks (Go ->
        /// Rust).
        Poll = 1,
        /// Trampoline task start or completion (Rust <-> Go).
        Trampoline = 2,
        /// Trampoline sub-type: Start fetch of a resource.
        TrampolineFetch = 3,
        /// Trampoline sub-type: Start validation of a capture.
        TrampolineValidateCapture = 4,
        /// Trampoline sub-type: Start validation of a materialization.
        TrampolineValidateMaterialization = 5,
        /// Build completed successfully (Rust -> Go).
        Done = 6,
        /// Build completed with errors (Rust -> Go).
        DoneWithErrors = 7,
        /// Generate catalog specification JSON schema (Go <-> Rust)
        CatalogSchema = 100,
    }
}
/// IngestRequest describes documents to ingest into collections.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IngestRequest {
    #[prost(message, repeated, tag="1")]
    pub collections: ::prost::alloc::vec::Vec<ingest_request::Collection>,
}
/// Nested message and enum types in `IngestRequest`.
pub mod ingest_request {
    /// Collection describes an ingest into a collection.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Collection {
        /// Name of the collection into which to ingest.
        #[prost(string, tag="1")]
        pub name: ::prost::alloc::string::String,
        /// Newline-separated JSON documents to ingest.
        /// TODO(johnny): this must be UTF-8, and can be "string" type.
        #[prost(bytes="vec", tag="2")]
        pub docs_json_lines: ::prost::alloc::vec::Vec<u8>,
    }
}
/// IngestResponse is the result of an Ingest RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IngestResponse {
    /// Journals appended to by this ingestion, and their maximum offset on commit.
    #[prost(map="string, int64", tag="1")]
    pub journal_write_heads: ::std::collections::HashMap<::prost::alloc::string::String, i64>,
    /// Etcd header which describes current journal partitions.
    #[prost(message, optional, tag="2")]
    pub journal_etcd: ::core::option::Option<super::protocol::header::Etcd>,
}
/// EndpointType enumerates the endpoint types understood by Flow.
#[derive(serde::Deserialize, serde::Serialize)] #[serde(deny_unknown_fields)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum EndpointType {
    /// Remote is an arbitrary gRPC materialization protocol server.
    Remote = 0,
    Postgresql = 1,
    Sqlite = 2,
    S3 = 3,
    Gs = 4,
    Snowflake = 5,
    Webhook = 6,
    AirbyteSource = 7,
    FlowSink = 8,
}
/// ContentType enumerates the content types understood by Flow.
#[derive(serde::Deserialize, serde::Serialize)] #[serde(deny_unknown_fields)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ContentType {
    CatalogSpec = 0,
    JsonSchema = 1,
    TypescriptModule = 2,
    NpmPackage = 3,
}
