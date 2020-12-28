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
/// Shuffle is a description of a document shuffle, where each document
/// is mapped into:
///  * An extracted, packed composite key (a "shuffle key").
///  * A rotated Clock value (an "r-clock").
/// The packed key and r-clock can then be compared to a reader RangeSpec.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Shuffle {
    /// Composite key over which shuffling occurs, specified as one or more
    /// JSON-Pointers indicating a message location to extract.
    #[prost(string, repeated, tag = "1")]
    pub shuffle_key_ptr: ::std::vec::Vec<std::string::String>,
    /// uses_source_key is true if shuffle_key_ptr is the source's native key,
    /// and false if it's some other key. When shuffling using the source's key,
    /// we can minimize data movement by assigning a shard coordinator for each
    /// journal such that the shard's key range overlap that of the journal.
    #[prost(bool, tag = "2")]
    pub uses_source_key: bool,
    /// filter_r_clocks is true if the shuffle coordinator should filter documents
    /// sent to each subscriber based on its covered r-clock ranges and the
    /// individual document clocks. If false, the subscriber's r-clock range is
    /// ignored and all documents which match the key range are sent.
    ///
    /// filter_r_clocks is set 'true' when reading on behalf of transforms having
    /// a "publish" but not an "update" lambda, as such documents have no
    /// side-effects on the reader's state store, and would not be published anyway
    /// for falling outside of the reader's r-clock range.
    #[prost(bool, tag = "3")]
    pub filter_r_clocks: bool,
    #[prost(enumeration = "shuffle::Hash", tag = "4")]
    pub hash: i32,
    /// Number of seconds for which documents of this collection are delayed
    /// while reading, relative to other documents (when back-filling) and the
    /// present wall-clock time (when tailing).
    #[prost(uint32, tag = "5")]
    pub read_delay_seconds: u32,
}
pub mod shuffle {
    /// Optional hash applied to extracted, packed shuffle keys. Hashes can:
    /// * Mitigate shard skew which might otherwise occur due to key locality
    ///   (many co-occurring updates to "nearby" keys).
    /// * Give predictable storage sizes for keys which are otherwise unbounded.
    /// * Allow for joins over sensitive fields, which should not be stored
    ///   in-the-clear at rest where possible.
    /// Either cryptographic or non-cryptographic functions may be appropriate
    /// depending on thse use case.
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
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct JournalShuffle {
    /// Journal to be shuffled.
    #[prost(string, tag = "1")]
    pub journal: std::string::String,
    /// Coordinator is the Shard ID which is responsible for reads of this journal.
    #[prost(string, tag = "2")]
    pub coordinator: std::string::String,
    /// Shuffle of this JournalShuffle.
    #[prost(message, optional, tag = "3")]
    pub shuffle: ::std::option::Option<Shuffle>,
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
#[derive(Clone, PartialEq, ::prost::Message, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Projection {
    /// Document location of this projection, as a JSON-Pointer.
    #[prost(string, tag = "1")]
    pub ptr: std::string::String,
    /// Field is the flattened, tabular alias of this projection.
    #[prost(string, tag = "2")]
    pub field: std::string::String,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inference: ::std::option::Option<Inference>,
}
/// Inference details type information which is statically known
/// about a given document location.
#[derive(Clone, PartialEq, ::prost::Message, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Inference {
    /// The possible types for this location.
    /// Subset of ["null", "boolean", "object", "array", "integer", "numeric", "string"].
    #[prost(string, repeated, tag = "1")]
    pub types: ::std::vec::Vec<std::string::String>,
    /// Whether the projection must always exist (either as a location within)
    /// the source document, or as a null-able column in the database.
    #[prost(bool, tag = "2")]
    pub must_exist: bool,
    #[prost(message, optional, tag = "3")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub string: ::std::option::Option<inference::String>,
    /// The title from the schema, if provided
    #[prost(string, tag = "4")]
    #[serde(default, skip_serializing_if = "str::is_empty")]
    #[doc("This field is optional. An empty String denotes a missing value.")]
    pub title: std::string::String,
    /// The description from the schema, if provided
    #[prost(string, tag = "5")]
    #[serde(default, skip_serializing_if = "str::is_empty")]
    #[doc("This field is optional. An empty String denotes a missing value.")]
    pub description: std::string::String,
}
pub mod inference {
    /// String type-specific inferences.
    #[derive(Clone, PartialEq, ::prost::Message, serde::Deserialize, serde::Serialize)]
    #[serde(deny_unknown_fields)]
    pub struct String {
        /// Annotated Content-Type when the projection is of "string" type.
        #[prost(string, tag = "3")]
        #[serde(default, skip_serializing_if = "str::is_empty")]
        #[doc("This field is optional. An empty String denotes a missing value.")]
        pub content_type: std::string::String,
        /// Annotated format when the projection is of "string" type.
        #[prost(string, tag = "4")]
        #[serde(default, skip_serializing_if = "str::is_empty")]
        #[doc("This field is optional. An empty String denotes a missing value.")]
        pub format: std::string::String,
        /// Whether the value is base64-encoded when the projection is of "string" type.
        #[prost(bool, tag = "5")]
        pub is_base64: bool,
        /// Maximum length when the projection is of "string" type. Zero for no limit.
        #[prost(uint32, tag = "6")]
        #[serde(default, skip_serializing_if = "crate::u32_is_0")]
        #[doc("This field is optional. A value of 0 represents a missing value.")]
        pub max_length: u32,
    }
}
#[derive(Clone, PartialEq, ::prost::Message, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct CollectionSpec {
    /// Name of this collection.
    #[prost(string, tag = "1")]
    pub name: std::string::String,
    /// Schema against which collection documents are validated,
    /// and which provides reduction annotations.
    #[prost(string, tag = "2")]
    pub schema_uri: std::string::String,
    /// Composite key of the collection, as JSON-Pointers.
    #[prost(string, repeated, tag = "3")]
    pub key_ptrs: ::std::vec::Vec<std::string::String>,
    /// JSON pointer locating the UUID of each collection document.
    #[prost(string, tag = "4")]
    #[serde(default, skip_serializing_if = "str::is_empty")]
    #[doc("This field is optional. An empty String denotes a missing value.")]
    pub uuid_ptr: std::string::String,
    /// Logical partition fields of this collection.
    #[prost(string, repeated, tag = "5")]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[doc("This field is optional. An empty Vec represents a missing value.")]
    pub partition_fields: ::std::vec::Vec<std::string::String>,
    /// Logical projections of this collection
    #[prost(message, repeated, tag = "6")]
    pub projections: ::std::vec::Vec<Projection>,
    /// JournalSpec used for dynamically-created journals of this collection.
    #[prost(message, optional, tag = "7")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub journal_spec: ::std::option::Option<super::protocol::JournalSpec>,
    /// JSON-encoded document template for creating Gazette consumer
    /// transaction acknowledgements of writes into this collection.
    #[prost(bytes, tag = "8")]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[doc("This field is optional. An empty Vec represents a missing value.")]
    pub ack_json_template: std::vec::Vec<u8>,
}
/// Transform describes a specific transform of a derived collection.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransformSpec {
    /// Stable name of this transform, scoped to it's Derivation.
    #[prost(string, tag = "1")]
    pub name: std::string::String,
    /// ID of this transform within the present catalog DB.
    /// This ID is *not* stable /across/ different catalog DBs.
    /// Instead, use |name| for equality testing.
    #[prost(int32, tag = "2")]
    pub catalog_db_id: i32,
    #[prost(message, optional, tag = "3")]
    pub source: ::std::option::Option<transform_spec::Source>,
    /// Shuffle applied to source documents for this transform.
    /// Note that the Shuffle embeds the Transform name.
    #[prost(message, optional, tag = "4")]
    pub shuffle: ::std::option::Option<Shuffle>,
    #[prost(message, optional, tag = "5")]
    pub derivation: ::std::option::Option<transform_spec::Derivation>,
}
pub mod transform_spec {
    /// Source collection read by this transform.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Source {
        /// Name of the collection.
        #[prost(string, tag = "1")]
        pub name: std::string::String,
        /// Selector of partitions of the collection which this transform reads.
        #[prost(message, optional, tag = "2")]
        pub partitions: ::std::option::Option<super::super::protocol::LabelSelector>,
    }
    /// Derived collection produced by this transform.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Derivation {
        #[prost(string, tag = "1")]
        pub name: std::string::String,
    }
}
/// RangeSpec describes the ranges of shuffle keys and r-clocks which a reader
/// is responsible for.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RangeSpec {
    /// Byte [begin, end) exclusive range of keys to be shuffled to this reader.
    #[prost(bytes, tag = "2")]
    pub key_begin: std::vec::Vec<u8>,
    #[prost(bytes, tag = "3")]
    pub key_end: std::vec::Vec<u8>,
    /// Rotated [begin, end) exclusive ranges of Clocks to be shuffled to this
    /// reader.
    #[prost(uint64, tag = "4")]
    pub r_clock_begin: u64,
    #[prost(uint64, tag = "5")]
    pub r_clock_end: u64,
}
/// ShuffleRequest is the request message of a Shuffle RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleRequest {
    #[prost(message, optional, tag = "1")]
    pub shuffle: ::std::option::Option<JournalShuffle>,
    #[prost(message, optional, tag = "2")]
    pub range: ::std::option::Option<RangeSpec>,
    /// Offset to begin reading the journal from.
    #[prost(int64, tag = "3")]
    pub offset: i64,
    /// Offset to stop reading the journal at, or zero if unbounded.
    #[prost(int64, tag = "4")]
    pub end_offset: i64,
    /// Resolution header of the |config.coordinator_index| shard.
    #[prost(message, optional, tag = "5")]
    pub resolution: ::std::option::Option<super::protocol::Header>,
}
/// ShuffleResponse is the streamed response message of a Shuffle RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleResponse {
    /// Status of the Shuffle RPC.
    #[prost(enumeration = "super::consumer::Status", tag = "1")]
    pub status: i32,
    /// Header of the response.
    #[prost(message, optional, tag = "2")]
    pub header: ::std::option::Option<super::protocol::Header>,
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
    pub terminal_error: std::string::String,
    /// Offset which was read through to produce this ShuffleResponse.
    #[prost(int64, tag = "4")]
    pub read_through: i64,
    /// WriteHead of the journal as reported by the broker, as of the creation of
    /// this ShuffleResponse.
    #[prost(int64, tag = "5")]
    pub write_head: i64,
    /// Memory arena of this message.
    #[prost(bytes, tag = "6")]
    pub arena: std::vec::Vec<u8>,
    /// Shuffled documents, each encoded in the 'application/json'
    /// media-type.
    #[prost(message, repeated, tag = "7")]
    pub docs_json: ::std::vec::Vec<Slice>,
    /// The begin offset of each document within the requested journal.
    #[prost(int64, repeated, packed = "false", tag = "8")]
    pub begin: ::std::vec::Vec<i64>,
    /// The end offset of each document within the journal.
    #[prost(int64, repeated, packed = "false", tag = "9")]
    pub end: ::std::vec::Vec<i64>,
    /// UUIDParts of each document.
    #[prost(message, repeated, tag = "10")]
    pub uuid_parts: ::std::vec::Vec<UuidParts>,
    /// Packed, embedded encoding of the shuffle key into a byte string.
    /// If the Shuffle specified a Hash to use, it's applied as well.
    #[prost(message, repeated, tag = "11")]
    pub packed_key: ::std::vec::Vec<Slice>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExtractApi {}
pub mod extract_api {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Config {
        /// JSON pointer of the document UUID to extract.
        /// If empty, UUIDParts are not extracted.
        #[prost(string, tag = "1")]
        pub uuid_ptr: std::string::String,
        /// Field JSON pointers to extract from documents and return.
        /// If empty, no fields are extracted.
        #[prost(string, repeated, tag = "2")]
        pub field_ptrs: ::std::vec::Vec<std::string::String>,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CombineApi {}
pub mod combine_api {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Config {
        /// Memory address of a 'static SchemaIndex to use for combining.
        /// Applicable only to the CGO bridged CombinerService.
        #[prost(fixed64, tag = "1")]
        pub schema_index_memptr: u64,
        /// Schema against which documents are to be validated,
        /// and which provides reduction annotations.
        #[prost(string, tag = "2")]
        pub schema_uri: std::string::String,
        /// Composite key used to group documents to be combined, specified as one or
        /// more JSON-Pointers indicating a message location to extract.
        /// If empty, all request documents are combined into a single response
        /// document.
        #[prost(string, repeated, tag = "3")]
        pub key_ptr: ::std::vec::Vec<std::string::String>,
        /// Field JSON pointers to be extracted from combined documents and returned.
        /// If empty, no fields are extracted.
        #[prost(string, repeated, tag = "4")]
        pub field_ptrs: ::std::vec::Vec<std::string::String>,
        /// JSON-Pointer at which a placeholder UUID should be inserted into
        /// returned documents. If empty, no placeholder is inserted.
        #[prost(string, tag = "5")]
        pub uuid_placeholder_ptr: std::string::String,
        /// Prune is true if this CombineRequest includes the root-most
        /// (equivalently, left-most) document of each key. Depending on the
        /// reduction strategy, additional pruning can be done in this case
        /// (i.e., removing tombstones) that isn't possible in a partial
        /// non-root reduction.
        #[prost(bool, tag = "6")]
        pub prune: bool,
    }
}
/// DeriveAPI is a meta-message which name spaces messages of the Derive API bridge.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeriveApi {}
pub mod derive_api {
    /// Config configures an instance of the derive service.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Config {
        /// Path to the catalog database.
        #[prost(string, tag = "1")]
        pub catalog_path: std::string::String,
        /// Name of the collection to derive.
        #[prost(string, tag = "2")]
        pub derivation: std::string::String,
        /// Local directory for ephemeral processing state.
        #[prost(string, tag = "3")]
        pub local_dir: std::string::String,
        /// Memory address of an RocksDB Environment to use (as a *rocksdb_env_t).
        /// Ownership of the environment is transferred with this message.
        #[prost(fixed64, tag = "4")]
        pub rocksdb_env_memptr: u64,
    }
    /// DocHeader preceds a JSON-encoded document.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct DocHeader {
        #[prost(message, optional, tag = "1")]
        pub uuid: ::std::option::Option<super::UuidParts>,
        #[prost(bytes, tag = "2")]
        pub packed_key: std::vec::Vec<u8>,
        #[prost(int32, tag = "3")]
        pub transform_id: i32,
    }
    /// Flush the transaction pipeline.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Flush {
        /// JSON-Pointer of the UUID placeholder in returned documents.
        #[prost(string, tag = "1")]
        pub uuid_placeholder_ptr: std::string::String,
        /// Field JSON pointers to be extracted from combined documents and returned.
        /// If empty, no fields are extracted.
        #[prost(string, repeated, tag = "2")]
        pub field_ptrs: ::std::vec::Vec<std::string::String>,
    }
    /// Prepare a commit of the transaction.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Prepare {
        /// Checkpoint to commit.
        #[prost(message, optional, tag = "1")]
        pub checkpoint: ::std::option::Option<super::super::consumer::Checkpoint>,
    }
}
/// IngestRequest describes documents to ingest into collections.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IngestRequest {
    #[prost(message, repeated, tag = "1")]
    pub collections: ::std::vec::Vec<ingest_request::Collection>,
}
pub mod ingest_request {
    /// Collection describes an ingest into a collection.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Collection {
        /// Name of the collection into which to ingest.
        #[prost(string, tag = "1")]
        pub name: std::string::String,
        /// Newline-separated JSON documents to ingest.
        #[prost(bytes, tag = "2")]
        pub docs_json_lines: std::vec::Vec<u8>,
    }
}
/// IngestResponse is the result of an Ingest RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IngestResponse {
    /// Journals appended to by this ingestion, and their maximum offset on commit.
    #[prost(map = "string, int64", tag = "1")]
    pub journal_write_heads: ::std::collections::HashMap<std::string::String, i64>,
    /// Etcd header which describes current journal partitions.
    #[prost(message, optional, tag = "2")]
    pub journal_etcd: ::std::option::Option<super::protocol::header::Etcd>,
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
    pub header: ::std::option::Option<super::protocol::Header>,
    #[prost(string, tag = "2")]
    pub shard_id: std::string::String,
}
/// ClearRegistersResponse is a testing-only response to remove all registers of a shard.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ClearRegistersResponse {
    #[prost(enumeration = "super::consumer::Status", tag = "1")]
    pub status: i32,
    #[prost(message, optional, tag = "2")]
    pub header: ::std::option::Option<super::protocol::Header>,
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
