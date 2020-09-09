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
/// Shuffle of documents, mapping each document to member indicies within a
/// Ring.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Shuffle {
    /// Transform for which this Shuffle is being applied.
    #[prost(string, tag = "1")]
    pub transform: std::string::String,
    /// Composite key over which shuffling occurs, specified as one or more
    /// JSON-Pointers indicating a message location to extract.
    #[prost(string, repeated, tag = "2")]
    pub shuffle_key_ptr: ::std::vec::Vec<std::string::String>,
    /// uses_source_key is true if shuffle_key_ptr is the source's native key,
    /// and false if it's some other key. When shuffling using the source's key,
    /// we can minimize data movement by assigning a shard coordinator for each
    /// journal such that the shard's key range overlap that of the journal.
    #[prost(bool, tag = "3")]
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
    #[prost(bool, tag = "6")]
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
}
/// Transform describes a specific transform of a derived collection.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransformSpec {
    #[prost(message, optional, tag = "2")]
    pub source: ::std::option::Option<transform_spec::Source>,
    /// Shuffle applied to source documents for this transform.
    /// Note that the Shuffle embeds the Transform name.
    #[prost(message, optional, tag = "3")]
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
/// Field holds a column of values extracted from a document location.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Field {
    #[prost(message, repeated, tag = "1")]
    pub values: ::std::vec::Vec<field::Value>,
}
pub mod field {
    /// Value is the extracted representation of the field value.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Value {
        #[prost(enumeration = "value::Kind", tag = "1")]
        pub kind: i32,
        #[prost(uint64, tag = "2")]
        pub unsigned: u64,
        #[prost(sint64, tag = "3")]
        pub signed: i64,
        #[prost(double, tag = "4")]
        pub double: f64,
        #[prost(message, optional, tag = "5")]
        pub bytes: ::std::option::Option<super::Slice>,
    }
    pub mod value {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
        #[repr(i32)]
        pub enum Kind {
            Invalid = 0,
            Null = 1,
            True = 2,
            False = 3,
            String = 4,
            Unsigned = 5,
            Signed = 6,
            Double = 7,
            Object = 8,
            Array = 9,
        }
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
    /// Transform name, passed through from the ShuffleRequest.
    #[prost(string, tag = "7")]
    pub transform: std::string::String,
    /// Shuffled documents, each encoded in the 'application/json'
    /// media-type.
    #[prost(message, repeated, tag = "9")]
    pub docs_json: ::std::vec::Vec<Slice>,
    /// The begin offset of each document within the requested journal.
    #[prost(int64, repeated, packed = "false", tag = "10")]
    pub begin: ::std::vec::Vec<i64>,
    /// The end offset of each document within the journal.
    #[prost(int64, repeated, packed = "false", tag = "11")]
    pub end: ::std::vec::Vec<i64>,
    /// UUIDParts of each document.
    #[prost(message, repeated, tag = "12")]
    pub uuid_parts: ::std::vec::Vec<UuidParts>,
    /// Packed, embedded encoding of the shuffle key into a byte string.
    /// If the Shuffle specified a Hash to use, it's applied as well.
    #[prost(message, repeated, tag = "13")]
    pub packed_key: ::std::vec::Vec<Slice>,
    /// Extracted shuffle key of each document, with one Field for each
    /// component of the composite shuffle key.
    #[prost(message, repeated, tag = "14")]
    pub shuffle_key: ::std::vec::Vec<Field>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExtractRequest {
    /// Memory arena of this message.
    #[prost(bytes, tag = "1")]
    pub arena: std::vec::Vec<u8>,
    /// Documents to extract, each encoded in the 'application/json'
    /// media-type.
    #[prost(message, repeated, tag = "2")]
    pub docs_json: ::std::vec::Vec<Slice>,
    /// JSON pointer of the document UUID to extract.
    /// If empty, UUIDParts are not extracted.
    #[prost(string, tag = "3")]
    pub uuid_ptr: std::string::String,
    /// Field JSON pointers to extract from documents and return.
    /// If empty, no fields are extracted.
    #[prost(string, repeated, tag = "4")]
    pub field_ptrs: ::std::vec::Vec<std::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExtractResponse {
    /// Memory arena of this message.
    #[prost(bytes, tag = "1")]
    pub arena: std::vec::Vec<u8>,
    /// UUIDParts extracted from request Documents.
    #[prost(message, repeated, tag = "2")]
    pub uuid_parts: ::std::vec::Vec<UuidParts>,
    /// Fields extracted from request Documents, one column per request pointer.
    #[prost(message, repeated, tag = "3")]
    pub fields: ::std::vec::Vec<Field>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CombineRequest {
    #[prost(oneof = "combine_request::Kind", tags = "1, 2")]
    pub kind: ::std::option::Option<combine_request::Kind>,
}
pub mod combine_request {
    /// Open a CombineRequest.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Open {
        /// Schema against which documents are to be validated,
        /// and which provides reduction annotations.
        #[prost(string, tag = "1")]
        pub schema_uri: std::string::String,
        /// Composite key used to group documents to be combined, specified as one or
        /// more JSON-Pointers indicating a message location to extract.
        /// If empty, all request documents are combined into a single response
        /// document.
        #[prost(string, repeated, tag = "2")]
        pub key_ptr: ::std::vec::Vec<std::string::String>,
        /// Field JSON pointers to be extracted from combined documents and returned.
        /// If empty, no fields are extracted.
        #[prost(string, repeated, tag = "3")]
        pub field_ptrs: ::std::vec::Vec<std::string::String>,
        /// JSON-Pointer at which a placeholder UUID should be inserted into
        /// returned documents. If empty, no placeholder is inserted.
        #[prost(string, tag = "4")]
        pub uuid_placeholder_ptr: std::string::String,
    }
    /// Continue an opened CombineRequest.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Continue {
        /// Memory arena of this message.
        #[prost(bytes, tag = "1")]
        pub arena: std::vec::Vec<u8>,
        /// Request documents to combine, each encoded in the 'application/json'
        /// media-type.
        #[prost(message, repeated, tag = "2")]
        pub docs_json: ::std::vec::Vec<super::Slice>,
    }
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Kind {
        #[prost(message, tag = "1")]
        Open(Open),
        #[prost(message, tag = "2")]
        Continue(Continue),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CombineResponse {
    /// Memory arena of this message.
    #[prost(bytes, tag = "1")]
    pub arena: std::vec::Vec<u8>,
    /// Combined response documents, each encoded in the 'application/json'
    /// media-type.
    #[prost(message, repeated, tag = "2")]
    pub docs_json: ::std::vec::Vec<Slice>,
    /// Fields extracted from request Documents, one column per request field
    /// pointer.
    #[prost(message, repeated, tag = "3")]
    pub fields: ::std::vec::Vec<Field>,
}
/// DeriveRequest is the streamed union type message of a Derive RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeriveRequest {
    #[prost(oneof = "derive_request::Kind", tags = "1, 2, 3, 4")]
    pub kind: ::std::option::Option<derive_request::Kind>,
}
pub mod derive_request {
    /// Open is sent (only) as the first message of a Derive RPC,
    /// and opens the derive transaction.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Open {
        /// Collection to be derived.
        #[prost(string, tag = "1")]
        pub collection: std::string::String,
    }
    /// Continue extends the derive transaction with additional
    /// source collection documents.
    ///
    /// * The flow consumer sends any number of EXTEND DeriveRequests,
    ///   containing source collection documents.
    /// * Concurrently, the derive worker responds with any number of
    ///   EXTEND DeriveResponses, each having documents to be added to
    ///   the collection being derived.
    /// * The flow consumer is responsible for publishing each derived
    ///   document to the appropriate collection & partition.
    /// * Note that DeriveRequest and DeriveResponse Continue messages are _not_
    /// 1:1.
    ///
    /// Continue transitions to continue or flush.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Continue {
        /// Memory arena of this message.
        #[prost(bytes, tag = "1")]
        pub arena: std::vec::Vec<u8>,
        /// Source documents to derive from, each encoded in the 'application/json'
        /// media-type.
        #[prost(message, repeated, tag = "2")]
        pub docs_json: ::std::vec::Vec<super::Slice>,
        /// UUIDParts extracted from documents.
        #[prost(message, repeated, tag = "3")]
        pub uuid_parts: ::std::vec::Vec<super::UuidParts>,
        /// Packed, embedded encoding of the shuffle key into a byte string.
        /// If the Shuffle specified a Hash to use, it's applied as well.
        #[prost(message, repeated, tag = "4")]
        pub packed_key: ::std::vec::Vec<super::Slice>,
        /// Catalog transform ID to which each document is applied.
        #[prost(int32, repeated, tag = "5")]
        pub transform_id: ::std::vec::Vec<i32>,
    }
    /// FLUSH indicates the transacton pipeline is to flush.
    ///
    /// * The flow consumer issues FLUSH when its consumer transaction begins to
    ///   close.
    /// * The derive worker responds with FLUSH to indicate that all source
    ///   documents have been processed and all derived documents emitted.
    /// * The flow consumer awaits the response FLUSH, while continuing to begin
    ///   publish operations for all derived documents seen in the meantime.
    /// * On seeing FLUSH, the flow consumer is assured it's sequenced and started
    ///   publishing all derived documents of the transaction, and can now build
    ///   the consumer.Checkpoint which will be committed to the store.
    ///
    /// FLUSH transitions to PREPARE.
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
    /// PREPARE begins a commit of the transaction.
    ///
    /// * The flow consumer sends PREPARE with its consumer.Checkpoint.
    /// * On receipt, the derive worker queues an atomic recoverylog.Recorder
    ///   block that's conditioned on an (unresolved) "commit" future. Within
    ///   this recording block, underlying store commits (SQLite COMMIT and writing
    ///   a RocksDB WriteBatch) are issued to persist all state changes of the
    ///   transaction, along with the consumer.Checkpoint.
    /// * The derive worker responds with PREPARE once all local commits have
    ///   completed, and recoverylog writes have been queued (but not started,
    ///   awaiting COMMIT).
    /// * On receipt, the flow consumer arranges to invoke COMMIT on the completion
    ///   of all outstanding journal writes -- this the OpFuture passed to the
    ///   Store.StartCommit interface. It returns a future which will resolve only
    ///   after reading COMMIT from this transaction -- the OpFuture returned by
    ///   that interface.
    ///
    /// It's an error if a prior transaction is still running at the onset of
    /// PREPARE. However at the completion of PREPARE, a new & concurrent
    /// Transaction may begin, though it itself cannot PREPARE until this
    /// Transaction fully completes.
    ///
    /// PREPARE transitions to COMMIT.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Prepare {
        /// Checkpoint to commit.
        #[prost(message, optional, tag = "1")]
        pub checkpoint: ::std::option::Option<super::super::consumer::Checkpoint>,
    }
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Kind {
        #[prost(message, tag = "1")]
        Open(Open),
        #[prost(message, tag = "2")]
        Continue(Continue),
        #[prost(message, tag = "3")]
        Flush(Flush),
        #[prost(message, tag = "4")]
        Prepare(Prepare),
    }
}
/// DeriveResponse is the streamed response message of a Derive RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeriveResponse {
    #[prost(oneof = "derive_response::Kind", tags = "1, 2")]
    pub kind: ::std::option::Option<derive_response::Kind>,
}
pub mod derive_response {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Continue {}
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Kind {
        /// Continue extends the derive transaction with additional derived
        /// and combined collection documents.
        #[prost(message, tag = "1")]
        Continue(Continue),
        /// Flushed CombineResponses are sent in response to a request flush.
        /// An empty CombineResponse signals that no further responses remain
        /// to be sent, and the server is ready to prepare to commit.
        #[prost(message, tag = "2")]
        Flush(super::CombineResponse),
    }
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
pub mod extract_client {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    pub struct ExtractClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ExtractClient<tonic::transport::Channel> {
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
    impl<T> ExtractClient<T>
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
        pub async fn extract(
            &mut self,
            request: impl tonic::IntoRequest<super::ExtractRequest>,
        ) -> Result<tonic::Response<super::ExtractResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/flow.Extract/Extract");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
    impl<T: Clone> Clone for ExtractClient<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }
    impl<T> std::fmt::Debug for ExtractClient<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "ExtractClient {{ ... }}")
        }
    }
}
#[doc = r" Generated client implementations."]
pub mod combine_client {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    pub struct CombineClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl CombineClient<tonic::transport::Channel> {
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
    impl<T> CombineClient<T>
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
        pub async fn combine(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = super::CombineRequest>,
        ) -> Result<tonic::Response<tonic::codec::Streaming<super::CombineResponse>>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/flow.Combine/Combine");
            self.inner
                .streaming(request.into_streaming_request(), path, codec)
                .await
        }
    }
    impl<T: Clone> Clone for CombineClient<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }
    impl<T> std::fmt::Debug for CombineClient<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CombineClient {{ ... }}")
        }
    }
}
#[doc = r" Generated client implementations."]
pub mod derive_client {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    pub struct DeriveClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl DeriveClient<tonic::transport::Channel> {
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
    impl<T> DeriveClient<T>
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
        #[doc = " RestoreCheckpoint recovers the most recent Checkpoint previously committed"]
        #[doc = " to the Store. It is called just once, at Shard start-up. If an external"]
        #[doc = " system is used, it should install a transactional \"write fence\" to ensure"]
        #[doc = " that an older Store instance of another process cannot successfully"]
        #[doc = " StartCommit after this RestoreCheckpoint returns."]
        pub async fn restore_checkpoint(
            &mut self,
            request: impl tonic::IntoRequest<()>,
        ) -> Result<tonic::Response<super::super::consumer::Checkpoint>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/flow.Derive/RestoreCheckpoint");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Derive begins a pipelined derive transaction, following the"]
        #[doc = " state machine detailed in DeriveState."]
        pub async fn derive(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = super::DeriveRequest>,
        ) -> Result<tonic::Response<tonic::codec::Streaming<super::DeriveResponse>>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/flow.Derive/Derive");
            self.inner
                .streaming(request.into_streaming_request(), path, codec)
                .await
        }
        #[doc = " BuildHints returns FSMHints which may be played back to fully reconstruct"]
        #[doc = " the local filesystem state produced by this derive worker. It may block"]
        #[doc = " while pending operations sync to the recovery log."]
        pub async fn build_hints(
            &mut self,
            request: impl tonic::IntoRequest<()>,
        ) -> Result<tonic::Response<super::super::recoverylog::FsmHints>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/flow.Derive/BuildHints");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
    impl<T: Clone> Clone for DeriveClient<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }
    impl<T> std::fmt::Debug for DeriveClient<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "DeriveClient {{ ... }}")
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
pub mod extract_server {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with ExtractServer."]
    #[async_trait]
    pub trait Extract: Send + Sync + 'static {
        async fn extract(
            &self,
            request: tonic::Request<super::ExtractRequest>,
        ) -> Result<tonic::Response<super::ExtractResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct ExtractServer<T: Extract> {
        inner: _Inner<T>,
    }
    struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
    impl<T: Extract> ExtractServer<T> {
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
    impl<T, B> Service<http::Request<B>> for ExtractServer<T>
    where
        T: Extract,
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
                "/flow.Extract/Extract" => {
                    #[allow(non_camel_case_types)]
                    struct ExtractSvc<T: Extract>(pub Arc<T>);
                    impl<T: Extract> tonic::server::UnaryService<super::ExtractRequest> for ExtractSvc<T> {
                        type Response = super::ExtractResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ExtractRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).extract(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = ExtractSvc(inner);
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
    impl<T: Extract> Clone for ExtractServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self { inner }
        }
    }
    impl<T: Extract> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone(), self.1.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Extract> tonic::transport::NamedService for ExtractServer<T> {
        const NAME: &'static str = "flow.Extract";
    }
}
#[doc = r" Generated server implementations."]
pub mod combine_server {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with CombineServer."]
    #[async_trait]
    pub trait Combine: Send + Sync + 'static {
        #[doc = "Server streaming response type for the Combine method."]
        type CombineStream: Stream<Item = Result<super::CombineResponse, tonic::Status>>
            + Send
            + Sync
            + 'static;
        async fn combine(
            &self,
            request: tonic::Request<tonic::Streaming<super::CombineRequest>>,
        ) -> Result<tonic::Response<Self::CombineStream>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct CombineServer<T: Combine> {
        inner: _Inner<T>,
    }
    struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
    impl<T: Combine> CombineServer<T> {
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
    impl<T, B> Service<http::Request<B>> for CombineServer<T>
    where
        T: Combine,
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
                "/flow.Combine/Combine" => {
                    #[allow(non_camel_case_types)]
                    struct CombineSvc<T: Combine>(pub Arc<T>);
                    impl<T: Combine> tonic::server::StreamingService<super::CombineRequest> for CombineSvc<T> {
                        type Response = super::CombineResponse;
                        type ResponseStream = T::CombineStream;
                        type Future =
                            BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<tonic::Streaming<super::CombineRequest>>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).combine(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1;
                        let inner = inner.0;
                        let method = CombineSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.streaming(method, req).await;
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
    impl<T: Combine> Clone for CombineServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self { inner }
        }
    }
    impl<T: Combine> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone(), self.1.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Combine> tonic::transport::NamedService for CombineServer<T> {
        const NAME: &'static str = "flow.Combine";
    }
}
#[doc = r" Generated server implementations."]
pub mod derive_server {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with DeriveServer."]
    #[async_trait]
    pub trait Derive: Send + Sync + 'static {
        #[doc = " RestoreCheckpoint recovers the most recent Checkpoint previously committed"]
        #[doc = " to the Store. It is called just once, at Shard start-up. If an external"]
        #[doc = " system is used, it should install a transactional \"write fence\" to ensure"]
        #[doc = " that an older Store instance of another process cannot successfully"]
        #[doc = " StartCommit after this RestoreCheckpoint returns."]
        async fn restore_checkpoint(
            &self,
            request: tonic::Request<()>,
        ) -> Result<tonic::Response<super::super::consumer::Checkpoint>, tonic::Status>;
        #[doc = "Server streaming response type for the Derive method."]
        type DeriveStream: Stream<Item = Result<super::DeriveResponse, tonic::Status>>
            + Send
            + Sync
            + 'static;
        #[doc = " Derive begins a pipelined derive transaction, following the"]
        #[doc = " state machine detailed in DeriveState."]
        async fn derive(
            &self,
            request: tonic::Request<tonic::Streaming<super::DeriveRequest>>,
        ) -> Result<tonic::Response<Self::DeriveStream>, tonic::Status>;
        #[doc = " BuildHints returns FSMHints which may be played back to fully reconstruct"]
        #[doc = " the local filesystem state produced by this derive worker. It may block"]
        #[doc = " while pending operations sync to the recovery log."]
        async fn build_hints(
            &self,
            request: tonic::Request<()>,
        ) -> Result<tonic::Response<super::super::recoverylog::FsmHints>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct DeriveServer<T: Derive> {
        inner: _Inner<T>,
    }
    struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
    impl<T: Derive> DeriveServer<T> {
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
    impl<T, B> Service<http::Request<B>> for DeriveServer<T>
    where
        T: Derive,
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
                "/flow.Derive/RestoreCheckpoint" => {
                    #[allow(non_camel_case_types)]
                    struct RestoreCheckpointSvc<T: Derive>(pub Arc<T>);
                    impl<T: Derive> tonic::server::UnaryService<()> for RestoreCheckpointSvc<T> {
                        type Response = super::super::consumer::Checkpoint;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(&mut self, request: tonic::Request<()>) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).restore_checkpoint(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = RestoreCheckpointSvc(inner);
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
                "/flow.Derive/Derive" => {
                    #[allow(non_camel_case_types)]
                    struct DeriveSvc<T: Derive>(pub Arc<T>);
                    impl<T: Derive> tonic::server::StreamingService<super::DeriveRequest> for DeriveSvc<T> {
                        type Response = super::DeriveResponse;
                        type ResponseStream = T::DeriveStream;
                        type Future =
                            BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<tonic::Streaming<super::DeriveRequest>>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).derive(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1;
                        let inner = inner.0;
                        let method = DeriveSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/flow.Derive/BuildHints" => {
                    #[allow(non_camel_case_types)]
                    struct BuildHintsSvc<T: Derive>(pub Arc<T>);
                    impl<T: Derive> tonic::server::UnaryService<()> for BuildHintsSvc<T> {
                        type Response = super::super::recoverylog::FsmHints;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(&mut self, request: tonic::Request<()>) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).build_hints(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = BuildHintsSvc(inner);
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
    impl<T: Derive> Clone for DeriveServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self { inner }
        }
    }
    impl<T: Derive> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone(), self.1.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Derive> tonic::transport::NamedService for DeriveServer<T> {
        const NAME: &'static str = "flow.Derive";
    }
}
