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
    /// Number of top-ranked processors to broadcast each message to, after
    /// shuffling. Usually this is one. If non-zero, |choose_from| cannot be set.
    #[prost(uint32, tag = "3")]
    pub broadcast_to: u32,
    /// Number of top-ranked readers from which a single reader index will be
    /// selected, after shuffling. The message Clock value is used to pseudo
    /// randomly pick the final index, making the selection deterministic.
    /// Values larger than one can be used to distribute "hot keys" which might
    /// otherwise overwhelm specific readers.
    /// Usually this is zero and |broadcast_to| is used instead. If non-zero,
    /// |broadcast_to| cannot be set.
    #[prost(uint32, tag = "4")]
    pub choose_from: u32,
    /// Number of seconds for which documents of this collection are delayed
    /// while reading, relative to other documents (when back-filling) and the
    /// present wall-clock time (when tailing).
    #[prost(uint32, tag = "5")]
    pub read_delay_seconds: u32,
}
/// Ring is a topology of members, working in concert to share a task.
/// Each derived collection has a Ring of member shards which are
/// responsible for its continuous derivation.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Ring {
    /// Unique name of this ring.
    #[prost(string, tag = "1")]
    pub name: std::string::String,
    #[prost(message, repeated, tag = "2")]
    pub members: ::std::vec::Vec<ring::Member>,
}
pub mod ring {
    /// Current members of this ring.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Member {
        /// Miniumum Clock of messages processed by this member, used to:
        /// - Lower-bound messages mapped to this member.
        /// - Lower-bound the fragment from which this member starts reading.
        #[prost(uint64, tag = "1")]
        pub min_msg_clock: u64,
        /// Maximum Clock of messages processed by this member, used to
        /// upper-bound messages mapped to this member.
        #[prost(uint64, tag = "2")]
        pub max_msg_clock: u64,
    }
}
/// ShuffleConfig places a Shuffle within a specific, configured execution
/// context within which it runs.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleConfig {
    /// Journal to be shuffled.
    #[prost(string, tag = "1")]
    pub journal: std::string::String,
    /// Ring on whose behalf this journal is being shuffled.
    #[prost(message, optional, tag = "2")]
    pub ring: ::std::option::Option<Ring>,
    /// Coordinator is the ring member index which is responsible for shuffled
    /// reads of this journal.
    #[prost(uint32, tag = "3")]
    pub coordinator: u32,
    /// Shuffle of this ShuffleConfig.
    #[prost(message, optional, tag = "4")]
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
/// ShuffleRequest is the request message of a Shuffle RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleRequest {
    #[prost(message, optional, tag = "1")]
    pub config: ::std::option::Option<ShuffleConfig>,
    /// Index of this member within the ring.
    #[prost(uint32, tag = "2")]
    pub ring_index: u32,
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
    /// ContentType of documents in this ShuffleResponse.
    #[prost(enumeration = "ContentType", tag = "8")]
    pub content_type: i32,
    /// Content of documents included in this ShuffleResponse.
    #[prost(message, repeated, tag = "9")]
    pub content: ::std::vec::Vec<Slice>,
    /// The begin offset of each document within the requested journal.
    #[prost(int64, repeated, packed = "false", tag = "10")]
    pub begin: ::std::vec::Vec<i64>,
    /// The end offset of each document within the journal.
    #[prost(int64, repeated, packed = "false", tag = "11")]
    pub end: ::std::vec::Vec<i64>,
    /// UUIDParts of each document.
    #[prost(message, repeated, tag = "12")]
    pub uuid_parts: ::std::vec::Vec<UuidParts>,
    /// Extracted shuffle key of each document, with one Field for each
    /// component of the composite shuffle key.
    #[prost(message, repeated, tag = "13")]
    pub shuffle_key: ::std::vec::Vec<Field>,
    /// Extracted unique hash of the document shuffle key (low 64-bits).
    #[prost(fixed64, repeated, tag = "14")]
    pub shuffle_hashes_low: ::std::vec::Vec<u64>,
    /// Extracted unique hash of the document shuffle key (high 64-bits).
    #[prost(fixed64, repeated, tag = "15")]
    pub shuffle_hashes_high: ::std::vec::Vec<u64>,
}
/// DeriveRequest is the streamed message of a Derive RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeriveRequest {
    #[prost(enumeration = "DeriveState", tag = "1")]
    pub state: i32,
    /// Memory arena of this message.
    #[prost(bytes, tag = "2")]
    pub arena: std::vec::Vec<u8>,
    /// ContentType of documents in this DeriveRequest. Set iff state == EXTEND.
    #[prost(enumeration = "ContentType", tag = "3")]
    pub content_type: i32,
    /// Content of documents included in this DeriveRequest.
    /// Set iff state == EXTEND.
    #[prost(message, repeated, tag = "4")]
    pub content: ::std::vec::Vec<Slice>,
    /// Checkpoint to commit. Set iff state == PREPARE.
    #[prost(message, optional, tag = "5")]
    pub checkpoint: ::std::option::Option<super::consumer::Checkpoint>,
}
/// DeriveResponse is the streamed response message of a Derive RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeriveResponse {
    #[prost(enumeration = "DeriveState", tag = "1")]
    pub state: i32,
    /// Memory arena of this message.
    #[prost(bytes, tag = "2")]
    pub arena: std::vec::Vec<u8>,
    /// ContentType of documents in this DeriveResponse. Set iff state == EXTEND.
    #[prost(enumeration = "ContentType", tag = "3")]
    pub content_type: i32,
    /// Content of documents included in this DeriveResponse.
    /// Set iff state == EXTEND.
    #[prost(message, repeated, tag = "4")]
    pub content: ::std::vec::Vec<Slice>,
    /// Logical partitions extracted from |documents|.
    #[prost(message, repeated, tag = "5")]
    pub partitions: ::std::vec::Vec<Field>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExtractRequest {
    /// Memory arena of this message.
    #[prost(bytes, tag = "1")]
    pub arena: std::vec::Vec<u8>,
    /// ContentType of documents in this ExtractRequest.
    #[prost(enumeration = "ContentType", tag = "2")]
    pub content_type: i32,
    /// Content of documents included in this ExtractRequest.
    #[prost(message, repeated, tag = "3")]
    pub content: ::std::vec::Vec<Slice>,
    /// JSON pointer of document UUID to extract.
    /// If empty, UUIDParts are not extracted.
    #[prost(string, tag = "4")]
    pub uuid_ptr: std::string::String,
    /// Composite of JSON pointers to extract from documents and hash.
    /// If empty, hashes are not extracted.
    #[prost(string, repeated, tag = "5")]
    pub hash_ptrs: ::std::vec::Vec<std::string::String>,
    /// Field JSON pointers to extract from documents and return.
    /// If empty, no fields are extracted.
    #[prost(string, repeated, tag = "6")]
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
    /// Hashes extracted from request Documents (low 64-bits).
    /// If the request |hash_ptrs| was empty, so are these.
    #[prost(fixed64, repeated, tag = "3")]
    pub hashes_low: ::std::vec::Vec<u64>,
    /// Hashes extracted from request Documents (high 64-bits).
    #[prost(fixed64, repeated, tag = "4")]
    pub hashes_high: ::std::vec::Vec<u64>,
    /// Fields extracted from request Documents, one column per request pointer.
    #[prost(message, repeated, tag = "5")]
    pub fields: ::std::vec::Vec<Field>,
}
/// ContentType is an encoding used for document content.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ContentType {
    Invalid = 0,
    /// JSON is the usual text encoding, with a trailing newline.
    Json = 1,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum DeriveState {
    /// IDLE indicates the transaction has not yet begun.
    /// IDLE transitions to EXTEND.
    Idle = 0,
    /// EXTEND extends the derive transaction with additional
    /// source or derived collection documents.
    ///
    /// * The flow consumer sends any number of EXTEND DeriveRequests,
    ///   containing source collection documents.
    /// * Concurrently, the derive worker responds with any number of
    ///   EXTEND DeriveResponses, each having documents to be added to
    ///   the collection being derived.
    /// * The flow consumer is responsible for publishing each derived
    ///   document to the appropriate collection & partition.
    /// * Note that DeriveRequest and DeriveResponse EXTEND messages are _not_ 1:1.
    ///
    /// EXTEND transitions to EXTEND or FLUSH.
    Extend = 1,
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
    Flush = 2,
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
    Prepare = 3,
    /// COMMIT commits the transaction by resolving the "commit" future created
    /// during PREPARE, allowing the atomic commit block created in PREPARE
    /// to flush to the recovery log. The derive worker responds with COMMIT
    /// when the commit barrier has fully resolved.
    ///
    /// COMMIT transitions to stream close.
    Commit = 4,
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
