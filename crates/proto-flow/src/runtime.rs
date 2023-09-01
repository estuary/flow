#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskServiceConfig {
    #[prost(int32, tag = "1")]
    pub log_file_fd: i32,
    #[prost(string, tag = "2")]
    pub task_name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub uds_path: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub container_network: ::prost::alloc::string::String,
}
/// ShuffleRequest is the request message of a Shuffle RPC.
/// It's a description of a document shuffle,
/// where a journal is read and each document is mapped into:
///    - An extracted, packed, and hashed composite key (a "shuffle key").
///    - A rotated Clock value (an "r-clock").
///
/// The packed key and r-clock can then be compared to individual reader
/// RangeSpec's.
///
/// ShuffleRequest instances are keyed and compared on (`journal`, `replay`, `build_id`),
/// in order to identify and group related reads. Note that `journal` has a metadata path
/// segment which uniquely identifies its particular derivation transform
/// or materialization binding. Reads with equivalent shuffles are placed into
/// common "read rings" which consolidate their underlying journal reads.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleRequest {
    /// Journal to be shuffled.
    #[prost(string, tag = "1")]
    pub journal: ::prost::alloc::string::String,
    /// Is this a reply of the journal's content? We separate ongoing vs replayed
    /// reads of a journal's content into distinct rings.
    #[prost(bool, tag = "2")]
    pub replay: bool,
    /// Build ID of the task which requested this JournalShuffle.
    #[prost(string, tag = "3")]
    pub build_id: ::prost::alloc::string::String,
    /// Offset to begin reading the journal from.
    #[prost(int64, tag = "4")]
    pub offset: i64,
    /// Offset to stop reading the journal at, or zero if unbounded.
    #[prost(int64, tag = "5")]
    pub end_offset: i64,
    /// Ranges of responsibility which are unique to this reader,
    /// against which document shuffle outcomes are matched to determine
    /// read eligibility.
    #[prost(message, optional, tag = "6")]
    pub range: ::core::option::Option<super::flow::RangeSpec>,
    /// Coordinator is the Shard ID which is responsible for reads of this journal.
    #[prost(string, tag = "7")]
    pub coordinator: ::prost::alloc::string::String,
    /// Resolution header of the |shuffle.coordinator| shard.
    #[prost(message, optional, tag = "8")]
    pub resolution: ::core::option::Option<::proto_gazette::broker::Header>,
    /// Index of the derivation transform or materialization
    /// binding on whose behalf we're reading.
    #[prost(uint32, tag = "9")]
    pub shuffle_index: u32,
    /// Derivation which is requesting the shuffle.
    #[prost(message, optional, tag = "10")]
    pub derivation: ::core::option::Option<super::flow::CollectionSpec>,
    /// Materialization which is requesting the shuffle.
    #[prost(message, optional, tag = "11")]
    pub materialization: ::core::option::Option<super::flow::MaterializationSpec>,
}
/// ShuffleResponse is the streamed response message of a Shuffle RPC.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleResponse {
    /// Status of the Shuffle RPC.
    #[prost(enumeration = "::proto_gazette::consumer::Status", tag = "1")]
    pub status: i32,
    /// Header of the response.
    #[prost(message, optional, tag = "2")]
    pub header: ::core::option::Option<::proto_gazette::broker::Header>,
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
    pub docs: ::prost::alloc::vec::Vec<super::flow::Slice>,
    /// The journal offsets of each document within the requested journal.
    /// For a document at index i, its offsets are [ offsets\[2*i\], offsets\[2*i+1\]
    /// ).
    #[prost(int64, repeated, packed = "false", tag = "8")]
    pub offsets: ::prost::alloc::vec::Vec<i64>,
    /// UUIDParts of each document.
    #[prost(message, repeated, tag = "9")]
    pub uuid_parts: ::prost::alloc::vec::Vec<super::flow::UuidParts>,
    /// Packed, embedded encoding of the shuffle key into a byte string.
    /// If the Shuffle specified a Hash to use, it's applied as well.
    #[prost(message, repeated, tag = "10")]
    pub packed_key: ::prost::alloc::vec::Vec<super::flow::Slice>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RocksDbDescriptor {
    /// Instrumented RocksDB environment which should be opened.
    #[prost(fixed64, tag = "1")]
    pub rocksdb_env_memptr: u64,
    /// Path to the RocksDB directory to be opened.
    #[prost(string, tag = "2")]
    pub rocksdb_path: ::prost::alloc::string::String,
}
/// Container is a description of a running connector container.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Container {
    #[prost(string, tag = "1")]
    pub ip_addr: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub network_ports: ::prost::alloc::vec::Vec<super::flow::NetworkPort>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CaptureRequestExt {
    #[prost(message, optional, tag = "1")]
    pub labels: ::core::option::Option<super::ops::ShardLabeling>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CaptureResponseExt {
    #[prost(message, optional, tag = "1")]
    pub container: ::core::option::Option<Container>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeriveRequestExt {
    #[prost(message, optional, tag = "1")]
    pub labels: ::core::option::Option<super::ops::ShardLabeling>,
    #[prost(message, optional, tag = "2")]
    pub open: ::core::option::Option<derive_request_ext::Open>,
}
/// Nested message and enum types in `DeriveRequestExt`.
pub mod derive_request_ext {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Open {
        /// URL with a registered SQLite VFS which should be opened.
        #[prost(string, tag = "1")]
        pub sqlite_vfs_uri: ::prost::alloc::string::String,
        /// RocksDB descriptor which should be opened.
        #[prost(message, optional, tag = "2")]
        pub rocksdb_descriptor: ::core::option::Option<super::RocksDbDescriptor>,
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeriveResponseExt {
    #[prost(message, optional, tag = "1")]
    pub container: ::core::option::Option<Container>,
    #[prost(message, optional, tag = "2")]
    pub opened: ::core::option::Option<derive_response_ext::Opened>,
    #[prost(message, optional, tag = "3")]
    pub published: ::core::option::Option<derive_response_ext::Published>,
    #[prost(message, optional, tag = "4")]
    pub flushed: ::core::option::Option<derive_response_ext::Flushed>,
}
/// Nested message and enum types in `DeriveResponseExt`.
pub mod derive_response_ext {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Opened {
        #[prost(message, optional, tag = "1")]
        pub runtime_checkpoint: ::core::option::Option<
            ::proto_gazette::consumer::Checkpoint,
        >,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Published {
        /// Maximum UUID Clock of sourced document clocks which lead to this published document.
        #[prost(fixed64, tag = "1")]
        pub max_clock: u64,
        /// Packed key extracted from the published document.
        #[prost(bytes = "bytes", tag = "2")]
        pub key_packed: ::prost::bytes::Bytes,
        /// Packed partition values extracted from the published document.
        #[prost(bytes = "bytes", tag = "3")]
        pub partitions_packed: ::prost::bytes::Bytes,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Flushed {
        #[prost(message, optional, tag = "1")]
        pub stats: ::core::option::Option<super::super::ops::Stats>,
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MaterializeRequestExt {
    #[prost(message, optional, tag = "1")]
    pub labels: ::core::option::Option<super::ops::ShardLabeling>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MaterializeResponseExt {
    #[prost(message, optional, tag = "1")]
    pub container: ::core::option::Option<Container>,
}
