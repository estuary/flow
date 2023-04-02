#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskServiceConfig {
    #[prost(int32, tag = "1")]
    pub log_file_fd: i32,
    #[prost(string, tag = "2")]
    pub task_name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub uds_path: ::prost::alloc::string::String,
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
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeriveRequestExt {
    #[prost(message, optional, tag = "1")]
    pub open: ::core::option::Option<derive_request_ext::Open>,
}
/// Nested message and enum types in `DeriveRequestExt`.
pub mod derive_request_ext {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Open {
        /// Configured log level.
        #[prost(enumeration = "super::super::ops::log::Level", tag = "1")]
        pub log_level: i32,
        /// URL with a registered SQLite VFS which should be opened.
        #[prost(string, tag = "2")]
        pub sqlite_vfs_uri: ::prost::alloc::string::String,
        /// RocksDB descriptor which should be opened.
        #[prost(message, optional, tag = "3")]
        pub rocksdb_descriptor: ::core::option::Option<super::RocksDbDescriptor>,
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeriveResponseExt {
    #[prost(message, optional, tag = "1")]
    pub opened: ::core::option::Option<derive_response_ext::Opened>,
    #[prost(message, optional, tag = "2")]
    pub published: ::core::option::Option<derive_response_ext::Published>,
    #[prost(message, optional, tag = "3")]
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
