/// ShardLabeling is a parsed and validated representation of the Flow
/// labels which are attached to Gazette ShardSpecs, that are understood
/// by the Flow runtime and influence its behavior with respect to the shard.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardLabeling {
    /// Catalog build identifier which the task uses.
    #[prost(string, tag = "1")]
    pub build: ::prost::alloc::string::String,
    /// Network-addressable hostname of this task shard.
    #[prost(string, tag = "2")]
    pub hostname: ::prost::alloc::string::String,
    /// Logging level of the task.
    #[prost(enumeration = "log::Level", tag = "3")]
    pub log_level: i32,
    /// Key and R-Clock range of the shard.
    #[prost(message, optional, tag = "5")]
    pub range: ::core::option::Option<super::flow::RangeSpec>,
    /// If non-empty, the shard which this task is splitting from.
    #[prost(string, tag = "6")]
    pub split_source: ::prost::alloc::string::String,
    /// If non-empty, the shard which this task is splitting into.
    #[prost(string, tag = "7")]
    pub split_target: ::prost::alloc::string::String,
    /// Name of the shard's task.
    #[prost(string, tag = "8")]
    pub task_name: ::prost::alloc::string::String,
    /// Type of this task (capture, derivation, or materialization).
    #[prost(enumeration = "TaskType", tag = "9")]
    pub task_type: i32,
    /// Journal to which task logs are directed.
    #[prost(string, tag = "10")]
    pub logs_journal: ::prost::alloc::string::String,
    /// Journal to which task stats are directed.
    #[prost(string, tag = "11")]
    pub stats_journal: ::prost::alloc::string::String,
}
/// Common `shard` sub-document logged by Stats and Log.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardRef {
    /// The type of catalog task.
    #[prost(enumeration = "TaskType", tag = "1")]
    pub kind: i32,
    /// The name of the catalog task.
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    /// The hex-encoded inclusive beginning of the shard's assigned key range.
    #[prost(string, tag = "3")]
    pub key_begin: ::prost::alloc::string::String,
    /// The hex-encoded inclusive beginning of the shard's assigned r_clock range.
    #[prost(string, tag = "4")]
    pub r_clock_begin: ::prost::alloc::string::String,
}
/// Common Meta sub-document of Log and Stats documents.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Meta {
    #[prost(string, tag = "1")]
    pub uuid: ::prost::alloc::string::String,
}
/// Log is Flow's unified representation of task logs.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Log {
    /// Meta sub-document added by the Flow runtime.
    #[prost(message, optional, tag = "1")]
    pub meta: ::core::option::Option<Meta>,
    /// The shard which produced this document..
    #[prost(message, optional, tag = "2")]
    pub shard: ::core::option::Option<ShardRef>,
    /// Timestamp corresponding to the start of the transaction.
    /// When aggregating, the timestamp is rounded to various UTC
    /// intervals (for example hour, day, and month).
    #[prost(message, optional, tag = "3")]
    pub timestamp: ::core::option::Option<::pbjson_types::Timestamp>,
    #[prost(enumeration = "log::Level", tag = "4")]
    pub level: i32,
    /// Message of the log.
    #[prost(string, tag = "5")]
    pub message: ::prost::alloc::string::String,
    /// Structured Fields of the log.
    #[prost(btree_map = "string, string", tag = "6")]
    pub fields_json_map: ::prost::alloc::collections::BTreeMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
    /// Parent spans of this log event.
    #[prost(message, repeated, tag = "7")]
    pub spans: ::prost::alloc::vec::Vec<Log>,
}
/// Nested message and enum types in `Log`.
pub mod log {
    /// Level of the log.
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum Level {
        UndefinedLevel = 0,
        Error = 1,
        Warn = 2,
        Info = 3,
        Debug = 4,
        Trace = 5,
    }
    impl Level {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Level::UndefinedLevel => "undefined_level",
                Level::Error => "error",
                Level::Warn => "warn",
                Level::Info => "info",
                Level::Debug => "debug",
                Level::Trace => "trace",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "undefined_level" => Some(Self::UndefinedLevel),
                "error" => Some(Self::Error),
                "warn" => Some(Self::Warn),
                "info" => Some(Self::Info),
                "debug" => Some(Self::Debug),
                "trace" => Some(Self::Trace),
                _ => None,
            }
        }
    }
}
/// Stats is Flow's unified representation of task metrics and statistics.
///
/// Next tag: 10.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Stats {
    /// Meta sub-document added by the Flow runtime.
    #[prost(message, optional, tag = "1")]
    pub meta: ::core::option::Option<Meta>,
    /// The shard which produced this document..
    #[prost(message, optional, tag = "2")]
    pub shard: ::core::option::Option<ShardRef>,
    /// Timestamp corresponding to the start of the transaction.
    /// When aggregating, the timestamp is rounded to various UTC
    /// intervals (for example hour, day, and month).
    #[prost(message, optional, tag = "3")]
    pub timestamp: ::core::option::Option<::pbjson_types::Timestamp>,
    /// Duration of time spent evaluating the transaction,
    /// When aggregating, this is total spent evaluating all transactions
    /// within the interval.
    #[prost(double, tag = "4")]
    pub open_seconds_total: f64,
    /// Number of transactions represented by this document.
    #[prost(uint32, tag = "5")]
    pub txn_count: u32,
    /// Capture metrics.
    #[prost(btree_map = "string, message", tag = "6")]
    pub capture: ::prost::alloc::collections::BTreeMap<
        ::prost::alloc::string::String,
        stats::Binding,
    >,
    #[prost(message, optional, tag = "7")]
    pub derive: ::core::option::Option<stats::Derive>,
    /// Materialization metrics.
    #[prost(btree_map = "string, message", tag = "8")]
    pub materialize: ::prost::alloc::collections::BTreeMap<
        ::prost::alloc::string::String,
        stats::Binding,
    >,
    #[prost(message, optional, tag = "9")]
    pub interval: ::core::option::Option<stats::Interval>,
}
/// Nested message and enum types in `Stats`.
pub mod stats {
    /// DocsAndBytes represents a count of JSON documents and their
    /// cumulative total size in bytes.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct DocsAndBytes {
        #[prost(uint32, tag = "1")]
        pub docs_total: u32,
        #[prost(uint64, tag = "2")]
        pub bytes_total: u64,
    }
    /// Binding represents counts of JSON documents and their
    /// cumulative total size in bytes, passing through the binding
    /// of a capture or materialization.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Binding {
        #[prost(message, optional, tag = "1")]
        pub left: ::core::option::Option<DocsAndBytes>,
        #[prost(message, optional, tag = "2")]
        pub right: ::core::option::Option<DocsAndBytes>,
        #[prost(message, optional, tag = "3")]
        pub out: ::core::option::Option<DocsAndBytes>,
    }
    /// Derivation metrics.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Derive {
        /// A map from transform name (not collection name), to metrics for that transform.
        #[prost(btree_map = "string, message", tag = "1")]
        pub transforms: ::prost::alloc::collections::BTreeMap<
            ::prost::alloc::string::String,
            derive::Transform,
        >,
        /// Documents published by the derivation connector.
        #[prost(message, optional, tag = "2")]
        pub published: ::core::option::Option<DocsAndBytes>,
        /// Documents written to the derived collection, after combining over published documents.
        #[prost(message, optional, tag = "3")]
        pub out: ::core::option::Option<DocsAndBytes>,
    }
    /// Nested message and enum types in `Derive`.
    pub mod derive {
        #[allow(clippy::derive_partial_eq_without_eq)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Transform {
            /// The name of the collection that this transform sourced from.
            #[prost(string, tag = "1")]
            pub source: ::prost::alloc::string::String,
            /// Input documents that were read by this transform.
            #[prost(message, optional, tag = "2")]
            pub input: ::core::option::Option<super::DocsAndBytes>,
        }
    }
    /// Interval metrics are emitted at regular intervals.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Interval {
        /// Number of seconds that the task shard is metered as having been running.
        /// This is measured by sampling for uptime at fixed wall-clock intervals
        /// (for example, at precisely XX:05:00, XX:10:00, XX:15:00, and so on).
        #[prost(uint32, tag = "1")]
        pub uptime_seconds: u32,
        /// Usage rate adjustment which accompanies and adjusts `uptime_seconds`.
        /// The effective number of "used" task seconds is:
        ///    round(uptime_seconds * usage_rate)
        ///
        /// At present, capture and materialization tasks always use a fixed value of 1.0,
        /// while derivation tasks use a fixed value of 0.0.
        /// The choice of `usage_rate` MAY have more critera in the future.
        #[prost(float, tag = "2")]
        pub usage_rate: f32,
    }
}
/// The type of a catalog task.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TaskType {
    InvalidType = 0,
    Capture = 1,
    Derivation = 2,
    Materialization = 3,
}
impl TaskType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            TaskType::InvalidType => "invalid_type",
            TaskType::Capture => "capture",
            TaskType::Derivation => "derivation",
            TaskType::Materialization => "materialization",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "invalid_type" => Some(Self::InvalidType),
            "capture" => Some(Self::Capture),
            "derivation" => Some(Self::Derivation),
            "materialization" => Some(Self::Materialization),
            _ => None,
        }
    }
}
