/// ShardSpec describes a shard and its configuration, and is the long-lived unit
/// of work and scaling for a consumer application. Each shard is allocated to a
/// one "primary" at-a-time selected from the current processes of a consumer
/// application, and is re-assigned on process fault or exit.
///
/// ShardSpecs describe all configuration of the shard and its processing,
/// including journals to consume, configuration for processing transactions, its
/// recovery log, hot standbys, etc. ShardSpecs may be further extended with
/// domain-specific labels & values to further define application behavior.
/// ShardSpec is-a allocator.ItemValue.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardSpec {
    /// ID of the shard.
    #[prost(string, tag="1")]
    pub id: ::prost::alloc::string::String,
    /// Sources of the shard, uniquely ordered on Source journal.
    #[prost(message, repeated, tag="2")]
    pub sources: ::prost::alloc::vec::Vec<shard_spec::Source>,
    /// Prefix of the Journal into which the shard's recovery log will be recorded.
    /// The complete Journal name is built as "{recovery_log_prefix}/{shard_id}".
    /// If empty, the shard does not use a recovery log.
    #[prost(string, tag="3")]
    pub recovery_log_prefix: ::prost::alloc::string::String,
    /// Prefix of Etcd keys into which recovery log FSMHints are written to and
    /// read from. FSMHints allow readers of the recovery log to efficiently
    /// determine the minimum fragments of log which must be read to fully recover
    /// local store state. The complete hint key written by the shard primary is:
    ///
    ///    "{hint_prefix}/{shard_id}.primary"
    ///
    /// The primary will regularly produce updated hints into this key, and
    /// players of the log will similarly utilize hints from this key.
    /// If |recovery_log_prefix| is set, |hint_prefix| must be also.
    #[prost(string, tag="4")]
    pub hint_prefix: ::prost::alloc::string::String,
    /// Backups of verified recovery log FSMHints, retained as a disaster-recovery
    /// mechanism. On completing playback, a player will write recovered hints to:
    ///
    ///    "{hints_prefix}/{shard_id}.backup.0".
    ///
    /// It also move hints previously stored under
    /// "{hints_prefix/{shard_id}.backup.0" to
    /// "{hints_prefix/{shard_id}.backup.1", and so on, keeping at most
    /// |hint_backups| distinct sets of FSMHints.
    ///
    /// In the case of disaster or data-loss, these copied hints can be an
    /// important fallback for recovering a consistent albeit older version of the
    /// shard's store, with each relying on only progressively older portions of
    /// the recovery log.
    ///
    /// When pruning the recovery log, log fragments which are older than (and no
    /// longer required by) the *oldest* backup are discarded, ensuring that
    /// all hints remain valid for playback.
    #[prost(int32, tag="5")]
    pub hint_backups: i32,
    /// Max duration of shard transactions. This duration upper-bounds the amount
    /// of time during which a transaction may process messages before it must
    /// flush and commit. It may run for less time if an input message stall occurs
    /// (eg, no decoded journal message is ready without blocking). A typical value
    /// would be `1s`: applications which perform extensive aggregation over
    /// message streams exhibiting locality of "hot" keys may benefit from larger
    /// values.
    #[prost(message, optional, tag="6")]
    pub max_txn_duration: ::core::option::Option<::pbjson_types::Duration>,
    /// Min duration of shard transactions. This duration lower-bounds the amount
    /// of time during which a transaction must process messages before it may
    /// flush and commit. It may run for more time if additional messages are
    /// available (eg, decoded journal messages are ready without blocking). Note
    /// also that transactions are pipelined: a current transaction may process
    /// messages while a prior transaction's recovery log writes flush to Gazette,
    /// but it cannot prepare to commit until the prior transaction writes
    /// complete. In other words even if |min_txn_quantum| is zero, some degree of
    /// message batching is expected due to the network delay inherent in Gazette
    /// writes. A typical value of would be `0s`: applications which perform
    /// extensive aggregation may benefit from larger values.
    #[prost(message, optional, tag="7")]
    pub min_txn_duration: ::core::option::Option<::pbjson_types::Duration>,
    /// Disable processing of the shard.
    #[prost(bool, tag="8")]
    pub disable: bool,
    /// Hot standbys is the desired number of consumer processes which should be
    /// replicating the primary consumer's recovery log. Standbys are allocated in
    /// a separate availability zone of the current primary, and tail the live log
    /// to continuously mirror the primary's on-disk DB file structure. Should the
    /// primary experience failure, one of the hot standbys will be assigned to
    /// take over as the new shard primary, which is accomplished by simply opening
    /// its local copy of the recovered store files.
    ///
    /// Note that under regular operation, shard hand-off is zero downtime even if
    /// standbys are zero, as the current primary will not cede ownership until the
    /// replacement process declares itself ready. However, without standbys a
    /// process failure will leave the shard without an active primary while its
    /// replacement starts and completes playback of its recovery log.
    #[prost(uint32, tag="9")]
    pub hot_standbys: u32,
    /// User-defined Labels of this ShardSpec. The label "id" is reserved and may
    /// not be used with a ShardSpec's labels.
    #[prost(message, optional, tag="10")]
    pub labels: ::core::option::Option<super::protocol::LabelSet>,
    /// Disable waiting for acknowledgements of pending message(s).
    ///
    /// If a consumer transaction reads uncommitted messages, it will by default
    /// remain open (subject to the max duration) awaiting an acknowledgement of
    /// those messages, in the hope that that acknowledgement will be quickly
    /// forthcoming and, by remaining open, we can process all messages in this
    /// transaction. Effectively we're trading a small amount of increased local
    /// latency for a global reduction in end-to-end latency.
    ///
    /// This works well for acyclic message flows, but can introduce unnecessary
    /// stalls if there are message cycles between shards. In the simplest case,
    /// a transaction could block awaiting an ACK of a message that it itself
    /// produced -- an ACK which can't arrive until the transaction closes.
    #[prost(bool, tag="11")]
    pub disable_wait_for_ack: bool,
    /// Size of the ring buffer used to sequence read-uncommitted messages
    /// into consumed, read-committed ones. The ring buffer is a performance
    /// optimization only: applications will replay portions of journals as
    /// needed when messages aren't available in the buffer.
    /// It can remain small if source journal transactions are small,
    /// but larger transactions will achieve better performance with a
    /// larger ring.
    /// If zero, a reasonable default (currently 8192) is used.
    #[prost(uint32, tag="12")]
    pub ring_buffer_size: u32,
    /// Size of the channel used to bridge message read and decode with
    /// sequencing and consumption. Larger values may reduce data stalls,
    /// particularly for larger transactions and/or bursty custom
    /// MessageProducer implementations.
    /// If zero, a reasonable default (currently 8192) is used.
    #[prost(uint32, tag="13")]
    pub read_channel_size: u32,
}
/// Nested message and enum types in `ShardSpec`.
pub mod shard_spec {
    /// Sources define the set of journals which this shard consumes. At least one
    /// Source must be specified, and in many use cases only one will be needed.
    /// For use cases which can benefit, multiple sources may be specified to
    /// represent a "join" over messages of distinct journals.
    ///
    /// Note the effective mapping of messages to each of the joined journals
    /// should align (eg, joining a journal of customer updates with one of orders,
    /// where both are mapped on customer ID). This typically means the
    /// partitioning of the two event "topics" must be the same.
    ///
    /// Another powerful pattern is to shard on partitions of a high-volume event
    /// stream, and also have each shard join against all events of a low-volume
    /// stream. For example, a shard might ingest and index "viewed product"
    /// events, read a comparably low-volume "purchase" event stream, and on each
    /// purchase publish the bundle of its corresponding prior product views.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Source {
        /// Journal which this shard is consuming.
        #[prost(string, tag="1")]
        pub journal: ::prost::alloc::string::String,
        /// Minimum journal byte offset the shard should begin reading from.
        /// Typically this should be zero, as read offsets are check-pointed and
        /// restored from the shard's Store as it processes. |min_offset| can be
        /// useful for shard initialization, directing it to skip over historical
        /// portions of the journal not needed for the application's use case.
        #[prost(int64, tag="3")]
        pub min_offset: i64,
    }
}
/// ConsumerSpec describes a Consumer process instance and its configuration.
/// It serves as a allocator MemberValue.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConsumerSpec {
    /// ProcessSpec of the consumer.
    #[prost(message, optional, tag="1")]
    pub process_spec: ::core::option::Option<super::protocol::ProcessSpec>,
    /// Maximum number of assigned Shards.
    #[prost(uint32, tag="2")]
    pub shard_limit: u32,
}
/// ReplicaStatus is the status of a ShardSpec assigned to a ConsumerSpec.
/// It serves as an allocator AssignmentValue. ReplicaStatus is reduced by taking
/// the maximum enum value among statuses. Eg, if a primary is PRIMARY, one
/// replica is BACKFILL and the other STANDBY, then the status is PRIMARY. If one
/// of the replicas transitioned to FAILED, than the status is FAILED. This
/// reduction behavior is used to summarize status across all replicas.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReplicaStatus {
    #[prost(enumeration="replica_status::Code", tag="1")]
    pub code: i32,
    /// Errors encountered during replica processing. Set iff |code| is FAILED.
    #[prost(string, repeated, tag="2")]
    pub errors: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
/// Nested message and enum types in `ReplicaStatus`.
pub mod replica_status {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Code {
        Idle = 0,
        /// The replica is actively playing the historical recovery log.
        Backfill = 100,
        /// The replica has finished playing the historical recovery log and is
        /// live-tailing it to locally mirror recorded operations as they are
        /// produced. It can take over as primary at any time.
        ///
        /// Shards not having recovery logs immediately transition to STANDBY.
        Standby = 200,
        /// The replica is actively serving as primary.
        Primary = 300,
        /// The replica has encountered an unrecoverable error.
        Failed = 400,
    }
    impl Code {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Code::Idle => "IDLE",
                Code::Backfill => "BACKFILL",
                Code::Standby => "STANDBY",
                Code::Primary => "PRIMARY",
                Code::Failed => "FAILED",
            }
        }
    }
}
/// Checkpoint is processing metadata of a consumer shard which allows for its
/// recovery on fault.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Checkpoint {
    /// Sources is metadata of journals consumed by the shard.
    #[prost(map="string, message", tag="1")]
    pub sources: ::std::collections::HashMap<::prost::alloc::string::String, checkpoint::Source>,
    /// AckIntents is acknowledgement intents to be written to journals to which
    /// uncommitted messages were published during the transaction which produced
    /// this Checkpoint.
    #[prost(map="string, bytes", tag="2")]
    pub ack_intents: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::vec::Vec<u8>>,
}
/// Nested message and enum types in `Checkpoint`.
pub mod checkpoint {
    /// Source is metadata of a consumed source journal.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Source {
        /// Offset of the journal which has been read-through.
        #[prost(int64, tag="1")]
        pub read_through: i64,
        #[prost(message, repeated, tag="2")]
        pub producers: ::prost::alloc::vec::Vec<source::ProducerEntry>,
    }
    /// Nested message and enum types in `Source`.
    pub mod source {
        /// States of journal producers. Producer keys are 6-byte,
        /// RFC 4122 v1 node identifiers (see message.ProducerID).
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct ProducerEntry {
            #[prost(bytes="vec", tag="1")]
            pub id: ::prost::alloc::vec::Vec<u8>,
            #[prost(message, optional, tag="2")]
            pub state: ::core::option::Option<super::ProducerState>,
        }
    }
    /// ProducerState is metadata of a producer as-of a read-through journal
    /// offset.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ProducerState {
        /// LastAck is the last acknowledged Clock of this producer.
        #[prost(fixed64, tag="1")]
        pub last_ack: u64,
        /// Begin is the offset of the first message byte having CONTINUE_TXN that's
        /// larger than LastAck. Eg, it's the offset which opens the next
        /// transaction. If there is no such message, Begin is -1.
        #[prost(int64, tag="2")]
        pub begin: i64,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListRequest {
    /// Selector optionally refines the set of shards which will be enumerated.
    /// If zero-valued, all shards are returned. Otherwise, only ShardSpecs
    /// matching the LabelSelector will be returned. One meta-label "id" is
    /// additionally supported by the selector, where "id=example-shard-ID"
    /// will match a ShardSpec with ID "example-shard-ID".
    #[prost(message, optional, tag="1")]
    pub selector: ::core::option::Option<super::protocol::LabelSelector>,
    /// Optional extension of the ListRequest.
    #[prost(bytes="vec", tag="100")]
    pub extension: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListResponse {
    /// Status of the List RPC.
    #[prost(enumeration="Status", tag="1")]
    pub status: i32,
    /// Header of the response.
    #[prost(message, optional, tag="2")]
    pub header: ::core::option::Option<super::protocol::Header>,
    #[prost(message, repeated, tag="3")]
    pub shards: ::prost::alloc::vec::Vec<list_response::Shard>,
    /// Optional extension of the ListResponse.
    #[prost(bytes="vec", tag="100")]
    pub extension: ::prost::alloc::vec::Vec<u8>,
}
/// Nested message and enum types in `ListResponse`.
pub mod list_response {
    /// Shards of the response.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Shard {
        #[prost(message, optional, tag="1")]
        pub spec: ::core::option::Option<super::ShardSpec>,
        /// Current ModRevision of the ShardSpec.
        #[prost(int64, tag="2")]
        pub mod_revision: i64,
        /// Route of the shard, including endpoints.
        #[prost(message, optional, tag="3")]
        pub route: ::core::option::Option<super::super::protocol::Route>,
        /// Status of each replica. Cardinality and ordering matches |route|.
        #[prost(message, repeated, tag="4")]
        pub status: ::prost::alloc::vec::Vec<super::ReplicaStatus>,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ApplyRequest {
    #[prost(message, repeated, tag="1")]
    pub changes: ::prost::alloc::vec::Vec<apply_request::Change>,
    /// Optional extension of the ApplyRequest.
    #[prost(bytes="vec", tag="100")]
    pub extension: ::prost::alloc::vec::Vec<u8>,
}
/// Nested message and enum types in `ApplyRequest`.
pub mod apply_request {
    /// Change defines an insertion, update, or deletion to be applied to the set
    /// of ShardSpecs. Exactly one of |upsert| or |delete| must be set.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Change {
        /// Expected ModRevision of the current ShardSpec. If the shard is being
        /// created, expect_mod_revision is zero.
        #[prost(int64, tag="1")]
        pub expect_mod_revision: i64,
        /// ShardSpec to be updated (if expect_mod_revision > 0) or created
        /// (if expect_mod_revision == 0).
        #[prost(message, optional, tag="2")]
        pub upsert: ::core::option::Option<super::ShardSpec>,
        /// Shard to be deleted. expect_mod_revision must not be zero.
        #[prost(string, tag="3")]
        pub delete: ::prost::alloc::string::String,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ApplyResponse {
    /// Status of the Apply RPC.
    #[prost(enumeration="Status", tag="1")]
    pub status: i32,
    /// Header of the response.
    #[prost(message, optional, tag="2")]
    pub header: ::core::option::Option<super::protocol::Header>,
    /// Optional extension of the ApplyResponse.
    #[prost(bytes="vec", tag="100")]
    pub extension: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StatRequest {
    /// Header may be attached by a proxying consumer peer.
    #[prost(message, optional, tag="1")]
    pub header: ::core::option::Option<super::protocol::Header>,
    /// Shard to Stat.
    #[prost(string, tag="2")]
    pub shard: ::prost::alloc::string::String,
    /// Journals and offsets which must be reflected in a completed consumer
    /// transaction before Stat returns, blocking if required. Offsets of journals
    /// not read by this shard are ignored.
    #[prost(map="string, int64", tag="3")]
    pub read_through: ::std::collections::HashMap<::prost::alloc::string::String, i64>,
    /// Optional extension of the StatRequest.
    #[prost(bytes="vec", tag="100")]
    pub extension: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StatResponse {
    /// Status of the Stat RPC.
    #[prost(enumeration="Status", tag="1")]
    pub status: i32,
    /// Header of the response.
    #[prost(message, optional, tag="2")]
    pub header: ::core::option::Option<super::protocol::Header>,
    /// Journals and offsets read through by the most recent completed consumer
    /// transaction.
    #[prost(map="string, int64", tag="3")]
    pub read_through: ::std::collections::HashMap<::prost::alloc::string::String, i64>,
    /// Journals and offsets this shard has published through, including
    /// acknowledgements, as-of the most recent completed consumer transaction.
    ///
    /// Formally, if an acknowledged message A results in this shard publishing
    /// messages B, and A falls within |read_through|, then all messages B & their
    /// acknowledgements fall within |publish_at|.
    ///
    /// The composition of |read_through| and |publish_at| allow CQRS applications
    /// to provide read-your-writes consistency, even if written events pass
    /// through multiple intermediate consumers and arbitrary transformations
    /// before arriving at the materialized view which is ultimately queried.
    #[prost(map="string, int64", tag="4")]
    pub publish_at: ::std::collections::HashMap<::prost::alloc::string::String, i64>,
    /// Optional extension of the StatResponse.
    #[prost(bytes="vec", tag="100")]
    pub extension: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetHintsRequest {
    /// Shard to fetch hints for.
    #[prost(string, tag="1")]
    pub shard: ::prost::alloc::string::String,
    /// Optional extension of the GetHintsRequest.
    #[prost(bytes="vec", tag="100")]
    pub extension: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetHintsResponse {
    /// Status of the Hints RPC.
    #[prost(enumeration="Status", tag="1")]
    pub status: i32,
    /// Header of the response.
    #[prost(message, optional, tag="2")]
    pub header: ::core::option::Option<super::protocol::Header>,
    /// Primary hints for the shard.
    #[prost(message, optional, tag="3")]
    pub primary_hints: ::core::option::Option<get_hints_response::ResponseHints>,
    /// List of backup hints for a shard. The most recent recovery log hints will
    /// be first, any subsequent hints are for historical backup. If there is no
    /// value for a hint key the value corresponding hints will be nil.
    #[prost(message, repeated, tag="4")]
    pub backup_hints: ::prost::alloc::vec::Vec<get_hints_response::ResponseHints>,
    /// Optional extension of the GetHintsResponse.
    #[prost(bytes="vec", tag="100")]
    pub extension: ::prost::alloc::vec::Vec<u8>,
}
/// Nested message and enum types in `GetHintsResponse`.
pub mod get_hints_response {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ResponseHints {
        /// If the hints value does not exist Hints will be nil.
        #[prost(message, optional, tag="1")]
        pub hints: ::core::option::Option<super::super::recoverylog::FsmHints>,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UnassignRequest {
    /// Shards to unassign.
    #[prost(string, repeated, tag="1")]
    pub shards: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Only unassign shards which have a primary in FAILED status.
    #[prost(bool, tag="2")]
    pub only_failed: bool,
    /// Avoids actually removing any shard assignments, but the response will
    /// report which shards would have been affected.
    #[prost(bool, tag="3")]
    pub dry_run: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UnassignResponse {
    /// Status of the Unassign RPC.
    #[prost(enumeration="Status", tag="1")]
    pub status: i32,
    /// Shards which had assignments removed.
    #[prost(string, repeated, tag="2")]
    pub shards: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
/// Status is a response status code, used across Gazette Consumer RPC APIs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Status {
    Ok = 0,
    /// The named shard does not exist.
    ShardNotFound = 1,
    /// There is no current primary consumer process for the shard. This is a
    /// temporary condition which should quickly resolve, assuming sufficient
    /// consumer capacity.
    NoShardPrimary = 2,
    /// The present consumer process is not the assigned primary for the shard,
    /// and was not instructed to proxy the request.
    NotShardPrimary = 3,
    /// The Etcd transaction failed. Returned by Update RPC when an
    /// expect_mod_revision of the UpdateRequest differs from the current
    /// ModRevision of the ShardSpec within the store.
    EtcdTransactionFailed = 4,
    /// The current primary shard has stopped, either due to reassignment or
    /// processing failure, and will not make further progress toward the
    /// requested operation.
    /// For example, a Stat RPC will return SHARD_STOPPED if the StatRequest
    /// cannot be satisfied.
    ShardStopped = 5,
}
impl Status {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Status::Ok => "OK",
            Status::ShardNotFound => "SHARD_NOT_FOUND",
            Status::NoShardPrimary => "NO_SHARD_PRIMARY",
            Status::NotShardPrimary => "NOT_SHARD_PRIMARY",
            Status::EtcdTransactionFailed => "ETCD_TRANSACTION_FAILED",
            Status::ShardStopped => "SHARD_STOPPED",
        }
    }
}
