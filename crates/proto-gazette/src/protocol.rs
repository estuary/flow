/// Label defines a key & value pair which can be attached to entities like
/// JournalSpecs and BrokerSpecs. Labels may be used to provide identifying
/// attributes which do not directly imply semantics to the core system, but
/// are meaningful to users or for higher-level Gazette tools.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Label {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub value: ::prost::alloc::string::String,
}
/// LabelSet is a collection of labels and their values.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LabelSet {
    /// Labels of the set. Instances must be unique and sorted over (Name, Value).
    #[prost(message, repeated, tag = "1")]
    pub labels: ::prost::alloc::vec::Vec<Label>,
}
/// LabelSelector defines a filter over LabelSets.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LabelSelector {
    /// Include is Labels which must be matched for a LabelSet to be selected. If
    /// empty, all Labels are included. An include Label with empty ("") value is
    /// matched by a Label of the same name having any value.
    #[prost(message, optional, tag = "1")]
    pub include: ::core::option::Option<LabelSet>,
    /// Exclude is Labels which cannot be matched for a LabelSet to be selected. If
    /// empty, no Labels are excluded. An exclude Label with empty ("") value
    /// excludes a Label of the same name having any value.
    #[prost(message, optional, tag = "2")]
    pub exclude: ::core::option::Option<LabelSet>,
}
/// JournalSpec describes a Journal and its configuration.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct JournalSpec {
    /// Name of the Journal.
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    /// Desired replication of this Journal. This defines the Journal's tolerance
    /// to broker failures before data loss can occur (eg, a replication factor
    /// of three means two failures are tolerated).
    #[prost(int32, tag = "2")]
    pub replication: i32,
    /// User-defined Labels of this JournalSpec. Two label names are reserved
    /// and may not be used within a JournalSpec's Labels: "name" and "prefix".
    #[prost(message, optional, tag = "3")]
    pub labels: ::core::option::Option<LabelSet>,
    #[prost(message, optional, tag = "4")]
    pub fragment: ::core::option::Option<journal_spec::Fragment>,
    /// Flags of the Journal, as a combination of Flag enum values. The Flag enum
    /// is not used directly, as protobuf enums do not allow for or'ed bitfields.
    #[prost(uint32, tag = "6")]
    pub flags: u32,
    /// Maximum rate, in bytes-per-second, at which appends of this journal will
    /// be processed. If zero (the default), no rate limiting is applied. A global
    /// rate limit still may be in effect, in which case the effective rate is the
    /// smaller of the journal vs global rate.
    #[prost(int64, tag = "7")]
    pub max_append_rate: i64,
}
/// Nested message and enum types in `JournalSpec`.
pub mod journal_spec {
    /// Fragment is JournalSpec configuration which pertains to the creation,
    /// persistence, and indexing of the Journal's Fragments.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Fragment {
        /// Target content length of each Fragment. In normal operation after
        /// Fragments reach at least this length, they will be closed and new ones
        /// begun. Note lengths may be smaller at times (eg, due to changes in
        /// Journal routing topology). Content length differs from Fragment file
        /// size, in that the former reflects uncompressed bytes.
        #[prost(int64, tag = "1")]
        pub length: i64,
        /// Codec used to compress Journal Fragments.
        #[prost(enumeration = "super::CompressionCodec", tag = "2")]
        pub compression_codec: i32,
        /// Storage backend base path for this Journal's Fragments. Must be in URL
        /// form, with the choice of backend defined by the scheme. The full path of
        /// a Journal's Fragment is derived by joining the store path with the
        /// Fragment's ContentPath. Eg, given a fragment_store of
        ///    "s3://My-AWS-bucket/a/prefix" and a JournalSpec of name "my/journal",
        /// a complete Fragment path might be:
        ///    "s3://My-AWS-bucket/a/prefix/my/journal/000123-000456-789abcdef.gzip
        ///
        /// Multiple stores may be specified, in which case the Journal's Fragments
        /// are the union of all Fragments present across all stores, and new
        /// Fragments always persist to the first specified store. This can be
        /// helpful in performing incremental migrations, where new Journal content
        /// is written to the new store, while content in the old store remains
        /// available (and, depending on fragment_retention or recovery log pruning,
        /// may eventually be removed).
        ///
        /// If no stores are specified, the Journal is still use-able but will
        /// not persist Fragments to any a backing fragment store. This allows for
        /// real-time streaming use cases where reads of historical data are not
        /// needed.
        #[prost(string, repeated, tag = "3")]
        pub stores: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
        /// Interval of time between refreshes of remote Fragment listings from
        /// configured fragment_stores.
        #[prost(message, optional, tag = "4")]
        pub refresh_interval: ::core::option::Option<::pbjson_types::Duration>,
        /// Retention duration for historical Fragments of this Journal within the
        /// Fragment stores. If less than or equal to zero, Fragments are retained
        /// indefinitely.
        #[prost(message, optional, tag = "5")]
        pub retention: ::core::option::Option<::pbjson_types::Duration>,
        /// Flush interval defines a uniform UTC time segment which, when passed,
        /// will prompt brokers to close and persist a fragment presently being
        /// written.
        ///
        /// Flush interval may be helpful in integrating the journal with a regularly
        /// scheduled batch work-flow which processes new files from the fragment
        /// store and has no particular awareness of Gazette. For example, setting
        /// flush_interval to 3600s will cause brokers to persist their present
        /// fragment on the hour, every hour, even if it has not yet reached its
        /// target length. A batch work-flow running at 5 minutes past the hour is
        /// then reasonably assured of seeing all events from the past hour.
        ///
        /// See also "gazctl journals fragments --help" for more discussion.
        #[prost(message, optional, tag = "6")]
        pub flush_interval: ::core::option::Option<::pbjson_types::Duration>,
        /// Path postfix template is a Go template which evaluates to a partial
        /// path under which fragments are persisted to the store. A complete
        /// fragment path is constructed by appending path components from the
        /// fragment store, then the journal name, and then the postfix template.
        /// Path post-fixes can help in maintaining Hive compatible partitioning
        /// over fragment creation time. The fields ".Spool" and ".JournalSpec"
        /// are available for introspection in the template. For example,
        /// to partition on the UTC date and hour of creation, use:
        ///
        ///     date={{ .Spool.FirstAppendTime.Format "2006-01-02" }}/hour={{
        ///     .Spool.FirstAppendTime.Format "15" }}
        ///
        /// Which will produce a path postfix like "date=2019-11-19/hour=22".
        #[prost(string, tag = "7")]
        pub path_postfix_template: ::prost::alloc::string::String,
    }
    /// Flags define Journal IO control behaviors. Where possible, flags are named
    /// after an equivalent POSIX flag.
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
    pub enum Flag {
        /// NOT_SPECIFIED is considered as equivalent to O_RDWR by the broker. When
        /// JournalSpecs are union'ed (eg, by the `journalspace` pkg), NOT_SPECIFIED
        /// is considered as unset relative to any other non-zero Flag value.
        NotSpecified = 0,
        /// The Journal is available for reads (only).
        ORdonly = 1,
        /// The Journal is available for writes (only).
        OWronly = 2,
        /// The Journal may be used for reads or writes.
        ORdwr = 4,
    }
    impl Flag {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Flag::NotSpecified => "NOT_SPECIFIED",
                Flag::ORdonly => "O_RDONLY",
                Flag::OWronly => "O_WRONLY",
                Flag::ORdwr => "O_RDWR",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "NOT_SPECIFIED" => Some(Self::NotSpecified),
                "O_RDONLY" => Some(Self::ORdonly),
                "O_WRONLY" => Some(Self::OWronly),
                "O_RDWR" => Some(Self::ORdwr),
                _ => None,
            }
        }
    }
}
/// ProcessSpec describes a uniquely identified process and its addressable
/// endpoint.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProcessSpec {
    #[prost(message, optional, tag = "1")]
    pub id: ::core::option::Option<process_spec::Id>,
    /// Advertised URL of the process.
    #[prost(string, tag = "2")]
    pub endpoint: ::prost::alloc::string::String,
}
/// Nested message and enum types in `ProcessSpec`.
pub mod process_spec {
    /// ID composes a zone and a suffix to uniquely identify a ProcessSpec.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Id {
        /// "Zone" in which the process is running. Zones may be AWS, Azure, or
        /// Google Cloud Platform zone identifiers, or rack locations within a colo,
        /// or given some other custom meaning. Gazette will replicate across
        /// multiple zones, and seeks to minimize traffic which must cross zones (for
        /// example, by proxying reads to a broker in the current zone).
        #[prost(string, tag = "1")]
        pub zone: ::prost::alloc::string::String,
        /// Unique suffix of the process within |zone|. It is permissible for a
        /// suffix value to repeat across zones, but never within zones. In practice,
        /// it's recommended to use a FQDN, Kubernetes Pod name, or comparable unique
        /// and self-describing value as the ID suffix.
        #[prost(string, tag = "2")]
        pub suffix: ::prost::alloc::string::String,
    }
}
/// BrokerSpec describes a Gazette broker and its configuration.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BrokerSpec {
    /// ProcessSpec of the broker.
    #[prost(message, optional, tag = "1")]
    pub process_spec: ::core::option::Option<ProcessSpec>,
    /// Maximum number of assigned Journal replicas.
    #[prost(uint32, tag = "2")]
    pub journal_limit: u32,
}
/// Fragment is a content-addressed description of a contiguous Journal span,
/// defined by the [begin, end) offset range covered by the Fragment and the
/// SHA1 sum of the corresponding Journal content.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Fragment {
    /// Journal of the Fragment.
    #[prost(string, tag = "1")]
    pub journal: ::prost::alloc::string::String,
    /// Begin (inclusive) and end (exclusive) offset of the Fragment within the
    /// Journal.
    #[prost(int64, tag = "2")]
    pub begin: i64,
    #[prost(int64, tag = "3")]
    pub end: i64,
    /// SHA1 sum of the Fragment's content.
    #[prost(message, optional, tag = "4")]
    pub sum: ::core::option::Option<Sha1Sum>,
    /// Codec with which the Fragment's content is compressed.
    #[prost(enumeration = "CompressionCodec", tag = "5")]
    pub compression_codec: i32,
    /// Fragment store which backs the Fragment. Empty if the Fragment has yet to
    /// be persisted and is still local to a Broker.
    #[prost(string, tag = "6")]
    pub backing_store: ::prost::alloc::string::String,
    /// Modification timestamp of the Fragment within the backing store,
    /// represented as seconds since the epoch.
    #[prost(int64, tag = "7")]
    pub mod_time: i64,
    /// Path postfix under which the fragment is persisted to the store.
    /// The complete Fragment store path is built from any path components of the
    /// backing store, followed by the journal name, followed by the path postfix.
    #[prost(string, tag = "8")]
    pub path_postfix: ::prost::alloc::string::String,
}
/// SHA1Sum is a 160-bit SHA1 digest.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Sha1Sum {
    #[prost(fixed64, tag = "1")]
    pub part1: u64,
    #[prost(fixed64, tag = "2")]
    pub part2: u64,
    #[prost(fixed32, tag = "3")]
    pub part3: u32,
}
/// ReadRequest is the unary request message of the broker Read RPC.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReadRequest {
    /// Header is attached by a proxying broker peer.
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<Header>,
    /// Journal to be read.
    #[prost(string, tag = "2")]
    pub journal: ::prost::alloc::string::String,
    /// Desired offset to begin reading from. Value -1 has special handling, where
    /// the read is performed from the current write head. All other positive
    /// values specify a desired exact byte offset to read from. If the offset is
    /// not available (eg, because it represents a portion of Journal which has
    /// been permanently deleted), the broker will return the next available
    /// offset. Callers should therefore always inspect the ReadResponse offset.
    #[prost(int64, tag = "3")]
    pub offset: i64,
    /// Whether the operation should block until content becomes available.
    /// OFFSET_NOT_YET_AVAILABLE is returned if a non-blocking read has no ready
    /// content.
    #[prost(bool, tag = "4")]
    pub block: bool,
    /// If do_not_proxy is true, the broker will not proxy the read to another
    /// broker, or open and proxy a remote Fragment on the client's behalf.
    #[prost(bool, tag = "5")]
    pub do_not_proxy: bool,
    /// If metadata_only is true, the broker will respond with Journal and
    /// Fragment metadata but not content.
    #[prost(bool, tag = "6")]
    pub metadata_only: bool,
    /// Offset to read through. If zero, then the read end offset is unconstrained.
    #[prost(int64, tag = "7")]
    pub end_offset: i64,
}
/// ReadResponse is the streamed response message of the broker Read RPC.
/// Responses messages are of two types:
///
/// * "Metadata" messages, which conveys the journal Fragment addressed by the
///     request which is ready to be read.
/// * "Chunk" messages, which carry associated journal Fragment content bytes.
///
/// A metadata message specifying a Fragment always precedes all "chunks" of the
/// Fragment's content. Response streams may be very long lived, having many
/// metadata and accompanying chunk messages. The reader may also block for long
/// periods of time awaiting the next metadata message (eg, if the next offset
/// hasn't yet committed). However once a metadata message is read, the reader
/// is assured that its associated chunk messages are immediately forthcoming.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReadResponse {
    /// Status of the Read RPC.
    #[prost(enumeration = "Status", tag = "1")]
    pub status: i32,
    /// Header of the response. Accompanies the first ReadResponse of the response
    /// stream.
    #[prost(message, optional, tag = "2")]
    pub header: ::core::option::Option<Header>,
    /// The effective offset of the read. See ReadRequest offset.
    #[prost(int64, tag = "3")]
    pub offset: i64,
    /// The offset to next be written, by the next append transaction served by
    /// broker. In other words, the last offset through which content is
    /// available to be read from the Journal. This is a metadata field and will
    /// not be returned with a content response.
    #[prost(int64, tag = "4")]
    pub write_head: i64,
    /// Fragment to which the offset was mapped. This is a metadata field and will
    /// not be returned with a content response.
    #[prost(message, optional, tag = "5")]
    pub fragment: ::core::option::Option<Fragment>,
    /// If Fragment is remote, a URL from which it may be directly read.
    #[prost(string, tag = "6")]
    pub fragment_url: ::prost::alloc::string::String,
    /// Content chunks of the read.
    #[prost(bytes = "vec", tag = "7")]
    pub content: ::prost::alloc::vec::Vec<u8>,
}
/// AppendRequest is the streamed request message of the broker Append RPC.
/// Append request streams consist of an initial message having all parameters
/// of the append, such as the journal to append to and preconditions, followed
/// by an unbounded number of messages having only content "chunks".
///
/// It's not required that the appender know the append size when starting the
/// Append RPC -- rather, the client indicates the stream is complete by sending
/// a final, empty "chunk" message. However be aware that the broker will
/// aggressively time out stalled Append clients, and clients should not start
/// RPCs until all content chunks are available for immediate writing.
///
/// Append RPCs also expose a concept of journal "registers": LabelSets
/// which participate in the journal's transactional append machinery.
/// Note that registers are sent and verified with every replicated journal
/// transaction, so they're _really_ intended to be very small.
///
/// Append RPCs may upsert (union) or delete (subtract) labels from the
/// journal's registers. Register consensus is achieved by piggy-backing on the
/// append itself: if peers disagree, the registers of the replica having the
/// largest journal byte offset always win. For this reason, only RPCs appending
/// at least one byte may modify registers.
///
/// Append RPCs may also require that registers match an arbitrary selector
/// before the RPC may proceed. For example, a write fence can be implemented
/// by requiring that a "author" register is of an expected value. At-most-once
/// semantics can be implemented as a check-and-set over a single register.
///
/// Also be aware that a register update can still occur even for RPCs which are
/// reported as failed to the client. That's because an append RPC succeeds
/// only after all replicas acknowledge it, but a RPC which applies to some
/// replicas but not all still moves the journal offset forward, and therefore
/// updates journal registers.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AppendRequest {
    /// Header is attached by a proxying broker peer to the first AppendRequest
    /// message.
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<Header>,
    /// Journal to be appended to.
    #[prost(string, tag = "2")]
    pub journal: ::prost::alloc::string::String,
    /// If do_not_proxy is true, the broker will not proxy the append if it is
    /// not the current primary.
    #[prost(bool, tag = "3")]
    pub do_not_proxy: bool,
    /// Journal offset at which the append should begin. Most clients should leave
    /// at zero, which uses the broker's tracked offset. The append offset must be
    /// one greater than furthest written offset of the journal, or
    /// WRONG_APPEND_OFFSET is returned.
    #[prost(int64, tag = "5")]
    pub offset: i64,
    /// Selector of journal registers which must be satisfied for the request
    /// to proceed. If not matched, the RPC is failed with REGISTER_MISMATCH.
    ///
    /// There's one important exception: if the set of registers associated with
    /// a journal is completely empty, then *any* selector is considered as
    /// matching. While perhaps surprising, this behavior supports the intended
    /// use of registers for cooperative locking, whereby an empty set of
    /// registers can be thought of as an "unlocked" state. More practically, if
    /// Etcd consensus is lost then so are current register values: on recovery
    /// journals will restart with an empty set. This behavior ensures that an
    /// existing process holding a prior lock can continue to write -- at least
    /// until another process updates registers once again.
    #[prost(message, optional, tag = "6")]
    pub check_registers: ::core::option::Option<LabelSelector>,
    /// Labels to union with current registers if the RPC succeeds and appended
    /// at least one byte.
    #[prost(message, optional, tag = "7")]
    pub union_registers: ::core::option::Option<LabelSet>,
    /// Labels to subtract from current registers if the RPC succeeds and appended
    /// at least one byte.
    #[prost(message, optional, tag = "8")]
    pub subtract_registers: ::core::option::Option<LabelSet>,
    /// Content chunks to be appended. Immediately prior to closing the stream,
    /// the client must send an empty chunk (eg, zero-valued AppendRequest) to
    /// indicate the Append should be committed. Absence of this empty chunk
    /// prior to EOF is interpreted by the broker as a rollback of the Append.
    #[prost(bytes = "vec", tag = "4")]
    pub content: ::prost::alloc::vec::Vec<u8>,
}
/// AppendResponse is the unary response message of the broker Append RPC.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AppendResponse {
    /// Status of the Append RPC.
    #[prost(enumeration = "Status", tag = "1")]
    pub status: i32,
    /// Header of the response.
    #[prost(message, optional, tag = "2")]
    pub header: ::core::option::Option<Header>,
    /// If status is OK, then |commit| is the Fragment which places the
    /// committed Append content within the Journal.
    #[prost(message, optional, tag = "3")]
    pub commit: ::core::option::Option<Fragment>,
    /// Current registers of the journal.
    #[prost(message, optional, tag = "4")]
    pub registers: ::core::option::Option<LabelSet>,
    /// Total number of RPC content chunks processed in this append.
    #[prost(int64, tag = "5")]
    pub total_chunks: i64,
    /// Number of content chunks which were delayed by journal flow control.
    #[prost(int64, tag = "6")]
    pub delayed_chunks: i64,
}
/// ReplicateRequest is the streamed request message of the broker's internal
/// Replicate RPC. Each message is either a pending content chunk or a
/// "proposal" to commit (or roll back) content chunks previously sent.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReplicateRequest {
    /// Header defines the primary broker, Route, and Etcd Revision under which
    /// this Replicate stream is being established. Each replication peer
    /// independently inspects and verifies the current Journal Route topology.
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<Header>,
    /// Proposed Fragment to commit, which is verified by each replica.
    #[prost(message, optional, tag = "3")]
    pub proposal: ::core::option::Option<Fragment>,
    /// Registers proposed to apply, which are also verified by each replica.
    #[prost(message, optional, tag = "7")]
    pub registers: ::core::option::Option<LabelSet>,
    /// Acknowledge requests that the peer send an acknowledging ReplicateResponse
    /// on successful application of the ReplicateRequest.
    #[prost(bool, tag = "6")]
    pub acknowledge: bool,
    /// Journal to be replicated to, which is also captured by |proposal|.
    /// Deprecated.
    #[prost(string, tag = "2")]
    pub deprecated_journal: ::prost::alloc::string::String,
    /// Content to be replicated.
    #[prost(bytes = "vec", tag = "4")]
    pub content: ::prost::alloc::vec::Vec<u8>,
    /// Delta offset of |content| relative to current Fragment |end|.
    #[prost(int64, tag = "5")]
    pub content_delta: i64,
}
/// ReplicateResponse is the streamed response message of the broker's internal
/// Replicate RPC. Each message is a 1:1 response to a previously read "proposal"
/// ReplicateRequest with |acknowledge| set.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReplicateResponse {
    /// Status of the Replicate RPC.
    #[prost(enumeration = "Status", tag = "1")]
    pub status: i32,
    /// Header of the response. Accompanies the first ReplicateResponse of the
    /// response stream.
    #[prost(message, optional, tag = "2")]
    pub header: ::core::option::Option<Header>,
    /// If status is PROPOSAL_MISMATCH, then |fragment| is the replica's current
    /// journal Fragment, and either it or |registers| will differ from the
    /// primary's proposal.
    #[prost(message, optional, tag = "3")]
    pub fragment: ::core::option::Option<Fragment>,
    /// If status is PROPOSAL_MISMATCH, then |registers| are the replica's current
    /// journal registers.
    #[prost(message, optional, tag = "4")]
    pub registers: ::core::option::Option<LabelSet>,
}
/// ListRequest is the unary request message of the broker List RPC.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListRequest {
    /// Selector optionally refines the set of journals which will be enumerated.
    /// If zero-valued, all journals are returned. Otherwise, only JournalSpecs
    /// matching the LabelSelector will be returned. Two meta-labels "name" and
    /// "prefix" are additionally supported by the selector, where:
    ///    * name=examples/a-name will match a JournalSpec with Name
    ///    "examples/a-name"
    ///    * prefix=examples/ will match any JournalSpec having prefix "examples/".
    ///      The prefix Label value must end in '/'.
    #[prost(message, optional, tag = "1")]
    pub selector: ::core::option::Option<LabelSelector>,
}
/// ListResponse is the unary response message of the broker List RPC.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListResponse {
    /// Status of the List RPC.
    #[prost(enumeration = "Status", tag = "1")]
    pub status: i32,
    /// Header of the response.
    #[prost(message, optional, tag = "2")]
    pub header: ::core::option::Option<Header>,
    #[prost(message, repeated, tag = "3")]
    pub journals: ::prost::alloc::vec::Vec<list_response::Journal>,
}
/// Nested message and enum types in `ListResponse`.
pub mod list_response {
    /// Journals of the response.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Journal {
        #[prost(message, optional, tag = "1")]
        pub spec: ::core::option::Option<super::JournalSpec>,
        /// Current ModRevision of the JournalSpec.
        #[prost(int64, tag = "2")]
        pub mod_revision: i64,
        /// Route of the journal, including endpoints.
        #[prost(message, optional, tag = "3")]
        pub route: ::core::option::Option<super::Route>,
    }
}
/// ApplyRequest is the unary request message of the broker Apply RPC.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ApplyRequest {
    #[prost(message, repeated, tag = "1")]
    pub changes: ::prost::alloc::vec::Vec<apply_request::Change>,
}
/// Nested message and enum types in `ApplyRequest`.
pub mod apply_request {
    /// Change defines an insertion, update, or deletion to be applied to the set
    /// of JournalSpecs. Exactly one of |upsert| or |delete| must be set.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Change {
        /// Expected ModRevision of the current JournalSpec. If the Journal is being
        /// created, expect_mod_revision is zero.
        #[prost(int64, tag = "1")]
        pub expect_mod_revision: i64,
        /// JournalSpec to be updated (if expect_mod_revision > 0) or created
        /// (if expect_mod_revision == 0).
        #[prost(message, optional, tag = "2")]
        pub upsert: ::core::option::Option<super::JournalSpec>,
        /// Journal to be deleted. expect_mod_revision must not be zero.
        #[prost(string, tag = "3")]
        pub delete: ::prost::alloc::string::String,
    }
}
/// ApplyResponse is the unary response message of the broker Apply RPC.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ApplyResponse {
    /// Status of the Apply RPC.
    #[prost(enumeration = "Status", tag = "1")]
    pub status: i32,
    /// Header of the response.
    #[prost(message, optional, tag = "2")]
    pub header: ::core::option::Option<Header>,
}
/// FragmentsRequest is the unary request message of the broker ListFragments
/// RPC.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FragmentsRequest {
    /// Header is attached by a proxying broker peer.
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<Header>,
    /// Journal to be read.
    #[prost(string, tag = "2")]
    pub journal: ::prost::alloc::string::String,
    /// BeginModTime is an optional field specifying an inclusive lower bound on
    /// the modification timestamp for a fragment to be returned. The timestamp is
    /// represented as seconds since the epoch.
    #[prost(int64, tag = "3")]
    pub begin_mod_time: i64,
    /// EndModTime is an optional field specifying an exclusive upper bound on
    /// the modification timestamp for a fragment to be returned. The timestamp is
    /// represented as seconds since the epoch.
    #[prost(int64, tag = "4")]
    pub end_mod_time: i64,
    /// The NextPageToke value returned from a previous, continued
    /// FragmentsRequest, if any.
    #[prost(int64, tag = "5")]
    pub next_page_token: i64,
    /// PageLimit is an optional field specifying how many fragments to return
    /// with the response. The default value for PageLimit is 1000.
    #[prost(int32, tag = "6")]
    pub page_limit: i32,
    /// SignatureTTL indicates that a temporary signed GET URL should be returned
    /// with each response Fragment, valid for |signatureTTL|.
    #[prost(message, optional, tag = "7")]
    pub signature_ttl: ::core::option::Option<::pbjson_types::Duration>,
    /// If do_not_proxy is true, the broker will not proxy the request to another
    /// broker on the client's behalf.
    #[prost(bool, tag = "8")]
    pub do_not_proxy: bool,
}
/// FragmentsResponse is the unary response message of the broker ListFragments
/// RPC.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FragmentsResponse {
    /// Status of the Apply RPC.
    #[prost(enumeration = "Status", tag = "1")]
    pub status: i32,
    /// Header of the response.
    #[prost(message, optional, tag = "2")]
    pub header: ::core::option::Option<Header>,
    #[prost(message, repeated, tag = "3")]
    pub fragments: ::prost::alloc::vec::Vec<fragments_response::Fragment>,
    /// The NextPageToke value to be returned on subsequent Fragments requests. If
    /// the value is zero then there are no more fragments to be returned for this
    /// page.
    #[prost(int64, tag = "4")]
    pub next_page_token: i64,
}
/// Nested message and enum types in `FragmentsResponse`.
pub mod fragments_response {
    /// Fragments of the Response.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Fragment {
        #[prost(message, optional, tag = "1")]
        pub spec: ::core::option::Option<super::Fragment>,
        /// SignedURL is a temporary URL at which a direct GET of the Fragment may
        /// be issued, signed by the broker's credentials. Set only if the request
        /// specified a SignatureTTL.
        #[prost(string, tag = "2")]
        pub signed_url: ::prost::alloc::string::String,
    }
}
/// Route captures the current topology of an item and the processes serving it.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Route {
    /// Members of the Route, ordered on ascending ProcessSpec.ID (zone, suffix).
    #[prost(message, repeated, tag = "1")]
    pub members: ::prost::alloc::vec::Vec<process_spec::Id>,
    /// Index of the ProcessSpec serving as primary within |members|,
    /// or -1 of no member is currently primary.
    #[prost(int32, tag = "2")]
    pub primary: i32,
    /// Endpoints of each Route member. If not empty, |endpoints| has the same
    /// length and order as |members|, and captures the endpoint of each one.
    #[prost(string, repeated, tag = "3")]
    pub endpoints: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
/// Header captures metadata such as the process responsible for processing
/// an RPC, and its effective Etcd state.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Header {
    /// ID of the process responsible for request processing. May be empty iff
    /// Header is being used within a proxied request, and that request may be
    /// dispatched to any member of the Route.
    #[prost(message, optional, tag = "1")]
    pub process_id: ::core::option::Option<process_spec::Id>,
    /// Route of processes specifically responsible for this RPC, or an empty Route
    /// if any process is capable of serving the RPC.
    #[prost(message, optional, tag = "2")]
    pub route: ::core::option::Option<Route>,
    #[prost(message, optional, tag = "3")]
    pub etcd: ::core::option::Option<header::Etcd>,
}
/// Nested message and enum types in `Header`.
pub mod header {
    /// Etcd represents the effective Etcd MVCC state under which a Gazette broker
    /// is operating in its processing of requests and responses. Its inclusion
    /// allows brokers to reason about relative "happened before" Revision ordering
    /// of apparent routing conflicts in proxied or replicated requests, as well
    /// as enabling sanity checks over equality of Etcd ClusterId (and precluding,
    /// for example, split-brain scenarios where different brokers are backed by
    /// different Etcd clusters). Etcd is kept in sync with
    /// etcdserverpb.ResponseHeader.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Etcd {
        /// cluster_id is the ID of the cluster.
        #[prost(uint64, tag = "1")]
        pub cluster_id: u64,
        /// member_id is the ID of the member.
        #[prost(uint64, tag = "2")]
        pub member_id: u64,
        /// revision is the Etcd key-value store revision when the request was
        /// applied.
        #[prost(int64, tag = "3")]
        pub revision: i64,
        /// raft_term is the raft term when the request was applied.
        #[prost(uint64, tag = "4")]
        pub raft_term: u64,
    }
}
/// Status is a response status code, used universally across Gazette RPC APIs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Status {
    Ok = 0,
    /// The named journal does not exist.
    JournalNotFound = 1,
    /// There is no current primary broker for the journal. This is a temporary
    /// condition which should quickly resolve, assuming sufficient broker
    /// capacity.
    NoJournalPrimaryBroker = 2,
    /// The present broker is not the assigned primary broker for the journal.
    NotJournalPrimaryBroker = 3,
    /// The present broker is not an assigned broker for the journal.
    NotJournalBroker = 5,
    /// There are an insufficient number of assigned brokers for the journal
    /// to meet its required replication.
    InsufficientJournalBrokers = 4,
    /// The requested offset is not yet available. This indicates either that the
    /// offset has not yet been written, or that the broker is not yet aware of a
    /// written fragment covering the offset. Returned only by non-blocking reads.
    OffsetNotYetAvailable = 6,
    /// The peer disagrees with the Route accompanying a ReplicateRequest.
    WrongRoute = 7,
    /// The peer disagrees with the proposal accompanying a ReplicateRequest.
    ProposalMismatch = 8,
    /// The Etcd transaction failed. Returned by Update RPC when an
    /// expect_mod_revision of the UpdateRequest differs from the current
    /// ModRevision of the JournalSpec within the store.
    EtcdTransactionFailed = 9,
    /// A disallowed journal access was attempted (eg, a write where the
    /// journal disables writes, or read where journals disable reads).
    NotAllowed = 10,
    /// The Append is refused because its requested offset is not equal
    /// to the furthest written offset of the journal.
    WrongAppendOffset = 11,
    /// The Append is refused because the replication pipeline tracks a smaller
    /// journal offset than that of the remote fragment index. This indicates
    /// that journal replication consistency has been lost in the past, due to
    /// too many broker or Etcd failures.
    IndexHasGreaterOffset = 12,
    /// The Append is refused because a registers selector was provided with the
    /// request, but it was not matched by current register values of the journal.
    RegisterMismatch = 13,
}
impl Status {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Status::Ok => "OK",
            Status::JournalNotFound => "JOURNAL_NOT_FOUND",
            Status::NoJournalPrimaryBroker => "NO_JOURNAL_PRIMARY_BROKER",
            Status::NotJournalPrimaryBroker => "NOT_JOURNAL_PRIMARY_BROKER",
            Status::NotJournalBroker => "NOT_JOURNAL_BROKER",
            Status::InsufficientJournalBrokers => "INSUFFICIENT_JOURNAL_BROKERS",
            Status::OffsetNotYetAvailable => "OFFSET_NOT_YET_AVAILABLE",
            Status::WrongRoute => "WRONG_ROUTE",
            Status::ProposalMismatch => "PROPOSAL_MISMATCH",
            Status::EtcdTransactionFailed => "ETCD_TRANSACTION_FAILED",
            Status::NotAllowed => "NOT_ALLOWED",
            Status::WrongAppendOffset => "WRONG_APPEND_OFFSET",
            Status::IndexHasGreaterOffset => "INDEX_HAS_GREATER_OFFSET",
            Status::RegisterMismatch => "REGISTER_MISMATCH",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "OK" => Some(Self::Ok),
            "JOURNAL_NOT_FOUND" => Some(Self::JournalNotFound),
            "NO_JOURNAL_PRIMARY_BROKER" => Some(Self::NoJournalPrimaryBroker),
            "NOT_JOURNAL_PRIMARY_BROKER" => Some(Self::NotJournalPrimaryBroker),
            "NOT_JOURNAL_BROKER" => Some(Self::NotJournalBroker),
            "INSUFFICIENT_JOURNAL_BROKERS" => Some(Self::InsufficientJournalBrokers),
            "OFFSET_NOT_YET_AVAILABLE" => Some(Self::OffsetNotYetAvailable),
            "WRONG_ROUTE" => Some(Self::WrongRoute),
            "PROPOSAL_MISMATCH" => Some(Self::ProposalMismatch),
            "ETCD_TRANSACTION_FAILED" => Some(Self::EtcdTransactionFailed),
            "NOT_ALLOWED" => Some(Self::NotAllowed),
            "WRONG_APPEND_OFFSET" => Some(Self::WrongAppendOffset),
            "INDEX_HAS_GREATER_OFFSET" => Some(Self::IndexHasGreaterOffset),
            "REGISTER_MISMATCH" => Some(Self::RegisterMismatch),
            _ => None,
        }
    }
}
/// CompressionCode defines codecs known to Gazette.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum CompressionCodec {
    /// INVALID is the zero-valued CompressionCodec, and is not a valid codec.
    Invalid = 0,
    /// NONE encodes Fragments without any applied compression, with default suffix
    /// ".raw".
    None = 1,
    /// GZIP encodes Fragments using the Gzip library, with default suffix ".gz".
    Gzip = 2,
    /// ZSTANDARD encodes Fragments using the ZStandard library, with default
    /// suffix ".zst".
    Zstandard = 3,
    /// SNAPPY encodes Fragments using the Snappy library, with default suffix
    /// ".sz".
    Snappy = 4,
    /// GZIP_OFFLOAD_DECOMPRESSION is the GZIP codec with additional behavior
    /// around reads and writes to remote Fragment stores, designed to offload
    /// the work of decompression onto compatible stores. Specifically:
    ///   * Fragments are written with a "Content-Encoding: gzip" header.
    ///   * Client read requests are made with "Accept-Encoding: identity".
    /// This can be helpful in contexts where reader IO bandwidth to the storage
    /// API is unconstrained, as the cost of decompression is offloaded to the
    /// store and CPU-intensive batch readers may receive a parallelism benefit.
    /// While this codec may provide substantial read-time performance
    /// improvements, it is an advanced configuration and the "Content-Encoding"
    /// header handling can be subtle and sometimes confusing. It uses the default
    /// suffix ".gzod".
    GzipOffloadDecompression = 5,
}
impl CompressionCodec {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            CompressionCodec::Invalid => "INVALID",
            CompressionCodec::None => "NONE",
            CompressionCodec::Gzip => "GZIP",
            CompressionCodec::Zstandard => "ZSTANDARD",
            CompressionCodec::Snappy => "SNAPPY",
            CompressionCodec::GzipOffloadDecompression => "GZIP_OFFLOAD_DECOMPRESSION",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "INVALID" => Some(Self::Invalid),
            "NONE" => Some(Self::None),
            "GZIP" => Some(Self::Gzip),
            "ZSTANDARD" => Some(Self::Zstandard),
            "SNAPPY" => Some(Self::Snappy),
            "GZIP_OFFLOAD_DECOMPRESSION" => Some(Self::GzipOffloadDecompression),
            _ => None,
        }
    }
}
