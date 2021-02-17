/// RecordedOp records states changes occuring within a local file-system.
/// Next tag: 11.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RecordedOp {
    /// Monotonically-increasing sequence number of this operation.
    #[prost(int64, tag = "1")]
    pub seq_no: i64,
    /// Previous FSM checksum to which this operation should be applied (eg, the
    /// expected checksum arrived at after applying the previous operation.
    #[prost(fixed32, tag = "2")]
    pub checksum: u32,
    /// Author is the unique ID of the Recorder which wrote this RecordedOp.
    /// Each Recorder randomly generates an Author ID at startup, and thereafter
    /// applies it to all operations it records.
    #[prost(fixed32, tag = "3")]
    pub author: u32,
    /// First and last byte offset (exclusive) of this RecordedOp. These are meta-
    /// fields which are not populated in the recorded log (as Recorders cannot
    /// know at what offsets their writes will land in the log). Instead, Players
    /// attach offsets as they deserialize RecordedOps from the committed log.
    #[prost(int64, tag = "9")]
    pub first_offset: i64,
    #[prost(int64, tag = "10")]
    pub last_offset: i64,
    #[prost(message, optional, tag = "4")]
    pub create: ::core::option::Option<recorded_op::Create>,
    #[prost(message, optional, tag = "5")]
    pub link: ::core::option::Option<recorded_op::Link>,
    #[prost(message, optional, tag = "6")]
    pub unlink: ::core::option::Option<recorded_op::Link>,
    #[prost(message, optional, tag = "7")]
    pub write: ::core::option::Option<recorded_op::Write>,
    /// Property indicates a property file has been created or updated.
    #[prost(message, optional, tag = "8")]
    pub property: ::core::option::Option<Property>,
}
/// Nested message and enum types in `RecordedOp`.
pub mod recorded_op {
    // RecordedOp is a union-type over the remaining fields.

    // A no-op may also be represented as a RecordedOp with no fields set. This
    // is principally useful for issuing transactional write-barriers at log
    // handoff. Eg, given a log Player which desires to be the log Recorder:
    //
    //   1) The Player will read all available log content, and then inject
    //      what it understands to be a correctly sequenced no-op with
    //      its unique author ID. Note that this injected operation may well
    //      lose a write race with another Recorder, resulting in its being
    //      mis-sequenced and ignored by other readers.
    //
    //   2) It will continue to read the log until its no-op is read.
    //      If the operation is mis-sequenced, it will restart from step 1.
    //
    //   3) If the no-op is sequenced correctly, it will terminate playback
    //      immediately after the no-op and transition to recording new log
    //      operations. Any following, raced writes must be mis-sequenced,
    //      having lost the write race, and will be ignored by other readers.

    /// Create a new "File Node" (Fnode), initially linked to |path|. Fnodes play
    /// a similar role to Posix inodes: they identify a specific file object while
    /// being invariant to (and spanning across) its current or future path links.
    /// The assigned Fnode ID is the |seq_no| of this RecordedOp.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Create {
        /// Filesystem path of this file, relative to the common base directory.
        #[prost(string, tag = "1")]
        pub path: ::prost::alloc::string::String,
    }
    /// Link or unlink an Fnode to a filesystem path.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Link {
        /// Fnode being linked or unlinked.
        #[prost(int64, tag = "1")]
        pub fnode: i64,
        /// Filesystem path being un/linked, relative to the common base directory.
        #[prost(string, tag = "2")]
        pub path: ::prost::alloc::string::String,
    }
    /// Write indicates |length| bytes should be written at |offset| to |fnode|.
    /// In a serialization stream, we expect |length| raw bytes of content to
    /// immediately follow this operation.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Write {
        /// Fnode being written to.
        #[prost(int64, tag = "1")]
        pub fnode: i64,
        /// Byte-offset within the file to which this write is applied.
        #[prost(int64, tag = "2")]
        pub offset: i64,
        /// Length of the write.
        #[prost(int64, tag = "3")]
        pub length: i64,
    }
}
/// Property is a small file which rarely changes, and is thus managed
/// outside of regular Fnode tracking. See FSM.Properties.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Property {
    /// Filesystem path of this property, relative to the common base directory.
    #[prost(string, tag = "1")]
    pub path: ::prost::alloc::string::String,
    /// Complete file content of this property.
    #[prost(string, tag = "2")]
    pub content: ::prost::alloc::string::String,
}
/// Segment is a contiguous chunk of recovery log written by a single Author.
/// Recorders track Segments they have written, for use in providing hints to
/// future readers of the log. A key point to understand is that Gazette append
/// semantics mean that Recorders *cannot know* exactly what offsets their writes
/// are applied to in the log, nor guarantee that their operations are not being
/// interleaved with those of other writers. Log Players are aware of these
/// limitations, and use Segments to resolve conflicts of possible interpretation
/// of the log. Segments produced by a Player are exact, since Players observe all
/// recorded operations at their exact offsets.
/// Next tag: 7.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Segment {
    /// Author which wrote RecordedOps of this Segment.
    #[prost(fixed32, tag = "1")]
    pub author: u32,
    /// First (lowest) sequence number of RecordedOps within this Segment.
    #[prost(int64, tag = "2")]
    pub first_seq_no: i64,
    /// First byte offset of the Segment, where |first_seq_no| is recorded.
    /// If this Segment was produced by a Recorder, this is guaranteed only to be a
    /// lower-bound (eg, a Player reading at this offset may encounter irrelevant
    /// operations prior to the RecordedOp indicated by the tuple
    /// (|author|, |first_seq_no|, |first_checksum|). If a Player produced the Segment,
    /// first_offset is exact.
    #[prost(int64, tag = "3")]
    pub first_offset: i64,
    /// Checksum of the RecordedOp having |first_seq_no|.
    #[prost(fixed32, tag = "4")]
    pub first_checksum: u32,
    /// Last (highest, inclusive) sequence number of RecordedOps within this Segment.
    #[prost(int64, tag = "5")]
    pub last_seq_no: i64,
    /// Last offset (exclusive) of the Segment. Zero means the offset is not known
    /// (eg, because the Segment was produced by a Recorder).
    #[prost(int64, tag = "6")]
    pub last_offset: i64,
}
/// FnodeSegments captures log Segments containing all RecordedOps of the Fnode.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FnodeSegments {
    /// Fnode being hinted.
    #[prost(int64, tag = "1")]
    pub fnode: i64,
    /// Segments of the Fnode in the log. Currently, FSM tracks only a single
    /// Segment per Fnode per Author. A specific implication of this is that Fnodes
    /// modified over long periods of time will result in Segments spanning large
    /// chunks of the log. For best performance, Fnodes should be opened & written
    /// once, and then never be modified again (this is RocksDB's behavior).
    /// If supporting this case is desired, FSM will have to be a bit smarter about
    /// not extending Segments which gap over significant portions of the log
    /// (eg, there's a trade-off to make over size of the hinted manifest, vs
    /// savings incurred on playback by being able to skip portions of the log).
    #[prost(message, repeated, tag = "2")]
    pub segments: ::prost::alloc::vec::Vec<Segment>,
}
/// FSMHints represents a manifest of Fnodes which were still live (eg, having
/// remaining links) at the time the FSMHints were produced, as well as any
/// Properties. It allows a Player of the log to identify minimal Segments which
/// must be read to recover all Fnodes, and also contains sufficient metadata for
/// a Player to resolve all possible conflicts it could encounter while reading
/// the log, to arrive at a consistent view of file state which exactly matches
/// that of the Recorder producing the FSMHints.
/// Next tag: 4.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FsmHints {
    /// Log is the Journal name holding recorded log content.
    #[prost(string, tag = "1")]
    pub log: ::prost::alloc::string::String,
    /// Live Fnodes and their Segments as-of the generation of these FSMHints.
    #[prost(message, repeated, tag = "2")]
    pub live_nodes: ::prost::alloc::vec::Vec<FnodeSegments>,
    /// Property files and contents as-of the generation of these FSMHints.
    #[prost(message, repeated, tag = "3")]
    pub properties: ::prost::alloc::vec::Vec<Property>,
}
