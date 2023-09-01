/// Slice represents a contiguous slice of bytes within an associated Arena.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Slice {
    #[prost(uint32, tag = "1")]
    pub begin: u32,
    #[prost(uint32, tag = "2")]
    pub end: u32,
}
/// UUIDParts is a deconstructed, RFC 4122 v1 variant Universally Unique
/// Identifier as used by Gazette.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UuidParts {
    /// "Node" identifier portion of a v1 UUID.
    ///
    /// A unique producer ID is encoded in the high 48 bits (MSB).
    /// Within them, the most-significant bit must be 1 to mark this producer
    /// as "multicast" and not an actual MAC address (as per RFC 4122).
    ///
    /// Bits 49-54 must be zero.
    ///
    /// The low 10 bits are the 10 least-significant bits of the v1 UUID clock
    /// sequence, used by Gazette to represent flags over message transaction
    /// semantics.
    #[prost(fixed64, tag = "1")]
    pub node: u64,
    /// Clock is a v1 UUID 60-bit timestamp (60 MSBs), followed by 4 bits of
    /// sequence counter.
    #[prost(fixed64, tag = "2")]
    pub clock: u64,
}
/// Projection is a mapping between a document location, specified as a
/// JSON-Pointer, and a corresponding field string in a flattened
/// (i.e. tabular or SQL) namespace which aliases it.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Projection {
    /// Document location of this projection, as a JSON-Pointer.
    #[prost(string, tag = "1")]
    pub ptr: ::prost::alloc::string::String,
    /// Field is the flattened, tabular alias of this projection.
    #[prost(string, tag = "2")]
    pub field: ::prost::alloc::string::String,
    /// Was this projection explicitly provided ?
    /// (As opposed to implicitly created through static analysis of the schema).
    #[prost(bool, tag = "3")]
    pub explicit: bool,
    /// Does this projection constitute a logical partitioning of the collection?
    #[prost(bool, tag = "4")]
    pub is_partition_key: bool,
    /// Does this location form (part of) the collection key?
    #[prost(bool, tag = "5")]
    pub is_primary_key: bool,
    /// Inference of this projection.
    #[prost(message, optional, tag = "6")]
    pub inference: ::core::option::Option<Inference>,
}
/// Inference details type information which is statically known
/// about a given document location.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Inference {
    /// The possible types for this location.
    /// Subset of ["null", "boolean", "object", "array", "integer", "numeric",
    /// "string"].
    #[prost(string, repeated, tag = "1")]
    pub types: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(message, optional, tag = "3")]
    pub string: ::core::option::Option<inference::String>,
    /// The title from the schema, if provided.
    #[prost(string, tag = "4")]
    pub title: ::prost::alloc::string::String,
    /// The description from the schema, if provided.
    #[prost(string, tag = "5")]
    pub description: ::prost::alloc::string::String,
    /// The default value from the schema, if provided.
    #[prost(string, tag = "6")]
    pub default_json: ::prost::alloc::string::String,
    /// Whether this location is marked as a secret, like a credential or password.
    #[prost(bool, tag = "7")]
    pub secret: bool,
    /// Existence of this document location.
    #[prost(enumeration = "inference::Exists", tag = "8")]
    pub exists: i32,
}
/// Nested message and enum types in `Inference`.
pub mod inference {
    /// String type-specific inferences, or nil iff types doesn't include "string".
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct String {
        /// Annotated Content-Type when the projection is of "string" type.
        #[prost(string, tag = "3")]
        pub content_type: ::prost::alloc::string::String,
        /// Annotated format when the projection is of "string" type.
        #[prost(string, tag = "4")]
        pub format: ::prost::alloc::string::String,
        /// Annotated Content-Encoding when the projection is of "string" type.
        #[prost(string, tag = "7")]
        pub content_encoding: ::prost::alloc::string::String,
        /// Maximum length when the projection is of "string" type. Zero for no
        /// limit.
        #[prost(uint32, tag = "6")]
        pub max_length: u32,
    }
    /// Exists enumerates the possible states of existence for a location.
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
    pub enum Exists {
        Invalid = 0,
        /// The location must exist.
        Must = 1,
        /// The location may exist or be undefined.
        /// Its schema has explicit keywords which allow it to exist
        /// and which may constrain its shape, such as additionalProperties,
        /// items, unevaluatedProperties, or unevaluatedItems.
        May = 2,
        /// The location may exist or be undefined.
        /// Its schema omits any associated keywords, but the specification's
        /// default behavior allows the location to exist.
        Implicit = 3,
        /// The location cannot exist. For example, it's outside of permitted
        /// array bounds, or is a disallowed property, or has an impossible type.
        Cannot = 4,
    }
    impl Exists {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Exists::Invalid => "INVALID",
                Exists::Must => "MUST",
                Exists::May => "MAY",
                Exists::Implicit => "IMPLICIT",
                Exists::Cannot => "CANNOT",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "INVALID" => Some(Self::Invalid),
                "MUST" => Some(Self::Must),
                "MAY" => Some(Self::May),
                "IMPLICIT" => Some(Self::Implicit),
                "CANNOT" => Some(Self::Cannot),
                _ => None,
            }
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NetworkPort {
    /// Number of this port, in the standard 1..65536 range.
    #[prost(uint32, tag = "1")]
    pub number: u32,
    /// ALPN protocol of this port, if known.
    #[prost(string, tag = "2")]
    pub protocol: ::prost::alloc::string::String,
    /// Is this port public?
    /// When true, unauthenticated requests are allowed.
    /// Otherwise only authenticated users with access to the task will be permitted.
    #[prost(bool, tag = "3")]
    pub public: bool,
}
/// Next tag: 13.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CollectionSpec {
    /// Name of this collection.
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    /// Bundled write-time JSON Schema of the collection.
    #[prost(string, tag = "8")]
    pub write_schema_json: ::prost::alloc::string::String,
    /// Bundled read-time JSON Schema of the collection.
    /// Optional. If not set then `write_schema_json` should be used.
    #[prost(string, tag = "11")]
    pub read_schema_json: ::prost::alloc::string::String,
    /// Composite key of the collection, as JSON-Pointers.
    #[prost(string, repeated, tag = "3")]
    pub key: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// JSON pointer locating the UUID of each collection document.
    #[prost(string, tag = "4")]
    pub uuid_ptr: ::prost::alloc::string::String,
    /// Logical partition fields of this collection, and their applied order.
    /// At present partitions are always in ascending lexicographic order on
    /// their field name, but this could change at some point.
    #[prost(string, repeated, tag = "5")]
    pub partition_fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Logical projections of this collection, ordered on ascending field.
    #[prost(message, repeated, tag = "6")]
    pub projections: ::prost::alloc::vec::Vec<Projection>,
    /// JSON-encoded document template for creating Gazette consumer
    /// transaction acknowledgements of writes into this collection.
    #[prost(string, tag = "7")]
    pub ack_template_json: ::prost::alloc::string::String,
    /// Template for partitions of this collection.
    #[prost(message, optional, tag = "9")]
    pub partition_template: ::core::option::Option<::proto_gazette::broker::JournalSpec>,
    #[prost(message, optional, tag = "12")]
    pub derivation: ::core::option::Option<collection_spec::Derivation>,
}
/// Nested message and enum types in `CollectionSpec`.
pub mod collection_spec {
    /// A Derivation is a collection that builds itself through transformation
    /// of other sourced collections.
    /// When a CollectionSpec is inlined into a CaptureSpec or MaterializationSpec,
    /// its derivation is cleared even if it is, in fact, a derivation.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Derivation {
        /// Type of the derivation's connector.
        #[prost(enumeration = "derivation::ConnectorType", tag = "1")]
        pub connector_type: i32,
        /// JSON-encoded connector configuration of this derivation.
        #[prost(string, tag = "2")]
        pub config_json: ::prost::alloc::string::String,
        #[prost(message, repeated, tag = "3")]
        pub transforms: ::prost::alloc::vec::Vec<derivation::Transform>,
        #[prost(enumeration = "derivation::ShuffleType", repeated, tag = "4")]
        pub shuffle_key_types: ::prost::alloc::vec::Vec<i32>,
        /// Template for shards of this derivation.
        #[prost(message, optional, tag = "5")]
        pub shard_template: ::core::option::Option<::proto_gazette::consumer::ShardSpec>,
        /// Template for recovery logs of shards of this derivation.
        #[prost(message, optional, tag = "6")]
        pub recovery_log_template: ::core::option::Option<
            ::proto_gazette::broker::JournalSpec,
        >,
        /// Network ports of this derivation.
        #[prost(message, repeated, tag = "7")]
        pub network_ports: ::prost::alloc::vec::Vec<super::NetworkPort>,
    }
    /// Nested message and enum types in `Derivation`.
    pub mod derivation {
        /// Transforms of the derivation.
        ///
        /// Next tag: 13.
        #[allow(clippy::derive_partial_eq_without_eq)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Transform {
            /// Stable name of this transform.
            #[prost(string, tag = "1")]
            pub name: ::prost::alloc::string::String,
            /// Source collection which is read by this transform.
            #[prost(message, optional, tag = "2")]
            pub collection: ::core::option::Option<super::super::CollectionSpec>,
            /// Selector of collection partitions which this materialization reads.
            #[prost(message, optional, tag = "3")]
            pub partition_selector: ::core::option::Option<
                ::proto_gazette::broker::LabelSelector,
            >,
            /// Priority of this transform, with respect to other transforms of the derivation.
            /// Higher values imply higher priority.
            #[prost(uint32, tag = "4")]
            pub priority: u32,
            /// Number of seconds for which documents of this transformed are delayed
            /// while reading, relative to other documents (when back-filling) and the
            /// present wall-clock time (when tailing).
            #[prost(uint32, tag = "5")]
            pub read_delay_seconds: u32,
            /// Shuffle key of this transform, or empty if a shuffle key is not defined.
            #[prost(string, repeated, tag = "6")]
            pub shuffle_key: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
            /// / JSON-encoded shuffle lambda of this transform, or empty if a shuffle lambda is not defined.
            #[prost(string, tag = "7")]
            pub shuffle_lambda_config_json: ::prost::alloc::string::String,
            /// JSON-encoded lambda of this transform.
            #[prost(string, tag = "8")]
            pub lambda_config_json: ::prost::alloc::string::String,
            /// Is this transform known to always be read-only?
            #[prost(bool, tag = "9")]
            pub read_only: bool,
            /// Stable, unique value used to suffix journal read checkpoints of this transform.
            /// Computed as "derive/{derivation}/{transform}".
            #[prost(string, tag = "10")]
            pub journal_read_suffix: ::prost::alloc::string::String,
            /// When set, documents before this timestamp are not processed.
            #[prost(message, optional, tag = "11")]
            pub not_before: ::core::option::Option<::pbjson_types::Timestamp>,
            /// When set, documents after this timestamp are not processed.
            #[prost(message, optional, tag = "12")]
            pub not_after: ::core::option::Option<::pbjson_types::Timestamp>,
        }
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
        pub enum ConnectorType {
            InvalidConnectorType = 0,
            Sqlite = 1,
            Typescript = 2,
            Image = 3,
        }
        impl ConnectorType {
            /// String value of the enum field names used in the ProtoBuf definition.
            ///
            /// The values are not transformed in any way and thus are considered stable
            /// (if the ProtoBuf definition does not change) and safe for programmatic use.
            pub fn as_str_name(&self) -> &'static str {
                match self {
                    ConnectorType::InvalidConnectorType => "INVALID_CONNECTOR_TYPE",
                    ConnectorType::Sqlite => "SQLITE",
                    ConnectorType::Typescript => "TYPESCRIPT",
                    ConnectorType::Image => "IMAGE",
                }
            }
            /// Creates an enum from field names used in the ProtoBuf definition.
            pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
                match value {
                    "INVALID_CONNECTOR_TYPE" => Some(Self::InvalidConnectorType),
                    "SQLITE" => Some(Self::Sqlite),
                    "TYPESCRIPT" => Some(Self::Typescript),
                    "IMAGE" => Some(Self::Image),
                    _ => None,
                }
            }
        }
        /// JSON types of shuffle key components extracted by the transforms of this derivation.
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
        pub enum ShuffleType {
            InvalidShuffleType = 0,
            Boolean = 1,
            Integer = 2,
            String = 3,
        }
        impl ShuffleType {
            /// String value of the enum field names used in the ProtoBuf definition.
            ///
            /// The values are not transformed in any way and thus are considered stable
            /// (if the ProtoBuf definition does not change) and safe for programmatic use.
            pub fn as_str_name(&self) -> &'static str {
                match self {
                    ShuffleType::InvalidShuffleType => "INVALID_SHUFFLE_TYPE",
                    ShuffleType::Boolean => "BOOLEAN",
                    ShuffleType::Integer => "INTEGER",
                    ShuffleType::String => "STRING",
                }
            }
            /// Creates an enum from field names used in the ProtoBuf definition.
            pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
                match value {
                    "INVALID_SHUFFLE_TYPE" => Some(Self::InvalidShuffleType),
                    "BOOLEAN" => Some(Self::Boolean),
                    "INTEGER" => Some(Self::Integer),
                    "STRING" => Some(Self::String),
                    _ => None,
                }
            }
        }
    }
}
/// FieldSelection is a selection of a collection's projection fields.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FieldSelection {
    /// Fields for each key component of the collection. Included key fields appear
    /// in the collection's key component order, and a given key pointer will be
    /// included at most once.
    #[prost(string, repeated, tag = "1")]
    pub keys: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// All other selected fields, other than those in keys and the document field.
    /// Entries are in ascending sorted order, and may be empty.
    #[prost(string, repeated, tag = "2")]
    pub values: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Field having a document pointer located at the document root.
    #[prost(string, tag = "3")]
    pub document: ::prost::alloc::string::String,
    /// Additional configuration, keyed by fields included in |keys|, |values|, or
    /// |document|. Values are arbitrary JSON-encoded objects.
    #[prost(btree_map = "string, string", tag = "4")]
    pub field_config_json_map: ::prost::alloc::collections::BTreeMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
}
/// CaptureSpec describes a collection and its capture from an endpoint.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CaptureSpec {
    /// Name of this capture.
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(enumeration = "capture_spec::ConnectorType", tag = "2")]
    pub connector_type: i32,
    /// JSON-encoded connector configuration of this capture.
    #[prost(string, tag = "3")]
    pub config_json: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "4")]
    pub bindings: ::prost::alloc::vec::Vec<capture_spec::Binding>,
    /// Minimum interval of time between successive invocations of the capture.
    #[prost(uint32, tag = "5")]
    pub interval_seconds: u32,
    /// Template for shards of this capture.
    #[prost(message, optional, tag = "6")]
    pub shard_template: ::core::option::Option<::proto_gazette::consumer::ShardSpec>,
    /// Template for recovery logs of shards of this capture.
    #[prost(message, optional, tag = "7")]
    pub recovery_log_template: ::core::option::Option<
        ::proto_gazette::broker::JournalSpec,
    >,
    /// Network ports of this capture.
    #[prost(message, repeated, tag = "8")]
    pub network_ports: ::prost::alloc::vec::Vec<NetworkPort>,
}
/// Nested message and enum types in `CaptureSpec`.
pub mod capture_spec {
    /// Bindings of endpoint resources and collections into which they're captured.
    /// Bindings are ordered and unique on the bound collection name,
    /// and are also unique on the resource path.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Binding {
        /// JSON-encoded configuration of the bound resource.
        #[prost(string, tag = "1")]
        pub resource_config_json: ::prost::alloc::string::String,
        /// Driver-supplied path components which fully qualify the
        /// subresource being captured.
        #[prost(string, repeated, tag = "2")]
        pub resource_path: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
        /// Collection to be captured into.
        #[prost(message, optional, tag = "3")]
        pub collection: ::core::option::Option<super::CollectionSpec>,
    }
    /// Type of the capture's connector.
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
    pub enum ConnectorType {
        Invalid = 0,
        Image = 7,
    }
    impl ConnectorType {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                ConnectorType::Invalid => "INVALID",
                ConnectorType::Image => "IMAGE",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "INVALID" => Some(Self::Invalid),
                "IMAGE" => Some(Self::Image),
                _ => None,
            }
        }
    }
}
/// MaterializationSpec describes a collection and its materialization to an
/// endpoint.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MaterializationSpec {
    /// Name of this materialization.
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(enumeration = "materialization_spec::ConnectorType", tag = "2")]
    pub connector_type: i32,
    /// JSON-encoded connector configuration of this materialization.
    #[prost(string, tag = "3")]
    pub config_json: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "4")]
    pub bindings: ::prost::alloc::vec::Vec<materialization_spec::Binding>,
    /// Template for shards of this materialization.
    #[prost(message, optional, tag = "5")]
    pub shard_template: ::core::option::Option<::proto_gazette::consumer::ShardSpec>,
    /// Template for recovery logs of shards of this materialization.
    #[prost(message, optional, tag = "6")]
    pub recovery_log_template: ::core::option::Option<
        ::proto_gazette::broker::JournalSpec,
    >,
    /// Network ports of this materialization.
    #[prost(message, repeated, tag = "7")]
    pub network_ports: ::prost::alloc::vec::Vec<NetworkPort>,
}
/// Nested message and enum types in `MaterializationSpec`.
pub mod materialization_spec {
    /// Bindings of endpoint resources and collections from which they're
    /// materialized. Bindings are ordered and unique on the bound collection name,
    /// and are also unique on the resource path.
    ///
    /// Next tag: 12.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Binding {
        /// JSON-encoded configuration of the bound resource.
        #[prost(string, tag = "1")]
        pub resource_config_json: ::prost::alloc::string::String,
        /// Driver-supplied path components which fully qualify the
        /// subresource being materialized.
        #[prost(string, repeated, tag = "2")]
        pub resource_path: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
        /// Collection to be materialized.
        #[prost(message, optional, tag = "3")]
        pub collection: ::core::option::Option<super::CollectionSpec>,
        /// Selector of collection partitions which this materialization reads.
        #[prost(message, optional, tag = "7")]
        pub partition_selector: ::core::option::Option<
            ::proto_gazette::broker::LabelSelector,
        >,
        /// Priority of this binding, with respect to other bindings of the materialization.
        /// Higher values imply higher priority.
        #[prost(uint32, tag = "9")]
        pub priority: u32,
        /// Resolved fields selected for materialization.
        #[prost(message, optional, tag = "4")]
        pub field_selection: ::core::option::Option<super::FieldSelection>,
        /// Materialize delta updates of documents rather than full reductions.
        #[prost(bool, tag = "5")]
        pub delta_updates: bool,
        #[prost(message, optional, tag = "6")]
        pub deprecated_shuffle: ::core::option::Option<binding::DeprecatedShuffle>,
        /// Stable, unique value used to suffix journal read checkpoints of this binding.
        /// Computed as "materialize/{materialization}/{encoded-resource-path}".
        #[prost(string, tag = "8")]
        pub journal_read_suffix: ::prost::alloc::string::String,
        /// When set, documents before this timestamp are not processed.
        #[prost(message, optional, tag = "10")]
        pub not_before: ::core::option::Option<::pbjson_types::Timestamp>,
        /// When set, documents after this timestamp are not processed.
        #[prost(message, optional, tag = "11")]
        pub not_after: ::core::option::Option<::pbjson_types::Timestamp>,
    }
    /// Nested message and enum types in `Binding`.
    pub mod binding {
        /// Deprecated shuffle message which holds an alternate location for `partition_selector`.
        #[allow(clippy::derive_partial_eq_without_eq)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct DeprecatedShuffle {
            #[prost(string, tag = "1")]
            pub group_name: ::prost::alloc::string::String,
            #[prost(message, optional, tag = "3")]
            pub partition_selector: ::core::option::Option<
                ::proto_gazette::broker::LabelSelector,
            >,
        }
    }
    /// Type of the materialization's connector.
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
    pub enum ConnectorType {
        Invalid = 0,
        Sqlite = 2,
        Image = 8,
    }
    impl ConnectorType {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                ConnectorType::Invalid => "INVALID",
                ConnectorType::Sqlite => "SQLITE",
                ConnectorType::Image => "IMAGE",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "INVALID" => Some(Self::Invalid),
                "SQLITE" => Some(Self::Sqlite),
                "IMAGE" => Some(Self::Image),
                _ => None,
            }
        }
    }
}
/// OAuth2 describes an OAuth2 provider
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OAuth2 {
    /// Name of the OAuth2 provider. This is a machine-readable key and must stay
    /// consistent. One example use case is to map providers to their respective
    /// style of buttons in the UI
    #[prost(string, tag = "1")]
    pub provider: ::prost::alloc::string::String,
    /// Template for authorization URL, this is the first step of the OAuth2 flow
    /// where the user is redirected to the OAuth2 provider to authorize access to
    /// their account
    #[prost(string, tag = "2")]
    pub auth_url_template: ::prost::alloc::string::String,
    /// Template for access token URL, this is the second step of the OAuth2 flow,
    /// where we request an access token from the provider
    #[prost(string, tag = "3")]
    pub access_token_url_template: ::prost::alloc::string::String,
    /// The method used to send access_token request. POST by default.
    #[prost(string, tag = "11")]
    pub access_token_method: ::prost::alloc::string::String,
    /// The POST body of the access_token request
    #[prost(string, tag = "4")]
    pub access_token_body: ::prost::alloc::string::String,
    /// Headers for the access_token request
    #[prost(btree_map = "string, string", tag = "5")]
    pub access_token_headers_json_map: ::prost::alloc::collections::BTreeMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
    /// A json map that maps the response from the OAuth provider for Access Token
    /// request to keys in the connector endpoint configuration.
    /// If the connector supports refresh tokens, must include `refresh_token` and
    /// `expires_in`. If this mapping is not provided, the keys from the response
    /// are passed as-is to the connector config.
    #[prost(btree_map = "string, string", tag = "6")]
    pub access_token_response_json_map: ::prost::alloc::collections::BTreeMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
    /// Template for refresh token URL, some providers require that the access
    /// token be refreshed.
    #[prost(string, tag = "7")]
    pub refresh_token_url_template: ::prost::alloc::string::String,
    /// The method used to send refresh_token request. POST by default.
    #[prost(string, tag = "12")]
    pub refresh_token_method: ::prost::alloc::string::String,
    /// The POST body of the refresh_token request
    #[prost(string, tag = "8")]
    pub refresh_token_body: ::prost::alloc::string::String,
    /// Headers for the refresh_token request
    #[prost(btree_map = "string, string", tag = "9")]
    pub refresh_token_headers_json_map: ::prost::alloc::collections::BTreeMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
    /// A json map that maps the response from the OAuth provider for Refresh Token
    /// request to keys in the connector endpoint configuration.
    /// If the connector supports refresh tokens, must include `refresh_token` and
    /// `expires_in`. If this mapping is not provided, the keys from the response
    /// are passed as-is to the connector config.
    #[prost(btree_map = "string, string", tag = "10")]
    pub refresh_token_response_json_map: ::prost::alloc::collections::BTreeMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
}
/// TestSpec describes a catalog test.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TestSpec {
    /// Name of this test.
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub steps: ::prost::alloc::vec::Vec<test_spec::Step>,
}
/// Nested message and enum types in `TestSpec`.
pub mod test_spec {
    /// Steps of the test.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Step {
        #[prost(enumeration = "step::Type", tag = "1")]
        pub step_type: i32,
        /// Index of this step within the test.
        #[prost(uint32, tag = "2")]
        pub step_index: u32,
        /// Description of this step.
        #[prost(string, tag = "3")]
        pub description: ::prost::alloc::string::String,
        /// Scope of the test definition location.
        #[prost(string, tag = "4")]
        pub step_scope: ::prost::alloc::string::String,
        /// Collection ingested or verified by this step.
        #[prost(string, tag = "5")]
        pub collection: ::prost::alloc::string::String,
        /// JSON documents to ingest or verify.
        #[prost(string, repeated, tag = "6")]
        pub docs_json_vec: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
        /// When verifying, selector over logical partitions of the collection.
        #[prost(message, optional, tag = "7")]
        pub partitions: ::core::option::Option<::proto_gazette::broker::LabelSelector>,
    }
    /// Nested message and enum types in `Step`.
    pub mod step {
        /// Type of this step.
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
        pub enum Type {
            Ingest = 0,
            Verify = 1,
        }
        impl Type {
            /// String value of the enum field names used in the ProtoBuf definition.
            ///
            /// The values are not transformed in any way and thus are considered stable
            /// (if the ProtoBuf definition does not change) and safe for programmatic use.
            pub fn as_str_name(&self) -> &'static str {
                match self {
                    Type::Ingest => "INGEST",
                    Type::Verify => "VERIFY",
                }
            }
            /// Creates an enum from field names used in the ProtoBuf definition.
            pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
                match value {
                    "INGEST" => Some(Self::Ingest),
                    "VERIFY" => Some(Self::Verify),
                    _ => None,
                }
            }
        }
    }
}
/// RangeSpec describes the ranges of shuffle keys and r-clocks which a reader
/// is responsible for.
#[allow(clippy::derive_partial_eq_without_eq)]
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
    #[prost(fixed32, tag = "2")]
    pub key_begin: u32,
    #[prost(fixed32, tag = "3")]
    pub key_end: u32,
    /// Rotated [begin, end] inclusive ranges of Clocks.
    #[prost(fixed32, tag = "4")]
    pub r_clock_begin: u32,
    #[prost(fixed32, tag = "5")]
    pub r_clock_end: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConnectorState {
    /// Update of the connector state, to be persisted by the Flow runtime
    /// and supplied in the Open of a future connector invocation.
    #[prost(string, tag = "1")]
    pub updated_json: ::prost::alloc::string::String,
    /// If true, then this state is applied to a previous state
    /// as a RFC7396 Merge Patch.
    #[prost(bool, tag = "2")]
    pub merge_patch: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExtractApi {}
/// Nested message and enum types in `ExtractAPI`.
pub mod extract_api {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Config {
        /// JSON pointer of the document UUID to extract.
        #[prost(string, tag = "1")]
        pub uuid_ptr: ::prost::alloc::string::String,
        /// JSON schema to validate non-ACK documents against.
        /// If empty then schema validation is not performed.
        #[prost(string, tag = "2")]
        pub schema_json: ::prost::alloc::string::String,
        /// Field JSON pointers to extract from documents and return as packed
        /// tuples.
        #[prost(string, repeated, tag = "3")]
        pub field_ptrs: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
        /// A set of Projections which must include `field_ptrs`.
        /// TODO(johnny): This is a kludge as we seek to remove this API.
        #[prost(message, repeated, tag = "4")]
        pub projections: ::prost::alloc::vec::Vec<super::Projection>,
    }
    /// Code labels message codes passed over the CGO bridge.
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
    impl Code {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Code::Invalid => "INVALID",
                Code::Configure => "CONFIGURE",
                Code::Extract => "EXTRACT",
                Code::ExtractedUuid => "EXTRACTED_UUID",
                Code::ExtractedFields => "EXTRACTED_FIELDS",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "INVALID" => Some(Self::Invalid),
                "CONFIGURE" => Some(Self::Configure),
                "EXTRACT" => Some(Self::Extract),
                "EXTRACTED_UUID" => Some(Self::ExtractedUuid),
                "EXTRACTED_FIELDS" => Some(Self::ExtractedFields),
                _ => None,
            }
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CombineApi {}
/// Nested message and enum types in `CombineAPI`.
pub mod combine_api {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Config {
        /// JSON schema against which documents are to be validated,
        /// and which provides reduction annotations.
        #[prost(string, tag = "1")]
        pub schema_json: ::prost::alloc::string::String,
        /// Composite key used to group documents to be combined, specified as one or
        /// more JSON-Pointers indicating a message location to extract.
        #[prost(string, repeated, tag = "2")]
        pub key_ptrs: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
        /// Fields to be extracted from combined documents and returned.
        /// If empty, no fields are extracted.
        #[prost(string, repeated, tag = "3")]
        pub fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
        /// JSON-Pointer at which a placeholder UUID should be inserted into
        /// returned documents. If empty, no placeholder is inserted.
        #[prost(string, tag = "4")]
        pub uuid_placeholder_ptr: ::prost::alloc::string::String,
        /// A set of Projections which must include `key_ptrs` and `fields`.
        /// TODO(johnny): This is a kludge as we seek to remove this API.
        #[prost(message, repeated, tag = "5")]
        pub projections: ::prost::alloc::vec::Vec<super::Projection>,
        /// The name of the collection that's being written to.
        #[prost(string, tag = "6")]
        pub collection_name: ::prost::alloc::string::String,
        /// JSON-encoded string representing the JSON schema to start inference
        /// from. If empty, do not emit inferred schemas.
        #[prost(string, tag = "7")]
        pub infer_schema_json: ::prost::alloc::string::String,
    }
    /// Stats holds statistics relating to one or more combiner transactions.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Stats {
        #[prost(message, optional, tag = "1")]
        pub left: ::core::option::Option<super::DocsAndBytes>,
        #[prost(message, optional, tag = "2")]
        pub right: ::core::option::Option<super::DocsAndBytes>,
        #[prost(message, optional, tag = "3")]
        pub out: ::core::option::Option<super::DocsAndBytes>,
    }
    /// Code labels message codes passed over the CGO bridge.
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
        DrainChunk = 200,
        /// Next drained document is partially combined (Rust -> Go).
        DrainedCombinedDocument = 201,
        /// Next drained document is fully reduced (Rust -> Go).
        DrainedReducedDocument = 202,
        /// Next drained key (follows drained document; Rust -> Go).
        DrainedKey = 203,
        /// Next drained fields (follows key; Rust -> Go).
        DrainedFields = 204,
        /// Drain stats, sent after all documents have been drained. (Rust -> Go)
        DrainedStats = 205,
    }
    impl Code {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Code::Invalid => "INVALID",
                Code::Configure => "CONFIGURE",
                Code::ReduceLeft => "REDUCE_LEFT",
                Code::CombineRight => "COMBINE_RIGHT",
                Code::DrainChunk => "DRAIN_CHUNK",
                Code::DrainedCombinedDocument => "DRAINED_COMBINED_DOCUMENT",
                Code::DrainedReducedDocument => "DRAINED_REDUCED_DOCUMENT",
                Code::DrainedKey => "DRAINED_KEY",
                Code::DrainedFields => "DRAINED_FIELDS",
                Code::DrainedStats => "DRAINED_STATS",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "INVALID" => Some(Self::Invalid),
                "CONFIGURE" => Some(Self::Configure),
                "REDUCE_LEFT" => Some(Self::ReduceLeft),
                "COMBINE_RIGHT" => Some(Self::CombineRight),
                "DRAIN_CHUNK" => Some(Self::DrainChunk),
                "DRAINED_COMBINED_DOCUMENT" => Some(Self::DrainedCombinedDocument),
                "DRAINED_REDUCED_DOCUMENT" => Some(Self::DrainedReducedDocument),
                "DRAINED_KEY" => Some(Self::DrainedKey),
                "DRAINED_FIELDS" => Some(Self::DrainedFields),
                "DRAINED_STATS" => Some(Self::DrainedStats),
                _ => None,
            }
        }
    }
}
/// BuildAPI is deprecated and will be removed.
/// We're currently keeping Config around only to
/// avoid churning various Go snapshot tests.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BuildApi {}
/// Nested message and enum types in `BuildAPI`.
pub mod build_api {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Config {
        /// Identifier of this build.
        /// The path of the output database is determined by joining the
        /// configured directory and configured build ID.
        #[prost(string, tag = "1")]
        pub build_id: ::prost::alloc::string::String,
        /// Path to the output build database that should be written.
        #[prost(string, tag = "2")]
        pub build_db: ::prost::alloc::string::String,
        /// Root catalog source specification. This may be either a local path
        /// relative to the current working directory, or an absolute URL.
        #[prost(string, tag = "3")]
        pub source: ::prost::alloc::string::String,
        /// Content type of the source.
        #[prost(enumeration = "super::ContentType", tag = "4")]
        pub source_type: i32,
        /// The Docker network the connectors are given access to during catalog
        /// builds.
        #[prost(string, tag = "5")]
        pub connector_network: ::prost::alloc::string::String,
        /// URL which roots the Flow project.
        #[prost(string, tag = "6")]
        pub project_root: ::prost::alloc::string::String,
    }
}
/// ResetStateRequest is the request of the Testing.ResetState RPC.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResetStateRequest {}
/// ResetStateResponse is the response of the Testing.ResetState RPC.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResetStateResponse {}
/// AdvanceTimeRequest is the request of the Testing.AdvanceTime RPC.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AdvanceTimeRequest {
    #[prost(uint64, tag = "1")]
    pub advance_seconds: u64,
}
/// AdvanceTimeResponse is the response of the Testing.AdvanceTime RPC.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AdvanceTimeResponse {}
/// DocsAndBytes represents a count of JSON documents, and their cumulative total
/// size in bytes. This is used by the various Stats messages.
/// Deprecated (johnny).
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DocsAndBytes {
    #[prost(uint32, tag = "1")]
    pub docs: u32,
    #[prost(uint64, tag = "2")]
    pub bytes: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IngestRequest {
    /// Name of the collection into which to ingest.
    #[prost(string, tag = "1")]
    pub collection: ::prost::alloc::string::String,
    /// Build ID of the ingested collection.
    #[prost(string, tag = "2")]
    pub build_id: ::prost::alloc::string::String,
    /// JSON documents to ingest or verify.
    #[prost(string, repeated, tag = "3")]
    pub docs_json_vec: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
/// IngestResponse is the response of the Testing.Ingest RPC.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IngestResponse {
    /// Journals appended to by this ingestion, and their maximum offset on commit.
    #[prost(btree_map = "string, int64", tag = "1")]
    pub journal_write_heads: ::prost::alloc::collections::BTreeMap<
        ::prost::alloc::string::String,
        i64,
    >,
    /// Etcd header which describes current journal partitions.
    #[prost(message, optional, tag = "2")]
    pub journal_etcd: ::core::option::Option<::proto_gazette::broker::header::Etcd>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskNetworkProxyRequest {
    #[prost(message, optional, tag = "1")]
    pub open: ::core::option::Option<task_network_proxy_request::Open>,
    #[prost(bytes = "vec", tag = "2")]
    pub data: ::prost::alloc::vec::Vec<u8>,
}
/// Nested message and enum types in `TaskNetworkProxyRequest`.
pub mod task_network_proxy_request {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Open {
        /// Header contains information about the shard resolution that was done by the client
        #[prost(message, optional, tag = "1")]
        pub header: ::core::option::Option<::proto_gazette::broker::Header>,
        #[prost(string, tag = "2")]
        pub shard_id: ::prost::alloc::string::String,
        /// The port number inside the container that the client wishes to connect to.
        #[prost(uint32, tag = "3")]
        pub target_port: u32,
        /// The network address of the client that is establishing the connection.
        #[prost(string, tag = "4")]
        pub client_addr: ::prost::alloc::string::String,
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskNetworkProxyResponse {
    #[prost(message, optional, tag = "1")]
    pub open_response: ::core::option::Option<task_network_proxy_response::OpenResponse>,
    #[prost(bytes = "vec", tag = "2")]
    pub data: ::prost::alloc::vec::Vec<u8>,
}
/// Nested message and enum types in `TaskNetworkProxyResponse`.
pub mod task_network_proxy_response {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct OpenResponse {
        #[prost(enumeration = "Status", tag = "1")]
        pub status: i32,
        #[prost(message, optional, tag = "2")]
        pub header: ::core::option::Option<::proto_gazette::broker::Header>,
    }
    /// Status represents the high-level response to an Open request. If OK, then
    /// the connection may proceed. Any other status indicates the reason for refusal.
    /// This enum is a superset of the consumer.Status enum used by the Shards service,
    /// though some statuses have taken on broader meanings.
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
        /// Used to indicate an error in the proxying machinery.
        /// This corresponds to consumer.Status_ETCD_TRANSACTION_FAILED, which is considered
        /// a specific case of the broader category of "internal" errors, since the proxy API
        /// doesn't directly expose anything about etcd.
        InternalError = 4,
        /// Either the shard itself is stopped or failed, or else the container is.
        ShardStopped = 5,
        /// The client is not allowed to connect to the port given in the request.
        /// This could be either because the port does not exist or for any other
        /// reason, such as if we implement IP-based access policies.
        PortNotAllowed = 1000,
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
                Status::InternalError => "INTERNAL_ERROR",
                Status::ShardStopped => "SHARD_STOPPED",
                Status::PortNotAllowed => "PORT_NOT_ALLOWED",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "OK" => Some(Self::Ok),
                "SHARD_NOT_FOUND" => Some(Self::ShardNotFound),
                "NO_SHARD_PRIMARY" => Some(Self::NoShardPrimary),
                "NOT_SHARD_PRIMARY" => Some(Self::NotShardPrimary),
                "INTERNAL_ERROR" => Some(Self::InternalError),
                "SHARD_STOPPED" => Some(Self::ShardStopped),
                "PORT_NOT_ALLOWED" => Some(Self::PortNotAllowed),
                _ => None,
            }
        }
    }
}
/// ContentType enumerates the content types understood by Flow.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ContentType {
    Catalog = 0,
    JsonSchema = 1,
    Config = 4,
    DocumentsFixture = 5,
}
impl ContentType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ContentType::Catalog => "CATALOG",
            ContentType::JsonSchema => "JSON_SCHEMA",
            ContentType::Config => "CONFIG",
            ContentType::DocumentsFixture => "DOCUMENTS_FIXTURE",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "CATALOG" => Some(Self::Catalog),
            "JSON_SCHEMA" => Some(Self::JsonSchema),
            "CONFIG" => Some(Self::Config),
            "DOCUMENTS_FIXTURE" => Some(Self::DocumentsFixture),
            _ => None,
        }
    }
}
