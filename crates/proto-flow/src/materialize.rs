#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Request {
    #[prost(message, optional, tag = "1")]
    pub spec: ::core::option::Option<request::Spec>,
    #[prost(message, optional, tag = "2")]
    pub validate: ::core::option::Option<request::Validate>,
    #[prost(message, optional, tag = "3")]
    pub apply: ::core::option::Option<request::Apply>,
    #[prost(message, optional, tag = "4")]
    pub open: ::core::option::Option<request::Open>,
    #[prost(message, optional, tag = "5")]
    pub load: ::core::option::Option<request::Load>,
    #[prost(message, optional, tag = "6")]
    pub flush: ::core::option::Option<request::Flush>,
    #[prost(message, optional, tag = "7")]
    pub store: ::core::option::Option<request::Store>,
    #[prost(message, optional, tag = "8")]
    pub start_commit: ::core::option::Option<request::StartCommit>,
    #[prost(message, optional, tag = "9")]
    pub acknowledge: ::core::option::Option<request::Acknowledge>,
    /// Reserved for internal use.
    #[prost(message, optional, tag = "100")]
    pub internal: ::core::option::Option<::pbjson_types::Any>,
}
/// Nested message and enum types in `Request`.
pub mod request {
    /// Spec requests the specification definition of this connector.
    /// Notably this includes its configuration JSON schemas.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Spec {
        /// Connector type addressed by this request.
        #[prost(
            enumeration = "super::super::flow::materialization_spec::ConnectorType",
            tag = "1"
        )]
        pub connector_type: i32,
        /// Connector configuration, as an encoded JSON object.
        /// This may be a partial specification (for example, a Docker image),
        /// providing only enough information to fetch the remainder of the
        /// specification schema.
        #[prost(string, tag = "2")]
        pub config_json: ::prost::alloc::string::String,
    }
    /// Validate a materialization configuration and proposed bindings.
    /// Validate is run out-of-band with ongoing capture invocations.
    /// It's purpose is to confirm that the proposed configuration
    /// is likely to succeed if applied and run, or to report any
    /// potential issues for the user to address.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Validate {
        /// Name of the materialization being validated.
        #[prost(string, tag = "1")]
        pub name: ::prost::alloc::string::String,
        /// Connector type addressed by this request.
        #[prost(
            enumeration = "super::super::flow::materialization_spec::ConnectorType",
            tag = "2"
        )]
        pub connector_type: i32,
        /// Connector configuration, as an encoded JSON object.
        #[prost(string, tag = "3")]
        pub config_json: ::prost::alloc::string::String,
        #[prost(message, repeated, tag = "4")]
        pub bindings: ::prost::alloc::vec::Vec<validate::Binding>,
        /// Network ports of this proposed materialization.
        #[prost(message, repeated, tag = "5")]
        pub network_ports: ::prost::alloc::vec::Vec<super::super::flow::NetworkPort>,
    }
    /// Nested message and enum types in `Validate`.
    pub mod validate {
        /// Bindings of endpoint resources and collections from which they would be
        /// materialized. Bindings are ordered and unique on the bound collection name.
        #[allow(clippy::derive_partial_eq_without_eq)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Binding {
            /// JSON-encoded object which specifies the endpoint resource to be materialized.
            #[prost(string, tag = "1")]
            pub resource_config_json: ::prost::alloc::string::String,
            /// Collection to be materialized.
            #[prost(message, optional, tag = "2")]
            pub collection: ::core::option::Option<
                super::super::super::flow::CollectionSpec,
            >,
            /// Projection configuration, keyed by the projection field name,
            /// with JSON-encoded and driver-defined configuration objects.
            #[prost(btree_map = "string, string", tag = "3")]
            pub field_config_json_map: ::prost::alloc::collections::BTreeMap<
                ::prost::alloc::string::String,
                ::prost::alloc::string::String,
            >,
        }
    }
    /// Apply a materialization configuration and bindings to its endpoint.
    /// Apply is run out-of-band with ongoing connector invocations,
    /// and may be run many times for a single materialization name,
    /// where each invocation has varying bindings, or even no bindings.
    /// The connector performs any required setup or cleanup.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Apply {
        /// Materialization to be applied.
        #[prost(message, optional, tag = "1")]
        pub materialization: ::core::option::Option<
            super::super::flow::MaterializationSpec,
        >,
        /// Version of the MaterializationSpec being applied.
        #[prost(string, tag = "2")]
        pub version: ::prost::alloc::string::String,
        /// Is this Apply a dry-run? If so, no action is undertaken and Apply will
        /// report only what would have happened.
        #[prost(bool, tag = "3")]
        pub dry_run: bool,
    }
    /// Open a materialization stream.
    ///
    /// If the Flow recovery log is authoritative:
    /// The driver is given its last committed checkpoint state in this request.
    /// It MAY return a runtime checkpoint in its opened response -- perhaps an older
    /// Flow checkpoint which was previously embedded within its driver checkpoint.
    ///
    /// If the remote store is authoritative:
    /// The driver MUST fence off other streams of this materialization that
    /// overlap the provided [key_begin, key_end) range, such that those streams
    /// cannot issue further commits. The driver MUST return its stored runtime
    /// checkpoint for this materialization and range [key_begin, key_end]
    /// in its Opened response.
    ///
    /// After Open, the runtime will send only Load, Flush, Store,
    /// StartCommit, and Acknowledge.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Open {
        /// Materialization to be transacted.
        #[prost(message, optional, tag = "1")]
        pub materialization: ::core::option::Option<
            super::super::flow::MaterializationSpec,
        >,
        /// Version of the opened MaterializationSpec.
        /// The driver may want to require that this match the version last
        /// provided to a successful Apply RPC. It's possible that it won't,
        /// due to expected propagation races in Flow's distributed runtime.
        #[prost(string, tag = "2")]
        pub version: ::prost::alloc::string::String,
        /// Range of documents to be processed by this invocation.
        #[prost(message, optional, tag = "3")]
        pub range: ::core::option::Option<super::super::flow::RangeSpec>,
        /// Last-persisted connector checkpoint state from a previous invocation.
        #[prost(string, tag = "4")]
        pub state_json: ::prost::alloc::string::String,
    }
    /// Load a document identified by its key. The given key may have never before been stored,
    /// but a given key will be sent in a transaction Load just one time.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Load {
        /// Index of the Open binding for which this document is to be loaded.
        #[prost(uint32, tag = "1")]
        pub binding: u32,
        /// key tuple, as an array of key components.
        /// Ordering matches `keys` of the materialization's field selection.
        #[prost(string, tag = "2")]
        pub key_json: ::prost::alloc::string::String,
        /// Packed tuple of the document key to load.
        #[prost(bytes = "bytes", tag = "3")]
        pub key_packed: ::prost::bytes::Bytes,
    }
    /// Flush loads. No further Loads will be sent in this transaction,
    /// and the runtime will await the connectors's remaining Loaded
    /// responses followed by one Flushed response.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Flush {}
    /// Store documents updated by the current transaction.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Store {
        /// Index of the Open binding for which this document is to be stored.
        #[prost(uint32, tag = "1")]
        pub binding: u32,
        /// Key tuple, as an array of key components.
        /// Ordering matches `keys` of the materialization's field selection.
        #[prost(string, tag = "2")]
        pub key_json: ::prost::alloc::string::String,
        /// Packed FoundationDB tuple of the document key to store.
        #[prost(bytes = "bytes", tag = "3")]
        pub key_packed: ::prost::bytes::Bytes,
        /// Values tuple, as an array of value components.
        /// Ordering matches `values` of the materialization's field selection.
        #[prost(string, tag = "4")]
        pub values_json: ::prost::alloc::string::String,
        /// Packed FoundationDB tuple of the document values to store.
        #[prost(bytes = "bytes", tag = "5")]
        pub values_packed: ::prost::bytes::Bytes,
        /// JSON document to store.
        #[prost(string, tag = "6")]
        pub doc_json: ::prost::alloc::string::String,
        /// Exists is true if this document has previously been loaded or stored.
        #[prost(bool, tag = "7")]
        pub exists: bool,
    }
    /// Mark the end of the Store phase, and if the remote store is authoritative,
    /// instruct it to start committing its transaction.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct StartCommit {
        /// Flow runtime checkpoint to commit with this transaction.
        #[prost(message, optional, tag = "1")]
        pub runtime_checkpoint: ::core::option::Option<
            ::proto_gazette::consumer::Checkpoint,
        >,
    }
    /// Acknowledge to the connector that the previous transaction
    /// has committed to the Flow runtime's recovery log.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Acknowledge {}
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Response {
    #[prost(message, optional, tag = "1")]
    pub spec: ::core::option::Option<response::Spec>,
    #[prost(message, optional, tag = "2")]
    pub validated: ::core::option::Option<response::Validated>,
    #[prost(message, optional, tag = "3")]
    pub applied: ::core::option::Option<response::Applied>,
    #[prost(message, optional, tag = "4")]
    pub opened: ::core::option::Option<response::Opened>,
    #[prost(message, optional, tag = "5")]
    pub loaded: ::core::option::Option<response::Loaded>,
    #[prost(message, optional, tag = "6")]
    pub flushed: ::core::option::Option<response::Flushed>,
    #[prost(message, optional, tag = "7")]
    pub started_commit: ::core::option::Option<response::StartedCommit>,
    #[prost(message, optional, tag = "8")]
    pub acknowledged: ::core::option::Option<response::Acknowledged>,
    /// Reserved for internal use.
    #[prost(message, optional, tag = "100")]
    pub internal: ::core::option::Option<::pbjson_types::Any>,
}
/// Nested message and enum types in `Response`.
pub mod response {
    /// Spec responds to Request.Spec.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Spec {
        /// Protocol version must be 3032023.
        #[prost(uint32, tag = "1")]
        pub protocol: u32,
        /// JSON schema of the connector's configuration.
        #[prost(string, tag = "2")]
        pub config_schema_json: ::prost::alloc::string::String,
        /// JSON schema of the connecor's resource configuration.
        #[prost(string, tag = "3")]
        pub resource_config_schema_json: ::prost::alloc::string::String,
        /// URL for connector's documention.
        #[prost(string, tag = "4")]
        pub documentation_url: ::prost::alloc::string::String,
        /// Optional OAuth2 configuration.
        #[prost(message, optional, tag = "5")]
        pub oauth2: ::core::option::Option<super::super::flow::OAuth2>,
    }
    /// Validated responds to Request.Validate.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Validated {
        #[prost(message, repeated, tag = "1")]
        pub bindings: ::prost::alloc::vec::Vec<validated::Binding>,
    }
    /// Nested message and enum types in `Validated`.
    pub mod validated {
        /// Constraint constrains the use of a flow.Projection within a materialization.
        #[allow(clippy::derive_partial_eq_without_eq)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Constraint {
            #[prost(enumeration = "constraint::Type", tag = "2")]
            pub r#type: i32,
            /// Optional human readable reason for the given constraint.
            /// Implementations are strongly encouraged to supply a descriptive message.
            #[prost(string, tag = "3")]
            pub reason: ::prost::alloc::string::String,
        }
        /// Nested message and enum types in `Constraint`.
        pub mod constraint {
            /// Type encodes a constraint type for this flow.Projection.
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
                Invalid = 0,
                /// This specific projection must be present.
                FieldRequired = 1,
                /// At least one projection with this location pointer must be present.
                LocationRequired = 2,
                /// A projection with this location is recommended, and should be included by
                /// default.
                LocationRecommended = 3,
                /// This projection may be included, but should be omitted by default.
                FieldOptional = 4,
                /// This projection must not be present in the materialization.
                FieldForbidden = 5,
                /// This specific projection is required but is also unacceptable (e.x.,
                /// because it uses an incompatible type with a previous applied version).
                Unsatisfiable = 6,
            }
            impl Type {
                /// String value of the enum field names used in the ProtoBuf definition.
                ///
                /// The values are not transformed in any way and thus are considered stable
                /// (if the ProtoBuf definition does not change) and safe for programmatic use.
                pub fn as_str_name(&self) -> &'static str {
                    match self {
                        Type::Invalid => "INVALID",
                        Type::FieldRequired => "FIELD_REQUIRED",
                        Type::LocationRequired => "LOCATION_REQUIRED",
                        Type::LocationRecommended => "LOCATION_RECOMMENDED",
                        Type::FieldOptional => "FIELD_OPTIONAL",
                        Type::FieldForbidden => "FIELD_FORBIDDEN",
                        Type::Unsatisfiable => "UNSATISFIABLE",
                    }
                }
                /// Creates an enum from field names used in the ProtoBuf definition.
                pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
                    match value {
                        "INVALID" => Some(Self::Invalid),
                        "FIELD_REQUIRED" => Some(Self::FieldRequired),
                        "LOCATION_REQUIRED" => Some(Self::LocationRequired),
                        "LOCATION_RECOMMENDED" => Some(Self::LocationRecommended),
                        "FIELD_OPTIONAL" => Some(Self::FieldOptional),
                        "FIELD_FORBIDDEN" => Some(Self::FieldForbidden),
                        "UNSATISFIABLE" => Some(Self::Unsatisfiable),
                        _ => None,
                    }
                }
            }
        }
        /// Validation responses for each binding of the request, and matching the
        /// request ordering. Each Binding must have a unique resource_path.
        #[allow(clippy::derive_partial_eq_without_eq)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Binding {
            /// Constraints over collection projections imposed by the Driver,
            /// keyed by the projection field name. Projections of the CollectionSpec
            /// which are missing from constraints are implicitly forbidden.
            #[prost(btree_map = "string, message", tag = "1")]
            pub constraints: ::prost::alloc::collections::BTreeMap<
                ::prost::alloc::string::String,
                Constraint,
            >,
            /// Components of the resource path which fully qualify the resource
            /// identified by this binding.
            /// - For an RDBMS, this might be []{dbname, schema, table}.
            /// - For Kafka, this might be []{topic}.
            /// - For Redis, this might be []{key_prefix}.
            #[prost(string, repeated, tag = "2")]
            pub resource_path: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
            /// Materialize combined delta updates of documents rather than full
            /// reductions.
            ///
            /// When set, the Flow runtime will not attempt to load documents via
            /// Request.Load, and also disables re-use of cached documents
            /// stored in prior transactions. Each stored document is exclusively
            /// combined from updates processed by the runtime within the current
            /// transaction only.
            ///
            /// This is appropriate for drivers over streams, WebHooks, and append-only
            /// files.
            ///
            /// For example, given a collection which reduces a sum count for each key,
            /// its materialization will produce a stream of delta updates to the count,
            /// such that a reader of the stream will arrive at the correct total count.
            #[prost(bool, tag = "3")]
            pub delta_updates: bool,
        }
    }
    /// Applied responds to Request.Apply.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Applied {
        /// Human-readable description of the action that the Driver took (or, if
        /// dry_run, would have taken). If empty, this Apply is to be considered a
        /// "no-op".
        #[prost(string, tag = "1")]
        pub action_description: ::prost::alloc::string::String,
    }
    /// Opened responds to Request.Open.
    /// After Opened, the connector sends only Loaded, Flushed,
    /// StartedCommit, and Acknowledged as per the materialization
    /// protocol.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Opened {
        /// Flow runtime checkpoint to begin processing from.
        /// If empty, the most recent checkpoint of the Flow recovery log is used.
        ///
        /// Or, a driver may send the value []byte{0xf8, 0xff, 0xff, 0xff, 0xf, 0x1}
        /// to explicitly begin processing from a zero-valued checkpoint, effectively
        /// rebuilding the materialization from scratch. This sentinel is a trivial
        /// encoding of the max-value 2^29-1 protobuf tag with boolean true.
        #[prost(message, optional, tag = "1")]
        pub runtime_checkpoint: ::core::option::Option<
            ::proto_gazette::consumer::Checkpoint,
        >,
    }
    /// Loaded responds to Request.Load.
    /// It returns documents of requested keys which have previously been stored.
    /// Keys not found in the store MUST be omitted. Documents may be in any order,
    /// both within and across Loaded response messages, but a document of a given
    /// key MUST be sent at most one time in a Transaction.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Loaded {
        /// Index of the Open binding for which this document was loaded.
        #[prost(uint32, tag = "1")]
        pub binding: u32,
        /// Loaded JSON document.
        #[prost(string, tag = "2")]
        pub doc_json: ::prost::alloc::string::String,
    }
    /// Flushed responds to a Request.Flush.
    /// The driver will send no further Loaded responses.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Flushed {}
    /// StartedCommit responds to a Request.StartCommit.
    /// The driver has processed all Store requests, it has started to commit its
    /// transaction (if it has one), and it is now ready for the runtime to start
    /// committing to its own recovery log.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct StartedCommit {
        #[prost(message, optional, tag = "1")]
        pub state: ::core::option::Option<super::super::flow::ConnectorState>,
    }
    /// Notify the runtime that the previous driver transaction has committed
    /// to the endpoint store (where applicable). On receipt, the runtime may
    /// begin to flush, store, and commit a next (pipelined) transaction.
    ///
    /// Acknowledged is _not_ a direct response to Request.Acknowledge,
    /// and Acknowledge vs Acknowledged may be written in either order.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Acknowledged {}
}
/// Extra messages used by connectors
/// TODO(johnny): Do we still need this?
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Extra {}
/// Nested message and enum types in `Extra`.
pub mod extra {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ValidateExistingProjectionRequest {
        #[prost(message, optional, tag = "1")]
        pub existing_binding: ::core::option::Option<
            super::super::flow::materialization_spec::Binding,
        >,
        #[prost(message, optional, tag = "2")]
        pub proposed_binding: ::core::option::Option<super::request::validate::Binding>,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ValidateBindingAgainstConstraints {
        #[prost(message, optional, tag = "1")]
        pub binding: ::core::option::Option<
            super::super::flow::materialization_spec::Binding,
        >,
        #[prost(btree_map = "string, message", tag = "2")]
        pub constraints: ::prost::alloc::collections::BTreeMap<
            ::prost::alloc::string::String,
            super::response::validated::Constraint,
        >,
    }
}
