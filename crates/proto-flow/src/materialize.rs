/// Constraint constrains the use of a flow.Projection within a materialization.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Constraint {
    #[prost(enumeration="constraint::Type", tag="2")]
    pub r#type: i32,
    /// Optional human readable reason for the given constraint.
    /// Implementations are strongly encouraged to supply a descriptive message.
    #[prost(string, tag="3")]
    pub reason: ::prost::alloc::string::String,
}
/// Nested message and enum types in `Constraint`.
pub mod constraint {
    /// Type encodes a constraint type for this flow.Projection.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Type {
        /// This specific projection must be present.
        FieldRequired = 0,
        /// At least one projection with this location pointer must be present.
        LocationRequired = 1,
        /// A projection with this location is recommended, and should be included by
        /// default.
        LocationRecommended = 2,
        /// This projection may be included, but should be omitted by default.
        FieldOptional = 3,
        /// This projection must not be present in the materialization.
        FieldForbidden = 4,
        /// This specific projection is required but is also unacceptable (e.x.,
        /// because it uses an incompatible type with a previous applied version).
        Unsatisfiable = 5,
    }
    impl Type {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Type::FieldRequired => "FIELD_REQUIRED",
                Type::LocationRequired => "LOCATION_REQUIRED",
                Type::LocationRecommended => "LOCATION_RECOMMENDED",
                Type::FieldOptional => "FIELD_OPTIONAL",
                Type::FieldForbidden => "FIELD_FORBIDDEN",
                Type::Unsatisfiable => "UNSATISFIABLE",
            }
        }
    }
}
/// SpecRequest is the request type of the Spec RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SpecRequest {
    /// Endpoint type addressed by this request.
    #[prost(enumeration="super::flow::EndpointType", tag="1")]
    pub endpoint_type: i32,
    /// Driver specification, as an encoded JSON object.
    /// This may be a partial specification (for example, a Docker image),
    /// providing only enough information to fetch the remainder of the
    /// specification schema.
    #[prost(string, tag="2")]
    pub endpoint_spec_json: ::prost::alloc::string::String,
}
/// SpecResponse is the response type of the Spec RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SpecResponse {
    /// JSON schema of an endpoint specification.
    #[prost(string, tag="1")]
    pub endpoint_spec_schema_json: ::prost::alloc::string::String,
    /// JSON schema of a resource specification.
    #[prost(string, tag="2")]
    pub resource_spec_schema_json: ::prost::alloc::string::String,
    /// URL for connector's documention.
    #[prost(string, tag="3")]
    pub documentation_url: ::prost::alloc::string::String,
    /// OAuth2 spec
    #[prost(message, optional, tag="4")]
    pub oauth2_spec: ::core::option::Option<super::flow::OAuth2Spec>,
}
/// ValidateRequest is the request type of the Validate RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValidateRequest {
    /// Name of the materialization being validated.
    #[prost(string, tag="1")]
    pub materialization: ::prost::alloc::string::String,
    /// Endpoint type addressed by this request.
    #[prost(enumeration="super::flow::EndpointType", tag="2")]
    pub endpoint_type: i32,
    /// Driver specification, as an encoded JSON object.
    #[prost(string, tag="3")]
    pub endpoint_spec_json: ::prost::alloc::string::String,
    #[prost(message, repeated, tag="4")]
    pub bindings: ::prost::alloc::vec::Vec<validate_request::Binding>,
}
/// Nested message and enum types in `ValidateRequest`.
pub mod validate_request {
    /// Bindings of endpoint resources and collections from which they would be
    /// materialized. Bindings are ordered and unique on the bound collection name.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Binding {
        /// JSON-encoded object which specifies the endpoint resource to be
        /// materialized.
        #[prost(string, tag="1")]
        pub resource_spec_json: ::prost::alloc::string::String,
        /// Collection to be materialized.
        #[prost(message, optional, tag="2")]
        pub collection: ::core::option::Option<super::super::flow::CollectionSpec>,
        /// Projection configuration, keyed by the projection field name,
        /// with JSON-encoded and driver-defined configuration objects.
        #[prost(map="string, string", tag="3")]
        pub field_config_json: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    }
}
/// ValidateResponse is the response type of the Validate RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValidateResponse {
    #[prost(message, repeated, tag="1")]
    pub bindings: ::prost::alloc::vec::Vec<validate_response::Binding>,
}
/// Nested message and enum types in `ValidateResponse`.
pub mod validate_response {
    /// Validation responses for each binding of the request, and matching the
    /// request ordering. Each Binding must have a unique resource_path.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Binding {
        /// Constraints over collection projections imposed by the Driver,
        /// keyed by the projection field name. Projections of the CollectionSpec
        /// which are missing from constraints are implicitly forbidden.
        #[prost(map="string, message", tag="1")]
        pub constraints: ::std::collections::HashMap<::prost::alloc::string::String, super::Constraint>,
        /// Components of the resource path which fully qualify the resource
        /// identified by this binding.
        /// - For an RDBMS, this might be []{dbname, schema, table}.
        /// - For Kafka, this might be []{topic}.
        /// - For Redis, this might be []{key_prefix}.
        #[prost(string, repeated, tag="2")]
        pub resource_path: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
        /// Materialize combined delta updates of documents rather than full
        /// reductions.
        ///
        /// When set, the Flow runtime will not attempt to load documents via
        /// TransactionRequest.Load, and also disables re-use of cached documents
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
        #[prost(bool, tag="3")]
        pub delta_updates: bool,
    }
}
/// ApplyRequest is the request type of the ApplyUpsert and ApplyDelete RPCs.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ApplyRequest {
    /// Materialization to be applied.
    #[prost(message, optional, tag="1")]
    pub materialization: ::core::option::Option<super::flow::MaterializationSpec>,
    /// Version of the MaterializationSpec being applied.
    #[prost(string, tag="2")]
    pub version: ::prost::alloc::string::String,
    /// Is this Apply a dry-run? If so, no action is undertaken and Apply will
    /// report only what would have happened.
    #[prost(bool, tag="3")]
    pub dry_run: bool,
}
/// ApplyResponse is the response type of the ApplyUpsert and ApplyDelete RPCs.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ApplyResponse {
    /// Human-readable description of the action that the Driver took (or, if
    /// dry_run, would have taken). If empty, this Apply is to be considered a
    /// "no-op".
    #[prost(string, tag="1")]
    pub action_description: ::prost::alloc::string::String,
}
/// TransactionRequest is the request type of a Transaction RPC.
/// It will have exactly one top-level field set, which represents its message
/// type.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransactionRequest {
    #[prost(message, optional, tag="1")]
    pub open: ::core::option::Option<transaction_request::Open>,
    #[prost(message, optional, tag="2")]
    pub load: ::core::option::Option<transaction_request::Load>,
    #[prost(message, optional, tag="3")]
    pub flush: ::core::option::Option<transaction_request::Flush>,
    #[prost(message, optional, tag="4")]
    pub store: ::core::option::Option<transaction_request::Store>,
    #[prost(message, optional, tag="5")]
    pub start_commit: ::core::option::Option<transaction_request::StartCommit>,
    #[prost(message, optional, tag="6")]
    pub acknowledge: ::core::option::Option<transaction_request::Acknowledge>,
}
/// Nested message and enum types in `TransactionRequest`.
pub mod transaction_request {
    /// Open a transaction stream.
    ///
    /// If the Flow recovery log is authoritative:
    /// The driver is given its last committed driver checkpoint in this request.
    /// It MAY return a Flow checkpoint in its opened response -- perhaps an older
    /// Flow checkpoint which was previously embedded within its driver checkpoint.
    ///
    /// If the remote store is authoritative:
    /// The driver MUST fence off other streams of this materialization that
    /// overlap the provided [key_begin, key_end) range, such that those streams
    /// cannot issue further commits. The driver MUST return its stored checkpoint
    /// for this materialization and range [key_begin, key_end] in its Opened
    /// response.
    ///
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Open {
        /// Materialization to be transacted.
        #[prost(message, optional, tag="1")]
        pub materialization: ::core::option::Option<super::super::flow::MaterializationSpec>,
        /// Version of the opened MaterializationSpec.
        /// The driver may want to require that this match the version last
        /// provided to a successful Apply RPC. It's possible that it won't,
        /// due to expected propagation races in Flow's distributed runtime.
        #[prost(string, tag="2")]
        pub version: ::prost::alloc::string::String,
        /// [begin, end] inclusive range of keys processed by this transaction
        /// stream. Ranges are with respect to a 32-bit hash of a packed document
        /// key.
        #[prost(fixed32, tag="3")]
        pub key_begin: u32,
        #[prost(fixed32, tag="4")]
        pub key_end: u32,
        /// Last-persisted driver checkpoint committed in the Flow runtime recovery
        /// log. Or empty, if the driver has cleared or never set its checkpoint.
        #[prost(bytes="vec", tag="5")]
        pub driver_checkpoint_json: ::prost::alloc::vec::Vec<u8>,
    }
    /// Load one or more documents identified by key.
    /// Keys may included documents which have never before been stored,
    /// but a given key will be sent in a transaction Load just one time.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Load {
        /// The materialization binding for documents of this Load request.
        #[prost(uint32, tag="1")]
        pub binding: u32,
        /// Byte arena of the request.
        #[prost(bytes="vec", tag="2")]
        pub arena: ::prost::alloc::vec::Vec<u8>,
        /// Packed tuples of collection keys, enumerating the documents to load.
        #[prost(message, repeated, tag="3")]
        pub packed_keys: ::prost::alloc::vec::Vec<super::super::flow::Slice>,
    }
    /// Flush loads. No further Loads will be sent in this transaction,
    /// and the runtime will await the driver's remaining Loaded responses
    /// followed by one Flushed response.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Flush {
    }
    /// Store documents of this transaction commit.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Store {
        /// The materialization binding for documents of this Store request.
        #[prost(uint32, tag="1")]
        pub binding: u32,
        /// Byte arena of the request.
        #[prost(bytes="vec", tag="2")]
        pub arena: ::prost::alloc::vec::Vec<u8>,
        /// Packed tuples holding keys of each document.
        #[prost(message, repeated, tag="3")]
        pub packed_keys: ::prost::alloc::vec::Vec<super::super::flow::Slice>,
        /// Packed tuples holding values for each document.
        #[prost(message, repeated, tag="4")]
        pub packed_values: ::prost::alloc::vec::Vec<super::super::flow::Slice>,
        /// JSON documents.
        #[prost(message, repeated, tag="5")]
        pub docs_json: ::prost::alloc::vec::Vec<super::super::flow::Slice>,
        /// Exists is true if this document as previously been loaded or stored.
        #[prost(bool, repeated, tag="6")]
        pub exists: ::prost::alloc::vec::Vec<bool>,
    }
    /// Mark the end of the Store phase, and if the remote store is authoritative,
    /// instruct it to start committing its transaction.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct StartCommit {
        /// Flow runtime checkpoint to commit with this transaction.
        #[prost(bytes="vec", tag="1")]
        pub runtime_checkpoint: ::prost::alloc::vec::Vec<u8>,
    }
    /// Notify the driver that the previous transaction has committed to the Flow
    /// runtime's recovery log.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Acknowledge {
    }
}
/// TransactionResponse is the response type of a Transaction RPC.
/// It will have exactly one top-level field set, which represents its message
/// type.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransactionResponse {
    #[prost(message, optional, tag="1")]
    pub opened: ::core::option::Option<transaction_response::Opened>,
    #[prost(message, optional, tag="2")]
    pub loaded: ::core::option::Option<transaction_response::Loaded>,
    #[prost(message, optional, tag="3")]
    pub flushed: ::core::option::Option<transaction_response::Flushed>,
    #[prost(message, optional, tag="4")]
    pub started_commit: ::core::option::Option<transaction_response::StartedCommit>,
    #[prost(message, optional, tag="5")]
    pub acknowledged: ::core::option::Option<transaction_response::Acknowledged>,
}
/// Nested message and enum types in `TransactionResponse`.
pub mod transaction_response {
    /// Opened responds to TransactionRequest.Open of the client.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Opened {
        /// Flow runtime checkpoint to begin processing from.
        /// If empty, the most recent checkpoint of the Flow recovery log is used.
        ///
        /// Or, a driver may send the value []byte{0xf8, 0xff, 0xff, 0xff, 0xf, 0x1}
        /// to explicitly begin processing from a zero-valued checkpoint, effectively
        /// rebuilding the materialization from scratch. This sentinel is a trivial
        /// encoding of the max-value 2^29-1 protobuf tag with boolean true.
        #[prost(bytes="vec", tag="1")]
        pub runtime_checkpoint: ::prost::alloc::vec::Vec<u8>,
    }
    /// Loaded responds to TransactionRequest.Loads of the client.
    /// It returns documents of requested keys which have previously been stored.
    /// Keys not found in the store MUST be omitted. Documents may be in any order,
    /// both within and across Loaded response messages, but a document of a given
    /// key MUST be sent at most one time in a Transaction.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Loaded {
        /// The materialization binding for documents of this Loaded response.
        #[prost(uint32, tag="1")]
        pub binding: u32,
        /// Byte arena of the request.
        #[prost(bytes="vec", tag="2")]
        pub arena: ::prost::alloc::vec::Vec<u8>,
        /// Loaded JSON documents.
        #[prost(message, repeated, tag="3")]
        pub docs_json: ::prost::alloc::vec::Vec<super::super::flow::Slice>,
    }
    /// Flushed responds to a TransactionRequest.Flush of the client.
    /// The driver will send no further Loaded responses.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Flushed {
    }
    /// StartedCommit responds to a TransactionRequest.StartCommit of the client.
    /// The driver has processed all Store requests, it has started to commit its
    /// transaction (if it has one), and it is now ready for the runtime to start
    /// committing to its own recovery log.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct StartedCommit {
        #[prost(message, optional, tag="1")]
        pub driver_checkpoint: ::core::option::Option<super::super::flow::DriverCheckpoint>,
    }
    /// Notify the runtime that the previous driver transaction has committed
    /// to the endpoint store (where applicable). On receipt, the runtime may
    /// begin to flush, store, and commit a next (pipelined) transaction.
    ///
    /// Acknowledged is _not_ a direct response to TransactionRequest.Acknowledge,
    /// and Acknowledge vs Acknowledged may be written in either order.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Acknowledged {
    }
}
/// Extra messages used by connectors
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Extra {
}
/// Nested message and enum types in `Extra`.
pub mod extra {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ValidateExistingProjectionRequest {
        /// Existing Materialization Binding
        #[prost(message, optional, tag="1")]
        pub existing_binding: ::core::option::Option<super::super::flow::materialization_spec::Binding>,
        /// Proposed ValidateRequest Binding
        #[prost(message, optional, tag="2")]
        pub proposed_binding: ::core::option::Option<super::validate_request::Binding>,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ValidateBindingAgainstConstraints {
        /// Materialization Binding
        #[prost(message, optional, tag="1")]
        pub binding: ::core::option::Option<super::super::flow::materialization_spec::Binding>,
        /// Constraints map
        #[prost(map="string, message", tag="2")]
        pub constraints: ::std::collections::HashMap<::prost::alloc::string::String, super::Constraint>,
    }
}
