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
}
/// DiscoverRequest is the request type of the Discover RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DiscoverRequest {
    /// Endpoint type addressed by this request.
    #[prost(enumeration="super::flow::EndpointType", tag="1")]
    pub endpoint_type: i32,
    /// Driver specification, as an encoded JSON object.
    #[prost(string, tag="2")]
    pub endpoint_spec_json: ::prost::alloc::string::String,
}
/// DiscoverResponse is the response type of the Discover RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DiscoverResponse {
    #[prost(message, repeated, tag="1")]
    pub bindings: ::prost::alloc::vec::Vec<discover_response::Binding>,
}
/// Nested message and enum types in `DiscoverResponse`.
pub mod discover_response {
    /// Potential bindings which the capture could provide.
    /// Bindings may be returned in any order.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Binding {
        /// A recommended display name for this discovered binding.
        #[prost(string, tag="1")]
        pub recommended_name: ::prost::alloc::string::String,
        /// JSON-encoded object which specifies the endpoint resource to be captured.
        #[prost(string, tag="2")]
        pub resource_spec_json: ::prost::alloc::string::String,
        /// JSON schema of documents produced by this binding.
        #[prost(string, tag="3")]
        pub document_schema_json: ::prost::alloc::string::String,
        /// Composite key of documents (if known), as JSON-Pointers.
        #[prost(string, repeated, tag="4")]
        pub key_ptrs: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    }
}
/// ValidateRequest is the request type of the Validate RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValidateRequest {
    /// Name of the capture being validated.
    #[prost(string, tag="1")]
    pub capture: ::prost::alloc::string::String,
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
    /// Bindings of endpoint resources and collections to which they would be
    /// captured. Bindings are ordered and unique on the bound collection name.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Binding {
        /// JSON-encoded object which specifies the endpoint resource to be captured.
        #[prost(string, tag="1")]
        pub resource_spec_json: ::prost::alloc::string::String,
        /// Collection to be captured.
        #[prost(message, optional, tag="2")]
        pub collection: ::core::option::Option<super::super::flow::CollectionSpec>,
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
        /// Components of the resource path which fully qualify the resource
        /// identified by this binding.
        /// - For an RDBMS, this might be []{dbname, schema, table}.
        /// - For Kafka, this might be []{topic}.
        /// - For Redis, this might be []{key_prefix}.
        #[prost(string, repeated, tag="1")]
        pub resource_path: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    }
}
/// ApplyRequest is the request type of the ApplyUpsert and ApplyDelete RPCs.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ApplyRequest {
    /// Capture to be applied.
    #[prost(message, optional, tag="1")]
    pub capture: ::core::option::Option<super::flow::CaptureSpec>,
    /// Version of the CaptureSpec being applied.
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
/// Documents is a set of documents drawn from a binding of the capture.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Documents {
    /// The capture binding for documents of this message.
    #[prost(uint32, tag="1")]
    pub binding: u32,
    /// Byte arena of the response.
    #[prost(bytes="vec", tag="2")]
    pub arena: ::prost::alloc::vec::Vec<u8>,
    /// Captured JSON documents.
    #[prost(message, repeated, tag="3")]
    pub docs_json: ::prost::alloc::vec::Vec<super::flow::Slice>,
}
/// Acknowledge is a notification that a Checkpoint has committed to the
/// Flow runtime's recovery log.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Acknowledge {
}
/// PullRequest is the request type of a Driver.Pull RPC.
/// It will have exactly one top-level field set, which represents its message
/// type.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PullRequest {
    #[prost(message, optional, tag="1")]
    pub open: ::core::option::Option<pull_request::Open>,
    /// Tell the driver that its Checkpoint has committed to the Flow recovery log.
    #[prost(message, optional, tag="2")]
    pub acknowledge: ::core::option::Option<Acknowledge>,
}
/// Nested message and enum types in `PullRequest`.
pub mod pull_request {
    /// Open opens a Pull of the driver, and is sent exactly once as the first
    /// message of the stream.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Open {
        /// CaptureSpec to be pulled.
        #[prost(message, optional, tag="1")]
        pub capture: ::core::option::Option<super::super::flow::CaptureSpec>,
        /// Version of the opened CaptureSpec.
        /// The driver may want to require that this match the version last
        /// provided to a successful Apply RPC. It's possible that it won't,
        /// due to expected propagation races in Flow's distributed runtime.
        #[prost(string, tag="2")]
        pub version: ::prost::alloc::string::String,
        /// [key_begin, key_end] inclusive range of keys processed by this
        /// transaction stream. Ranges reflect the disjoint chunks of ownership
        /// specific to each instance of a scale-out capture implementation.
        #[prost(fixed32, tag="3")]
        pub key_begin: u32,
        #[prost(fixed32, tag="4")]
        pub key_end: u32,
        /// Last-persisted driver checkpoint from a previous capture stream.
        /// Or empty, if the driver has cleared or never set its checkpoint.
        #[prost(bytes="vec", tag="5")]
        pub driver_checkpoint_json: ::prost::alloc::vec::Vec<u8>,
        /// If true, perform a blocking tail of the capture.
        /// If false, produce all ready output and then close the stream.
        #[prost(bool, tag="6")]
        pub tail: bool,
    }
}
/// PullResponse is the response type of a Driver.Pull RPC.
/// It will have exactly one top-level field set, which represents its message
/// type.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PullResponse {
    #[prost(message, optional, tag="1")]
    pub opened: ::core::option::Option<pull_response::Opened>,
    /// Captured documents of the stream.
    #[prost(message, optional, tag="2")]
    pub documents: ::core::option::Option<Documents>,
    /// Checkpoint all preceeding Documents of this stream.
    #[prost(message, optional, tag="3")]
    pub checkpoint: ::core::option::Option<super::flow::DriverCheckpoint>,
}
/// Nested message and enum types in `PullResponse`.
pub mod pull_response {
    /// Opened responds to PullRequest.Open of the runtime,
    /// and is sent exactly once as the first message of the stream.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Opened {
    }
}
/// PushRequest is the request message of the Runtime.Push RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PushRequest {
    #[prost(message, optional, tag="1")]
    pub open: ::core::option::Option<push_request::Open>,
    /// Captured documents of the stream.
    #[prost(message, optional, tag="2")]
    pub documents: ::core::option::Option<Documents>,
    /// Checkpoint all preceeding Documents of this stream.
    #[prost(message, optional, tag="3")]
    pub checkpoint: ::core::option::Option<super::flow::DriverCheckpoint>,
}
/// Nested message and enum types in `PushRequest`.
pub mod push_request {
    /// Open opens a Push of the runtime, and is sent exactly once as the first
    /// message of the stream.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Open {
        /// Header identifies a specific Shard and Route to which this stream is
        /// directed. It's optional, and is typically attached by a proxying peer.
        #[prost(message, optional, tag="1")]
        pub header: ::core::option::Option<super::super::protocol::Header>,
        /// Name of the capture under which we're pushing.
        #[prost(string, tag="2")]
        pub capture: ::prost::alloc::string::String,
    }
}
/// PushResponse is the response message of the Runtime.Push RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PushResponse {
    #[prost(message, optional, tag="1")]
    pub opened: ::core::option::Option<push_response::Opened>,
    /// Tell the driver that its Checkpoint has committed to the Flow recovery log.
    #[prost(message, optional, tag="2")]
    pub acknowledge: ::core::option::Option<Acknowledge>,
}
/// Nested message and enum types in `PushResponse`.
pub mod push_response {
    /// Opened responds to PushRequest.Open of the driver,
    /// and is sent exactly once as the first message of the stream.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Opened {
        /// Status of the Push open.
        #[prost(enumeration="super::super::consumer::Status", tag="1")]
        pub status: i32,
        /// Header of the response.
        #[prost(message, optional, tag="2")]
        pub header: ::core::option::Option<super::super::protocol::Header>,
        /// CaptureSpec to be pushed.
        #[prost(message, optional, tag="3")]
        pub capture: ::core::option::Option<super::super::flow::CaptureSpec>,
        /// Version of the opened CaptureSpec.
        /// The driver may want to require that this match the version last
        /// provided to a successful Apply RPC. It's possible that it won't,
        /// due to expected propagation races in Flow's distributed runtime.
        #[prost(string, tag="4")]
        pub version: ::prost::alloc::string::String,
        /// [key_begin, key_end] inclusive range of keys processed by this
        /// transaction stream. Ranges reflect the disjoint chunks of ownership
        /// specific to each instance of a scale-out capture implementation.
        #[prost(fixed32, tag="5")]
        pub key_begin: u32,
        #[prost(fixed32, tag="6")]
        pub key_end: u32,
        /// Last-persisted driver checkpoint from a previous capture stream.
        /// Or empty, if the driver has cleared or never set its checkpoint.
        #[prost(bytes="vec", tag="7")]
        pub driver_checkpoint_json: ::prost::alloc::vec::Vec<u8>,
    }
}
