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
    /// Bindings of endpoint resources and collections to which they would be captured.
    /// Bindings are ordered and unique on the bound collection name.
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
    /// Validation responses for each binding of the request,
    /// and matching the request ordering.
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
/// CaptureRequest is the request type of a Capture RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CaptureRequest {
    /// Capture to be run, which is the CaptureSpec
    /// last provided to a successful Validate RPC.
    #[prost(message, optional, tag="1")]
    pub capture: ::core::option::Option<super::flow::CaptureSpec>,
    /// [key_begin, key_end] inclusive range of keys processed by this
    /// transaction stream. Ranges reflect the disjoint chunks of ownership
    /// specific to each instance of a scale-out capture implementation.
    #[prost(fixed32, tag="2")]
    pub key_begin: u32,
    #[prost(fixed32, tag="3")]
    pub key_end: u32,
    /// Last-persisted driver checkpoint from a previous capture stream.
    /// Or empty, if the driver hasn't returned a checkpoint.
    #[prost(bytes="vec", tag="4")]
    pub driver_checkpoint_json: ::prost::alloc::vec::Vec<u8>,
    /// If true, perform a blocking tail of the capture.
    /// If false, produce all ready output and then close the stream.
    #[prost(bool, tag="5")]
    pub tail: bool,
}
/// CaptureResponse is the response type of a Capture RPC.
/// It will have exactly one top-level field set, which represents its message
/// type.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CaptureResponse {
    #[prost(message, optional, tag="1")]
    pub opened: ::core::option::Option<capture_response::Opened>,
    #[prost(message, optional, tag="2")]
    pub captured: ::core::option::Option<capture_response::Captured>,
    #[prost(message, optional, tag="3")]
    pub commit: ::core::option::Option<capture_response::Commit>,
}
/// Nested message and enum types in `CaptureResponse`.
pub mod capture_response {
    /// Opened responds to CaptureRequest of the client,
    /// and is sent exactly once as the first message of the stream.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Opened {
    }
    /// Captured returns documents of the capture stream.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Captured {
        /// The capture binding for documents of this Captured response.
        #[prost(uint32, tag="1")]
        pub binding: u32,
        /// Byte arena of the response.
        #[prost(bytes="vec", tag="2")]
        pub arena: ::prost::alloc::vec::Vec<u8>,
        /// Captured JSON documents.
        #[prost(message, repeated, tag="3")]
        pub docs_json: ::prost::alloc::vec::Vec<super::super::flow::Slice>,
    }
    /// Commit previous captured documents.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Commit {
        /// Optional driver checkpoint of this transaction.
        /// If provided, the most recent checkpoint will be persisted by the
        /// Flow runtime and returned in a future CaptureRequest.
        #[prost(bytes="vec", tag="1")]
        pub driver_checkpoint_json: ::prost::alloc::vec::Vec<u8>,
    }
}
