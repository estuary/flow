/// SpecRequest is the request type of the Spec RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SpecRequest {
    /// Endpoint type addressed by this request.
    #[prost(enumeration = "super::flow::EndpointType", tag = "1")]
    pub endpoint_type: i32,
    /// Driver specification, as an encoded JSON object.
    /// This may be a partial specification (for example, a Docker image),
    /// providing only enough information to fetch the remainder of the
    /// specification schema.
    #[prost(string, tag = "2")]
    pub endpoint_spec_json: ::prost::alloc::string::String,
}
/// SpecResponse is the response type of the Spec RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SpecResponse {
    /// JSON schema of an endpoint specification.
    #[prost(string, tag = "1")]
    pub endpoint_spec_schema_json: ::prost::alloc::string::String,
    /// JSON schema of a resource specification.
    #[prost(string, tag = "2")]
    pub resource_spec_schema_json: ::prost::alloc::string::String,
    /// URL for connector's documention.
    #[prost(string, tag = "3")]
    pub documentation_url: ::prost::alloc::string::String,
}
/// DiscoverRequest is the request type of the Discover RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DiscoverRequest {
    /// Endpoint type addressed by this request.
    #[prost(enumeration = "super::flow::EndpointType", tag = "1")]
    pub endpoint_type: i32,
    /// Driver specification, as an encoded JSON object.
    #[prost(string, tag = "2")]
    pub endpoint_spec_json: ::prost::alloc::string::String,
}
/// DiscoverResponse is the response type of the Discover RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DiscoverResponse {
    #[prost(message, repeated, tag = "1")]
    pub bindings: ::prost::alloc::vec::Vec<discover_response::Binding>,
}
/// Nested message and enum types in `DiscoverResponse`.
pub mod discover_response {
    /// Potential bindings which the capture could provide.
    /// Bindings may be returned in any order.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Binding {
        /// A recommended display name for this discovered binding.
        #[prost(string, tag = "1")]
        pub recommended_name: ::prost::alloc::string::String,
        /// JSON-encoded object which specifies the endpoint resource to be captured.
        #[prost(string, tag = "2")]
        pub resource_spec_json: ::prost::alloc::string::String,
        /// JSON schema of documents produced by this binding.
        #[prost(string, tag = "3")]
        pub document_schema_json: ::prost::alloc::string::String,
        /// Composite key of documents (if known), as JSON-Pointers.
        #[prost(string, repeated, tag = "4")]
        pub key_ptrs: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    }
}
/// ValidateRequest is the request type of the Validate RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValidateRequest {
    /// Name of the capture being validated.
    #[prost(string, tag = "1")]
    pub capture: ::prost::alloc::string::String,
    /// Endpoint type addressed by this request.
    #[prost(enumeration = "super::flow::EndpointType", tag = "2")]
    pub endpoint_type: i32,
    /// Driver specification, as an encoded JSON object.
    #[prost(string, tag = "3")]
    pub endpoint_spec_json: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "4")]
    pub bindings: ::prost::alloc::vec::Vec<validate_request::Binding>,
}
/// Nested message and enum types in `ValidateRequest`.
pub mod validate_request {
    /// Bindings of endpoint resources and collections to which they would be
    /// captured. Bindings are ordered and unique on the bound collection name.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Binding {
        /// JSON-encoded object which specifies the endpoint resource to be captured.
        #[prost(string, tag = "1")]
        pub resource_spec_json: ::prost::alloc::string::String,
        /// Collection to be captured.
        #[prost(message, optional, tag = "2")]
        pub collection: ::core::option::Option<super::super::flow::CollectionSpec>,
    }
}
/// ValidateResponse is the response type of the Validate RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValidateResponse {
    #[prost(message, repeated, tag = "1")]
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
        #[prost(string, repeated, tag = "1")]
        pub resource_path: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    }
}
/// ApplyRequest is the request type of the ApplyUpsert and ApplyDelete RPCs.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ApplyRequest {
    /// Capture to be applied.
    #[prost(message, optional, tag = "1")]
    pub capture: ::core::option::Option<super::flow::CaptureSpec>,
    /// Version of the CaptureSpec being applied.
    #[prost(string, tag = "2")]
    pub version: ::prost::alloc::string::String,
    /// Is this Apply a dry-run? If so, no action is undertaken and Apply will
    /// report only what would have happened.
    #[prost(bool, tag = "3")]
    pub dry_run: bool,
}
/// ApplyResponse is the response type of the ApplyUpsert and ApplyDelete RPCs.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ApplyResponse {
    /// Human-readable description of the action that the Driver took (or, if
    /// dry_run, would have taken). If empty, this Apply is to be considered a
    /// "no-op".
    #[prost(string, tag = "1")]
    pub action_description: ::prost::alloc::string::String,
}
/// Documents is a set of documents drawn from a binding of the capture.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Documents {
    /// The capture binding for documents of this message.
    #[prost(uint32, tag = "1")]
    pub binding: u32,
    /// Byte arena of the response.
    #[prost(bytes = "vec", tag = "2")]
    pub arena: ::prost::alloc::vec::Vec<u8>,
    /// Captured JSON documents.
    #[prost(message, repeated, tag = "3")]
    pub docs_json: ::prost::alloc::vec::Vec<super::flow::Slice>,
}
/// Acknowledge is a notification that a Checkpoint has committed to the
/// Flow runtime's recovery log.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Acknowledge {}
/// PullRequest is the request type of a Driver.Pull RPC.
/// It will have exactly one top-level field set, which represents its message
/// type.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PullRequest {
    #[prost(message, optional, tag = "1")]
    pub open: ::core::option::Option<pull_request::Open>,
    /// Tell the driver that its Checkpoint has committed to the Flow recovery log.
    #[prost(message, optional, tag = "2")]
    pub acknowledge: ::core::option::Option<Acknowledge>,
}
/// Nested message and enum types in `PullRequest`.
pub mod pull_request {
    /// Open opens a Pull of the driver, and is sent exactly once as the first
    /// message of the stream.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Open {
        /// CaptureSpec to be pulled.
        #[prost(message, optional, tag = "1")]
        pub capture: ::core::option::Option<super::super::flow::CaptureSpec>,
        /// Version of the opened CaptureSpec.
        /// The driver may want to require that this match the version last
        /// provided to a successful Apply RPC. It's possible that it won't,
        /// due to expected propagation races in Flow's distributed runtime.
        #[prost(string, tag = "2")]
        pub version: ::prost::alloc::string::String,
        /// [key_begin, key_end] inclusive range of keys processed by this
        /// transaction stream. Ranges reflect the disjoint chunks of ownership
        /// specific to each instance of a scale-out capture implementation.
        #[prost(fixed32, tag = "3")]
        pub key_begin: u32,
        #[prost(fixed32, tag = "4")]
        pub key_end: u32,
        /// Last-persisted driver checkpoint from a previous capture stream.
        /// Or empty, if the driver has cleared or never set its checkpoint.
        #[prost(bytes = "vec", tag = "5")]
        pub driver_checkpoint_json: ::prost::alloc::vec::Vec<u8>,
        /// If true, perform a blocking tail of the capture.
        /// If false, produce all ready output and then close the stream.
        #[prost(bool, tag = "6")]
        pub tail: bool,
    }
}
/// PullResponse is the response type of a Driver.Pull RPC.
/// It will have exactly one top-level field set, which represents its message
/// type.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PullResponse {
    #[prost(message, optional, tag = "1")]
    pub opened: ::core::option::Option<pull_response::Opened>,
    /// Captured documents of the stream.
    #[prost(message, optional, tag = "2")]
    pub documents: ::core::option::Option<Documents>,
    /// Checkpoint all preceeding Documents of this stream.
    #[prost(message, optional, tag = "3")]
    pub checkpoint: ::core::option::Option<super::flow::DriverCheckpoint>,
}
/// Nested message and enum types in `PullResponse`.
pub mod pull_response {
    /// Opened responds to PullRequest.Open of the runtime,
    /// and is sent exactly once as the first message of the stream.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Opened {}
}
/// PushRequest is the request message of the Runtime.Push RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PushRequest {
    #[prost(message, optional, tag = "1")]
    pub open: ::core::option::Option<push_request::Open>,
    /// Captured documents of the stream.
    #[prost(message, optional, tag = "2")]
    pub documents: ::core::option::Option<Documents>,
    /// Checkpoint all preceeding Documents of this stream.
    #[prost(message, optional, tag = "3")]
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
        #[prost(message, optional, tag = "1")]
        pub header: ::core::option::Option<super::super::protocol::Header>,
        /// Name of the capture under which we're pushing.
        #[prost(string, tag = "2")]
        pub capture: ::prost::alloc::string::String,
    }
}
/// PushResponse is the response message of the Runtime.Push RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PushResponse {
    #[prost(message, optional, tag = "1")]
    pub opened: ::core::option::Option<push_response::Opened>,
    /// Tell the driver that its Checkpoint has committed to the Flow recovery log.
    #[prost(message, optional, tag = "2")]
    pub acknowledge: ::core::option::Option<Acknowledge>,
}
/// Nested message and enum types in `PushResponse`.
pub mod push_response {
    /// Opened responds to PushRequest.Open of the driver,
    /// and is sent exactly once as the first message of the stream.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Opened {
        /// Status of the Push open.
        #[prost(enumeration = "super::super::consumer::Status", tag = "1")]
        pub status: i32,
        /// Header of the response.
        #[prost(message, optional, tag = "2")]
        pub header: ::core::option::Option<super::super::protocol::Header>,
        /// CaptureSpec to be pushed.
        #[prost(message, optional, tag = "3")]
        pub capture: ::core::option::Option<super::super::flow::CaptureSpec>,
        /// Version of the opened CaptureSpec.
        /// The driver may want to require that this match the version last
        /// provided to a successful Apply RPC. It's possible that it won't,
        /// due to expected propagation races in Flow's distributed runtime.
        #[prost(string, tag = "4")]
        pub version: ::prost::alloc::string::String,
        /// [key_begin, key_end] inclusive range of keys processed by this
        /// transaction stream. Ranges reflect the disjoint chunks of ownership
        /// specific to each instance of a scale-out capture implementation.
        #[prost(fixed32, tag = "5")]
        pub key_begin: u32,
        #[prost(fixed32, tag = "6")]
        pub key_end: u32,
        /// Last-persisted driver checkpoint from a previous capture stream.
        /// Or empty, if the driver has cleared or never set its checkpoint.
        #[prost(bytes = "vec", tag = "7")]
        pub driver_checkpoint_json: ::prost::alloc::vec::Vec<u8>,
    }
}
#[doc = r" Generated client implementations."]
#[cfg(feature = "capture_client")]
pub mod driver_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[doc = " Driver is the service implemented by a capture connector."]
    #[derive(Debug, Clone)]
    pub struct DriverClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl DriverClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> DriverClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> DriverClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                Into<StdError> + Send + Sync,
        {
            DriverClient::new(InterceptedService::new(inner, interceptor))
        }
        #[doc = r" Compress requests with `gzip`."]
        #[doc = r""]
        #[doc = r" This requires the server to support it otherwise it might respond with an"]
        #[doc = r" error."]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        #[doc = r" Enable decompressing responses with `gzip`."]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        #[doc = " Spec returns the specification definition of this driver."]
        #[doc = " Notably this includes its endpoint and resource configuration JSON schema."]
        pub async fn spec(
            &mut self,
            request: impl tonic::IntoRequest<super::SpecRequest>,
        ) -> Result<tonic::Response<super::SpecResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/capture.Driver/Spec");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Discover returns the set of resources available from this Driver."]
        pub async fn discover(
            &mut self,
            request: impl tonic::IntoRequest<super::DiscoverRequest>,
        ) -> Result<tonic::Response<super::DiscoverResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/capture.Driver/Discover");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Validate that store resources and proposed collection bindings are"]
        #[doc = " compatible."]
        pub async fn validate(
            &mut self,
            request: impl tonic::IntoRequest<super::ValidateRequest>,
        ) -> Result<tonic::Response<super::ValidateResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/capture.Driver/Validate");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " ApplyUpsert applies a new or updated capture to the store."]
        pub async fn apply_upsert(
            &mut self,
            request: impl tonic::IntoRequest<super::ApplyRequest>,
        ) -> Result<tonic::Response<super::ApplyResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/capture.Driver/ApplyUpsert");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " ApplyDelete deletes an existing capture from the store."]
        pub async fn apply_delete(
            &mut self,
            request: impl tonic::IntoRequest<super::ApplyRequest>,
        ) -> Result<tonic::Response<super::ApplyResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/capture.Driver/ApplyDelete");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Pull is a very long lived RPC through which the Flow runtime and a"]
        #[doc = " Driver cooperatively execute an unbounded number of transactions."]
        #[doc = ""]
        #[doc = " The Pull workflow pulls streams of documents into capturing Flow"]
        #[doc = " collections. Streams are incremental and resume-able, with resumption"]
        #[doc = " semantics defined by the driver. The Flow Runtime uses a transactional"]
        #[doc = " recovery log to support this workflow, and the driver may persist arbitrary"]
        #[doc = " driver checkpoints into that log as part of the RPC lifecycle,"]
        #[doc = " to power its chosen resumption semantics."]
        #[doc = ""]
        #[doc = " Pull tasks are split-able, and many concurrent invocations of the RPC"]
        #[doc = " may collectively capture from a source, where each task split has an"]
        #[doc = " identified range of keys it's responsible for. The meaning of a \"key\","]
        #[doc = " and it's application within the remote store being captured from, is up"]
        #[doc = " to the driver. The driver might map partitions or shards into the keyspace,"]
        #[doc = " and from there to a covering task split. Or, it might map distinct files,"]
        #[doc = " or some other unit of scaling."]
        #[doc = ""]
        #[doc = " RPC Lifecycle"]
        #[doc = " ============="]
        #[doc = ""]
        #[doc = " :PullRequest.Open:"]
        #[doc = "    - The Flow runtime opens the pull stream."]
        #[doc = " :PullResponse.Opened:"]
        #[doc = "    - The driver responds with Opened."]
        #[doc = ""]
        #[doc = " PullRequest.Open and PullRequest.Opened are sent only once, at the"]
        #[doc = " commencement of the stream. Thereafter the protocol loops:"]
        #[doc = ""]
        #[doc = " :PullResponse.Documents:"]
        #[doc = "    - The driver tells the runtime of some documents, which are pending a"]
        #[doc = "      future Checkpoint."]
        #[doc = "    - If the driver sends multiple Documents messages without an"]
        #[doc = "      interleaving Checkpoint, the Flow runtime MUST commit"]
        #[doc = "      documents of all such messages in a single transaction."]
        #[doc = " :PullResponse.Checkpoint:"]
        #[doc = "    - The driver tells the runtime of a checkpoint: a watermark in the"]
        #[doc = "      captured documents stream which is eligble to be used as a"]
        #[doc = "      transaction commit boundary."]
        #[doc = "    - Whether the checkpoint becomes a commit boundary is at the"]
        #[doc = "      discretion of the Flow runtime. It may combine multiple checkpoints"]
        #[doc = "      into a single transaction."]
        #[doc = " :PullRequest.Acknowledge:"]
        #[doc = "    - The Flow runtime tells the driver that its Checkpoint has committed."]
        #[doc = "    - The runtime sends one ordered Acknowledge for each Checkpoint."]
        #[doc = ""]
        pub async fn pull(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = super::PullRequest>,
        ) -> Result<tonic::Response<tonic::codec::Streaming<super::PullResponse>>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/capture.Driver/Pull");
            self.inner
                .streaming(request.into_streaming_request(), path, codec)
                .await
        }
    }
}
#[doc = r" Generated client implementations."]
#[cfg(feature = "capture_client")]
pub mod runtime_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[doc = " Runtime is the Flow runtime service which implements push-based captures."]
    #[derive(Debug, Clone)]
    pub struct RuntimeClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl RuntimeClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> RuntimeClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> RuntimeClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                Into<StdError> + Send + Sync,
        {
            RuntimeClient::new(InterceptedService::new(inner, interceptor))
        }
        #[doc = r" Compress requests with `gzip`."]
        #[doc = r""]
        #[doc = r" This requires the server to support it otherwise it might respond with an"]
        #[doc = r" error."]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        #[doc = r" Enable decompressing responses with `gzip`."]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        #[doc = " Push may be a short or very long lived RPC through which the Flow runtime"]
        #[doc = " and a driver cooperatively execute an unbounded number of transactions."]
        #[doc = ""]
        #[doc = " The Push workflow pushes streams of documents into capturing Flow"]
        #[doc = " collections. The driver is responsible for initiation and resumption of"]
        #[doc = " push streams. The Flow runtime uses a transactional recovery log to support"]
        #[doc = " this workflow, and the driver may persist arbitrary driver checkpoints into"]
        #[doc = " that log as part of the RPC lifecycle, to power its chosen resumption"]
        #[doc = " semantics."]
        #[doc = ""]
        #[doc = " A push RPC is evaluated against a specific task shard split, which is"]
        #[doc = " encoded in the PushRequest.Open.Header. A driver may perform its own load"]
        #[doc = " balancing by obtain a shard listing and embedding a selected shard into"]
        #[doc = " that header. Or, it may leave it empty and an arbitary shard will be"]
        #[doc = " randomly chosen for it."]
        #[doc = ""]
        #[doc = " RPC Lifecycle"]
        #[doc = " ============="]
        #[doc = ""]
        #[doc = " :PushRequest.Open:"]
        #[doc = "    - The driver opens the push stream, naming its capture and"]
        #[doc = "      optional routing header."]
        #[doc = " :PushResponse.Opened:"]
        #[doc = "    - The Flow runtime responds with Opened, which tells the driver"]
        #[doc = "      of the specific CaptureSpec and [key_begin, key_end] range of"]
        #[doc = "      this RPC, as well as the last driver checkpoint."]
        #[doc = "    - The semantics and treatment of the key range is up to the driver."]
        #[doc = ""]
        #[doc = " PushRequest.Open and PushRequest.Opened are sent only once, at the"]
        #[doc = " commencement of the stream. Thereafter the protocol loops:"]
        #[doc = ""]
        #[doc = " :PushRequest.Documents:"]
        #[doc = "    - The driver tells the runtime of some documents, which are pending a"]
        #[doc = "      future Checkpoint."]
        #[doc = "    - If the driver sends multiple Documents messages without an"]
        #[doc = "      interleaving Checkpoint, the Flow runtime MUST commit"]
        #[doc = "      documents of all such messages in a single transaction."]
        #[doc = " :PushRequest.Checkpoint:"]
        #[doc = "    - The driver tells the runtime of a checkpoint: a watermark in the"]
        #[doc = "      captured documents stream which is eligble to be used as a"]
        #[doc = "      transaction commit boundary."]
        #[doc = "    - Whether the checkpoint becomes a commit boundary is at the"]
        #[doc = "      discretion of the Flow runtime. It may combine multiple checkpoints"]
        #[doc = "      into a single transaction."]
        #[doc = " :PushResponse.Acknowledge:"]
        #[doc = "    - The Flow runtime tells the driver that its Checkpoint has committed."]
        #[doc = "    - The runtime sends one ordered Acknowledge for each Checkpoint."]
        #[doc = ""]
        pub async fn push(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = super::PushRequest>,
        ) -> Result<tonic::Response<tonic::codec::Streaming<super::PushResponse>>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/capture.Runtime/Push");
            self.inner
                .streaming(request.into_streaming_request(), path, codec)
                .await
        }
    }
}
#[doc = r" Generated server implementations."]
#[cfg(feature = "capture_server")]
pub mod driver_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with DriverServer."]
    #[async_trait]
    pub trait Driver: Send + Sync + 'static {
        #[doc = " Spec returns the specification definition of this driver."]
        #[doc = " Notably this includes its endpoint and resource configuration JSON schema."]
        async fn spec(
            &self,
            request: tonic::Request<super::SpecRequest>,
        ) -> Result<tonic::Response<super::SpecResponse>, tonic::Status>;
        #[doc = " Discover returns the set of resources available from this Driver."]
        async fn discover(
            &self,
            request: tonic::Request<super::DiscoverRequest>,
        ) -> Result<tonic::Response<super::DiscoverResponse>, tonic::Status>;
        #[doc = " Validate that store resources and proposed collection bindings are"]
        #[doc = " compatible."]
        async fn validate(
            &self,
            request: tonic::Request<super::ValidateRequest>,
        ) -> Result<tonic::Response<super::ValidateResponse>, tonic::Status>;
        #[doc = " ApplyUpsert applies a new or updated capture to the store."]
        async fn apply_upsert(
            &self,
            request: tonic::Request<super::ApplyRequest>,
        ) -> Result<tonic::Response<super::ApplyResponse>, tonic::Status>;
        #[doc = " ApplyDelete deletes an existing capture from the store."]
        async fn apply_delete(
            &self,
            request: tonic::Request<super::ApplyRequest>,
        ) -> Result<tonic::Response<super::ApplyResponse>, tonic::Status>;
        #[doc = "Server streaming response type for the Pull method."]
        type PullStream: futures_core::Stream<Item = Result<super::PullResponse, tonic::Status>>
            + Send
            + 'static;
        #[doc = " Pull is a very long lived RPC through which the Flow runtime and a"]
        #[doc = " Driver cooperatively execute an unbounded number of transactions."]
        #[doc = ""]
        #[doc = " The Pull workflow pulls streams of documents into capturing Flow"]
        #[doc = " collections. Streams are incremental and resume-able, with resumption"]
        #[doc = " semantics defined by the driver. The Flow Runtime uses a transactional"]
        #[doc = " recovery log to support this workflow, and the driver may persist arbitrary"]
        #[doc = " driver checkpoints into that log as part of the RPC lifecycle,"]
        #[doc = " to power its chosen resumption semantics."]
        #[doc = ""]
        #[doc = " Pull tasks are split-able, and many concurrent invocations of the RPC"]
        #[doc = " may collectively capture from a source, where each task split has an"]
        #[doc = " identified range of keys it's responsible for. The meaning of a \"key\","]
        #[doc = " and it's application within the remote store being captured from, is up"]
        #[doc = " to the driver. The driver might map partitions or shards into the keyspace,"]
        #[doc = " and from there to a covering task split. Or, it might map distinct files,"]
        #[doc = " or some other unit of scaling."]
        #[doc = ""]
        #[doc = " RPC Lifecycle"]
        #[doc = " ============="]
        #[doc = ""]
        #[doc = " :PullRequest.Open:"]
        #[doc = "    - The Flow runtime opens the pull stream."]
        #[doc = " :PullResponse.Opened:"]
        #[doc = "    - The driver responds with Opened."]
        #[doc = ""]
        #[doc = " PullRequest.Open and PullRequest.Opened are sent only once, at the"]
        #[doc = " commencement of the stream. Thereafter the protocol loops:"]
        #[doc = ""]
        #[doc = " :PullResponse.Documents:"]
        #[doc = "    - The driver tells the runtime of some documents, which are pending a"]
        #[doc = "      future Checkpoint."]
        #[doc = "    - If the driver sends multiple Documents messages without an"]
        #[doc = "      interleaving Checkpoint, the Flow runtime MUST commit"]
        #[doc = "      documents of all such messages in a single transaction."]
        #[doc = " :PullResponse.Checkpoint:"]
        #[doc = "    - The driver tells the runtime of a checkpoint: a watermark in the"]
        #[doc = "      captured documents stream which is eligble to be used as a"]
        #[doc = "      transaction commit boundary."]
        #[doc = "    - Whether the checkpoint becomes a commit boundary is at the"]
        #[doc = "      discretion of the Flow runtime. It may combine multiple checkpoints"]
        #[doc = "      into a single transaction."]
        #[doc = " :PullRequest.Acknowledge:"]
        #[doc = "    - The Flow runtime tells the driver that its Checkpoint has committed."]
        #[doc = "    - The runtime sends one ordered Acknowledge for each Checkpoint."]
        #[doc = ""]
        async fn pull(
            &self,
            request: tonic::Request<tonic::Streaming<super::PullRequest>>,
        ) -> Result<tonic::Response<Self::PullStream>, tonic::Status>;
    }
    #[doc = " Driver is the service implemented by a capture connector."]
    #[derive(Debug)]
    pub struct DriverServer<T: Driver> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: Driver> DriverServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for DriverServer<T>
    where
        T: Driver,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/capture.Driver/Spec" => {
                    #[allow(non_camel_case_types)]
                    struct SpecSvc<T: Driver>(pub Arc<T>);
                    impl<T: Driver> tonic::server::UnaryService<super::SpecRequest> for SpecSvc<T> {
                        type Response = super::SpecResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SpecRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).spec(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = SpecSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/capture.Driver/Discover" => {
                    #[allow(non_camel_case_types)]
                    struct DiscoverSvc<T: Driver>(pub Arc<T>);
                    impl<T: Driver> tonic::server::UnaryService<super::DiscoverRequest> for DiscoverSvc<T> {
                        type Response = super::DiscoverResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DiscoverRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).discover(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DiscoverSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/capture.Driver/Validate" => {
                    #[allow(non_camel_case_types)]
                    struct ValidateSvc<T: Driver>(pub Arc<T>);
                    impl<T: Driver> tonic::server::UnaryService<super::ValidateRequest> for ValidateSvc<T> {
                        type Response = super::ValidateResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ValidateRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).validate(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ValidateSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/capture.Driver/ApplyUpsert" => {
                    #[allow(non_camel_case_types)]
                    struct ApplyUpsertSvc<T: Driver>(pub Arc<T>);
                    impl<T: Driver> tonic::server::UnaryService<super::ApplyRequest> for ApplyUpsertSvc<T> {
                        type Response = super::ApplyResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ApplyRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).apply_upsert(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ApplyUpsertSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/capture.Driver/ApplyDelete" => {
                    #[allow(non_camel_case_types)]
                    struct ApplyDeleteSvc<T: Driver>(pub Arc<T>);
                    impl<T: Driver> tonic::server::UnaryService<super::ApplyRequest> for ApplyDeleteSvc<T> {
                        type Response = super::ApplyResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ApplyRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).apply_delete(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ApplyDeleteSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/capture.Driver/Pull" => {
                    #[allow(non_camel_case_types)]
                    struct PullSvc<T: Driver>(pub Arc<T>);
                    impl<T: Driver> tonic::server::StreamingService<super::PullRequest> for PullSvc<T> {
                        type Response = super::PullResponse;
                        type ResponseStream = T::PullStream;
                        type Future =
                            BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<tonic::Streaming<super::PullRequest>>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).pull(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = PullSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(empty_body())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: Driver> Clone for DriverServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: Driver> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Driver> tonic::transport::NamedService for DriverServer<T> {
        const NAME: &'static str = "capture.Driver";
    }
}
#[doc = r" Generated server implementations."]
#[cfg(feature = "capture_server")]
pub mod runtime_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with RuntimeServer."]
    #[async_trait]
    pub trait Runtime: Send + Sync + 'static {
        #[doc = "Server streaming response type for the Push method."]
        type PushStream: futures_core::Stream<Item = Result<super::PushResponse, tonic::Status>>
            + Send
            + 'static;
        #[doc = " Push may be a short or very long lived RPC through which the Flow runtime"]
        #[doc = " and a driver cooperatively execute an unbounded number of transactions."]
        #[doc = ""]
        #[doc = " The Push workflow pushes streams of documents into capturing Flow"]
        #[doc = " collections. The driver is responsible for initiation and resumption of"]
        #[doc = " push streams. The Flow runtime uses a transactional recovery log to support"]
        #[doc = " this workflow, and the driver may persist arbitrary driver checkpoints into"]
        #[doc = " that log as part of the RPC lifecycle, to power its chosen resumption"]
        #[doc = " semantics."]
        #[doc = ""]
        #[doc = " A push RPC is evaluated against a specific task shard split, which is"]
        #[doc = " encoded in the PushRequest.Open.Header. A driver may perform its own load"]
        #[doc = " balancing by obtain a shard listing and embedding a selected shard into"]
        #[doc = " that header. Or, it may leave it empty and an arbitary shard will be"]
        #[doc = " randomly chosen for it."]
        #[doc = ""]
        #[doc = " RPC Lifecycle"]
        #[doc = " ============="]
        #[doc = ""]
        #[doc = " :PushRequest.Open:"]
        #[doc = "    - The driver opens the push stream, naming its capture and"]
        #[doc = "      optional routing header."]
        #[doc = " :PushResponse.Opened:"]
        #[doc = "    - The Flow runtime responds with Opened, which tells the driver"]
        #[doc = "      of the specific CaptureSpec and [key_begin, key_end] range of"]
        #[doc = "      this RPC, as well as the last driver checkpoint."]
        #[doc = "    - The semantics and treatment of the key range is up to the driver."]
        #[doc = ""]
        #[doc = " PushRequest.Open and PushRequest.Opened are sent only once, at the"]
        #[doc = " commencement of the stream. Thereafter the protocol loops:"]
        #[doc = ""]
        #[doc = " :PushRequest.Documents:"]
        #[doc = "    - The driver tells the runtime of some documents, which are pending a"]
        #[doc = "      future Checkpoint."]
        #[doc = "    - If the driver sends multiple Documents messages without an"]
        #[doc = "      interleaving Checkpoint, the Flow runtime MUST commit"]
        #[doc = "      documents of all such messages in a single transaction."]
        #[doc = " :PushRequest.Checkpoint:"]
        #[doc = "    - The driver tells the runtime of a checkpoint: a watermark in the"]
        #[doc = "      captured documents stream which is eligble to be used as a"]
        #[doc = "      transaction commit boundary."]
        #[doc = "    - Whether the checkpoint becomes a commit boundary is at the"]
        #[doc = "      discretion of the Flow runtime. It may combine multiple checkpoints"]
        #[doc = "      into a single transaction."]
        #[doc = " :PushResponse.Acknowledge:"]
        #[doc = "    - The Flow runtime tells the driver that its Checkpoint has committed."]
        #[doc = "    - The runtime sends one ordered Acknowledge for each Checkpoint."]
        #[doc = ""]
        async fn push(
            &self,
            request: tonic::Request<tonic::Streaming<super::PushRequest>>,
        ) -> Result<tonic::Response<Self::PushStream>, tonic::Status>;
    }
    #[doc = " Runtime is the Flow runtime service which implements push-based captures."]
    #[derive(Debug)]
    pub struct RuntimeServer<T: Runtime> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: Runtime> RuntimeServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for RuntimeServer<T>
    where
        T: Runtime,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/capture.Runtime/Push" => {
                    #[allow(non_camel_case_types)]
                    struct PushSvc<T: Runtime>(pub Arc<T>);
                    impl<T: Runtime> tonic::server::StreamingService<super::PushRequest> for PushSvc<T> {
                        type Response = super::PushResponse;
                        type ResponseStream = T::PushStream;
                        type Future =
                            BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<tonic::Streaming<super::PushRequest>>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).push(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = PushSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(empty_body())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: Runtime> Clone for RuntimeServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: Runtime> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Runtime> tonic::transport::NamedService for RuntimeServer<T> {
        const NAME: &'static str = "capture.Runtime";
    }
}
