/// Constraint constrains the use of a flow.Projection within a materialization.
#[derive(Clone, PartialEq, ::prost::Message, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Constraint {
    #[prost(enumeration = "constraint::Type", tag = "2")]
    pub r#type: i32,
    /// Optional human readable reason for the given constraint.
    /// Implementations are strongly encouraged to supply a descriptive message.
    #[prost(string, tag = "3")]
    pub reason: std::string::String,
}
pub mod constraint {
    /// Type encodes a constraint type for this flow.Projection.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Type {
        /// This specific projection must be present.
        FieldRequired = 0,
        /// At least one projection with this location pointer must be present.
        LocationRequired = 1,
        /// A projection with this location is recommended, and should be included by default.
        LocationRecommended = 2,
        /// This projection may be included, but should be omitted by default.
        FieldOptional = 3,
        /// This projection must not be present in the materialization.
        FieldForbidden = 4,
        /// This specific projection is required but is also unacceptable (e.x.,
        /// because it uses an incompatible type with a previous applied version).
        Unsatisfiable = 5,
    }
}
/// SessionRequest is the request type of the StartSession RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SessionRequest {
    /// Endpoint URL of the materialization system.
    #[prost(string, tag = "1")]
    pub endpoint_url: std::string::String,
    /// Target name within the materialization system, where applicable.
    /// This could be a SQL schema & table, or a pub/sub topic, etc.
    #[prost(string, tag = "2")]
    pub target: std::string::String,
    /// Stable ID of the flow consumer shard that this session belongs to. A null or empty value
    /// indicates that the caller is not a flow consumer shard, but some other process (e.g. flowctl).
    #[prost(string, tag = "3")]
    pub shard_id: std::string::String,
}
/// SessionResponse is the response type of the StartSession RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SessionResponse {
    /// Opaque session handle.
    #[prost(bytes, tag = "1")]
    pub handle: std::vec::Vec<u8>,
}
/// ValidateRequest is the request type of the Validate RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValidateRequest {
    /// Opaque session handle.
    #[prost(bytes, tag = "1")]
    pub handle: std::vec::Vec<u8>,
    /// Collection to be materialized.
    #[prost(message, optional, tag = "2")]
    pub collection: ::std::option::Option<super::flow::CollectionSpec>,
    /// Projection configuration, keyed by the projection field name,
    /// with JSON-encoded and driver-defined configuration objects.
    #[prost(map = "string, string", tag = "3")]
    pub field_config: ::std::collections::HashMap<std::string::String, std::string::String>,
}
/// ValidateResponse is the response type of the Validate RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValidateResponse {
    /// Constraints over collection projections imposed by the Driver,
    /// keyed by the projection field name. Projections of the CollectionSpec
    /// which are missing from constraints are implicitly forbidden.
    #[prost(map = "string, message", tag = "1")]
    pub constraints: ::std::collections::HashMap<std::string::String, Constraint>,
}
/// FieldSelection represents the entire set of fields for a materialization. Projected fields are
/// separated into keys and values.
#[derive(Clone, PartialEq, ::prost::Message, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct FieldSelection {
    /// The fields that are being used as the primary key for this materialization. Flow will guarantee
    /// that each location that's part of a collection's key is represented here exactly once, and in
    /// the same order as the keys are declared in the collection.
    #[prost(string, repeated, tag = "1")]
    pub keys: ::std::vec::Vec<std::string::String>,
    /// All other materialized fields, except for those in keys and the root document field, will be listed here in
    /// a stable order. Note that not all materializations will have or need any "values" fields (e.g.
    /// materializing to a key-value store like dynamo)
    #[prost(string, repeated, tag = "2")]
    pub values: ::std::vec::Vec<std::string::String>,
    /// The name of the field holding the root document.
    #[prost(string, tag = "3")]
    pub document: std::string::String,
    /// Projection configuration, keyed by the projection field name,
    /// with JSON-encoded and driver-defined configuration objects.
    #[prost(map = "string, string", tag = "4")]
    pub field_config: ::std::collections::HashMap<std::string::String, std::string::String>,
}
/// ApplyRequest is the request type of the Apply RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ApplyRequest {
    /// Opaque session handle.
    #[prost(bytes, tag = "1")]
    pub handle: std::vec::Vec<u8>,
    /// Collection to be materialized.
    #[prost(message, optional, tag = "2")]
    pub collection: ::std::option::Option<super::flow::CollectionSpec>,
    /// Selected fields for materialization
    #[prost(message, optional, tag = "3")]
    pub fields: ::std::option::Option<FieldSelection>,
    /// Is this Apply a dry-run? If so, no action is undertaken and Apply will
    /// report only what would have happened.
    #[prost(bool, tag = "4")]
    pub dry_run: bool,
}
/// ApplyResponse is the response type of the Apply RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ApplyResponse {
    /// Human-readable description of the action that the Driver took (or, if dry_run, would have taken).
    /// If empty, this Apply is to be considered a "no-op".
    #[prost(string, tag = "1")]
    pub action_description: std::string::String,
}
/// FenceRequest is the request type of a Fence RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FenceRequest {
    /// Opaque session handle.
    #[prost(bytes, tag = "1")]
    pub handle: std::vec::Vec<u8>,
    /// Driver checkpoint which was last committed from a Store RPC.
    /// Or empty, if the Driver has never returned a checkpoint.
    #[prost(bytes, tag = "2")]
    pub driver_checkpoint: std::vec::Vec<u8>,
}
/// FenceResponse is the response type of a Fence RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FenceResponse {
    /// Flow checkpoint which was previously committed with this caller ID.
    /// Or nil, if unknown or transactional semantics are not supported.
    #[prost(bytes, tag = "1")]
    pub flow_checkpoint: std::vec::Vec<u8>,
}
/// LoadRequest is the request type of the Load RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LoadRequest {
    /// Opaque session handle.
    #[prost(bytes, tag = "1")]
    pub handle: std::vec::Vec<u8>,
    /// Byte arena of the request.
    #[prost(bytes, tag = "2")]
    pub arena: std::vec::Vec<u8>,
    /// Packed tuples of collection keys, enumerating the documents to load.
    #[prost(message, repeated, tag = "3")]
    pub packed_keys: ::std::vec::Vec<super::flow::Slice>,
}
/// LoadRequest is the response type of the Load RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LoadResponse {
    /// Byte arena of the request.
    #[prost(bytes, tag = "1")]
    pub arena: std::vec::Vec<u8>,
    /// Loaded JSON documents, 1:1 with keys of the LoadRequest.
    /// Documents which don't exist in the target are represented as an empty Slice.
    #[prost(message, repeated, tag = "2")]
    pub docs_json: ::std::vec::Vec<super::flow::Slice>,
    /// Always empty hint which, when set true, hints to Flow that it may skip future
    /// calls of the Load RPC for this handle, as they will always return an empty
    /// LoadResponse.
    #[prost(bool, tag = "3")]
    pub always_empty_hint: bool,
}
/// StoreRequest is the request type of the Store RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StoreRequest {
    #[prost(message, optional, tag = "1")]
    pub start: ::std::option::Option<store_request::Start>,
    #[prost(message, optional, tag = "2")]
    pub r#continue: ::std::option::Option<store_request::Continue>,
}
pub mod store_request {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Start {
        /// Opaque session handle.
        #[prost(bytes, tag = "1")]
        pub handle: std::vec::Vec<u8>,
        /// Projection fields to be stored. This repeats the selection and ordering
        /// of the last Apply RPC, but is provided here also as a convenience.
        #[prost(message, optional, tag = "2")]
        pub fields: ::std::option::Option<super::FieldSelection>,
        /// Checkpoint to write with this Store transaction, to be associated with
        /// the session's caller ID and to be returned by a future Fence RPC.
        /// This may be ignored if the Driver doesn't support exactly-once semantics.
        #[prost(bytes, tag = "3")]
        pub flow_checkpoint: std::vec::Vec<u8>,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Continue {
        /// Byte arena of the request.
        #[prost(bytes, tag = "1")]
        pub arena: std::vec::Vec<u8>,
        #[prost(message, repeated, tag = "2")]
        pub packed_keys: ::std::vec::Vec<super::super::flow::Slice>,
        /// Packed tuples holding projection values for each document.
        #[prost(message, repeated, tag = "3")]
        pub packed_values: ::std::vec::Vec<super::super::flow::Slice>,
        /// JSON documents.
        #[prost(message, repeated, tag = "4")]
        pub docs_json: ::std::vec::Vec<super::super::flow::Slice>,
        /// Exists is true if this document previously been loaded or stored.
        ///
        /// [ (gogoproto.nullable) = false, (gogoproto.embed) = true ];
        #[prost(bool, repeated, tag = "5")]
        pub exists: ::std::vec::Vec<bool>,
    }
}
/// StoreResponse is the response type of the Store RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StoreResponse {
    /// Arbitrary driver defined checkpoint. Flow persists the provided checkpoint
    /// within the same internal transaction which triggered this Store RPC,
    /// and will present the latest checkpoint to a future Fence RPC.
    /// This may be ignored if the Driver has no checkpoints.
    #[prost(bytes, tag = "1")]
    pub driver_checkpoint: std::vec::Vec<u8>,
}
#[doc = r" Generated client implementations."]
pub mod driver_client {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = " Driver is the service implemented by a materialization target system."]
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
        T::ResponseBody: Body + HttpBody + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = tonic::client::Grpc::with_interceptor(inner, interceptor);
            Self { inner }
        }
        #[doc = " Session begins a scoped interaction with the driver from a single process context."]
        #[doc = " It maps an endpoint URL, target, and caller ID to a returned opaque session handle,"]
        #[doc = " which is to be used with further Driver interactions. Note that at any given time,"]
        #[doc = " there may be *many* concurrent Sessions."]
        pub async fn start_session(
            &mut self,
            request: impl tonic::IntoRequest<super::SessionRequest>,
        ) -> Result<tonic::Response<super::SessionResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/materialize.Driver/StartSession");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Validate that a CollectionSpec is compatible with a materialization target,"]
        #[doc = " and return constraints over the projections of the collection."]
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
            let path = http::uri::PathAndQuery::from_static("/materialize.Driver/Validate");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Apply a CollectionSpec and selected Projections to a materialization target."]
        pub async fn apply(
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
            let path = http::uri::PathAndQuery::from_static("/materialize.Driver/Apply");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Fence inserts a transactional \"write fence\" boundary by fencing the caller"]
        #[doc = " ID encapsulated within a session, to the session's unique handle. Typically this"]
        #[doc = " is done by tying the caller ID to a unique session nonce in a transaction,"]
        #[doc = " or by increasing a epoch value of the caller ID."]
        #[doc = ""]
        #[doc = " For example a RDBMS might use a \"writers\" table holding a caller ID key,"]
        #[doc = " a current session nonce, and a last checkpoint. The Fence RPC would update the"]
        #[doc = " nonce to the current session's unique value -- effectively \"poisoning\" transactions"]
        #[doc = " of prior sessions -- and return the checkpoint. Store RPCs must in turn verify"]
        #[doc = " their session nonce is still effective before committing a transaction."]
        #[doc = ""]
        #[doc = " On return, it's guaranteed that no session previously fenced to the caller ID"]
        #[doc = " (now a \"zombie\" session) can commit transactions as part of Store RPCs which"]
        #[doc = " update documents or checkpoints. Fence returns the checkpoint last committed"]
        #[doc = " by this caller ID in a Store RPC."]
        #[doc = ""]
        #[doc = " Fence is an *optional* API which is required for materialization targets that"]
        #[doc = " support end-to-end \"exactly once\" semantics. Stores which support only \"at least once\""]
        #[doc = " semantics can implement Fence as a no-op, returning a zero-value FenceResponse."]
        pub async fn fence(
            &mut self,
            request: impl tonic::IntoRequest<super::FenceRequest>,
        ) -> Result<tonic::Response<super::FenceResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/materialize.Driver/Fence");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Load one or more keyed collection documents from the materialization target."]
        #[doc = " Flow guarantees that independent producers always load and store non-overlapping"]
        #[doc = " subsets of documents."]
        pub async fn load(
            &mut self,
            request: impl tonic::IntoRequest<super::LoadRequest>,
        ) -> Result<tonic::Response<super::LoadResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/materialize.Driver/Load");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Store one or more collection documents to a materialization target."]
        pub async fn store(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = super::StoreRequest>,
        ) -> Result<tonic::Response<super::StoreResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/materialize.Driver/Store");
            self.inner
                .client_streaming(request.into_streaming_request(), path, codec)
                .await
        }
    }
    impl<T: Clone> Clone for DriverClient<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }
    impl<T> std::fmt::Debug for DriverClient<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "DriverClient {{ ... }}")
        }
    }
}
#[doc = r" Generated server implementations."]
pub mod driver_server {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with DriverServer."]
    #[async_trait]
    pub trait Driver: Send + Sync + 'static {
        #[doc = " Session begins a scoped interaction with the driver from a single process context."]
        #[doc = " It maps an endpoint URL, target, and caller ID to a returned opaque session handle,"]
        #[doc = " which is to be used with further Driver interactions. Note that at any given time,"]
        #[doc = " there may be *many* concurrent Sessions."]
        async fn start_session(
            &self,
            request: tonic::Request<super::SessionRequest>,
        ) -> Result<tonic::Response<super::SessionResponse>, tonic::Status>;
        #[doc = " Validate that a CollectionSpec is compatible with a materialization target,"]
        #[doc = " and return constraints over the projections of the collection."]
        async fn validate(
            &self,
            request: tonic::Request<super::ValidateRequest>,
        ) -> Result<tonic::Response<super::ValidateResponse>, tonic::Status>;
        #[doc = " Apply a CollectionSpec and selected Projections to a materialization target."]
        async fn apply(
            &self,
            request: tonic::Request<super::ApplyRequest>,
        ) -> Result<tonic::Response<super::ApplyResponse>, tonic::Status>;
        #[doc = " Fence inserts a transactional \"write fence\" boundary by fencing the caller"]
        #[doc = " ID encapsulated within a session, to the session's unique handle. Typically this"]
        #[doc = " is done by tying the caller ID to a unique session nonce in a transaction,"]
        #[doc = " or by increasing a epoch value of the caller ID."]
        #[doc = ""]
        #[doc = " For example a RDBMS might use a \"writers\" table holding a caller ID key,"]
        #[doc = " a current session nonce, and a last checkpoint. The Fence RPC would update the"]
        #[doc = " nonce to the current session's unique value -- effectively \"poisoning\" transactions"]
        #[doc = " of prior sessions -- and return the checkpoint. Store RPCs must in turn verify"]
        #[doc = " their session nonce is still effective before committing a transaction."]
        #[doc = ""]
        #[doc = " On return, it's guaranteed that no session previously fenced to the caller ID"]
        #[doc = " (now a \"zombie\" session) can commit transactions as part of Store RPCs which"]
        #[doc = " update documents or checkpoints. Fence returns the checkpoint last committed"]
        #[doc = " by this caller ID in a Store RPC."]
        #[doc = ""]
        #[doc = " Fence is an *optional* API which is required for materialization targets that"]
        #[doc = " support end-to-end \"exactly once\" semantics. Stores which support only \"at least once\""]
        #[doc = " semantics can implement Fence as a no-op, returning a zero-value FenceResponse."]
        async fn fence(
            &self,
            request: tonic::Request<super::FenceRequest>,
        ) -> Result<tonic::Response<super::FenceResponse>, tonic::Status>;
        #[doc = " Load one or more keyed collection documents from the materialization target."]
        #[doc = " Flow guarantees that independent producers always load and store non-overlapping"]
        #[doc = " subsets of documents."]
        async fn load(
            &self,
            request: tonic::Request<super::LoadRequest>,
        ) -> Result<tonic::Response<super::LoadResponse>, tonic::Status>;
        #[doc = " Store one or more collection documents to a materialization target."]
        async fn store(
            &self,
            request: tonic::Request<tonic::Streaming<super::StoreRequest>>,
        ) -> Result<tonic::Response<super::StoreResponse>, tonic::Status>;
    }
    #[doc = " Driver is the service implemented by a materialization target system."]
    #[derive(Debug)]
    pub struct DriverServer<T: Driver> {
        inner: _Inner<T>,
    }
    struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
    impl<T: Driver> DriverServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, None);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, Some(interceptor.into()));
            Self { inner }
        }
    }
    impl<T, B> Service<http::Request<B>> for DriverServer<T>
    where
        T: Driver,
        B: HttpBody + Send + Sync + 'static,
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
                "/materialize.Driver/StartSession" => {
                    #[allow(non_camel_case_types)]
                    struct StartSessionSvc<T: Driver>(pub Arc<T>);
                    impl<T: Driver> tonic::server::UnaryService<super::SessionRequest> for StartSessionSvc<T> {
                        type Response = super::SessionResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SessionRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).start_session(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = StartSessionSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/materialize.Driver/Validate" => {
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
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = ValidateSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/materialize.Driver/Apply" => {
                    #[allow(non_camel_case_types)]
                    struct ApplySvc<T: Driver>(pub Arc<T>);
                    impl<T: Driver> tonic::server::UnaryService<super::ApplyRequest> for ApplySvc<T> {
                        type Response = super::ApplyResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ApplyRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).apply(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = ApplySvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/materialize.Driver/Fence" => {
                    #[allow(non_camel_case_types)]
                    struct FenceSvc<T: Driver>(pub Arc<T>);
                    impl<T: Driver> tonic::server::UnaryService<super::FenceRequest> for FenceSvc<T> {
                        type Response = super::FenceResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::FenceRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).fence(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = FenceSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/materialize.Driver/Load" => {
                    #[allow(non_camel_case_types)]
                    struct LoadSvc<T: Driver>(pub Arc<T>);
                    impl<T: Driver> tonic::server::UnaryService<super::LoadRequest> for LoadSvc<T> {
                        type Response = super::LoadResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::LoadRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).load(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = LoadSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/materialize.Driver/Store" => {
                    #[allow(non_camel_case_types)]
                    struct StoreSvc<T: Driver>(pub Arc<T>);
                    impl<T: Driver> tonic::server::ClientStreamingService<super::StoreRequest> for StoreSvc<T> {
                        type Response = super::StoreResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<tonic::Streaming<super::StoreRequest>>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).store(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1;
                        let inner = inner.0;
                        let method = StoreSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.client_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .body(tonic::body::BoxBody::empty())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: Driver> Clone for DriverServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self { inner }
        }
    }
    impl<T: Driver> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone(), self.1.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Driver> tonic::transport::NamedService for DriverServer<T> {
        const NAME: &'static str = "materialize.Driver";
    }
}
