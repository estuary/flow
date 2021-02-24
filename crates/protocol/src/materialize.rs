/// Constraint constrains the use of a flow.Projection within a materialization.
#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
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
/// ValidateRequest is the request type of the Validate RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValidateRequest {
    /// Endpoint type addressed by this request.
    #[prost(enumeration = "super::flow::EndpointType", tag = "1")]
    pub endpoint_type: i32,
    /// Driver-specific configuration, as an encoded JSON object.
    #[prost(string, tag = "2")]
    pub endpoint_config_json: ::prost::alloc::string::String,
    /// Collection to be materialized.
    #[prost(message, optional, tag = "3")]
    pub collection: ::core::option::Option<super::flow::CollectionSpec>,
    /// Projection configuration, keyed by the projection field name,
    /// with JSON-encoded and driver-defined configuration objects.
    #[prost(map = "string, string", tag = "4")]
    pub field_config_json:
        ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
}
/// ValidateResponse is the response type of the Validate RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValidateResponse {
    /// Constraints over collection projections imposed by the Driver,
    /// keyed by the projection field name. Projections of the CollectionSpec
    /// which are missing from constraints are implicitly forbidden.
    #[prost(map = "string, message", tag = "1")]
    pub constraints: ::std::collections::HashMap<::prost::alloc::string::String, Constraint>,
}
/// ApplyRequest is the request type of the Apply RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ApplyRequest {
    /// Endpoint type addressed by this request.
    #[prost(enumeration = "super::flow::EndpointType", tag = "1")]
    pub endpoint_type: i32,
    /// Driver-specific configuration, as an encoded JSON object.
    #[prost(string, tag = "2")]
    pub endpoint_config_json: ::prost::alloc::string::String,
    /// Collection to be materialized.
    #[prost(message, optional, tag = "3")]
    pub collection: ::core::option::Option<super::flow::CollectionSpec>,
    /// Selected fields for materialization
    #[prost(message, optional, tag = "4")]
    pub fields: ::core::option::Option<super::flow::FieldSelection>,
    /// Is this Apply a dry-run? If so, no action is undertaken and Apply will
    /// report only what would have happened.
    #[prost(bool, tag = "5")]
    pub dry_run: bool,
}
/// ApplyResponse is the response type of the Apply RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ApplyResponse {
    /// Human-readable description of the action that the Driver took (or, if dry_run, would have taken).
    /// If empty, this Apply is to be considered a "no-op".
    #[prost(string, tag = "1")]
    pub action_description: ::prost::alloc::string::String,
}
/// TransactionRequest is the request type of a Transaction RPC.
/// It will have exactly one top-level field set, which represents its message type.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransactionRequest {
    #[prost(message, optional, tag = "1")]
    pub open: ::core::option::Option<transaction_request::Open>,
    #[prost(message, optional, tag = "2")]
    pub load: ::core::option::Option<transaction_request::Load>,
    #[prost(message, optional, tag = "3")]
    pub prepare: ::core::option::Option<transaction_request::Prepare>,
    #[prost(message, optional, tag = "4")]
    pub store: ::core::option::Option<transaction_request::Store>,
    #[prost(message, optional, tag = "5")]
    pub commit: ::core::option::Option<transaction_request::Commit>,
}
/// Nested message and enum types in `TransactionRequest`.
pub mod transaction_request {
    /// Open a transaction stream and, where supported, fence off other
    /// streams having this same |fence_id| from issuing further commits.
    ///
    /// Fencing semantics are optional, but required for exactly-once semantics.
    /// Non-transactional stores can ignore this aspect and achieve at-least-once.
    ///
    /// Where implemented, servers must guarantee that no other streams of this
    /// |fence_id| (now "zombie" streams) can commit transactions, and must then
    /// return the final checkpoint committed by this |fence_id| in its response.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Open {
        /// Endpoint type addressed by this request.
        #[prost(enumeration = "super::super::flow::EndpointType", tag = "1")]
        pub endpoint_type: i32,
        /// Driver-specific configuration, as an encoded JSON object.
        #[prost(string, tag = "2")]
        pub endpoint_config_json: ::prost::alloc::string::String,
        /// Fields represents the projection fields to be stored. This repeats the selection and ordering
        /// of the last Apply RPC, but is provided here also as a convenience.
        #[prost(message, optional, tag = "3")]
        pub fields: ::core::option::Option<super::super::flow::FieldSelection>,
        /// Stable producer ID (aka Flow runtime shard ID) to fence.
        #[prost(string, tag = "4")]
        pub fence_id: ::prost::alloc::string::String,
        /// Last-persisted driver checkpoint from a previous transaction stream.
        /// Or empty, if the driver hasn't returned a checkpoint.
        #[prost(bytes = "vec", tag = "5")]
        pub driver_checkpoint: ::prost::alloc::vec::Vec<u8>,
    }
    /// Load one or more documents identified by key.
    /// Keys may included documents which have never before been stored,
    /// but a given key will be sent in a transaction Load just one time.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Load {
        /// Byte arena of the request.
        #[prost(bytes = "vec", tag = "2")]
        pub arena: ::prost::alloc::vec::Vec<u8>,
        /// Packed tuples of collection keys, enumerating the documents to load.
        #[prost(message, repeated, tag = "3")]
        pub packed_keys: ::prost::alloc::vec::Vec<super::super::flow::Slice>,
    }
    /// Prepare to commit. No further Loads will be sent in this transaction.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Prepare {
        /// Flow checkpoint to commit with this transaction.
        #[prost(bytes = "vec", tag = "1")]
        pub flow_checkpoint: ::prost::alloc::vec::Vec<u8>,
    }
    /// Store documents of this transaction commit.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Store {
        /// Byte arena of the request.
        #[prost(bytes = "vec", tag = "1")]
        pub arena: ::prost::alloc::vec::Vec<u8>,
        /// Packed tuples holding keys of each document.
        #[prost(message, repeated, tag = "2")]
        pub packed_keys: ::prost::alloc::vec::Vec<super::super::flow::Slice>,
        /// Packed tuples holding values for each document.
        #[prost(message, repeated, tag = "3")]
        pub packed_values: ::prost::alloc::vec::Vec<super::super::flow::Slice>,
        /// JSON documents.
        #[prost(message, repeated, tag = "4")]
        pub docs_json: ::prost::alloc::vec::Vec<super::super::flow::Slice>,
        /// Exists is true if this document as previously been loaded or stored.
        #[prost(bool, repeated, tag = "5")]
        pub exists: ::prost::alloc::vec::Vec<bool>,
    }
    /// Commit the transaction.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Commit {}
}
/// TransactionResponse is the response type of a Transaction RPC.
/// It will have exactly one top-level field set, which represents its message type.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransactionResponse {
    #[prost(message, optional, tag = "1")]
    pub opened: ::core::option::Option<transaction_response::Opened>,
    #[prost(message, optional, tag = "2")]
    pub loaded: ::core::option::Option<transaction_response::Loaded>,
    #[prost(message, optional, tag = "3")]
    pub prepared: ::core::option::Option<transaction_response::Prepared>,
    #[prost(message, optional, tag = "4")]
    pub committed: ::core::option::Option<transaction_response::Committed>,
}
/// Nested message and enum types in `TransactionResponse`.
pub mod transaction_response {
    /// Opened responds to TransactionRequest.Open of the client.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Opened {
        /// Flow checkpoint which was previously committed with this |fence_id|.
        /// May be nil, if unknown or if transactional semantics are not supported,
        /// in which case the Flow runtime will use its most-recent persisted checkpoint.
        #[prost(bytes = "vec", tag = "1")]
        pub flow_checkpoint: ::prost::alloc::vec::Vec<u8>,
        /// Materialize combined delta updates of documents rather than full reductions.
        ///
        /// When set, the Flow runtime will not attempt to load documents via
        /// TransactionRequest.Load, and also disables re-use of cached documents
        /// stored in prior transactions. Each stored document is exclusively combined
        /// from updates processed by the runtime within the current transaction only.
        ///
        /// This is appropriate for drivers over streams, WebHooks, and append-only files.
        ///
        /// For example, given a collection which reduces a sum count for each key,
        /// its materialization will produce a stream of delta updates to the count,
        /// such that a reader of the stream will arrive at the correct total count.
        #[prost(bool, tag = "2")]
        pub delta_updates: bool,
    }
    /// Loaded responds to TransactionRequest.Loads of the client.
    /// It returns documents of requested keys which have previously been stored.
    /// Keys not found in the store MUST be omitted. Documents may be in any order,
    /// both within and across Loaded response messages, but a document of a given
    /// key MUST be sent at most one time in a Transaction.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Loaded {
        /// Byte arena of the request.
        #[prost(bytes = "vec", tag = "1")]
        pub arena: ::prost::alloc::vec::Vec<u8>,
        /// Loaded JSON documents.
        #[prost(message, repeated, tag = "2")]
        pub docs_json: ::prost::alloc::vec::Vec<super::super::flow::Slice>,
    }
    /// Prepared responds to a TransactionRequest.Prepare of the client.
    /// No further Loaded responses will be sent.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Prepared {
        /// Optional driver checkpoint of this transaction.
        /// If provided, the most recent checkpoint will be persisted by the
        /// Flow runtime and returned in a future Fence request.
        #[prost(bytes = "vec", tag = "1")]
        pub driver_checkpoint: ::prost::alloc::vec::Vec<u8>,
    }
    /// Acknowledge the transaction as committed.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Committed {}
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
        #[doc = " Apply a CollectionSpec and FieldSelections to a materialization target."]
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
        #[doc = " Transactions is a very long lived RPC through which the Flow runtime and a"]
        #[doc = " materialization endpoint cooperatively execute an unbounded number of"]
        #[doc = " transactions. The RPC follows the following lifecycle:"]
        #[doc = ""]
        #[doc = " :Open:"]
        #[doc = "    - The Flow runtime client sends TransactionRequest.Open,"]
        #[doc = "      opening the stream and requesting it be fenced from other streams."]
        #[doc = " :Opened:"]
        #[doc = "    - The driver server sends TransactionResponse.Opened after,"]
        #[doc = "      where supported, ensuring other stream clients are fenced."]
        #[doc = ""]
        #[doc = " TransactionRequest.Open and TransactionResponse.Opened are sent only"]
        #[doc = " once, at the commencement of the stream. Thereafter the protocol loops:"]
        #[doc = ""]
        #[doc = " :Load:"]
        #[doc = "    - The client sends zero or more TransactionRequest.Load."]
        #[doc = "    - The driver server may immediately send any number of"]
        #[doc = "      TransactionResponse.Loaded in response."]
        #[doc = "    - Or, it may defer responding with some or all loads until later."]
        #[doc = " :Prepare:"]
        #[doc = "    - The client sends TransactionRequest.Prepare."]
        #[doc = "    - At this time, the server must flush remaining TransactionResponse.Loaded."]
        #[doc = " :Prepared:"]
        #[doc = "    - The server sends TransactionResponse.Prepared."]
        #[doc = " :Store:"]
        #[doc = "    - The client sends zero or more TransactionRequest.Store."]
        #[doc = " :Commit:"]
        #[doc = "    - The client sends TransactionRequest.Commit."]
        #[doc = "    - The server commits the prepared Flow checkpoint and all stores."]
        #[doc = " :Committed:"]
        #[doc = "    - The server sends TransactionResponse.Committed."]
        #[doc = "    - The Flow runtime persists the prepared driver checkpoint."]
        #[doc = "    - Client and server begin a new transaction and loop to \"Load\"."]
        #[doc = ""]
        #[doc = " An error of any kind rolls back the transaction in progress and terminates the stream."]
        pub async fn transactions(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = super::TransactionRequest>,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::TransactionResponse>>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/materialize.Driver/Transactions");
            self.inner
                .streaming(request.into_streaming_request(), path, codec)
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
        #[doc = " Validate that a CollectionSpec is compatible with a materialization target,"]
        #[doc = " and return constraints over the projections of the collection."]
        async fn validate(
            &self,
            request: tonic::Request<super::ValidateRequest>,
        ) -> Result<tonic::Response<super::ValidateResponse>, tonic::Status>;
        #[doc = " Apply a CollectionSpec and FieldSelections to a materialization target."]
        async fn apply(
            &self,
            request: tonic::Request<super::ApplyRequest>,
        ) -> Result<tonic::Response<super::ApplyResponse>, tonic::Status>;
        #[doc = "Server streaming response type for the Transactions method."]
        type TransactionsStream: Stream<Item = Result<super::TransactionResponse, tonic::Status>>
            + Send
            + Sync
            + 'static;
        #[doc = " Transactions is a very long lived RPC through which the Flow runtime and a"]
        #[doc = " materialization endpoint cooperatively execute an unbounded number of"]
        #[doc = " transactions. The RPC follows the following lifecycle:"]
        #[doc = ""]
        #[doc = " :Open:"]
        #[doc = "    - The Flow runtime client sends TransactionRequest.Open,"]
        #[doc = "      opening the stream and requesting it be fenced from other streams."]
        #[doc = " :Opened:"]
        #[doc = "    - The driver server sends TransactionResponse.Opened after,"]
        #[doc = "      where supported, ensuring other stream clients are fenced."]
        #[doc = ""]
        #[doc = " TransactionRequest.Open and TransactionResponse.Opened are sent only"]
        #[doc = " once, at the commencement of the stream. Thereafter the protocol loops:"]
        #[doc = ""]
        #[doc = " :Load:"]
        #[doc = "    - The client sends zero or more TransactionRequest.Load."]
        #[doc = "    - The driver server may immediately send any number of"]
        #[doc = "      TransactionResponse.Loaded in response."]
        #[doc = "    - Or, it may defer responding with some or all loads until later."]
        #[doc = " :Prepare:"]
        #[doc = "    - The client sends TransactionRequest.Prepare."]
        #[doc = "    - At this time, the server must flush remaining TransactionResponse.Loaded."]
        #[doc = " :Prepared:"]
        #[doc = "    - The server sends TransactionResponse.Prepared."]
        #[doc = " :Store:"]
        #[doc = "    - The client sends zero or more TransactionRequest.Store."]
        #[doc = " :Commit:"]
        #[doc = "    - The client sends TransactionRequest.Commit."]
        #[doc = "    - The server commits the prepared Flow checkpoint and all stores."]
        #[doc = " :Committed:"]
        #[doc = "    - The server sends TransactionResponse.Committed."]
        #[doc = "    - The Flow runtime persists the prepared driver checkpoint."]
        #[doc = "    - Client and server begin a new transaction and loop to \"Load\"."]
        #[doc = ""]
        #[doc = " An error of any kind rolls back the transaction in progress and terminates the stream."]
        async fn transactions(
            &self,
            request: tonic::Request<tonic::Streaming<super::TransactionRequest>>,
        ) -> Result<tonic::Response<Self::TransactionsStream>, tonic::Status>;
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
                "/materialize.Driver/Transactions" => {
                    #[allow(non_camel_case_types)]
                    struct TransactionsSvc<T: Driver>(pub Arc<T>);
                    impl<T: Driver> tonic::server::StreamingService<super::TransactionRequest> for TransactionsSvc<T> {
                        type Response = super::TransactionResponse;
                        type ResponseStream = T::TransactionsStream;
                        type Future =
                            BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<tonic::Streaming<super::TransactionRequest>>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).transactions(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1;
                        let inner = inner.0;
                        let method = TransactionsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
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
