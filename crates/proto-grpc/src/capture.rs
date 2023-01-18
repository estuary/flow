/// Generated client implementations.
#[cfg(feature = "capture_client")]
pub mod driver_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    /// Driver is the service implemented by a capture connector.
    #[derive(Debug, Clone)]
    pub struct DriverClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl DriverClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
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
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> DriverClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            DriverClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Spec returns the specification definition of this driver.
        /// Notably this includes its endpoint and resource configuration JSON schema.
        pub async fn spec(
            &mut self,
            request: impl tonic::IntoRequest<::proto_flow::capture::SpecRequest>,
        ) -> Result<
            tonic::Response<::proto_flow::capture::SpecResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/capture.Driver/Spec");
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Discover returns the set of resources available from this Driver.
        pub async fn discover(
            &mut self,
            request: impl tonic::IntoRequest<::proto_flow::capture::DiscoverRequest>,
        ) -> Result<
            tonic::Response<::proto_flow::capture::DiscoverResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/capture.Driver/Discover");
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Validate that store resources and proposed collection bindings are
        /// compatible.
        pub async fn validate(
            &mut self,
            request: impl tonic::IntoRequest<::proto_flow::capture::ValidateRequest>,
        ) -> Result<
            tonic::Response<::proto_flow::capture::ValidateResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/capture.Driver/Validate");
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// ApplyUpsert applies a new or updated capture to the store.
        pub async fn apply_upsert(
            &mut self,
            request: impl tonic::IntoRequest<::proto_flow::capture::ApplyRequest>,
        ) -> Result<
            tonic::Response<::proto_flow::capture::ApplyResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/capture.Driver/ApplyUpsert",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// ApplyDelete deletes an existing capture from the store.
        pub async fn apply_delete(
            &mut self,
            request: impl tonic::IntoRequest<::proto_flow::capture::ApplyRequest>,
        ) -> Result<
            tonic::Response<::proto_flow::capture::ApplyResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/capture.Driver/ApplyDelete",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Pull is a very long lived RPC through which the Flow runtime and a
        /// Driver cooperatively execute an unbounded number of transactions.
        ///
        /// The Pull workflow pulls streams of documents into capturing Flow
        /// collections. Streams are incremental and resume-able, with resumption
        /// semantics defined by the driver. The Flow Runtime uses a transactional
        /// recovery log to support this workflow, and the driver may persist arbitrary
        /// driver checkpoints into that log as part of the RPC lifecycle,
        /// to power its chosen resumption semantics.
        ///
        /// Pull tasks are split-able, and many concurrent invocations of the RPC
        /// may collectively capture from a source, where each task split has an
        /// identified range of keys it's responsible for. The meaning of a "key",
        /// and it's application within the remote store being captured from, is up
        /// to the driver. The driver might map partitions or shards into the keyspace,
        /// and from there to a covering task split. Or, it might map distinct files,
        /// or some other unit of scaling.
        ///
        /// RPC Lifecycle
        /// =============
        ///
        /// :PullRequest.Open:
        ///    - The Flow runtime opens the pull stream.
        /// :PullResponse.Opened:
        ///    - The driver responds with Opened.
        ///
        /// PullRequest.Open and PullRequest.Opened are sent only once, at the
        /// commencement of the stream. Thereafter the protocol loops:
        ///
        /// :PullResponse.Documents:
        ///    - The driver tells the runtime of some documents, which are pending a
        ///      future Checkpoint.
        ///    - If the driver sends multiple Documents messages without an
        ///      interleaving Checkpoint, the Flow runtime MUST commit
        ///      documents of all such messages in a single transaction.
        /// :PullResponse.Checkpoint:
        ///    - The driver tells the runtime of a checkpoint: a watermark in the
        ///      captured documents stream which is eligble to be used as a
        ///      transaction commit boundary.
        ///    - Whether the checkpoint becomes a commit boundary is at the
        ///      discretion of the Flow runtime. It may combine multiple checkpoints
        ///      into a single transaction.
        /// :PullRequest.Acknowledge:
        ///    - The Flow runtime tells the driver that its Checkpoint has committed.
        ///    - The runtime sends one ordered Acknowledge for each Checkpoint.
        ///
        pub async fn pull(
            &mut self,
            request: impl tonic::IntoStreamingRequest<
                Message = ::proto_flow::capture::PullRequest,
            >,
        ) -> Result<
            tonic::Response<
                tonic::codec::Streaming<::proto_flow::capture::PullResponse>,
            >,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/capture.Driver/Pull");
            self.inner.streaming(request.into_streaming_request(), path, codec).await
        }
    }
}
/// Generated client implementations.
#[cfg(feature = "capture_client")]
pub mod runtime_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    /// Runtime is the Flow runtime service which implements push-based captures.
    #[derive(Debug, Clone)]
    pub struct RuntimeClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl RuntimeClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
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
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> RuntimeClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            RuntimeClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Push may be a short or very long lived RPC through which the Flow runtime
        /// and a driver cooperatively execute an unbounded number of transactions.
        ///
        /// The Push workflow pushes streams of documents into capturing Flow
        /// collections. The driver is responsible for initiation and resumption of
        /// push streams. The Flow runtime uses a transactional recovery log to support
        /// this workflow, and the driver may persist arbitrary driver checkpoints into
        /// that log as part of the RPC lifecycle, to power its chosen resumption
        /// semantics.
        ///
        /// A push RPC is evaluated against a specific task shard split, which is
        /// encoded in the PushRequest.Open.Header. A driver may perform its own load
        /// balancing by obtain a shard listing and embedding a selected shard into
        /// that header. Or, it may leave it empty and an arbitary shard will be
        /// randomly chosen for it.
        ///
        /// RPC Lifecycle
        /// =============
        ///
        /// :PushRequest.Open:
        ///    - The driver opens the push stream, naming its capture and
        ///      optional routing header.
        /// :PushResponse.Opened:
        ///    - The Flow runtime responds with Opened, which tells the driver
        ///      of the specific CaptureSpec and [key_begin, key_end] range of
        ///      this RPC, as well as the last driver checkpoint.
        ///    - The semantics and treatment of the key range is up to the driver.
        ///
        /// PushRequest.Open and PushRequest.Opened are sent only once, at the
        /// commencement of the stream. Thereafter the protocol loops:
        ///
        /// :PushRequest.Documents:
        ///    - The driver tells the runtime of some documents, which are pending a
        ///      future Checkpoint.
        ///    - If the driver sends multiple Documents messages without an
        ///      interleaving Checkpoint, the Flow runtime MUST commit
        ///      documents of all such messages in a single transaction.
        /// :PushRequest.Checkpoint:
        ///    - The driver tells the runtime of a checkpoint: a watermark in the
        ///      captured documents stream which is eligble to be used as a
        ///      transaction commit boundary.
        ///    - Whether the checkpoint becomes a commit boundary is at the
        ///      discretion of the Flow runtime. It may combine multiple checkpoints
        ///      into a single transaction.
        /// :PushResponse.Acknowledge:
        ///    - The Flow runtime tells the driver that its Checkpoint has committed.
        ///    - The runtime sends one ordered Acknowledge for each Checkpoint.
        ///
        pub async fn push(
            &mut self,
            request: impl tonic::IntoStreamingRequest<
                Message = ::proto_flow::capture::PushRequest,
            >,
        ) -> Result<
            tonic::Response<
                tonic::codec::Streaming<::proto_flow::capture::PushResponse>,
            >,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/capture.Runtime/Push");
            self.inner.streaming(request.into_streaming_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
#[cfg(feature = "capture_server")]
pub mod driver_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with DriverServer.
    #[async_trait]
    pub trait Driver: Send + Sync + 'static {
        /// Spec returns the specification definition of this driver.
        /// Notably this includes its endpoint and resource configuration JSON schema.
        async fn spec(
            &self,
            request: tonic::Request<::proto_flow::capture::SpecRequest>,
        ) -> Result<tonic::Response<::proto_flow::capture::SpecResponse>, tonic::Status>;
        /// Discover returns the set of resources available from this Driver.
        async fn discover(
            &self,
            request: tonic::Request<::proto_flow::capture::DiscoverRequest>,
        ) -> Result<
            tonic::Response<::proto_flow::capture::DiscoverResponse>,
            tonic::Status,
        >;
        /// Validate that store resources and proposed collection bindings are
        /// compatible.
        async fn validate(
            &self,
            request: tonic::Request<::proto_flow::capture::ValidateRequest>,
        ) -> Result<
            tonic::Response<::proto_flow::capture::ValidateResponse>,
            tonic::Status,
        >;
        /// ApplyUpsert applies a new or updated capture to the store.
        async fn apply_upsert(
            &self,
            request: tonic::Request<::proto_flow::capture::ApplyRequest>,
        ) -> Result<
            tonic::Response<::proto_flow::capture::ApplyResponse>,
            tonic::Status,
        >;
        /// ApplyDelete deletes an existing capture from the store.
        async fn apply_delete(
            &self,
            request: tonic::Request<::proto_flow::capture::ApplyRequest>,
        ) -> Result<
            tonic::Response<::proto_flow::capture::ApplyResponse>,
            tonic::Status,
        >;
        ///Server streaming response type for the Pull method.
        type PullStream: futures_core::Stream<
                Item = Result<::proto_flow::capture::PullResponse, tonic::Status>,
            >
            + Send
            + 'static;
        /// Pull is a very long lived RPC through which the Flow runtime and a
        /// Driver cooperatively execute an unbounded number of transactions.
        ///
        /// The Pull workflow pulls streams of documents into capturing Flow
        /// collections. Streams are incremental and resume-able, with resumption
        /// semantics defined by the driver. The Flow Runtime uses a transactional
        /// recovery log to support this workflow, and the driver may persist arbitrary
        /// driver checkpoints into that log as part of the RPC lifecycle,
        /// to power its chosen resumption semantics.
        ///
        /// Pull tasks are split-able, and many concurrent invocations of the RPC
        /// may collectively capture from a source, where each task split has an
        /// identified range of keys it's responsible for. The meaning of a "key",
        /// and it's application within the remote store being captured from, is up
        /// to the driver. The driver might map partitions or shards into the keyspace,
        /// and from there to a covering task split. Or, it might map distinct files,
        /// or some other unit of scaling.
        ///
        /// RPC Lifecycle
        /// =============
        ///
        /// :PullRequest.Open:
        ///    - The Flow runtime opens the pull stream.
        /// :PullResponse.Opened:
        ///    - The driver responds with Opened.
        ///
        /// PullRequest.Open and PullRequest.Opened are sent only once, at the
        /// commencement of the stream. Thereafter the protocol loops:
        ///
        /// :PullResponse.Documents:
        ///    - The driver tells the runtime of some documents, which are pending a
        ///      future Checkpoint.
        ///    - If the driver sends multiple Documents messages without an
        ///      interleaving Checkpoint, the Flow runtime MUST commit
        ///      documents of all such messages in a single transaction.
        /// :PullResponse.Checkpoint:
        ///    - The driver tells the runtime of a checkpoint: a watermark in the
        ///      captured documents stream which is eligble to be used as a
        ///      transaction commit boundary.
        ///    - Whether the checkpoint becomes a commit boundary is at the
        ///      discretion of the Flow runtime. It may combine multiple checkpoints
        ///      into a single transaction.
        /// :PullRequest.Acknowledge:
        ///    - The Flow runtime tells the driver that its Checkpoint has committed.
        ///    - The runtime sends one ordered Acknowledge for each Checkpoint.
        ///
        async fn pull(
            &self,
            request: tonic::Request<tonic::Streaming<::proto_flow::capture::PullRequest>>,
        ) -> Result<tonic::Response<Self::PullStream>, tonic::Status>;
    }
    /// Driver is the service implemented by a capture connector.
    #[derive(Debug)]
    pub struct DriverServer<T: Driver> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: Driver> DriverServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for DriverServer<T>
    where
        T: Driver,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/capture.Driver/Spec" => {
                    #[allow(non_camel_case_types)]
                    struct SpecSvc<T: Driver>(pub Arc<T>);
                    impl<
                        T: Driver,
                    > tonic::server::UnaryService<::proto_flow::capture::SpecRequest>
                    for SpecSvc<T> {
                        type Response = ::proto_flow::capture::SpecResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<::proto_flow::capture::SpecRequest>,
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
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
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
                    impl<
                        T: Driver,
                    > tonic::server::UnaryService<::proto_flow::capture::DiscoverRequest>
                    for DiscoverSvc<T> {
                        type Response = ::proto_flow::capture::DiscoverResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                ::proto_flow::capture::DiscoverRequest,
                            >,
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
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
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
                    impl<
                        T: Driver,
                    > tonic::server::UnaryService<::proto_flow::capture::ValidateRequest>
                    for ValidateSvc<T> {
                        type Response = ::proto_flow::capture::ValidateResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                ::proto_flow::capture::ValidateRequest,
                            >,
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
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
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
                    impl<
                        T: Driver,
                    > tonic::server::UnaryService<::proto_flow::capture::ApplyRequest>
                    for ApplyUpsertSvc<T> {
                        type Response = ::proto_flow::capture::ApplyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<::proto_flow::capture::ApplyRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).apply_upsert(request).await
                            };
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
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
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
                    impl<
                        T: Driver,
                    > tonic::server::UnaryService<::proto_flow::capture::ApplyRequest>
                    for ApplyDeleteSvc<T> {
                        type Response = ::proto_flow::capture::ApplyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<::proto_flow::capture::ApplyRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).apply_delete(request).await
                            };
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
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
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
                    impl<
                        T: Driver,
                    > tonic::server::StreamingService<::proto_flow::capture::PullRequest>
                    for PullSvc<T> {
                        type Response = ::proto_flow::capture::PullResponse;
                        type ResponseStream = T::PullStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                tonic::Streaming<::proto_flow::capture::PullRequest>,
                            >,
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
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
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
    impl<T: Driver> tonic::server::NamedService for DriverServer<T> {
        const NAME: &'static str = "capture.Driver";
    }
}
/// Generated server implementations.
#[cfg(feature = "capture_server")]
pub mod runtime_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with RuntimeServer.
    #[async_trait]
    pub trait Runtime: Send + Sync + 'static {
        ///Server streaming response type for the Push method.
        type PushStream: futures_core::Stream<
                Item = Result<::proto_flow::capture::PushResponse, tonic::Status>,
            >
            + Send
            + 'static;
        /// Push may be a short or very long lived RPC through which the Flow runtime
        /// and a driver cooperatively execute an unbounded number of transactions.
        ///
        /// The Push workflow pushes streams of documents into capturing Flow
        /// collections. The driver is responsible for initiation and resumption of
        /// push streams. The Flow runtime uses a transactional recovery log to support
        /// this workflow, and the driver may persist arbitrary driver checkpoints into
        /// that log as part of the RPC lifecycle, to power its chosen resumption
        /// semantics.
        ///
        /// A push RPC is evaluated against a specific task shard split, which is
        /// encoded in the PushRequest.Open.Header. A driver may perform its own load
        /// balancing by obtain a shard listing and embedding a selected shard into
        /// that header. Or, it may leave it empty and an arbitary shard will be
        /// randomly chosen for it.
        ///
        /// RPC Lifecycle
        /// =============
        ///
        /// :PushRequest.Open:
        ///    - The driver opens the push stream, naming its capture and
        ///      optional routing header.
        /// :PushResponse.Opened:
        ///    - The Flow runtime responds with Opened, which tells the driver
        ///      of the specific CaptureSpec and [key_begin, key_end] range of
        ///      this RPC, as well as the last driver checkpoint.
        ///    - The semantics and treatment of the key range is up to the driver.
        ///
        /// PushRequest.Open and PushRequest.Opened are sent only once, at the
        /// commencement of the stream. Thereafter the protocol loops:
        ///
        /// :PushRequest.Documents:
        ///    - The driver tells the runtime of some documents, which are pending a
        ///      future Checkpoint.
        ///    - If the driver sends multiple Documents messages without an
        ///      interleaving Checkpoint, the Flow runtime MUST commit
        ///      documents of all such messages in a single transaction.
        /// :PushRequest.Checkpoint:
        ///    - The driver tells the runtime of a checkpoint: a watermark in the
        ///      captured documents stream which is eligble to be used as a
        ///      transaction commit boundary.
        ///    - Whether the checkpoint becomes a commit boundary is at the
        ///      discretion of the Flow runtime. It may combine multiple checkpoints
        ///      into a single transaction.
        /// :PushResponse.Acknowledge:
        ///    - The Flow runtime tells the driver that its Checkpoint has committed.
        ///    - The runtime sends one ordered Acknowledge for each Checkpoint.
        ///
        async fn push(
            &self,
            request: tonic::Request<tonic::Streaming<::proto_flow::capture::PushRequest>>,
        ) -> Result<tonic::Response<Self::PushStream>, tonic::Status>;
    }
    /// Runtime is the Flow runtime service which implements push-based captures.
    #[derive(Debug)]
    pub struct RuntimeServer<T: Runtime> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: Runtime> RuntimeServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for RuntimeServer<T>
    where
        T: Runtime,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/capture.Runtime/Push" => {
                    #[allow(non_camel_case_types)]
                    struct PushSvc<T: Runtime>(pub Arc<T>);
                    impl<
                        T: Runtime,
                    > tonic::server::StreamingService<::proto_flow::capture::PushRequest>
                    for PushSvc<T> {
                        type Response = ::proto_flow::capture::PushResponse;
                        type ResponseStream = T::PushStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                tonic::Streaming<::proto_flow::capture::PushRequest>,
                            >,
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
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
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
    impl<T: Runtime> tonic::server::NamedService for RuntimeServer<T> {
        const NAME: &'static str = "capture.Runtime";
    }
}
