/// Generated client implementations.
#[cfg(feature = "consumer_client")]
pub mod shard_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    /// Shard is the Consumer service API for interacting with Shards. Applications
    /// are able to wrap or alter the behavior of Shard API implementations via the
    /// Service.ShardAPI structure. They're also able to implement additional gRPC
    /// service APIs which are registered against the common gRPC server.
    #[derive(Debug, Clone)]
    pub struct ShardClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ShardClient<tonic::transport::Channel> {
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
    impl<T> ShardClient<T>
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
        ) -> ShardClient<InterceptedService<T, F>>
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
            ShardClient::new(InterceptedService::new(inner, interceptor))
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
        /// Stat returns detailed status of a given Shard.
        pub async fn stat(
            &mut self,
            request: impl tonic::IntoRequest<::proto_gazette::consumer::StatRequest>,
        ) -> Result<
            tonic::Response<::proto_gazette::consumer::StatResponse>,
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
            let path = http::uri::PathAndQuery::from_static("/consumer.Shard/Stat");
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// List Shards, their ShardSpecs and their processing status.
        pub async fn list(
            &mut self,
            request: impl tonic::IntoRequest<::proto_gazette::consumer::ListRequest>,
        ) -> Result<
            tonic::Response<::proto_gazette::consumer::ListResponse>,
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
            let path = http::uri::PathAndQuery::from_static("/consumer.Shard/List");
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Apply changes to the collection of Shards managed by the consumer.
        pub async fn apply(
            &mut self,
            request: impl tonic::IntoRequest<::proto_gazette::consumer::ApplyRequest>,
        ) -> Result<
            tonic::Response<::proto_gazette::consumer::ApplyResponse>,
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
            let path = http::uri::PathAndQuery::from_static("/consumer.Shard/Apply");
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// GetHints fetches hints for a shard.
        pub async fn get_hints(
            &mut self,
            request: impl tonic::IntoRequest<::proto_gazette::consumer::GetHintsRequest>,
        ) -> Result<
            tonic::Response<::proto_gazette::consumer::GetHintsResponse>,
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
            let path = http::uri::PathAndQuery::from_static("/consumer.Shard/GetHints");
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Unassign a Shard.
        pub async fn unassign(
            &mut self,
            request: impl tonic::IntoRequest<::proto_gazette::consumer::UnassignRequest>,
        ) -> Result<
            tonic::Response<::proto_gazette::consumer::UnassignResponse>,
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
            let path = http::uri::PathAndQuery::from_static("/consumer.Shard/Unassign");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
#[cfg(feature = "consumer_server")]
pub mod shard_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with ShardServer.
    #[async_trait]
    pub trait Shard: Send + Sync + 'static {
        /// Stat returns detailed status of a given Shard.
        async fn stat(
            &self,
            request: tonic::Request<::proto_gazette::consumer::StatRequest>,
        ) -> Result<
            tonic::Response<::proto_gazette::consumer::StatResponse>,
            tonic::Status,
        >;
        /// List Shards, their ShardSpecs and their processing status.
        async fn list(
            &self,
            request: tonic::Request<::proto_gazette::consumer::ListRequest>,
        ) -> Result<
            tonic::Response<::proto_gazette::consumer::ListResponse>,
            tonic::Status,
        >;
        /// Apply changes to the collection of Shards managed by the consumer.
        async fn apply(
            &self,
            request: tonic::Request<::proto_gazette::consumer::ApplyRequest>,
        ) -> Result<
            tonic::Response<::proto_gazette::consumer::ApplyResponse>,
            tonic::Status,
        >;
        /// GetHints fetches hints for a shard.
        async fn get_hints(
            &self,
            request: tonic::Request<::proto_gazette::consumer::GetHintsRequest>,
        ) -> Result<
            tonic::Response<::proto_gazette::consumer::GetHintsResponse>,
            tonic::Status,
        >;
        /// Unassign a Shard.
        async fn unassign(
            &self,
            request: tonic::Request<::proto_gazette::consumer::UnassignRequest>,
        ) -> Result<
            tonic::Response<::proto_gazette::consumer::UnassignResponse>,
            tonic::Status,
        >;
    }
    /// Shard is the Consumer service API for interacting with Shards. Applications
    /// are able to wrap or alter the behavior of Shard API implementations via the
    /// Service.ShardAPI structure. They're also able to implement additional gRPC
    /// service APIs which are registered against the common gRPC server.
    #[derive(Debug)]
    pub struct ShardServer<T: Shard> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: Shard> ShardServer<T> {
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
    impl<T, B> tonic::codegen::Service<http::Request<B>> for ShardServer<T>
    where
        T: Shard,
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
                "/consumer.Shard/Stat" => {
                    #[allow(non_camel_case_types)]
                    struct StatSvc<T: Shard>(pub Arc<T>);
                    impl<
                        T: Shard,
                    > tonic::server::UnaryService<::proto_gazette::consumer::StatRequest>
                    for StatSvc<T> {
                        type Response = ::proto_gazette::consumer::StatResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                ::proto_gazette::consumer::StatRequest,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).stat(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = StatSvc(inner);
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
                "/consumer.Shard/List" => {
                    #[allow(non_camel_case_types)]
                    struct ListSvc<T: Shard>(pub Arc<T>);
                    impl<
                        T: Shard,
                    > tonic::server::UnaryService<::proto_gazette::consumer::ListRequest>
                    for ListSvc<T> {
                        type Response = ::proto_gazette::consumer::ListResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                ::proto_gazette::consumer::ListRequest,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).list(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ListSvc(inner);
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
                "/consumer.Shard/Apply" => {
                    #[allow(non_camel_case_types)]
                    struct ApplySvc<T: Shard>(pub Arc<T>);
                    impl<
                        T: Shard,
                    > tonic::server::UnaryService<
                        ::proto_gazette::consumer::ApplyRequest,
                    > for ApplySvc<T> {
                        type Response = ::proto_gazette::consumer::ApplyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                ::proto_gazette::consumer::ApplyRequest,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).apply(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ApplySvc(inner);
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
                "/consumer.Shard/GetHints" => {
                    #[allow(non_camel_case_types)]
                    struct GetHintsSvc<T: Shard>(pub Arc<T>);
                    impl<
                        T: Shard,
                    > tonic::server::UnaryService<
                        ::proto_gazette::consumer::GetHintsRequest,
                    > for GetHintsSvc<T> {
                        type Response = ::proto_gazette::consumer::GetHintsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                ::proto_gazette::consumer::GetHintsRequest,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get_hints(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetHintsSvc(inner);
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
                "/consumer.Shard/Unassign" => {
                    #[allow(non_camel_case_types)]
                    struct UnassignSvc<T: Shard>(pub Arc<T>);
                    impl<
                        T: Shard,
                    > tonic::server::UnaryService<
                        ::proto_gazette::consumer::UnassignRequest,
                    > for UnassignSvc<T> {
                        type Response = ::proto_gazette::consumer::UnassignResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                ::proto_gazette::consumer::UnassignRequest,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).unassign(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = UnassignSvc(inner);
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
    impl<T: Shard> Clone for ShardServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: Shard> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Shard> tonic::server::NamedService for ShardServer<T> {
        const NAME: &'static str = "consumer.Shard";
    }
}
