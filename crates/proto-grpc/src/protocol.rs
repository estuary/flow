/// Generated client implementations.
#[cfg(feature = "broker_client")]
pub mod journal_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    /// Journal is the Gazette broker service API for interacting with Journals.
    #[derive(Debug, Clone)]
    pub struct JournalClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl JournalClient<tonic::transport::Channel> {
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
    impl<T> JournalClient<T>
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
        ) -> JournalClient<InterceptedService<T, F>>
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
            JournalClient::new(InterceptedService::new(inner, interceptor))
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
        /// List Journals, their JournalSpecs and current Routes.
        pub async fn list(
            &mut self,
            request: impl tonic::IntoRequest<::proto_gazette::broker::ListRequest>,
        ) -> Result<
            tonic::Response<::proto_gazette::broker::ListResponse>,
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
            let path = http::uri::PathAndQuery::from_static("/protocol.Journal/List");
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Apply changes to the collection of Journals managed by the brokers.
        pub async fn apply(
            &mut self,
            request: impl tonic::IntoRequest<::proto_gazette::broker::ApplyRequest>,
        ) -> Result<
            tonic::Response<::proto_gazette::broker::ApplyResponse>,
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
            let path = http::uri::PathAndQuery::from_static("/protocol.Journal/Apply");
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Read from a specific Journal.
        pub async fn read(
            &mut self,
            request: impl tonic::IntoRequest<::proto_gazette::broker::ReadRequest>,
        ) -> Result<
            tonic::Response<
                tonic::codec::Streaming<::proto_gazette::broker::ReadResponse>,
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
            let path = http::uri::PathAndQuery::from_static("/protocol.Journal/Read");
            self.inner.server_streaming(request.into_request(), path, codec).await
        }
        /// Append content to a specific Journal.
        pub async fn append(
            &mut self,
            request: impl tonic::IntoStreamingRequest<
                Message = ::proto_gazette::broker::AppendRequest,
            >,
        ) -> Result<
            tonic::Response<::proto_gazette::broker::AppendResponse>,
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
            let path = http::uri::PathAndQuery::from_static("/protocol.Journal/Append");
            self.inner
                .client_streaming(request.into_streaming_request(), path, codec)
                .await
        }
        /// Replicate appended content of a Journal. Replicate is used between broker
        /// peers in the course of processing Append transactions, but is not intended
        /// for direct use by clients.
        pub async fn replicate(
            &mut self,
            request: impl tonic::IntoStreamingRequest<
                Message = ::proto_gazette::broker::ReplicateRequest,
            >,
        ) -> Result<
            tonic::Response<
                tonic::codec::Streaming<::proto_gazette::broker::ReplicateResponse>,
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
            let path = http::uri::PathAndQuery::from_static(
                "/protocol.Journal/Replicate",
            );
            self.inner.streaming(request.into_streaming_request(), path, codec).await
        }
        /// List Fragments of a Journal.
        pub async fn list_fragments(
            &mut self,
            request: impl tonic::IntoRequest<::proto_gazette::broker::FragmentsRequest>,
        ) -> Result<
            tonic::Response<::proto_gazette::broker::FragmentsResponse>,
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
                "/protocol.Journal/ListFragments",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
#[cfg(feature = "broker_server")]
pub mod journal_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with JournalServer.
    #[async_trait]
    pub trait Journal: Send + Sync + 'static {
        /// List Journals, their JournalSpecs and current Routes.
        async fn list(
            &self,
            request: tonic::Request<::proto_gazette::broker::ListRequest>,
        ) -> Result<
            tonic::Response<::proto_gazette::broker::ListResponse>,
            tonic::Status,
        >;
        /// Apply changes to the collection of Journals managed by the brokers.
        async fn apply(
            &self,
            request: tonic::Request<::proto_gazette::broker::ApplyRequest>,
        ) -> Result<
            tonic::Response<::proto_gazette::broker::ApplyResponse>,
            tonic::Status,
        >;
        ///Server streaming response type for the Read method.
        type ReadStream: futures_core::Stream<
                Item = Result<::proto_gazette::broker::ReadResponse, tonic::Status>,
            >
            + Send
            + 'static;
        /// Read from a specific Journal.
        async fn read(
            &self,
            request: tonic::Request<::proto_gazette::broker::ReadRequest>,
        ) -> Result<tonic::Response<Self::ReadStream>, tonic::Status>;
        /// Append content to a specific Journal.
        async fn append(
            &self,
            request: tonic::Request<
                tonic::Streaming<::proto_gazette::broker::AppendRequest>,
            >,
        ) -> Result<
            tonic::Response<::proto_gazette::broker::AppendResponse>,
            tonic::Status,
        >;
        ///Server streaming response type for the Replicate method.
        type ReplicateStream: futures_core::Stream<
                Item = Result<::proto_gazette::broker::ReplicateResponse, tonic::Status>,
            >
            + Send
            + 'static;
        /// Replicate appended content of a Journal. Replicate is used between broker
        /// peers in the course of processing Append transactions, but is not intended
        /// for direct use by clients.
        async fn replicate(
            &self,
            request: tonic::Request<
                tonic::Streaming<::proto_gazette::broker::ReplicateRequest>,
            >,
        ) -> Result<tonic::Response<Self::ReplicateStream>, tonic::Status>;
        /// List Fragments of a Journal.
        async fn list_fragments(
            &self,
            request: tonic::Request<::proto_gazette::broker::FragmentsRequest>,
        ) -> Result<
            tonic::Response<::proto_gazette::broker::FragmentsResponse>,
            tonic::Status,
        >;
    }
    /// Journal is the Gazette broker service API for interacting with Journals.
    #[derive(Debug)]
    pub struct JournalServer<T: Journal> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: Journal> JournalServer<T> {
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
    impl<T, B> tonic::codegen::Service<http::Request<B>> for JournalServer<T>
    where
        T: Journal,
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
                "/protocol.Journal/List" => {
                    #[allow(non_camel_case_types)]
                    struct ListSvc<T: Journal>(pub Arc<T>);
                    impl<
                        T: Journal,
                    > tonic::server::UnaryService<::proto_gazette::broker::ListRequest>
                    for ListSvc<T> {
                        type Response = ::proto_gazette::broker::ListResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<::proto_gazette::broker::ListRequest>,
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
                "/protocol.Journal/Apply" => {
                    #[allow(non_camel_case_types)]
                    struct ApplySvc<T: Journal>(pub Arc<T>);
                    impl<
                        T: Journal,
                    > tonic::server::UnaryService<::proto_gazette::broker::ApplyRequest>
                    for ApplySvc<T> {
                        type Response = ::proto_gazette::broker::ApplyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                ::proto_gazette::broker::ApplyRequest,
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
                "/protocol.Journal/Read" => {
                    #[allow(non_camel_case_types)]
                    struct ReadSvc<T: Journal>(pub Arc<T>);
                    impl<
                        T: Journal,
                    > tonic::server::ServerStreamingService<
                        ::proto_gazette::broker::ReadRequest,
                    > for ReadSvc<T> {
                        type Response = ::proto_gazette::broker::ReadResponse;
                        type ResponseStream = T::ReadStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<::proto_gazette::broker::ReadRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).read(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ReadSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/protocol.Journal/Append" => {
                    #[allow(non_camel_case_types)]
                    struct AppendSvc<T: Journal>(pub Arc<T>);
                    impl<
                        T: Journal,
                    > tonic::server::ClientStreamingService<
                        ::proto_gazette::broker::AppendRequest,
                    > for AppendSvc<T> {
                        type Response = ::proto_gazette::broker::AppendResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                tonic::Streaming<::proto_gazette::broker::AppendRequest>,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).append(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = AppendSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.client_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/protocol.Journal/Replicate" => {
                    #[allow(non_camel_case_types)]
                    struct ReplicateSvc<T: Journal>(pub Arc<T>);
                    impl<
                        T: Journal,
                    > tonic::server::StreamingService<
                        ::proto_gazette::broker::ReplicateRequest,
                    > for ReplicateSvc<T> {
                        type Response = ::proto_gazette::broker::ReplicateResponse;
                        type ResponseStream = T::ReplicateStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                tonic::Streaming<::proto_gazette::broker::ReplicateRequest>,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).replicate(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ReplicateSvc(inner);
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
                "/protocol.Journal/ListFragments" => {
                    #[allow(non_camel_case_types)]
                    struct ListFragmentsSvc<T: Journal>(pub Arc<T>);
                    impl<
                        T: Journal,
                    > tonic::server::UnaryService<
                        ::proto_gazette::broker::FragmentsRequest,
                    > for ListFragmentsSvc<T> {
                        type Response = ::proto_gazette::broker::FragmentsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                ::proto_gazette::broker::FragmentsRequest,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).list_fragments(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ListFragmentsSvc(inner);
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
    impl<T: Journal> Clone for JournalServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: Journal> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Journal> tonic::server::NamedService for JournalServer<T> {
        const NAME: &'static str = "protocol.Journal";
    }
}
