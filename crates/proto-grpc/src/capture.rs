/// Generated client implementations.
#[cfg(feature = "capture_client")]
pub mod connector_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    /// Captures is a very long lived RPC through which the Flow runtime and a
    /// connector cooperatively execute an unbounded number of transactions.
    ///
    /// The Pull workflow pulls streams of documents into capturing Flow
    /// collections. Streams are incremental and resume-able, with resumption
    /// semantics defined by the connector. The Flow Runtime uses a transactional
    /// recovery log to support this workflow, and the connector may persist arbitrary
    /// driver checkpoints into that log as part of the RPC lifecycle,
    /// to power its chosen resumption semantics.
    ///
    /// Pull tasks are split-able, and many concurrent invocations of the RPC
    /// may collectively capture from a source, where each task split has an
    /// identified range of keys it's responsible for. The meaning of a "key",
    /// and it's application within the remote store being captured from, is up
    /// to the connector. The connector might map partitions or shards into the keyspace,
    /// and from there to a covering task split. Or, it might map distinct files,
    /// or some other unit of scaling.
    ///
    /// RPC Lifecycle
    /// =============
    ///
    /// :Request.Open:
    ///    - The Flow runtime opens the pull stream.
    /// :Response.Opened:
    ///    - The connector responds with Opened.
    ///
    /// Request.Open and Request.Opened are sent only once, at the
    /// commencement of the stream. Thereafter the protocol loops:
    ///
    /// :Response.Captured:
    ///    - The connector tells the runtime of documents,
    ///      which are pending a future Checkpoint.
    ///    - If the connector sends multiple Documents messages without an
    ///      interleaving Checkpoint, the Flow runtime MUST commit
    ///      documents of all such messages in a single transaction.
    /// :Response.Checkpoint:
    ///    - The connector tells the runtime of a checkpoint: a watermark in the
    ///      captured documents stream which is eligble to be used as a
    ///      transaction commit boundary.
    ///    - Whether the checkpoint becomes a commit boundary is at the
    ///      discretion of the Flow runtime. It may combine multiple checkpoints
    ///      into a single transaction.
    /// :Request.Acknowledge:
    ///    - The Flow runtime tells the connector that its Checkpoint has committed.
    ///    - The runtime sends one ordered Acknowledge for each Checkpoint.
    #[derive(Debug, Clone)]
    pub struct ConnectorClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ConnectorClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> ConnectorClient<T>
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
        ) -> ConnectorClient<InterceptedService<T, F>>
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
            ConnectorClient::new(InterceptedService::new(inner, interceptor))
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
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        pub async fn capture(
            &mut self,
            request: impl tonic::IntoStreamingRequest<
                Message = ::proto_flow::capture::Request,
            >,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<::proto_flow::capture::Response>>,
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
                "/capture.Connector/Capture",
            );
            let mut req = request.into_streaming_request();
            req.extensions_mut().insert(GrpcMethod::new("capture.Connector", "Capture"));
            self.inner.streaming(req, path, codec).await
        }
    }
}
/// Generated server implementations.
#[cfg(feature = "capture_server")]
pub mod connector_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with ConnectorServer.
    #[async_trait]
    pub trait Connector: Send + Sync + 'static {
        /// Server streaming response type for the Capture method.
        type CaptureStream: futures_core::Stream<
                Item = std::result::Result<
                    ::proto_flow::capture::Response,
                    tonic::Status,
                >,
            >
            + Send
            + 'static;
        async fn capture(
            &self,
            request: tonic::Request<tonic::Streaming<::proto_flow::capture::Request>>,
        ) -> std::result::Result<tonic::Response<Self::CaptureStream>, tonic::Status>;
    }
    /// Captures is a very long lived RPC through which the Flow runtime and a
    /// connector cooperatively execute an unbounded number of transactions.
    ///
    /// The Pull workflow pulls streams of documents into capturing Flow
    /// collections. Streams are incremental and resume-able, with resumption
    /// semantics defined by the connector. The Flow Runtime uses a transactional
    /// recovery log to support this workflow, and the connector may persist arbitrary
    /// driver checkpoints into that log as part of the RPC lifecycle,
    /// to power its chosen resumption semantics.
    ///
    /// Pull tasks are split-able, and many concurrent invocations of the RPC
    /// may collectively capture from a source, where each task split has an
    /// identified range of keys it's responsible for. The meaning of a "key",
    /// and it's application within the remote store being captured from, is up
    /// to the connector. The connector might map partitions or shards into the keyspace,
    /// and from there to a covering task split. Or, it might map distinct files,
    /// or some other unit of scaling.
    ///
    /// RPC Lifecycle
    /// =============
    ///
    /// :Request.Open:
    ///    - The Flow runtime opens the pull stream.
    /// :Response.Opened:
    ///    - The connector responds with Opened.
    ///
    /// Request.Open and Request.Opened are sent only once, at the
    /// commencement of the stream. Thereafter the protocol loops:
    ///
    /// :Response.Captured:
    ///    - The connector tells the runtime of documents,
    ///      which are pending a future Checkpoint.
    ///    - If the connector sends multiple Documents messages without an
    ///      interleaving Checkpoint, the Flow runtime MUST commit
    ///      documents of all such messages in a single transaction.
    /// :Response.Checkpoint:
    ///    - The connector tells the runtime of a checkpoint: a watermark in the
    ///      captured documents stream which is eligble to be used as a
    ///      transaction commit boundary.
    ///    - Whether the checkpoint becomes a commit boundary is at the
    ///      discretion of the Flow runtime. It may combine multiple checkpoints
    ///      into a single transaction.
    /// :Request.Acknowledge:
    ///    - The Flow runtime tells the connector that its Checkpoint has committed.
    ///    - The runtime sends one ordered Acknowledge for each Checkpoint.
    #[derive(Debug)]
    pub struct ConnectorServer<T: Connector> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: Connector> ConnectorServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
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
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for ConnectorServer<T>
    where
        T: Connector,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/capture.Connector/Capture" => {
                    #[allow(non_camel_case_types)]
                    struct CaptureSvc<T: Connector>(pub Arc<T>);
                    impl<
                        T: Connector,
                    > tonic::server::StreamingService<::proto_flow::capture::Request>
                    for CaptureSvc<T> {
                        type Response = ::proto_flow::capture::Response;
                        type ResponseStream = T::CaptureStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                tonic::Streaming<::proto_flow::capture::Request>,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move { (*inner).capture(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CaptureSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
    impl<T: Connector> Clone for ConnectorServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    impl<T: Connector> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Connector> tonic::server::NamedService for ConnectorServer<T> {
        const NAME: &'static str = "capture.Connector";
    }
}
