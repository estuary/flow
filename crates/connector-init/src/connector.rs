use super::{codec::Codec, rpc, serve};
use proto_flow::connector::{Request, Response};

/// Serves exactly one `connector.Connector` stream, and rejects any other.
/// `done` is notified once the stream is served and its connector has exited.
pub struct Proxy {
    pub entrypoint: Vec<String>,
    pub codec: Codec,
    pub spec_alone: bool,
    pub taken: std::sync::atomic::AtomicBool,
    pub done: std::sync::Arc<tokio::sync::Notify>,
}

#[tonic::async_trait]
impl proto_grpc::connector::connector_server::Connector for Proxy {
    type ConnectorStream = futures::stream::BoxStream<'static, tonic::Result<Response>>;

    async fn connector(
        &self,
        request: tonic::Request<tonic::Streaming<Request>>,
    ) -> tonic::Result<tonic::Response<Self::ConnectorStream>> {
        if self.taken.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return Err(tonic::Status::failed_precondition(
                "flow-connector-init serves only one Connector stream",
            ));
        }
        crate::inc(&crate::GRPC_SERVER_STARTED_TOTAL);
        let guard = crate::IncOnDrop(&crate::GRPC_SERVER_HANDLED_TOTAL);

        let (response_tx, response_rx) = tokio::sync::mpsc::channel(proto_grpc::CHANNEL_BUFFER);
        let entrypoint = self.entrypoint.clone();
        let (codec, spec_alone, done) = (self.codec, self.spec_alone, self.done.clone());

        tokio::spawn(async move {
            serve::serve(
                || rpc::new_command(&entrypoint),
                codec,
                spec_alone,
                proto_grpc::chunk::join(request.into_inner()),
                &response_tx,
            )
            .await;

            std::mem::drop(guard);
            done.notify_one();
        });

        Ok(tonic::Response::new(proto_grpc::chunk::split_responses(
            tokio_stream::wrappers::ReceiverStream::new(response_rx),
        )))
    }
}
