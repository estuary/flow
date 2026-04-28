use crate::proto;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Service is the implementation of the Leader gRPC service trait.
#[derive(Clone)]
pub struct Service(Arc<ServiceImpl>);

/// ServiceImpl holds shared implementation state for the Leader gRPC service.
pub struct ServiceImpl {
    /// In-progress Materialize session Joins, keyed by task name.
    pub(crate) materialize_joins:
        std::sync::Mutex<HashMap<String, super::PendingJoin<proto::Materialize>>>,
    /// Service used by leader sessions to open shuffle Sessions.
    pub(crate) shuffle_service: shuffle::Service,
    /// Factory for building Gazette clients for publish operations.
    pub(crate) publisher_factory: gazette::journal::ClientFactory,
    /// Process-wide HTTP client used by the actor to deliver trigger webhooks.
    pub(crate) http_client: reqwest::Client,
}

impl Service {
    pub fn new(
        shuffle_service: shuffle::Service,
        publisher_factory: gazette::journal::ClientFactory,
    ) -> Self {
        Self(Arc::new(ServiceImpl {
            materialize_joins: std::sync::Mutex::new(HashMap::new()),
            shuffle_service,
            publisher_factory,
            http_client: reqwest::Client::new(),
        }))
    }

    /// Wrap this service in its typed tonic server, applying the
    /// max-message-size overrides so it can be composed with sibling
    /// services on a shared `tonic::transport::Server::builder()`.
    pub fn into_tonic_service(self) -> proto_grpc::runtime::leader_server::LeaderServer<Self> {
        proto_grpc::runtime::leader_server::LeaderServer::new(self)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX)
    }

    pub fn spawn_derive<R>(
        &self,
        request_rx: R,
    ) -> mpsc::UnboundedReceiver<tonic::Result<proto::Derive>>
    where
        R: futures::Stream<Item = tonic::Result<proto::Derive>> + Send + Unpin + 'static,
    {
        let service = self.clone();
        let (response_tx, response_rx) = mpsc::unbounded_channel::<tonic::Result<proto::Derive>>();
        let error_tx = response_tx.clone();

        tokio::spawn(async move {
            if let Err(e) = serve_derive(service, request_rx, response_tx).await {
                let _ = error_tx.send(Err(crate::anyhow_to_status(e)));
            }
        });
        response_rx
    }

    pub fn spawn_materialize<R>(
        &self,
        request_rx: R,
    ) -> mpsc::UnboundedReceiver<tonic::Result<proto::Materialize>>
    where
        R: futures::Stream<Item = tonic::Result<proto::Materialize>> + Send + Unpin + 'static,
    {
        let service = self.clone();
        let (response_tx, response_rx) =
            mpsc::unbounded_channel::<tonic::Result<proto::Materialize>>();
        let error_tx = response_tx.clone();

        tokio::spawn(async move {
            if let Err(e) =
                crate::materialize::leader::serve(service, request_rx, response_tx).await
            {
                let _ = error_tx.send(Err(crate::anyhow_to_status(e)));
            }
        });
        response_rx
    }
}

impl std::ops::Deref for Service {
    type Target = ServiceImpl;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[tonic::async_trait]
impl proto_grpc::runtime::leader_server::Leader for Service {
    type DeriveStream =
        tokio_stream::wrappers::UnboundedReceiverStream<tonic::Result<proto::Derive>>;
    type MaterializeStream =
        tokio_stream::wrappers::UnboundedReceiverStream<tonic::Result<proto::Materialize>>;

    async fn derive(
        &self,
        request: tonic::Request<tonic::Streaming<proto::Derive>>,
    ) -> tonic::Result<tonic::Response<Self::DeriveStream>> {
        Ok(tonic::Response::new(
            tokio_stream::wrappers::UnboundedReceiverStream::new(
                self.spawn_derive(request.into_inner()),
            ),
        ))
    }

    async fn materialize(
        &self,
        request: tonic::Request<tonic::Streaming<proto::Materialize>>,
    ) -> tonic::Result<tonic::Response<Self::MaterializeStream>> {
        Ok(tonic::Response::new(
            tokio_stream::wrappers::UnboundedReceiverStream::new(
                self.spawn_materialize(request.into_inner()),
            ),
        ))
    }
}

async fn serve_derive<R>(
    _service: Service,
    _request_rx: R,
    _response_tx: mpsc::UnboundedSender<tonic::Result<proto::Derive>>,
) -> anyhow::Result<()>
where
    R: futures::Stream<Item = tonic::Result<proto::Derive>> + Send + Unpin + 'static,
{
    anyhow::bail!("Leader Derive RPC is not yet implemented")
}
