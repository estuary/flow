use crate::proto;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Service is the implementation of the Leader gRPC service trait.
#[derive(Clone)]
pub struct Service(Arc<ServiceImpl>);

/// ServiceImpl holds shared implementation state for the Leader gRPC service.
pub struct ServiceImpl {
    /// In-progress Derive session Joins, keyed by task name.
    pub(crate) derive_joins: std::sync::Mutex<HashMap<String, super::PendingJoin<proto::Derive>>>,
    /// In-progress Materialize session Joins, keyed by task name.
    pub(crate) materialize_joins:
        std::sync::Mutex<HashMap<String, super::PendingJoin<proto::Materialize>>>,
    /// Service used by leader sessions to open shuffle Sessions.
    pub(crate) shuffle_service: shuffle::Service,
    /// Factory for building Gazette clients for publish operations.
    pub(crate) publisher_factory: gazette::journal::ClientFactory,
    /// Process-wide HTTP client used by the actor to deliver trigger webhooks.
    pub(crate) http_client: reqwest::Client,
    /// Registry of in-flight Leader session handlers, for the admin surface.
    pub(crate) registry: service_kit::Registry,
    /// When true, disarm AuthN+AuthZ enforcement (trusted local contexts only).
    pub(crate) disarm_auth: bool,
}

impl Service {
    pub fn new(
        shuffle_service: shuffle::Service,
        publisher_factory: gazette::journal::ClientFactory,
        registry: service_kit::Registry,
        disarm_auth: bool,
    ) -> Self {
        Self(Arc::new(ServiceImpl {
            derive_joins: std::sync::Mutex::new(HashMap::new()),
            materialize_joins: std::sync::Mutex::new(HashMap::new()),
            shuffle_service,
            publisher_factory,
            http_client: reqwest::Client::new(),
            registry,
            disarm_auth,
        }))
    }

    /// Wrap this service in its typed tonic server, for composition
    /// with sibling services on a `tonic::transport::Server::builder()`.
    pub fn into_tonic_service(self) -> proto_grpc::runtime::leader_server::LeaderServer<Self> {
        proto_grpc::runtime::leader_server::LeaderServer::new(self)
            .max_decoding_message_size(crate::MAX_MESSAGE_SIZE)
            .max_encoding_message_size(usize::MAX)
    }

    pub fn spawn_derive<R>(
        &self,
        authz: proto_grpc::Authorizer,
        request_rx: R,
    ) -> mpsc::UnboundedReceiver<tonic::Result<proto::Derive>>
    where
        R: futures::Stream<Item = tonic::Result<proto::Derive>> + Send + Unpin + 'static,
    {
        let service = self.clone();
        let (response_tx, response_rx) = mpsc::unbounded_channel::<tonic::Result<proto::Derive>>();
        let error_tx = response_tx.clone();

        tokio::spawn(async move {
            if let Err(e) = super::derive::serve(service, authz, request_rx, response_tx).await {
                let _ = error_tx.send(Err(crate::anyhow_to_status(e)));
            }
        });
        response_rx
    }

    pub fn spawn_materialize<R>(
        &self,
        authz: proto_grpc::Authorizer,
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
            if let Err(e) = super::materialize::serve(service, authz, request_rx, response_tx).await
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
        mut request: tonic::Request<tonic::Streaming<proto::Derive>>,
    ) -> tonic::Result<tonic::Response<Self::DeriveStream>> {
        let authz = proto_grpc::Authorizer::from_request(&mut request, self.disarm_auth)?;
        Ok(tonic::Response::new(
            tokio_stream::wrappers::UnboundedReceiverStream::new(
                self.spawn_derive(authz, request.into_inner()),
            ),
        ))
    }

    async fn materialize(
        &self,
        mut request: tonic::Request<tonic::Streaming<proto::Materialize>>,
    ) -> tonic::Result<tonic::Response<Self::MaterializeStream>> {
        let authz = proto_grpc::Authorizer::from_request(&mut request, self.disarm_auth)?;
        Ok(tonic::Response::new(
            tokio_stream::wrappers::UnboundedReceiverStream::new(
                self.spawn_materialize(authz, request.into_inner()),
            ),
        ))
    }
}
