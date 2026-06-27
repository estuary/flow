use crate::proto;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Service is the implementation of the Leader gRPC service trait.
pub struct Service<
    Shuffle: crate::ShuffleSessionFactory,
    Pub: crate::PublisherFactory,
    Obs: crate::ObserverFactory,
>(Arc<ServiceImpl<Shuffle, Pub, Obs>>);

impl<
    Shuffle: crate::ShuffleSessionFactory,
    Pub: crate::PublisherFactory,
    Obs: crate::ObserverFactory,
> Clone for Service<Shuffle, Pub, Obs>
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

/// ServiceImpl holds shared implementation state for the Leader gRPC service.
pub struct ServiceImpl<
    Shuffle: crate::ShuffleSessionFactory,
    Pub: crate::PublisherFactory,
    Obs: crate::ObserverFactory,
> {
    /// In-progress Derive session Joins, keyed by task name.
    pub(crate) derive_joins: std::sync::Mutex<HashMap<String, super::PendingJoin<proto::Derive>>>,
    /// In-progress Materialize session Joins, keyed by task name.
    pub(crate) materialize_joins:
        std::sync::Mutex<HashMap<String, super::PendingJoin<proto::Materialize>>>,
    /// Factory used by leader sessions to obtain checkpoint Frontiers. The
    /// standard factory reads journals; fixture previews supply their own.
    pub(crate) shuffle_factory: Shuffle,
    /// Factory used by leader sessions to publish stats and ACK intents. The
    /// standard factory performs Gazette journal IO; previews supply their own.
    pub(crate) publisher_factory: Pub,
    /// Factory used by leader sessions to open an [`Observer`](crate::Observer)
    /// for runtime events (connector-state persists, Apply actions). Production
    /// installs a no-op; previews render observations as preview lines.
    pub(crate) observer_factory: Obs,
    /// Process-wide HTTP client used by the actor to deliver trigger webhooks.
    pub(crate) http_client: reqwest::Client,
    /// Registry of in-flight Leader session handlers, for the admin surface.
    pub(crate) registry: service_kit::Registry,
    /// When true, disarm AuthN+AuthZ enforcement (trusted local contexts only).
    pub(crate) disarm_auth: bool,
}

impl<
    Shuffle: crate::ShuffleSessionFactory,
    Pub: crate::PublisherFactory,
    Obs: crate::ObserverFactory,
> Service<Shuffle, Pub, Obs>
{
    pub fn new(
        shuffle_factory: Shuffle,
        publisher_factory: Pub,
        observer_factory: Obs,
        registry: service_kit::Registry,
        disarm_auth: bool,
    ) -> Self {
        Self(Arc::new(ServiceImpl {
            derive_joins: std::sync::Mutex::new(HashMap::new()),
            materialize_joins: std::sync::Mutex::new(HashMap::new()),
            shuffle_factory,
            publisher_factory,
            observer_factory,
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

impl<
    Shuffle: crate::ShuffleSessionFactory,
    Pub: crate::PublisherFactory,
    Obs: crate::ObserverFactory,
> std::ops::Deref for Service<Shuffle, Pub, Obs>
{
    type Target = ServiceImpl<Shuffle, Pub, Obs>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[tonic::async_trait]
impl<
    Shuffle: crate::ShuffleSessionFactory,
    Pub: crate::PublisherFactory,
    Obs: crate::ObserverFactory,
> proto_grpc::runtime::leader_server::Leader for Service<Shuffle, Pub, Obs>
{
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
