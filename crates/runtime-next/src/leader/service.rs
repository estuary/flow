use crate::proto;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Service is the implementation of the Leader gRPC service trait.
pub struct Service<
    S: crate::ShuffleSessionFactory,
    P: crate::PublisherFactory,
    L: crate::LoggerFactory,
>(Arc<ServiceImpl<S, P, L>>);

impl<S: crate::ShuffleSessionFactory, P: crate::PublisherFactory, L: crate::LoggerFactory> Clone
    for Service<S, P, L>
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

/// ServiceImpl holds shared implementation state for the Leader gRPC service.
pub struct ServiceImpl<
    S: crate::ShuffleSessionFactory,
    P: crate::PublisherFactory,
    L: crate::LoggerFactory,
> {
    /// In-progress Derive session Joins, keyed by task name.
    pub(crate) derive_joins: std::sync::Mutex<HashMap<String, super::PendingJoin<proto::Derive>>>,
    /// In-progress Materialize session Joins, keyed by task name.
    pub(crate) materialize_joins:
        std::sync::Mutex<HashMap<String, super::PendingJoin<proto::Materialize>>>,
    /// Factory used by leader sessions to open a [`ShuffleSession`](crate::ShuffleSession).
    pub(crate) shuffle_factory: S,
    /// Factory used by leader sessions to open a [`Publisher`](crate::Publisher) of stats and ACK intents.
    pub(crate) publisher_factory: P,
    /// Factory used by leader sessions to open a [`Logger`](crate::Logger)
    /// of task-centric state changes and events.
    pub(crate) logger_factory: L,
    /// Process-wide HTTP client used by the actor to deliver trigger webhooks.
    pub(crate) http_client: reqwest::Client,
    /// Registry of in-flight Leader session handlers, for the admin surface.
    pub(crate) registry: service_kit::Registry,
    /// When true, disarm AuthN+AuthZ enforcement (trusted local contexts only).
    pub(crate) disarm_auth: bool,
    /// Sync-now handles of live Materialize leader sessions, keyed by task
    /// name: the delivery points for TaskControl.SyncNow requests.
    pub(crate) sync_now_handles: std::sync::Mutex<HashMap<String, SyncNowHandle>>,
}

/// A live Materialize session's sync-now delivery handle.
pub(crate) struct SyncNowHandle {
    /// Shard-zero ID of the session: the concrete scope that a TaskControl
    /// caller's claims must authorize.
    shard_zero: String,
    /// SyncNow delivery channel into the session's Actor.
    sync_now_tx: mpsc::UnboundedSender<super::materialize::SyncNow>,
}

impl<S: crate::ShuffleSessionFactory, P: crate::PublisherFactory, L: crate::LoggerFactory>
    Service<S, P, L>
{
    pub fn new(
        shuffle_factory: S,
        publisher_factory: P,
        logger_factory: L,
        registry: service_kit::Registry,
        disarm_auth: bool,
    ) -> Self {
        Self(Arc::new(ServiceImpl {
            derive_joins: std::sync::Mutex::new(HashMap::new()),
            materialize_joins: std::sync::Mutex::new(HashMap::new()),
            shuffle_factory,
            publisher_factory,
            logger_factory,
            http_client: reqwest::Client::new(),
            registry,
            disarm_auth,
            sync_now_handles: std::sync::Mutex::new(HashMap::new()),
        }))
    }

    /// Wrap this service in its typed tonic server, for composition
    /// with sibling services on a `tonic::transport::Server::builder()`.
    pub fn into_tonic_service(self) -> proto_grpc::runtime::leader_server::LeaderServer<Self> {
        proto_grpc::runtime::leader_server::LeaderServer::new(self)
            .max_decoding_message_size(crate::MAX_MESSAGE_SIZE)
            .max_encoding_message_size(usize::MAX)
    }

    /// Wrap this service in the TaskControl tonic server. It's distinct from
    /// the Leader service because its AuthN floor is gazette READ over the
    /// task's shards (held by any `/authorize/user/task` caller), not LEAD.
    pub fn into_task_control_service(
        self,
    ) -> proto_grpc::runtime::task_control_server::TaskControlServer<Self> {
        proto_grpc::runtime::task_control_server::TaskControlServer::new(self)
            .max_decoding_message_size(crate::MAX_MESSAGE_SIZE)
            .max_encoding_message_size(usize::MAX)
    }

    /// Register a live Materialize session's sync-now handle, replacing any
    /// prior registration for the task. The returned guard un-registers it
    /// when dropped — tie the guard to the session's serve scope.
    pub(crate) fn register_sync_now_handle(
        &self,
        task_name: &str,
        shard_zero: String,
        sync_now_tx: mpsc::UnboundedSender<super::materialize::SyncNow>,
    ) -> SyncNowGuard<S, P, L> {
        self.sync_now_handles.lock().unwrap().insert(
            task_name.to_string(),
            SyncNowHandle {
                shard_zero,
                sync_now_tx: sync_now_tx.clone(),
            },
        );
        SyncNowGuard {
            service: self.clone(),
            task_name: task_name.to_string(),
            sync_now_tx,
        }
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

impl<S: crate::ShuffleSessionFactory, P: crate::PublisherFactory, L: crate::LoggerFactory>
    std::ops::Deref for Service<S, P, L>
{
    type Target = ServiceImpl<S, P, L>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[tonic::async_trait]
impl<S: crate::ShuffleSessionFactory, P: crate::PublisherFactory, L: crate::LoggerFactory>
    proto_grpc::runtime::leader_server::Leader for Service<S, P, L>
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

/// Un-registers a Materialize session's [`SyncNowHandle`] when dropped.
pub(crate) struct SyncNowGuard<
    S: crate::ShuffleSessionFactory,
    P: crate::PublisherFactory,
    L: crate::LoggerFactory,
> {
    service: Service<S, P, L>,
    task_name: String,
    sync_now_tx: mpsc::UnboundedSender<super::materialize::SyncNow>,
}

impl<S: crate::ShuffleSessionFactory, P: crate::PublisherFactory, L: crate::LoggerFactory> Drop
    for SyncNowGuard<S, P, L>
{
    fn drop(&mut self) {
        let mut guard = self.service.sync_now_handles.lock().unwrap();
        // Remove only our own registration: a replacement session for the
        // same task may have re-registered while this one wound down.
        if guard
            .get(&self.task_name)
            .is_some_and(|handle| handle.sync_now_tx.same_channel(&self.sync_now_tx))
        {
            guard.remove(&self.task_name);
        }
    }
}

#[tonic::async_trait]
impl<S: crate::ShuffleSessionFactory, P: crate::PublisherFactory, L: crate::LoggerFactory>
    proto_grpc::runtime::task_control_server::TaskControl for Service<S, P, L>
{
    type SyncNowStream =
        tokio_stream::wrappers::UnboundedReceiverStream<tonic::Result<proto::SyncNowResponse>>;

    async fn sync_now(
        &self,
        mut request: tonic::Request<proto::SyncNowRequest>,
    ) -> tonic::Result<tonic::Response<Self::SyncNowStream>> {
        let authz = proto_grpc::Authorizer::from_request(&mut request, self.disarm_auth)?;
        let proto::SyncNowRequest { task_name } = request.into_inner();

        if task_name.is_empty() {
            return Err(tonic::Status::invalid_argument("task_name is required"));
        }

        // A live Materialize session? Authorize the caller against its
        // concrete shard-zero ID and deliver the request; the Actor drives
        // the response stream from there.
        let handle = {
            let guard = self.sync_now_handles.lock().unwrap();
            guard
                .get(&task_name)
                .map(|handle| (handle.shard_zero.clone(), handle.sync_now_tx.clone()))
        };
        let Some((shard_zero, sync_now_tx)) = handle else {
            // Not running here, not on the V2 runtime, or not a
            // materialization at all: only the reactor front door can tell
            // those apart, since only it has the shard keyspace. It answers
            // for captures and derivations without dialing us.
            return Err(not_found(&task_name));
        };
        let _authorized = authz.authorize_id(&shard_zero)?;

        let (reply_tx, reply_rx) = mpsc::unbounded_channel();
        if sync_now_tx
            .send(super::materialize::SyncNow { reply_tx })
            .is_ok()
        {
            return Ok(tonic::Response::new(
                tokio_stream::wrappers::UnboundedReceiverStream::new(reply_rx),
            ));
        }
        // The session exited between lookup and delivery.
        Err(tonic::Status::unavailable(format!(
            "leader session of task {task_name} is winding down; retry"
        )))
    }
}

fn not_found(task_name: &str) -> tonic::Status {
    tonic::Status::not_found(format!(
        "task {task_name} has no live leader session here (it may not be running, or may not be on the V2 runtime)"
    ))
}
