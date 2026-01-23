use crate::{anyhow_to_status, new_channel, queue, session, slice};
use proto_flow::shuffle;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Service is the implementation of the Shuffle gRPC service trait.
#[derive(Clone)]
pub struct Service(Arc<ServiceImpl>);

/// ServiceImpl holds shared implementation state for the Shuffle gRPC service.
pub struct ServiceImpl {
    /// The endpoint of this service as seen by peers (e.g. "http://127.0.0.1:9876").
    pub(crate) peer_endpoint: String,
    /// Factory for building Gazette journal Clients.
    pub(crate) gazette_factory: GazetteClientFactory,
    /// Transport channels to dialed peers.
    pub(crate) channels: std::sync::Mutex<HashMap<String, tonic::transport::Channel>>,
    /// Shared state for coordinating Queue RPCs from multiple Slices into a single QueueActor.
    /// Keyed by (session_id, queue_member_index).
    pub(crate) queue_joins: std::sync::Mutex<HashMap<(u64, u32), queue::QueueJoin>>,
}

/// GazetteClientFactory is a boxed closure which builds and returns a Gazette
/// journal Client for reads of the Collection on behalf of a task Name.
pub type GazetteClientFactory =
    Box<dyn Fn(models::Collection, models::Name) -> gazette::journal::Client + Send + Sync>;

impl Service {
    pub fn new(peer_endpoint: String, gazette_factory: GazetteClientFactory) -> Self {
        Self(Arc::new(ServiceImpl {
            peer_endpoint,
            gazette_factory,
            channels: std::sync::Mutex::new(HashMap::new()),
            queue_joins: std::sync::Mutex::new(HashMap::new()),
        }))
    }

    /// Build a tonic Router containing the Shuffle service.
    pub fn build_tonic_server(self) -> tonic::transport::server::Router {
        tonic::transport::Server::builder().add_service(
            proto_grpc::shuffle::shuffle_server::ShuffleServer::new(self)
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX),
        )
    }

    pub fn spawn_session<R>(
        &self,
        request_rx: R,
    ) -> mpsc::Receiver<tonic::Result<shuffle::SessionResponse>>
    where
        R: futures::Stream<Item = tonic::Result<shuffle::SessionRequest>> + Send + Unpin + 'static,
    {
        let service = self.clone();
        let (response_tx, response_rx) = new_channel::<tonic::Result<shuffle::SessionResponse>>();
        let error_tx = response_tx.clone();

        tokio::spawn(async move {
            if let Err(e) = session::serve_session(service, request_rx, response_tx).await {
                let _ = error_tx.send(Err(anyhow_to_status(e))).await;
            }
        });
        response_rx
    }

    pub fn spawn_slice<R>(
        &self,
        request_rx: R,
    ) -> mpsc::Receiver<tonic::Result<shuffle::SliceResponse>>
    where
        R: futures::Stream<Item = tonic::Result<shuffle::SliceRequest>> + Send + Unpin + 'static,
    {
        let service = self.clone();
        let (response_tx, response_rx) = new_channel::<tonic::Result<shuffle::SliceResponse>>();
        let error_tx = response_tx.clone();

        tokio::spawn(async move {
            if let Err(e) = slice::serve_slice(service, request_rx, response_tx).await {
                let _ = error_tx.send(Err(anyhow_to_status(e))).await;
            }
        });
        response_rx
    }

    pub fn spawn_queue<R>(
        &self,
        request_rx: R,
    ) -> mpsc::Receiver<tonic::Result<shuffle::QueueResponse>>
    where
        R: futures::Stream<Item = tonic::Result<shuffle::QueueRequest>> + Send + Unpin + 'static,
    {
        let service = self.clone();
        let (response_tx, response_rx) = new_channel::<tonic::Result<shuffle::QueueResponse>>();
        let error_tx = response_tx.clone();

        tokio::spawn(async move {
            if let Err(e) = queue::serve_queue(service, request_rx, response_tx).await {
                let _ = error_tx.send(Err(anyhow_to_status(e))).await;
            }
        });
        response_rx
    }

    pub(crate) fn dial_channel(&self, endpoint: &str) -> tonic::Result<tonic::transport::Channel> {
        let mut guard = self.channels.lock().unwrap();

        if let Some(channel) = guard.get(endpoint) {
            return Ok(channel.clone());
        }

        let channel = tonic::transport::Endpoint::from_shared(endpoint.to_string())
            .map_err(|err| tonic::Status::invalid_argument(err.to_string()))?
            // Note this connect_timeout accounts only for TCP connection time and
            // does not apply to time required for TLS or HTTP/2 transport start,
            // which can block indefinitely if the server is bound but not listening.
            // Also, this timeout gets split between all of the IP addresses that endpoint
            // resolves to. Thus, if the endpoint resolves to 10 different addresses, then
            // the effective timeout per address is 60 / 10 = 6 seconds. This is why
            // the value is relatively high.
            .connect_timeout(std::time::Duration::from_secs(60))
            // HTTP/2 keep-alive sends a PING frame every interval to confirm the
            // health of the end-to-end HTTP/2 transport. The duration was selected
            // to be compatible with the default grpc server setting of 5 minutes
            // for `GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS`. If we
            // send pings more frequently than that, then the server may close the
            // connection unexpectedly.
            // See: https://github.com/grpc/grpc/blob/master/doc/keepalive.md
            .http2_keep_alive_interval(std::time::Duration::from_secs(301))
            .initial_connection_window_size(i32::MAX as u32)
            .connect_lazy();

        guard.insert(endpoint.to_string(), channel.clone());
        Ok(channel)
    }
}

impl std::ops::Deref for Service {
    type Target = ServiceImpl;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[tonic::async_trait]
impl proto_grpc::shuffle::shuffle_server::Shuffle for Service {
    type SessionStream =
        tokio_stream::wrappers::ReceiverStream<tonic::Result<shuffle::SessionResponse>>;
    type SliceStream =
        tokio_stream::wrappers::ReceiverStream<tonic::Result<shuffle::SliceResponse>>;
    type QueueStream =
        tokio_stream::wrappers::ReceiverStream<tonic::Result<shuffle::QueueResponse>>;

    async fn session(
        &self,
        request: tonic::Request<tonic::Streaming<shuffle::SessionRequest>>,
    ) -> tonic::Result<tonic::Response<Self::SessionStream>> {
        Ok(tonic::Response::new(
            tokio_stream::wrappers::ReceiverStream::new(self.spawn_session(request.into_inner())),
        ))
    }

    async fn slice(
        &self,
        request: tonic::Request<tonic::Streaming<shuffle::SliceRequest>>,
    ) -> tonic::Result<tonic::Response<Self::SliceStream>> {
        Ok(tonic::Response::new(
            tokio_stream::wrappers::ReceiverStream::new(self.spawn_slice(request.into_inner())),
        ))
    }

    async fn queue(
        &self,
        request: tonic::Request<tonic::Streaming<shuffle::QueueRequest>>,
    ) -> tonic::Result<tonic::Response<Self::QueueStream>> {
        Ok(tonic::Response::new(
            tokio_stream::wrappers::ReceiverStream::new(self.spawn_queue(request.into_inner())),
        ))
    }
}
