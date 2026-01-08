use anyhow::Context;
use futures::{StreamExt, TryStreamExt};
use proto_flow::shuffle::{
    QueueRequest, QueueResponse, SessionRequest, SessionResponse, SliceRequest, SliceResponse,
};
use std::collections::HashMap;
use std::sync::Arc;

mod queue;
mod session;
mod slice;

/// Service is the implementation of the Shuffle gRPC service trait.
#[derive(Clone)]
pub struct Service(Arc<ServiceImpl>);

/// ServiceImpl holds shared implementation state for the Shuffle gRPC service.
struct ServiceImpl {
    /// The endpoint of this service as seen by peers (e.g. "http://127.0.0.1:9876").
    peer_endpoint: String,
    /// Transport channels to dialed peers.
    channels: std::sync::Mutex<HashMap<String, tonic::transport::Channel>>,
    /// Shared state for coordinating Queue RPCs from multiple Slices.
    /// Keyed by (session_id, queue_member_index).
    queue_joins: tokio::sync::Mutex<HashMap<(u64, u32), queue::QueueJoin>>,
}

impl Service {
    pub fn new(peer_endpoint: String) -> Self {
        Self(Arc::new(ServiceImpl {
            peer_endpoint,
            channels: std::sync::Mutex::new(HashMap::new()),
            queue_joins: tokio::sync::Mutex::new(HashMap::new()),
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

    pub fn spawn_session(
        &self,
        request_rx: impl futures::Stream<Item = anyhow::Result<SessionRequest>> + Send + Unpin + 'static,
    ) -> tokio::sync::mpsc::Receiver<tonic::Result<SessionResponse>> {
        let (response_tx, response_rx) = new_channel::<tonic::Result<SessionResponse>>();

        let service = self.clone();
        let error_tx = response_tx.clone();

        tokio::spawn(async move {
            if let Err(e) = session::serve_session(service, request_rx, response_tx).await {
                let _ = error_tx.send(Err(anyhow_to_status(e))).await;
            }
        });

        response_rx
    }

    pub fn spawn_slice(
        &self,
        request_rx: impl futures::Stream<Item = anyhow::Result<SliceRequest>> + Send + Unpin + 'static,
    ) -> tokio::sync::mpsc::Receiver<tonic::Result<SliceResponse>> {
        let (response_tx, response_rx) = new_channel::<tonic::Result<SliceResponse>>();

        let service = self.clone();
        let error_tx = response_tx.clone();

        tokio::spawn(async move {
            if let Err(e) = slice::serve_slice(service, request_rx, response_tx).await {
                let _ = error_tx.send(Err(anyhow_to_status(e))).await;
            }
        });

        response_rx
    }

    pub fn spawn_queue(
        &self,
        request_rx: impl futures::Stream<Item = anyhow::Result<QueueRequest>> + Send + Unpin + 'static,
    ) -> tokio::sync::mpsc::Receiver<tonic::Result<QueueResponse>> {
        let (response_tx, response_rx) = new_channel::<tonic::Result<QueueResponse>>();

        let service = self.clone();
        let error_tx = response_tx.clone();

        tokio::spawn(async move {
            if let Err(e) = queue::serve_queue(service, request_rx, response_tx).await {
                let _ = error_tx.send(Err(anyhow_to_status(e))).await;
            }
        });

        response_rx
    }

    fn dial_channel(&self, endpoint: &str) -> anyhow::Result<tonic::transport::Channel> {
        let mut guard = self.0.channels.lock().unwrap();

        if let Some(channel) = guard.get(endpoint) {
            return Ok(channel.clone());
        }

        let channel = tonic::transport::Endpoint::from_shared(endpoint.to_string())?
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

    pub async fn dial_slice(
        &self,
        address: &str,
        request_rx: impl futures::Stream<Item = SliceRequest> + Send + Unpin + 'static,
    ) -> anyhow::Result<futures::stream::BoxStream<'static, anyhow::Result<SliceResponse>>> {
        let response_rx = if address == self.0.peer_endpoint {
            // In-process: spawn the handler directly.
            tokio_stream::wrappers::ReceiverStream::new(self.spawn_slice(request_rx.map(Ok)))
                .map_err(status_to_anyhow)
                .boxed()
        } else {
            // Remote: start RPC via tonic.
            let channel = self.dial_channel(address)?;
            let mut client = proto_grpc::shuffle::shuffle_client::ShuffleClient::new(channel);

            let tonic_rx = client
                .slice(request_rx)
                .await
                .context("starting Slice RPC")?
                .into_inner();

            tonic_rx.map_err(status_to_anyhow).boxed()
        };
        Ok(response_rx)
    }

    pub async fn dial_queue(
        &self,
        endpoint: &str,
        request_rx: impl futures::Stream<Item = QueueRequest> + Send + Unpin + 'static,
    ) -> anyhow::Result<futures::stream::BoxStream<'static, anyhow::Result<QueueResponse>>> {
        let response_rx = if endpoint == self.0.peer_endpoint {
            // In-process: spawn the handler directly.
            tokio_stream::wrappers::ReceiverStream::new(self.spawn_queue(request_rx.map(Ok)))
                .map_err(status_to_anyhow)
                .boxed()
        } else {
            // Remote: start RPC via tonic.
            let channel = self.dial_channel(endpoint)?;
            let mut client = proto_grpc::shuffle::shuffle_client::ShuffleClient::new(channel);

            let tonic_rx = client
                .queue(request_rx)
                .await
                .context("starting Queue RPC")?
                .into_inner();

            tonic_rx.map_err(status_to_anyhow).boxed()
        };
        Ok(response_rx)
    }
}

#[tonic::async_trait]
impl proto_grpc::shuffle::shuffle_server::Shuffle for Service {
    type SessionStream = tokio_stream::wrappers::ReceiverStream<tonic::Result<SessionResponse>>;
    type SliceStream = tokio_stream::wrappers::ReceiverStream<tonic::Result<SliceResponse>>;
    type QueueStream = tokio_stream::wrappers::ReceiverStream<tonic::Result<QueueResponse>>;

    async fn session(
        &self,
        request: tonic::Request<tonic::Streaming<SessionRequest>>,
    ) -> tonic::Result<tonic::Response<Self::SessionStream>> {
        Ok(tonic::Response::new(
            tokio_stream::wrappers::ReceiverStream::new(
                self.spawn_session(request.into_inner().map_err(status_to_anyhow)),
            ),
        ))
    }

    async fn slice(
        &self,
        request: tonic::Request<tonic::Streaming<SliceRequest>>,
    ) -> tonic::Result<tonic::Response<Self::SliceStream>> {
        Ok(tonic::Response::new(
            tokio_stream::wrappers::ReceiverStream::new(
                self.spawn_slice(request.into_inner().map_err(status_to_anyhow)),
            ),
        ))
    }

    async fn queue(
        &self,
        request: tonic::Request<tonic::Streaming<QueueRequest>>,
    ) -> tonic::Result<tonic::Response<Self::QueueStream>> {
        Ok(tonic::Response::new(
            tokio_stream::wrappers::ReceiverStream::new(
                self.spawn_queue(request.into_inner().map_err(status_to_anyhow)),
            ),
        ))
    }
}

fn new_channel<T>() -> (tokio::sync::mpsc::Sender<T>, tokio::sync::mpsc::Receiver<T>) {
    tokio::sync::mpsc::channel::<T>(32)
}

// Map an anyhow::Error into a tonic::Status.
fn anyhow_to_status(err: anyhow::Error) -> tonic::Status {
    match err.downcast::<tonic::Status>() {
        Ok(status) => status,
        Err(err) => tonic::Status::internal(format!("{err:?}")),
    }
}

// Map a tonic::Status into an anyhow::Error.
fn status_to_anyhow(status: tonic::Status) -> anyhow::Error {
    match status.code() {
        tonic::Code::Internal => anyhow::anyhow!(status.message().to_owned()),
        _ => anyhow::Error::new(status),
    }
}
