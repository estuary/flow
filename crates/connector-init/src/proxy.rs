use anyhow::Context;
use futures::{FutureExt, Stream, StreamExt};
use proto_flow::flow::{self, TaskNetworkProxyRequest, TaskNetworkProxyResponse};
use proto_grpc::flow::network_proxy_server::NetworkProxy;
use std::future::Future;
use std::task::Poll;
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpStream,
};
use tokio_util::io::ReaderStream;

pub struct ProxyHandler {
    proxy_to_host: String,
}

impl ProxyHandler {
    pub fn new(proxy_to_host: impl Into<String>) -> ProxyHandler {
        ProxyHandler {
            proxy_to_host: proxy_to_host.into(),
        }
    }
}

#[async_trait::async_trait]
impl NetworkProxy for ProxyHandler {
    type ProxyStream = ProxyResponseStream;

    async fn proxy(
        &self,
        request: tonic::Request<tonic::Streaming<TaskNetworkProxyRequest>>,
    ) -> Result<tonic::Response<Self::ProxyStream>, tonic::Status> {
        let mut recv_from_client = request.into_inner();
        let Some(msg) = recv_from_client.message().await? else {
            return Err(tonic::Status::cancelled("did not receive an open message"));
        };
        if !msg.data.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "data must not be sent along with Open message",
            ));
        }
        let Some(open) = msg.open else {
            return Err(tonic::Status::invalid_argument("expected first message to be Open"));
        };
        tracing::debug!(client_addr = %open.client_addr, requested_port = %open.target_port, "processing new proxy request");
        if open.target_port == 0 {
            return Err(tonic::Status::invalid_argument("target_port cannot be 0"));
        }

        let target_addr = format!("{}:{}", self.proxy_to_host, open.target_port);
        let target_stream = TcpStream::connect(target_addr)
            .await
            .context("failed to connect to target port")
            .map_err(|e| tonic::Status::from_error(e.into()))?;
        let (target_reader, target_writer) = target_stream.into_split();
        tracing::debug!(client_addr = %open.client_addr, target_port = %open.target_port, "connected to target port");

        // Spawn the background task that will copy data from the request stream to the connector.
        // The write half of the tcp stream will be closed when this task completes.
        let write_task =
            tokio::task::spawn(
                async move { copy_inbound_data(recv_from_client, target_writer).await },
            );

        let resp = flow::task_network_proxy_response::OpenResponse {
            status: flow::task_network_proxy_response::Status::Ok as i32,
            header: None,
        };
        let io = Io {
            n_bytes: 0,
            reader: ReaderStream::new(target_reader),
            write_task,
        };

        Ok(tonic::Response::new(ProxyResponseStream {
            open: Some(resp),
            io: Some(io),
        }))
    }
}

async fn copy_inbound_data(
    mut requests: tonic::Streaming<TaskNetworkProxyRequest>,
    mut target: OwnedWriteHalf,
) -> Result<usize, anyhow::Error> {
    use tokio::io::AsyncWriteExt;

    let mut n = 0;
    while let Some(req) = requests.message().await? {
        n += req.data.len();
        target.write_all(&req.data).await?;
    }
    tracing::debug!(
        n_bytes = n,
        "finished copying from proxy client to connector"
    );
    Ok(n)
}

/// Encapsulates the network i/o of a proxy rpc that has had a successful handshake.
struct Io {
    n_bytes: u64,
    reader: ReaderStream<OwnedReadHalf>,
    write_task: tokio::task::JoinHandle<Result<usize, anyhow::Error>>,
}

impl Io {
    fn poll_next(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<TaskNetworkProxyResponse, tonic::Status>>> {
        let bytes = futures::ready!(self.reader.next().poll_unpin(cx));
        if let Some(read_result) = bytes {
            match read_result {
                Ok(data) => {
                    self.n_bytes += data.len() as u64;
                    Poll::Ready(Some(Ok(TaskNetworkProxyResponse {
                        open_response: None,
                        data: data.into(),
                    })))
                }
                Err(err) => {
                    let err = anyhow::Error::from(err);
                    self.log_done(cx, Some(&err));
                    Poll::Ready(Some(Err(tonic::Status::from_error(err.into()))))
                }
            }
        } else {
            self.log_done(cx, None);
            Poll::Ready(None)
        }
    }

    fn log_done(&mut self, cx: &mut std::task::Context<'_>, r_err: Option<&anyhow::Error>) {
        let r_bytes = self.n_bytes;
        // In the happy path, the write_task should have already completed
        // before reaching this point because the client would have closed its
        // side of the connection first. If that didn't happen for some reason,
        // we want to be a little more noisy.
        let pinned = std::pin::Pin::new(&mut self.write_task);
        let write_result = pinned
            .poll(cx)
            .map(|result| result.map_err(anyhow::Error::from).and_then(|r| r));
        match (r_err, write_result) {
            (None, Poll::Ready(Ok(wbytes))) => tracing::debug!(
                client_to_connector = wbytes,
                connector_to_client = r_bytes,
                "proxy connection closed normally"
            ),

            // :( cases
            (Some(r_err), Poll::Ready(Ok(w_bytes))) => tracing::warn!(
                outbound_bytes = r_bytes,
                client_to_connector = w_bytes,
                error = %r_err,
                "proxy connection closed with error reading from connector"
            ),
            (None, Poll::Ready(Err(w_err))) => tracing::warn!(
                connector_to_client = r_bytes,
                error = %w_err,
                "proxy connection closed with client-to-connector error"
            ),
            (Some(r_err), Poll::Ready(Err(w_err))) => tracing::warn!(
                connector_to_client = r_bytes,
                connector_to_client_error = %r_err,
                client_to_connector_error = %w_err,
                "proxy connection closed with error"
            ),
            (Some(r_err), Poll::Pending) => tracing::warn!(
                connector_to_client = r_bytes,
                client_to_connector_error = %r_err,
                "proxy connection read half closed with error while write half is still unfinished"
            ),
            (None, Poll::Pending) => tracing::warn!(
                connector_to_client = r_bytes,
                "proxy connection read half closed without error while write half is still pending"
            ),
        };
    }
}

/// The type returned from the `Proxy` RPC. This always be initialized with an OpenResponse,
/// which may indicate either success or failure. If successful, then `Io` will also be present.
pub struct ProxyResponseStream {
    open: Option<flow::task_network_proxy_response::OpenResponse>,
    /// io will be None if the open response has a non-OK status, and Some
    /// if we are actually proxying data.
    io: Option<Io>,
}

impl Stream for ProxyResponseStream {
    type Item = Result<TaskNetworkProxyResponse, tonic::Status>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(opened) = this.open.take() {
            return Poll::Ready(Some(Ok(TaskNetworkProxyResponse {
                open_response: Some(opened),
                data: Vec::new(),
            })));
        }

        if let Some(io) = this.io.as_mut() {
            io.poll_next(cx)
        } else {
            Poll::Ready(None)
        }
    }
}
