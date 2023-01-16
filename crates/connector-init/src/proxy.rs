use anyhow::Context;
use bytes::Bytes;
use futures::{FutureExt, Stream, StreamExt, TryFutureExt};
use proto_flow::flow::{self, TaskNetworkProxyRequest, TaskNetworkProxyResponse};
use proto_grpc::flow::network_proxy_server::NetworkProxy;
use std::collections::BTreeMap;
use std::future::Future;
use std::task::Poll;
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpStream,
};
use tokio_util::io::ReaderStream;
// use tokio::stream::StreamExt;
use tokio::sync::mpsc::{Receiver, Sender};

pub struct ProxyHandler {
    shard_id: String,
    proxy_to_host: String,
    ports: BTreeMap<String, u16>,
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

        tracing::debug!(client_ip = %open.client_ip, requested_port = %open.port_name, "processing new proxy request");

        let Some(port_number) = self.ports.get(&open.port_name) else {
            return Err(tonic::Status::failed_precondition("invalid port_name, not exposed for this shard"));
        };
        let target_addr = format!("{}:{port_number}", self.proxy_to_host);

        let target_stream = TcpStream::connect(&target_addr)
            .await
            .context("failed to connect to target port")
            .map_err(|e| tonic::Status::from_error(e.into()))?;
        let (target_reader, target_writer) = target_stream.into_split();

        let write_task =
            tokio::task::spawn(async move { copy_data(recv_from_client, target_writer).await });

        Ok(tonic::Response::new(ProxyResponseStream {
            open: Some(flow::task_network_proxy_response::Opened { err: String::new() }),
            reader: ReaderStream::new(target_reader),
            write_task,
            n_bytes: 0,
        }))
    }
}

async fn copy_data(
    mut requests: tonic::Streaming<TaskNetworkProxyRequest>,
    mut target: OwnedWriteHalf,
) -> Result<usize, anyhow::Error> {
    use tokio::io::AsyncWriteExt;

    let mut n = 0;
    while let Some(req) = requests.message().await? {
        target.write_all(&req.data).await?;
    }
    tracing::debug!(
        n_bytes = n,
        "finished copying from proxy client to connector"
    );
    Ok(n)
}

pub struct ProxyResponseStream {
    n_bytes: u64,
    open: Option<flow::task_network_proxy_response::Opened>,
    reader: ReaderStream<OwnedReadHalf>,
    write_task: tokio::task::JoinHandle<Result<usize, anyhow::Error>>,
}

impl ProxyResponseStream {
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
        // TODO: are there more idiomatic names that proxies use for these values?
        match (r_err, write_result) {
            (None, Poll::Ready(Ok(wbytes))) => tracing::info!(
                client_to_connector = wbytes,
                connector_to_client = r_bytes,
                "proxy connection closed normally"
            ),
            (Some(r_err), Poll::Ready(Ok(w_bytes))) => tracing::warn!(
                connector_to_client = r_bytes,
                client_to_connector = w_bytes,
                connector_to_client_error = %r_err,
                "proxy connection closed with error reading from connector"
            ),
            // The write task failed to receive a message or to write to the connector
            (None, Poll::Ready(Err(w_err))) => tracing::warn!(
                connector_to_client = r_bytes,
                "proxy connection closed with client_to_connector error"
            ),
            // The write task failed to receive a message or to write to the connector
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

impl Stream for ProxyResponseStream {
    type Item = Result<TaskNetworkProxyResponse, tonic::Status>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(opened) = this.open.take() {
            Poll::Ready(Some(Ok(TaskNetworkProxyResponse {
                opened: Some(opened),
                data: Vec::new(),
            })))
        } else {
            let bytes = futures::ready!(this.reader.next().poll_unpin(cx));
            if let Some(read_result) = bytes {
                match read_result {
                    Ok(data) => {
                        this.n_bytes += data.len() as u64;
                        Poll::Ready(Some(Ok(TaskNetworkProxyResponse {
                            opened: None,
                            data: data.into(),
                        })))
                    }
                    Err(err) => {
                        let err = anyhow::Error::from(err);
                        this.log_done(cx, Some(&err));
                        Poll::Ready(Some(Err(tonic::Status::from_error(err.into()))))
                    }
                }
            } else {
                this.log_done(cx, None);
                Poll::Ready(None)
            }
        }
    }
}

/*
struct ClientReadStream {
    inner:  tonic::Streaming<TaskNetworkProxyRequest>,
    current_
}

impl tokio::io::AsyncBufRead for ClientReadStream {
    fn poll_fill_buf(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<&[u8]>> {
        todo!()
    }

    fn consume(self: std::pin::Pin<&mut Self>, amt: usize) {
        todo!()
    }
}
*/
