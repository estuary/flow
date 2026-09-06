//! Image connectors: start a container, dial `flow-connector-init`, and open
//! the protocol RPC over the container's channel.
use crate::container;
use futures::{Stream, future::BoxFuture};
use tokio::sync::mpsc;

/// StartRpcFuture is the response type of a function that starts a connector RPC.
pub(crate) type StartRpcFuture<Response> =
    BoxFuture<'static, tonic::Result<tonic::Response<tonic::Streaming<Response>>>>;

/// Serve an image-based connector by starting a container, dialing connector-init,
/// and then starting a gRPC request.
///
/// The container [`Guard`](container::Guard) is returned rather than tucked into
/// the response stream: the caller drops it explicitly at teardown, which
/// SIGKILLs `docker run` and closes the container's stderr so its log pump can
/// finish. See `serve.rs`.
pub(crate) async fn serve<Request, Response, StartRpc>(
    image: String,                       // Container image to run.
    log_sink: crate::LogSink,            // Sink for connector logs and lifecycle.
    log_level: ops::LogLevel,            // Log-level of the connector, if known.
    network: &str,                       // Container network to use.
    request_rx: mpsc::Receiver<Request>, // Caller's input request stream.
    start_rpc: StartRpc,                 // Begins RPC over a started container channel.
    task_name: &str,                     // Name of this task, used to label container.
    task_type: ops::TaskType,            // Type of this task, for labeling container.
    plane: crate::Plane,                 // Data-plane context in which the connector is running.
) -> anyhow::Result<(
    impl Stream<Item = tonic::Result<Response>> + Send + use<Request, Response, StartRpc>,
    crate::Container,
    connector_init::Codec,
    container::Guard,
)>
where
    Request: serde::Serialize + Send + 'static,
    Response: Send + Sync + 'static,
    StartRpc: Fn(tonic::transport::Channel, mpsc::Receiver<Request>) -> StartRpcFuture<Response>
        + Send
        + 'static,
{
    let (container, channel, guard, codec) = container::start(
        &image, log_sink, log_level, &network, &task_name, task_type, plane,
    )
    .await?;

    // Start RPC over the container's gRPC `channel`.
    let container_rx = (start_rpc)(channel, request_rx).await?.into_inner();

    Ok((container_rx, container, codec, guard))
}
