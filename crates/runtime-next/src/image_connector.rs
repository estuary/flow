use super::container;
use futures::{Stream, StreamExt, channel::mpsc, future::BoxFuture};

/// StartRpcFuture is the response type of a function that starts a connector RPC.
pub type StartRpcFuture<Response> =
    BoxFuture<'static, tonic::Result<tonic::Response<tonic::Streaming<Response>>>>;

/// Serve an image-based connector by starting a container, dialing connector-init,
/// and then starting a gRPC request.
pub async fn serve<Request, Response, StartRpc, L: crate::LogHandler>(
    image: String,                       // Container image to run.
    log_handler: L,                      // Handler for connector logs.
    log_level: ops::LogLevel,            // Log-level of the connector, if known.
    network: &str,                       // Container network to use.
    request_rx: mpsc::Receiver<Request>, // Caller's input request stream.
    start_rpc: StartRpc,                 // Begins RPC over a started container channel.
    task_name: &str,                     // Name of this task, used to label container.
    task_type: ops::TaskType,            // Type of this task, for labeling container.
    plane: crate::Plane,                 // Data-plane context in which the connector is running.
) -> anyhow::Result<(
    impl Stream<Item = tonic::Result<Response>> + Send + use<Request, Response, StartRpc, L>,
    crate::proto::Container,
)>
where
    Request: serde::Serialize + Send + 'static,
    Response: Send + Sync + 'static,
    StartRpc: Fn(tonic::transport::Channel, mpsc::Receiver<Request>) -> StartRpcFuture<Response>
        + Send
        + 'static,
{
    let (container, channel, guard) = container::start(
        &image,
        log_handler.clone(),
        log_level,
        &network,
        &task_name,
        task_type,
        plane,
    )
    .await?;

    // Start RPC over the container's gRPC `channel`.
    let container_rx = (start_rpc)(channel, request_rx).await?.into_inner();

    let container_rx = container_rx.map(move |result| {
        let _guard = &guard; // Move into Stream.
        result
    });

    Ok((container_rx, container))
}
