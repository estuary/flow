use super::container;
use futures::{channel::mpsc, future::BoxFuture, Stream, TryStreamExt};

/// Container is a description of a running Container instance.
pub use proto_flow::runtime::Container;

/// StartRpcFuture is the response type of a function that starts a connector RPC.
pub type StartRpcFuture<Response> =
    BoxFuture<'static, tonic::Result<tonic::Response<tonic::Streaming<Response>>>>;

/// Serve an image-based connector by starting a container, dialing connector-init,
/// and then starting a gRPC request.
pub async fn serve<Request, Response, StartRpc, Attach>(
    attach_container: Attach, // Attaches a Container description to a response.
    image: String,            // Container image to run.
    log_handler: impl crate::LogHandler, // Handler for connector logs.
    log_level: Option<ops::LogLevel>, // Log-level of the connector, if known.
    network: &str,            // Container network to use.
    request_rx: mpsc::Receiver<Request>, // Caller's input request stream.
    start_rpc: StartRpc,      // Begins RPC over a started container channel.
    task_name: &str,          // Name of this task, used to label container.
    task_type: ops::TaskType, // Type of this task, for labeling container.
) -> anyhow::Result<impl Stream<Item = anyhow::Result<Response>> + Send>
where
    Request: serde::Serialize + Send + 'static,
    Response: Send + Sync + 'static,
    StartRpc: Fn(tonic::transport::Channel, mpsc::Receiver<Request>) -> StartRpcFuture<Response>
        + Send
        + 'static,
    Attach: Fn(&mut Response, Container) + Send + 'static,
{
    let (container, channel, guard) = container::start(
        &image,
        log_handler.clone(),
        log_level,
        &network,
        &task_name,
        task_type,
    )
    .await?;

    // Start RPC over the container's gRPC `channel`.
    let mut container_rx =
        crate::stream_status_to_error((start_rpc)(channel, request_rx).await?.into_inner());

    let container_rx = coroutines::try_coroutine(move |mut co| async move {
        let _guard = guard; // Move into future.

        // Attach `container` to the first request.
        let Some(mut first) = container_rx.try_next().await? else {
            return Ok(());
        };
        (attach_container)(&mut first, container);
        () = co.yield_(first).await;

        // Stream successive requests.
        while let Some(response) = container_rx.try_next().await? {
            () = co.yield_(response).await;
        }
        Ok(())
    });

    Ok(container_rx)
}
