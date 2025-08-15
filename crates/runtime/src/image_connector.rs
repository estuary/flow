use super::container;
use futures::{Stream, TryStreamExt, channel::mpsc, future::BoxFuture};

/// Container is a description of a running Container instance.
pub use proto_flow::runtime::Container;

/// StartRpcFuture is the response type of a function that starts a connector RPC.
pub type StartRpcFuture<Response> =
    BoxFuture<'static, tonic::Result<tonic::Response<tonic::Streaming<Response>>>>;

/// Serve an image-based connector by starting a container, dialing connector-init,
/// and then starting a gRPC request.
pub async fn serve<Request, Response, StartRpc, Attach, L: crate::LogHandler>(
    attach_container: Attach, // Attaches a Container description to a response.
    attach_offset: usize,     // Offset of response stream to which the container is attached.
    image: String,            // Container image to run.
    log_handler: L,           // Handler for connector logs.
    log_level: ops::LogLevel, // Log-level of the connector, if known.
    network: &str,            // Container network to use.
    request_rx: mpsc::Receiver<Request>, // Caller's input request stream.
    start_rpc: StartRpc,      // Begins RPC over a started container channel.
    task_name: &str,          // Name of this task, used to label container.
    task_type: ops::TaskType, // Type of this task, for labeling container.
    publish_ports: bool,      // Whether to expose container ports. Must be true on mac/windoze.
) -> anyhow::Result<
    impl Stream<Item = anyhow::Result<Response>> + Send + use<Request, Response, StartRpc, Attach, L>,
>
where
    Request: serde::Serialize + Send + 'static,
    Response: Send + Sync + 'static,
    StartRpc: Fn(tonic::transport::Channel, mpsc::Receiver<Request>) -> StartRpcFuture<Response>
        + Send
        + 'static,
    Attach: Fn(&mut Response, Container) + Send + 'static,
{
    let (mut container, channel, guard) = container::start(
        &image,
        log_handler.clone(),
        log_level,
        &network,
        &task_name,
        task_type,
        publish_ports,
    )
    .await?;

    // Start RPC over the container's gRPC `channel`.
    let mut container_rx =
        crate::stream_status_to_error((start_rpc)(channel, request_rx).await?.into_inner());

    let container_rx = coroutines::try_coroutine(move |mut co| async move {
        let _guard = guard; // Move into future.
        let mut offset = 0;

        while let Some(mut response) = container_rx.try_next().await? {
            if offset == attach_offset {
                (attach_container)(&mut response, std::mem::take(&mut container));
            }
            offset += 1;
            () = co.yield_(response).await;
        }
        Ok(())
    });

    Ok(container_rx)
}
