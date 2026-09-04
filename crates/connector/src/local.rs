//! Local connectors: a subprocess speaking the protocol over stdin / stdout,
//! with `ops::Log`s on stderr. Permitted only in `Plane::Local` contexts.
use futures::{Stream, StreamExt};
use std::collections::BTreeMap;
use tokio::sync::mpsc;

/// Serve a local connector by starting its program and adapting its stdin and stdout.
///
/// There is no container lifecycle here, and no [`Guard`](crate::container::Guard):
/// `connector_init::rpc::bidi` reads the subprocess's stderr through before the
/// returned stream's terminal item, so trailing logs precede it already.
pub fn serve<Request, Response>(
    command: Vec<String>,                // Connector to run.
    env: BTreeMap<String, String>,       // Environment variables.
    log_sink: crate::LogSink,            // Sink for connector logs.
    log_level: ops::LogLevel,            // Log-level of the container, if known.
    codec: connector_init::Codec,        // Codec spoken by the connector.
    request_rx: mpsc::Receiver<Request>, // Caller's input request stream.
) -> anyhow::Result<impl Stream<Item = tonic::Result<Response>> + Send>
where
    Request: serde::Serialize + prost::Message + Send + Sync + 'static,
    Response: prost::Message + for<'de> serde::Deserialize<'de> + Default + 'static,
{
    // Invoke the underlying local connector.
    let mut connector = connector_init::rpc::new_command(&command);
    connector.envs(&env);

    connector.env("LOG_FORMAT", "json");
    connector.env("LOG_LEVEL", log_level.or(ops::LogLevel::Info).as_str_name());

    let container_rx = connector_init::rpc::bidi::<Request, Response, _, _, _>(
        connector,
        codec,
        tokio_stream::wrappers::ReceiverStream::new(request_rx).map(Result::Ok),
        move |log| {
            let log_sink = log_sink.clone();
            async move { log_sink.send(log).await }
        },
    )?;

    Ok(container_rx)
}
