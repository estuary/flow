use futures::{Stream, StreamExt};
use std::collections::BTreeMap;
use tokio::sync::mpsc;

/// Serve a local connector by starting its program and adapting its stdin and stdout.
pub fn serve<Request, Response>(
    command: Vec<String>,                // Connector to run.
    env: BTreeMap<String, String>,       // Environment variables.
    logger: impl crate::Logger,          // Logger for connector logs.
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

    // Local connectors have no container lifecycle; the logger is used only as
    // the connector log sink.
    let log_sink = move |log: &ops::Log| logger.log(log);

    let container_rx = connector_init::rpc::bidi::<Request, Response, _, _>(
        connector,
        codec,
        tokio_stream::wrappers::ReceiverStream::new(request_rx).map(Result::Ok),
        log_sink,
    )?;

    Ok(container_rx)
}
