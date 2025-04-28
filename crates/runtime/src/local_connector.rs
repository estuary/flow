use futures::{channel::mpsc, Stream, StreamExt};
use std::collections::BTreeMap;

/// Serve a local connector by starting its program and adapting its stdin and stdout.
pub fn serve<Request, Response>(
    command: Vec<String>,                // Connector to run.
    env: BTreeMap<String, String>,       // Environment variables.
    log_handler: impl crate::LogHandler, // Handler for connector logs.
    log_level: ops::LogLevel,            // Log-level of the container, if known.
    protobuf: bool,                      // Whether to use protobuf codec.
    request_rx: mpsc::Receiver<Request>, // Caller's input request stream.
) -> anyhow::Result<impl Stream<Item = anyhow::Result<Response>> + Send>
where
    Request: serde::Serialize + prost::Message + Send + Sync + 'static,
    Response: prost::Message + for<'de> serde::Deserialize<'de> + Default + 'static,
{
    let codec = if protobuf {
        connector_init::Codec::Proto
    } else {
        connector_init::Codec::Json
    };

    // Invoke the underlying local connector.
    let mut connector = connector_init::rpc::new_command(&command);
    connector.envs(&env);

    connector.env("LOG_LEVEL", log_level.or(ops::LogLevel::Info).as_str_name());

    let container_rx = connector_init::rpc::bidi::<Request, Response, _, _>(
        connector,
        codec,
        request_rx.map(Result::Ok),
        log_handler.clone().as_fn(),
    )?;
    let container_rx = crate::stream_status_to_error(container_rx);

    Ok(container_rx)
}
