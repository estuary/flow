#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("channel send error: {source:?}.")]
    ChannelSendError {
        #[from]
        source: std::sync::mpsc::SendError<bool>,
    },

    #[error("channel timeout in receiving messages after 5 seconds.")]
    ChannelTimeoutError,

    #[error("command execution failed: {0}.")]
    CommandExecutionError(String),

    #[error("duplicated key: {0}.")]
    DuplicatedKeyError(&'static str),

    #[error("Entrypoint is an empty string.")]
    EmptyEntrypointError,

    #[error("missing process io pipes.")]
    MissingIOPipe,

    #[error("invalid endpoint json config.")]
    InvalidEndpointConfig,

    #[error("invalid json pointer '{0}' to config.")]
    InvalidJsonPointer(String),

    #[error("IO execution error: {source:?}.")]
    IOError {
        #[from]
        source: std::io::Error,
    },

    #[error("Json serialization error: {source:?}.")]
    JsonError {
        #[from]
        source: serde_json::Error,
    },

    #[error("prost message decode error: {source:?}.")]
    MessageDecodeError {
        #[from]
        source: prost::DecodeError,
    },

    #[error("prost message encode error: {source:?}.")]
    MessageEncodeError {
        #[from]
        source: prost::EncodeError,
    },

    #[error("network proxy error: {source:?}.")]
    NetworkProxyError {
        #[from]
        source: network_proxy::errors::Error,
    },

    #[error("Tokio task execution error: {source:?}.")]
    TokioTaskExecutionError {
        #[from]
        source: tokio::task::JoinError,
    },
}
