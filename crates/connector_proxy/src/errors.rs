#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("channel timeout in receiving messages after 5 seconds.")]
    ChannelTimeoutError,

    #[error("command execution failed: {0}.")]
    CommandExecutionError(String),

    #[error("duplicated key: {0}.")]
    DuplicatedKeyError(&'static str),

    #[error("Entrypoint is an empty string.")]
    EmptyEntrypointError,

    #[error("unable to parse the inspect file.")]
    InvalidImageInspectFile,

    #[error("missing process io pipes.")]
    MissingIOPipe,

    #[error("invalid endpoint json config.")]
    InvalidEndpointConfig,

    #[error("invalid json pointer '{0}' to config.")]
    InvalidJsonPointer(String),

    #[error(transparent)]
    IOError(#[from] std::io::Error),

    #[error(transparent)]
    JsonError(#[from] serde_json::Error),

    #[error(transparent)]
    MessageDecodeError(#[from] prost::DecodeError),

    #[error(transparent)]
    MessageEncodeError(#[from] prost::EncodeError),

    #[error(transparent)]
    NetworkProxyError(#[from] network_proxy::errors::Error),

    #[error("Tokio task execution error.")]
    TokioTaskExecutionError(#[from] tokio::task::JoinError),
}

pub trait Must<T> {
    fn or_bail(self) -> T;
}

impl<T, E> Must<T> for Result<T, E>
where
    E: std::fmt::Display + std::fmt::Debug,
{
    fn or_bail(self) -> T {
        match self {
            Ok(t) => t,
            Err(e) => {
                tracing::debug!(error_details = ?e);
                std::process::exit(1);
            }
        }
    }
}
