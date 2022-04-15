#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("failed in starting bouncer process.")]
    BouncerProcessStartError,

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

    #[error("mismatching runtime protocol")]
    MismatchingRuntimeProtocol,

    #[error("No ready signal is received. {0}")]
    NotReady(&'static str),

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

    #[error("Missing required image inspect file. Specify it via --image-inspect-json-path in command line.")]
    MissingImageInspectFile,

    #[error(transparent)]
    NetworkTunnelError(#[from] network_tunnel::errors::Error),

    #[error(transparent)]
    TempfilePersistError(#[from] tempfile::PersistError),

    #[error("Tokio task execution error.")]
    TokioTaskExecutionError(#[from] tokio::task::JoinError),

    #[error("The operation of '{0}' is not expected for the given protocol.")]
    UnexpectedOperation(String),
}

pub fn raise_err<T>(message: &str) -> Result<T, std::io::Error> {
    Err(create_custom_error(message))
}

pub fn create_custom_error(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, message)
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
