pub use proto_gazette::{broker, consumer, uuid};

pub mod journal;
pub mod shard;

mod router;
pub use router::Router;

pub mod metadata;
pub use metadata::Metadata;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid gRPC endpoint: '{0}'")]
    InvalidEndpoint(String),
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
    #[error(transparent)]
    Grpc(#[from] tonic::Status),
    #[error("failed to fetch fragment from storage URL")]
    FetchFragment(#[source] reqwest::Error),
    #[error("failed to read fetched fragment from storage URL")]
    ReadFragment(#[source] std::io::Error),
    #[error("invalid bearer token")]
    BearerToken(#[source] tonic::metadata::errors::InvalidMetadataValue),
    #[error("unexpected broker status: {0:?}")]
    BrokerStatus(broker::Status),
    #[error("unexpected consumer status: {0:?}")]
    ConsumerStatus(consumer::Status),
    #[error("failed to parse document at journal offset range {location:?}")]
    Parsing {
        location: std::ops::Range<i64>,
        #[source]
        err: std::io::Error,
    },
    #[error("{0}")]
    Protocol(&'static str),
    #[error("failed to read from input stream")]
    AppendRead(#[source] std::io::Error),
    #[error(transparent)]
    UUID(#[from] uuid::Error),
    #[error("unexpected server EOF")]
    UnexpectedEof,
    #[error("JWT error")]
    JWT(#[from] jsonwebtoken::errors::Error),
}

/// RetryError is an Error encountered during a retry-able operation.
#[derive(Debug)]
pub struct RetryError {
    /// Number of operation attempts since the last success.
    pub attempt: usize,
    /// Error encountered with this attempt.
    pub inner: Error,
}

impl Error {
    pub fn with_attempt(self, attempt: usize) -> RetryError {
        RetryError {
            attempt: attempt,
            inner: self,
        }
    }

    pub fn is_transient(&self) -> bool {
        match self {
            // These errors are generally failure of a transport, and can be retried.
            Error::Transport(_) => true,
            Error::FetchFragment(_) => true,
            Error::ReadFragment(_) => true,
            Error::UnexpectedEof => true,

            // Some gRPC codes are transient failures.
            Error::Grpc(status) => match status.code() {
                tonic::Code::Unavailable => true,
                tonic::Code::Cancelled => true,
                tonic::Code::Aborted => true,
                _ => false, // Others are not.
            },

            // At this level, we do not consider BrokerStatus or ConsumerStatus
            // to be transient. Callers may want to special-case certain values
            // as fits their circumstances however.
            Error::AppendRead(_) => false,
            Error::BearerToken(_) => false,
            Error::BrokerStatus(_) => false,
            Error::ConsumerStatus(_) => false,
            Error::InvalidEndpoint(_) => false,
            Error::JWT(_) => false,
            Error::Parsing { .. } => false,
            Error::Protocol(_) => false,
            Error::UUID(_) => false,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// RetryResult is a single Result of a retry-able operation.
pub type RetryResult<T> = std::result::Result<T, RetryError>;

/// Lazily dial a gRPC endpoint with opinionated defaults and
/// support for TLS and Unix Domain Sockets.
pub fn dial_channel(endpoint: &str) -> Result<tonic::transport::Channel> {
    use std::time::Duration;

    let ep = tonic::transport::Endpoint::from_shared(endpoint.to_string())
        .map_err(|_err| Error::InvalidEndpoint(endpoint.to_string()))?
        // Note this connect_timeout accounts only for TCP connection time and
        // does not apply to time required for TLS or HTTP/2 transport start,
        // which can block indefinitely if the server is bound but not listening.
        .connect_timeout(Duration::from_secs(5))
        // HTTP/2 keep-alive sends a PING frame every interval to confirm the
        // health of the end-to-end HTTP/2 transport. The duration was selected
        // to be compatible with the default grpc server setting of 5 minutes
        // for `GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS`. If we
        // send pings more frequently than that, then the server may close the
        // connection unexpectedly.
        // See: https://github.com/grpc/grpc/blob/master/doc/keepalive.md
        .http2_keep_alive_interval(std::time::Duration::from_secs(301))
        .tls_config(
            tonic::transport::ClientTlsConfig::new()
                .with_native_roots()
                .assume_http2(true),
        )?;

    let channel =
        match ep.uri().scheme_str() {
            Some("unix") => ep.connect_with_connector_lazy(tower::util::service_fn(
                |uri: tonic::transport::Uri| connect_unix(uri),
            )),
            Some("https" | "http") => ep.connect_lazy(),

            _ => return Err(Error::InvalidEndpoint(endpoint.to_string())),
        };

    Ok(channel)
}

async fn connect_unix(
    uri: tonic::transport::Uri,
) -> std::io::Result<hyper_util::rt::TokioIo<tokio::net::UnixStream>> {
    let path = uri.path();
    // Wait until the filesystem path exists, because it's hard to tell from
    // the error so that we can re-try. This is expected to be cut short by the
    // connection timeout if the path never appears.
    for i in 1.. {
        if let Ok(meta) = tokio::fs::metadata(path).await {
            tracing::debug!(?path, ?meta, "UDS path now exists");
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20 * i)).await;
    }
    Ok(hyper_util::rt::TokioIo::new(
        tokio::net::UnixStream::connect(path).await?,
    ))
}

fn backoff(attempt: usize) -> std::time::Duration {
    // The choices of backoff duration reflect that we're usually waiting for
    // the cluster to converge on a shared understanding of ownership, and that
    // involves a couple of Nagle-like read delays (~30ms) as Etcd watch
    // updates are applied by participants.
    match attempt {
        0 => std::time::Duration::ZERO,
        1 => std::time::Duration::from_millis(50),
        2 | 3 => std::time::Duration::from_millis(100),
        4 | 5 => std::time::Duration::from_secs(1),
        _ => std::time::Duration::from_secs(5),
    }
}
