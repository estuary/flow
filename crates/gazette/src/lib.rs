pub use proto_gazette::{broker, consumer, uuid};

pub mod delta;
pub mod journal;
pub mod shard;

mod router;
pub use router::Router;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid gRPC endpoint: '{0}'")]
    InvalidEndpoint(String),
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
    #[error("gRPC code: {:?}, message: {}", .0.code(), .0.message())]
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
    #[error("reading lines: {message} (at offset {offset})")]
    ReadLines { message: &'static str, offset: i64 },
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
            attempt,
            inner: self,
        }
    }

    pub fn is_transient(&self) -> bool {
        match self {
            // These errors are generally failure of a transport, and can be retried.
            Error::Transport(_) => true,
            Error::ReadFragment(_) => true,
            Error::UnexpectedEof => true,

            // When no HTTP status is available (e.g. connection refused, DNS failure,
            // timeout) the error is assumed transient — these are network-level
            // failures that are generally worth retrying.
            Error::FetchFragment(inner) => inner
                .status()
                .map(|status| !status.is_client_error())
                .unwrap_or(true),

            // Some gRPC codes are transient failures.
            Error::Grpc(status) => match status.code() {
                tonic::Code::Unavailable => true,
                tonic::Code::Cancelled => true,
                tonic::Code::Aborted => true,

                // Broken transports are Unknown with specific messages.
                tonic::Code::Unknown
                    if matches!(
                        status.message(),
                        "h2 protocol error: error reading a body from connection"
                            | "transport error"
                    ) =>
                {
                    true
                }

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
            Error::ReadLines { .. } => false,
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

    // Normalize "unix://<authority>/path" to "unix:/path". Go's gRPC requires
    // a URI authority in UDS endpoints (e.g. "unix://localhost/path" or
    // "unix://hostname/path"), but tonic strips the "unix://" prefix and uses
    // the remainder as the socket file path, incorrectly including the authority
    // (e.g. "localhost/tmp/sock" instead of "/tmp/sock"). Parse as a URL and
    // drop the host so tonic sees the correct absolute path.
    let endpoint = match url::Url::parse(endpoint) {
        Ok(url) if url.scheme() == "unix" && url.has_host() => {
            std::borrow::Cow::Owned(format!("unix:{}", url.path()))
        }
        _ => std::borrow::Cow::Borrowed(endpoint),
    };

    let ep = tonic::transport::Endpoint::from_shared(endpoint.to_string())
        .map_err(|_err| Error::InvalidEndpoint(endpoint.to_string()))?
        // Note this connect_timeout accounts only for TCP connection time and
        // does not apply to time required for TLS or HTTP/2 transport start,
        // which can block indefinitely if the server is bound but not listening.
        // Also, this timeout gets split between all of the IP addresses that endpoint
        // resolves to. Thus, if the endpoint resolves to 10 different addresses, then
        // the effective timeout per address is 60 / 10 = 6 seconds. This is why
        // the value is relatively high.
        .connect_timeout(Duration::from_secs(60))
        // HTTP/2 keep-alive sends a PING frame every interval to confirm the
        // health of the end-to-end HTTP/2 transport. The duration was selected
        // to be compatible with the default grpc server setting of 5 minutes
        // for `GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS`. If we
        // send pings more frequently than that, then the server may close the
        // connection unexpectedly.
        // See: https://github.com/grpc/grpc/blob/master/doc/keepalive.md
        .http2_keep_alive_interval(std::time::Duration::from_secs(301))
        .initial_connection_window_size(i32::MAX as u32);

    // TLS is only meaningful for TCP, not UDS. Tonic 0.14+ rejects tls_config on UDS endpoints.
    let ep = if endpoint.starts_with("unix:") {
        ep
    } else {
        ep.tls_config(
            tonic::transport::ClientTlsConfig::new()
                .with_native_roots()
                .assume_http2(true),
        )?
    };

    Ok(ep.connect_lazy())
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
