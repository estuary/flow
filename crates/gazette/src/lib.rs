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
    #[error(transparent)]
    UUID(#[from] uuid::Error),
    #[error("unexpected server EOF")]
    UnexpectedEof,
    #[error("JWT error")]
    JWT(#[from] jsonwebtoken::errors::Error),
}

impl Error {
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
            Error::BearerToken(_) => false,
            Error::BrokerStatus(_) => false,
            Error::ConsumerStatus(_) => false,
            Error::InvalidEndpoint(_) => false,
            Error::Parsing { .. } => false,
            Error::Protocol(_) => false,
            Error::UUID(_) => false,
            Error::JWT(_) => false,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

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
        // health of the end-to-end HTTP/2 transport.
        .http2_keep_alive_interval(std::time::Duration::from_secs(5))
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
