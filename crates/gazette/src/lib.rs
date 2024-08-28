pub use proto_gazette::{broker, consumer, uuid};

pub mod journal;
pub mod shard;

mod router;
pub use router::Router;

mod auth;
pub use auth::Auth;

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
            Error::Parsing(_, _) => false,
            Error::Protocol(_) => false,
            Error::UUID(_) => false,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
