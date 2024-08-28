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
}

pub type Result<T> = std::result::Result<T, Error>;
