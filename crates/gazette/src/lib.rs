pub mod journal;

mod router;
pub use router::Router;

mod interceptor;
pub use interceptor::Interceptor;

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

    //#[error("{}", .0.as_str_name())]
    //Broker(proto_gazette::broker::Status),
    //#[error("{}", .0.as_str_name())]
    //Consumer(proto_gazette::consumer::Status),
    #[error("{0}")]
    Protocol(&'static str),
}
