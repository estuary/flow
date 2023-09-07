pub mod fragments;
pub mod list;
pub mod read;
pub mod append;

use proto_grpc::broker::journal_client::JournalClient;
use tonic::{
    codegen::InterceptedService, metadata::AsciiMetadataValue, service::Interceptor,
    transport::channel::Channel,
};

pub use proto_gazette::broker;

pub type Client = JournalClient<InterceptedService<Channel, AuthHeader>>;

#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error("bad uri: '{0}'")]
    BadUri(String),

    #[error("grpc transport error: {0}")]
    Grpc(#[from] tonic::transport::Error),

    #[error("invalid bearer token")]
    InvalidBearerToken,
}

pub async fn connect_journal_client(
    broker_url: String,
    bearer_token: Option<String>,
) -> Result<Client, ConnectError> {
    tracing::trace!("about to connect channel");

    let auth_header = if let Some(token) = bearer_token {
        Some(
            format!("Bearer {}", &token)
                .parse()
                // parse can only fail if the bearer token contains invalid characters
                .map_err(|_| ConnectError::InvalidBearerToken)?,
        )
    } else {
        None
    };

    let channel = Channel::from_shared(broker_url.clone())
        .map_err(|_| ConnectError::BadUri(broker_url))?
        .connect_timeout(std::time::Duration::from_secs(20))
        .connect()
        .await?;

    tracing::trace!("channel is connected");
    Ok(JournalClient::with_interceptor(
        channel,
        AuthHeader(auth_header),
    ))
}

#[derive(Clone)]
pub struct AuthHeader(Option<AsciiMetadataValue>);
impl Interceptor for AuthHeader {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(bearer) = self.0.as_ref() {
            request
                .metadata_mut()
                .insert("authorization", bearer.clone());
        }
        Ok(request)
    }
}
