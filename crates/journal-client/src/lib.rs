pub mod fragments;
pub mod list;
pub mod read;

use proto_grpc::broker::journal_client::JournalClient;
use tonic::{codegen::http::HeaderValue, transport::channel::Channel};

pub use proto_gazette::broker;

pub type Client = JournalClient<WithAuthToken>;

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
    let client = JournalClient::new(WithAuthToken {
        inner: channel,
        token_header: auth_header,
    });

    Ok(client)
}

#[derive(Clone)]
pub struct WithAuthToken {
    inner: Channel,
    token_header: Option<HeaderValue>,
}

impl tonic::client::GrpcService<tonic::body::BoxBody> for WithAuthToken {
    type ResponseBody = <::tonic::transport::Channel as tonic::client::GrpcService<
        tonic::body::BoxBody,
    >>::ResponseBody;
    type Error =
        <::tonic::transport::Channel as tonic::client::GrpcService<tonic::body::BoxBody>>::Error;
    type Future =
        <::tonic::transport::Channel as tonic::client::GrpcService<tonic::body::BoxBody>>::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(
        &mut self,
        mut request: tonic::codegen::http::Request<tonic::body::BoxBody>,
    ) -> Self::Future {
        tracing::trace!(?request, "sending grpc request");

        if let Some(header) = self.token_header.as_ref() {
            request
                .headers_mut()
                .insert("Authorization", header.clone());
        }
        self.inner.call(request)
    }
}
