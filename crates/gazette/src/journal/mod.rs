use crate::router;
use proto_gazette::broker;
use tonic::transport::Channel;

mod append;
mod list;
mod read;

mod read_json_lines;
pub use read_json_lines::{ReadJsonLine, ReadJsonLines};

/// ClientStream is an infinite Stream which yields either ready Clients,
/// or tonic::Status indicating a client-facing error in refreshing
/// or extracting from a Token.
///
/// ClientStream will yield no more than once for every update of the
/// underlying TokenStream. The recommended usage is to await a first Client,
/// and to thereafter poll on-demand to check for updated Clients or errors.
pub type ClientStream = futures::stream::BoxStream<'static, tonic::Result<Client>>;

/// Client for interacting with Gazette journal brokers.
#[derive(Clone)]
pub struct Client {
    bearer_token: proto_auth::BearerToken,
    default_id: broker::process_spec::Id,
    fragment_client: reqwest::Client,
    router: crate::Router,
}

// SubClient is the routed sub-client of Client.
type SubClient = proto_grpc::broker::journal_client::JournalClient<
    tonic::service::interceptor::InterceptedService<Channel, proto_auth::BearerToken>,
>;

/// Build a reqwest Client suited for fetching journal fragments.
/// This client should be built once and cloned across many ClientStreams.
pub fn new_fragment_client() -> reqwest::Client {
    // Use HTTP/1 for fetching fragments, as storage backends may have
    // restricted HTTP/2 flow control and we may have concurrent streams
    // with high throughput / stuffed flow control windows.
    reqwest::Client::builder().http1_only().build().unwrap()
}

/// Build a ClientStream from a Router, TokenStream, and Extract function.
/// Extract maps Tokens into (BearerToken, default_address) pairs.
pub fn new_client_stream<Token, Extract>(
    router: crate::Router,
    fragment_client: reqwest::Client,
    tokens: proto_auth::TokenStream<Token>,
    mut extract: Extract,
) -> ClientStream
where
    Token: Send + Sync + 'static,
    Extract:
        FnMut(&Token) -> tonic::Result<(proto_auth::BearerToken, String)> + Send + Sync + 'static,
{
    use futures::StreamExt;

    tokens
        .map_changes(move |token| {
            let (bearer_token, default_endpoint) = extract(token)?;

            let default_id = broker::process_spec::Id {
                zone: String::new(),
                suffix: default_endpoint,
            };

            Ok(Client {
                bearer_token,
                default_id,
                fragment_client: fragment_client.clone(),
                router: router.clone(),
            })
        })
        .boxed()
}

impl Client {
    /// Invoke the Gazette journal Apply API.
    pub async fn apply(&self, req: broker::ApplyRequest) -> crate::Result<broker::ApplyResponse> {
        let mut client = self
            .subclient(
                None, // No route header (any member can apply).
                router::Mode::Default,
            )
            .await?;

        let resp = client
            .apply(req)
            .await
            .map_err(crate::Error::Grpc)?
            .into_inner();

        check_ok(resp.status(), resp)
    }

    /// Invoke the Gazette journal ListFragments API.
    pub async fn list_fragments(
        &self,
        mut req: broker::FragmentsRequest,
    ) -> crate::Result<broker::FragmentsResponse> {
        let mut client = self
            .subclient(
                req.header.as_mut(),
                if req.do_not_proxy {
                    router::Mode::Replica
                } else {
                    router::Mode::Default
                },
            )
            .await?;

        let resp = client
            .list_fragments(req)
            .await
            .map_err(crate::Error::Grpc)?
            .into_inner();

        check_ok(resp.status(), resp)
    }

    async fn subclient(
        &self,
        route_header: Option<&mut broker::Header>,
        route_mode: router::Mode,
    ) -> crate::Result<SubClient> {
        let (channel, _local) = self
            .router
            .route(route_header, route_mode, &self.default_id)?;

        // TODO(johnny): Use `_local` to selectively enable LZ4 compression
        // when traversing a non-local zone.
        Ok(
            proto_grpc::broker::journal_client::JournalClient::with_interceptor(
                channel,
                self.bearer_token.clone(),
            ),
        )
    }
}

fn check_ok<R>(status: broker::Status, r: R) -> Result<R, crate::Error> {
    if status == broker::Status::Ok {
        Ok(r)
    } else {
        Err(crate::Error::BrokerStatus(status))
    }
}
