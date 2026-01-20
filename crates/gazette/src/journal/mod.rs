use crate::router;
use proto_gazette::broker;
use tonic::transport::Channel;

mod append;
pub mod list;
mod read;

mod read_json_lines;
pub use read_json_lines::{ReadJsonLine, ReadJsonLines};

// SubClient is the routed sub-client of Client.
type SubClient = proto_grpc::broker::journal_client::JournalClient<
    tonic::service::interceptor::InterceptedService<Channel, proto_grpc::Metadata>,
>;

/// Client for interacting with Gazette journals.
#[derive(Clone)]
pub struct Client {
    fragment_client: reqwest::Client,
    router: crate::Router,
    tokens: tokens::PendingWatch<(proto_grpc::Metadata, broker::process_spec::Id)>,
}

impl Client {
    /// Build a reqwest Client suited for fetching journal fragments.
    /// This client should be built once and cloned across many Clients.
    pub fn new_fragment_client() -> reqwest::Client {
        // Use HTTP/1 for fetching fragments, as storage backends may have
        // restricted HTTP/2 flow control and we may have concurrent streams
        // with high throughput / stuffed flow control windows.
        reqwest::Client::builder().http1_only().build().unwrap()
    }

    /// Build a Client which dispatches request to the given default endpoint with the given Metadata.
    pub fn new(
        default_endpoint: String,
        fragment_client: reqwest::Client,
        metadata: proto_grpc::Metadata,
        router: crate::Router,
    ) -> Self {
        Self::new_with_tokens(
            |(metadata, endpoint)| Ok((metadata.clone(), endpoint.clone())),
            fragment_client,
            router,
            tokens::fixed(Ok((metadata, default_endpoint))),
        )
    }

    /// Build a Client which draws Metadata and a default endpoint from a tokens::Watch.
    /// The `extract` closure maps the arbitrary Token type into Metadata and default endpoint.
    pub fn new_with_tokens<Token, Extract>(
        extract: Extract,
        fragment_client: reqwest::Client,
        router: crate::Router,
        tokens: tokens::PendingWatch<Token>,
    ) -> Self
    where
        Token: Send + Sync + 'static,
        Extract:
            Fn(&Token) -> tonic::Result<(proto_grpc::Metadata, String)> + Send + Sync + 'static,
    {
        let tokens = tokens.map(move |token, _prior| {
            let (metadata, default_endpoint) = extract(token)?;

            let default_id = broker::process_spec::Id {
                zone: String::new(),
                suffix: default_endpoint,
            };
            Ok((metadata, default_id))
        });

        Self {
            fragment_client,
            router,
            tokens,
        }
    }

    // TODO(johnny): Remove this method once all clients use tokens.
    pub fn with_endpoint_and_metadata(
        &self,
        default_endpoint: String,
        metadata: proto_grpc::Metadata,
    ) -> Self {
        Self::new(
            default_endpoint,
            self.fragment_client.clone(),
            metadata,
            self.router.clone(),
        )
    }

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

    pub async fn fragment_store_health(
        &self,
        req: broker::FragmentStoreHealthRequest,
    ) -> crate::Result<broker::FragmentStoreHealthResponse> {
        let mut client = self
            .subclient(
                None, // No route header (any member can check health).
                router::Mode::Default,
            )
            .await?;

        let resp = client
            .fragment_store_health(req)
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
        let token = self.tokens.ready().await.token();
        let (metadata, default_id) = token.result()?;
        let (channel, _local) = self.router.route(route_header, route_mode, default_id)?;

        // TODO(johnny): Use `_local` to selectively enable LZ4 compression
        // when traversing a non-local zone.
        Ok(
            proto_grpc::broker::journal_client::JournalClient::with_interceptor(
                channel,
                metadata.clone(),
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
