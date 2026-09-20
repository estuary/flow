use crate::router;
use proto_gazette::broker;
use tonic::transport::Channel;

mod append;
pub mod list;
pub mod read;

// TODO(johnny): Update usages to gazette::journal::read::ReadJsonLines;
pub use read::{ReadJsonLine, ReadJsonLines};

/// ClientFactory is a standard closure shape that builds Gazette
/// journal Clients. It's used for lazy initialization of Clients
/// in dynamically-authorized contexts.
pub type ClientFactory = std::sync::Arc<
    dyn Fn(
            // Authorization Subject.
            // Generally this is a shard ID or template prefix.
            String,
            // Authorization Object.
            // Generally this is a journal name prefix.
            String,
        ) -> Client
        + Send
        + Sync,
>;

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
        reqwest::Client::builder()
            // Use HTTP/1 for fetching fragments, as storage backends may have
            // restricted HTTP/2 flow control and we may have concurrent streams
            // with high throughput / stuffed flow control windows that we expect
            // to read in specific orders (leading to live-lock).
            .http1_only()
            // Bound connection establishment (TCP + TLS). Without this a storage
            // endpoint that accepts the socket but never completes the handshake
            // wedges a fragment fetch forever — TCP keepalive can't help while the
            // peer is still ACKing. reqwest sets no connect timeout by default.
            .connect_timeout(std::time::Duration::from_secs(30))
            // We don't use `read_timeout` because it's a little heavy (a boxed
            // future with every read), and because we may poll infrequently,
            // but reqwest biases toward `read_timeout` rather than completion
            // of a ready read.
            .build()
            .unwrap()
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

    /// Derive a Client which signs its own bearer tokens from `claims` and `key`,
    /// renewing each ahead of its expiry, while sharing this Client's fragment
    /// client and Router. `duration` is both the lifetime of a token and the
    /// cadence at which it's renewed.
    ///
    /// Signing for itself is how a data-plane component reaches the journals of
    /// its own state — shard recovery logs and disk journals — because those are
    /// authorized under the data plane's signing key rather than through the
    /// control plane's authorization API.
    ///
    /// This is the counterpart of wrapping a Go client in `NewAuthJournalClient`
    /// with a `KeyedAuth`, with one difference: claims are fixed per Client here,
    /// where Go derives them per request.
    pub fn with_signed_claims(
        &self,
        claims: proto_gazette::Claims,
        key: tokens::jwt::EncodingKey,
        duration: tokens::TimeDelta,
        default_endpoint: String,
    ) -> Self {
        let source = tokens::jwt::SignedSource {
            claims,
            set_time_claims: Box::new(|claims: &mut proto_gazette::Claims, iat, exp| {
                (claims.iat, claims.exp) = (iat.timestamp() as u64, exp.timestamp() as u64);
            }),
            duration,
            key,
        };

        Self::new_with_tokens(
            move |token: &String| {
                Ok((
                    proto_grpc::Metadata::new().with_bearer_token(token)?,
                    default_endpoint.clone(),
                ))
            },
            self.fragment_client.clone(),
            self.router.clone(),
            tokens::watch(source),
        )
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
                &mut None, // No route header (any member can apply).
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
            .subclient(&mut req.header, router::Mode::Replica)
            .await?;

        let resp = client
            .list_fragments(req)
            .await
            .map_err(crate::Error::Grpc)?
            .into_inner();

        check_ok(resp.status(), resp)
    }

    /// Invoke the Gazette journal ListFragments API, paged to completion as Go's
    /// `client.ListAllFragments` does.
    ///
    /// The listing is what a pruner of a fragment store works from.
    pub async fn list_all_fragments(
        &self,
        mut req: broker::FragmentsRequest,
    ) -> crate::Result<broker::FragmentsResponse> {
        let mut all = self.list_fragments(req.clone()).await?;

        while all.next_page_token != 0 {
            req.next_page_token = all.next_page_token;

            let mut next = self.list_fragments(req.clone()).await?;
            all.fragments.append(&mut next.fragments);
            all.next_page_token = next.next_page_token;
        }
        Ok(all)
    }

    pub async fn fragment_store_health(
        &self,
        req: broker::FragmentStoreHealthRequest,
    ) -> crate::Result<broker::FragmentStoreHealthResponse> {
        let mut client = self
            .subclient(
                &mut None, // No route header (any member can check health).
                router::Mode::Default,
            )
            .await?;

        let resp = client
            .fragment_store_health(req)
            .await
            .map_err(crate::Error::Grpc)?
            .into_inner();

        // Surface the store's own diagnostic, which `check_ok` would discard.
        if resp.status() == broker::Status::FragmentStoreUnhealthy {
            return Err(crate::Error::FragmentStoreUnhealthy(
                resp.store_health_error,
            ));
        }
        check_ok(resp.status(), resp)
    }

    async fn subclient(
        &self,
        route_header: &mut Option<broker::Header>,
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

/// Selector which matches exactly one journal. Gazette indexes a journal's own
/// name as a label of its spec, so a name is selected as any other label is.
pub(crate) fn name_selector(journal: &str) -> broker::LabelSelector {
    broker::LabelSelector {
        include: Some(broker::LabelSet {
            labels: vec![broker::Label {
                name: "name".to_string(),
                value: journal.to_string(),
                prefix: false,
            }],
        }),
        exclude: None,
    }
}

fn check_ok<R>(status: broker::Status, r: R) -> Result<R, crate::Error> {
    if status == broker::Status::Ok {
        Ok(r)
    } else {
        Err(crate::Error::BrokerStatus(status))
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_a_name_selector_matches_exactly_one_journal() {
        let selector = super::name_selector("acmeCo/journal");
        let include = selector.include.expect("names are an include");

        assert_eq!(include.labels.len(), 1);
        assert_eq!(include.labels[0].name, "name");
        assert_eq!(include.labels[0].value, "acmeCo/journal");
        assert!(!include.labels[0].prefix);
        assert_eq!(selector.exclude, None);
    }
}
