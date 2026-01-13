use crate::router;
use proto_gazette::{broker, consumer};
use tonic::transport::Channel;

/// Client for interacting with Gazette shards.
#[derive(Clone)]
pub struct Client {
    router: crate::Router,
    tokens: tokens::PendingWatch<(proto_grpc::Metadata, broker::process_spec::Id)>,
}

// SubClient is the routed sub-client of Client.
type SubClient = proto_grpc::consumer::shard_client::ShardClient<
    tonic::service::interceptor::InterceptedService<Channel, proto_grpc::Metadata>,
>;

impl Client {
    /// Build a Client which dispatches request to the given default endpoint with the given Metadata.
    pub fn new(
        default_endpoint: String,
        metadata: proto_grpc::Metadata,
        router: crate::Router,
    ) -> Self {
        Self::new_with_tokens(
            |(metadata, endpoint)| Ok((metadata.clone(), endpoint.clone())),
            router,
            tokens::fixed(Ok((metadata, default_endpoint))),
        )
    }

    /// Build a Client which draws Metadata and a default endpoint from a tokens::Watch.
    /// The `extract` closure maps the arbitrary Token type into Metadata and default endpoint.
    pub fn new_with_tokens<Token, Extract>(
        extract: Extract,
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

        Self { router, tokens }
    }

    // TODO(johnny): Remove this method once all clients use tokens.
    pub fn with_endpoint_and_metadata(
        &self,
        default_endpoint: String,
        metadata: proto_grpc::Metadata,
    ) -> Self {
        Self::new(default_endpoint, metadata, self.router.clone())
    }

    /// Invoke the Gazette shard List RPC.
    pub async fn list(
        &self,
        req: consumer::ListRequest,
    ) -> Result<consumer::ListResponse, crate::Error> {
        let mut client = self
            .subclient(
                None, // No route header (any member can answer).
                router::Mode::Default,
            )
            .await?;

        let resp = client
            .list(req)
            .await
            .map_err(crate::Error::Grpc)?
            .into_inner();

        check_ok(resp.status(), resp)
    }

    /// Invoke the Gazette shard Apply RPC.
    pub async fn apply(
        &self,
        req: consumer::ApplyRequest,
    ) -> Result<consumer::ApplyResponse, crate::Error> {
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

    /// Invoke the Gazette shard Unassign RPC.
    pub async fn unassign(
        &self,
        req: consumer::UnassignRequest,
    ) -> Result<consumer::UnassignResponse, crate::Error> {
        let mut client = self
            .subclient(
                None, // No route header (any member can unassign).
                router::Mode::Default,
            )
            .await?;

        let only_failed = req.only_failed;

        let resp = client
            .unassign(req)
            .await
            .map_err(crate::Error::Grpc)?
            .into_inner();

        match resp.status() {
            consumer::Status::Ok => Ok(resp),
            // EtcdTransactionFailed is a kind of "success":
            // there was a raced removal of the failed assignment, and we lost.
            consumer::Status::EtcdTransactionFailed if only_failed => Ok(resp),
            status => Err(crate::Error::ConsumerStatus(status)),
        }
    }

    /// Invoke the Gazette shard GetHints RPC.
    pub async fn get_hints(
        &self,
        req: consumer::GetHintsRequest,
    ) -> Result<consumer::GetHintsResponse, crate::Error> {
        let mut client = self
            .subclient(
                None, // No route header (any member can get hints).
                router::Mode::Default,
            )
            .await?;

        let resp = client
            .get_hints(req)
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

        Ok(
            proto_grpc::consumer::shard_client::ShardClient::with_interceptor(
                channel,
                metadata.clone(),
            ),
        )
    }
}

fn check_ok<R>(status: consumer::Status, r: R) -> Result<R, crate::Error> {
    if status == consumer::Status::Ok {
        Ok(r)
    } else {
        Err(crate::Error::ConsumerStatus(status))
    }
}
