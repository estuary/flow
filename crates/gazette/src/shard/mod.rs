use crate::router;
use proto_gazette::{broker, consumer};
use tonic::transport::Channel;

/// ClientStream is an infinite Stream which yields either ready Clients,
/// or tonic::Status indicating a client-facing error in refreshing
/// or extracting from a Token.
///
/// ClientStream will yield no more than once for every update of the
/// underlying TokenStream. The recommended usage is to await a first Client,
/// and to thereafter poll on-demand to check for updated Clients or errors.
pub type ClientStream = futures::stream::BoxStream<'static, tonic::Result<Client>>;

/// Client for interacting with Gazette consumer shards.
#[derive(Clone)]
pub struct Client {
    bearer_token: proto_auth::BearerToken,
    default_id: broker::process_spec::Id,
    router: crate::Router,
}

// SubClient is the routed sub-client of Client.
type SubClient = proto_grpc::consumer::shard_client::ShardClient<
    tonic::service::interceptor::InterceptedService<Channel, proto_auth::BearerToken>,
>;

/// Build a ClientStream from a Router, TokenStream, and Extract function.
/// Extract maps Tokens into (BearerToken, default_address) pairs.
pub fn new_client_stream<Token, Extract>(
    router: crate::Router,
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
                router: router.clone(),
            })
        })
        .boxed()
}

impl Client {
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
        let (channel, _local) = self
            .router
            .route(route_header, route_mode, &self.default_id)?;

        Ok(
            proto_grpc::consumer::shard_client::ShardClient::with_interceptor(
                channel,
                self.bearer_token.clone(),
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
