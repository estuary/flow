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
                &mut None, // No route header (any member can answer).
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

    /// Invoke the Gazette shard Unassign RPC.
    pub async fn unassign(
        &self,
        req: consumer::UnassignRequest,
    ) -> Result<consumer::UnassignResponse, crate::Error> {
        let mut client = self
            .subclient(
                &mut None, // No route header (any member can unassign).
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
                &mut None, // No route header (any member can get hints).
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
        route_header: &mut Option<broker::Header>,
        route_mode: router::Mode,
    ) -> crate::Result<SubClient> {
        let (channel, metadata) = self.route_channel(route_header, route_mode).await?;

        Ok(proto_grpc::consumer::shard_client::ShardClient::with_interceptor(channel, metadata))
    }

    /// Route a Channel to a member of a shard's serving topology, honoring the
    /// given route header and Mode, and return it with the request Metadata to
    /// attach. This is the primitive behind `subclient`, exposed so callers can
    /// dial non-consumer services co-hosted on the reactor (e.g. the Flow
    /// SyncNow API) that require primary routing.
    pub async fn route_channel(
        &self,
        route_header: &mut Option<broker::Header>,
        route_mode: router::Mode,
    ) -> crate::Result<(Channel, proto_grpc::Metadata)> {
        let token = self.tokens.ready().await.token();
        let (metadata, default_id) = token.result()?;
        let (channel, _local) = self.router.route(route_header, route_mode, default_id)?;

        Ok((channel, metadata.clone()))
    }

    /// Invoke the Flow SyncNow RPC against the primary of `shard_id`, forcing it
    /// to immediately commit its open transaction and blocking until that commit
    /// is durable.
    ///
    /// Unlike the gazette shard RPCs (which any member may answer), this routes
    /// to the shard's primary. Seed `route_header` from the shard's Route (as
    /// returned by a prior `list`); on a `NOT_SHARD_PRIMARY` response, retry with
    /// the header carried on the response to converge on the current primary.
    pub async fn sync_now(
        &self,
        shard_id: String,
        route_header: &mut Option<broker::Header>,
    ) -> Result<proto_flow::flow::SyncNowResponse, crate::Error> {
        // Preserve the header we route with to send as the request's ProxyHeader
        // (`route()` clears `route_header` as a side effect of member selection).
        let request_header = route_header.clone();
        let (channel, metadata) = self
            .route_channel(route_header, router::Mode::Primary)
            .await?;

        let mut client =
            proto_grpc::flow::sync_now_client::SyncNowClient::with_interceptor(channel, metadata);

        let resp = client
            .sync_now(proto_flow::flow::SyncNowRequest {
                shard_id,
                header: request_header,
            })
            .await
            .map_err(crate::Error::Grpc)?
            .into_inner();

        Ok(resp)
    }
}

/// Per-task outcome of [`sync_task_shards`].
#[derive(Debug, Default)]
pub struct SyncSummary {
    /// IDs of shards which committed their forced transaction.
    pub synced: Vec<String>,
    /// Shards which could not be synced, paired with a human-readable reason.
    pub failed: Vec<(String, String)>,
}

/// Force every shard of a task to immediately commit its open transaction
/// ("sync now"), routing to each shard's primary and blocking until commit.
///
/// This mirrors the Gazette shard Stat fan-out (list shards by task, then act on
/// each shard's primary). `list_req` selects the task's shards (e.g. by an
/// `id:prefix` or task-name label). Shards are synced concurrently; a per-shard
/// failure is recorded rather than aborting the batch. Errors only if the task
/// has no shards or the List itself fails.
pub async fn sync_task_shards(
    client: &Client,
    list_req: consumer::ListRequest,
) -> Result<SyncSummary, crate::Error> {
    let listing = client.list(list_req).await?;

    if listing.shards.is_empty() {
        return Err(crate::Error::Protocol("task has no shards"));
    }

    let outcomes = futures::future::join_all(listing.shards.into_iter().map(|shard| {
        let client = client.clone();
        async move {
            let shard_id = shard.spec.map(|s| s.id).unwrap_or_default();
            // Seed the route from the listing so the first attempt aims at the
            // shard's current primary.
            let header = shard.route.map(|route| broker::Header {
                route: Some(route),
                ..Default::default()
            });
            let result = sync_one_shard(&client, shard_id.clone(), header).await;
            (shard_id, result)
        }
    }))
    .await;

    let mut summary = SyncSummary::default();
    for (shard_id, result) in outcomes {
        match result {
            Ok(()) => summary.synced.push(shard_id),
            Err(reason) => summary.failed.push((shard_id, reason)),
        }
    }
    Ok(summary)
}

/// Drive SyncNow for a single shard to a terminal outcome, retrying to converge
/// on the primary (`NOT_SHARD_PRIMARY`) and to wait out a momentarily-absent
/// primary (`NO_SHARD_PRIMARY`), bounded by `MAX_RETRIES`.
async fn sync_one_shard(
    client: &Client,
    shard_id: String,
    mut header: Option<broker::Header>,
) -> Result<(), String> {
    use proto_flow::flow::sync_now_response::Status;

    const MAX_RETRIES: usize = 5;

    for attempt in 0..=MAX_RETRIES {
        let resp = client
            .sync_now(shard_id.clone(), &mut header)
            .await
            .map_err(|err| format!("{err}"))?;

        match resp.status() {
            Status::Ok => return Ok(()),
            // Route discovery: retry against the primary named by the response.
            Status::NotShardPrimary => header = resp.header,
            // Transient: the shard is momentarily without a primary. Back off.
            Status::NoShardPrimary if attempt != MAX_RETRIES => {
                header = resp.header;
                tokio::time::sleep(std::time::Duration::from_millis(200 * (attempt as u64 + 1)))
                    .await;
            }
            status => return Err(format!("{}", status.as_str_name())),
        }
    }
    Err("exhausted retries awaiting shard primary".to_string())
}

fn check_ok<R>(status: consumer::Status, r: R) -> Result<R, crate::Error> {
    if status == consumer::Status::Ok {
        Ok(r)
    } else {
        Err(crate::Error::ConsumerStatus(status))
    }
}
