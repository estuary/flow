use crate::router;
use proto_gazette::{broker, consumer};
use tonic::transport::Channel;

// SubClient is the routed sub-client of Client.
type SubClient = proto_grpc::consumer::shard_client::ShardClient<
    tonic::service::interceptor::InterceptedService<Channel, crate::Metadata>,
>;

#[derive(Clone)]
pub struct Client {
    default: broker::process_spec::Id,
    metadata: crate::Metadata,
    router: crate::Router,
}

impl Client {
    /// Build a Client which dispatches request to the given default endpoint with the given Metadata.
    /// The provider Router enables re-use of connections to consumers.
    pub fn new(endpoint: String, metadata: crate::Metadata, router: crate::Router) -> Self {
        Self {
            default: broker::process_spec::Id {
                zone: String::new(),
                suffix: endpoint,
            },
            metadata,
            router,
        }
    }

    pub fn into_parts(self) -> (String, crate::Metadata, crate::Router) {
        (self.default.suffix, self.metadata, self.router)
    }

    /// Invoke the Gazette shard List RPC.
    pub async fn list(
        &self,
        req: consumer::ListRequest,
    ) -> Result<consumer::ListResponse, crate::Error> {
        let mut client = self.into_sub(self.router.route(
            None,
            router::Mode::Default,
            &self.default,
        )?);

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
        let mut client = self.into_sub(self.router.route(
            None,
            router::Mode::Default,
            &self.default,
        )?);

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
        let mut client = self.into_sub(self.router.route(
            None,
            router::Mode::Default,
            &self.default,
        )?);

        let resp = client
            .unassign(req)
            .await
            .map_err(crate::Error::Grpc)?
            .into_inner();

        check_ok(resp.status(), resp)
    }

    /// Invoke the Gazette shard GetHints RPC.
    pub async fn get_hints(
        &self,
        req: consumer::GetHintsRequest,
    ) -> Result<consumer::GetHintsResponse, crate::Error> {
        let mut client = self.into_sub(self.router.route(
            None,
            router::Mode::Default,
            &self.default,
        )?);

        let resp = client
            .get_hints(req)
            .await
            .map_err(crate::Error::Grpc)?
            .into_inner();

        check_ok(resp.status(), resp)
    }

    fn into_sub(&self, (channel, _local): (Channel, bool)) -> SubClient {
        proto_grpc::consumer::shard_client::ShardClient::with_interceptor(
            channel,
            self.metadata.clone(),
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
