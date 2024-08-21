use proto_gazette::consumer;
use std::sync::Arc;
use tonic::transport::Channel;

// SubClient is the routed sub-client of Client.
type SubClient = proto_grpc::consumer::shard_client::ShardClient<
    tonic::service::interceptor::InterceptedService<Channel, crate::Auth>,
>;

#[derive(Clone)]
pub struct Client {
    auth: crate::Auth,
    router: Arc<crate::Router>,
}

impl Client {
    pub fn new(router: crate::Router, auth: crate::Auth) -> Self {
        Self {
            auth,
            router: Arc::new(router),
        }
    }

    pub async fn list(
        &self,
        req: consumer::ListRequest,
    ) -> Result<consumer::ListResponse, crate::Error> {
        let mut client = self.into_sub(self.router.route(None, false).await?);

        let resp = client
            .list(req)
            .await
            .map_err(crate::Error::Grpc)?
            .into_inner();

        check_ok(resp.status(), resp)
    }

    pub async fn apply(
        &self,
        req: consumer::ApplyRequest,
    ) -> Result<consumer::ApplyResponse, crate::Error> {
        let mut client = self.into_sub(self.router.route(None, false).await?);

        let resp = client
            .apply(req)
            .await
            .map_err(crate::Error::Grpc)?
            .into_inner();

        check_ok(resp.status(), resp)
    }

    pub async fn unassign(
        &self,
        req: consumer::UnassignRequest,
    ) -> Result<consumer::UnassignResponse, crate::Error> {
        let mut client = self.into_sub(self.router.route(None, false).await?);

        let resp = client
            .unassign(req)
            .await
            .map_err(crate::Error::Grpc)?
            .into_inner();

        check_ok(resp.status(), resp)
    }

    fn into_sub(&self, channel: Channel) -> SubClient {
        proto_grpc::consumer::shard_client::ShardClient::with_interceptor(
            channel,
            self.auth.clone(),
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
