use crate::router::connect_unix;
use futures::FutureExt;
use proto_gazette::consumer;
use std::sync::Arc;
use tonic::transport::Uri;

// SubClient is the routed sub-client of Client.
type SubClient = proto_grpc::consumer::shard_client::ShardClient<
    tonic::service::interceptor::InterceptedService<tonic::transport::Channel, crate::auth::Auth>,
>;
pub type Router = crate::Router<SubClient>;

#[derive(Clone)]
pub struct Client {
    router: Arc<Router>,
}

impl Client {
    pub fn new(router: Router) -> Self {
        Self {
            router: Arc::new(router),
        }
    }

    pub async fn list(
        &self,
        req: consumer::ListRequest,
    ) -> Result<consumer::ListResponse, crate::Error> {
        let mut client = self.router.route(None, false).await?;

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
        let mut client = self.router.route(None, false).await?;

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
        let mut client = self.router.route(None, false).await?;

        let resp = client
            .unassign(req)
            .await
            .map_err(crate::Error::Grpc)?
            .into_inner();

        check_ok(resp.status(), resp)
    }
}

impl crate::Router<SubClient> {
    pub fn new(endpoint: &str, interceptor: crate::Auth, zone: &str) -> Result<Self, crate::Error> {
        Router::delegated_new(
            move |endpoint| {
                let interceptor = interceptor.clone();

                async move {
                    let endpoint = &endpoint.connect_timeout(std::time::Duration::from_secs(5));
                    let channel = if endpoint.uri().scheme_str() == Some("unix") {
                        endpoint
                            .connect_with_connector(tower::util::service_fn(move |uri: Uri| {
                                connect_unix(uri)
                            }))
                            .await?
                    } else {
                        endpoint.connect().await?
                    };
                    Ok(
                        proto_grpc::consumer::shard_client::ShardClient::with_interceptor(
                            channel,
                            interceptor.clone(),
                        ),
                    )
                }
                .boxed()
            },
            endpoint,
            zone,
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
