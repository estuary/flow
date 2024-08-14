use crate::router::connect_unix;
use futures::FutureExt;
use proto_gazette::broker;
use std::sync::Arc;
use tonic::transport::Uri;

mod list;
mod read;

mod read_json_lines;
pub use read_json_lines::{ReadJsonLine, ReadJsonLines};

// SubClient is the routed sub-client of Client.
type SubClient = proto_grpc::broker::journal_client::JournalClient<
    tonic::service::interceptor::InterceptedService<tonic::transport::Channel, crate::auth::Auth>,
>;
pub type Router = crate::Router<SubClient>;

#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    router: Arc<Router>,
}

impl Client {
    pub fn new(http: reqwest::Client, router: Router) -> Self {
        Self {
            http,
            router: Arc::new(router),
        }
    }

    pub async fn apply(&self, req: broker::ApplyRequest) -> crate::Result<broker::ApplyResponse> {
        let mut client = self.router.route(None, false).await?;

        let resp = client
            .apply(req)
            .await
            .map_err(crate::Error::Grpc)?
            .into_inner();

        check_ok(resp.status(), resp)
    }

    pub async fn list_fragments(
        &self,
        req: broker::FragmentsRequest,
    ) -> crate::Result<broker::FragmentsResponse> {
        let mut client = self.router.route(None, false).await?;

        let resp = client
            .list_fragments(req)
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
                            .connect_with_connector(tower::util::service_fn(|uri: Uri| {
                                connect_unix(uri)
                            }))
                            .await?
                    } else {
                        endpoint.connect().await?
                    };
                    Ok(
                        proto_grpc::broker::journal_client::JournalClient::with_interceptor(
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

fn check_ok<R>(status: broker::Status, r: R) -> Result<R, crate::Error> {
    if status == broker::Status::Ok {
        Ok(r)
    } else {
        Err(crate::Error::BrokerStatus(status))
    }
}
