use futures::FutureExt;
use proto_gazette::broker;
use std::sync::Arc;

mod read;

mod read_docs;
pub use read_docs::{Read, ReadDocs};

// Sub is the routed sub-client of Client.
type Sub = proto_grpc::broker::journal_client::JournalClient<
    tonic::service::interceptor::InterceptedService<
        tonic::transport::Channel,
        crate::interceptor::Interceptor,
    >,
>;
pub type Router = crate::Router<Sub>;

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

    pub async fn list(
        &self,
        req: broker::ListRequest,
    ) -> Result<broker::ListResponse, crate::Error> {
        let mut client = self.router.route(None, false).await?;

        let resp = client
            .list(req)
            .await
            .map_err(crate::Error::Grpc)?
            .into_inner();

        check_ok(resp.status(), resp)
    }
}

impl crate::Router<Sub> {
    pub fn new(
        endpoint: &str,
        interceptor: crate::Interceptor,
        zone: &str,
    ) -> Result<Self, crate::Error> {
        Router::delegated_new(
            move |endpoint| {
                let interceptor = interceptor.clone();

                async move {
                    let channel = endpoint
                        .connect_timeout(std::time::Duration::from_secs(5))
                        .connect()
                        .await?;
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
