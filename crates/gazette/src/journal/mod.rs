use futures::FutureExt;
use proto_gazette::broker;
use std::sync::Arc;

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

    pub async fn list_fragments(
        &self,
        req: broker::FragmentsRequest,
    ) -> Result<broker::FragmentsResponse, crate::Error> {
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

async fn backoff(attempt: u32) {
    use std::time::Duration;

    if attempt == 0 {
        return;
    }
    let dur = match attempt {
        1 | 2 => Duration::from_millis(50),
        3 | 4 => Duration::from_millis(100),
        5 | 6 => Duration::from_secs(1),
        _ => Duration::from_secs(5),
    };
    tokio::time::sleep(dur).await;
}
