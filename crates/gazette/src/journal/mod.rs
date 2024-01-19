use futures::FutureExt;
use std::sync::Arc;

mod read;

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
