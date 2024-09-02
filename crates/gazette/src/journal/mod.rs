use proto_gazette::broker;
use tonic::transport::Channel;

mod list;
mod read;

mod read_json_lines;
pub use read_json_lines::{ReadJsonLine, ReadJsonLines};

// SubClient is the routed sub-client of Client.
type SubClient = proto_grpc::broker::journal_client::JournalClient<
    tonic::service::interceptor::InterceptedService<Channel, crate::Metadata>,
>;

#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    metadata: crate::Metadata,
    router: crate::Router,
}

impl Client {
    pub fn new(http: reqwest::Client, router: crate::Router, metadata: crate::Metadata) -> Self {
        Self {
            metadata,
            http,
            router,
        }
    }

    pub async fn apply(&self, req: broker::ApplyRequest) -> crate::Result<broker::ApplyResponse> {
        let mut client = self.into_sub(self.router.route(None, false).await?);

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
        let mut client = self.into_sub(self.router.route(None, false).await?);

        let resp = client
            .list_fragments(req)
            .await
            .map_err(crate::Error::Grpc)?
            .into_inner();

        check_ok(resp.status(), resp)
    }

    fn into_sub(&self, channel: Channel) -> SubClient {
        proto_grpc::broker::journal_client::JournalClient::with_interceptor(
            channel,
            self.metadata.clone(),
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
