use crate::router;
use proto_gazette::broker;
use tonic::transport::Channel;

mod append;
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
    default: broker::process_spec::Id,
    metadata: crate::Metadata,
    router: crate::Router,
}

impl Client {
    /// Build a Client which dispatches request to the given default endpoint with the given Metadata.
    /// The provider Router enables re-use of connections to brokers.
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

    /// Invoke the Gazette journal Apply API.
    pub async fn apply(&self, req: broker::ApplyRequest) -> crate::Result<broker::ApplyResponse> {
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

    /// Invoke the Gazette journal ListFragments API.
    pub async fn list_fragments(
        &self,
        req: broker::FragmentsRequest,
    ) -> crate::Result<broker::FragmentsResponse> {
        let mut client = self.into_sub(self.router.route(
            None,
            router::Mode::Default,
            &self.default,
        )?);

        let resp = client
            .list_fragments(req)
            .await
            .map_err(crate::Error::Grpc)?
            .into_inner();

        check_ok(resp.status(), resp)
    }

    fn into_sub(&self, (channel, _local): (Channel, bool)) -> SubClient {
        proto_grpc::broker::journal_client::JournalClient::with_interceptor(
            channel,
            self.metadata.clone(),
        )
        // TODO(johnny): Use `_local` to selectively enable LZ4 compression
        // when traversing a non-local zone.
    }
}

fn check_ok<R>(status: broker::Status, r: R) -> Result<R, crate::Error> {
    if status == broker::Status::Ok {
        Ok(r)
    } else {
        Err(crate::Error::BrokerStatus(status))
    }
}
