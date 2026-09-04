//! In-process routing for a process which hosts a connector service.

use futures::{StreamExt, stream::BoxStream};
use tokio_stream::wrappers::ReceiverStream;

/// Issuer FQDN of a local service's self-signed tokens.
pub const LOCAL_ISSUER: &str = "local";

/// `Router` of a process which hosts its own connector `Service`.
#[derive(Clone)]
pub struct ServiceRouter {
    service: crate::Service,
    signer: proto_grpc::Signer,
}

impl ServiceRouter {
    pub fn new(service: crate::Service, signer: proto_grpc::Signer) -> Self {
        Self { service, signer }
    }
}

impl proto_grpc::connector::Router for ServiceRouter {
    fn open(
        &self,
        task_type: ops::TaskType,
        task_name: &str,
        request_rx: tokio::sync::mpsc::Receiver<crate::proto::Request>,
    ) -> BoxStream<'static, tonic::Result<crate::proto::Response>> {
        let metadata =
            match proto_grpc::connector::connector_bearer(&self.signer, task_type, task_name) {
                Ok(metadata) => metadata,
                Err(status) => return futures::stream::once(async { Err(status) }).boxed(),
            };

        ReceiverStream::new(self.service.spawn_connector(
            metadata,
            ReceiverStream::new(request_rx).map(tonic::Result::Ok),
        ))
        .boxed()
    }
}
