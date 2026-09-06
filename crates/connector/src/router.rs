//! In-process routing for a process which hosts a connector service.

use tokio::sync::mpsc;

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

    /// Signer minting this router's bearers, for tests which address a
    /// `Service` directly rather than through the router.
    #[cfg(test)]
    pub(crate) fn signer(&self) -> &proto_grpc::Signer {
        &self.signer
    }
}

impl proto_grpc::connector::Router for ServiceRouter {
    fn open(
        &self,
        task_type: ops::TaskType,
        task_name: &str,
        request_rx: mpsc::Receiver<crate::proto::Request>,
    ) -> mpsc::Receiver<tonic::Result<crate::proto::Response>> {
        let metadata =
            match proto_grpc::connector::connector_bearer(&self.signer, task_type, task_name) {
                Ok(metadata) => metadata,
                Err(status) => {
                    let (response_tx, response_rx) = mpsc::channel(proto_grpc::CHANNEL_BUFFER);
                    response_tx
                        .try_send(Err(status))
                        .expect("new response channel is open and empty");
                    return response_rx;
                }
            };

        self.service.spawn_connector(metadata, request_rx)
    }
}
