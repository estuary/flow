//! The `connector.Connector` gRPC service, and its in-process entry point.
//!
//! **Only the `spawn_*` adapters below may put an `Err` on a response stream,
//! and only after `serve` has returned.** Failing a stream means returning the
//! error up the stack; the adapter then reports it, so a terminal `Status` is
//! unambiguously the stream's last word.

use crate::proto;
use futures::Stream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

/// Service implements the `connector.Connector` gRPC service: it starts and
/// drives connectors on behalf of an authorized caller.
#[derive(Clone)]
pub struct Service(std::sync::Arc<ServiceImpl>);

pub struct ServiceImpl {
    /// Type of data plane in which connectors run, which gates local
    /// connectors and non-Estuary images.
    pub(crate) plane: crate::Plane,
    /// Docker network attached to connector containers.
    pub(crate) container_network: String,
    /// Authenticates the `PROXY_CONNECTOR` bearer of every stream, in-process
    /// and over the wire alike.
    pub(crate) authenticator: proto_grpc::Authenticator,
    /// Advertised in `Started.process`, telling a caller which reactor its
    /// connector landed on. `None` in local contexts.
    pub(crate) process: Option<proto_gazette::broker::ProcessSpec>,
    /// Registry of in-flight handlers, for the admin surface.
    pub(crate) registry: service_kit::Registry,
}

impl std::ops::Deref for Service {
    type Target = ServiceImpl;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Service {
    pub fn new(
        plane: crate::Plane,
        container_network: String,
        authenticator: proto_grpc::Authenticator,
        process: Option<proto_gazette::broker::ProcessSpec>,
        registry: service_kit::Registry,
    ) -> Self {
        Self(std::sync::Arc::new(ServiceImpl {
            plane,
            container_network,
            authenticator,
            process,
            registry,
        }))
    }

    /// Build a `Plane::Local` Service and its [`ServiceRouter`](crate::ServiceRouter)
    /// which reaches it. The pair shares a throwaway random HMAC key, so they
    /// cannot be mismatched: authorization is still exercised end-to-end
    /// exactly as in production, but no key is configured or shared.
    pub fn new_local(
        container_network: String,
        registry: service_kit::Registry,
    ) -> (Self, crate::ServiceRouter) {
        let key: [u8; 32] = rand::random();

        let service = Self::new(
            crate::Plane::Local,
            container_network,
            proto_grpc::Authenticator::new(
                crate::router::LOCAL_ISSUER.to_string(),
                vec![tokens::jwt::DecodingKey::from_secret(&key)],
            ),
            None, // No dial-able process in a local context.
            registry,
        );
        let signer = proto_grpc::Signer::new(
            crate::router::LOCAL_ISSUER.to_string(),
            tokens::jwt::EncodingKey::from_secret(&key),
        );

        let router = crate::ServiceRouter::new(service.clone(), signer);
        (service, router)
    }

    /// Wrap this service in its typed tonic server, for composition
    /// with sibling services on a `tonic::transport::Server::builder()`.
    pub fn into_tonic_service(
        self,
    ) -> proto_grpc::connector::connector_server::ConnectorServer<Self> {
        proto_grpc::connector::connector_server::ConnectorServer::new(self)
            .max_decoding_message_size(proto_grpc::MAX_MESSAGE_SIZE)
            .max_encoding_message_size(usize::MAX)
    }

    /// In-process entry point, with no wire hop and no protobuf ser/deser.
    /// `metadata` bears an `Authorization: Bearer` which is authenticated
    /// exactly as the gRPC path does, and then authorized against the stream's
    /// first request. There is no disarm.
    pub fn spawn_connector<R>(
        &self,
        metadata: proto_grpc::Metadata,
        request_rx: R,
    ) -> mpsc::Receiver<tonic::Result<proto::Response>>
    where
        R: Stream<Item = tonic::Result<proto::Request>> + Send + Unpin + 'static,
    {
        let (response_tx, response_rx) = mpsc::channel(proto_grpc::CHANNEL_BUFFER);

        match self
            .authenticator
            .authenticate(&metadata.0, proto_flow::capability::PROXY_CONNECTOR)
        {
            Ok(verified) => self.spawn_verified(verified, request_rx, response_tx),
            Err(status) => {
                _ = response_tx.try_send(Err(status)); // The ONLY send of Err.
            }
        }
        response_rx
    }

    /// Shared tail of both entry points: run `serve` under the verified claims,
    /// reporting its outcome on the response channel.
    fn spawn_verified<R>(
        &self,
        verified: tokens::jwt::Verified<proto_gazette::Claims>,
        request_rx: R,
        response_tx: mpsc::Sender<tonic::Result<proto::Response>>,
    ) where
        R: Stream<Item = tonic::Result<proto::Request>> + Send + Unpin + 'static,
    {
        let service = self.clone();
        let error_tx = response_tx.clone();

        tokio::spawn(async move {
            let handler = crate::serve::serve(service, verified, request_rx, response_tx);
            if let Err(status) = proto_grpc::catch_panic(handler).await {
                let _ = error_tx.send(Err(status)).await; // The ONLY send of Err.
            }
        });
    }
}

#[tonic::async_trait]
impl proto_grpc::connector::connector_server::Connector for Service {
    type ConnectorStream = ReceiverStream<tonic::Result<proto::Response>>;

    async fn connector(
        &self,
        request: tonic::Request<tonic::Streaming<proto::Request>>,
    ) -> tonic::Result<tonic::Response<Self::ConnectorStream>> {
        // There's just this one RPC, so the handler authenticates directly
        // rather than through an interceptor layer.
        let verified = self
            .authenticator
            .authenticate(request.metadata(), proto_flow::capability::PROXY_CONNECTOR)?;

        let (response_tx, response_rx) = mpsc::channel(proto_grpc::CHANNEL_BUFFER);
        self.spawn_verified(verified, request.into_inner(), response_tx);

        Ok(tonic::Response::new(ReceiverStream::new(response_rx)))
    }
}
