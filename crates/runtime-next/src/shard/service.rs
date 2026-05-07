//! Top-level Shard service implementation.
//!
//! `Service` directly implements the controller-facing `Shard` trait.
//! "Controller" here is the peer that drives the shard's lifecycle: the
//! Go runtime in production, an in-process driver such as `flowctl
//! preview`, or a unit-test harness. From this crate's perspective the
//! controller is just the peer of the bidi stream that commands the
//! runtime and bounds its lifecycle.

use crate::proto;
use futures::Stream;
use tokio::sync::mpsc;
use tokio_stream::wrappers;

/// Service is the implementation of the controller-facing `Shard` gRPC
/// service trait, hosting one shard's transaction loop.
#[derive(Clone)]
pub struct Service<L: crate::LogHandler> {
    pub plane: crate::Plane,
    pub container_network: String,
    pub log_handler: L,
    pub set_log_level: Option<std::sync::Arc<dyn Fn(ops::LogLevel) + Send + Sync>>,
    pub task_name: String,
    pub publisher_factory: gazette::journal::ClientFactory,
}

impl<L: crate::LogHandler> Service<L> {
    /// Build a new Shard Service.
    /// - `plane`: the type of data plane in which this Service is operating.
    /// - `container_network`: the Docker container network used for connector containers.
    /// - `log_handler`: handler to which connector logs are dispatched.
    /// - `set_log_level`: callback for adjusting the log level implied by runtime requests.
    /// - `task_name`: name which is used to label any started connector containers.
    /// - `publisher_factory`: client factory for creating and appending to collection partitions.
    pub fn new(
        plane: crate::Plane,
        container_network: String,
        log_handler: L,
        set_log_level: Option<std::sync::Arc<dyn Fn(ops::LogLevel) + Send + Sync>>,
        task_name: String,
        publisher_factory: gazette::journal::ClientFactory,
    ) -> Self {
        Self {
            plane,
            container_network,
            log_handler,
            set_log_level,
            task_name,
            publisher_factory,
        }
    }

    /// Wrap this service in its typed tonic server, for composition
    /// with sibling services on a `tonic::transport::Server::builder()`.
    pub fn into_tonic_service(self) -> proto_grpc::runtime::shard_server::ShardServer<Self> {
        proto_grpc::runtime::shard_server::ShardServer::new(self)
            .max_decoding_message_size(crate::MAX_MESSAGE_SIZE)
            .max_encoding_message_size(usize::MAX)
    }

    /// Apply the dynamic log level if a setter was provided.
    pub fn set_log_level(&self, level: ops::LogLevel) {
        if level == ops::LogLevel::UndefinedLevel {
            // No-op
        } else if let Some(set_log_level) = &self.set_log_level {
            (set_log_level)(level);
        }
    }

    pub fn spawn_materialize<R>(
        &self,
        controller_rx: R,
    ) -> mpsc::UnboundedReceiver<tonic::Result<proto::Materialize>>
    where
        R: Stream<Item = tonic::Result<proto::Materialize>> + Send + Unpin + 'static,
    {
        let service = self.clone();
        let (controller_tx, response_rx) =
            mpsc::unbounded_channel::<tonic::Result<proto::Materialize>>();
        let error_tx = controller_tx.clone();

        tokio::spawn(async move {
            if let Err(err) = super::materialize::serve(service, controller_rx, controller_tx).await
            {
                let _ = error_tx.send(Err(crate::anyhow_to_status(err)));
            }
        });
        response_rx
    }
}

#[tonic::async_trait]
impl<L: crate::LogHandler> proto_grpc::runtime::shard_server::Shard for Service<L> {
    type MaterializeStream = wrappers::UnboundedReceiverStream<tonic::Result<proto::Materialize>>;
    //type DeriveStream = wrappers::ReceiverStream<tonic::Result<proto::Derive>>;

    async fn materialize(
        &self,
        request: tonic::Request<tonic::Streaming<proto::Materialize>>,
    ) -> tonic::Result<tonic::Response<Self::MaterializeStream>> {
        Ok(tonic::Response::new(
            wrappers::UnboundedReceiverStream::new(self.spawn_materialize(request.into_inner())),
        ))
    }

    /*
    async fn derive(
        &self,
        _request: tonic::Request<tonic::Streaming<proto::Derive>>,
    ) -> tonic::Result<tonic::Response<Self::DeriveStream>> {
        Err(tonic::Status::unimplemented(
            "Shard.Derive: not in scope for the materialize phase",
        ))
    }
    */
}
