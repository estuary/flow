//! Top-level Shard service implementation.
//!
//! `Service` directly implements the controller-facing `Shard` trait.
//! "Controller" here is the peer that drives the shard's lifecycle: the
//! Go runtime in production, an in-process driver such as `flowctl
//! preview`, or a unit-test harness. From this crate's perspective the
//! controller is just the peer of the bidi stream that commands the
//! runtime and bounds its lifecycle.
//!
//! `Service` is monomorphized over its [`PublisherFactory`](crate::PublisherFactory)
//! `P`, so the publish path is statically dispatched.

use crate::proto;
use futures::Stream;
use tokio::sync::mpsc;
use tokio_stream::wrappers;

/// Service is the implementation of the controller-facing `Shard` gRPC
/// service trait, hosting one shard's transaction loop.
#[derive(Clone)]
pub struct Service<P: crate::PublisherFactory, L: crate::LoggerFactory> {
    pub plane: crate::Plane,
    pub container_network: String,
    pub set_log_level: Option<std::sync::Arc<dyn Fn(ops::LogLevel) + Send + Sync>>,
    pub task_name: String,
    pub publisher_factory: P,
    pub logger_factory: L,
    pub registry: service_kit::Registry,
    pub data_plane_signer: Option<proto_grpc::Signer>,
    /// When set, image connectors run remotely in the task's data plane through
    /// the connector proxy rather than as local containers (see
    /// [`RemoteConnectors`](crate::RemoteConnectors)). Set only by the catalog-test
    /// harness's agent path; `None` for production shards, `flowctl preview`, and
    /// `flowctl test` (all-local).
    pub remote_connectors: Option<std::sync::Arc<dyn crate::RemoteConnectors>>,
}

impl<P: crate::PublisherFactory, L: crate::LoggerFactory> Service<P, L> {
    /// Build a new Shard Service.
    /// - `plane`: the type of data plane in which this Service is operating.
    /// - `container_network`: the Docker container network used for connector containers.
    /// - `set_log_level`: callback for adjusting the log level implied by runtime requests.
    /// - `task_name`: name which is used to label any started connector containers.
    /// - `publisher_factory`: opens publishers for appending to collection partitions.
    /// - `logger_factory`: opens a Logger per session, which sinks connector
    ///   logs and reports runtime events.
    /// - `registry`: in-flight handler registry, shared with any co-hosted admin surface.
    pub fn new(
        plane: crate::Plane,
        container_network: String,
        set_log_level: Option<std::sync::Arc<dyn Fn(ops::LogLevel) + Send + Sync>>,
        task_name: String,
        publisher_factory: P,
        logger_factory: L,
        registry: service_kit::Registry,
        data_plane_signer: Option<proto_grpc::Signer>,
    ) -> Self {
        Self {
            plane,
            container_network,
            set_log_level,
            task_name,
            publisher_factory,
            logger_factory,
            registry,
            data_plane_signer,
            remote_connectors: None,
        }
    }

    /// Configure this service to run image connectors remotely through the
    /// connector proxy (see [`RemoteConnectors`](crate::RemoteConnectors)).
    /// Consuming builder so it composes with the `new` constructor; the harness
    /// runner is the only caller.
    pub fn with_remote_connectors(
        mut self,
        remote_connectors: Option<std::sync::Arc<dyn crate::RemoteConnectors>>,
    ) -> Self {
        self.remote_connectors = remote_connectors;
        self
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

    pub fn spawn_capture<R>(
        &self,
        controller_rx: R,
    ) -> mpsc::UnboundedReceiver<tonic::Result<proto::Capture>>
    where
        R: Stream<Item = tonic::Result<proto::Capture>> + Send + Unpin + 'static,
    {
        let service = self.clone();
        let (controller_tx, response_rx) =
            mpsc::unbounded_channel::<tonic::Result<proto::Capture>>();
        let error_tx = controller_tx.clone();

        tokio::spawn(async move {
            if let Err(err) = super::capture::serve(service, controller_rx, controller_tx).await {
                let _ = error_tx.send(Err(crate::anyhow_to_status(err)));
            }
        });
        response_rx
    }

    pub fn spawn_derive<R>(
        &self,
        controller_rx: R,
    ) -> mpsc::UnboundedReceiver<tonic::Result<proto::Derive>>
    where
        R: Stream<Item = tonic::Result<proto::Derive>> + Send + Unpin + 'static,
    {
        let service = self.clone();
        let (controller_tx, response_rx) =
            mpsc::unbounded_channel::<tonic::Result<proto::Derive>>();
        let error_tx = controller_tx.clone();

        tokio::spawn(async move {
            if let Err(err) = super::derive::serve(service, controller_rx, controller_tx).await {
                let _ = error_tx.send(Err(crate::anyhow_to_status(err)));
            }
        });
        response_rx
    }
}

#[tonic::async_trait]
impl<P: crate::PublisherFactory, L: crate::LoggerFactory> proto_grpc::runtime::shard_server::Shard
    for Service<P, L>
{
    type CaptureStream = wrappers::UnboundedReceiverStream<tonic::Result<proto::Capture>>;
    type DeriveStream = wrappers::UnboundedReceiverStream<tonic::Result<proto::Derive>>;
    type MaterializeStream = wrappers::UnboundedReceiverStream<tonic::Result<proto::Materialize>>;

    async fn materialize(
        &self,
        request: tonic::Request<tonic::Streaming<proto::Materialize>>,
    ) -> tonic::Result<tonic::Response<Self::MaterializeStream>> {
        Ok(tonic::Response::new(
            wrappers::UnboundedReceiverStream::new(self.spawn_materialize(request.into_inner())),
        ))
    }

    async fn capture(
        &self,
        request: tonic::Request<tonic::Streaming<proto::Capture>>,
    ) -> tonic::Result<tonic::Response<Self::CaptureStream>> {
        Ok(tonic::Response::new(
            wrappers::UnboundedReceiverStream::new(self.spawn_capture(request.into_inner())),
        ))
    }

    async fn derive(
        &self,
        request: tonic::Request<tonic::Streaming<proto::Derive>>,
    ) -> tonic::Result<tonic::Response<Self::DeriveStream>> {
        Ok(tonic::Response::new(
            wrappers::UnboundedReceiverStream::new(self.spawn_derive(request.into_inner())),
        ))
    }
}
