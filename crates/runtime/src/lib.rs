use futures::TryStreamExt;
use std::sync::Arc;

mod capture;
mod container;
mod derive;
mod image_connector;
mod local_connector;
mod materialize;
mod task_service;
mod tokio_context;
mod unary;
mod unseal;

pub use task_service::TaskService;
pub use tokio_context::TokioContext;

// This constant is shared between Rust and Go code.
// See go/protocols/flow/document_extensions.go.
pub const UUID_PLACEHOLDER: &str = "DocUUIDPlaceholder-329Bb50aa48EAa9ef";

/// CHANNEL_BUFFER is the standard buffer size used for holding documents in an
/// asynchronous processing pipeline. User documents can be large -- up to 64MB --
/// so this value should be small. At the same time, processing steps such as
/// schema validation are greatly accelerated when they can loop over multiple
/// documents without yielding, so it should not be *too* small.
pub const CHANNEL_BUFFER: usize = 8;

fn anyhow_to_status(err: anyhow::Error) -> tonic::Status {
    tonic::Status::internal(format!("{err:?}"))
}

fn stream_error_to_status<T, S: futures::Stream<Item = anyhow::Result<T>>>(
    s: S,
) -> impl futures::Stream<Item = tonic::Result<T>> {
    s.map_err(|err: anyhow::Error| match err.downcast::<tonic::Status>() {
        Ok(status) => status,
        Err(err) => anyhow_to_status(err),
    })
}

fn stream_status_to_error<T, S: futures::Stream<Item = tonic::Result<T>>>(
    s: S,
) -> impl futures::Stream<Item = anyhow::Result<T>> {
    s.map_err(anyhow::Error::new)
}

/// Runtime implements the various services that constitute the Flow Runtime.
#[derive(Clone)]
pub struct Runtime<L>
where
    L: Fn(&ops::Log) + Send + Sync + Clone + 'static,
{
    allow_local: bool,
    container_network: String,
    log_handler: L,
    set_log_level: Option<Arc<dyn Fn(ops::LogLevel) + Send + Sync>>,
    task_name: String,
}

impl<L> Runtime<L>
where
    L: Fn(&ops::Log) + Send + Sync + Clone + 'static,
{
    /// Build a new Runtime.
    /// * `allow_local`: Whether local connectors are permitted by this Runtime.
    /// * `container_network`: the Docker container network used for connector containers.
    /// * `log_handler`: handler to which connector logs are dispatched.
    /// * `set_log_level`: callback for adjusting the log level implied by runtime requests.
    /// * `task_name`: name which is used to label any started connector containers.
    pub fn new(
        allow_local: bool,
        container_network: String,
        log_handler: L,
        set_log_level: Option<Arc<dyn Fn(ops::LogLevel) + Send + Sync>>,
        task_name: String,
    ) -> Self {
        Self {
            allow_local,
            container_network,
            log_handler,
            set_log_level,
            task_name,
        }
    }

    /// Build a tonic Server which includes all of the Runtime's services.
    pub fn build_tonic_server(self) -> tonic::transport::server::Router {
        tonic::transport::Server::builder()
            .add_service(
                proto_grpc::capture::connector_server::ConnectorServer::new(self.clone())
                    .max_decoding_message_size(usize::MAX) // Up from 4MB. Accept whatever the Go runtime sends.
                    .max_encoding_message_size(usize::MAX), // The default, made explicit.
            )
            .add_service(
                proto_grpc::derive::connector_server::ConnectorServer::new(self.clone())
                    .max_decoding_message_size(usize::MAX) // Up from 4MB. Accept whatever the Go runtime sends.
                    .max_encoding_message_size(usize::MAX), // The default, made explicit.
            )
            .add_service(
                proto_grpc::materialize::connector_server::ConnectorServer::new(self)
                    .max_decoding_message_size(usize::MAX) // Up from 4MB. Accept whatever the Go runtime sends.
                    .max_encoding_message_size(usize::MAX), // The default, made explicit.
            )
    }
}
