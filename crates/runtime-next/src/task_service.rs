//! CGO entry point: binds a UDS, registers the `Shard` gRPC service, and
//! serves until cancellation. Modeled on `crates/runtime/src/task_service.rs`,
//! adapted to the runtime-next service set (just `Shard`).

use crate::{LogHandler, Runtime, TokioContext};
use anyhow::Context;
use futures::FutureExt;
use futures::channel::oneshot;
use proto_flow::runtime::{Plane, TaskServiceConfig};

pub struct TaskService {
    cancel_tx: oneshot::Sender<()>,
    tokio_context: TokioContext,
    server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl TaskService {
    pub fn new(config: TaskServiceConfig, log_file: std::fs::File) -> anyhow::Result<Self> {
        let TaskServiceConfig {
            log_file_fd: _,
            task_name,
            uds_path,
            container_network,
            plane,
            data_plane_fqdn,
            data_plane_signing_key,
            control_api_endpoint,
            availability_zone,
        } = config;

        if !std::path::Path::new(&uds_path).is_absolute() {
            anyhow::bail!("uds_path must be an absolute filesystem path");
        }

        let log_handler = ::ops::new_encoded_json_write_handler(std::sync::Arc::new(
            std::sync::Mutex::new(log_file),
        ));
        let tokio_context = TokioContext::new(
            ops::LogLevel::Warn,
            log_handler.clone(),
            task_name.clone(),
            1,
        );

        let control_api_endpoint: url::Url =
            url::Url::parse(&control_api_endpoint).context("invalid control API endpoint URL")?;

        let publisher_factory =
            flow_client_next::workflows::task_collection_auth::new_journal_client_factory(
                flow_client_next::rest::Client::new(&control_api_endpoint, "task-service"),
                proto_gazette::capability::APPEND | proto_gazette::capability::APPLY,
                gazette::Router::new(&availability_zone),
                data_plane_fqdn,
                tokens::jwt::EncodingKey::from_secret(&data_plane_signing_key),
            );

        std::mem::drop(data_plane_signing_key);

        let runtime = Runtime::new(
            Plane::try_from(plane).context("invalid TaskServiceConfig.plane")?,
            container_network,
            log_handler,
            Some(tokio_context.set_log_level_fn()),
            task_name,
            publisher_factory,
        );

        let uds = tokio_context
            .block_on(async move { tokio::net::UnixListener::bind(uds_path) })
            .context("failed to bind task service unix domain socket")?;
        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();

        let uds_stream = futures::stream::try_unfold(uds, move |uds| async move {
            let (conn, addr) = uds.accept().await?;
            tracing::debug!(?addr, "accepted new unix socket connection");
            Ok::<_, std::io::Error>(Some((conn, uds)))
        });

        let server =
            build_tonic_server(runtime).serve_with_incoming_shutdown(uds_stream, async move {
                _ = cancel_rx.await;
            });
        let server = tokio_context.spawn(server);

        Ok(Self {
            cancel_tx,
            tokio_context,
            server,
        })
    }

    pub fn graceful_stop(self) {
        let Self {
            cancel_tx,
            tokio_context,
            server,
        } = self;

        _ = cancel_tx.send(());

        let log = match tokio_context.block_on(server) {
            Err(panic) => async move {
                tracing::error!(?panic, "task gRPC service exited with panic");
            }
            .boxed(),
            Ok(Err(error)) => async move {
                tracing::error!(?error, "task gRPC service exited with error");
            }
            .boxed(),
            Ok(Ok(())) => async move {
                tracing::debug!("task gRPC service stopped gracefully");
            }
            .boxed(),
        };
        let () = tokio_context.block_on(tokio_context.spawn(log)).unwrap();
    }
}

fn build_tonic_server<L: LogHandler>(runtime: Runtime<L>) -> tonic::transport::server::Router {
    tonic::transport::Server::builder().add_service(
        proto_grpc::runtime::shard_server::ShardServer::new(runtime)
            .max_decoding_message_size(usize::MAX) // Up from 4MB. Accept whatever the controller sends.
            .max_encoding_message_size(usize::MAX),
    )
}
