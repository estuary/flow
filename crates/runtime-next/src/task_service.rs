//! CGO entry point: binds a UDS, registers the `Shard` and `Connector` gRPC
//! services, and serves until cancellation.
use crate::{proto, shard};
use anyhow::Context;
use futures::FutureExt;
use futures::channel::oneshot;

pub struct TaskService {
    cancel_tx: oneshot::Sender<()>,
    tokio_context: crate::TokioContext,
    server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl TaskService {
    pub fn new(config: proto::TaskServiceConfig, log_file: std::fs::File) -> anyhow::Result<Self> {
        let proto::TaskServiceConfig {
            log_file_fd: _,
            task_name,
            uds_path,
            container_network,
            plane,
            process,
        } = config;

        if !std::path::Path::new(&uds_path).is_absolute() {
            anyhow::bail!("uds_path must be an absolute filesystem path");
        }
        let plane =
            crate::proto::Plane::try_from(plane).context("invalid TaskServiceConfig.plane")?;

        // Data-plane configuration variables:
        let data_plane_fqdn =
            std::env::var("FLOW_DATA_PLANE_FQDN").context("FLOW_DATA_PLANE_FQDN not set")?;
        let control_api_endpoint =
            std::env::var("FLOW_CONTROL_API").context("FLOW_CONTROL_API not set")?;
        let availability_zone = std::env::var("CONSUMER_ZONE").context("CONSUMER_ZONE not set")?;

        // Every key verifies (supporting rotation); the first also signs.
        let consumer_auth_keys =
            std::env::var("CONSUMER_AUTH_KEYS").context("CONSUMER_AUTH_KEYS not set")?;
        let (data_plane_signing_key, data_plane_verify_keys) =
            tokens::jwt::parse_base64_hmac_keys_str(&consumer_auth_keys)
                .context("parsing CONSUMER_AUTH_KEYS")?;

        let log_handler = ::ops::new_encoded_json_write_handler(std::sync::Arc::new(
            std::sync::Mutex::new(log_file),
        ));
        let tokio_context = crate::TokioContext::new(
            ops::LogLevel::Warn,
            log_handler.clone(),
            task_name.clone(),
            1,
        );

        let control_api_endpoint: url::Url =
            url::Url::parse(&control_api_endpoint).context("invalid control API endpoint URL")?;

        use proto_gazette::capability::{APPEND, APPLY, LIST};
        let publisher_factory =
            flow_client_next::workflows::task_collection_auth::new_journal_client_factory(
                flow_client_next::rest::Client::new(&control_api_endpoint, "task-service"),
                APPEND | APPLY | LIST,
                gazette::Router::new(&availability_zone),
                data_plane_fqdn.clone(),
                data_plane_signing_key.clone(),
            );

        // Inert registry: TaskService is the CGO entry point and does not
        // serve an admin surface; event! tracks still capture per-handler.
        let registry = service_kit::Registry::default();

        // The connector service is served both in-process and on this task's UDS, where
        // the Go connector proxy reaches it on behalf of the control plane.
        let connector_svc = connector::Service::new(
            plane,
            container_network,
            proto_grpc::Authenticator::new(data_plane_fqdn.clone(), data_plane_verify_keys),
            process,
            registry.clone(),
        );
        let data_plane_signer =
            proto_grpc::Signer::new(data_plane_fqdn, data_plane_signing_key.clone());

        let shard_svc = shard::Service::new(
            std::sync::Arc::new(connector::ServiceRouter::new(
                connector_svc.clone(),
                data_plane_signer.clone(),
            )),
            Some(tokio_context.set_log_level_fn()),
            task_name,
            crate::JournalPublisherFactory::new(publisher_factory),
            // Each session's Logger sinks the task's log stream — connector
            // logs and flattened runtime Events alike — to the task-log file
            // (the same encoded-JSON handler the runtime's own tracing uses),
            // which the Go runtime forwards to the task's ops-log journal.
            crate::FnLoggerFactory::new(log_handler, tokio_context.log_level_handle()),
            registry,
            Some(data_plane_signer),
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

        let server = tonic::transport::Server::builder()
            .add_service(shard_svc.into_tonic_service())
            .add_service(connector_svc.into_tonic_service())
            .serve_with_incoming_shutdown(uds_stream, async move {
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
