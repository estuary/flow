use super::{Runtime, TokioContext};
use anyhow::Context;
use futures::channel::oneshot;
use futures::FutureExt;
use proto_flow::runtime::TaskServiceConfig;

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
        } = config;

        if !std::path::Path::new(&uds_path).is_absolute() {
            anyhow::bail!("uds_path must be an absolute filesystem path");
        }

        // We'll gather logs from tokio-tracing events of our TaskRuntime,
        // as well as logs which are forwarded from connector container delegates,
        // and sequence & dispatch them into this task-level `log_handler`.
        // These are read on the Go side and written to the task ops collection.
        let log_handler = ::ops::new_encoded_json_write_handler(std::sync::Arc::new(
            std::sync::Mutex::new(log_file),
        ));
        let tokio_context = TokioContext::new(
            ops::LogLevel::Info,
            log_handler.clone(),
            task_name.clone(),
            1,
        );

        // Instantiate selected task service definitions.
        let runtime = Runtime::new(
            container_network,
            log_handler,
            Some(tokio_context.set_log_level_fn()),
            task_name,
        );

        let uds = tokio_context
            .block_on(async move { tokio::net::UnixListener::bind(uds_path) })
            .context("failed to bind task service unix domain socket")?;
        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();

        // Construct a futures::Stream of io::Result<UnisStream>
        let uds_stream = futures::stream::try_unfold(uds, move |uds| async move {
            let (conn, addr) = uds.accept().await?;
            tracing::debug!(?addr, "accepted new unix socket connection");
            Ok::<_, std::io::Error>(Some((conn, uds)))
        });

        // Serve our bound unix domain socket until cancellation.
        // Upon cancellation, the server will wait until all client RPCs have
        // completed, and will then immediately tear down client transports.
        // This means we MUST mask SIGPIPE, because it's quite common for us or our
        // peer to attempt to send messages over a transport that the other side has torn down.
        let server =
            runtime
                .build_tonic_server()
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
        // Spawn to log from a runtime thread, then block the current thread awaiting it.
        let () = tokio_context.block_on(tokio_context.spawn(log)).unwrap();

        // TokioContext implements Drop for shutdown.
    }
}
