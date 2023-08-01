use super::{derive, TaskRuntime};
use anyhow::Context;
use futures::channel::oneshot;
use futures::FutureExt;
use proto_flow::{ops, runtime::TaskServiceConfig};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use tracing_subscriber::prelude::*;

pub struct TaskService {
    cancel_tx: oneshot::Sender<()>,
    runtime: TaskRuntime,
    server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl TaskService {
    pub fn new(config: TaskServiceConfig, log_file: std::fs::File) -> anyhow::Result<Self> {
        let TaskServiceConfig {
            log_file_fd: _,
            task_name,
            uds_path,
        } = config;

        if !std::path::Path::new(&uds_path).is_absolute() {
            anyhow::bail!("uds_path must be an absolute filesystem path");
        }

        // Dynamically configurable ops::log::Level, as a shared atomic.
        let log_level = std::sync::Arc::new(AtomicI32::new(ops::log::Level::Info as i32));

        // Dynamic tracing log filter which uses our dynamic Level.
        let log_level_clone = log_level.clone();
        let log_filter = tracing_subscriber::filter::DynFilterFn::new(move |metadata, _cx| {
            let cur_level = match metadata.level().as_str() {
                "TRACE" => ops::log::Level::Trace as i32,
                "DEBUG" => ops::log::Level::Debug as i32,
                "INFO" => ops::log::Level::Info as i32,
                "WARN" => ops::log::Level::Warn as i32,
                "ERROR" => ops::log::Level::Error as i32,
                _ => ops::log::Level::UndefinedLevel as i32,
            };

            if let Some(path) = metadata.module_path() {
                // Hyper / HTTP/2 debug logs are just too noisy and not very useful.
                if path.starts_with("h2::") && cur_level >= ops::log::Level::Debug as i32 {
                    return false;
                }
            }

            cur_level <= log_level_clone.load(Ordering::Relaxed)
        });

        // Function closure which allows for changing the dynamic log level.
        let set_log_level = Arc::new(move |level: ops::log::Level| {
            log_level.store(level as i32, Ordering::Relaxed)
        });

        // We'll gather logs from tokio-tracing events of our TaskRuntime,
        // as well as logs which are forwarded from connector container delegates,
        // and sequence & dispatch them into this task-level `log_handler`.
        // These are read on the Go side and written to the task ops collection.
        let log_handler = ::ops::new_encoded_json_write_handler(std::sync::Arc::new(
            std::sync::Mutex::new(log_file),
        ));
        // Configure a tracing::Dispatch, which is a type-erased form of a tracing::Subscriber,
        // that gathers tracing events & spans and logs them to `log_handler`.
        let log_dispatch: tracing::Dispatch = tracing_subscriber::registry()
            .with(
                ::ops::tracing::Layer::new(log_handler.clone(), std::time::SystemTime::now)
                    .with_filter(log_filter),
            )
            .into();
        let runtime = TaskRuntime::new(task_name, log_dispatch);

        // Instantiate selected task service definitions.
        let derive_service = Some(derive::Middleware::new(
            log_handler.clone(),
            Some(set_log_level.clone()),
        ));

        let uds = runtime
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
        let server = tonic::transport::Server::builder()
            .add_optional_service(derive_service.map(|s| {
                proto_grpc::derive::connector_server::ConnectorServer::new(s)
                    .max_decoding_message_size(usize::MAX) // Up from 4MB. Accept whatever the Go runtime sends.
                    .max_encoding_message_size(usize::MAX) // The default, made explicit.
            }))
            .serve_with_incoming_shutdown(uds_stream, async move {
                _ = cancel_rx.await;
            });
        let server = runtime.spawn(server);

        Ok(Self {
            cancel_tx,
            runtime,
            server,
        })
    }

    pub fn graceful_stop(self) {
        let Self {
            cancel_tx,
            runtime,
            server,
        } = self;

        _ = cancel_tx.send(());

        let log = match runtime.block_on(server) {
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
        let () = runtime.block_on(runtime.spawn(log)).unwrap();

        // TaskRuntime implements Drop for shutdown.
    }
}
