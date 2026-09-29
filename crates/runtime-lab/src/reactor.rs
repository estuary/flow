//! The reactor process of a host: what a Go reactor's per-shard `TaskService`s
//! are in production, minus Go.
//!
//! Each assigned shard gets what `runtime_next::TaskService` gives it: its own
//! tokio runtime (`FLOW_RUNTIME_WORKER_THREADS` workers, default one), serving
//! the `Shard` service on its own Unix socket. The shard's runtime threads are
//! named by its label (`<host>-t<task>-s<shard>`), and the main thread by
//! `<host>-reactor`. The controller dials each socket and drives the shard's
//! sessions.
//!
//! Unlike production there's no data-plane signer or AuthN, connectors are
//! `local:` processes of this one, and every journal write goes to the run's
//! own broker.

use anyhow::Context;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Args {
    /// Name of this process's host, which names its main thread.
    #[clap(long)]
    host: String,
    /// Shard to serve, as `LABEL,TASK,SOCKET`. Repeated for each shard.
    #[clap(long = "shard", required = true)]
    shards: Vec<String>,
    /// Path to which readiness (pid and admin port) is written once serving.
    #[clap(long)]
    ready_file: std::path::PathBuf,
    /// Endpoint of the run's broker, to which shards publish.
    #[clap(long)]
    broker: String,
}

pub fn run(args: Args, registry: service_kit::Registry) -> anyhow::Result<()> {
    let Args {
        host,
        shards,
        ready_file,
        broker,
    } = args;
    crate::set_thread_name(&format!("{host}-reactor"));

    let worker_threads = std::env::var("FLOW_RUNTIME_WORKER_THREADS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n != 0)
        .unwrap_or(1);

    // Hosts the admin surface and signal handling, and is otherwise idle.
    let control = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    // Building the admin router installs the metrics recorder (and spawns its
    // upkeep), which must precede the creation of any metric handle.
    let _guard = control.enter();
    let admin = service_kit::admin::build_router("runtime-lab-reactor", registry.clone());
    let admin_listener = control.block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))?;
    let admin_port = admin_listener.local_addr()?.port();
    control.spawn(async move { axum::serve(admin_listener, admin).await });

    let connector_router = runtime_local::local_router(String::new(), registry.clone());
    let publisher =
        runtime_next::JournalPublisherFactory::new(crate::journal_client_factory(broker));

    let mut runtimes = Vec::new();
    for shard in &shards {
        let [label, task, socket] = shard.splitn(3, ',').collect::<Vec<_>>()[..] else {
            anyhow::bail!("--shard {shard:?} is not LABEL,TASK,SOCKET");
        };

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .thread_name(label)
            .enable_all()
            .build()?;

        let service = runtime_next::shard::Service::new(
            connector_router.clone(),
            None,
            task.to_string(),
            publisher.clone(),
            runtime_next::TracingLoggerFactory,
            registry.clone(),
            None, // No AuthN+AuthZ signer (local loopback).
        );
        let listener = runtime
            .block_on(async { tokio::net::UnixListener::bind(socket) })
            .with_context(|| format!("binding {socket}"))?;

        runtime.spawn(
            tonic::transport::Server::builder()
                .add_service(service.into_tonic_service())
                .serve_with_incoming(tokio_stream::wrappers::UnixListenerStream::new(listener)),
        );
        tracing::info!(label, task, socket, worker_threads, "serving shard");
        runtimes.push(runtime);
    }

    crate::layout::write_json_atomic(
        &ready_file,
        &crate::layout::Ready {
            pid: std::process::id(),
            admin_port,
            endpoint: None,
        },
    )?;

    // The controller owns our lifecycle: it stops us with SIGTERM, or we die
    // with it (PR_SET_PDEATHSIG).
    control.block_on(crate::await_signal());
    tracing::info!("reactor stopping");

    for runtime in runtimes {
        runtime.shutdown_background();
    }
    control.shutdown_background();
    Ok(())
}
