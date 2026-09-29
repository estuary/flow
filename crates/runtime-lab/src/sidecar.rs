//! The sidecar process of a host: what `runtime-sidecar` is in production.
//!
//! It serves the Leader (for every task whose shard zero is on this host) and
//! the shuffle service (for every shard on this host), on one loopback port.
//! Unlike production, journal reads authorize with the user's token rather
//! than data-plane keys, peer AuthN is disarmed (everything is loopback), and
//! Leaders publish stats and ACK intents to the run's own broker.

use anyhow::Context;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Args {
    /// Name of this process's host, which names its threads (`<host>-sidecar`).
    #[clap(long)]
    host: String,
    /// flowctl profile whose credentials authorize journal reads, unless
    /// FLOW_AUTH_TOKEN is set. It's read, and never written.
    #[clap(long, default_value = "default")]
    profile: String,
    /// Tokio worker threads. Production's sidecar runs one per core, which
    /// (via `available_parallelism`) honors a cpuset and a cpu.max quota
    /// applied before the process starts, but not changes made afterwards.
    #[clap(long)]
    worker_threads: Option<usize>,
    /// Default shuffle disk limit of a task which doesn't set
    /// `estuary.dev/shuffle-disk-limit`.
    #[clap(long, default_value_t = shuffle::DEFAULT_SHUFFLE_DISK_LIMIT_BYTES)]
    shuffle_disk_limit: u64,
    /// Path to which readiness (pid, admin port, endpoint) is written once serving.
    #[clap(long)]
    ready_file: std::path::PathBuf,
    /// Endpoint of the run's broker, to which Leaders publish.
    #[clap(long)]
    broker: String,
}

pub fn run(args: Args, registry: service_kit::Registry) -> anyhow::Result<()> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    if let Some(n) = args.worker_threads {
        builder.worker_threads(n);
    }
    let name = format!("{}-sidecar", args.host);
    crate::set_thread_name(&name);
    let runtime = builder.thread_name(name).enable_all().build()?;
    let result = runtime.block_on(serve(args, registry));
    runtime.shutdown_background();
    result
}

async fn serve(args: Args, registry: service_kit::Registry) -> anyhow::Result<()> {
    let Args {
        host: _,
        profile,
        worker_threads: _,
        shuffle_disk_limit,
        ready_file,
        broker,
    } = args;

    let admin = service_kit::admin::build_router("runtime-lab-sidecar", registry.clone());
    let admin_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let admin_port = admin_listener.local_addr()?.port();
    tokio::spawn(async move { axum::serve(admin_listener, admin).await });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("binding sidecar listener")?;
    let endpoint = format!("http://{}", listener.local_addr()?);

    let session = crate::auth::start(&profile).await?;
    let factory = flow_client_next::workflows::user_collection_auth::new_journal_client_factory(
        session.rest,
        models::Capability::Read,
        gazette::Router::new("local"),
        session.tokens,
    );
    let shuffle_svc = shuffle::Service::new(
        endpoint.clone(),
        factory,
        shuffle_disk_limit,
        registry.clone(),
        None, // No AuthN+AuthZ signer (local loopback).
    );
    let leader_svc = runtime_next::Service::new(
        runtime_next::ShuffleServiceFactory::new(shuffle_svc.clone()),
        runtime_next::JournalPublisherFactory::new(crate::journal_client_factory(broker)),
        runtime_next::TracingLoggerFactory,
        registry,
        true, // Disarm AuthN+AuthZ (local loopback).
    );

    let server = tonic::transport::Server::builder()
        .add_service(shuffle_svc.into_tonic_service())
        .add_service(leader_svc.into_tonic_service())
        .serve_with_incoming_shutdown(
            tokio_stream::wrappers::TcpListenerStream::new(listener),
            async {
                crate::await_signal().await;
            },
        );

    crate::layout::write_json_atomic(
        &ready_file,
        &crate::layout::Ready {
            pid: std::process::id(),
            admin_port,
            endpoint: Some(endpoint.clone()),
        },
    )?;
    tracing::info!(%endpoint, admin_port, "sidecar serving");

    server.await.context("serving sidecar")
}
