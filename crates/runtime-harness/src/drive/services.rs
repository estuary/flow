//! Run-scoped state for driving runtime-next tasks locally.
//!
//! For materializations / derivations: one tonic server hosting
//! `runtime_next::Service` (plus, optionally, a caller-provided
//! `shuffle::Service` for live journal-reading previews) on a single ephemeral
//! 127.0.0.1 port, plus tempdirs for shard-zero RocksDB and shuffle log
//! segments. The caller supplies the [`ShuffleSessionFactory`] — a
//! channel-fed [`segments::FixtureOpener`](super::segments::FixtureOpener) for
//! fixture / test replay, or a live shuffle factory — so this layer is unaware
//! of where source documents come from. Drivers keep SessionLoop streams open
//! across `--sessions` iterations so RocksDB is reused without closing.
//!
//! For captures: just a RocksDB tempdir and (optionally) the admin surface.
//! Captures are leaderless and don't read journals, so the in-process Leader /
//! Shuffle services are not constructed.
use anyhow::Context;
use runtime_next::{LoggerFactory, PublisherFactory, ShuffleSessionFactory};
use tokio_stream::wrappers::TcpListenerStream;

/// Run-scoped resources for one drive invocation. Field order matters for
/// `Drop`: the server task aborts before the tempdirs disappear out from under
/// any in-flight handler.
pub struct Run {
    // Materialize/derive-only: in-process tonic server + shuffle log tempdir.
    _server_task: Option<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
    _admin_task: Option<tokio::task::JoinHandle<()>>,
    _rocksdb_tmp: tempfile::TempDir,
    _shuffle_log_tmp: Option<tempfile::TempDir>,
    /// Empty for capture; the materialize / derive driver dials this peer for
    /// Leader and Shuffle RPCs.
    pub peer_endpoint: String,
    pub network: String,
    pub rocksdb_path: String,
    /// Empty for capture.
    pub shuffle_log_dir: String,
    pub n_shards: u32,
    pub registry: service_kit::Registry,
    // Triggered on `Run::drop` (via the channel closing) to stop the admin
    // surface gracefully alongside the tonic server.
    _shutdown_tx: tokio::sync::broadcast::Sender<()>,
}

impl Run {
    /// Resources for driving a capture: a RocksDB tempdir and the optional
    /// admin surface. Captures are leaderless and don't read journals, so the
    /// in-process Leader / Shuffle services are not required.
    pub async fn start_capture(
        network: String,
        n_shards: u32,
        debug_port: Option<u16>,
        registry: service_kit::Registry,
    ) -> anyhow::Result<Self> {
        // `rocksdb_path` is the inspectable shard-0 tempdir; shards >=1 each
        // get their own auto-managed tempdir via `RocksDB::open(None)`.
        let _rocksdb_tmp = tempfile::tempdir().context("creating RocksDB tempdir")?;
        let rocksdb_path = _rocksdb_tmp.path().to_string_lossy().into_owned();

        let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
        let _admin_task = spawn_admin_task(debug_port, &registry, &shutdown_tx);

        tracing::info!(
            %rocksdb_path,
            n_shards,
            debug_port = ?debug_port,
            "capture services started",
        );

        Ok(Self {
            _server_task: None,
            _admin_task,
            _rocksdb_tmp,
            _shuffle_log_tmp: None,
            peer_endpoint: String::new(),
            network,
            rocksdb_path,
            shuffle_log_dir: String::new(),
            n_shards,
            registry,
            _shutdown_tx: shutdown_tx,
        })
    }

    /// Resources for driving a materialization or derivation: the capture set
    /// plus an ephemeral tonic server hosting `runtime_next::Service` and the
    /// shuffle log tempdir.
    ///
    /// The caller supplies the source shuffle factory through `build_shuffle`,
    /// which is handed the freshly-bound `peer_endpoint` (a live journal-reading
    /// preview needs it to construct its loopback `shuffle::Service`) and returns
    /// the [`ShuffleSessionFactory`] plus an optional `shuffle::Service` to
    /// co-host. A fixture / test run returns `None` for the service and reads no
    /// journals; a live preview returns `Some(svc)`. This keeps journal auth and
    /// the fixture-vs-live choice in the caller, out of the generic drive layer.
    pub async fn start_with_shuffle_leader<S, P, L, F>(
        network: String,
        n_shards: u32,
        debug_port: Option<u16>,
        registry: service_kit::Registry,
        publisher_factory: P,
        logger_factory: L,
        build_shuffle: F,
    ) -> anyhow::Result<Self>
    where
        S: ShuffleSessionFactory,
        P: PublisherFactory,
        L: LoggerFactory,
        F: FnOnce(&str) -> anyhow::Result<(S, Option<shuffle::Service>)>,
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .context("binding ephemeral listener")?;
        let local_addr = listener.local_addr()?;
        let peer_endpoint = format!("http://{local_addr}");

        let (shuffle_factory, shuffle_svc) = build_shuffle(&peer_endpoint)?;

        let runtime_svc = runtime_next::Service::new(
            shuffle_factory,
            publisher_factory,
            logger_factory,
            registry.clone(),
            true, // Disarm AuthN+AuthZ (local loopback).
        );

        // Only a live preview serves peer Slice/Log RPCs; a fixture / test run
        // reads its pre-written segments from disk and never dials shuffle.
        let router =
            tonic::transport::Server::builder().add_service(runtime_svc.into_tonic_service());
        let router = match shuffle_svc {
            Some(svc) => router.add_service(svc.into_tonic_service()),
            None => router,
        };

        // `_shutdown_tx` lives on `Run` so the channel closes on drop, which
        // resolves the admin surface's graceful-shutdown future.
        let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);

        let _server_task =
            tokio::spawn(router.serve_with_incoming(TcpListenerStream::new(listener)));

        let _admin_task = spawn_admin_task(debug_port, &registry, &shutdown_tx);

        let _rocksdb_tmp = tempfile::tempdir().context("creating RocksDB tempdir")?;
        let _shuffle_log_tmp = tempfile::tempdir().context("creating shuffle-log tempdir")?;
        let rocksdb_path = _rocksdb_tmp.path().to_string_lossy().into_owned();
        let shuffle_log_dir = _shuffle_log_tmp.path().to_string_lossy().into_owned();

        tracing::info!(
            %peer_endpoint,
            %rocksdb_path,
            %shuffle_log_dir,
            n_shards,
            debug_port = ?debug_port,
            "leader + shuffle services started",
        );

        Ok(Self {
            _server_task: Some(_server_task),
            _admin_task,
            _rocksdb_tmp,
            _shuffle_log_tmp: Some(_shuffle_log_tmp),
            peer_endpoint,
            network,
            rocksdb_path,
            shuffle_log_dir,
            n_shards,
            registry,
            _shutdown_tx: shutdown_tx,
        })
    }
}

fn spawn_admin_task(
    debug_port: Option<u16>,
    registry: &service_kit::Registry,
    shutdown_tx: &tokio::sync::broadcast::Sender<()>,
) -> Option<tokio::task::JoinHandle<()>> {
    let debug_port = debug_port?;
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], debug_port));
    let registry = registry.clone();
    let mut shutdown_rx = shutdown_tx.subscribe();
    Some(tokio::spawn(async move {
        let shutdown = async move {
            let _ = shutdown_rx.recv().await;
        };
        if let Err(err) =
            service_kit::admin::serve("runtime-harness", registry, addr, shutdown).await
        {
            tracing::error!(?err, "runtime-harness admin surface exited with error");
        }
    }))
}
