//! Run-scoped state for `flowctl preview`.
//!
//! For materializations: one tonic server hosting `runtime_next::Service` (plus
//! `shuffle::Service`, for live previews that read journals) on a single
//! ephemeral 127.0.0.1 port, plus tempdirs for shard-zero RocksDB and shuffle
//! log segments. A fixture preview instead hands the runtime a fixture
//! [`ShuffleSessionFactory`](runtime_next::ShuffleSessionFactory) and hosts no
//! shuffle service. The preview driver keeps SessionLoop streams open across
//! `--sessions` iterations so RocksDB is reused without closing.
//!
//! For captures: just a RocksDB tempdir and (optionally) the admin surface.
//! Captures are leaderless and don't read journals, so the in-process Leader /
//! Shuffle services and the journal-reading auth token are not constructed.
use anyhow::Context;
use runtime_next::{ShuffleSession, ShuffleSessionFactory};
use tokio::sync::mpsc;
use tokio_stream::wrappers::TcpListenerStream;

/// Run-scoped resources for one `flowctl preview` invocation. Field order
/// matters for `Drop`: the server task aborts before the tempdirs disappear
/// out from under any in-flight handler.
pub struct Run {
    // Materialize-only: in-process tonic server + shuffle log tempdir.
    _server_task: Option<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
    _admin_task: Option<tokio::task::JoinHandle<()>>,
    _rocksdb_tmp: tempfile::TempDir,
    _shuffle_log_tmp: Option<tempfile::TempDir>,
    /// Empty for capture; the materialize driver dials this peer for Leader and
    /// Shuffle RPCs.
    pub peer_endpoint: String,
    pub network: String,
    pub rocksdb_path: String,
    /// Empty for capture.
    pub shuffle_log_dir: String,
    pub n_shards: u32,
    pub registry: service_kit::Registry,
    /// `Some` only for a fixture preview: the channel into the fixture
    /// [`ShuffleSessionFactory`](runtime_next::ShuffleSessionFactory). flowctl
    /// pushes one synthetic checkpoint Frontier per fixture transaction, then a
    /// `Boundary` per session boundary; dropping the sender signals
    /// end-of-fixtures.
    pub frontier_tx: Option<mpsc::UnboundedSender<super::fixture::FixtureItem>>,
    // Triggered on `Run::drop` (via the channel closing) to stop the admin
    // surface gracefully alongside the tonic server.
    _shutdown_tx: tokio::sync::broadcast::Sender<()>,
}

impl Run {
    /// Resources for previewing a capture: a RocksDB tempdir and the optional
    /// admin surface. Captures are leaderless and don't read journals, so the
    /// in-process Leader / Shuffle services and a logged-in token are not
    /// required.
    pub async fn start_capture(
        network: String,
        n_shards: u32,
        debug_port: Option<u16>,
        registry: service_kit::Registry,
    ) -> anyhow::Result<Self> {
        // `rocksdb_path` is the inspectable shard-0 tempdir; shards >=1 each
        // get their own auto-managed tempdir via `RocksDB::open(None)`.
        let _rocksdb_tmp = tempfile::tempdir().context("creating preview RocksDB tempdir")?;
        let rocksdb_path = _rocksdb_tmp.path().to_string_lossy().into_owned();

        let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
        let _admin_task = spawn_admin_task(debug_port, &registry, &shutdown_tx);

        tracing::info!(
            %rocksdb_path,
            n_shards,
            debug_port = ?debug_port,
            "preview capture services started",
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
            frontier_tx: None,
            _shutdown_tx: shutdown_tx,
        })
    }

    /// Resources for previewing a materialization or derivation: the capture
    /// set plus an ephemeral tonic server hosting `runtime_next::Service` and the
    /// shuffle log tempdir. A live preview additionally hosts a loopback
    /// `shuffle::Service` and requires a logged-in flowctl token to read
    /// source-journal documents; a fixture preview hands the runtime a fixture
    /// [`ShuffleSessionFactory`](runtime_next::ShuffleSessionFactory) and reads
    /// no journals.
    pub async fn start_with_shuffle_leader(
        ctx: &mut crate::CliContext,
        network: String,
        n_shards: u32,
        debug_port: Option<u16>,
        registry: service_kit::Registry,
        fixture: bool,
        publisher_factory: super::publish::PreviewPublisherFactory,
        observer_factory: super::observe::PreviewObserverFactory,
    ) -> anyhow::Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .context("binding ephemeral preview listener")?;
        let local_addr = listener.local_addr()?;
        let peer_endpoint = format!("http://{local_addr}");

        // A fixture preview reads no journals (flowctl feeds synthetic frontiers
        // and writes the segments itself), so it constructs no shuffle Service —
        // just a fixture shuffle factory — and needs neither a logged-in token
        // nor a journal client factory. A live preview authenticates and reads
        // source collections from real journals via a loopback shuffle Service.
        let (shuffle_factory, shuffle_svc, frontier_tx): (
            PreviewShuffleFactory,
            Option<shuffle::Service>,
            Option<mpsc::UnboundedSender<super::fixture::FixtureItem>>,
        ) = if fixture {
            let (opener, tx) = super::fixture::fixture_opener();
            (PreviewShuffleFactory::Fixture(opener), None, Some(tx))
        } else {
            anyhow::ensure!(
                ctx.access_token().is_some(),
                "you must be logged in to preview. Try `flowctl auth login`"
            );

            // Share the live, auto-refreshing user-token watch so a long-lived
            // preview re-mints collection authorizations with a currently-valid
            // access token and survives rotation of both token layers.
            let user_tokens = ctx.user_tokens.clone();
            let factory =
                flow_client_next::workflows::user_collection_auth::new_journal_client_factory(
                    ctx.rest.clone(),
                    models::Capability::Read,
                    ctx.router.clone(),
                    user_tokens,
                );
            let svc =
                shuffle::Service::new_loopback(peer_endpoint.clone(), factory, registry.clone());
            (
                PreviewShuffleFactory::Live(runtime_next::ShuffleServiceFactory::new(svc.clone())),
                Some(svc),
                None,
            )
        };

        let runtime_svc = runtime_next::Service::new(
            shuffle_factory,
            publisher_factory,
            observer_factory,
            registry.clone(),
            true, // Disarm AuthN+AuthZ (local loopback).
        );

        // Only a live preview serves peer Slice/Log RPCs; a fixture preview reads
        // its pre-written segments from disk and never dials shuffle.
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

        let _rocksdb_tmp = tempfile::tempdir().context("creating preview RocksDB tempdir")?;
        let _shuffle_log_tmp =
            tempfile::tempdir().context("creating preview shuffle-log tempdir")?;
        let rocksdb_path = _rocksdb_tmp.path().to_string_lossy().into_owned();
        let shuffle_log_dir = _shuffle_log_tmp.path().to_string_lossy().into_owned();

        tracing::info!(
            %peer_endpoint,
            %rocksdb_path,
            %shuffle_log_dir,
            n_shards,
            debug_port = ?debug_port,
            "preview leader + shuffle services started",
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
            frontier_tx,
            _shutdown_tx: shutdown_tx,
        })
    }
}

/// Shuffle-session factory for a preview leader. The new
/// [`ShuffleSessionFactory`] seam is monomorphized (`open` / `recv_checkpoint`
/// are `-> impl Future` and `close` takes `self`, so it is not object-safe);
/// this enum lets one leader `Service` host either source — a fixture replay
/// (`--fixture`) or a live in-process journal-reading shuffle Session — chosen
/// per run.
pub(crate) enum PreviewShuffleFactory {
    Fixture(super::fixture::FixtureOpener),
    Live(runtime_next::ShuffleServiceFactory),
}

impl ShuffleSessionFactory for PreviewShuffleFactory {
    type Session = PreviewShuffleSession;

    async fn open(
        &self,
        task: shuffle::proto::Task,
        shards: Vec<shuffle::proto::Shard>,
        resume: shuffle::Frontier,
    ) -> anyhow::Result<PreviewShuffleSession> {
        Ok(match self {
            Self::Fixture(f) => PreviewShuffleSession::Fixture(f.open(task, shards, resume).await?),
            Self::Live(f) => PreviewShuffleSession::Live(f.open(task, shards, resume).await?),
        })
    }
}

/// Per-session shuffle source opened by [`PreviewShuffleFactory`].
pub(crate) enum PreviewShuffleSession {
    Fixture(super::fixture::FixtureCheckpoints),
    Live(shuffle::SessionClient),
}

impl ShuffleSession for PreviewShuffleSession {
    fn request_checkpoint(&self) {
        match self {
            Self::Fixture(s) => s.request_checkpoint(),
            Self::Live(s) => s.request_checkpoint(),
        }
    }

    async fn recv_checkpoint(&mut self) -> anyhow::Result<shuffle::Frontier> {
        match self {
            Self::Fixture(s) => s.recv_checkpoint().await,
            Self::Live(s) => s.recv_checkpoint().await,
        }
    }

    async fn close(self) -> anyhow::Result<()> {
        match self {
            Self::Fixture(s) => s.close().await,
            Self::Live(s) => s.close().await,
        }
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
            service_kit::admin::serve("flowctl-preview", registry, addr, shutdown).await
        {
            tracing::error!(?err, "flowctl-preview admin surface exited with error");
        }
    }))
}
