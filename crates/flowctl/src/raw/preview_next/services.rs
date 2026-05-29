//! Run-scoped state for `flowctl preview`.
//!
//! For materializations: one tonic server hosting both `runtime_next::Service`
//! and `shuffle::Service` on a single ephemeral 127.0.0.1 port, plus tempdirs
//! for shard-zero RocksDB and shuffle log segments. The preview driver keeps
//! SessionLoop streams open across `--sessions` iterations so RocksDB is reused
//! without closing.
//!
//! For captures: just a RocksDB tempdir and (optionally) the admin surface.
//! Captures are leaderless and don't read journals, so the in-process Leader /
//! Shuffle services and the journal-reading auth token are not constructed.
use anyhow::Context;
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
    pub log_handler: fn(&::ops::Log),
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
    /// Resources for previewing a capture: a RocksDB tempdir and the optional
    /// admin surface. Captures are leaderless and don't read journals, so the
    /// in-process Leader / Shuffle services and a logged-in token are not
    /// required.
    pub async fn start_capture(
        log_json: bool,
        network: String,
        n_shards: u32,
        debug_port: Option<u16>,
        registry: service_kit::Registry,
    ) -> anyhow::Result<Self> {
        let log_handler = pick_log_handler(log_json);

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
            log_handler,
            network,
            rocksdb_path,
            shuffle_log_dir: String::new(),
            n_shards,
            registry,
            _shutdown_tx: shutdown_tx,
        })
    }

    /// Resources for previewing a materialization or derivation: the capture
    /// set plus an ephemeral tonic server hosting `runtime_next::Service` +
    /// `shuffle::Service` and the shuffle log tempdir. Requires a logged-in
    /// flowctl token to read source-journal documents. Both task types drive
    /// the same in-process Leader + Shuffle stack.
    pub async fn start_with_shuffle_leader(
        ctx: &mut crate::CliContext,
        network: String,
        log_json: bool,
        n_shards: u32,
        debug_port: Option<u16>,
        registry: service_kit::Registry,
    ) -> anyhow::Result<Self> {
        let log_handler = pick_log_handler(log_json);

        let access_token = ctx
            .config
            .user_access_token
            .clone()
            .context("you must be logged in to preview. Try `flowctl auth login`")?;

        // TODO(johnny): handle refresh rotation.
        let user_tokens = tokens::fixed(Ok(flow_client_next::user_auth::UserToken {
            access_token: Some(access_token),
            refresh_token: None,
        }));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .context("binding ephemeral preview listener")?;
        let local_addr = listener.local_addr()?;
        let peer_endpoint = format!("http://{local_addr}");

        let factory = flow_client_next::workflows::user_collection_auth::new_journal_client_factory(
            flow_client_next::rest::Client::new(ctx.config.get_agent_url(), "flowctl"),
            models::Capability::Read,
            gazette::Router::new("local"),
            user_tokens,
        );
        // 2 GiB matches the runtime-next default for sidecar-hosted shuffle services.
        let shuffle_svc = shuffle::Service::new(
            peer_endpoint.clone(),
            factory,
            2 * 1024 * 1024 * 1024,
            registry.clone(),
            None, // No AuthN+AuthZ signer (local loopback).
        );

        let publisher_factory: gazette::journal::ClientFactory = std::sync::Arc::new({
            move |_authz_sub: String, _authz_obj: String| -> gazette::journal::Client {
                unreachable!("live Publisher is not used by preview ({_authz_sub}, {_authz_obj})")
            }
        });
        let runtime_svc = runtime_next::Service::new(
            shuffle_svc.clone(),
            publisher_factory,
            registry.clone(),
            true, // Disarm AuthN+AuthZ (local loopback).
        );

        let router = tonic::transport::Server::builder()
            .add_service(runtime_svc.into_tonic_service())
            .add_service(shuffle_svc.into_tonic_service());

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
            log_handler,
            network,
            rocksdb_path,
            shuffle_log_dir,
            n_shards,
            registry,
            _shutdown_tx: shutdown_tx,
        })
    }
}

fn pick_log_handler(log_json: bool) -> fn(&::ops::Log) {
    if log_json {
        ::ops::stderr_log_handler
    } else {
        ::ops::tracing_log_handler
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
