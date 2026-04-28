//! Run-scoped state for `flowctl preview`: one tonic server hosting both
//! `runtime_next::Service` and `shuffle::Service` on a single ephemeral
//! 127.0.0.1 port, plus the persistent tempdirs (RocksDB + shuffle log
//! segments) that survive across `--sessions` iterations.
//!
//! The auth wiring mirrors `flowctl raw shuffle` — gazette client is built
//! from the user's flowctl access token, tokens refresh automatically while
//! the run lives. Long previews can outlive a token (same posture as `raw
//! shuffle`).

use anyhow::Context;
use tokio_stream::wrappers::TcpListenerStream;

/// Run-scoped resources for one `flowctl preview` invocation. Field order
/// matters for `Drop`: the server task aborts before the tempdirs disappear
/// out from under any in-flight handler.
pub struct Run {
    _server_task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    _rocksdb_tmp: tempfile::TempDir,
    _shuffle_log_tmp: tempfile::TempDir,
    pub peer_endpoint: String,
    pub log_handler: fn(&::ops::Log),
    pub network: String,
    pub rocksdb_path: String,
    pub shuffle_log_dir: String,
    pub n_shards: u32,
}

impl Run {
    pub async fn start(
        ctx: &mut crate::CliContext,
        network: String,
        log_json: bool,
        n_shards: u32,
    ) -> anyhow::Result<Self> {
        let log_handler: fn(&::ops::Log) = if log_json {
            ::ops::stderr_log_handler
        } else {
            ::ops::tracing_log_handler
        };

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
        let shuffle_svc = shuffle::Service::new(peer_endpoint.clone(), factory);

        let publisher_factory: gazette::journal::ClientFactory = std::sync::Arc::new({
            move |_authz_sub: String, _authz_obj: String| -> gazette::journal::Client {
                unreachable!("live Publisher is not used by preview ({_authz_sub}, {_authz_obj})")
            }
        });
        let runtime_svc = runtime_next::Service::new(shuffle_svc.clone(), publisher_factory);

        let router = tonic::transport::Server::builder()
            .add_service(runtime_svc.into_tonic_service())
            .add_service(shuffle_svc.into_tonic_service());

        let _server_task =
            tokio::spawn(router.serve_with_incoming(TcpListenerStream::new(listener)));

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
            "preview services started",
        );

        Ok(Self {
            _server_task,
            _rocksdb_tmp,
            _shuffle_log_tmp,
            peer_endpoint,
            log_handler,
            network,
            rocksdb_path,
            shuffle_log_dir,
            n_shards,
        })
    }
}
