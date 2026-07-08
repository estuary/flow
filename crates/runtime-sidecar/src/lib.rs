use anyhow::Context;
use clap::Parser;
use std::path::PathBuf;

/// Command-line arguments for the runtime-sidecar process.
///
/// Naming aligns with sibling Rust services (`dekaf`) for unprefixed
/// `DATA_PLANE_*`, `CERTIFICATE_*`, and `AGENT_ENDPOINT` envs. The
/// reactor (Go consumer) uses `FLOW_*` and `CONSUMER_*` namespaced
/// envs because that's `go-flags`/gazette `mainboilerplate` convention;
/// we follow the unprefixed form here.
#[derive(Debug, Parser)]
#[command(about, version)]
pub struct Args {
    #[arg(long = "log-format", env = "LOG_FORMAT", default_value = "text")]
    pub log_format: LogFormat,

    /// TCP port to listen on, binding `[::]:<port>`.
    #[arg(long, env = "LISTEN_PORT")]
    pub listen_port: u16,

    /// When set, serve the admin surface (live gRPC handler inventory) on
    /// `127.0.0.1:<port>`. Loopback-only: this surface has no authentication.
    #[arg(long, env = "ADMIN_PORT")]
    pub admin_port: Option<u16>,

    /// Externally-reachable URL of this sidecar, advertised to peer
    /// shuffle clients (e.g. `https://reactor-foo.flow.localhost:9100`).
    #[arg(long, env = "PEER_ENDPOINT")]
    pub peer_endpoint: String,

    /// Fully-qualified domain name of the data-plane that this sidecar
    /// belongs to; used as the issuer claim of authorization tokens.
    #[arg(long, env = "DATA_PLANE_FQDN")]
    pub data_plane_fqdn: String,

    /// Whitespace- or comma-separated base64 HMAC keys recognized by
    /// the data plane. The first key signs outgoing `/authorize/task`
    /// requests and RPCs to sidecar peers; all keys are accepted as
    /// verifiers for incoming gRPC traffic.
    #[arg(long, env = "DATA_PLANE_AUTH_KEYS")]
    pub data_plane_auth_keys: String,

    /// TLS server certificate PEM. Both `--certificate-file` and
    /// `--certificate-key-file` must be provided together.
    #[arg(long, env = "CERTIFICATE_FILE", requires = "certificate_key_file")]
    pub certificate_file: Option<PathBuf>,

    /// TLS server private key PEM. Required iff `--certificate-file` is set.
    #[arg(long, env = "CERTIFICATE_KEY_FILE", requires = "certificate_file")]
    pub certificate_key_file: Option<PathBuf>,

    /// Estuary agent REST base URL used to issue `/authorize/task` calls.
    #[arg(long, env = "AGENT_ENDPOINT")]
    pub agent_endpoint: url::Url,

    /// Broker zone passed to `gazette::Router::new`.
    #[arg(long, env = "GAZETTE_ZONE", default_value = "local")]
    pub gazette_zone: String,

    /// Data-plane default shuffle disk limit in bytes, used when a task doesn't
    /// set its own `estuary.dev/shuffle-disk-limit` label. Default is 2 GiB.
    #[arg(long, env = "SHUFFLE_DISK_LIMIT_BYTES", default_value_t = shuffle::DEFAULT_SHUFFLE_DISK_LIMIT_BYTES)]
    pub shuffle_disk_limit_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, clap::ValueEnum)]
pub enum LogFormat {
    Text,
    Json,
}

pub async fn run(args: Args, registry: service_kit::Registry) -> anyhow::Result<()> {
    // Parse comma/whitespace-separated base64 HMAC keys. The first key signs
    // outgoing requests. All keys are accepted as verifiers for incoming gRPCs.
    let (signing_key, verification_keys) =
        tokens::jwt::parse_base64_hmac_keys_str(&args.data_plane_auth_keys)
            .map_err(|status| anyhow::anyhow!("parsing --data-plane-auth-keys: {status}"))?;

    let authn = proto_grpc::Authenticator::new(args.data_plane_fqdn.clone(), verification_keys);
    let shuffle_signer = proto_grpc::Signer::new(args.data_plane_fqdn.clone(), signing_key.clone());

    // Build REST + Router.
    let api_client = flow_client_next::rest::Client::new(&args.agent_endpoint, "runtime-sidecar");
    let router = gazette::Router::new(&args.gazette_zone);

    use proto_gazette::capability::{APPEND, APPLY, LIST, READ};
    // Shuffle read factory watches (LIST) and reads (READ) source journals.
    let read_factory =
        flow_client_next::workflows::task_collection_auth::new_journal_client_factory(
            api_client.clone(),
            LIST | READ,
            router.clone(),
            args.data_plane_fqdn.clone(),
            signing_key.clone(),
        );
    // Publisher factory watches (LIST), creates partitions (APPLY),
    // and appends (APPEND) to dest journals.
    let publisher_factory =
        flow_client_next::workflows::task_collection_auth::new_journal_client_factory(
            api_client,
            APPEND | APPLY | LIST,
            router,
            args.data_plane_fqdn,
            signing_key,
        );

    let shuffle_svc = shuffle::Service::new(
        args.peer_endpoint,
        read_factory,
        args.shuffle_disk_limit_bytes,
        registry.clone(),
        Some(shuffle_signer),
    );
    let runtime_svc = runtime_next::Service::new(
        shuffle_svc.clone(),
        publisher_factory,
        registry.clone(),
        false, // Don't disarm, enforce AuthN+AuthZ.
    );

    // Build a TLS identity if both files were given.
    // clap `requires` enforces both-or-neither.
    let tls_identity = if let (Some(cert), Some(key)) = (
        args.certificate_file.as_ref(),
        args.certificate_key_file.as_ref(),
    ) {
        let cert_bytes = tokio::fs::read(cert)
            .await
            .with_context(|| format!("reading {}", cert.display()))?;
        let key_bytes = tokio::fs::read(key)
            .await
            .with_context(|| format!("reading {}", key.display()))?;
        Some(tonic::transport::Identity::from_pem(cert_bytes, key_bytes))
    } else {
        None
    };

    // SIGTERM (systemd) and SIGINT (interactive Ctrl+C) both initiate graceful shutdown.
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    {
        let shutdown_tx = shutdown_tx.clone();
        tokio::spawn(async move {
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("install SIGTERM handler");
            tokio::select! {
                _ = term.recv() => tracing::info!("SIGTERM received"),
                _ = tokio::signal::ctrl_c() => tracing::info!("SIGINT received"),
            }
            let _ = shutdown_tx.send(());
        });
    }

    // Optionally serve the loopback admin surface (handler dashboard).
    if let Some(admin_port) = args.admin_port {
        let addr = std::net::SocketAddr::from(([127, 0, 0, 1], admin_port));
        let registry = registry.clone();
        let mut shutdown_rx = shutdown_rx.resubscribe();
        tokio::spawn(async move {
            let shutdown = async move {
                let _ = shutdown_rx.recv().await;
            };
            if let Err(err) =
                service_kit::admin::serve("runtime-sidecar", registry, addr, shutdown).await
            {
                tracing::error!(?err, "runtime-sidecar admin surface exited with error");
            }
        });
    }

    let addr = format!("[::]:{}", args.listen_port);
    let tcp = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("binding TCP {addr}"))?;
    tracing::info!(%addr, tls = tls_identity.is_some(), "runtime-sidecar listening on TCP");

    let mut builder = tonic::transport::Server::builder();
    if let Some(identity) = tls_identity {
        builder = builder
            .tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))
            .context("configuring TCP TLS")?;
    }
    builder
        .add_service(tonic::service::interceptor::InterceptedService::new(
            runtime_svc.into_tonic_service(),
            authn.clone().interceptor(proto_flow::capability::LEAD),
        ))
        .add_service(tonic::service::interceptor::InterceptedService::new(
            shuffle_svc.into_tonic_service(),
            authn.interceptor(proto_flow::capability::SHUFFLE),
        ))
        .serve_with_incoming_shutdown(
            tokio_stream::wrappers::TcpListenerStream::new(tcp),
            async move {
                let _ = shutdown_rx.recv().await;
            },
        )
        .await
        .context("serving runtime-sidecar TCP")
}
