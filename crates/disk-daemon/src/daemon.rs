use anyhow::Context;

#[derive(Debug, clap::Parser)]
#[command(about, version)]
pub struct Args {
    /// Unix socket the attachment service listens on.
    #[arg(long, env = "UDS_PATH")]
    pub uds_path: std::path::PathBuf,

    /// Brokers serving every disk journal this daemon opens.
    #[arg(long, env = "BROKER_ADDRESS")]
    pub broker_address: String,

    /// Availability zone of this daemon. Replays prefer to read from a broker in
    /// this zone, which avoids cross-zone egress.
    #[arg(long, env = "GAZETTE_ZONE", default_value = "local")]
    pub gazette_zone: String,

    /// FQDN of the data plane whose key signs this daemon's broker tokens, stamped
    /// as the `iss` of each one.
    #[arg(long, env = "DATA_PLANE_FQDN")]
    pub data_plane_fqdn: String,

    /// Base64 HMAC keys of the data plane, comma- or whitespace-separated.
    #[arg(long, env = "DATA_PLANE_AUTH_KEYS")]
    pub data_plane_auth_keys: String,

    /// Directory a disk's sparse image is created in.
    #[arg(long, env = "IMAGE_DIR")]
    pub image_dir: std::path::PathBuf,

    /// Directory a disk's filesystem is mounted under.
    #[arg(long, env = "MOUNT_DIR")]
    pub mount_dir: std::path::PathBuf,

    /// Text or JSON log output.
    #[arg(long, env = "LOG_FORMAT", default_value = "text")]
    pub log_format: LogFormat,

    /// Journal range above the recovery floor beyond which a disk opens a
    /// recovery horizon, as a multiple of that disk's live allocated size.
    /// Together with the copy ratio it bounds the range a recovery reads.
    #[arg(long, env = "HORIZON_OPEN_RATIO", default_value = "2.0")]
    pub horizon_open_ratio: f64,

    /// Unchanged bytes a delta may copy for each byte it changed. These copies
    /// discharge a horizon over blocks nothing is writing. Journal write
    /// amplification during compaction is at most one plus this.
    #[arg(long, env = "HORIZON_COPY_RATIO", default_value = "0.5")]
    pub horizon_copy_ratio: f64,

    /// Journal range below which no horizon opens, whatever the ratio. It keeps
    /// a small disk from compacting constantly.
    #[arg(long, env = "HORIZON_MINIMUM_BYTES", default_value_t = 1 << 30)]
    pub horizon_minimum_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, clap::ValueEnum)]
pub enum LogFormat {
    Text,
    Json,
}

/// What every attachment of a daemon is served with.
pub(crate) struct Config {
    /// Directory for disposable sparse images.
    pub(crate) image_dir: std::path::PathBuf,
    /// Parent directory of per-device mounts.
    pub(crate) mount_dir: std::path::PathBuf,
    /// When a disk opens a recovery horizon, and how fast it discharges one.
    pub(crate) horizon: crate::horizon::Policy,
    /// Brokers of every disk journal.
    pub(crate) brokers: gazette::journal::Client,
}

pub async fn run(args: Args) -> anyhow::Result<()> {
    let control = std::sync::Arc::new(crate::ublk::Control::open()?);

    () = std::fs::create_dir_all(&args.mount_dir)
        .with_context(|| format!("creating {:?}", args.mount_dir))?;

    let brokers = broker_client(
        &args.broker_address,
        &args.gazette_zone,
        &args.data_plane_fqdn,
        &args.data_plane_auth_keys,
    )?;

    let config = std::sync::Arc::new(Config {
        image_dir: args.image_dir.clone(),
        mount_dir: args.mount_dir.clone(),
        horizon: crate::horizon::Policy {
            open_ratio: args.horizon_open_ratio,
            copy_ratio: args.horizon_copy_ratio,
            minimum_bytes: args.horizon_minimum_bytes,
        },
        brokers,
    });

    tracing::info!(
        image_dir = ?args.image_dir,
        mount_dir = ?args.mount_dir,
        horizon = ?config.horizon,
        zone = args.gazette_zone,
        "disk daemon starting",
    );

    let draining = tokio_util::sync::CancellationToken::new();
    {
        let draining = draining.clone();

        tokio::spawn(async move {
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("install SIGTERM handler");
            tokio::select! {
                _ = term.recv() => tracing::info!("SIGTERM received"),
                _ = tokio::signal::ctrl_c() => tracing::info!("SIGINT received"),
            }
            draining.cancel();
        });
    }

    let service = Service {
        config,
        control,
        draining: draining.clone(),
    };
    let listener = listen(&args.uds_path)?;

    let incoming = futures::stream::try_unfold(listener, |listener| async move {
        let (connection, _address) = listener.accept().await?;
        Ok::<_, std::io::Error>(Some((connection, listener)))
    });

    let mut serving = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(proto_grpc::disk::disk_server::DiskServer::new(service))
            .serve_with_incoming_shutdown(incoming, {
                let draining = draining.clone();
                async move { draining.cancelled().await }
            }),
    );
    tracing::info!(socket = ?args.uds_path, "disk daemon is serving attachments");

    // Attachments are indefinite, so a drain cancels them rather than waiting on them.
    // Each attachment closes its stream only once its disk is torn down, and the
    // server waits for that.
    let served = tokio::select! {
        joined = &mut serving => joined?,

        _ = draining.cancelled() => {
            // Unlink the socket first. A client which reconnects must not open a
            // disk this daemon is about to stop serving.
            _ = std::fs::remove_file(&args.uds_path);

            tracing::info!("draining attachments");
            serving.await?
        }
    };
    _ = std::fs::remove_file(&args.uds_path);

    served.context("serving the attachment socket")
}

/// Serves the `Disk` gRPC service, spawning an attachment for each `Attach` stream.
struct Service {
    /// Configuration shared by all attachments.
    config: std::sync::Arc<Config>,
    /// Shared kernel device control handle.
    control: std::sync::Arc<crate::ublk::Control>,
    /// Cancelled when the daemon drains, so that no attachment outlives it.
    draining: tokio_util::sync::CancellationToken,
}

#[tonic::async_trait]
impl proto_grpc::disk::disk_server::Disk for Service {
    type AttachStream =
        tokio_stream::wrappers::ReceiverStream<tonic::Result<crate::proto::Response>>;

    async fn attach(
        &self,
        request: tonic::Request<tonic::Streaming<crate::proto::Request>>,
    ) -> tonic::Result<tonic::Response<Self::AttachStream>> {
        // Use the socket's authenticated peer credentials for mount ownership,
        // so the client needs no mount privilege.
        let peer = request
            .extensions()
            .get::<tonic::transport::server::UdsConnectInfo>()
            .and_then(|info| info.peer_cred)
            .map(|cred| (cred.uid(), cred.gid()));

        let (responses, stream) = tokio::sync::mpsc::channel(1);

        tokio::spawn(crate::attachment::run(
            self.config.clone(),
            self.control.clone(),
            peer,
            self.draining.child_token(),
            request.into_inner(),
            responses,
        ));

        Ok(tonic::Response::new(
            tokio_stream::wrappers::ReceiverStream::new(stream),
        ))
    }
}

/// A shared client with a refreshing, self-signed token.
pub(crate) fn broker_client(
    broker_address: &str,
    zone: &str,
    fqdn: &str,
    auth_keys: &str,
) -> anyhow::Result<gazette::journal::Client> {
    use proto_gazette::capability::{APPEND, APPLY, LIST, READ};

    let (key, _verify) = tokens::jwt::parse_base64_hmac_keys_str(auth_keys)
        .map_err(|status| anyhow::anyhow!("parsing --data-plane-auth-keys: {status}"))?;

    let claims = proto_gazette::Claims {
        cap: APPEND | APPLY | LIST | READ,
        // Stamped at each signing, from `duration`.
        exp: 0,
        iat: 0,
        iss: fqdn.to_string(),
        sel: proto_gazette::broker::LabelSelector {
            include: Some(labels::build_set([(
                labels::CONTENT_TYPE,
                crate::CONTENT_TYPE_DISK,
            )])),
            exclude: None,
        },
        // A broker enforces the capability and the selector, not this. It names what
        // is acting, for a broker's own logs.
        sub: "disk-daemon".to_string(),
    };

    let source = tokens::jwt::SignedSource {
        claims,
        set_time_claims: Box::new(|claims: &mut proto_gazette::Claims, iat, exp| {
            (claims.iat, claims.exp) = (iat.timestamp() as u64, exp.timestamp() as u64);
        }),
        duration: tokens::TimeDelta::minutes(10),
        key,
    };
    let endpoint = broker_address.to_string();

    Ok(gazette::journal::Client::new_with_tokens(
        move |token: &String| {
            Ok((
                proto_grpc::Metadata::new().with_bearer_token(token)?,
                endpoint.clone(),
            ))
        },
        gazette::journal::Client::new_fragment_client(),
        gazette::Router::new(zone),
        tokens::watch(source),
    ))
}

/// Bind the socket which attachments are served over.
fn listen(path: &std::path::Path) -> anyhow::Result<tokio::net::UnixListener> {
    let listener =
        tokio::net::UnixListener::bind(path).with_context(|| format!("binding {path:?}"))?;

    let mode = std::os::unix::fs::PermissionsExt::from_mode(0o666);
    () = std::fs::set_permissions(path, mode).with_context(|| format!("opening up {path:?}"))?;

    Ok(listener)
}
