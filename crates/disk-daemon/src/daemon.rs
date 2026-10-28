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

    /// Bytes since the recovery floor beyond which a disk opens a recovery horizon,
    /// as a multiple of that disk's live allocated size. They are the encoded size of
    /// the chunks applied since the floor, which is the journal's content
    /// less its record framing. Together with the copy ratio it bounds what a
    /// recovery reads.
    #[arg(
        long,
        env = "HORIZON_OPEN_RATIO",
        default_value_t = crate::device::Policy::DEFAULT.open_ratio
    )]
    pub horizon_open_ratio: f64,

    /// Unchanged bytes a horizon may copy for each byte written while it is open.
    /// These copies clear a horizon's pending blocks which nothing is writing. Journal
    /// write amplification during compaction is at most one plus this.
    #[arg(
        long,
        env = "HORIZON_COPY_RATIO",
        default_value_t = crate::device::Policy::DEFAULT.copy_ratio
    )]
    pub horizon_copy_ratio: f64,

    /// Bytes since the recovery floor below which no horizon opens, whatever the ratio. It keeps a small disk from compacting constantly.
    #[arg(
        long,
        env = "HORIZON_MINIMUM_BYTES",
        default_value_t = crate::device::Policy::DEFAULT.minimum_bytes
    )]
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
    pub image_dir: std::path::PathBuf,
    /// Parent directory of per-device mounts.
    pub mount_dir: std::path::PathBuf,
    /// When a disk opens a recovery horizon, and how fast it completes one.
    pub horizon_policy: crate::device::Policy,
    /// Brokers of every disk journal.
    pub brokers: gazette::journal::Client,
}

pub async fn run(args: Args) -> anyhow::Result<()> {
    let brokers = broker_client(
        &args.broker_address,
        &args.gazette_zone,
        &args.data_plane_fqdn,
        &args.data_plane_auth_keys,
    )?;

    let config = std::sync::Arc::new(Config {
        image_dir: args.image_dir.clone(),
        mount_dir: args.mount_dir.clone(),
        horizon_policy: crate::device::Policy {
            open_ratio: args.horizon_open_ratio,
            copy_ratio: args.horizon_copy_ratio,
            minimum_bytes: args.horizon_minimum_bytes,
        },
        brokers,
    });

    tracing::info!(
        image_dir = ?args.image_dir,
        mount_dir = ?args.mount_dir,
        horizon_policy = ?config.horizon_policy,
        zone = args.gazette_zone,
        "disk-daemon starting",
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

    let attachments = tokio_util::task::TaskTracker::new();

    let service = Service {
        config,
        draining: draining.clone(),
        attachments: attachments.clone(),
    };
    let listener = listen(&args.uds_path)?;

    // The server takes the end of this stream as its shutdown, so a failed accept,
    // such as one out of descriptors, is retried rather than yielded.
    let incoming = futures::stream::unfold(listener, |listener| async move {
        loop {
            match listener.accept().await {
                Ok((connection, _address)) => {
                    return Some((Ok::<_, std::io::Error>(connection), listener));
                }
                Err(err) => {
                    tracing::warn!(%err, "accepting an attachment failed (will retry)");
                    () = tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    });

    let mut serving = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(proto_grpc::disk::disk_server::DiskServer::new(service))
            .serve_with_incoming_shutdown(incoming, {
                let draining = draining.clone();
                async move { draining.cancelled().await }
            }),
    );
    tracing::info!(socket = ?args.uds_path, "disk-daemon is serving attachments");

    // Attachments are indefinite, so a drain cancels them rather than waiting on them.
    // Each attachment closes its stream only once its disk is torn down, and the
    // server waits for the streams of clients still connected.
    let served = tokio::select! {
        joined = &mut serving => {
            _ = std::fs::remove_file(&args.uds_path);
            joined
        }

        _ = draining.cancelled() => {
            // Unlink the socket first. A client which reconnects must not open a
            // disk this daemon is about to stop serving.
            _ = std::fs::remove_file(&args.uds_path);

            tracing::info!("draining attachments");
            serving.await
        }
    };

    // An attachment whose client left may still be tearing its disk down, with no
    // stream for the server to wait on.
    draining.cancel();
    attachments.close();
    () = attachments.wait().await;

    served?.context("serving the attachment socket")
}

/// Serves the `Disk` gRPC service, spawning an attachment for each `Attach` stream.
struct Service {
    /// Configuration shared by all attachments.
    config: std::sync::Arc<Config>,
    /// Cancelled when the daemon drains, so that no attachment outlives it.
    draining: tokio_util::sync::CancellationToken,
    /// Every attachment's task, which the daemon waits on before it exits.
    attachments: tokio_util::task::TaskTracker,
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
        let cred = request
            .extensions()
            .get::<tonic::transport::server::UdsConnectInfo>()
            .and_then(|info| info.peer_cred)
            .expect("an accepted Unix socket carries its peer's credentials");
        let peer = (cred.uid(), cred.gid());

        let (responses, stream) = tokio::sync::mpsc::channel(1);

        self.attachments.spawn(crate::attachment::run(
            self.config.clone(),
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
fn broker_client(
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

/// Bind the socket which attachments are served over. A daemon killed without
/// draining leaves its socket behind, so this first removes any socket at `path`,
/// and the daemon started last on a path serves it.
fn listen(path: &std::path::Path) -> anyhow::Result<tokio::net::UnixListener> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            // A path that is not a socket is not the daemon's to remove.
            anyhow::ensure!(
                std::os::unix::fs::FileTypeExt::is_socket(&metadata.file_type()),
                "{path:?} exists and is not a socket"
            );
            () = std::fs::remove_file(path).with_context(|| format!("removing {path:?}"))?;
            tracing::info!(socket = ?path, "removed the socket already at the path");
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => (),
        Err(err) => return Err(anyhow::Error::new(err).context(format!("reading {path:?}"))),
    }

    let listener =
        tokio::net::UnixListener::bind(path).with_context(|| format!("binding {path:?}"))?;

    let mode = std::os::unix::fs::PermissionsExt::from_mode(0o666);
    () = std::fs::set_permissions(path, mode).with_context(|| format!("opening up {path:?}"))?;

    Ok(listener)
}
