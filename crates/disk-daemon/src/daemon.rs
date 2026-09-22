//! The daemon process. This covers what it serves, and how it stops.

use crate::args::Args;
use crate::ublk::Control;
use anyhow::Context;

/// How long a drain waits for tenures to end. It sizes a healthy teardown rather
/// than a policy, so it is not configurable. It also sits well under systemd's
/// default `TimeoutStopSec`, so a drain which does not finish is this daemon's own
/// report rather than a SIGKILL.
const DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// What every tenure of a daemon is served with.
pub struct Config {
    pub image_dir: std::path::PathBuf,
    pub mount_dir: std::path::PathBuf,
    /// When a disk opens a recovery horizon, and how fast it discharges one.
    pub horizon: crate::horizon::Policy,
    /// Brokers of every disk journal, authorized by [`client`]. Shared by every
    /// tenure's journal writer.
    pub client: gazette::journal::Client,
}

/// Duration of a token the daemon signs. `tokens` renews two minutes ahead of
/// expiry and never more often than once a minute, so this is minutes rather than
/// seconds. Rotating the key needs a restart, as it does for every component of the
/// data plane.
const TOKEN_DURATION: tokens::TimeDelta = match tokens::TimeDelta::try_minutes(10) {
    Some(duration) => duration,
    None => panic!("ten minutes is a valid duration"),
};

/// The daemon's broker client: one self-signed credential over one transport, which
/// every tenure shares.
///
/// The daemon holds the data plane's signing key and mints its own tokens, rather
/// than being handed one by a client and having to be handed another before it
/// expires. This is how a reactor reaches its shard recovery logs: a journal of the
/// data plane's own state is self-signed, and only user collection data goes through
/// the control plane's authorization API. Only the first of `auth_keys` signs; the
/// daemon verifies nothing, so it needs no others.
///
/// One token serves every disk. A narrower one would not be authorization between
/// clients, because socket access already reaches every disk journal of the data
/// plane. What the content-type selector does bound is the daemon itself, which
/// cannot touch a collection's journal or a shard's recovery log with it.
///
/// It also bounds what the daemon can see: a journal carrying some other content
/// type does not merely fail `spec`'s validation, it lists as absent, and `Open`
/// reports it as a journal which does not exist.
pub(crate) fn client(
    broker_address: &str,
    zone: &str,
    fqdn: &str,
    auth_keys: &str,
) -> anyhow::Result<gazette::journal::Client> {
    use proto_gazette::capability::{APPEND, APPLY, LIST, READ};

    let (key, _verify) = tokens::jwt::parse_base64_hmac_keys_str(auth_keys)
        .map_err(|status| anyhow::anyhow!("parsing --data-plane-auth-keys: {status}"))?;

    let claims = proto_gazette::Claims {
        // Everything the daemon does: it lists a journal to validate its spec, reads
        // it back to recover a disk, appends its deltas, and applies the recovery
        // floor it derives.
        cap: APPEND | APPLY | LIST | READ,
        // Stamped at each signing, from `TOKEN_DURATION`.
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
        duration: TOKEN_DURATION,
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

/// Prefix of a disk's mount point. The rest of the name is the device number, so
/// that a mount point names the device it presents.
pub const MOUNT_PREFIX: &str = "disk-";

pub async fn run(args: Args) -> anyhow::Result<()> {
    let control = std::sync::Arc::new(Control::open()?);

    () = std::fs::create_dir_all(&args.mount_dir)
        .with_context(|| format!("creating {:?}", args.mount_dir))?;

    let client = client(
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
        client,
    });

    tracing::info!(
        image_dir = ?args.image_dir,
        mount_dir = ?args.mount_dir,
        horizon = ?config.horizon,
        // A zone which matches no broker is silent otherwise, and it is what a
        // replay's cross-zone egress traces back to.
        zone = args.gazette_zone,
        "disk daemon starting",
    );

    // SIGTERM from systemd and SIGINT from a terminal both drain the daemon. One
    // token ends everything the daemon runs, tenures included.
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

    let service = crate::tenure::Service::new(config, control, draining.clone());
    let listener = listen(&args.uds_path)?;

    let incoming = futures::stream::try_unfold(listener, |listener| async move {
        let (connection, _address) = listener.accept().await?;
        Ok::<_, std::io::Error>(Some((connection, listener)))
    });

    let mut serving = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(service.into_tonic_service())
            .serve_with_incoming_shutdown(incoming, {
                let draining = draining.clone();
                async move { draining.cancelled().await }
            }),
    );
    tracing::info!(socket = ?args.uds_path, "disk daemon is serving tenures");

    // Tenures are indefinite, so a drain ends them rather than waiting on them.
    // Each tenure closes its stream only once its disk is torn down. The server
    // waits for that, and the timeout bounds the wait.
    let served = tokio::select! {
        served = &mut serving => served?,

        _ = draining.cancelled() => {
            // Unlink the socket first. A client which reconnects must not open a
            // disk this daemon is about to stop serving.
            _ = std::fs::remove_file(&args.uds_path);

            tracing::info!(timeout = ?DRAIN_TIMEOUT, "draining tenures");

            match tokio::time::timeout(DRAIN_TIMEOUT, serving).await {
                Ok(served) => served?,
                Err(_elapsed) => {
                    tracing::warn!(
                        "the drain timed out; a device or mount which a tenure left behind \
                         stays until an operator removes it",
                    );
                    Ok(())
                }
            }
        }
    };
    _ = std::fs::remove_file(&args.uds_path);

    served.context("serving the tenure socket")
}

/// Bind the socket which tenures are served over.
///
/// The socket is left reachable by any user, per [`crate::args::Args`].
fn listen(path: &std::path::Path) -> anyhow::Result<tokio::net::UnixListener> {
    let listener =
        tokio::net::UnixListener::bind(path).with_context(|| format!("binding {path:?}"))?;

    let mode = std::os::unix::fs::PermissionsExt::from_mode(0o666);
    () = std::fs::set_permissions(path, mode).with_context(|| format!("opening up {path:?}"))?;

    Ok(listener)
}
