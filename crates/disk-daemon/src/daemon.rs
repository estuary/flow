//! The daemon process. This covers what it serves, and how it stops.

use crate::args::Args;
use crate::journal;
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
    /// Shared by every tenure's journal writer.
    pub client: gazette::journal::Client,
    /// Brokers of every disk journal, and the key which authorizes this daemon to
    /// them. Each tenure signs a token of its own journal from these.
    pub auth: journal::Auth,
}

/// Prefix of a disk's mount point. The rest of the name is the device number, so
/// that a mount point names the device it presents.
pub const MOUNT_PREFIX: &str = "disk-";

pub async fn run(args: Args) -> anyhow::Result<()> {
    let control = std::sync::Arc::new(Control::open()?);

    () = std::fs::create_dir_all(&args.mount_dir)
        .with_context(|| format!("creating {:?}", args.mount_dir))?;

    let client = journal::shared_client(&args.broker_address, &args.gazette_zone);

    // Only the first key signs. The daemon verifies nothing, so it needs no others.
    let (key, _verify) = tokens::jwt::parse_base64_hmac_keys_str(&args.data_plane_auth_keys)
        .map_err(|status| anyhow::anyhow!("parsing --data-plane-auth-keys: {status}"))?;

    let config = std::sync::Arc::new(Config {
        image_dir: args.image_dir.clone(),
        mount_dir: args.mount_dir.clone(),
        horizon: crate::horizon::Policy {
            open_ratio: args.horizon_open_ratio,
            copy_ratio: args.horizon_copy_ratio,
            minimum_bytes: args.horizon_minimum_bytes,
        },
        client,
        auth: journal::Auth {
            endpoint: args.broker_address.clone(),
            fqdn: args.data_plane_fqdn.clone(),
            key,
        },
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
            () = service_kit::shutdown_signal().await;
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
