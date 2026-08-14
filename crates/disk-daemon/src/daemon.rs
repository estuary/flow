//! The daemon process: what it validates about its host, what it serves, and
//! how it stops.

use crate::args::Args;
use crate::filesystem;
use crate::journal;
use crate::ublk::Control;
use anyhow::Context;

/// Interval at which idle broker connections are closed. It is short relative
/// to a session, which holds its connections open by using them.
const SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// How long a drain waits for sessions to end. It sizes a healthy teardown
/// rather than a policy, so it is not configurable, and it sits well under
/// systemd's default `TimeoutStopSec` so that this daemon's error is what an
/// operator sees rather than a SIGKILL.
const DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// What every session of a daemon is served with.
pub struct Config {
    pub image_dir: std::path::PathBuf,
    pub mount_dir: std::path::PathBuf,
    /// Label on a disk journal's own spec which holds its recovery floor, and
    /// which every replay reads back as its seek hint.
    pub floor_label: String,
    /// Shared by every session's journal writer.
    pub client: gazette::journal::Client,
    /// Routing table of `client`, swept periodically to close connections to
    /// brokers which no longer serve any disk.
    pub router: gazette::Router,
}

/// Prefix of a disk's mount point, which carries the device number so that a
/// later daemon can delete the device a mount it inherits was made from.
pub const MOUNT_PREFIX: &str = "disk-";

pub async fn run(args: Args, registry: service_kit::Registry) -> anyhow::Result<()> {
    () = validate(&args).context("this host cannot serve disks")?;

    let (client, router) = journal::shared_client();

    let config = std::sync::Arc::new(Config {
        image_dir: args.image_dir.clone(),
        mount_dir: args.mount_dir.clone(),
        floor_label: args.floor_label.clone(),
        client,
        router,
    });

    let control = std::sync::Arc::new(Control::open()?);
    () = reclaim(&control, &args.mount_dir, crate::filesystem::MOUNT_TIMEOUT).await?;

    tracing::info!(
        image_dir = ?args.image_dir,
        mount_dir = ?args.mount_dir,
        ublks_max = control.ublks_max(),
        floor_label = args.floor_label,
        "disk daemon starting",
    );

    // SIGTERM (systemd) and SIGINT (interactive) both drain the daemon.
    let (shutdown, mut draining) = tokio::sync::broadcast::channel(1);
    {
        let shutdown = shutdown.clone();

        tokio::spawn(async move {
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("install SIGTERM handler");

            tokio::select! {
                _ = term.recv() => tracing::info!("SIGTERM received"),
                _ = tokio::signal::ctrl_c() => tracing::info!("SIGINT received"),
            }
            _ = shutdown.send(());
        });
    }

    if let Some(admin_port) = args.admin_port {
        let address = std::net::SocketAddr::from(([127, 0, 0, 1], admin_port));
        let (registry, mut draining) = (registry.clone(), shutdown.subscribe());

        tokio::spawn(async move {
            let outcome =
                service_kit::admin::serve("flow-disk-daemon", registry, address, async move {
                    _ = draining.recv().await;
                })
                .await;

            if let Err(err) = outcome {
                tracing::error!(?err, "admin surface exited with an error");
            }
        });
    }
    {
        let (router, mut draining) = (config.router.clone(), shutdown.subscribe());

        tokio::spawn(async move {
            while tokio::time::timeout(SWEEP_INTERVAL, draining.recv())
                .await
                .is_err()
            {
                router.sweep();
            }
        });
    }
    let mut drained = shutdown.subscribe();

    let service = crate::session::Service::new(config, control, registry, shutdown);
    let listener = listen(&args.uds_path)?;

    let incoming = futures::stream::try_unfold(listener, |listener| async move {
        let (connection, _address) = listener.accept().await?;
        Ok::<_, std::io::Error>(Some((connection, listener)))
    });

    let mut serving = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(service.into_tonic_service())
            .serve_with_incoming_shutdown(incoming, async move {
                _ = draining.recv().await;
            }),
    );
    tracing::info!(socket = ?args.uds_path, "disk daemon is serving sessions");

    // Sessions are indefinite, so a drain ends them rather than waiting on
    // them. Each closes its stream only once its disk is torn down, which is
    // what the server then waits for and what the timeout bounds.
    let served = tokio::select! {
        served = &mut serving => served?,

        _ = drained.recv() => {
            tracing::info!(timeout = ?DRAIN_TIMEOUT, "draining sessions");

            match tokio::time::timeout(DRAIN_TIMEOUT, serving).await {
                Ok(served) => served?,
                Err(_elapsed) => anyhow::bail!(
                    "sessions did not end within the drain timeout, so devices are left behind",
                ),
            }
        }
    };
    _ = std::fs::remove_file(&args.uds_path);

    served.context("serving the session socket")
}

/// Bind `path`, taking over a socket which a previous daemon left behind.
///
/// The socket is left reachable by any user, because the daemon's clients are
/// not the privileged user it runs as and it has no user model of its own.
/// Which of them may reach it is the socket directory's permissions to decide.
fn listen(path: &std::path::Path) -> anyhow::Result<tokio::net::UnixListener> {
    let listener = match tokio::net::UnixListener::bind(path) {
        Err(err) if err.kind() == std::io::ErrorKind::AddrInUse => {
            // A socket file outlives the process which bound it, so its
            // presence is not proof that a daemon is running. A connection is.
            if std::os::unix::net::UnixStream::connect(path).is_ok() {
                anyhow::bail!("another daemon is already listening on {path:?}");
            }
            std::fs::remove_file(path)
                .with_context(|| format!("removing the stale socket {path:?}"))?;

            tokio::net::UnixListener::bind(path)
        }
        result => result,
    }
    .with_context(|| format!("binding {path:?}"))?;

    let mode = std::os::unix::fs::PermissionsExt::from_mode(0o666);
    () = std::fs::set_permissions(path, mode).with_context(|| format!("opening up {path:?}"))?;

    Ok(listener)
}

/// Unmount and delete whatever a previous daemon left behind.
///
/// A daemon which is killed outright cannot unmount, and the kernel removes the
/// block device under a mount but neither the mount nor the character device.
/// The mount directory is this daemon's own, so anything under it belongs to a
/// daemon which is gone, and a mount point names the device it was made from.
///
/// Only devices named that way are deleted, and only once the kernel confirms
/// the process which served them is gone. Another application's abandoned
/// device may be one it intends to recover, and is not this daemon's to reap.
async fn reclaim(
    control: &Control,
    dir: &std::path::Path,
    timeout: std::time::Duration,
) -> anyhow::Result<()> {
    let mounts = std::fs::read_to_string("/proc/mounts").context("reading /proc/mounts")?;

    for point in mounts.lines().filter_map(mount_point) {
        if !point.starts_with(dir) {
            continue;
        }
        tracing::warn!(
            ?point,
            "unmounting a disk which a previous daemon left behind"
        );

        () = crate::filesystem::unmount(&point, timeout)
            .await
            .with_context(|| format!("reclaiming {point:?}"))?;

        let Some(dev_id) = point
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_prefix(MOUNT_PREFIX))
            .and_then(|dev_id| dev_id.parse().ok())
        else {
            continue;
        };
        () = delete_abandoned(control, dev_id)?;
    }
    Ok(())
}

/// Delete `dev_id` if the process which served it is gone.
///
/// A live server means the kernel has already given the number to a device
/// which is not the one just unmounted.
fn delete_abandoned(control: &Control, dev_id: u32) -> anyhow::Result<()> {
    let Some(info) = control.dev_info(dev_id)? else {
        return Ok(());
    };
    if serving(info.ublksrv_pid) {
        tracing::warn!(
            dev_id,
            pid = info.ublksrv_pid,
            "left a device which is served"
        );
        return Ok(());
    }
    tracing::warn!(dev_id, "deleting a device whose server is gone");

    control
        .del_dev(dev_id)
        .with_context(|| format!("deleting abandoned ublk device {dev_id}"))
}

/// Whether `pid` names a live process.
fn serving(pid: i32) -> bool {
    if pid <= 0 {
        return false;
    }
    // SAFETY: signal zero performs the permission and existence checks of a
    // signal without sending one, and reads no user memory.
    unsafe { libc::kill(pid, 0) == 0 }
}

/// Mount point of a line of `/proc/mounts`, whose fields are separated by
/// spaces and escape the characters which would be ambiguous.
fn mount_point(line: &str) -> Option<std::path::PathBuf> {
    let point = line.split(' ').nth(1)?;

    let unescaped = point
        .replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\");

    Some(unescaped.into())
}

/// Fail unless this host can serve disks, naming what to do about it.
fn validate(args: &Args) -> anyhow::Result<()> {
    anyhow::ensure!(
        std::path::Path::new("/sys/module/ublk_drv").exists(),
        "the ublk_drv kernel module is not loaded, so no block device can be served. \
         Load it with `modprobe ublk_drv`",
    );
    anyhow::ensure!(
        std::path::Path::new("/dev/ublk-control").exists(),
        "/dev/ublk-control is absent though ublk_drv is loaded, so this kernel's module \
         was built without the control device",
    );
    () = filesystem::validate(filesystem::Type::Ext4)?;

    () = punches_holes(&args.image_dir).with_context(|| {
        format!(
            "the image directory {:?} cannot hold a sparse image",
            args.image_dir
        )
    })?;
    () = std::fs::create_dir_all(&args.mount_dir)
        .with_context(|| format!("creating {:?}", args.mount_dir))?;
    Ok(())
}

/// Fail unless `dir` is a filesystem which deallocates part of a file.
///
/// Without hole punching a disk's image grows to its whole device size and a
/// discard frees nothing, so the local copy of a 10 GiB disk holding 200 MiB
/// would occupy 10 GiB.
fn punches_holes(dir: &std::path::Path) -> anyhow::Result<()> {
    const BLOCK_SIZE: u32 = 4096;

    let mut image = crate::image::Image::create(dir, 1, BLOCK_SIZE)?;
    () = image.write_at(0, &vec![0xff; BLOCK_SIZE as usize])?;
    () = image.punch(0, 1)?;

    anyhow::ensure!(
        std::os::unix::fs::MetadataExt::blocks(&image.file().metadata()?) == 0,
        "a punched hole did not free the blocks behind it",
    );
    Ok(())
}
