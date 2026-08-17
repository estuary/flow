//! The daemon process: what it validates about its host, what it serves, and
//! how it stops.

use crate::args::Serve;
use crate::filesystem;
use crate::journal;
use crate::ublk::{Control, sys};
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
    /// When a disk opens a recovery horizon, and how fast it discharges one.
    pub horizon: crate::horizon::Policy,
    /// Shared by every session's journal writer.
    pub client: gazette::journal::Client,
    /// Routing table of `client`, swept periodically to close connections to
    /// brokers which no longer serve any disk.
    pub router: gazette::Router,
    /// Bytes every live disk holds, which each adds its own share to.
    pub footprint: crate::metrics::Footprint,
}

/// Prefix of a disk's mount point, which carries the device number so that a
/// later daemon can delete the device a mount it inherits was made from.
pub const MOUNT_PREFIX: &str = "disk-";

pub async fn run(args: Serve, registry: service_kit::Registry) -> anyhow::Result<()> {
    let control = std::sync::Arc::new(validate(&args).context("this host cannot serve disks")?);

    let (client, router) = journal::shared_client();

    let config = std::sync::Arc::new(Config {
        image_dir: args.image_dir.clone(),
        mount_dir: args.mount_dir.clone(),
        floor_label: args.floor_label.clone(),
        horizon: crate::horizon::Policy {
            open_ratio: args.horizon_open_ratio,
            copy_ratio: args.horizon_copy_ratio,
            minimum_bytes: args.horizon_minimum_bytes,
        },
        client,
        router,
        footprint: crate::metrics::Footprint::default(),
    });

    () = reclaim(&control, &args.mount_dir, crate::filesystem::MOUNT_TIMEOUT).await?;

    tracing::info!(
        image_dir = ?args.image_dir,
        mount_dir = ?args.mount_dir,
        ublks_max = control.ublks_max(),
        floor_label = args.floor_label,
        horizon = ?config.horizon,
        "disk daemon starting",
    );

    // SIGTERM (systemd) and SIGINT (interactive) both drain the daemon. One
    // token ends everything it runs, sessions included.
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

    if let Some(admin_port) = args.admin_port {
        let address = std::net::SocketAddr::from(([127, 0, 0, 1], admin_port));
        let (registry, draining) = (registry.clone(), draining.clone());

        tokio::spawn(async move {
            let outcome =
                service_kit::admin::serve("flow-disk-daemon", registry, address, async move {
                    draining.cancelled().await
                })
                .await;

            if let Err(err) = outcome {
                tracing::error!(?err, "admin surface exited with an error");
            }
        });
    }
    {
        let (router, draining) = (config.router.clone(), draining.clone());

        tokio::spawn(async move {
            while !ticked(&draining, SWEEP_INTERVAL).await {
                router.sweep();
            }
        });
    }
    tokio::spawn(crate::metrics::host(
        args.image_dir.clone(),
        control.clone(),
        config.footprint.clone(),
        draining.clone(),
    ));

    let service = crate::session::Service::new(config, control, registry.clone(), draining.clone());
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
    tracing::info!(socket = ?args.uds_path, "disk daemon is serving sessions");

    // Sessions are indefinite, so a drain ends them rather than waiting on
    // them. Each closes its stream only once its disk is torn down, which is
    // what the server then waits for and what the timeout bounds.
    let served = tokio::select! {
        served = &mut serving => served?,

        _ = draining.cancelled() => {
            // Unlinking first stops a client which reconnects from opening a
            // disk this daemon is about to stop serving.
            _ = std::fs::remove_file(&args.uds_path);

            tracing::info!(
                sessions = registry.snapshot().live.len(),
                timeout = ?DRAIN_TIMEOUT,
                "draining sessions",
            );

            match tokio::time::timeout(DRAIN_TIMEOUT, serving).await {
                Ok(served) => served?,
                Err(_elapsed) => {
                    let stuck = stuck(&registry);

                    anyhow::ensure!(
                        stuck.is_empty(),
                        "these disks did not tear down within the drain timeout, so their \
                         devices are left behind: {stuck:?}",
                    );
                    // No session outlived the drain, so no device was left
                    // behind and the wait was on a client which had not closed
                    // its connection. That is the client's to answer for.
                    tracing::warn!("a client connection outlived the drain timeout");
                    Ok(())
                }
            }
        }
    };
    _ = std::fs::remove_file(&args.uds_path);

    served.context("serving the session socket")
}

/// Sleep for `interval`, reporting true if the daemon began draining instead.
/// It is what makes a periodic task end promptly on a drain.
pub(crate) async fn ticked(
    draining: &tokio_util::sync::CancellationToken,
    interval: std::time::Duration,
) -> bool {
    tokio::select! {
        _ = draining.cancelled() => true,
        _ = tokio::time::sleep(interval) => false,
    }
}

/// The disks of every session which is still live, and what each is doing.
///
/// It names the thing an operator must go and look at, which a timeout alone
/// does not: a drain which does not finish leaves ublk devices and mounts on the
/// host for the next daemon to reclaim.
fn stuck(registry: &service_kit::Registry) -> Vec<(String, String)> {
    registry
        .snapshot()
        .live
        .into_iter()
        .map(|handler| (handler.label, handler.phase))
        .collect()
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

/// Fail unless this host can serve disks, naming what to do about it, and
/// return the control device that proved it.
///
/// Every one of these is otherwise found by a client's first session, at which
/// point the failure is a device which will not add or an image which quietly
/// occupies its whole logical size.
fn validate(args: &Serve) -> anyhow::Result<Control> {
    anyhow::ensure!(
        std::path::Path::new("/sys/module/ublk_drv").exists(),
        "the ublk_drv kernel module is not loaded, so no block device can be served. \
         Load it with `modprobe ublk_drv`",
    );
    anyhow::ensure!(
        !args.floor_label.is_empty(),
        "--floor-label names the journal-spec label a disk's recovery floor is written to, \
         and it has no default. Without one every recovery replays from the earliest \
         fragment and the journal a disk needs grows without bound",
    );
    let control = Control::open()?;

    let features = control.features().context(
        "this kernel's ublk_drv does not answer UBLK_U_CMD_GET_FEATURES, so it is older \
         than Linux 6.1",
    )?;
    anyhow::ensure!(
        features & sys::UBLK_F_USER_COPY != 0,
        "this kernel's ublk_drv implements features {features:#x}, without the \
         UBLK_F_USER_COPY of Linux 6.2 which is how a disk's request data moves",
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

    Ok(control)
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
