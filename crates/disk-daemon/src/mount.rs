use anyhow::Context;

/// Root of the filesystem mounted in a disk's directory `dir`.
pub(crate) fn root(dir: &std::path::Path) -> std::path::PathBuf {
    dir.join("fs")
}

/// Mount the filesystem on `device` in `dir`, at its `root`, for the client
/// `owner`.
pub(crate) fn mount(
    dir: &std::path::Path,
    device: &str,
    fstype: &str,
    options: &str,
    owner: u32,
) -> anyhow::Result<()> {
    // Only a block filesystem mounts `device`: other types ignore it and mount
    // something else. A type the kernel hasn't loaded is refused, so a client
    // can't have it load a module.
    let filesystems =
        std::fs::read_to_string("/proc/filesystems").context("reading /proc/filesystems")?;
    if !filesystems
        .lines()
        .any(|line| line.strip_prefix('\t') == Some(fstype))
    {
        return Err(crate::invalid_argument(format!(
            "{fstype:?} is not a block filesystem which the host has loaded"
        )));
    }

    let mut flags = rustix::mount::MountFlags::NODEV | rustix::mount::MountFlags::NOSUID;
    let mut discard = false;

    // mount(2) takes mount(8)'s generic options as flags, apart from the
    // filesystem's own. Of the filesystem's own, only `discard` is accepted:
    // others can name a device or path for root to open, or panic the host.
    for option in options.split(',').filter(|option| !option.is_empty()) {
        match option {
            "dirsync" => flags |= rustix::mount::MountFlags::DIRSYNC,
            "lazytime" => flags |= rustix::mount::MountFlags::LAZYTIME,
            "noatime" => flags |= rustix::mount::MountFlags::NOATIME,
            "nodiratime" => flags |= rustix::mount::MountFlags::NODIRATIME,
            "noexec" => flags |= rustix::mount::MountFlags::NOEXEC,
            "relatime" => flags |= rustix::mount::MountFlags::RELATIME,
            "ro" => flags |= rustix::mount::MountFlags::RDONLY,
            "strictatime" => flags |= rustix::mount::MountFlags::STRICTATIME,
            "sync" => flags |= rustix::mount::MountFlags::SYNCHRONOUS,
            "nodev" | "nosuid" => (),
            "discard" => discard = true,
            other => {
                return Err(crate::invalid_argument(format!(
                    "mount option {other:?} is not supported"
                )));
            }
        }
    }

    let root = root(dir);

    () = std::os::unix::fs::DirBuilderExt::mode(&mut std::fs::DirBuilder::new(), 0o700)
        .create(dir)
        .with_context(|| format!("creating {dir:?}"))?;
    () = std::fs::create_dir(&root).with_context(|| format!("creating {root:?}"))?;

    let data = discard.then_some(c"discard");
    if let Err(err) = rustix::mount::mount(device, &root, fstype, flags, data) {
        _ = std::fs::remove_dir_all(dir);

        return Err(crate::invalid_argument(format!(
            "mounting {device} as {fstype} with options {options:?}: {err}"
        )));
    }
    std::os::unix::fs::chown(dir, Some(owner), None)
        .with_context(|| format!("giving {dir:?} to uid {owner}"))
}

/// Write the changes to the filesystem mounted in `dir` through to its device.
pub(crate) fn sync(dir: &std::path::Path) -> anyhow::Result<()> {
    let root = root(dir);
    let fd = rustix::fs::open(
        &root,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .with_context(|| format!("opening {root:?}"))?;

    rustix::fs::syncfs(&fd).with_context(|| format!("syncing {root:?}"))
}

/// Unmount and remove a disk's directory `dir`, if it exists.
pub(crate) fn unmount(dir: &std::path::Path) -> anyhow::Result<()> {
    let root = root(dir);

    // `DETACH` unmounts while the client holds files open, and `NOFOLLOW` skips
    // a symlink the client put at `root`.
    match rustix::mount::unmount(
        &root,
        rustix::mount::UnmountFlags::DETACH | rustix::mount::UnmountFlags::NOFOLLOW,
    ) {
        // `INVAL` and `NOENT` mean nothing is mounted at `root`.
        Ok(()) | Err(rustix::io::Errno::INVAL | rustix::io::Errno::NOENT) => (),
        Err(err) => return Err(anyhow::Error::new(err).context(format!("unmounting {root:?}"))),
    }

    // The client may have created files in `dir`. `remove_dir_all` follows no
    // symlink it planted.
    match std::fs::remove_dir_all(dir) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(anyhow::Error::new(err).context(format!("removing {dir:?}"))),
    }
}

/// Create `mount_dir`, and unmount and remove every disk's directory which a
/// daemon that didn't drain left in it. A mount left on an orphan keeps its device
/// number after the reaper deletes it, and a leftover directory fails the `Mount`
/// of a new device which takes its number.
pub(crate) fn reset_dir(mount_dir: &std::path::Path) -> anyhow::Result<()> {
    () = std::fs::create_dir_all(mount_dir).with_context(|| format!("creating {mount_dir:?}"))?;
    // Clients go through it to reach their own directories.
    () = std::fs::set_permissions(
        mount_dir,
        std::os::unix::fs::PermissionsExt::from_mode(0o755),
    )
    .with_context(|| format!("setting the mode of {mount_dir:?}"))?;

    for entry in std::fs::read_dir(mount_dir).with_context(|| format!("listing {mount_dir:?}"))? {
        let dir = entry
            .with_context(|| format!("listing {mount_dir:?}"))?
            .path();
        () = unmount(&dir)?;
        tracing::info!(?dir, "unmounted a disk which an earlier daemon left");
    }
    Ok(())
}
