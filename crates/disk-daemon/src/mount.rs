use anyhow::Context;

/// Root of the filesystem mounted in a disk's directory `dir`.
pub(crate) fn root(dir: &std::path::Path) -> std::path::PathBuf {
    dir.join("fs")
}

const REQUIRED: [&str; 5] = [
    "nodev",
    "nosuid",
    "noexec",
    "nosymfollow",
    "errors=remount-ro",
];

/// Mount the filesystem on `device` in `dir`, at its `root`, for the client
/// `owner`.
pub(crate) fn mount(
    dir: &std::path::Path,
    device: &str,
    fstype: &str,
    options: &str,
    owner: u32,
) -> anyhow::Result<()> {
    let (flags, data) = parse_options(fstype, options)?;
    let root = root(dir);

    () = std::os::unix::fs::DirBuilderExt::mode(&mut std::fs::DirBuilder::new(), 0o700)
        .create(dir)
        .with_context(|| format!("creating {dir:?}"))?;
    () = std::fs::create_dir(&root).with_context(|| format!("creating {root:?}"))?;

    if let Err(err) = rustix::mount::mount(device, &root, fstype, flags, data.as_c_str()) {
        _ = std::fs::remove_dir_all(dir);

        return Err(crate::invalid_argument(format!(
            "mounting {device} as {fstype} with options {options:?}: {err}"
        )));
    }
    std::os::unix::fs::chown(dir, Some(owner), None)
        .with_context(|| format!("giving {dir:?} to uid {owner}"))
}

/// Map a `Mount`'s `fstype` and `options` to the flags and data of mount(2),
/// refusing any the daemon doesn't support.
fn parse_options(
    fstype: &str,
    options: &str,
) -> anyhow::Result<(rustix::mount::MountFlags, std::ffi::CString)> {
    if fstype != "ext4" {
        return Err(crate::invalid_argument(format!(
            "{fstype:?} is not a supported filesystem"
        )));
    }

    let requested: Vec<&str> = options.split(',').collect();
    if let Some(missing) = REQUIRED
        .into_iter()
        .find(|required| !requested.contains(required))
    {
        return Err(crate::invalid_argument(format!(
            "mount option {missing:?} is required"
        )));
    }

    // mount(2) takes generic options as flags, and ext4's own as a string.
    let mut flags = rustix::mount::MountFlags::empty();
    let mut ext4_options = Vec::new();

    for option in requested {
        match option {
            "nodev" => flags |= rustix::mount::MountFlags::NODEV,
            "nosuid" => flags |= rustix::mount::MountFlags::NOSUID,
            "noexec" => flags |= rustix::mount::MountFlags::NOEXEC,
            "nosymfollow" => flags |= rustix::mount::MountFlags::NOSYMFOLLOW,
            "noatime" => flags |= rustix::mount::MountFlags::NOATIME,
            "errors=remount-ro" | "discard" => ext4_options.push(option),
            other => {
                return Err(crate::invalid_argument(format!(
                    "mount option {other:?} is not supported"
                )));
            }
        }
    }

    let data = std::ffi::CString::new(ext4_options.join(","))
        .expect("ext4 options are fixed strings without NUL");
    Ok((flags, data))
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

#[cfg(test)]
mod test {
    #[test]
    fn test_parse_options() {
        let cases = [
            ("ext4", "nodev,nosuid,noexec,nosymfollow,errors=remount-ro"),
            (
                "ext4",
                "discard,errors=remount-ro,nosymfollow,noexec,nosuid,nodev,noatime",
            ),
            ("ext4", "nodev,nosuid,noexec,nosymfollow,errors=remount-ro,"),
            ("ext2", "nodev,nosuid,noexec,nosymfollow,errors=remount-ro"),
            ("ext4", "noatime,discard"),
            (
                "ext4",
                "nodev,nosuid,noexec,nosymfollow,errors=remount-ro,errors=panic",
            ),
        ];
        let parsed: Vec<_> = cases
            .into_iter()
            .map(|(fstype, options)| {
                let parsed = super::parse_options(fstype, options).map_err(|err| err.to_string());
                (fstype, options, parsed)
            })
            .collect();

        insta::assert_debug_snapshot!(parsed, @r#"
        [
            (
                "ext4",
                "nodev,nosuid,noexec,nosymfollow,errors=remount-ro",
                Ok(
                    (
                        MountFlags(
                            NODEV | NOEXEC | NOSUID | NOSYMFOLLOW,
                        ),
                        "errors=remount-ro",
                    ),
                ),
            ),
            (
                "ext4",
                "discard,errors=remount-ro,nosymfollow,noexec,nosuid,nodev,noatime",
                Ok(
                    (
                        MountFlags(
                            NOATIME | NODEV | NOEXEC | NOSUID | NOSYMFOLLOW,
                        ),
                        "discard,errors=remount-ro",
                    ),
                ),
            ),
            (
                "ext4",
                "nodev,nosuid,noexec,nosymfollow,errors=remount-ro,",
                Err(
                    "mount option \"\" is not supported (InvalidArgument)",
                ),
            ),
            (
                "ext2",
                "nodev,nosuid,noexec,nosymfollow,errors=remount-ro",
                Err(
                    "\"ext2\" is not a supported filesystem (InvalidArgument)",
                ),
            ),
            (
                "ext4",
                "noatime,discard",
                Err(
                    "mount option \"nodev\" is required (InvalidArgument)",
                ),
            ),
            (
                "ext4",
                "nodev,nosuid,noexec,nosymfollow,errors=remount-ro,errors=panic",
                Err(
                    "mount option \"errors=panic\" is not supported (InvalidArgument)",
                ),
            ),
        ]
        "#);
    }
}
