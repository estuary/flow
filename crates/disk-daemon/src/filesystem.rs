use anyhow::Context;

/// Options a disk is mounted with, fresh or recovered:
///
/// - `noatime`: reads update no access times, so they create no deltas.
/// - `nodev`, `nosuid`, `noexec`: the client's files grant nothing on the host. A
///   sandbox which re-exports the directory must apply them again: host mount
///   options do not propagate through a bind or `virtio-fs` mount.
/// - `discard`: freed blocks return to the image.
/// - `errors=remount-ro`: the first error ext4 finds stops every write, and
///   `Mount::sync` then fails rather than commit a delta without them.
const MOUNT_OPTIONS: &str = "noatime,nodev,nosuid,noexec,discard,errors=remount-ro";

/// A mounted filesystem. It has no `Drop`: the attachment's `stop`, which runs
/// however serving ends, unmounts it.
pub struct Mount {
    path: std::path::PathBuf,
    /// ext4's count of the errors it has found on the disk, in sysfs.
    errors: std::path::PathBuf,
    /// That count once mounted, which a recovered disk carries from earlier mounts.
    errors_at_mount: u32,
}

impl Mount {
    /// Mount `device` at `path`, formatting it first if it's `fresh`, and give its
    /// root directory to `uid:gid` if another user holds it, as a disk recovered
    /// from an earlier client may.
    pub async fn new(
        device: std::path::PathBuf,
        path: std::path::PathBuf,
        (uid, gid): (u32, u32),
        fresh: bool,
    ) -> anyhow::Result<Self> {
        if fresh {
            // Set ownership during format to avoid a separate metadata mutation.
            let extended = format!("nodiscard,assume_storage_prezeroed=1,root_owner={uid}:{gid}");

            () = run(async_process::Command::new("mkfs.ext4")
                .args(["-F", "-b"])
                .arg(crate::BLOCK_SIZE.to_string())
                // A disk never grows, so it needs no blocks reserved for an online resize,
                // and its journal keeps every commit crash-consistent, so two backups of
                // the superblock and group descriptors suffice. Both cut what a format
                // writes.
                .args(["-O", "^resize_inode,sparse_super2"])
                .args(["-m", "0", "-E", &extended])
                .arg(&device))
            .await?;
        }
        std::fs::create_dir_all(&path).with_context(|| format!("creating {path:?}"))?;

        if let Err(err) = run(async_process::Command::new("mount")
            .args(["-t", "ext4", "-o", MOUNT_OPTIONS])
            .arg(&device)
            .arg(&path))
        .await
        {
            _ = std::fs::remove_dir(&path);
            return Err(err);
        }

        // ext4 names a filesystem's sysfs directory for its device.
        let name = device.file_name().expect("a block device has a name");
        let errors = std::path::Path::new("/sys/fs/ext4")
            .join(name)
            .join("errors_count");

        // On a blocking thread, since the `chown` may wait on the device, and the
        // device waits for the caller, which polls this future, to take its live chunks.
        let (root, errors_path) = (path.clone(), errors.clone());
        let errors_at_mount = tokio::task::spawn_blocking(move || {
            let stat = std::fs::metadata(&root).with_context(|| format!("reading {root:?}"))?;
            let held = (
                std::os::unix::fs::MetadataExt::uid(&stat),
                std::os::unix::fs::MetadataExt::gid(&stat),
            );
            // Only when another user holds it: a `chown` updates the inode's ctime even
            // when the owner is unchanged, and the next delta would record that.
            if held != (uid, gid) {
                () = std::os::unix::fs::chown(&root, Some(uid), Some(gid))
                    .with_context(|| format!("giving {root:?} to {uid}:{gid}"))?;
            }
            read_errors(&errors_path)
        })
        .await
        .map_err(anyhow::Error::from)
        .and_then(|errors_at_mount| errors_at_mount);

        match errors_at_mount {
            Ok(errors_at_mount) => Ok(Self {
                path,
                errors,
                errors_at_mount,
            }),
            Err(err) => {
                let mount = Self {
                    path,
                    errors,
                    errors_at_mount: 0,
                };
                Err(match mount.unmount().await {
                    Ok(()) => err,
                    Err(unmount_err) => unmount_err.context(format!("unmounting after: {err:#}")),
                })
            }
        }
    }

    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Flush every filesystem write of the mount to its device, failing if ext4
    /// has found an error since mounting. `syncfs` blocks until the device has
    /// served them, so it runs on a blocking thread, which a dropped call leaves
    /// to finish.
    pub fn sync(&self) -> impl Future<Output = anyhow::Result<()>> + use<> {
        let (path, errors, errors_at_mount) =
            (self.path.clone(), self.errors.clone(), self.errors_at_mount);

        async move {
            tokio::task::spawn_blocking(move || {
                let dir =
                    std::fs::File::open(&path).with_context(|| format!("opening {path:?}"))?;
                let synced = rustix::fs::syncfs(&dir).with_context(|| format!("syncing {path:?}"));

                // Before Linux 6.13, a `syncfs` of a filesystem which an error stopped
                // succeeds without writing what it holds. Later kernels fail it, with
                // only an errno. ext4's count of its errors tells on every kernel, so
                // it's checked first.
                anyhow::ensure!(
                    read_errors(&errors)? == errors_at_mount,
                    "ext4 recorded an error on {path:?} and stopped its writes: see the kernel log",
                );
                synced
            })
            .await?
        }
    }

    /// Unmount and remove the mount point.
    ///
    /// A filesystem which a client or sandbox still holds will not unmount, and is
    /// detached instead. Teardown stops the device next either way, and a filesystem
    /// left mounted would stay at its path, over a deleted device, until reboot.
    pub async fn unmount(self) -> anyhow::Result<()> {
        let outcome = match run(async_process::Command::new("umount").arg(&self.path)).await {
            Ok(()) => Ok(()),
            Err(err) => {
                // Before Linux 6.11, deleting the device waits on a detached filesystem's
                // last user, so this is what explains a teardown which stalls there.
                tracing::warn!(?err, path = ?self.path, "detaching a filesystem which would not unmount");

                run(async_process::Command::new("umount")
                    .arg("-l")
                    .arg(&self.path))
                .await
                .with_context(|| format!("detaching {:?} after: {err:#}", self.path))
            }
        };
        _ = std::fs::remove_dir(&self.path);

        outcome
    }
}

/// Read ext4's count of the errors it has found on a filesystem.
fn read_errors(path: &std::path::Path) -> anyhow::Result<u32> {
    let count = std::fs::read_to_string(path).with_context(|| format!("reading {path:?}"))?;
    count
        .trim()
        .parse()
        .with_context(|| format!("parsing {path:?} ({count:?})"))
}

/// Run `command` to completion, and require that it succeed. `async_process::output`
/// fails only if the command can't run, and leaves its exit status to the caller.
async fn run(command: &mut async_process::Command) -> anyhow::Result<()> {
    let what = format!("{command:?}");
    let output = async_process::output(command)
        .await
        .with_context(|| format!("running {what}"))?;

    anyhow::ensure!(
        output.status.success(),
        "{what} failed ({}): {}{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    Ok(())
}
