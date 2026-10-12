use anyhow::Context;
use std::path::Path;

/// Options a disk is mounted with, fresh or recovered.
///
/// `noatime` keeps reads from creating deltas. `discard` returns freed blocks to
/// the image. If a sandbox re-exports the directory, it must apply `nodev`,
/// `nosuid`, and `noexec` again: host mount options do not propagate through a
/// bind or `virtio-fs` mount.
const MOUNT_OPTIONS: &str = "noatime,nodev,nosuid,noexec,discard";

/// Format `device`.
pub async fn format(device: &Path, peer: Option<(u32, u32)>) -> anyhow::Result<()> {
    // Set ownership during format to avoid a separate metadata mutation.
    let mut extended = String::from("nodiscard,assume_storage_prezeroed=1");

    if let Some((uid, gid)) = peer {
        extended.push_str(&format!(",root_owner={uid}:{gid}"));
    }

    let mut command = async_process::Command::new("mkfs.ext4");
    command
        .args(["-F", "-b"])
        .arg(crate::BLOCK_SIZE.to_string())
        .args(["-m", "0", "-E", &extended])
        .arg(device);

    run(command).await
}

/// A mounted filesystem.
pub struct Mount {
    path: std::path::PathBuf,
    mounted: bool,
}

impl Mount {
    /// Mount `device` at `path`, and give its root directory to `peer` if another
    /// user holds it, as a disk recovered from an earlier client may.
    pub async fn new(
        device: &Path,
        path: std::path::PathBuf,
        peer: Option<(u32, u32)>,
    ) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&path).with_context(|| format!("creating {path:?}"))?;

        let mut command = async_process::Command::new("mount");
        command
            .args(["-t", "ext4", "-o", MOUNT_OPTIONS])
            .arg(device)
            .arg(&path);
        () = run(command).await?;

        // Built before the change below, so that a failure of it still unmounts.
        let mount = Self {
            path,
            mounted: true,
        };

        if let Some((uid, gid)) = peer {
            let stat = std::fs::metadata(&mount.path)
                .with_context(|| format!("reading {:?}", mount.path))?;

            let held = (
                std::os::unix::fs::MetadataExt::uid(&stat),
                std::os::unix::fs::MetadataExt::gid(&stat),
            );

            if held != (uid, gid) {
                () = std::os::unix::fs::chown(&mount.path, Some(uid), Some(gid))
                    .with_context(|| format!("giving {:?} to {uid}:{gid}", mount.path))?;
            }
        }
        Ok(mount)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Unmount and remove the mount point. Idempotent.
    ///
    /// A filesystem which will not unmount is detached instead. The device under it
    /// is stopping or already gone.
    pub async fn unmount(&mut self) -> anyhow::Result<()> {
        if !std::mem::take(&mut self.mounted) {
            return Ok(());
        }
        let mut command = async_process::Command::new("umount");
        command.arg(&self.path);

        let outcome = match run(command).await {
            Ok(()) => Ok(()),
            Err(err) => {
                let mut command = async_process::Command::new("umount");
                command.arg("-l").arg(&self.path);

                run(command)
                    .await
                    .with_context(|| format!("detaching {:?} after: {err:#}", self.path))
            }
        };
        _ = std::fs::remove_dir(&self.path);

        outcome
    }
}

impl Drop for Mount {
    fn drop(&mut self) {
        if !std::mem::take(&mut self.mounted) {
            return;
        }
        // Only an attachment which failed while it was opening reaches Drop, so an
        // unmount here is not expected to block.
        let outcome = std::process::Command::new("umount")
            .arg(&self.path)
            .status();

        match outcome {
            Ok(status) if status.success() => _ = std::fs::remove_dir(&self.path),
            outcome => tracing::error!(path = ?self.path, ?outcome, "failed to unmount"),
        }
    }
}

/// Flush every filesystem write of the mount at `path` to its device.
pub fn sync(path: &Path) -> anyhow::Result<()> {
    let dir = std::fs::File::open(path).with_context(|| format!("opening {path:?}"))?;

    // SAFETY: `dir` holds the descriptor open across the call, which reads no
    // user memory.
    let rc = unsafe { libc::syncfs(std::os::fd::AsRawFd::as_raw_fd(&dir)) };

    if rc != 0 {
        return Err(std::io::Error::last_os_error()).with_context(|| format!("syncing {path:?}"));
    }
    Ok(())
}

/// Run `command` to completion, and require that it succeed.
async fn run(mut command: async_process::Command) -> anyhow::Result<()> {
    let what = format!("{command:?}");
    let output = async_process::output(&mut command)
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
