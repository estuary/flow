//! Formatting and mounting the filesystem a disk presents.
//!
//! This module decides the type, and nothing else does. No detail of a
//! filesystem reaches the journal, the chunk codec, or the bitmaps, so the
//! durable format is already type-agnostic. A type touches only three things: the
//! invocation which formats a fresh device, the options it is mounted with, and
//! the assumption that a free produces a discard.

use anyhow::Context;
use std::path::Path;

/// Filesystem of a disk.
///
/// ext4 is the only implementation, and no configuration exposes the choice.
/// Another type would have to be committed to the full crash matrix. ext4 is
/// right for a disk for two reasons. `assume_storage_prezeroed` lets `mkfs` leave
/// the unused inode tables and the internal journal as holes, which keeps the
/// first delta small. Metadata-only journaling keeps down the journal appends a
/// rewrite costs.
#[derive(Clone, Copy, Debug)]
pub enum Type {
    Ext4,
}

/// Smallest `mkfs` this daemon formats with, which is the first to support
/// `assume_storage_prezeroed`.
const MIN_MKFS_VERSION: (u32, u32) = (1, 47);

/// Invocation which formats a fresh device.
///
/// The filesystem block size is the daemon's own, so a device request never
/// straddles a block and every mutation covers whole blocks. Reserved blocks are
/// zero. No privileged user recovers a full disk by spending them. `nodiscard`
/// keeps the format from discarding a device which is already entirely holes.
fn mkfs(fs: Type, device: &Path) -> async_process::Command {
    match fs {
        Type::Ext4 => {
            let mut command = async_process::Command::new("mkfs.ext4");
            command
                .args(["-F", "-b"])
                .arg(crate::BLOCK_SIZE.to_string())
                .args(["-m", "0", "-E", "nodiscard,assume_storage_prezeroed=1"])
                .arg(device);
            command
        }
    }
}

/// Options a disk of `fs` is mounted with, fresh or recovered.
///
/// `noatime` keeps reads from creating deltas. `discard` returns freed blocks to
/// the image. The sandbox which re-exports the directory applies `nodev`,
/// `nosuid`, and `noexec` again. Host mount options do not propagate through a
/// bind or `virtio-fs` mount.
fn mount_options(fs: Type) -> &'static str {
    match fs {
        Type::Ext4 => "noatime,nodev,nosuid,noexec,discard",
    }
}

/// How long a fresh disk's format may take. A format of a large device writes
/// its metadata, so this bounds a hang rather than a slow disk.
pub const MKFS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

/// How long a mount or unmount may take.
pub const MOUNT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Fail unless this host can format `fs`.
pub fn validate(fs: Type) -> anyhow::Result<()> {
    let Type::Ext4 = fs;

    let output = std::process::Command::new("mkfs.ext4")
        .arg("-V")
        .output()
        .context("running `mkfs.ext4 -V`, which a fresh disk is formatted with")?;

    // mke2fs reports its version on stderr, like `mke2fs 1.47.0 (5-Feb-2023)`.
    let reported = String::from_utf8_lossy(&output.stderr);
    let version = reported
        .split_whitespace()
        .nth(1)
        .and_then(parse_version)
        .with_context(|| format!("`mkfs.ext4 -V` reported no version: {reported}"))?;

    anyhow::ensure!(
        version >= MIN_MKFS_VERSION,
        "e2fsprogs {}.{} is older than the {}.{} which supports assume_storage_prezeroed",
        version.0,
        version.1,
        MIN_MKFS_VERSION.0,
        MIN_MKFS_VERSION.1,
    );
    Ok(())
}

/// Format `device`, which must be a fresh disk. Recovery never formats. It
/// replays filesystem structures as the data they are.
pub async fn format(fs: Type, device: &Path, timeout: std::time::Duration) -> anyhow::Result<()> {
    run(mkfs(fs, device), timeout).await
}

/// A mounted filesystem.
///
/// Both `Drop` and [`Mount::unmount`] unmount it, so no failure path leaves one
/// behind.
pub struct Mount {
    path: std::path::PathBuf,
    mounted: bool,
}

impl Mount {
    pub async fn new(
        fs: Type,
        device: &Path,
        path: std::path::PathBuf,
        timeout: std::time::Duration,
    ) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&path).with_context(|| format!("creating {path:?}"))?;

        let mut command = async_process::Command::new("mount");
        command
            .args(["-t", type_name(fs), "-o", mount_options(fs)])
            .arg(device)
            .arg(&path);

        () = run(command, timeout).await?;

        Ok(Self {
            path,
            mounted: true,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Unmount and remove the mount point. Idempotent.
    pub async fn unmount(&mut self, timeout: std::time::Duration) -> anyhow::Result<()> {
        if !std::mem::take(&mut self.mounted) {
            return Ok(());
        }
        unmount(&self.path, timeout).await
    }
}

impl Drop for Mount {
    fn drop(&mut self) {
        if !std::mem::take(&mut self.mounted) {
            return;
        }
        // Only a session which failed while it was opening reaches Drop, so an
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

/// Unmount `path` and remove the mount point.
///
/// A filesystem which will not unmount is detached instead. The device under it
/// is stopping or already gone. A mount over a device which cannot complete a
/// write would never come off any other way.
pub async fn unmount(path: &Path, timeout: std::time::Duration) -> anyhow::Result<()> {
    let mut command = async_process::Command::new("umount");
    command.arg(path);

    let outcome = match run(command, timeout).await {
        Ok(()) => Ok(()),
        Err(err) => {
            let mut command = async_process::Command::new("umount");
            command.arg("-l").arg(path);

            run(command, timeout)
                .await
                .with_context(|| format!("detaching {path:?} after: {err:#}"))
        }
    };
    _ = std::fs::remove_dir(path);

    outcome
}

/// Flush every filesystem write of the mount at `path` to its device.
///
/// The daemon issues this itself. A publication must not depend on how a client's
/// own `fsync` propagates through a bind or `virtio-fs` mount.
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

fn type_name(fs: Type) -> &'static str {
    match fs {
        Type::Ext4 => "ext4",
    }
}

/// Run `command` to completion, failing if it does not finish within `timeout`.
///
/// A timed-out command is killed. Dropping the child of `async_process::output`
/// signals it.
async fn run(
    mut command: async_process::Command,
    timeout: std::time::Duration,
) -> anyhow::Result<()> {
    let output = match tokio::time::timeout(timeout, async_process::output(&mut command)).await {
        Err(_elapsed) => anyhow::bail!("{command:?} did not finish within {timeout:?}"),
        Ok(output) => output.with_context(|| format!("running {command:?}"))?,
    };

    anyhow::ensure!(
        output.status.success(),
        "{command:?} failed ({}): {}{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    Ok(())
}

/// Parse the leading `major.minor` of a version string.
fn parse_version(version: &str) -> Option<(u32, u32)> {
    let mut parts = version.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;

    Some((major, minor))
}

#[cfg(test)]
mod test {
    use super::{Type, mkfs, mount_options, parse_version};

    #[test]
    fn test_a_fresh_format_keeps_the_image_sparse() {
        let command = mkfs(Type::Ext4, std::path::Path::new("/dev/ublkb7"));
        let rendered = format!("{command:?}");

        assert!(
            rendered.contains("assume_storage_prezeroed=1"),
            "{rendered}"
        );
        assert!(rendered.contains("nodiscard"), "{rendered}");
        assert!(rendered.contains("\"-b\" \"4096\""), "{rendered}");
        assert!(rendered.contains("\"-m\" \"0\""), "{rendered}");
    }

    #[test]
    fn test_mount_options_are_fixed() {
        assert_eq!(
            mount_options(Type::Ext4),
            "noatime,nodev,nosuid,noexec,discard"
        );
    }

    #[test]
    fn test_versions_parse_and_order() {
        assert_eq!(parse_version("1.47.0"), Some((1, 47)));
        assert_eq!(parse_version("1.46.5"), Some((1, 46)));
        assert_eq!(parse_version("mke2fs"), None);

        assert!(parse_version("1.47.0") > parse_version("1.46.5"));
    }
}
