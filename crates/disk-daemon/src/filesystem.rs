//! Formatting and mounting the filesystem a disk presents.
//!
//! The filesystem is ext4, and only this module knows it. No detail of a
//! filesystem reaches the journal, the chunk codec, or the bitmaps, so the
//! durable format is type-agnostic already: another type would touch only the
//! three things which are here — the invocation which formats a fresh device,
//! the options it is mounted with, and the assumption that a free produces a
//! discard.
//!
//! ext4 is right for a disk for two reasons. `assume_storage_prezeroed` lets
//! `mkfs` leave the unused inode tables and ext4's internal journal as holes, which
//! keeps the first delta small. Metadata-only journaling keeps down the Gazette
//! appends a rewrite costs. Another type would have to be committed to the full
//! crash matrix, so no configuration exposes the choice.

use anyhow::Context;
use std::path::Path;

/// Invocation which formats a fresh device.
///
/// The filesystem block size is the daemon's own, so a device request never
/// straddles a block and every mutation covers whole blocks. Reserved blocks are
/// zero. No privileged user recovers a full disk by spending them. `nodiscard`
/// keeps the format from discarding a device which is already entirely holes.
pub(crate) fn mkfs(device: &Path, peer: Option<(u32, u32)>) -> async_process::Command {
    // `root_owner` gives the root directory to the client as the filesystem is
    // made. A `chown` after the format would be a write, and a disk which is
    // opened and never written must append nothing at all.
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
    command
}

/// Options a disk is mounted with, fresh or recovered.
///
/// `noatime` keeps reads from creating deltas. `discard` returns freed blocks to
/// the image. The sandbox which re-exports the directory applies `nodev`,
/// `nosuid`, and `noexec` again. Host mount options do not propagate through a
/// bind or `virtio-fs` mount.
pub const MOUNT_OPTIONS: &str = "noatime,nodev,nosuid,noexec,discard";

/// How long a fresh disk's format may take. A format of a large device writes
/// its metadata, so this bounds a hang rather than a slow disk.
pub const MKFS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

/// How long a mount or unmount may take.
pub const MOUNT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Format `device`, which must be a fresh disk. Recovery never formats. It
/// replays filesystem structures as the data they are.
///
/// `peer` receives the root directory of the new filesystem, so that a client needs
/// no privilege to use the mount it is given.
pub async fn format(
    device: &Path,
    peer: Option<(u32, u32)>,
    timeout: std::time::Duration,
) -> anyhow::Result<()> {
    run(mkfs(device, peer), timeout).await
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
    /// Mount `device` at `path`, and give its root directory to `peer` if it
    /// belongs to somebody else.
    ///
    /// [`format()`] already gives that directory to the client of a fresh disk, so
    /// this changes nothing there. It repairs a recovered disk whose filesystem was
    /// formatted for a different client, which would otherwise receive a mount it
    /// cannot write. `peer` is absent only when the transport carries no peer
    /// credential.
    ///
    /// The change is conditional because it is a write. A recovered disk is already
    /// serving, so that write joins its next delta.
    ///
    /// A mount which outlasts `timeout` is released rather than killed, per
    /// [`settle`], and fails.
    pub async fn new(
        device: &Path,
        path: std::path::PathBuf,
        peer: Option<(u32, u32)>,
        timeout: std::time::Duration,
        release: impl FnOnce(),
    ) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&path).with_context(|| format!("creating {path:?}"))?;

        let mut command = async_process::Command::new("mount");
        command
            .args(["-t", "ext4", "-o", MOUNT_OPTIONS])
            .arg(device)
            .arg(&path);

        let what = format!("{command:?}");
        () = settle(
            &what,
            async_process::output(&mut command),
            timeout,
            release,
            || unmount(&path, timeout),
        )
        .await?;

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
        // Only a tenure which failed while it was opening reaches Drop, so an
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

/// Await `mounting`, the mount `what` names, and succeed only if it lands within
/// `timeout`.
///
/// A mount which outlasts `timeout` is waiting on its device, which waits on a
/// recording channel nothing takes from. It is not killed, because a mount killed
/// within the kernel may land after this has given up on it, and nothing would
/// unmount it. A device still mounted can never be deleted. `release` instead frees
/// what the device waits on, and the mount is awaited as long again, then undone by
/// `unmount` if it landed. It fails either way.
async fn settle<U>(
    what: &str,
    mounting: impl Future<Output = std::io::Result<async_process::Output>>,
    timeout: std::time::Duration,
    release: impl FnOnce(),
    unmount: impl FnOnce() -> U,
) -> anyhow::Result<()>
where
    U: Future<Output = anyhow::Result<()>>,
{
    let mut mounting = std::pin::pin!(mounting);

    let output = match tokio::time::timeout(timeout, &mut mounting).await {
        Ok(output) => output,
        Err(_elapsed) => {
            () = release();
            let stalled = format!("{what} did not finish within {timeout:?}");

            return Err(match tokio::time::timeout(timeout, &mut mounting).await {
                // Returning drops `mounting`, which kills the mount. It may land
                // yet, and nothing more can be done about it here.
                Err(_elapsed) => anyhow::anyhow!("{stalled}, nor once released"),
                Ok(output) => match succeeded(what, output) {
                    Err(err) => err.context(stalled),
                    Ok(()) => match unmount().await {
                        Ok(()) => anyhow::anyhow!("{stalled}, and was undone once it had"),
                        Err(err) => err.context(format!("{stalled}, and landed late")),
                    },
                },
            });
        }
    };
    succeeded(what, output)
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
/// The daemon issues this itself. A prepare must not depend on how a client's
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

/// Run `command` to completion, failing if it does not finish within `timeout`.
///
/// A timed-out command is killed. Dropping the child of `async_process::output`
/// signals it.
async fn run(
    mut command: async_process::Command,
    timeout: std::time::Duration,
) -> anyhow::Result<()> {
    match tokio::time::timeout(timeout, async_process::output(&mut command)).await {
        Err(_elapsed) => anyhow::bail!("{command:?} did not finish within {timeout:?}"),
        Ok(output) => succeeded(&format!("{command:?}"), output),
    }
}

/// Whether the command `what` names ran and exited successfully, per `output`.
fn succeeded(what: &str, output: std::io::Result<async_process::Output>) -> anyhow::Result<()> {
    let output = output.with_context(|| format!("running {what}"))?;

    anyhow::ensure!(
        output.status.success(),
        "{what} failed ({}): {}{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    Ok(())
}

#[cfg(test)]
mod test {
    use super::{MOUNT_OPTIONS, mkfs, settle};
    use std::fmt::Write as _;

    #[test]
    fn test_a_format_gives_the_root_directory_to_its_client() {
        let owned = mkfs(std::path::Path::new("/dev/ublkb7"), Some((1000, 100)));
        let rendered = format!("{owned:?}");

        assert!(rendered.contains("root_owner=1000:100"), "{rendered}");

        let unowned = mkfs(std::path::Path::new("/dev/ublkb7"), None);
        let rendered = format!("{unowned:?}");

        assert!(!rendered.contains("root_owner"), "{rendered}");
    }

    #[test]
    fn test_a_fresh_format_keeps_the_image_sparse() {
        let command = mkfs(std::path::Path::new("/dev/ublkb7"), None);
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
        assert_eq!(MOUNT_OPTIONS, "noatime,nodev,nosuid,noexec,discard");
    }

    /// How a mount in flight goes, in each of the ways [`settle`] tells apart.
    #[derive(Debug, Clone, Copy)]
    enum Mounting {
        Lands,
        Fails,
        /// Waits on its device until released, and then lands.
        LandsOnceReleased,
        /// Lands once released, and then will not unmount.
        LandsOnceReleasedAndSticks,
        FailsOnceReleased,
        /// Waits on its device even once released.
        Hangs,
    }

    fn exited(code: i32) -> async_process::Output {
        async_process::Output {
            status: std::os::unix::process::ExitStatusExt::from_raw(code << 8),
            stdout: Vec::new(),
            stderr: if code == 0 {
                Vec::new()
            } else {
                b"wrong fs type".to_vec()
            },
        }
    }

    /// A mount which stalls on its device is released rather than killed, and one
    /// which then lands is unmounted before the mount fails. A mount killed within
    /// the kernel could land after its caller had given up on it, and a device still
    /// mounted can never be deleted.
    #[tokio::test]
    async fn test_a_stalled_mount_is_released_and_undone_if_it_lands() {
        use Mounting::*;
        const TIMEOUT: std::time::Duration = std::time::Duration::from_millis(10);

        let mut out = String::new();

        for case in [
            Lands,
            Fails,
            LandsOnceReleased,
            LandsOnceReleasedAndSticks,
            FailsOnceReleased,
            Hangs,
        ] {
            let (freed, is_freed) = tokio::sync::oneshot::channel::<()>();
            let (released, unmounted) = (std::cell::Cell::new(false), std::cell::Cell::new(false));

            // A mount which waits on its device lands only once `release` frees it.
            let mounting = async {
                match case {
                    Lands => Ok(exited(0)),
                    Fails => Ok(exited(32)),
                    Hangs => std::future::pending().await,
                    LandsOnceReleased | LandsOnceReleasedAndSticks => {
                        _ = is_freed.await;
                        Ok(exited(0))
                    }
                    FailsOnceReleased => {
                        _ = is_freed.await;
                        Ok(exited(32))
                    }
                }
            };
            let release = || {
                released.set(true);
                _ = freed.send(());
            };
            let unmount = || async {
                unmounted.set(true);

                match case {
                    LandsOnceReleasedAndSticks => Err(anyhow::anyhow!("umount /mnt failed")),
                    _ => Ok(()),
                }
            };

            let settled = settle(
                "mount /dev/ublkb7 /mnt",
                mounting,
                TIMEOUT,
                release,
                unmount,
            )
            .await
            .map_err(|err| format!("{err:#}"));
            let case = format!("{case:?}");

            writeln!(
                out,
                "{case:<28}released {:<7}unmounted {:<7}{settled:?}",
                released.get(),
                unmounted.get(),
            )
            .unwrap();
        }
        insta::assert_snapshot!(out, @r#"
        Lands                       released false  unmounted false  Ok(())
        Fails                       released false  unmounted false  Err("mount /dev/ublkb7 /mnt failed (exit status: 32): wrong fs type")
        LandsOnceReleased           released true   unmounted true   Err("mount /dev/ublkb7 /mnt did not finish within 10ms, and was undone once it had")
        LandsOnceReleasedAndSticks  released true   unmounted true   Err("mount /dev/ublkb7 /mnt did not finish within 10ms, and landed late: umount /mnt failed")
        FailsOnceReleased           released true   unmounted false  Err("mount /dev/ublkb7 /mnt did not finish within 10ms: mount /dev/ublkb7 /mnt failed (exit status: 32): wrong fs type")
        Hangs                       released true   unmounted false  Err("mount /dev/ublkb7 /mnt did not finish within 10ms, nor once released")
        "#);
    }
}
