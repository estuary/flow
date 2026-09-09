//! The guest's scratch disk: an unnamed file that dies with the helper.
//!
//! `O_TMPFILE` means SIGKILL of the helper returns the blocks immediately,
//! with no cleanup path for the runtime to get wrong. The cost is that the
//! only way to name it - for `mkfs.ext4` and for libkrun - is `/proc/self/fd/N`,
//! so the descriptor must not be `O_CLOEXEC`: `mkfs.ext4` resolves that path
//! in its own process.

use std::os::fd::RawFd;
use std::path::Path;

const MIB: u64 = 1024 * 1024;

/// Options chosen so nothing is left for the guest to finish. Lazy inode-table
/// init would otherwise have the guest's `ext4lazyinit` thread write into the
/// sparse file after mount, growing the host's backing store unpredictably;
/// `lazy_itable_init=0` makes mke2fs flag every group ITABLE_ZEROED up front
/// instead. The journal is dropped outright (the disk never outlives the VM),
/// which also makes `lazy_journal_init` moot, and the reserved-block percentage
/// is zeroed because there is no root-versus-user distinction in the guest.
///
/// Measured on a 1024 MiB image: 664 KiB allocated, all nine groups
/// ITABLE_ZEROED.
const MKFS_ARGS: &[&str] = &[
    "-E",
    "lazy_itable_init=0",
    "-O",
    "^has_journal",
    "-m",
    "0",
    "-q",
    "-F",
];

pub fn create(backing_dir: &Path, disk_mib: u64) -> anyhow::Result<RawFd> {
    let dir = std::ffi::CString::new(backing_dir.as_os_str().as_encoded_bytes())?;

    // Safety: `dir` outlives the call and the flags are a valid O_TMPFILE open.
    let fd = unsafe {
        libc::open(
            dir.as_ptr(),
            libc::O_TMPFILE | libc::O_RDWR | libc::O_EXCL,
            0o600,
        )
    };
    if fd < 0 {
        return Err(anyhow::anyhow!(std::io::Error::last_os_error())
            .context(format!("O_TMPFILE in {}", backing_dir.display())));
    }

    // Safety: `fd` is open and owned by this process.
    if unsafe { libc::ftruncate(fd, (disk_mib * MIB) as libc::off_t) } < 0 {
        return Err(anyhow::anyhow!(std::io::Error::last_os_error())
            .context(format!("sizing the scratch disk to {disk_mib} MiB")));
    }

    let path = proc_path(fd);
    let status = std::process::Command::new("mkfs.ext4")
        .args(MKFS_ARGS)
        .arg(&path)
        .status()
        .map_err(|e| anyhow::anyhow!("running mkfs.ext4: {e}"))?;
    if !status.success() {
        anyhow::bail!("mkfs.ext4 on the scratch disk: {status}");
    }
    Ok(fd)
}

/// The only name an `O_TMPFILE` descriptor has. libkrun re-opens it, which
/// works because the helper still holds the descriptor.
pub fn proc_path(fd: RawFd) -> String {
    format!("/proc/self/fd/{fd}")
}
