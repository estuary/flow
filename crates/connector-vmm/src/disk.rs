//! The guest's scratch disk: an unnamed file that dies with the VMM.
//!
//! `O_TMPFILE` means a SIGKILL of the VMM returns the blocks immediately, with
//! no cleanup path for the runtime to get wrong. The cost is that the only way
//! to name it - for `mkfs.ext4`, and for libkrun - is `/proc/self/fd/N`, so
//! the descriptor must not be `O_CLOEXEC`: `mkfs.ext4` resolves that path in
//! its own process. It belongs to this VM and is discarded with it; nothing
//! carries across a replacement VMM.

use std::os::fd::RawFd;
use std::path::Path;

const MIB: u64 = 1024 * 1024;

/// Options chosen so nothing is left for the guest to finish. Lazy inode-table
/// init would otherwise have the guest's `ext4lazyinit` thread write into the
/// sparse file after mount, growing the host's backing store unpredictably;
/// `lazy_itable_init=0` makes mke2fs flag every group ITABLE_ZEROED up front
/// instead. The journal is dropped outright, which also makes
/// `lazy_journal_init` moot, and the reserved-block percentage is zeroed
/// because there is no root-versus-user distinction in the guest.
const MKFS: &str = "mkfs.ext4";
const MKFS_OPTIONS: &[&str] = &[
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
    let dir = std::ffi::CString::new(backing_dir.as_os_str().as_encoded_bytes())
        .map_err(|e| anyhow::anyhow!("{} cannot be a C string: {e}", backing_dir.display()))?;

    // SAFETY: `dir` outlives the call and the flags are a valid O_TMPFILE open.
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
    // SAFETY: `fd` is open and owned by this process.
    if unsafe { libc::ftruncate(fd, (disk_mib * MIB) as libc::off_t) } < 0 {
        return Err(anyhow::anyhow!(std::io::Error::last_os_error())
            .context(format!("sizing the scratch disk to {disk_mib} MiB")));
    }

    let argv = mkfs_argv(&proc_path(fd));
    let (program, args) = argv.split_first().expect("mkfs_argv is never empty");
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    crate::sys::run(program, &args, None)?;

    Ok(fd)
}

pub fn mkfs_argv(path: &str) -> Vec<String> {
    let mut argv = vec![MKFS.to_string()];
    argv.extend(MKFS_OPTIONS.iter().map(ToString::to_string));
    argv.push(path.to_string());
    argv
}

/// The only name an `O_TMPFILE` descriptor has. libkrun re-opens it, which
/// works because the VMM still holds the descriptor.
pub fn proc_path(fd: RawFd) -> String {
    format!("/proc/self/fd/{fd}")
}

#[cfg(test)]
mod tests {
    #[test]
    fn mkfs_invocation() {
        insta::assert_snapshot!(super::mkfs_argv(&super::proc_path(3)).join(" "));
    }
}
