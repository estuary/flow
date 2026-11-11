//! The guest's scratch disk: an unnamed file that dies with the VMM.
//!
//! `O_TMPFILE` means a SIGKILL of the VMM returns the blocks immediately, with
//! no cleanup path for the runtime to get wrong. The cost is that the only way
//! to name it - for `mkfs.ext4`, and for libkrun - is `/proc/self/fd/N`, so
//! the descriptor must not be `O_CLOEXEC`: `mkfs.ext4` resolves that path in
//! its own process. It belongs to this VM and is discarded with it; nothing
//! carries across a replacement VMM.

use std::os::fd::{AsRawFd, IntoRawFd, RawFd};
use std::path::Path;

const MIB: u64 = 1024 * 1024;

/// The new sparse file reads as zero throughout. Mark inode tables zeroed
/// without writing them or leaving work for the guest's ext4lazyinit thread.
/// Scratch needs neither a journal nor blocks reserved for root.
const MKFS: &str = "mkfs.ext4";
const MKFS_OPTIONS: &[&str] = &[
    "-E",
    "assume_storage_prezeroed=1",
    "-O",
    "^has_journal",
    "-m",
    "0",
    "-q",
    "-F",
];

/// A raw descriptor, because `launch::sweep` moves and closes it by number.
pub fn create(backing_dir: &Path, disk_mib: u64) -> anyhow::Result<RawFd> {
    let fd = rustix::fs::open(
        backing_dir,
        rustix::fs::OFlags::TMPFILE | rustix::fs::OFlags::RDWR | rustix::fs::OFlags::EXCL,
        rustix::fs::Mode::from_raw_mode(0o600),
    )
    .map_err(|e| anyhow::anyhow!(e).context(format!("O_TMPFILE in {}", backing_dir.display())))?;
    rustix::fs::ftruncate(&fd, disk_mib * MIB).map_err(|e| {
        anyhow::anyhow!(e).context(format!("sizing the scratch disk to {disk_mib} MiB"))
    })?;

    let argv = mkfs_argv(&proc_path(fd.as_raw_fd()));
    let (program, args) = argv.split_first().expect("mkfs_argv is never empty");
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    crate::sys::run(program, &args, None)?;

    Ok(fd.into_raw_fd())
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
