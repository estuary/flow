//! The mount work: the three extra filesystems the connector needs, and the
//! two files podman would have written into its root.
//!
//! The root itself needs nothing done to it. It is podman's per-container
//! writable layer over the connector image, served read-write over virtiofs,
//! so it behaves as a container's root does today.

use std::ffi::CString;
use std::net::Ipv4Addr;

use crate::sys::{self, Result};

pub const VENV: &str = "/venv";
pub const SCRATCH: &str = "/scratch";
pub const DEPS: &str = "/opt/venv";

/// Resolution and naming, straight into the root, which podman writes into a
/// container today.
pub fn write_etc(nameserver: Ipv4Addr, guest_ip: Ipv4Addr) -> Result<()> {
    sys::mkdir("/etc", 0o755)?;
    sys::write_file("/etc/resolv.conf", &format!("nameserver {nameserver}\n"))?;

    // No `::1` line: IPv6 is off, and a client that tried it first would wait
    // out a connect timeout on every localhost lookup.
    let mut hosts = String::from("127.0.0.1 localhost\n");
    let hostname = std::fs::read_to_string("/proc/sys/kernel/hostname")
        .map_err(|e| format!("reading /proc/sys/kernel/hostname: {e}"))?;
    let hostname = hostname.trim();
    if !hostname.is_empty() && hostname != "localhost" {
        hosts.push_str(&format!("{guest_ip} {hostname}\n"));
    }
    sys::write_file("/etc/hosts", &hosts)
}

/// The virtiofs share holding the connector's Python environment. `dax` maps
/// the host's page cache into the guest instead of copying every read through
/// the FUSE queue (PLAN experiment 5).
pub fn mount_venv(dax: bool) -> Result<()> {
    sys::mkdir(VENV, 0o755)?;
    sys::mount(
        "venv",
        VENV,
        "virtiofs",
        libc::MS_RDONLY,
        if dax { "dax" } else { "" },
    )
}

/// The scratch disk: an ext4 image the helper opened with O_TMPFILE, so it is
/// unlinked already and returns to the host filesystem when the VM dies.
///
/// mkfs leaves the filesystem root owned by root, but the workload runs as the
/// image's user and `TMPDIR` points here, so hand it over.
pub fn mount_scratch(uid: u32, gid: u32) -> Result<()> {
    sys::mkdir(SCRATCH, 0o755)?;
    sys::mount("/dev/vda", SCRATCH, "ext4", 0, "")?;

    let path = CString::new(SCRATCH).expect("a constant path contains no NUL");
    // Safety: a NUL-terminated path that outlives the call.
    if unsafe { libc::chown(path.as_ptr(), uid, gid) } < 0 {
        return Err(sys::last_error(format!("chown {SCRATCH} to {uid}:{gid}")));
    }
    Ok(())
}

/// The per-tag dependency image on `/dev/vdb`, which is what the connector's
/// Python actually imports from. A block device rather than a second virtiofs
/// share because WP08 measured virtiofs at 2.4x-2.9x of podman on a cold
/// `import pandas` and ext4 over virtio-blk at 0.9x: the cost was per-file
/// metadata round trips, which a block device answers from the guest's own
/// caches.
///
/// No `chown`, unlike scratch: this is read-only and the image is built with
/// world-readable modes.
pub fn mount_deps(dev: &str, fstype: &str) -> Result<()> {
    // Two levels, and `/opt` usually does not exist in a connector image.
    sys::mkdir("/opt", 0o755)?;
    sys::mkdir(DEPS, 0o755)?;
    sys::mount(dev, DEPS, fstype, libc::MS_RDONLY, "")
}
