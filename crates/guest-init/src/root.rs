//! The mounts the connector needs, and the two files a container runtime would
//! have written into its root.

use std::ffi::CString;
use std::net::Ipv4Addr;

use crate::sys::{self, Result};

pub const SCRATCH: &str = "/scratch";
/// Separate from `TMPDIR`: uv refuses projects inside its cache, and
/// derive-python creates projects in `TMPDIR`.
pub const UV_CACHE: &str = "/scratch/uv-cache";

/// The virtiofs tag the VMM gives the task's persistent disk share.
const PERSISTENT_DISK: &str = "persistent-disk";

/// The virtiofs tag the VMM gives the connector mount share.
const CONNECTOR_MOUNT: &str = "connector-mount";

pub fn write_etc(nameserver: Ipv4Addr, guest_ip: Ipv4Addr) -> Result<()> {
    let hostname = std::fs::read_to_string("/proc/sys/kernel/hostname")
        .map_err(|e| format!("reading /proc/sys/kernel/hostname: {e}"))?;
    let (resolv_conf, hosts) = etc_files(nameserver, guest_ip, hostname.trim());

    sys::mkdir("/etc", 0o755)?;
    sys::write_file("/etc/resolv.conf", &resolv_conf)?;
    sys::write_file("/etc/hosts", &hosts)
}

/// `/etc/resolv.conf` and `/etc/hosts`, in that order.
///
/// No `::1` line: IPv6 is off, and a client that tried it first would wait out
/// a connect timeout on every localhost lookup.
fn etc_files(nameserver: Ipv4Addr, guest_ip: Ipv4Addr, hostname: &str) -> (String, String) {
    let mut hosts = String::from("127.0.0.1 localhost\n");
    if !hostname.is_empty() && hostname != "localhost" {
        hosts.push_str(&format!("{guest_ip} {hostname}\n"));
    }
    (format!("nameserver {nameserver}\n"), hosts)
}

/// The scratch disk: an ext4 image the VMM opened with `O_TMPFILE`, so it is
/// unlinked already and returns to the host filesystem when the VM dies.
///
/// mkfs leaves the filesystem root owned by root, but the workload runs as the
/// image's user and `TMPDIR` points here, so hand it over.
pub fn mount_scratch(uid: u32, gid: u32) -> Result<()> {
    sys::mkdir(SCRATCH, 0o755)?;
    check_mount_target(SCRATCH)?;
    sys::mount("/dev/vda", SCRATCH, "ext4", 0, "")?;

    let path = CString::new(SCRATCH).expect("a constant path contains no NUL");
    // Safety: a NUL-terminated path that outlives the call.
    if unsafe { libc::chown(path.as_ptr(), uid, gid) } < 0 {
        return Err(sys::last_error(format!("chown {SCRATCH} to {uid}:{gid}")));
    }
    Ok(())
}

/// The runtime's read-only channel to the connector: its entrypoint, its image
/// inspection and, where the task has one, the credential it re-reads.
///
/// Read-only because the host owns every byte of it, and `nodev,nosuid`
/// because none of it arrived from inside the guest. Deliberately not
/// `noexec`: `flow-connector-init` is executed from here.
pub fn mount_connector_mount(guest_path: &str) -> Result<()> {
    sys::mkdir_all(guest_path, 0o755)?;
    check_mount_target(guest_path)?;
    sys::mount(
        CONNECTOR_MOUNT,
        guest_path,
        "virtiofs",
        libc::MS_RDONLY | libc::MS_NODEV | libc::MS_NOSUID,
        "",
    )
}

/// `nodev,nosuid,noexec` because the contents are task data and stay that way
/// across reattach, and no `chown`: the share's owner formats its root for the
/// client and reads a later `chown` as a delta.
pub fn mount_persistent_disk(guest_path: &str) -> Result<()> {
    sys::mkdir_all(guest_path, 0o755)?;
    check_mount_target(guest_path)?;
    sys::mount(
        PERSISTENT_DISK,
        guest_path,
        "virtiofs",
        libc::MS_NODEV | libc::MS_NOSUID | libc::MS_NOEXEC,
        "",
    )
}

fn check_mount_target(path: &str) -> Result<()> {
    // Image-provided symlinks and dot components can name `/` even when the
    // CLI rejects its literal spelling. No workload runs until mounting ends.
    let resolved =
        std::fs::canonicalize(path).map_err(|e| format!("resolving mount target {path}: {e}"))?;
    if resolved == std::path::Path::new("/") {
        return Err("the guest root is not a mount point for this disk".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    #[test]
    fn mount_targets() {
        let temp = tempfile::tempdir().unwrap();
        let scratch = temp.path().join("scratch");
        let persist = temp.path().join("persist");
        std::os::unix::fs::symlink("/", &scratch).unwrap();
        std::os::unix::fs::symlink(temp.path(), &persist).unwrap();

        let mut table = String::new();
        for (name, path) in [
            ("root", std::path::Path::new("/")),
            ("dot", std::path::Path::new("/./")),
            ("parent", std::path::Path::new("/tmp/..")),
            ("symlink to root", scratch.as_path()),
            ("directory", temp.path()),
            ("symlink to directory", persist.as_path()),
        ] {
            table.push_str(&format!(
                "{name}: {:?}\n",
                super::check_mount_target(path.to_str().unwrap())
            ));
        }
        insta::assert_snapshot!(table, @r#"
        root: Err("the guest root is not a mount point for this disk")
        dot: Err("the guest root is not a mount point for this disk")
        parent: Err("the guest root is not a mount point for this disk")
        symlink to root: Err("the guest root is not a mount point for this disk")
        directory: Ok(())
        symlink to directory: Ok(())
        "#);
    }

    #[test]
    fn etc_files() {
        let (nameserver, guest_ip) = (Ipv4Addr::new(192, 0, 2, 1), Ipv4Addr::new(192, 0, 2, 2));
        let mut rendered = String::new();

        for hostname in ["acmeCo-capture-1", "", "localhost"] {
            let (resolv_conf, hosts) = super::etc_files(nameserver, guest_ip, hostname);
            rendered.push_str(&format!(
                "# hostname {hostname:?}\n\
                 ## /etc/resolv.conf\n{resolv_conf}\
                 ## /etc/hosts\n{hosts}\n"
            ));
        }
        insta::assert_snapshot!(rendered);
    }
}
