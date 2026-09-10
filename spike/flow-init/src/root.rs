//! The mount work: a writable root over the read-only image, and the two
//! extra filesystems the connector needs.

use std::ffi::CString;
use std::net::Ipv4Addr;

use crate::sys::{self, Result};

/// The staging directory for the new root. devtmpfs, which libkrun's init
/// mounted at `/dev`, is the only writable place in the guest before the
/// overlay exists: the image's own root share is read-only.
const FLOW_DIR: &str = "/dev/.flow";
const NEW_ROOT: &str = "/dev/.flow/root";

pub const VENV: &str = "/venv";
pub const SCRATCH: &str = "/scratch";

/// A capped tmpfs over the image, and the guest root becomes it. Everything
/// the connector writes outside `/scratch` lands in that tmpfs and is charged
/// to the VM's memory, which is why the size is a cap and not a default.
pub fn pivot(upper_mib: u32) -> Result<()> {
    // A mount namespace of our own, because libkrun's init is in this one and
    // still needs its original root: when the workload exits, its
    // `set_exit_code` reports the code with an ioctl on `/`, and only if
    // `statfs("/")` returns virtiofs magic. Pivot the shared namespace and
    // init's root becomes the overlay, so it silently skips the report and the
    // VM exits 0 whatever the workload returned.
    // Safety: unshare takes no pointer arguments.
    if unsafe { libc::unshare(libc::CLONE_NEWNS) } < 0 {
        return Err(sys::last_error("unshare(CLONE_NEWNS)"));
    }

    // Then private, because libkrun's init ends with `mount(NULL, "/", NULL,
    // MS_REC | MS_SHARED, NULL)`: the new namespace inherits that propagation,
    // which would push these mounts back into init's namespace, and both
    // MS_MOVE and pivot_root refuse a mount whose parent propagates (EINVAL).
    sys::mount("", "/", "", libc::MS_REC | libc::MS_PRIVATE, "")?;

    sys::mkdir(FLOW_DIR, 0o700)?;
    sys::mount("tmpfs", FLOW_DIR, "tmpfs", 0, &format!("size={upper_mib}m"))?;
    for directory in ["upper", "work", "root"] {
        sys::mkdir(&format!("{FLOW_DIR}/{directory}"), 0o755)?;
    }
    sys::mount(
        "overlay",
        NEW_ROOT,
        "overlay",
        0,
        &format!("lowerdir=/,upperdir={FLOW_DIR}/upper,workdir={FLOW_DIR}/work"),
    )?;

    // /proc and /sys move wholesale, submounts (cgroup2) included.
    for directory in ["/proc", "/sys"] {
        let target = format!("{NEW_ROOT}{directory}");
        sys::mkdir(&target, 0o555)?;
        sys::mount(directory, &target, "", libc::MS_MOVE, "")?;
    }

    // /dev cannot: the tmpfs holding the new root is inside it, and MS_MOVE
    // refuses a target within the mount being moved. devtmpfs is one
    // kernel-wide instance, so a second mount of it shows the same nodes, and
    // its two submounts cost nothing to remake.
    let dev = format!("{NEW_ROOT}/dev");
    sys::mkdir(&dev, 0o755)?;
    sys::mount("devtmpfs", &dev, "devtmpfs", 0, "")?;
    sys::mkdir(&format!("{dev}/pts"), 0o755)?;
    sys::mount(
        "devpts",
        &format!("{dev}/pts"),
        "devpts",
        0,
        "mode=0620,ptmxmode=0666",
    )?;
    sys::mkdir(&format!("{dev}/shm"), 0o1777)?;
    sys::mount("shm", &format!("{dev}/shm"), "tmpfs", 0, "mode=1777")?;

    // The `pivot_root(".", ".")` form from pivot_root(2): the old root ends up
    // over the new one and is detached immediately, so the guest root carries
    // no directory whose only purpose was to hold it. The lazy detach is what
    // lets the overlay keep the lower and upper mounts it still references.
    sys::chdir(NEW_ROOT)?;
    pivot_root(".", ".")?;
    umount_lazily(".")?;
    sys::chdir("/")
}

/// Resolution and naming, which podman writes into a container today.
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

fn pivot_root(new_root: &str, put_old: &str) -> Result<()> {
    let (new_root_c, put_old_c) = (
        CString::new(new_root).expect("a constant path contains no NUL"),
        CString::new(put_old).expect("a constant path contains no NUL"),
    );
    // Safety: two NUL-terminated paths that outlive the call. musl has no
    // pivot_root wrapper, so the syscall is made directly.
    let rc = unsafe {
        libc::syscall(
            libc::SYS_pivot_root,
            new_root_c.as_ptr(),
            put_old_c.as_ptr(),
        )
    };
    if rc < 0 {
        return Err(sys::last_error(format!(
            "pivot_root({new_root}, {put_old})"
        )));
    }
    Ok(())
}

fn umount_lazily(target: &str) -> Result<()> {
    let target_c = CString::new(target).expect("a constant path contains no NUL");
    // Safety: a NUL-terminated path that outlives the call.
    if unsafe { libc::umount2(target_c.as_ptr(), libc::MNT_DETACH) } < 0 {
        return Err(sys::last_error(format!("umount2({target}, MNT_DETACH)")));
    }
    Ok(())
}
