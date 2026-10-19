//! Writable mounts and generated `/etc` files over a read-only image root.

use std::net::Ipv4Addr;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;

use crate::sys::{self, Result};

/// Must match the VMM's `SCRATCH_MOUNT_POINT`, a virtual directory in the
/// read-only root.
const SCRATCH: &str = "/scratch";
/// Separate from `TMPDIR`: uv refuses projects inside its cache, and
/// derive-python creates projects in `TMPDIR`.
pub const UV_CACHE: &str = "/scratch/uv-cache";
pub const TMP: &str = "/tmp";

/// Scratch directories bound over the image's own temporary directories, so
/// a workload that names them rather than `TMPDIR` still writes to scratch.
const TEMP_DIRS: [(&str, &str); 2] = [("/scratch/tmp", TMP), ("/scratch/var-tmp", "/var/tmp")];

const ETC: &str = "/scratch/etc";

/// Not `noexec`: uv runs a project's environment from `TMPDIR`.
const SCRATCH_FLAGS: libc::c_ulong = libc::MS_NODEV | libc::MS_NOSUID;

/// The virtiofs tag the VMM gives the task's persistent disk share.
const PERSISTENT_DISK: &str = "persistent-disk";

/// The virtiofs tag the VMM gives the connector mount share.
const CONNECTOR_MOUNT: &str = "connector-mount";

/// The scratch disk: an ext4 image the VMM opened with `O_TMPFILE`, so it is
/// unlinked already and returns to the host filesystem when the VM dies.
///
/// mkfs leaves the filesystem root owned by root, and it stays so. The
/// workload owns uv's cache, and the temporary directories are shared and
/// sticky, as a container's are.
pub fn mount_scratch(uid: u32, gid: u32) -> Result<()> {
    check_mount_target(SCRATCH)?;
    sys::mount("/dev/vda", SCRATCH, "ext4", SCRATCH_FLAGS, "")?;

    for (dir, target) in TEMP_DIRS {
        make_dir(dir, 0o1777)?;
        bind(dir, target, SCRATCH_FLAGS)?;
    }
    make_dir(UV_CACHE, 0o755)?;
    std::os::unix::fs::chown(UV_CACHE, Some(uid), Some(gid))
        .map_err(|e| format!("chown {UV_CACHE} to {uid}:{gid}: {e}"))
}

/// Copy `/etc` before generating files: the image root is read-only, and
/// container images commonly lack `/etc/hosts` because the runtime supplies it.
pub fn write_etc(nameserver: Ipv4Addr, guest_ip: Ipv4Addr) -> Result<String> {
    let hostname = std::fs::read_to_string("/proc/sys/kernel/hostname")
        .map_err(|e| format!("reading /proc/sys/kernel/hostname: {e}"))?;
    let hostname = hostname.trim();
    let (resolv_conf, hosts) = etc_files(nameserver, guest_ip, hostname);

    copy_tree(Path::new("/etc"), Path::new(ETC))?;
    write_etc_files(ETC, &resolv_conf, &hosts)?;
    bind(ETC, "/etc", libc::MS_RDONLY | SCRATCH_FLAGS)?;
    Ok(hostname.to_string())
}

/// Image symlinks can point into directories unavailable during init. Replace
/// the links without touching their targets; no workload runs yet.
fn write_etc_files(etc: &str, resolv_conf: &str, hosts: &str) -> Result<()> {
    for (name, content) in [("resolv.conf", resolv_conf), ("hosts", hosts)] {
        let path = format!("{etc}/{name}");
        match std::fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.is_symlink() => {
                std::fs::remove_file(&path).map_err(|e| format!("removing symlink {path}: {e}"))?
            }
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(format!("inspecting {path}: {e}")),
        }
        sys::write_file(&path, content)?;
    }
    Ok(())
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

/// Directories, regular files and symlinks, each with its owner and mode.
/// Timestamps, extended attributes and hard links are not kept, and any other
/// kind of file is refused: none belongs in an image's `/etc`.
fn copy_tree(from: &Path, to: &Path) -> Result<()> {
    let display = from.display();
    let metadata =
        std::fs::symlink_metadata(from).map_err(|e| format!("inspecting {display}: {e}"))?;
    let file_type = metadata.file_type();

    let copying = |e: std::io::Error| format!("copying {display}: {e}");
    if file_type.is_symlink() {
        let target = std::fs::read_link(from).map_err(copying)?;
        std::os::unix::fs::symlink(&target, to).map_err(copying)?;
    } else if file_type.is_dir() {
        std::fs::create_dir(to).map_err(copying)?;
        for entry in std::fs::read_dir(from).map_err(copying)? {
            let entry = entry.map_err(copying)?;
            copy_tree(&entry.path(), &to.join(entry.file_name()))?;
        }
    } else if file_type.is_file() {
        std::fs::copy(from, to).map_err(copying)?;
    } else {
        return Err(format!(
            "copying {display}: only directories, regular files and symlinks are copied"
        ));
    }

    // After the contents, which a directory's own mode could refuse, and
    // before the mode, since a chown clears setuid and setgid bits.
    std::os::unix::fs::lchown(to, Some(metadata.uid()), Some(metadata.gid()))
        .map_err(|e| format!("chown {}: {e}", to.display()))?;
    if !file_type.is_symlink() {
        std::fs::set_permissions(to, std::fs::Permissions::from_mode(metadata.mode()))
            .map_err(|e| format!("chmod {}: {e}", to.display()))?;
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
    check_mount_target(guest_path)?;
    sys::mount(
        PERSISTENT_DISK,
        guest_path,
        "virtiofs",
        libc::MS_NODEV | libc::MS_NOSUID | libc::MS_NOEXEC,
        "",
    )
}

/// `MS_BIND` ignores every other flag, so the flags take a remount.
fn bind(source: &str, target: &str, flags: libc::c_ulong) -> Result<()> {
    check_mount_target(target)?;
    sys::mount(source, target, "", libc::MS_BIND, "")?;
    sys::mount(
        target,
        target,
        "",
        libc::MS_REMOUNT | libc::MS_BIND | flags,
        "",
    )
}

/// The mode is set apart from `mkdir`, whose mode the umask narrows.
fn make_dir(path: &str, mode: u32) -> Result<()> {
    std::fs::create_dir(path)
        .and_then(|()| std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)))
        .map_err(|e| format!("creating {path}: {e}"))
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

    #[test]
    fn write_etc_files() {
        let mut rendered = String::new();

        for case in [
            "regular files",
            "absent",
            "dangling relative symlinks",
            "dangling absolute symlinks",
            "relative symlinks to files",
            "absolute symlinks to files",
        ] {
            let temp = tempfile::tempdir().unwrap();
            let (etc, image) = (temp.path().join("etc"), temp.path().join("image"));
            std::fs::create_dir(&etc).unwrap();
            std::fs::create_dir(&image).unwrap();

            for name in ["resolv.conf", "hosts"] {
                let (path, target) = (etc.join(name), image.join(name));
                match case {
                    // The hard link shows each write landed in place.
                    "regular files" => {
                        std::fs::write(&path, "from the image\n").unwrap();
                        std::fs::hard_link(&path, &target).unwrap();
                    }
                    "absent" => {}
                    // Into a directory that does not exist, so following it fails.
                    "dangling relative symlinks" => {
                        std::os::unix::fs::symlink(format!("../run/{name}"), &path).unwrap();
                    }
                    // Into a directory that does, so following it creates the target.
                    "dangling absolute symlinks" => {
                        std::os::unix::fs::symlink(&target, &path).unwrap();
                    }
                    "relative symlinks to files" => {
                        std::fs::write(&target, "from the image\n").unwrap();
                        std::os::unix::fs::symlink(format!("../image/{name}"), &path).unwrap();
                    }
                    "absolute symlinks to files" => {
                        std::fs::write(&target, "from the image\n").unwrap();
                        std::os::unix::fs::symlink(&target, &path).unwrap();
                    }
                    _ => unreachable!(),
                }
            }

            let result = super::write_etc_files(
                etc.to_str().unwrap(),
                "nameserver 192.0.2.1\n",
                "127.0.0.1 localhost\n",
            );
            rendered.push_str(&format!("# {case}: {result:?}\n"));

            for relative in [
                "etc/resolv.conf",
                "etc/hosts",
                "image/resolv.conf",
                "image/hosts",
            ] {
                let path = temp.path().join(relative);
                let state = match std::fs::symlink_metadata(&path) {
                    Ok(metadata) if metadata.is_symlink() => "symlink".to_string(),
                    Ok(_) => format!("{:?}", std::fs::read_to_string(&path).unwrap()),
                    Err(error) => format!("{:?}", error.kind()),
                };
                rendered.push_str(&format!("{relative}: {state}\n"));
            }
        }
        insta::assert_snapshot!(rendered, @r#"
        # regular files: Ok(())
        etc/resolv.conf: "nameserver 192.0.2.1\n"
        etc/hosts: "127.0.0.1 localhost\n"
        image/resolv.conf: "nameserver 192.0.2.1\n"
        image/hosts: "127.0.0.1 localhost\n"
        # absent: Ok(())
        etc/resolv.conf: "nameserver 192.0.2.1\n"
        etc/hosts: "127.0.0.1 localhost\n"
        image/resolv.conf: NotFound
        image/hosts: NotFound
        # dangling relative symlinks: Ok(())
        etc/resolv.conf: "nameserver 192.0.2.1\n"
        etc/hosts: "127.0.0.1 localhost\n"
        image/resolv.conf: NotFound
        image/hosts: NotFound
        # dangling absolute symlinks: Ok(())
        etc/resolv.conf: "nameserver 192.0.2.1\n"
        etc/hosts: "127.0.0.1 localhost\n"
        image/resolv.conf: NotFound
        image/hosts: NotFound
        # relative symlinks to files: Ok(())
        etc/resolv.conf: "nameserver 192.0.2.1\n"
        etc/hosts: "127.0.0.1 localhost\n"
        image/resolv.conf: "from the image\n"
        image/hosts: "from the image\n"
        # absolute symlinks to files: Ok(())
        etc/resolv.conf: "nameserver 192.0.2.1\n"
        etc/hosts: "127.0.0.1 localhost\n"
        image/resolv.conf: "from the image\n"
        image/hosts: "from the image\n"
        "#);
    }

    #[test]
    fn copy_tree() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let temp = tempfile::tempdir().unwrap();
        let (etc, copy) = (temp.path().join("etc"), temp.path().join("copy"));
        let mode = |path: &std::path::Path, mode: u32| {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap()
        };

        // Every mode is set, so the umask of whoever runs this can't show.
        std::fs::create_dir_all(etc.join("ssl/private")).unwrap();
        mode(&etc, 0o755);
        mode(&etc.join("ssl"), 0o751);
        std::fs::write(etc.join("passwd"), "root:x:0:0::/root:/bin/sh\n").unwrap();
        mode(&etc.join("passwd"), 0o644);
        std::fs::write(etc.join("shadow"), "root:*:::::::\n").unwrap();
        mode(&etc.join("shadow"), 0o640);
        std::fs::write(etc.join("ssl/private/key.pem"), "acmeCo\n").unwrap();
        mode(&etc.join("ssl/private/key.pem"), 0o600);
        mode(&etc.join("ssl/private"), 0o500);
        std::os::unix::fs::symlink("../run/resolv.conf", etc.join("resolv.conf")).unwrap();
        std::os::unix::fs::symlink("/usr/share/zoneinfo/UTC", etc.join("localtime")).unwrap();
        std::os::unix::fs::symlink("passwd", etc.join("passwd-")).unwrap();

        let result = super::copy_tree(&etc, &copy);
        let mut rendered = format!("{result:?}\n");
        let mut paths: Vec<_> = walk(&copy);
        paths.sort();
        for path in paths {
            let (source, copied) = (etc.join(&path), copy.join(&path));
            let (want, got) = (
                std::fs::symlink_metadata(&source).unwrap(),
                std::fs::symlink_metadata(&copied).unwrap(),
            );
            assert_eq!(
                (want.uid(), want.gid()),
                (got.uid(), got.gid()),
                "the owner of {path:?}"
            );
            let what = if got.is_symlink() {
                format!("-> {}", std::fs::read_link(&copied).unwrap().display())
            } else if got.is_dir() {
                format!("{:o}", got.mode())
            } else {
                format!(
                    "{:o} {:?}",
                    got.mode(),
                    std::fs::read_to_string(&copied).unwrap()
                )
            };
            let name = match path.as_os_str().is_empty() {
                true => ".".to_string(),
                false => path.display().to_string(),
            };
            rendered.push_str(&format!("{name}: {what}\n"));
        }
        // Restore what the test made unwritable, so the temporary directory
        // can be removed.
        mode(&etc.join("ssl/private"), 0o700);
        mode(&copy.join("ssl/private"), 0o700);

        insta::assert_snapshot!(rendered, @r#"
        Ok(())
        .: 40755
        localtime: -> /usr/share/zoneinfo/UTC
        passwd: 100644 "root:x:0:0::/root:/bin/sh\n"
        passwd-: -> passwd
        resolv.conf: -> ../run/resolv.conf
        shadow: 100640 "root:*:::::::\n"
        ssl: 40751
        ssl/private: 40500
        ssl/private/key.pem: 100600 "acmeCo\n"
        "#);

        let fifo = temp.path().join("fifo");
        let fifo_c = std::ffi::CString::new(fifo.to_str().unwrap()).unwrap();
        // Safety: a NUL-terminated path that outlives the call.
        assert_eq!(unsafe { libc::mkfifo(fifo_c.as_ptr(), 0o644) }, 0);
        let refused = super::copy_tree(&fifo, &temp.path().join("fifo-copy"));
        insta::assert_snapshot!(
            format!("{refused:?}").replace(temp.path().to_str().unwrap(), "$TEMP"),
            @r#"Err("copying $TEMP/fifo: only directories, regular files and symlinks are copied")"#
        );
    }

    /// Paths beneath `root`, relative to it, without following links.
    fn walk(root: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut paths = vec![std::path::PathBuf::new()];
        let mut pending = vec![std::path::PathBuf::new()];
        while let Some(dir) = pending.pop() {
            for entry in std::fs::read_dir(root.join(&dir)).unwrap() {
                let entry = entry.unwrap();
                let path = dir.join(entry.file_name());
                if entry.file_type().unwrap().is_dir() {
                    pending.push(path.clone());
                }
                paths.push(path);
            }
        }
        paths
    }

    #[test]
    fn write_etc_files_inspection_error() {
        let temp = tempfile::tempdir().unwrap();
        let etc = temp.path().join("etc");
        std::fs::write(&etc, "").unwrap();

        let result = super::write_etc_files(etc.to_str().unwrap(), "", "");
        insta::assert_snapshot!(
            format!("{result:?}").replace(temp.path().to_str().unwrap(), "$TEMP"),
            @r#"Err("inspecting $TEMP/etc/resolv.conf: Not a directory (os error 20)")"#
        );
    }
}
