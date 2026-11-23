//! Landlock rules for libkrun's virtiofs server, which runs in this process.
//!
//! libkrun's `lookup` passes a guest's name to `openat` unchecked, so a guest
//! whose kernel is compromised can name any file in this container. Each
//! lookup takes only an `O_PATH` descriptor, which Landlock does not govern.
//! The server then opens file contents through `/proc/self/fd/N`, and the
//! kernel checks that open against the file's own path. The rules therefore
//! bound which contents a guest reads and writes, and not which names it sees.

use anyhow::Context;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

const EXECUTE: u64 = 1 << 0;
const WRITE_FILE: u64 = 1 << 1;
const READ_FILE: u64 = 1 << 2;
const READ_DIR: u64 = 1 << 3;
const MAKE_SOCK: u64 = 1 << 9;

/// ABI 3 denies these rights unless a rule grants them.
const HANDLED: u64 = (1 << 15) - 1;
const ABI: libc::c_long = 3;

const CREATE_RULESET_VERSION: u32 = 1;
const RULE_PATH_BENEATH: u32 = 1;

/// `struct landlock_ruleset_attr`, as of ABI 1. The kernel accepts the
/// shorter structure of an earlier ABI.
#[repr(C)]
struct RulesetAttr {
    handled_access_fs: u64,
}

/// `struct landlock_path_beneath_attr`.
#[repr(C, packed)]
struct PathBeneathAttr {
    allowed_access: u64,
    parent_fd: libc::c_int,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// Read files and list directories beneath a directory.
    ReadTree,
    /// Everything but execute beneath a directory.
    WriteTree,
    /// Read and write one file, or the files beneath a directory.
    ReadWriteFile,
    /// Bind Unix sockets in a directory.
    BindSocket,
}

impl Access {
    fn rights(self) -> u64 {
        match self {
            Access::ReadTree => READ_FILE | READ_DIR,
            Access::WriteTree => HANDLED & !EXECUTE,
            Access::ReadWriteFile => READ_FILE | WRITE_FILE,
            Access::BindSocket => MAKE_SOCK,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rule {
    pub path: String,
    pub access: Access,
}

/// Why this kernel cannot apply a ruleset, from its answer to the ABI query.
/// ENOSYS is a kernel built without Landlock, or a seccomp profile which
/// refuses the call. EOPNOTSUPP is Landlock left out of the boot's LSMs.
fn unavailable(abi: std::io::Result<libc::c_long>) -> Option<String> {
    match abi {
        Err(error) => Some(format!("querying its ABI: {error}")),
        Ok(abi) if abi < ABI => Some(format!("the kernel has ABI {abi}, not {ABI}")),
        Ok(_) => None,
    }
}

/// Permanently restrict the calling thread and its future children.
/// `Ok(Err(reason))` means Landlock is unavailable. Setup failures propagate.
pub fn restrict_self(rules: &[Rule]) -> anyhow::Result<Result<(), String>> {
    // SAFETY: a version query, which takes no attribute.
    let abi = unsafe {
        libc::syscall(
            libc::SYS_landlock_create_ruleset,
            std::ptr::null::<RulesetAttr>(),
            0usize,
            CREATE_RULESET_VERSION,
        )
    };
    let abi = if abi < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(abi)
    };
    if let Some(why) = unavailable(abi) {
        return Ok(Err(why));
    }

    let attr = RulesetAttr {
        handled_access_fs: HANDLED,
    };
    // SAFETY: `attr` outlives the call, and the size is its own.
    let ruleset = unsafe {
        libc::syscall(
            libc::SYS_landlock_create_ruleset,
            &attr as *const RulesetAttr,
            std::mem::size_of::<RulesetAttr>(),
            0u32,
        )
    };
    if ruleset < 0 {
        return Err(std::io::Error::last_os_error()).context("creating the Landlock ruleset");
    }
    // SAFETY: the kernel returned a new descriptor that nothing else owns.
    let ruleset = unsafe { OwnedFd::from_raw_fd(ruleset as libc::c_int) };

    for rule in rules {
        let parent = rustix::fs::open(
            rule.path.as_str(),
            rustix::fs::OFlags::PATH | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .with_context(|| format!("opening {} for its Landlock rule", rule.path))?;

        let attr = PathBeneathAttr {
            allowed_access: rule.access.rights(),
            parent_fd: parent.as_raw_fd(),
        };
        // SAFETY: `attr` and both descriptors outlive the call.
        let added = unsafe {
            libc::syscall(
                libc::SYS_landlock_add_rule,
                ruleset.as_raw_fd(),
                RULE_PATH_BENEATH,
                &attr as *const PathBeneathAttr,
                0u32,
            )
        };
        if added < 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("adding the Landlock rule for {}", rule.path));
        }
    }

    // A thread without CAP_SYS_ADMIN must set this before it restricts itself.
    // SAFETY: a prctl of scalar arguments.
    if unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) } < 0 {
        return Err(std::io::Error::last_os_error()).context("prctl(PR_SET_NO_NEW_PRIVS)");
    }
    // SAFETY: two scalar arguments, and the descriptor is open across the call.
    if unsafe { libc::syscall(libc::SYS_landlock_restrict_self, ruleset.as_raw_fd(), 0u32) } < 0 {
        return Err(std::io::Error::last_os_error()).context("applying the Landlock ruleset");
    }
    Ok(Ok(()))
}

#[cfg(test)]
mod tests {
    use super::{Access, Rule};
    use std::os::fd::{AsRawFd, OwnedFd};

    /// libkrun's `lookup`: an `O_PATH` descriptor from a name it does not check.
    fn lookup(parent: &OwnedFd, name: &str) -> std::io::Result<OwnedFd> {
        Ok(rustix::fs::openat(
            parent,
            name,
            rustix::fs::OFlags::PATH | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )?)
    }

    /// libkrun's `open_inode`: the contents, through `/proc/self/fd/N`.
    fn open_inode(inode: &OwnedFd, flags: rustix::fs::OFlags) -> String {
        let opened = rustix::fs::open(
            format!("/proc/self/fd/{}", inode.as_raw_fd()),
            flags | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        );
        match opened {
            Ok(_) => "opens".to_string(),
            Err(error) => format!("{error:?}"),
        }
    }

    /// Runs on its own thread, because a ruleset restricts the thread which
    /// applies it for good, and the harness reuses its threads.
    #[test]
    fn a_share_opens_only_what_its_rules_allow() {
        let table = std::thread::spawn(|| {
            let directory = tempfile::tempdir().expect("a temporary directory");
            let (read_only, writable, backing, outside) = (
                directory.path().join("read-only"),
                directory.path().join("writable"),
                directory.path().join("backing"),
                directory.path().join("outside"),
            );
            for share in [&read_only, &writable, &backing, &outside] {
                std::fs::create_dir(share).expect("creating a share");
                std::fs::write(share.join("file"), b"content").expect("writing a file");
            }
            // The scratch disk: a file with no name, which libkrun opens
            // through `/proc/self/fd/N`.
            let unnamed: OwnedFd = rustix::fs::open(
                &backing,
                rustix::fs::OFlags::TMPFILE | rustix::fs::OFlags::RDWR,
                rustix::fs::Mode::from_raw_mode(0o600),
            )
            .expect("an unnamed file");

            let rule = |path: &std::path::Path, access| Rule {
                path: path.to_str().expect("a UTF-8 path").to_string(),
                access,
            };
            super::restrict_self(&[
                rule(&read_only, Access::ReadTree),
                rule(&writable, Access::WriteTree),
                rule(&backing, Access::ReadWriteFile),
            ])
            .expect("a ruleset this kernel accepts")
            .expect("a kernel with Landlock");

            let root = |path: &std::path::Path| -> OwnedFd {
                rustix::fs::open(
                    path,
                    rustix::fs::OFlags::PATH | rustix::fs::OFlags::CLOEXEC,
                    rustix::fs::Mode::empty(),
                )
                .expect("a share's root")
            };
            let (read_only, writable) = (root(&read_only), root(&writable));
            let (read, write) = (rustix::fs::OFlags::RDONLY, rustix::fs::OFlags::RDWR);

            let mut table = String::new();
            for (what, share, name, flags) in [
                ("read-only share, its file, read", &read_only, "file", read),
                (
                    "read-only share, its file, write",
                    &read_only,
                    "file",
                    write,
                ),
                ("writable share, its file, write", &writable, "file", write),
                (
                    "read-only share, ../outside/file, read",
                    &read_only,
                    "../outside/file",
                    read,
                ),
                (
                    "writable share, ../outside/file, write",
                    &writable,
                    "../outside/file",
                    write,
                ),
                (
                    "writable share, ../read-only/file, write",
                    &writable,
                    "../read-only/file",
                    write,
                ),
                (
                    "writable share, /proc/self/mem, read",
                    &writable,
                    "/proc/self/mem",
                    read,
                ),
                (
                    "writable share, /proc/self/mem, write",
                    &writable,
                    "/proc/self/mem",
                    write,
                ),
                (
                    "writable share, /etc/passwd, read",
                    &writable,
                    "/etc/passwd",
                    read,
                ),
            ] {
                let outcome = match lookup(share, name) {
                    Ok(inode) => format!("lookup succeeds, open: {}", open_inode(&inode, flags)),
                    Err(error) => format!("lookup: {error:?}"),
                };
                table.push_str(&format!("{what}\n  {outcome}\n"));
            }
            table.push_str(&format!(
                "the unnamed scratch file, write\n  open: {}\n",
                open_inode(&unnamed, write)
            ));
            table
        })
        .join()
        .expect("the restricted thread");

        insta::assert_snapshot!(table);
    }

    #[test]
    fn a_kernel_without_landlock_is_reported() {
        let os = |code| Err(std::io::Error::from_raw_os_error(code));
        let table: Vec<String> = [os(libc::ENOSYS), os(libc::EOPNOTSUPP), Ok(2), Ok(3), Ok(7)]
            .into_iter()
            .map(|abi| format!("{:?}", super::unavailable(abi)))
            .collect();
        insta::assert_snapshot!(table.join("\n"));
    }
}
