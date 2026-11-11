//! The syscall wrappers the other modules are written in terms of.
//!
//! Every failure becomes one line of text: there is no logger here, and the
//! launcher sees only those lines and the exit code.

pub type Result<T> = std::result::Result<T, String>;

/// `<what>: <errno text>`, e.g. `setuid(1000): Operation not permitted (os
/// error 1)`.
pub fn last_error(what: impl std::fmt::Display) -> String {
    format!("{what}: {}", std::io::Error::last_os_error())
}

pub fn mount(
    source: &str,
    target: &str,
    fstype: &str,
    flags: rustix::mount::MountFlags,
    options: Option<&std::ffi::CStr>,
) -> Result<()> {
    rustix::mount::mount(source, target, fstype, flags, options)
        .map_err(|e| format!("mount {fstype} {source} at {target}: {e}"))
}

pub fn mount_bind(source: &str, target: &str) -> Result<()> {
    rustix::mount::mount_bind(source, target)
        .map_err(|e| format!("bind mount {source} at {target}: {e}"))
}

/// Bind remounts ignore filesystem options.
pub fn mount_remount(target: &str, flags: rustix::mount::MountFlags) -> Result<()> {
    rustix::mount::mount_remount(target, flags, c"")
        .map_err(|e| format!("remount {target} ({flags:?}): {e}"))
}

pub fn write_file(path: &str, content: &str) -> Result<()> {
    std::fs::write(path, content).map_err(|e| format!("writing {path}: {e}"))
}
