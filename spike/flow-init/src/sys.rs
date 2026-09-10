//! The syscall wrappers the other modules are written in terms of.
//!
//! Every failure becomes one line of text; flow-init has no logger and the
//! reactor sees only that line and the exit code.

use std::ffi::CString;
use std::os::raw::c_ulong;

pub type Result<T> = std::result::Result<T, String>;

/// `<what>: <errno text>`, e.g. `mount overlay at /dev/.flow/root: Invalid
/// argument (os error 22)`.
pub fn last_error(what: impl std::fmt::Display) -> String {
    format!("{what}: {}", std::io::Error::last_os_error())
}

/// Paths and mount options are all built from our own constants and from
/// values the CLI has already parsed as addresses or integers, so an interior
/// NUL is an impossible state rather than an error to handle.
fn cstring(value: &str) -> CString {
    CString::new(value).expect("flow-init builds its own strings; none contain NUL")
}

pub fn mount(
    source: &str,
    target: &str,
    fstype: &str,
    flags: c_ulong,
    options: &str,
) -> Result<()> {
    let (source_c, target_c, fstype_c, options_c) = (
        cstring(source),
        cstring(target),
        cstring(fstype),
        cstring(options),
    );
    // Safety: four NUL-terminated strings that outlive the call.
    let rc = unsafe {
        libc::mount(
            source_c.as_ptr(),
            target_c.as_ptr(),
            if fstype.is_empty() {
                std::ptr::null()
            } else {
                fstype_c.as_ptr()
            },
            flags,
            if options.is_empty() {
                std::ptr::null()
            } else {
                options_c.as_ptr().cast()
            },
        )
    };
    if rc < 0 {
        let what = match fstype {
            "" => format!("mount {source} at {target} (flags {flags:#x})"),
            _ => format!("mount {fstype} {source} at {target}"),
        };
        return Err(last_error(what));
    }
    Ok(())
}

/// An already-present directory is the normal case: every mount point but
/// `/dev/.flow` exists in the connector image, or was injected into its root
/// share by the helper.
pub fn mkdir(path: &str, mode: libc::mode_t) -> Result<()> {
    let path_c = cstring(path);
    // Safety: a NUL-terminated path that outlives the call.
    if unsafe { libc::mkdir(path_c.as_ptr(), mode) } < 0 {
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::AlreadyExists {
            return Err(format!("mkdir {path}: {error}"));
        }
    }
    Ok(())
}

pub fn write_file(path: &str, content: &str) -> Result<()> {
    std::fs::write(path, content).map_err(|e| format!("writing {path}: {e}"))
}
