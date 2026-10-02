//! The syscall wrappers the other modules are written in terms of.
//!
//! Every failure becomes one line of text: there is no logger here, and the
//! launcher sees only those lines and the exit code.

use std::ffi::CString;
use std::os::raw::c_ulong;

pub type Result<T> = std::result::Result<T, String>;

/// `<what>: <errno text>`, e.g. `mount ext4 /dev/vda at /scratch: Invalid
/// argument (os error 22)`.
pub fn last_error(what: impl std::fmt::Display) -> String {
    format!("{what}: {}", std::io::Error::last_os_error())
}

/// Paths and mount options are built from our own constants and from values
/// the CLI has already parsed as addresses, integers or absolute paths, so an
/// interior NUL is an impossible state rather than an error to handle.
fn cstring(value: &str) -> CString {
    CString::new(value).expect("flow-guest-init builds its own strings; none contain NUL")
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

/// An already-present directory is the normal case: the mount points exist in
/// most connector images.
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

/// `mkdir -p` over an absolute path, in terms of `mkdir` above so that every
/// component gets the same mode and the same error text as the single-level
/// mount points.
pub fn mkdir_all(path: &str, mode: libc::mode_t) -> Result<()> {
    let mut prefix = String::with_capacity(path.len());
    for component in path.split('/').filter(|component| !component.is_empty()) {
        prefix.push('/');
        prefix.push_str(component);
        mkdir(&prefix, mode)?;
    }
    Ok(())
}

pub fn write_file(path: &str, content: &str) -> Result<()> {
    std::fs::write(path, content).map_err(|e| format!("writing {path}: {e}"))
}
