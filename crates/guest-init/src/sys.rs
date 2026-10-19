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

pub fn write_file(path: &str, content: &str) -> Result<()> {
    std::fs::write(path, content).map_err(|e| format!("writing {path}: {e}"))
}
