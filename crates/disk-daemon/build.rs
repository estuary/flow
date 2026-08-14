//! Generates Rust definitions for the ublk ABI from the vendored
//! `ublk_cmd.h`, so that nothing in `src/ublk/sys.rs` is transcribed by hand.
//!
//! The header is a copy of `include/uapi/linux/ublk_cmd.h`. Updating it to pick
//! up a newer kernel's features means replacing the file, not editing Rust.

fn main() {
    println!("cargo:rerun-if-changed=ublk_cmd.h");
    println!("cargo:rerun-if-changed=build.rs");

    // The `UBLK_U_*` opcodes are `_IOWR` macros, which bindgen cannot evaluate.
    // Assigning each to a constant lets the C compiler fold it into a value
    // bindgen does emit. The names differ from the macros they wrap because a
    // macro expands on both sides of its own definition.
    const SHIM: &str = r#"
#include <asm/ioctl.h>
#include "ublk_cmd.h"

#define ENCODED(macro, name) const unsigned int name = macro;

ENCODED(UBLK_U_CMD_ADD_DEV, UBLK_IOCTL_ADD_DEV)
ENCODED(UBLK_U_CMD_DEL_DEV, UBLK_IOCTL_DEL_DEV)
ENCODED(UBLK_U_CMD_GET_DEV_INFO, UBLK_IOCTL_GET_DEV_INFO)
ENCODED(UBLK_U_CMD_START_DEV, UBLK_IOCTL_START_DEV)
ENCODED(UBLK_U_CMD_STOP_DEV, UBLK_IOCTL_STOP_DEV)
ENCODED(UBLK_U_CMD_SET_PARAMS, UBLK_IOCTL_SET_PARAMS)
ENCODED(UBLK_U_IO_FETCH_REQ, UBLK_IOCTL_FETCH_REQ)
ENCODED(UBLK_U_IO_COMMIT_AND_FETCH_REQ, UBLK_IOCTL_COMMIT_AND_FETCH_REQ)
"#;

    let out = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap()).join("ublk_cmd.rs");
    let dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();

    bindgen::Builder::default()
        .header_contents("shim.h", SHIM)
        .clang_arg(format!("-I{dir}"))
        .allowlist_type("ublk.*")
        .allowlist_var("UBLK.*")
        .derive_default(true)
        .prepend_enum_name(false)
        .use_core()
        .ctypes_prefix("libc")
        .generate()
        .expect("generating ublk bindings")
        .write_to_file(&out)
        .expect("writing ublk bindings");
}
