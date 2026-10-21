fn main() {
    println!("cargo:rerun-if-changed=ublk_cmd.h");

    // The `UBLK_U_*` opcodes are `_IOR` and `_IOWR` macros, which bindgen cannot
    // evaluate. Assigning each one to a constant lets the C compiler fold it into a
    // value bindgen does emit. Each name differs from the macro it wraps, because a
    // macro expands on both sides of its own definition. `errno.h` lets bindgen
    // evaluate `UBLK_IO_RES_ABORT`, which is `-ENODEV`.
    const SHIM: &str = r#"
#include <asm/ioctl.h>
#include <errno.h>
#include "ublk_cmd.h"

#define ENCODED(macro, name) const unsigned int name = macro;

ENCODED(UBLK_U_CMD_GET_FEATURES, UBLK_IOCTL_GET_FEATURES)
ENCODED(UBLK_U_CMD_ADD_DEV, UBLK_IOCTL_ADD_DEV)
ENCODED(UBLK_U_CMD_DEL_DEV, UBLK_IOCTL_DEL_DEV)
ENCODED(UBLK_U_CMD_START_DEV, UBLK_IOCTL_START_DEV)
ENCODED(UBLK_U_CMD_STOP_DEV, UBLK_IOCTL_STOP_DEV)
ENCODED(UBLK_U_CMD_SET_PARAMS, UBLK_IOCTL_SET_PARAMS)
ENCODED(UBLK_U_IO_FETCH_REQ, UBLK_IOCTL_FETCH_REQ)
ENCODED(UBLK_U_IO_COMMIT_AND_FETCH_REQ, UBLK_IOCTL_COMMIT_AND_FETCH_REQ)
"#;

    let out = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap()).join("ublk_cmd.rs");

    // Update the vendored Linux 6.8 `include/uapi/linux/ublk_cmd.h` to change the ABI.
    bindgen::Builder::default()
        .header_contents("shim.h", SHIM)
        .allowlist_type("ublk.*")
        .allowlist_var("UBLK.*")
        .derive_default(true)
        .parse_callbacks(Box::new(Derives))
        .use_core()
        .ctypes_prefix("libc")
        .generate()
        .expect("generating ublk bindings")
        .write_to_file(&out)
        .expect("writing ublk bindings");
}

/// Derives zerocopy's `IntoBytes` for the control command, so it copies into a
/// `uring_cmd` entry without `unsafe`. The derive fails the build if the struct
/// has padding.
#[derive(Debug)]
struct Derives;

impl bindgen::callbacks::ParseCallbacks for Derives {
    fn add_derives(&self, info: &bindgen::callbacks::DeriveInfo<'_>) -> Vec<String> {
        match info.name {
            "ublksrv_ctrl_cmd" => vec![
                "zerocopy::IntoBytes".to_string(),
                "zerocopy::Immutable".to_string(),
            ],
            _ => Vec::new(),
        }
    }
}
