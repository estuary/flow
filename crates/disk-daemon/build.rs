fn main() {
    println!("cargo:rerun-if-changed=ublk_cmd.h");

    // The `UBLK_U_*` opcodes are `_IOR` and `_IOWR` macros, which bindgen cannot
    // evaluate. Assigning each one to a constant lets the C compiler fold it into a
    // value bindgen does emit, under a prefixed name, because a macro expands on
    // both sides of its own definition. `Callbacks::item_name` strips the prefix.
    const ENCODED: &str = r#"
#define ENCODED(macro) const unsigned int ENCODED_##macro = macro;

ENCODED(UBLK_U_CMD_ADD_DEV)
ENCODED(UBLK_U_CMD_DEL_DEV)
ENCODED(UBLK_U_CMD_DEL_DEV_ASYNC)
ENCODED(UBLK_U_CMD_START_DEV)
ENCODED(UBLK_U_CMD_STOP_DEV)
ENCODED(UBLK_U_CMD_SET_PARAMS)
ENCODED(UBLK_U_IO_FETCH_REQ)
ENCODED(UBLK_U_IO_COMMIT_AND_FETCH_REQ)
"#;

    // Each union becomes the one member this crate uses, so that reading a field
    // needs no `unsafe`, and the I/O command can derive `IntoBytes`. The members
    // of each union have one size, so each struct keeps its layout. The other
    // members serve only zoned devices.
    let header = std::fs::read_to_string("ublk_cmd.h").expect("reading ublk_cmd.h");
    let header = flatten(&header, r"union \{[^{}]*?(__u64\s+addr;)[^{}]*\};");
    let header = flatten(&header, r"union \{\s*(__u32\s+nr_sectors;)[^{}]*\};");

    // `errno.h` lets bindgen evaluate `UBLK_IO_RES_ABORT`, which is `-ENODEV`.
    let shim = format!("#include <asm/ioctl.h>\n#include <errno.h>\n{header}\n{ENCODED}");

    let out = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap()).join("ublk_cmd.rs");

    // Update the vendored Linux 7.0 `include/uapi/linux/ublk_cmd.h` to change the ABI.
    bindgen::Builder::default()
        .header_contents("shim.h", &shim)
        .allowlist_type("ublk.*")
        // The allowlist sees each name before `Callbacks::item_name` renames it.
        .allowlist_var("(ENCODED_)?UBLK.*")
        .derive_default(true)
        .parse_callbacks(Box::new(Callbacks))
        .use_core()
        .generate()
        .expect("generating ublk bindings")
        .write_to_file(&out)
        .expect("writing ublk bindings");
}

/// Replace the one match of `pattern` in `header` with its first group. A header
/// that does not match exactly once fails the build, rather than keep its union.
fn flatten(header: &str, pattern: &str) -> String {
    let re = regex::Regex::new(pattern).unwrap();
    assert_eq!(
        re.find_iter(header).count(),
        1,
        "`{pattern}` must match ublk_cmd.h exactly once"
    );
    re.replace(header, "$1").into_owned()
}

#[derive(Debug)]
struct Callbacks;

impl bindgen::callbacks::ParseCallbacks for Callbacks {
    /// Derives zerocopy's `IntoBytes` for the commands, so each copies into a
    /// `uring_cmd` entry without `unsafe`. The derive fails the build if a command
    /// has padding.
    fn add_derives(&self, info: &bindgen::callbacks::DeriveInfo<'_>) -> Vec<String> {
        match info.name {
            "ublksrv_ctrl_cmd" | "ublksrv_io_cmd" => vec![
                "zerocopy::IntoBytes".to_string(),
                "zerocopy::Immutable".to_string(),
            ],
            _ => Vec::new(),
        }
    }

    fn item_name(&self, item_info: bindgen::callbacks::ItemInfo) -> Option<String> {
        item_info.name.strip_prefix("ENCODED_").map(str::to_string)
    }
}
