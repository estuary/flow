//! libkrun's C ABI, transcribed from v1.19.4's `include/libkrun.h`.
//!
//! The `krun-sys` crate on crates.io is pinned at 1.10.1 and predates every
//! call the shim needs (`krun_add_virtiofs3`, `krun_fs_add_overlay_file`, the
//! `krun_disable_implicit_*` family, `krun_add_disk3`), so the declarations
//! live here instead. Only the calls the shim makes are declared.

use std::ffi::CStr;
use std::os::raw::{c_char, c_int};

pub const KRUN_LOG_LEVEL_WARN: u32 = 2;
pub const KRUN_LOG_LEVEL_DEBUG: u32 = 4;
pub const KRUN_LOG_STYLE_NEVER: u32 = 2;

pub const KRUN_DISK_FORMAT_RAW: u32 = 0;
pub const KRUN_SYNC_NONE: u32 = 0;
pub const KRUN_SYNC_RELAXED: u32 = 1;

/// `KRUN_FS_ROOT_TAG`: the virtiofs tag libkrun's kernel cmdline mounts as `/`.
pub const KRUN_FS_ROOT_TAG: &CStr = c"/dev/root";

/// `COMPAT_NET_FEATURES`: the virtio-net feature bits `krun_set_passt_fd` and
/// `krun_set_gvproxy_path` enable. CSUM, GUEST_CSUM, GUEST_TSO4, GUEST_UFO,
/// HOST_TSO4, HOST_UFO.
pub const COMPAT_NET_FEATURES: u32 =
    (1 << 0) | (1 << 1) | (1 << 7) | (1 << 10) | (1 << 11) | (1 << 14);

#[link(name = "krun")]
extern "C" {
    pub fn krun_init_log(target_fd: c_int, level: u32, style: u32, options: u32) -> i32;
    pub fn krun_create_ctx() -> i32;
    pub fn krun_set_vm_config(ctx_id: u32, num_vcpus: u8, ram_mib: u32) -> i32;
    pub fn krun_add_virtiofs3(
        ctx_id: u32,
        c_tag: *const c_char,
        c_path: *const c_char,
        shm_size: u64,
        read_only: bool,
    ) -> i32;
    pub fn krun_fs_add_overlay_file(
        ctx_id: u32,
        fs_tag: *const c_char,
        path: *const c_char,
        data: *const u8,
        data_len: usize,
        mode: u32,
        one_shot: bool,
    ) -> i32;
    pub fn krun_add_disk3(
        ctx_id: u32,
        block_id: *const c_char,
        disk_path: *const c_char,
        disk_format: u32,
        read_only: bool,
        direct_io: bool,
        sync_mode: u32,
    ) -> i32;
    pub fn krun_add_net_tap(
        ctx_id: u32,
        c_tap_name: *mut c_char,
        c_mac: *const u8,
        features: u32,
        flags: u32,
    ) -> i32;
    pub fn krun_disable_implicit_vsock(ctx_id: u32) -> i32;
    pub fn krun_add_vsock(ctx_id: u32, tsi_features: u32) -> i32;
    pub fn krun_add_vsock_port2(
        ctx_id: u32,
        port: u32,
        c_filepath: *const c_char,
        listen: bool,
    ) -> i32;
    pub fn krun_disable_implicit_console(ctx_id: u32) -> i32;
    pub fn krun_add_virtio_console_default(
        ctx_id: u32,
        input_fd: c_int,
        output_fd: c_int,
        err_fd: c_int,
    ) -> i32;
    pub fn krun_start_enter(ctx_id: u32) -> i32;
}

/// Every libkrun entry point returns a negative errno on failure.
pub fn check(what: &str, rc: i32) -> anyhow::Result<i32> {
    if rc < 0 {
        anyhow::bail!("{what}: {}", std::io::Error::from_raw_os_error(-rc));
    }
    Ok(rc)
}
