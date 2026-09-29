//! libkrun's C ABI, transcribed from v1.19.4's `include/libkrun.h`, behind a
//! trait the launch sequence is written against.
//!
//! Runtime loading lets the binary build without libkrun installed; only the
//! VMM image needs the library. This module owns C strings across each call.
//!
//! No `krun_free_ctx` is needed: launch failures exit the process, and
//! `krun_start_enter` takes over the process on success.

use std::convert::Infallible;
use std::ffi::CString;
use std::os::fd::RawFd;
use std::os::raw::{c_char, c_int};

/// The virtiofs tag libkrun's kernel command line mounts as `/`.
pub const ROOT_TAG: &str = "/dev/root";

/// The virtio-net feature bits `krun_set_passt_fd` and `krun_set_gvproxy_path`
/// enable: CSUM, GUEST_CSUM, GUEST_TSO4, GUEST_UFO, HOST_TSO4, HOST_UFO.
const NET_FEATURES: u32 = (1 << 0) | (1 << 1) | (1 << 7) | (1 << 10) | (1 << 11) | (1 << 14);

const LOG_LEVEL_WARN: u32 = 2;
const LOG_LEVEL_DEBUG: u32 = 4;
const LOG_STYLE_NEVER: u32 = 2;

const DISK_FORMAT_RAW: u32 = 0;
const SYNC_RELAXED: u32 = 1;

/// The calls `launch::enter` makes, in the order it makes them.
///
/// Fixed arguments are not parameters: `add_disk3` is only ever the scratch
/// disk, so its format, writability and sync mode are settled here rather than
/// at each call site.
pub trait Krun {
    fn init_log(&self, target: RawFd, debug: bool) -> anyhow::Result<()>;
    fn create_ctx(&self) -> anyhow::Result<u32>;
    fn set_vm_config(&self, ctx: u32, vcpus: u8, memory_mib: u32) -> anyhow::Result<()>;
    fn add_virtiofs3(&self, ctx: u32, tag: &str, path: &str, read_only: bool)
    -> anyhow::Result<()>;
    fn add_overlay_file(
        &self,
        ctx: u32,
        path: &str,
        data: &'static [u8],
        mode: u32,
    ) -> anyhow::Result<()>;
    fn add_scratch_disk(&self, ctx: u32, id: &str, path: &str) -> anyhow::Result<()>;
    fn add_net_tap(&self, ctx: u32, tap: &str, mac: &[u8; 6]) -> anyhow::Result<()>;
    fn disable_implicit_vsock(&self, ctx: u32) -> anyhow::Result<()>;
    fn add_vsock(&self, ctx: u32, tsi_features: u32) -> anyhow::Result<()>;
    fn add_vsock_port2(&self, ctx: u32, port: u32, path: &str, listen: bool) -> anyhow::Result<()>;
    fn disable_implicit_console(&self, ctx: u32) -> anyhow::Result<()>;
    fn add_console(
        &self,
        ctx: u32,
        input: RawFd,
        output: RawFd,
        error: RawFd,
    ) -> anyhow::Result<()>;
    /// Only returns on failure: otherwise libkrun takes over the process and
    /// exits with the guest workload's code.
    fn start_enter(&self, ctx: u32) -> anyhow::Result<Infallible>;
}

type InitLog = unsafe extern "C" fn(c_int, u32, u32, u32) -> i32;
type CreateCtx = unsafe extern "C" fn() -> i32;
type SetVmConfig = unsafe extern "C" fn(u32, u8, u32) -> i32;
type AddVirtiofs3 = unsafe extern "C" fn(u32, *const c_char, *const c_char, u64, bool) -> i32;
type AddOverlayFile =
    unsafe extern "C" fn(u32, *const c_char, *const c_char, *const u8, usize, u32, bool) -> i32;
type AddDisk3 =
    unsafe extern "C" fn(u32, *const c_char, *const c_char, u32, bool, bool, u32) -> i32;
type AddNetTap = unsafe extern "C" fn(u32, *mut c_char, *const u8, u32, u32) -> i32;
type CtxOnly = unsafe extern "C" fn(u32) -> i32;
type AddVsock = unsafe extern "C" fn(u32, u32) -> i32;
type AddVsockPort2 = unsafe extern "C" fn(u32, u32, *const c_char, bool) -> i32;
type AddConsole = unsafe extern "C" fn(u32, c_int, c_int, c_int) -> i32;

/// `_library` keeps the copied function pointers valid until this is dropped.
pub struct Dynamic {
    _library: libloading::Library,
    init_log: InitLog,
    create_ctx: CreateCtx,
    set_vm_config: SetVmConfig,
    add_virtiofs3: AddVirtiofs3,
    add_overlay_file: AddOverlayFile,
    add_disk3: AddDisk3,
    add_net_tap: AddNetTap,
    disable_implicit_vsock: CtxOnly,
    add_vsock: AddVsock,
    add_vsock_port2: AddVsockPort2,
    disable_implicit_console: CtxOnly,
    add_console: AddConsole,
    start_enter: CtxOnly,
}

const LIBRARY: &str = "libkrun.so.1";

impl Dynamic {
    pub fn load() -> anyhow::Result<Self> {
        // SAFETY: loading a library runs its initializers, which is what the
        // whole process is here to do. The name is a constant.
        let library = unsafe { libloading::Library::new(LIBRARY) }
            .map_err(|e| anyhow::anyhow!("loading {LIBRARY}: {e}"))?;

        macro_rules! bind {
            ($name:literal) => {{
                // SAFETY: the signature is transcribed from libkrun.h v1.19.4;
                // the pointer is copied out and the library outlives it.
                let symbol = unsafe { library.get($name) }
                    .map_err(|e| anyhow::anyhow!("binding {} in {LIBRARY}: {e}", name($name)))?;
                *symbol
            }};
        }

        Ok(Dynamic {
            init_log: bind!(b"krun_init_log\0"),
            create_ctx: bind!(b"krun_create_ctx\0"),
            set_vm_config: bind!(b"krun_set_vm_config\0"),
            add_virtiofs3: bind!(b"krun_add_virtiofs3\0"),
            add_overlay_file: bind!(b"krun_fs_add_overlay_file\0"),
            add_disk3: bind!(b"krun_add_disk3\0"),
            add_net_tap: bind!(b"krun_add_net_tap\0"),
            disable_implicit_vsock: bind!(b"krun_disable_implicit_vsock\0"),
            add_vsock: bind!(b"krun_add_vsock\0"),
            add_vsock_port2: bind!(b"krun_add_vsock_port2\0"),
            disable_implicit_console: bind!(b"krun_disable_implicit_console\0"),
            add_console: bind!(b"krun_add_virtio_console_default\0"),
            start_enter: bind!(b"krun_start_enter\0"),
            _library: library,
        })
    }
}

impl Krun for Dynamic {
    fn init_log(&self, target: RawFd, debug: bool) -> anyhow::Result<()> {
        let level = if debug {
            LOG_LEVEL_DEBUG
        } else {
            LOG_LEVEL_WARN
        };
        // SAFETY: every argument is a scalar, and `target` is open.
        check("krun_init_log", unsafe {
            (self.init_log)(target, level, LOG_STYLE_NEVER, 0)
        })?;
        Ok(())
    }

    fn create_ctx(&self) -> anyhow::Result<u32> {
        // SAFETY: no arguments.
        let ctx = check("krun_create_ctx", unsafe { (self.create_ctx)() })?;
        Ok(ctx as u32)
    }

    fn set_vm_config(&self, ctx: u32, vcpus: u8, memory_mib: u32) -> anyhow::Result<()> {
        // SAFETY: every argument is a scalar.
        check("krun_set_vm_config", unsafe {
            (self.set_vm_config)(ctx, vcpus, memory_mib)
        })?;
        Ok(())
    }

    fn add_virtiofs3(
        &self,
        ctx: u32,
        tag: &str,
        path: &str,
        read_only: bool,
    ) -> anyhow::Result<()> {
        let (tag_c, path_c) = (cstring(tag)?, cstring(path)?);
        // SAFETY: both strings are bound to locals that outlive the call, and
        // libkrun copies what it keeps. `shm_size` is zero: no DAX window.
        check(&format!("krun_add_virtiofs3({tag})"), unsafe {
            (self.add_virtiofs3)(ctx, tag_c.as_ptr(), path_c.as_ptr(), 0, read_only)
        })?;
        Ok(())
    }

    fn add_overlay_file(
        &self,
        ctx: u32,
        path: &str,
        data: &'static [u8],
        mode: u32,
    ) -> anyhow::Result<()> {
        let (tag_c, path_c) = (cstring(ROOT_TAG)?, cstring(path)?);
        // SAFETY: the strings outlive the call, and `data` is `'static`
        // because libkrun serves overlay files straight out of this memory for
        // the VM's whole life and never copies it.
        check(&format!("krun_fs_add_overlay_file({path})"), unsafe {
            (self.add_overlay_file)(
                ctx,
                tag_c.as_ptr(),
                path_c.as_ptr(),
                data.as_ptr(),
                data.len(),
                mode,
                false,
            )
        })?;
        Ok(())
    }

    fn add_scratch_disk(&self, ctx: u32, id: &str, path: &str) -> anyhow::Result<()> {
        let (id_c, path_c) = (cstring(id)?, cstring(path)?);
        // SAFETY: both strings outlive the call. Writable, no direct IO, and
        // relaxed sync: the disk is unlinked already and dies with the VM, so
        // there is nothing for a flush to make durable.
        check(&format!("krun_add_disk3({id})"), unsafe {
            (self.add_disk3)(
                ctx,
                id_c.as_ptr(),
                path_c.as_ptr(),
                DISK_FORMAT_RAW,
                false,
                false,
                SYNC_RELAXED,
            )
        })?;
        Ok(())
    }

    fn add_net_tap(&self, ctx: u32, tap: &str, mac: &[u8; 6]) -> anyhow::Result<()> {
        // Mutable because libkrun takes the interface name as a writable
        // buffer, in the shape of the `TUNSETIFF` ifreq it fills in.
        let mut tap_c = cstring(tap)?.into_bytes_with_nul();
        // SAFETY: the buffer and the MAC outlive the call.
        check("krun_add_net_tap", unsafe {
            (self.add_net_tap)(
                ctx,
                tap_c.as_mut_ptr().cast(),
                mac.as_ptr(),
                NET_FEATURES,
                0,
            )
        })?;
        Ok(())
    }

    fn disable_implicit_vsock(&self, ctx: u32) -> anyhow::Result<()> {
        // SAFETY: one scalar argument.
        check("krun_disable_implicit_vsock", unsafe {
            (self.disable_implicit_vsock)(ctx)
        })?;
        Ok(())
    }

    fn add_vsock(&self, ctx: u32, tsi_features: u32) -> anyhow::Result<()> {
        // SAFETY: two scalar arguments.
        check("krun_add_vsock", unsafe {
            (self.add_vsock)(ctx, tsi_features)
        })?;
        Ok(())
    }

    fn add_vsock_port2(&self, ctx: u32, port: u32, path: &str, listen: bool) -> anyhow::Result<()> {
        let path_c = cstring(path)?;
        // SAFETY: the string outlives the call.
        check("krun_add_vsock_port2", unsafe {
            (self.add_vsock_port2)(ctx, port, path_c.as_ptr(), listen)
        })?;
        Ok(())
    }

    fn disable_implicit_console(&self, ctx: u32) -> anyhow::Result<()> {
        // SAFETY: one scalar argument.
        check("krun_disable_implicit_console", unsafe {
            (self.disable_implicit_console)(ctx)
        })?;
        Ok(())
    }

    fn add_console(
        &self,
        ctx: u32,
        input: RawFd,
        output: RawFd,
        error: RawFd,
    ) -> anyhow::Result<()> {
        // SAFETY: every argument is a scalar and each descriptor is open.
        check("krun_add_virtio_console_default", unsafe {
            (self.add_console)(ctx, input, output, error)
        })?;
        Ok(())
    }

    fn start_enter(&self, ctx: u32) -> anyhow::Result<Infallible> {
        // SAFETY: one scalar argument. On success this never returns.
        check("krun_start_enter", unsafe { (self.start_enter)(ctx) })?;
        anyhow::bail!("krun_start_enter returned without starting the VM")
    }
}

fn cstring(value: &str) -> anyhow::Result<CString> {
    CString::new(value).map_err(|e| anyhow::anyhow!("{value:?} cannot be a C string: {e}"))
}

/// Every libkrun entry point returns a negative errno on failure.
fn check(what: &str, rc: i32) -> anyhow::Result<i32> {
    if rc < 0 {
        anyhow::bail!("{what}: {}", std::io::Error::from_raw_os_error(-rc));
    }
    Ok(rc)
}

fn name(symbol: &[u8]) -> String {
    String::from_utf8_lossy(&symbol[..symbol.len() - 1]).into_owned()
}
