//! `flow-sandbox-helper`: boots a connector image as a libkrun microVM.
//!
//! Runs as PID 1 of a podman container that the reactor starts in place of the
//! connector. Every path it reads is a mount the caller supplied; see
//! CONTRACTS.md "Helper CLI".

mod cli;
mod console;
mod disk;
mod image;
mod net;
mod sys;

use std::ffi::CString;
use std::os::fd::AsRawFd;
use std::path::Path;

const ROOTFS: &str = "/rootfs";
const INSPECT_JSON: &str = "/init/image-inspect.json";
const CONNECTOR_INIT: &str = "/init/flow-connector-init";
const FLOW_INIT: &str = "/flow-init";
const VSOCK_ECHO: &str = "/vsock-echo";
const VENV_DIR: &str = "/venv";
const SCRATCH_BACKING: &str = "/scratch-backing";
const INIT_SOCK: &str = "/sock/init.sock";

/// The deps disk's guest device name. Second disk added, so second name.
const DEPS_DEV: &str = "/dev/vdb";

const VSOCK_PORT: u32 = 49092;
const VENV_DAX_BYTES: u64 = 512 * 1024 * 1024;

fn main() -> std::process::ExitCode {
    // `run` is typed to never return successfully: libkrun takes over the
    // process, so control comes back here only on the way to an error exit.
    let Err(error) = run();
    // Diagnostics must never begin with a space; the reactor reads a leading
    // space on stderr as connector-init's readiness signal.
    eprintln!("flow-sandbox-helper: {error:#}");
    std::process::ExitCode::from(2)
}

fn run() -> anyhow::Result<std::convert::Infallible> {
    timing("start");
    let args = cli::parse(std::env::args().skip(1).collect())?;
    let egress = read_egress_mode(&args.policy)?;

    sys::check("krun_init_log", unsafe {
        sys::krun_init_log(
            libc::STDERR_FILENO,
            if args.debug {
                sys::KRUN_LOG_LEVEL_DEBUG
            } else {
                sys::KRUN_LOG_LEVEL_WARN
            },
            sys::KRUN_LOG_STYLE_NEVER,
            0,
        )
    })?;

    // The guest's first packet must meet a finished ruleset, so all of the
    // network setup precedes the VM.
    net::create_tap()?;
    net::load_egress(&args.policy)?;
    let _resolver = match egress.as_str() {
        "none" => None,
        _ => {
            let upstream = match &args.resolver_upstream {
                Some(upstream) => upstream.clone(),
                None => net::upstream_nameserver()?,
            };
            Some(net::spawn_resolver(&args.policy, &upstream)?)
        }
    };

    let scratch_fd = disk::create(Path::new(SCRATCH_BACKING), args.disk_mib)?;
    let image = image::load(Path::new(INSPECT_JSON), Path::new(ROOTFS))?;
    let krun_config = image::krun_config(&image, &guest_cmd(&args, &image));

    let ctx = sys::check("krun_create_ctx", unsafe { sys::krun_create_ctx() })? as u32;

    // Bound to locals rather than passed as temporaries: libkrun copies each
    // string, but a `CString::new(..).as_ptr()` temporary is a well-known way to
    // hand C a dangling pointer.
    let rootfs = CString::new(ROOTFS)?;
    let venv_dir = CString::new(VENV_DIR)?;
    let scratch_path = CString::new(disk::proc_path(scratch_fd))?;
    let deps_image = args
        .deps_image
        .as_ref()
        .map(|path| CString::new(path.as_os_str().as_encoded_bytes()))
        .transpose()?;
    let init_sock = CString::new(INIT_SOCK)?;
    let mut tap = CString::new(net::TAP)?.into_bytes_with_nul();
    let console_out = console::output_fd(args.debug)?;
    let devnull = std::fs::File::open("/dev/null")?;

    sys::check("krun_set_vm_config", unsafe {
        sys::krun_set_vm_config(ctx, args.vcpus, args.memory_mib)
    })?;

    // Writable root: `/rootfs` is podman's per-container layer over the
    // connector image (`--mount type=image,...,rw=true`), removed with the
    // container. The guest root is writable exactly as a container's is today,
    // and nothing lays an overlay inside the guest - one over a virtiofs lower
    // cannot copy up at all, see CONTRACTS "Helper CLI".
    sys::check("krun_add_virtiofs3(/dev/root)", unsafe {
        sys::krun_add_virtiofs3(
            ctx,
            sys::KRUN_FS_ROOT_TAG.as_ptr(),
            rootfs.as_ptr(),
            0,
            false,
        )
    })?;
    sys::check("krun_add_virtiofs3(venv)", unsafe {
        sys::krun_add_virtiofs3(
            ctx,
            c"venv".as_ptr(),
            venv_dir.as_ptr(),
            if args.venv_dax { VENV_DAX_BYTES } else { 0 },
            true,
        )
    })?;

    // Overlay paths carry no leading slash: they are entries in the root
    // virtiofs, which is how libkrun serves its own /init.krun.
    add_overlay_file(ctx, "flow-init", mmap_file(Path::new(FLOW_INIT))?, 0o100755)?;
    add_overlay_file(
        ctx,
        "vsock-echo",
        mmap_file(Path::new(VSOCK_ECHO))?,
        0o100755,
    )?;
    add_overlay_file(
        ctx,
        "flow-connector-init",
        mmap_file(Path::new(CONNECTOR_INIT))?,
        0o100755,
    )?;
    add_overlay_file(
        ctx,
        "image-inspect.json",
        mmap_file(Path::new(INSPECT_JSON))?,
        0o100644,
    )?;
    add_overlay_file(
        ctx,
        ".krun_config.json",
        Box::leak(krun_config.into_boxed_slice()),
        0o100644,
    )?;

    sys::check("krun_add_disk3(scratch)", unsafe {
        sys::krun_add_disk3(
            ctx,
            c"scratch".as_ptr(),
            scratch_path.as_ptr(),
            sys::KRUN_DISK_FORMAT_RAW,
            false,
            false,
            sys::KRUN_SYNC_RELAXED,
        )
    })?;

    // The per-tag dependency image, second so that scratch keeps `/dev/vda`
    // and this is `/dev/vdb` (flow-init is told the name, and the test asserts
    // it). Read-only, and KRUN_SYNC_NONE because nothing can write it: a flush
    // the guest cannot cause is a virtqueue round trip for nothing.
    if let Some(deps_image) = &deps_image {
        sys::check("krun_add_disk3(deps)", unsafe {
            sys::krun_add_disk3(
                ctx,
                c"deps".as_ptr(),
                deps_image.as_ptr(),
                sys::KRUN_DISK_FORMAT_RAW,
                true,
                false,
                sys::KRUN_SYNC_NONE,
            )
        })?;
    }

    sys::check("krun_add_net_tap", unsafe {
        sys::krun_add_net_tap(
            ctx,
            tap.as_mut_ptr().cast(),
            net::GUEST_MAC.as_ptr(),
            sys::COMPAT_NET_FEATURES,
            0,
        )
    })?;

    // The explicit zero is load-bearing. The implicit vsock device enables TSI
    // INET hijacking, which would leave host-side socket proxies live even with
    // a tap in place.
    sys::check("krun_disable_implicit_vsock", unsafe {
        sys::krun_disable_implicit_vsock(ctx)
    })?;
    sys::check("krun_add_vsock", unsafe { sys::krun_add_vsock(ctx, 0) })?;
    sys::check("krun_add_vsock_port2", unsafe {
        sys::krun_add_vsock_port2(ctx, VSOCK_PORT, init_sock.as_ptr(), true)
    })?;

    sys::check("krun_disable_implicit_console", unsafe {
        sys::krun_disable_implicit_console(ctx)
    })?;
    sys::check("krun_add_virtio_console_default", unsafe {
        sys::krun_add_virtio_console_default(
            ctx,
            devnull.as_raw_fd(),
            console_out,
            libc::STDERR_FILENO,
        )
    })?;

    if args.thp_disable {
        // Safety: PR_SET_THP_DISABLE takes no pointer arguments.
        if unsafe { libc::prctl(libc::PR_SET_THP_DISABLE, 1, 0, 0, 0) } < 0 {
            return Err(anyhow::anyhow!(std::io::Error::last_os_error())
                .context("prctl(PR_SET_THP_DISABLE)"));
        }
    }

    // Only returns on failure: otherwise libkrun takes over the process and
    // exits with the guest workload's code.
    timing("krun_start_enter");
    sys::check("krun_start_enter", unsafe { sys::krun_start_enter(ctx) })?;
    anyhow::bail!("krun_start_enter returned without starting the VM")
}

/// One line per launch stage on stderr, stamped with the host wall clock, so
/// experiment 2 can split the launch into podman's share and the shim's. The
/// host clock is the right one here: the reactor's `podman run` and the
/// readiness byte it waits for are both measured against it. Never a leading
/// space, which is connector-init's readiness signal.
fn timing(stage: &str) {
    let micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_micros());
    eprintln!("flow-sandbox-helper: timing stage={stage} wall_us={micros}");
}

/// The argv libkrun's guest init execs. flow-init leads it unless the caller
/// asked for the workload alone, and everything after `--` is the workload.
///
/// Note that `krun_set_exec` is deliberately never called: libkrun's init
/// consults `Cmd` only when `KRUN_INIT` (which `krun_set_exec` sets) is absent,
/// so setting both would silently discard this argv.
fn guest_cmd(args: &cli::Args, image: &image::ImageConfig) -> Vec<String> {
    let workload = args.exec.clone().unwrap_or_else(|| {
        vec![
            "/flow-connector-init".to_string(),
            "--image-inspect-json-path=/image-inspect.json".to_string(),
            format!("--vsock-port={VSOCK_PORT}"),
        ]
    });
    if args.no_flow_init {
        return workload;
    }

    let mut cmd = vec![
        FLOW_INIT.to_string(),
        "--guest-ip".to_string(),
        net::GUEST_CIDR.to_string(),
        "--gateway".to_string(),
        net::HELPER_IP.to_string(),
        "--nameserver".to_string(),
        net::HELPER_IP.to_string(),
        "--uid".to_string(),
        image.uid.to_string(),
        "--gid".to_string(),
        image.gid.to_string(),
    ];
    if args.deps_image.is_some() {
        cmd.push("--deps-dev".to_string());
        cmd.push(DEPS_DEV.to_string());
        cmd.push("--deps-fstype".to_string());
        cmd.push(args.deps_fstype.clone());
    }
    if args.venv_dax {
        cmd.push("--venv-dax".to_string());
    }
    if args.run_as_root {
        cmd.push("--run-as-root".to_string());
    }
    if let Some(command) = &args.as_root_exec {
        cmd.push("--as-root-exec".to_string());
        cmd.push(command.clone());
    }
    cmd.push("--".to_string());
    cmd.extend(workload);
    cmd
}

/// Only `egress` matters to the shim: it decides whether a resolver runs. The
/// rest of the policy belongs to the egress binaries.
fn read_egress_mode(policy: &Path) -> anyhow::Result<String> {
    #[derive(serde::Deserialize)]
    struct Policy {
        egress: String,
    }

    let content =
        std::fs::read(policy).map_err(|e| anyhow::anyhow!("reading {}: {e}", policy.display()))?;
    let Policy { egress } = serde_json::from_slice(&content)
        .map_err(|e| anyhow::anyhow!("parsing {}: {e}", policy.display()))?;
    Ok(egress)
}

fn add_overlay_file(ctx: u32, path: &str, data: &'static [u8], mode: u32) -> anyhow::Result<()> {
    let path_c = CString::new(path)?;
    sys::check(&format!("krun_fs_add_overlay_file({path})"), unsafe {
        sys::krun_fs_add_overlay_file(
            ctx,
            sys::KRUN_FS_ROOT_TAG.as_ptr(),
            path_c.as_ptr(),
            data.as_ptr(),
            data.len(),
            mode,
            false,
        )
    })?;
    Ok(())
}

/// libkrun serves overlay files straight out of this memory for the VM's whole
/// life and never copies it, so the mapping is deliberately leaked.
fn mmap_file(path: &Path) -> anyhow::Result<&'static [u8]> {
    let file = std::fs::File::open(path)
        .map_err(|e| anyhow::anyhow!("opening {}: {e}", path.display()))?;
    let len = file.metadata()?.len() as usize;
    if len == 0 {
        return Ok(&[]);
    }

    // Safety: `len` is the file's size and the mapping is never unmapped.
    let addr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ,
            libc::MAP_PRIVATE,
            file.as_raw_fd(),
            0,
        )
    };
    if addr == libc::MAP_FAILED {
        return Err(anyhow::anyhow!(std::io::Error::last_os_error())
            .context(format!("mmap {}", path.display())));
    }
    // Safety: mmap returned a readable mapping of `len` bytes that outlives the
    // process.
    Ok(unsafe { std::slice::from_raw_parts(addr.cast::<u8>(), len) })
}
