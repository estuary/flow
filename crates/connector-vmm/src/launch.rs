//! The ordered sequence `run` executes, from a policy on disk to a running VM.
//!
//! `run` resolves paths, descriptors and image config into a `Machine`.
//! `enter` issues libkrun calls that tests can record without a VM.

use crate::image;
use crate::krun::{self, Krun};
use crate::net;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};

/// The guest init, carried in the VMM's own image and injected into the guest
/// root as an overlay file. The leading slash is the guest path; the overlay
/// entry below has none, because overlay paths are entries in the root
/// virtiofs, which is how libkrun serves its own `/init.krun`.
pub const GUEST_INIT: &str = "/flow-guest-init";
const GUEST_INIT_OVERLAY: &str = "flow-guest-init";
const KRUN_CONFIG_OVERLAY: &str = ".krun_config.json";

/// The connector image, mounted `type=image,rw=true` so the guest root is
/// podman's per-container layer.
const ROOTFS: &str = "/rootfs";
const SCRATCH_BACKING: &str = "/scratch-backing";
const PERSISTENT_DISK_SOURCE: &str = "/persistent-disk";
const INIT_SOCK: &str = "/sock/init.sock";

const CONNECTOR_MOUNT_TAG: &str = "connector-mount";
const PERSISTENT_DISK_TAG: &str = "persistent-disk";
const SCRATCH_ID: &str = "scratch";

const VSOCK_PORT: u32 = 49092;

/// Where the scratch descriptor lands, so that everything above it can be
/// closed in one call and the name libkrun is given is a constant.
const SCRATCH_FD: RawFd = 3;

#[derive(clap::Args, Debug)]
pub struct Args {
    /// The egress policy JSON, as the launcher writes it into /init/policy.json.
    #[arg(long, value_name = "PATH")]
    pub policy: PathBuf,

    /// The connector mount, at the absolute path it has on the host. Shared
    /// into the guest read-only at that same path, and named by
    /// CONNECTOR_MOUNT in the workload's environment.
    #[arg(long, value_name = "PATH", value_parser = guest_path)]
    pub connector_mount: String,

    /// Guest RAM. The caller sizes the container's own limit above it.
    #[arg(long, value_name = "N")]
    pub memory_mib: u32,

    #[arg(long, value_name = "N")]
    pub vcpus: u8,

    /// The size of the guest's scratch disk, which lives and dies with this VM.
    #[arg(long, value_name = "N")]
    pub disk_mib: u64,

    /// Mount the task's persistent disk share at this absolute guest path.
    #[arg(long, value_name = "GUEST_PATH", value_parser = guest_path)]
    pub persistent_disk: Option<String>,

    /// Test only: run the workload as guest root.
    #[arg(long)]
    pub run_as_root: bool,

    /// Test only: run CMD under /bin/sh as guest root before privileges drop.
    #[arg(long, value_name = "CMD")]
    pub as_root_exec: Option<String>,

    /// Test only: libkrun debug logging, VMM stage lines, the console tee and
    /// the resolver's per-query decision log.
    #[arg(long)]
    pub debug: bool,

    /// Test only: the upstream the resolver forwards to, in place of the
    /// nameserver in the VMM's own /etc/resolv.conf.
    #[arg(long, value_name = "IP:PORT")]
    pub resolver_upstream: Option<SocketAddr>,

    /// Test only: replace the default workload argv. Takes every remaining
    /// argument, so it must come last.
    #[arg(long, value_name = "ARGV", num_args = 1.., allow_hyphen_values = true)]
    pub exec: Option<Vec<String>>,
}

/// Everything `enter` needs, with nothing left to look up.
pub struct Machine {
    pub vcpus: u8,
    pub memory_mib: u32,
    pub debug: bool,
    pub connector_mount: String,
    pub persistent_disk: bool,
    pub overlays: Vec<Overlay>,
    pub scratch_path: String,
    pub console: Console,
}

pub struct Overlay {
    pub path: &'static str,
    /// `'static` because libkrun serves overlay files out of this memory for
    /// the VM's whole life and never copies it, so the mapping is leaked.
    pub data: &'static [u8],
    pub mode: u32,
}

pub struct Console {
    pub input: RawFd,
    pub output: RawFd,
    pub error: RawFd,
}

pub fn run(args: &Args) -> anyhow::Result<Infallible> {
    check_read_only(&args.connector_mount)?;

    let policy = crate::policy::load(&args.policy)?;

    // The first side-effecting step, so a missing or wrong library is the
    // first line on stderr with nothing half-built behind it.
    let krun = krun::Dynamic::load()?;
    stage(args.debug, "libkrun-loaded");

    // The guest's first packet has to meet a finished ruleset, so the whole
    // network is up before anything else happens.
    let vmm_subnets = net::vmm_subnets()?;
    let ruleset = crate::ruleset::render(&policy, &vmm_subnets)?;
    net::create_tap()?;
    net::check_ipv6_disabled()?;
    net::apply_ruleset(&ruleset)?;
    stage(args.debug, "network-ready");

    let scratch = crate::disk::create(Path::new(SCRATCH_BACKING), args.disk_mib)?;
    stage(args.debug, "scratch-ready");

    let mount = args.connector_mount.as_str();
    let image = image::load(
        &Path::new(mount).join("image-inspect.json"),
        Path::new(ROOTFS),
    )?;
    let guest_argv = image::guest_argv(&image::Guest {
        connector_mount: mount,
        persistent_disk: args.persistent_disk.as_deref(),
        run_as_root: args.run_as_root,
        as_root_exec: args.as_root_exec.as_deref(),
        exec: args.exec.as_deref(),
        uid: image.uid,
        gid: image.gid,
        vsock_port: VSOCK_PORT,
    });
    let krun_config = image::krun_config(&image, &guest_argv, &contract_env(mount));

    let overlays = vec![
        Overlay {
            path: GUEST_INIT_OVERLAY,
            data: mmap(Path::new(GUEST_INIT))?,
            mode: 0o100755,
        },
        Overlay {
            path: KRUN_CONFIG_OVERLAY,
            data: Box::leak(krun_config.into_boxed_slice()),
            mode: 0o100644,
        },
    ];
    stage(args.debug, "image-config-ready");

    // Sweep before starting workers, which share this descriptor table.
    let scratch = sweep(scratch)?;
    stage(args.debug, "descriptors-swept");

    let console = Console {
        input: crate::console::input()?,
        output: crate::console::output(args.debug)?,
        error: libc::STDERR_FILENO,
    };

    if policy.egress == crate::policy::Mode::Public {
        let upstream = match args.resolver_upstream {
            Some(upstream) => upstream,
            None => net::upstream_nameserver()?,
        };
        // Returns once the resolver is listening, so a failure here is still a
        // pre-VM failure. After it, the resolver ends the process itself.
        crate::resolver::start(crate::resolver::Config::new(
            &policy,
            &vmm_subnets,
            SocketAddr::new(net::VMM_IP.into(), 53),
            upstream,
            args.debug,
        ))?;
        stage(args.debug, "resolver-listening");
    }

    // The socket libkrun binds inherits this, so `init.sock` is mode 0777 and
    // an unprivileged process can dial it. The `sock/` directory's mode is the
    // access control, and it belongs to the launcher.
    // SAFETY: umask takes no pointers and cannot fail.
    unsafe { libc::umask(0) };

    let machine = Machine {
        vcpus: args.vcpus,
        memory_mib: args.memory_mib,
        debug: args.debug,
        connector_mount: mount.to_string(),
        persistent_disk: args.persistent_disk.is_some(),
        overlays,
        scratch_path: crate::disk::proc_path(scratch),
        console,
    };
    stage(args.debug, "krun-start-enter");

    enter(&krun, &machine)
}

pub fn enter(krun: &dyn Krun, machine: &Machine) -> anyhow::Result<Infallible> {
    krun.init_log(libc::STDERR_FILENO, machine.debug)?;
    let ctx = krun.create_ctx()?;
    krun.set_vm_config(ctx, machine.vcpus, machine.memory_mib)?;

    // Writable root: `/rootfs` is podman's per-container layer over the
    // connector image, removed with the container. The guest root is writable
    // exactly as a container's is today, and nothing lays an overlay inside
    // the guest, because one over a virtiofs lower cannot copy up at all.
    krun.add_virtiofs3(ctx, krun::ROOT_TAG, ROOTFS, false)?;
    krun.add_virtiofs3(ctx, CONNECTOR_MOUNT_TAG, &machine.connector_mount, true)?;

    if machine.persistent_disk {
        krun.add_virtiofs3(ctx, PERSISTENT_DISK_TAG, PERSISTENT_DISK_SOURCE, false)?;
    }
    for overlay in &machine.overlays {
        krun.add_overlay_file(ctx, overlay.path, overlay.data, overlay.mode)?;
    }
    krun.add_scratch_disk(ctx, SCRATCH_ID, &machine.scratch_path)?;
    krun.add_net_tap(ctx, net::TAP, &net::GUEST_MAC)?;

    // The explicit zero is load-bearing. The implicit vsock device enables TSI
    // INET hijacking, which would leave host-side socket proxies live even
    // with a tap in place.
    krun.disable_implicit_vsock(ctx)?;
    krun.add_vsock(ctx, 0)?;
    // libkrun listens, the guest connects, and neither side half-closes: a
    // shutdown of one direction would be read as end of stream by connector-init.
    krun.add_vsock_port2(ctx, VSOCK_PORT, INIT_SOCK, true)?;

    krun.disable_implicit_console(ctx)?;
    krun.add_console(
        ctx,
        machine.console.input,
        machine.console.output,
        machine.console.error,
    )?;

    krun.start_enter(ctx)
}

/// Move the scratch disk to a known descriptor and close everything above it.
///
/// The descriptors this process still owns afterwards are exactly stdio and
/// the scratch disk. The overlay mappings survive their files being closed,
/// nothing holds a tap descriptor - `ip` creates the device and libkrun opens
/// `/dev/net/tun` itself - and the resolver's sockets do not exist yet.
fn sweep(scratch: RawFd) -> anyhow::Result<RawFd> {
    if scratch != SCRATCH_FD {
        // SAFETY: both are descriptor numbers; dup2 closes whatever was at the
        // destination, and the source stays open until the range close below.
        if unsafe { libc::dup2(scratch, SCRATCH_FD) } < 0 {
            return Err(anyhow::anyhow!(std::io::Error::last_os_error())
                .context(format!("moving the scratch disk to fd {SCRATCH_FD}")));
        }
    }
    // SAFETY: closes descriptors this process has no further use for.
    if unsafe { libc::close_range(SCRATCH_FD as libc::c_uint + 1, libc::c_uint::MAX, 0) } < 0 {
        return Err(anyhow::anyhow!(std::io::Error::last_os_error())
            .context("closing inherited descriptors"));
    }
    Ok(SCRATCH_FD)
}

/// `/init` is excluded because its policy is read once before VM setup.
fn read_only_mounts(connector_mount: &str) -> [(&str, &str); 2] {
    [
        ("/", "--read-only"),
        (
            connector_mount,
            "--mount type=bind,source=M,target=M,readonly",
        ),
    ]
}

/// The host owns the connector mount, which may carry task credentials;
/// its container bind must be read-only even though the guest share already is.
fn check_read_only(connector_mount: &str) -> anyhow::Result<()> {
    for (path, flag) in read_only_mounts(connector_mount) {
        let c_path = std::ffi::CString::new(path)
            .map_err(|e| anyhow::anyhow!("{path} cannot be a C string: {e}"))?;
        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };

        // SAFETY: a NUL-terminated path and a `statvfs` the call fills in,
        // both alive across it.
        if unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) } < 0 {
            return Err(
                anyhow::anyhow!(std::io::Error::last_os_error()).context(format!("statvfs {path}"))
            );
        }
        if stat.f_flag & libc::ST_RDONLY == 0 {
            anyhow::bail!("{}", writable_mount(path, flag));
        }
    }
    Ok(())
}

fn writable_mount(path: &str, flag: &str) -> String {
    format!("{path} is writable; the VMM container must be launched with `{flag}`")
}

/// Forward the runtime's log settings: defaults depend on the data plane.
fn contract_env(mount: &str) -> Vec<(&'static str, String)> {
    let mut env = vec![("CONNECTOR_MOUNT", mount.to_string())];

    for name in ["LOG_FORMAT", "LOG_LEVEL"] {
        if let Ok(value) = std::env::var(name) {
            env.push((name, value));
        }
    }
    env
}

fn stage(debug: bool, name: &str) {
    if debug {
        eprint!("{}", crate::framed(&format!("stage={name}")));
    }
}

/// Leak the overlay mapping for libkrun; the backing file can close on return.
fn mmap(path: &Path) -> anyhow::Result<&'static [u8]> {
    let file = std::fs::File::open(path)
        .map_err(|e| anyhow::anyhow!("opening {}: {e}", path.display()))?;
    let len = file
        .metadata()
        .map_err(|e| anyhow::anyhow!("sizing {}: {e}", path.display()))?
        .len() as usize;
    if len == 0 {
        anyhow::bail!("{} is empty", path.display());
    }

    // SAFETY: `len` is the file's size and the mapping is never unmapped.
    let address = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ,
            libc::MAP_PRIVATE,
            file.as_raw_fd(),
            0,
        )
    };
    if address == libc::MAP_FAILED {
        return Err(anyhow::anyhow!(std::io::Error::last_os_error())
            .context(format!("mmap {}", path.display())));
    }
    // SAFETY: mmap returned a readable mapping of `len` bytes that outlives
    // the process.
    Ok(unsafe { std::slice::from_raw_parts(address.cast::<u8>(), len) })
}

/// A relative path would be resolved against the image's working directory,
/// and mounting over `/` would replace the root of the guest's mount
/// namespace, which silently costs the workload's exit code.
fn guest_path(raw: &str) -> Result<String, String> {
    if !raw.starts_with('/') {
        return Err("expected an absolute path".to_string());
    }
    if raw.trim_end_matches('/').is_empty() {
        return Err("the guest root is not a mount point for this share".to_string());
    }
    Ok(raw.to_string())
}

#[cfg(test)]
mod tests {
    use super::{Console, Machine, Overlay};
    use std::convert::Infallible;
    use std::os::fd::{AsRawFd, RawFd};
    use std::sync::Mutex;

    const CHILD_MARKER: &str = "CONNECTOR_VMM_DESCRIPTOR_CHILD";
    const CHILD_TEST: &str = "launch::tests::descriptor_ownership_child";
    const REPORT_BEGIN: &str = "-- descriptor report --";
    const REPORT_END: &str = "-- end --";

    #[test]
    fn libkrun_call_sequence() {
        let mut table = String::new();

        for (name, machine) in [
            ("base", base()),
            (
                "with a persistent disk",
                Machine {
                    persistent_disk: true,
                    ..base()
                },
            ),
            (
                "--debug",
                Machine {
                    debug: true,
                    console: Console {
                        input: 4,
                        // The tee's pipe, not stdout.
                        output: 6,
                        error: 2,
                    },
                    ..base()
                },
            ),
        ] {
            let recorder = Recording::default();
            let error = super::enter(&recorder, &machine)
                .expect_err("the recording implementation never starts a VM");

            table.push_str(&format!("## {name}\n"));
            table.push_str(&recorder.calls());
            table.push_str(&format!("-> {error}\n\n"));
        }
        insta::assert_snapshot!(table);
    }

    #[test]
    fn the_read_only_mounts_the_launcher_owns() {
        let mut table = String::new();

        for (path, flag) in super::read_only_mounts("/tmp/connector-mounts-0/mount-acme") {
            table.push_str(&format!("{}\n", super::writable_mount(path, flag)));
        }
        for line in crate::framed(&table).lines() {
            assert!(
                !line.starts_with(' '),
                "framed a line beginning with a space: {line:?}"
            );
        }
        insta::assert_snapshot!(table);
    }

    /// Runs the real sweep in a child process and reports on it, because a
    /// sweep inside the test harness would close the harness's own
    /// descriptors.
    #[test]
    fn descriptor_ownership() {
        let output =
            std::process::Command::new(std::env::current_exe().expect("a test binary has a path"))
                .args(["--exact", CHILD_TEST, "--nocapture", "--test-threads=1"])
                .env(CHILD_MARKER, "1")
                .output()
                .expect("spawning the child");

        let stdout = String::from_utf8(output.stdout).expect("the child prints UTF-8");
        let stderr = String::from_utf8(output.stderr).expect("the child prints UTF-8");

        assert!(
            output.status.success(),
            "child failed: {:?}\nstdout:\n{stdout}\nstderr:\n{stderr}",
            output.status,
        );
        assert!(
            stderr.contains("the child still reaches stderr"),
            "stderr was not usable after the sweep: {stderr:?}",
        );

        let report = stdout
            .split_once(REPORT_BEGIN)
            .and_then(|(_, rest)| rest.split_once(REPORT_END))
            .map(|(report, _)| report.trim().to_string())
            .unwrap_or_else(|| panic!("no report in the child's output:\n{stdout}"));

        insta::assert_snapshot!(report);
    }

    #[test]
    fn descriptor_ownership_child() {
        if std::env::var_os(CHILD_MARKER).is_none() {
            return;
        }
        let directory = tempfile::tempdir().expect("a temporary directory");

        // Opened first so it takes the lower number: the sweep must close it
        // even though the descriptor it keeps is opened afterwards.
        let unrelated = open(&directory.path().join("unrelated"), b"unrelated");
        let needed = open(&directory.path().join("needed"), b"needed");

        let scratch = super::sweep(needed.as_raw_fd()).expect("the sweep succeeds");
        let (unrelated, needed) = (unrelated.as_raw_fd(), needed.as_raw_fd());

        println!("{REPORT_BEGIN}");
        println!(
            "the retained descriptor is at fd {scratch}: {}",
            scratch == 3
        );
        println!(
            "it still reads what it held: {}",
            read(scratch) == b"needed",
        );
        println!(
            "the unrelated descriptor is closed: {}",
            unrelated == scratch || is_closed(unrelated),
        );
        println!(
            "the original descriptor number is closed: {}",
            needed == scratch || is_closed(needed),
        );
        println!("{REPORT_END}");

        eprintln!("the child still reaches stderr");

        // libtest would otherwise tear down through descriptors that are gone.
        std::io::Write::flush(&mut std::io::stdout()).expect("flushing stdout");
        std::process::exit(0);
    }

    fn open(path: &std::path::Path, content: &[u8]) -> std::fs::File {
        std::fs::write(path, content).expect("writing the fixture");
        std::fs::File::open(path).expect("opening the fixture")
    }

    fn read(fd: RawFd) -> Vec<u8> {
        let mut buffer = [0u8; 64];
        // SAFETY: `buffer` outlives the call and its length is its own.
        let read = unsafe { libc::pread(fd, buffer.as_mut_ptr().cast(), buffer.len(), 0) };
        assert!(
            read >= 0,
            "reading fd {fd}: {}",
            std::io::Error::last_os_error()
        );
        buffer[..read as usize].to_vec()
    }

    fn is_closed(fd: RawFd) -> bool {
        // SAFETY: a query of one descriptor number, which is safe whether or
        // not it names anything.
        let queried = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        queried < 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::EBADF)
    }

    /// Records what the launch sequence asked of libkrun, without a VM.
    #[derive(Default)]
    struct Recording {
        calls: Mutex<Vec<String>>,
    }

    impl Recording {
        fn record(&self, call: String) -> anyhow::Result<()> {
            self.calls.lock().unwrap().push(call);
            Ok(())
        }

        fn calls(&self) -> String {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .map(|call| format!("{call}\n"))
                .collect()
        }
    }

    impl crate::krun::Krun for Recording {
        fn init_log(&self, target: RawFd, debug: bool) -> anyhow::Result<()> {
            self.record(format!("init_log(target={target}, debug={debug})"))
        }

        fn create_ctx(&self) -> anyhow::Result<u32> {
            self.record("create_ctx()".to_string())?;
            Ok(7)
        }

        fn set_vm_config(&self, ctx: u32, vcpus: u8, memory_mib: u32) -> anyhow::Result<()> {
            self.record(format!(
                "set_vm_config(ctx={ctx}, vcpus={vcpus}, memory_mib={memory_mib})"
            ))
        }

        fn add_virtiofs3(
            &self,
            ctx: u32,
            tag: &str,
            path: &str,
            read_only: bool,
        ) -> anyhow::Result<()> {
            self.record(format!(
                "add_virtiofs3(ctx={ctx}, tag={tag:?}, path={path:?}, read_only={read_only})"
            ))
        }

        fn add_overlay_file(
            &self,
            ctx: u32,
            path: &str,
            data: &'static [u8],
            mode: u32,
        ) -> anyhow::Result<()> {
            self.record(format!(
                "add_overlay_file(ctx={ctx}, path={path:?}, len={}, mode={mode:o})",
                data.len()
            ))
        }

        fn add_scratch_disk(&self, ctx: u32, id: &str, path: &str) -> anyhow::Result<()> {
            self.record(format!(
                "add_scratch_disk(ctx={ctx}, id={id:?}, path={path:?})"
            ))
        }

        fn add_net_tap(&self, ctx: u32, tap: &str, mac: &[u8; 6]) -> anyhow::Result<()> {
            let mac: Vec<String> = mac.iter().map(|byte| format!("{byte:02x}")).collect();
            self.record(format!(
                "add_net_tap(ctx={ctx}, tap={tap:?}, mac={})",
                mac.join(":")
            ))
        }

        fn disable_implicit_vsock(&self, ctx: u32) -> anyhow::Result<()> {
            self.record(format!("disable_implicit_vsock(ctx={ctx})"))
        }

        fn add_vsock(&self, ctx: u32, tsi_features: u32) -> anyhow::Result<()> {
            self.record(format!("add_vsock(ctx={ctx}, tsi_features={tsi_features})"))
        }

        fn add_vsock_port2(
            &self,
            ctx: u32,
            port: u32,
            path: &str,
            listen: bool,
        ) -> anyhow::Result<()> {
            self.record(format!(
                "add_vsock_port2(ctx={ctx}, port={port}, path={path:?}, listen={listen})"
            ))
        }

        fn disable_implicit_console(&self, ctx: u32) -> anyhow::Result<()> {
            self.record(format!("disable_implicit_console(ctx={ctx})"))
        }

        fn add_console(
            &self,
            ctx: u32,
            input: RawFd,
            output: RawFd,
            error: RawFd,
        ) -> anyhow::Result<()> {
            self.record(format!(
                "add_console(ctx={ctx}, input={input}, output={output}, error={error})"
            ))
        }

        fn start_enter(&self, ctx: u32) -> anyhow::Result<Infallible> {
            self.record(format!("start_enter(ctx={ctx})"))?;
            anyhow::bail!("the recording implementation does not start a VM")
        }
    }

    /// Fixed descriptors, so the sequence is the only thing the snapshot can
    /// move on.
    fn base() -> Machine {
        Machine {
            vcpus: 2,
            memory_mib: 1024,
            debug: false,
            connector_mount: "/tmp/connector-mounts-0/mount-acme".to_string(),
            persistent_disk: false,
            overlays: vec![
                Overlay {
                    path: "flow-guest-init",
                    data: b"<the guest init ELF>",
                    mode: 0o100755,
                },
                Overlay {
                    path: ".krun_config.json",
                    data: b"{\"Cmd\":[],\"WorkingDir\":\"/\",\"Env\":[]}",
                    mode: 0o100644,
                },
            ],
            scratch_path: "/proc/self/fd/3".to_string(),
            console: Console {
                input: 4,
                output: 1,
                error: 2,
            },
        }
    }
}
