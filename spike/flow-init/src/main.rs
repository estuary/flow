//! `flow-init`: the guest init that gives a connector image what podman gives
//! a container today - network, mounts, environment, user - and then becomes
//! the connector. The root arrives writable from podman, so nothing here
//! touches it.
//!
//! Runs as guest root, as a child of libkrun's init, which has already mounted
//! /dev, /proc, /sys, /sys/fs/cgroup, /dev/pts and /dev/shm, brought up `lo`,
//! and applied the image's `Env` and `WorkingDir`. See CONTRACTS.md
//! "flow-init"; the shim builds this argv in `spike/helper/shim/src/main.rs`.

mod cli;
mod net;
mod root;
mod sys;

fn main() -> std::process::ExitCode {
    // `run` is typed to never return successfully: it ends in an `execve` that
    // replaces this process, so control comes back only on the way to a
    // failure exit.
    let Err(error) = run();
    // One line, and never with a leading space: the reactor reads a leading
    // space on stderr as connector-init's readiness byte.
    eprintln!("flow-init: {error}");
    std::process::ExitCode::from(125)
}

fn run() -> Result<std::convert::Infallible, String> {
    timing("start");
    let args = cli::parse(std::env::args().skip(1).collect())?;

    net::configure(args.guest_ip, args.prefix_len, args.gateway)?;
    net::disable_ipv6()?;

    root::write_etc(args.nameserver, args.guest_ip)?;
    root::mount_venv(args.venv_dax)?;
    root::mount_scratch(args.uid, args.gid)?;
    if let (Some(dev), Some(fstype)) = (&args.deps_dev, &args.deps_fstype) {
        root::mount_deps(dev, fstype)?;
    }

    // `--as-root-exec`: a probe that needs guest root - sysctls, drop_caches -
    // run while that privilege is still here. Its exit status is deliberately
    // ignored; the workload runs either way.
    if let Some(command) = &args.as_root_exec {
        std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg(command)
            .status()
            .map_err(|e| format!("running --as-root-exec via /bin/sh: {e}"))?;
    }

    exec_workload(&args)
}

/// Environment, user, and finally the workload itself. The working directory
/// is libkrun's init's doing and nothing since has changed it.
///
/// Never returns: either `execv` replaces this process, or the failure is
/// reported with the code libkrun's init and a shell would use for it, so the
/// reactor cannot tell flow-init's exec apart from the one it replaced.
fn exec_workload(args: &cli::Args) -> Result<std::convert::Infallible, String> {
    // Both point at the scratch disk, which is sized and disposable: a pip or
    // uv install of any size would otherwise land in podman's container layer
    // on host disk, where nothing bounds it. `setenv` overwrites, so these
    // beat the image's own values.
    std::env::set_var("TMPDIR", root::SCRATCH);
    std::env::set_var("UV_CACHE_DIR", root::SCRATCH);

    if !args.run_as_root {
        drop_privileges(args.uid, args.gid)?;
    }

    let program = std::ffi::CString::new(args.argv[0].as_str())
        .map_err(|e| format!("workload argv[0] {:?}: {e}", args.argv[0]))?;
    let arguments: Vec<std::ffi::CString> = args
        .argv
        .iter()
        .map(|argument| {
            std::ffi::CString::new(argument.as_str())
                .map_err(|e| format!("workload argv {argument:?}: {e}"))
        })
        .collect::<Result<_, _>>()?;
    let mut pointers: Vec<*const libc::c_char> =
        arguments.iter().map(|argument| argument.as_ptr()).collect();
    pointers.push(std::ptr::null());

    timing("exec");

    // Safety: a NUL-terminated program path and a NULL-terminated argv, both
    // alive across the call. `execv` passes the current environment.
    unsafe { libc::execv(program.as_ptr(), pointers.as_ptr()) };

    let error = std::io::Error::last_os_error();
    eprintln!("flow-init: exec {:?}: {error}", args.argv[0]);
    // The exit codes libkrun's init uses, which are a shell's: 127 for a
    // workload that is not there, 126 for one that cannot be run.
    std::process::exit(match error.raw_os_error() {
        Some(libc::ENOENT) | Some(libc::ENOTDIR) => 127,
        Some(libc::EACCES) | Some(libc::EPERM) | Some(libc::ENOEXEC) | Some(libc::EISDIR)
        | Some(libc::ELOOP) => 126,
        _ => 125,
    })
}

/// One line per stage on stderr, stamped with the guest's time since boot, so
/// experiment 2 can split the guest's share of the launch. A boot-relative
/// clock rather than a wall clock because it is the only one here that measures
/// from a fixed guest event: the first reading IS the kernel's boot time, since
/// nothing in the guest runs before flow-init. Never a leading space, which is
/// connector-init's readiness signal.
fn timing(stage: &str) {
    eprintln!(
        "flow-init: timing stage={stage} boot_us={}",
        sys::monotonic_micros()
    );
}

/// setgroups before setgid before setuid: after the uid is dropped there is no
/// privilege left to do the other two.
fn drop_privileges(uid: u32, gid: u32) -> Result<(), String> {
    // Safety: none of the three take pointer arguments except setgroups, whose
    // zero-length list is passed as NULL.
    unsafe {
        if libc::setgroups(0, std::ptr::null()) < 0 {
            return Err(sys::last_error("setgroups([])"));
        }
        if libc::setgid(gid) < 0 {
            return Err(sys::last_error(format!("setgid({gid})")));
        }
        if libc::setuid(uid) < 0 {
            return Err(sys::last_error(format!("setuid({uid})")));
        }
    }
    Ok(())
}
