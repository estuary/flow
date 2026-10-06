//! Configure network, mounts, environment and user, then exec the connector.
//! Runs as guest root after libkrun's init (see the crate README).

mod cli;
mod net;
mod root;
mod sys;

fn main() -> std::process::ExitCode {
    let args = match <cli::Args as clap::Parser>::try_parse() {
        Ok(args) => args,
        // `--help` and `--version` are clap's other "errors", and go to stdout.
        Err(error) if !error.use_stderr() => {
            let _ = error.print();
            return std::process::ExitCode::from(0);
        }
        Err(error) => {
            report(&error.render().to_string());
            return std::process::ExitCode::from(125);
        }
    };

    let Err(error) = run(&args);
    report(&error);
    std::process::ExitCode::from(125)
}

fn run(args: &cli::Args) -> Result<std::convert::Infallible, String> {
    net::configure(
        args.guest_ip.address,
        args.guest_ip.prefix_len,
        args.gateway,
    )?;
    net::disable_ipv6()?;

    root::write_etc(args.nameserver, args.guest_ip.address)?;
    root::mount_scratch(args.uid, args.gid)?;
    root::mount_connector_mount(&args.connector_mount)?;

    if let Some(guest_path) = &args.persistent_disk {
        root::mount_persistent_disk(guest_path)?;
    }

    // Ignore the probe's exit status so a failed probe does not stop the workload.
    if let Some(command) = &args.as_root_exec {
        std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg(command)
            .status()
            .map_err(|e| format!("running --as-root-exec via /bin/sh: {e}"))?;
    }

    exec_workload(args)
}

fn exec_workload(args: &cli::Args) -> Result<std::convert::Infallible, String> {
    // Keep temporary files and package caches on the bounded scratch disk.
    //
    // Safety: nothing here has spawned a thread, so no other thread can be in
    // `getenv` while these are set.
    unsafe {
        std::env::set_var("TMPDIR", root::SCRATCH);
        std::env::set_var("UV_CACHE_DIR", root::UV_CACHE);
    }

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

    // Safety: a NUL-terminated program path and a NULL-terminated argv, both
    // alive across the call. `execv` passes the current environment.
    unsafe { libc::execv(program.as_ptr(), pointers.as_ptr()) };

    let error = std::io::Error::last_os_error();
    report(&format!("exec {:?}: {error}", args.argv[0]));
    std::process::exit(i32::from(exec_exit_code(error.raw_os_error())));
}

/// The exit codes libkrun's init uses for a failed exec, which are a shell's:
/// 127 for a workload that is not there, 126 for one that cannot be run, and
/// this binary's own 125 for anything else.
fn exec_exit_code(errno: Option<i32>) -> u8 {
    match errno {
        Some(libc::ENOENT) | Some(libc::ENOTDIR) => 127,
        Some(libc::EACCES) | Some(libc::EPERM) | Some(libc::ENOEXEC) | Some(libc::EISDIR)
        | Some(libc::ELOOP) => 126,
        _ => 125,
    }
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

fn report(message: &str) {
    eprint!("{}", framed(message));
}

/// Prefix every line so clap's indentation cannot trigger the launcher's
/// readiness signal (a leading space on stderr).
fn framed(message: &str) -> String {
    let mut framed = String::with_capacity(message.len());

    for line in message.trim_end().lines() {
        framed.push_str("flow-guest-init:");
        if !line.is_empty() {
            framed.push(' ');
            framed.push_str(line);
        }
        framed.push('\n');
    }
    framed
}

#[cfg(test)]
mod tests {
    #[test]
    fn exec_exit_codes() {
        let mut table = String::new();

        for (name, errno) in [
            ("ENOENT", libc::ENOENT),
            ("ENOTDIR", libc::ENOTDIR),
            ("EACCES", libc::EACCES),
            ("EPERM", libc::EPERM),
            ("ENOEXEC", libc::ENOEXEC),
            ("EISDIR", libc::EISDIR),
            ("ELOOP", libc::ELOOP),
            ("ENOMEM", libc::ENOMEM),
            ("E2BIG", libc::E2BIG),
        ] {
            table.push_str(&format!(
                "{name} => {}\n",
                super::exec_exit_code(Some(errno))
            ));
        }
        table.push_str(&format!("(none) => {}\n", super::exec_exit_code(None)));

        insta::assert_snapshot!(table);
    }
}
