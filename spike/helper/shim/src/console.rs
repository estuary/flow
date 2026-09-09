//! Console wiring.
//!
//! `krun_add_virtio_console_default` gives hvc0 (the kernel console) and the
//! `krun-stdout` port the SAME host descriptor, so the guest workload's stdout
//! and the kernel's console text are interleaved on it by construction. The
//! reactor discards helper stdout today, which is why the workload's stderr
//! gets its own descriptor.

use std::io::{BufRead, Write};
use std::os::fd::{FromRawFd, RawFd};

/// The descriptor libkrun writes the console to. Without `--debug` that is
/// stdout directly; with it, a pipe whose reader copies each line to stdout
/// unchanged and to stderr under a `kernel: ` prefix.
///
/// The prefix therefore also lands on workload stdout, which shares the
/// descriptor. The tee thread dies with the process, so a partial final line
/// can be lost when the VM exits - it is a debugging aid, not a log path.
pub fn output_fd(debug: bool) -> anyhow::Result<RawFd> {
    if !debug {
        return Ok(libc::STDOUT_FILENO);
    }

    let mut fds = [0 as RawFd; 2];
    // Safety: `fds` is a two-element array, which is what pipe() writes.
    if unsafe { libc::pipe(fds.as_mut_ptr()) } < 0 {
        return Err(anyhow::anyhow!(std::io::Error::last_os_error()).context("console tee pipe"));
    }
    let [read_fd, write_fd] = fds;

    std::thread::spawn(move || {
        // Safety: `read_fd` is owned by this thread from here on.
        let reader = std::io::BufReader::new(unsafe { std::fs::File::from_raw_fd(read_fd) });
        for line in reader.split(b'\n') {
            let Ok(line) = line else { return };
            let mut stdout = std::io::stdout().lock();
            let _ = stdout.write_all(&line);
            let _ = stdout.write_all(b"\n");
            let _ = stdout.flush();
            // Never a leading space: the reactor reads a leading space byte on
            // stderr as connector-init's readiness signal.
            eprintln!("kernel: {}", String::from_utf8_lossy(&line));
        }
    });

    Ok(write_fd)
}
