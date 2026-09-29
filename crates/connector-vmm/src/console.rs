//! Console wiring.
//!
//! `krun_add_virtio_console_default` gives hvc0 - the kernel console - and the
//! `krun-stdout` port the SAME host descriptor, so the guest workload's stdout
//! and the kernel's console text are interleaved on it by construction. That
//! is why the workload's stderr gets a descriptor of its own: it is the stream
//! the runtime reads, and it has to stay byte for byte.

use std::io::Write;
use std::os::fd::{FromRawFd, IntoRawFd, RawFd};

/// Console input is /dev/null. Transfer ownership with `into_raw_fd` because
/// libkrun holds the descriptor for the VM's lifetime.
pub fn input() -> anyhow::Result<RawFd> {
    let devnull = std::fs::File::open("/dev/null")
        .map_err(|e| anyhow::anyhow!("opening /dev/null for the console: {e}"))?;

    Ok(devnull.into_raw_fd())
}

/// The descriptor libkrun writes the console to. Without `--debug` that is
/// stdout directly; with it, a pipe whose reader copies each line to stdout
/// unchanged and to stderr behind the VMM's own prefix.
///
/// The prefix therefore also lands on workload stdout, which shares the
/// descriptor. The tee thread dies with the process, so a partial final line
/// can be lost when the VM exits: it is a debugging aid, not a log path.
///
/// Started after the descriptor sweep, because a thread shares the descriptor
/// table with it.
pub fn output(debug: bool) -> anyhow::Result<RawFd> {
    if !debug {
        return Ok(libc::STDOUT_FILENO);
    }

    let mut fds = [0 as RawFd; 2];
    // SAFETY: `fds` is the two-element array pipe() writes.
    if unsafe { libc::pipe(fds.as_mut_ptr()) } < 0 {
        return Err(anyhow::anyhow!(std::io::Error::last_os_error()).context("console tee pipe"));
    }
    let [read_fd, write_fd] = fds;

    std::thread::Builder::new()
        .name("console-tee".to_string())
        .spawn(move || tee(read_fd))
        .map_err(|e| anyhow::anyhow!("spawning the console tee: {e}"))?;

    Ok(write_fd)
}

fn tee(read_fd: RawFd) {
    use std::io::BufRead;

    // SAFETY: `read_fd` is owned by this thread from here on.
    let reader = std::io::BufReader::new(unsafe { std::fs::File::from_raw_fd(read_fd) });

    for line in reader.split(b'\n') {
        let Ok(line) = line else { return };

        let mut stdout = std::io::stdout().lock();
        let _ = stdout.write_all(&line);
        let _ = stdout.write_all(b"\n");
        let _ = stdout.flush();

        eprint!(
            "{}",
            crate::framed(&format!("kernel: {}", String::from_utf8_lossy(&line)))
        );
    }
}

#[cfg(test)]
mod tests {
    use std::os::fd::RawFd;

    #[test]
    fn the_console_input_stays_open() {
        let fd = super::input().expect("/dev/null opens");

        assert!(is_open(fd), "the console input was closed on return");

        let mut byte = [0u8; 1];
        // SAFETY: `byte` outlives the call and its length is its own.
        let read = unsafe { libc::read(fd, byte.as_mut_ptr().cast(), byte.len()) };
        assert_eq!(read, 0, "/dev/null reads as end of file");

        // SAFETY: this test owns the descriptor it just created.
        unsafe { libc::close(fd) };
    }

    fn is_open(fd: RawFd) -> bool {
        // SAFETY: a query of one descriptor number, safe whether or not it
        // names anything.
        let queried = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        queried >= 0
    }
}
