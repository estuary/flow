//! Waking an owner thread which is parked on its ring.

/// A handle which wakes one owner thread.
///
/// The owner keeps a read of this eventfd armed on its ring, so a wake becomes
/// an ordinary completion. An eventfd counts rather than latches. A wake which
/// lands between completions is therefore not lost, and the next armed read
/// returns immediately.
#[derive(Clone)]
pub struct Waker(std::sync::Arc<std::fs::File>);

impl Waker {
    pub fn new() -> std::io::Result<Self> {
        // SAFETY: eventfd reads no user memory.
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: the descriptor is new and nothing else owns it, so `File` may
        // take responsibility for closing it.
        let file = unsafe { <std::fs::File as std::os::fd::FromRawFd>::from_raw_fd(fd) };

        Ok(Self(std::sync::Arc::new(file)))
    }

    pub fn wake(&self) {
        // An eventfd is not seekable, so this is a plain write. Only a counter
        // overflow can make it fail, and the owner drains the counter on every
        // read.
        let mut file: &std::fs::File = &self.0;
        std::io::Write::write_all(&mut file, &1u64.to_ne_bytes())
            .expect("writing an eventfd cannot fail");
    }

    pub fn as_raw_fd(&self) -> std::os::fd::RawFd {
        std::os::fd::AsRawFd::as_raw_fd(&*self.0)
    }
}
