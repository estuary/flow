//! Waking an owner thread which is parked on its ring.

/// A handle which wakes one owner thread.
///
/// The owner keeps a poll of this eventfd armed on its ring, so a wake becomes
/// an ordinary completion. An eventfd counts rather than latches. The owner
/// drains the count when it sees the poll complete, and before it looks for what
/// the wake announced. A wake which lands after that drain leaves the eventfd
/// readable, so the next armed poll completes immediately.
///
/// It converts into a [`std::task::Waker`] which wakes the same eventfd. That is
/// how the recording channel, whose room the owner polls for, wakes an owner parked
/// behind it.
#[derive(Clone)]
pub struct Waker(std::sync::Arc<EventFd>);

struct EventFd(std::fs::File);

impl Waker {
    pub fn new() -> std::io::Result<Self> {
        // Non-blocking, so a drain which found nothing would fail loudly rather
        // than park the owner.
        // SAFETY: eventfd reads no user memory.
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: the descriptor is new and nothing else owns it, so `File` may
        // take responsibility for closing it.
        let file = unsafe { <std::fs::File as std::os::fd::FromRawFd>::from_raw_fd(fd) };

        Ok(Self(std::sync::Arc::new(EventFd(file))))
    }

    pub fn wake(&self) {
        std::task::Wake::wake_by_ref(&self.0)
    }

    /// Reset the count of wakes, once a poll has seen it nonzero. Only the owner
    /// drains, so nothing can have reset it in between.
    pub fn drain(&self) {
        let mut count = [0; 8];
        let mut file: &std::fs::File = &self.0.0;

        std::io::Read::read_exact(&mut file, &mut count)
            .expect("draining a readable eventfd cannot fail");
    }

    pub fn as_raw_fd(&self) -> std::os::fd::RawFd {
        std::os::fd::AsRawFd::as_raw_fd(&self.0.0)
    }
}

impl From<Waker> for std::task::Waker {
    fn from(waker: Waker) -> Self {
        std::task::Waker::from(waker.0)
    }
}

impl std::task::Wake for EventFd {
    fn wake(self: std::sync::Arc<Self>) {
        std::task::Wake::wake_by_ref(&self)
    }

    fn wake_by_ref(self: &std::sync::Arc<Self>) {
        // An eventfd is not seekable, so this is a plain write. Only a counter
        // overflow can make it fail, and the owner drains the counter on every
        // wake it sees.
        let mut file: &std::fs::File = &self.0;
        std::io::Write::write_all(&mut file, &1u64.to_ne_bytes())
            .expect("writing an eventfd cannot fail");
    }
}
