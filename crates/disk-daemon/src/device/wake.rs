/// Wakes a device's owner thread out of its io_uring wait.
///
/// A wake increments an eventfd which the owner keeps polled on its ring, so
/// the wake arrives as an ordinary completion. The owner drains the eventfd
/// before it looks for new work, so a wake that lands after the drain leaves
/// the eventfd readable and completes the next poll, rather than being lost.
///
/// Converts into a [`std::task::Waker`], which the recording channel uses to
/// wake the owner when a full channel frees room.
#[derive(Clone)]
pub(super) struct Waker(std::sync::Arc<EventFd>);

impl Waker {
    pub fn new() -> std::io::Result<Self> {
        // Non-blocking, so a drain which finds nothing panics instead of hanging the owner.
        // SAFETY: eventfd reads no user memory.
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: `fd` is new, and nothing else owns it.
        let file = unsafe { <std::fs::File as std::os::fd::FromRawFd>::from_raw_fd(fd) };

        Ok(Self(std::sync::Arc::new(EventFd(file))))
    }

    pub fn wake(&self) {
        std::task::Wake::wake_by_ref(&self.0)
    }

    /// Reset the eventfd after the owner's poll of it completes. Only the owner
    /// drains, so it is still readable.
    pub fn drain(&self) {
        std::io::Read::read_exact(&mut &self.0.0, &mut [0; 8])
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

struct EventFd(std::fs::File);

impl std::task::Wake for EventFd {
    fn wake(self: std::sync::Arc<Self>) {
        std::task::Wake::wake_by_ref(&self)
    }

    fn wake_by_ref(self: &std::sync::Arc<Self>) {
        // Fails only if the 64-bit count overflows, and the owner drains it on every wake.
        std::io::Write::write_all(&mut &self.0, &1u64.to_ne_bytes())
            .expect("writing an eventfd cannot fail");
    }
}
