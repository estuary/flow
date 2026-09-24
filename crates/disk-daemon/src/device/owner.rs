//! The thread which serves one disk. It waits on its queue's ring, takes the
//! commands its [`Device`] and [`Compactor`] send it, and passes each request the
//! queue hands over to the disk's [`Backend`], and each reply back. The queue moves
//! the data and the backend decides; the owner does neither. Every step between the
//! fetch and the commit is a blocking call on the owner's thread.
//!
//! The thread is an I/O flusher, per `prctl(PR_SET_IO_FLUSHER)`, because it is the
//! one thread which cleans its disk's dirty pages, and it writes the host's page
//! cache to do it. Dirty-page throttling therefore judges its writes against the
//! host device it writes to, rather than against the host-wide dirty limit. Without
//! that, one disk whose writeback has stalled, as behind a slow journal, fills the
//! host's dirty budget with pages only its owner can clean, and every other owner is
//! throttled for them. Its memory allocations also never wait on I/O, which may be
//! I/O to the very disk it serves.
//!
//! [`Device`]: super::Device
//! [`Compactor`]: super::Compactor
//! [`Backend`]: super::backend::Backend

use super::backend::Backend;
use super::queue::{Queue, Reply, Request};
use super::{Command, Commands};
use crate::wake::Waker;
use anyhow::Context;

/// `prctl` option which marks the calling thread an I/O flusher (Linux 5.6+).
/// `libc` exports it only for Android targets, though the option is Linux's.
const PR_SET_IO_FLUSHER: libc::c_int = 57;

/// A disk for an owner to serve. Its device must already have its parameters
/// set, and must not be started until [`spawn`] returns.
pub(super) struct Inputs {
    pub dev_id: u32,
    pub cdev: std::fs::File,
    pub backend: Backend,
    /// Interrupts this owner's wait on its ring. The caller supplies it, because
    /// the recording channel behind `backend` is built around it. Taking a mutation is
    /// one of the two events which must wake an owner.
    pub waker: Waker,
    /// Requests the device may have outstanding.
    pub queue_depth: u16,
}

/// Serve `inputs` from a thread of its own. This returns once every tag of the
/// queue has a fetch in flight. The caller then starts the device.
///
/// The thread returns once the device has stopped.
pub(super) fn spawn(
    inputs: Inputs,
) -> anyhow::Result<(Commands, std::thread::JoinHandle<anyhow::Result<()>>)> {
    let (dev_id, waker) = (inputs.dev_id, inputs.waker.clone());
    let (commands, received) = std::sync::mpsc::channel();
    let (armed, is_armed) = std::sync::mpsc::channel();

    let thread = std::thread::Builder::new()
        .name(format!("disk-{dev_id}"))
        .spawn(move || {
            () = io_flusher(dev_id)?;
            let owner = Owner::new(inputs)?;
            _ = armed.send(());
            owner.serve(received)
        })
        .with_context(|| format!("spawning the owner of device {dev_id}"))?;

    // A thread which fails before its queue is fetching drops `armed` unsent, and
    // returns why.
    if let Err(std::sync::mpsc::RecvError) = is_armed.recv() {
        return Err(match thread.join() {
            Ok(Err(err)) => err,
            Ok(Ok(())) => panic!("the owner of device {dev_id} returned without serving it"),
            Err(_panic) => anyhow::anyhow!("the owner of device {dev_id} panicked"),
        });
    }
    let commands = Commands {
        dev_id,
        sender: commands,
        waker,
    };
    Ok((commands, thread))
}

/// Mark the calling thread, which is disk `dev_id`'s owner, an I/O flusher.
fn io_flusher(dev_id: u32) -> anyhow::Result<()> {
    // SAFETY: this `prctl` option reads no user memory.
    let rc = unsafe { libc::prctl(PR_SET_IO_FLUSHER, 1, 0, 0, 0) };

    if rc != 0 {
        return Err(std::io::Error::last_os_error()).with_context(|| {
            format!(
                "marking the owner of device {dev_id} an I/O flusher, which requires \
                 CAP_SYS_RESOURCE"
            )
        });
    }
    Ok(())
}

/// What one disk's owner thread holds. Only that thread touches any of it.
struct Owner {
    dev_id: u32,
    queue: Queue,
    backend: Backend,
    /// Requests the queue handed over, and the replies the backend returned for the
    /// queue to complete. Both are kept across passes, so neither costs an
    /// allocation.
    requests: Vec<(u16, Request)>,
    replies: Vec<(u16, Reply)>,
}

impl Owner {
    fn new(inputs: Inputs) -> anyhow::Result<Self> {
        let Inputs {
            dev_id,
            cdev,
            backend,
            waker,
            queue_depth,
        } = inputs;

        Ok(Self {
            dev_id,
            queue: Queue::new(dev_id, cdev, waker, queue_depth, backend.blocks())
                .with_context(|| format!("setting up the queue of device {dev_id}"))?,
            backend,
            requests: Vec::new(),
            replies: Vec::new(),
        })
    }

    /// Serve the disk until its device has stopped.
    fn serve(mut self, commands: std::sync::mpsc::Receiver<Command>) -> anyhow::Result<()> {
        // Stopping the device ends an owner, and nothing else does. Every handle may
        // be gone while the device is still stopping, or while one which failed to
        // stop still serves.
        while !self.queue.stopped() {
            while let Ok(command) = commands.try_recv() {
                self.backend.on_command(command);
            }
            // Room may have freed since the last pass, which is what a wake announces,
            // or a command may have reopened admission. A pass with neither costs at
            // most one refused reservation.
            self.backend.admit(&mut self.replies);
            self.backend.compact();

            // Replies travel to the kernel with the next wait's submission, so those
            // of the last pass's requests go with these.
            for (tag, reply) in self.replies.drain(..) {
                self.queue.complete(tag, reply);
            }
            if let Err(err) = self.queue.wait(&mut self.requests) {
                let dev_id = self.dev_id;
                return Err(anyhow::Error::new(err).context(format!("serving device {dev_id}")));
            }
            for (tag, request) in self.requests.drain(..) {
                self.backend.on_request(tag, request, &mut self.replies);
            }
        }
        assert_eq!(
            self.backend.parked(),
            0,
            "device {} stopped with requests parked",
            self.dev_id,
        );

        // Returning drops the character device and unmaps its descriptors, so the
        // kernel may delete the device. Dropping the recording channel with them
        // tells the journal writer the disk is gone.
        Ok(())
    }
}
