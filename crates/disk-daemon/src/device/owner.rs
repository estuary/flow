use super::backend;
use super::queue;
use super::wake;
use anyhow::Context;

/// `prctl` option which marks the calling thread an I/O flusher (Linux 5.6+).
/// `libc` exports it only for Android targets, though the option is Linux's.
const PR_SET_IO_FLUSHER: libc::c_int = 57;

/// A disk for an owner to serve. Its device must already have its parameters
/// set, and must not be started until [`spawn`] returns.
pub(super) struct Inputs {
    /// Kernel device ID.
    pub dev_id: u32,
    /// Character device used to exchange request data.
    pub cdev: std::fs::File,
    /// Image and admission state transferred to the owner.
    pub backend: backend::Backend,
    /// Shared with the recording channel so freeing a slot wakes the owner.
    pub waker: wake::Waker,
    /// Requests the device may have outstanding.
    pub queue_depth: u16,
}

/// Serve `inputs` from a thread of its own. This returns once every tag of the
/// queue has a fetch in flight. The caller then starts the device.
///
/// The thread returns once the device has stopped.
pub(super) fn spawn(
    inputs: Inputs,
) -> anyhow::Result<(super::Commands, std::thread::JoinHandle<anyhow::Result<()>>)> {
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
    let commands = super::Commands {
        dev_id,
        sender: commands,
        waker,
    };
    Ok((commands, thread))
}

/// Account writes against the host device's dirty-page limit, so a slow journal
/// cannot consume the host's dirty budget and throttle other owners. Also prevent
/// allocations from waiting on I/O to the disk being served.
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
    queue: queue::Queue,
    backend: backend::Backend,
    requests: Vec<(u16, queue::Request)>,
    replies: Vec<(u16, queue::Reply)>,
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
            queue: queue::Queue::new(dev_id, cdev, waker, queue_depth, backend.blocks())
                .with_context(|| format!("setting up the queue of device {dev_id}"))?,
            backend,
            requests: Vec::new(),
            replies: Vec::new(),
        })
    }

    /// Serve the disk until its device has stopped.
    fn serve(mut self, commands: std::sync::mpsc::Receiver<super::Command>) -> anyhow::Result<()> {
        // Keep serving until the kernel stops the queue, even after every handle
        // is dropped or a stop command fails.
        while !self.queue.stopped() {
            while let Ok(command) = commands.try_recv() {
                self.backend.on_command(command);
            }
            // Retry after a channel wake or a command reopening admission. With no
            // progress, this costs at most one refused reservation.
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

        // Dropping the queue releases the character device and descriptors for
        // deletion; dropping the recorder tells the writer the disk is gone.
        Ok(())
    }
}
