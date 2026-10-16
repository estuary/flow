use super::queue;
use super::target;
use super::wake;
use anyhow::Context;

/// Serve `inputs` from a thread of its own. This returns once every tag of the
/// queue has a fetch in flight. The caller then starts the device.
pub(super) fn spawn(
    inputs: Inputs,
) -> anyhow::Result<(
    std::sync::mpsc::Sender<super::Command>,
    std::thread::JoinHandle<anyhow::Result<()>>,
)> {
    let dev_id = inputs.dev_id;
    let (commands, received) = std::sync::mpsc::channel();
    let (armed, is_armed) = std::sync::mpsc::channel();

    let thread = std::thread::Builder::new()
        .name(format!("disk-{dev_id}"))
        .spawn(move || {
            // Mark this thread part of the disk's I/O path, so its allocations never
            // wait on I/O and its writes are throttled only by the image's host
            // device. Otherwise it could block on writeback of dirty pages which only
            // it can clean.
            //
            // 57 is `PR_SET_IO_FLUSHER` (Linux 5.6+), which `libc` exports only for
            // Android targets, though the option is Linux's.
            // SAFETY: this `prctl` option reads no user memory.
            if unsafe { libc::prctl(57, 1, 0, 0, 0) } != 0 {
                return Err(std::io::Error::last_os_error()).with_context(|| {
                    format!(
                        "marking the owner of device {dev_id} an I/O flusher, which \
                         requires CAP_SYS_RESOURCE"
                    )
                });
            }
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
    Ok((commands, thread))
}

/// What [`spawn`] hands an owner thread to serve one disk's device.
pub(super) struct Inputs {
    /// Kernel device ID.
    pub dev_id: u32,
    /// Character device used to exchange request data.
    pub cdev: std::fs::File,
    /// Image and admission state transferred to the owner.
    pub target: target::Target,
    /// Shared with the recording channel so freeing a slot wakes the owner.
    pub waker: wake::Waker,
    /// Requests the device may have outstanding.
    pub queue_depth: u16,
}

/// What one disk's owner thread holds.
struct Owner {
    dev_id: u32,
    queue: queue::Queue,
    target: target::Target,
    requests: Vec<(u16, queue::Request)>,
    replies: Vec<(u16, queue::Reply)>,
}

impl Owner {
    fn new(inputs: Inputs) -> anyhow::Result<Self> {
        let Inputs {
            dev_id,
            cdev,
            target,
            waker,
            queue_depth,
        } = inputs;

        Ok(Self {
            dev_id,
            queue: queue::Queue::new(dev_id, cdev, waker, queue_depth, target.blocks())
                .with_context(|| format!("setting up the queue of device {dev_id}"))?,
            target,
            requests: Vec::new(),
            replies: Vec::new(),
        })
    }

    fn serve(mut self, commands: std::sync::mpsc::Receiver<super::Command>) -> anyhow::Result<()> {
        // Keep serving until the kernel stops the queue, even after every handle
        // is dropped or a stop command fails.
        while !self.queue.stopped() {
            for command in commands.try_iter() {
                match command {
                    super::Command::CloseAdmission(closed) => {
                        _ = closed.send(self.target.close_admission());
                    }
                    super::Command::ResumeAdmission(range) => self.target.resume_admission(range),
                }
            }
            // Admit the mutations the last wait handed over, and retry parked ones after
            // a channel wake or a command reopening admission. With no progress, this
            // costs at most one refused reservation.
            self.target.admit(&mut self.replies);
            self.target.compact();

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
                self.target.on_request(tag, request, &mut self.replies);
            }
        }
        Ok(())
    }
}
