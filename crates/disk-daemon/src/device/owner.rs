//! The owner thread, the only one which may drive a disk's ublk queue.
//!
//! ublk binds a queue to the task which issued its fetches and refuses commits
//! from any other, so the ring is built and served here rather than by the
//! caller. `START_DEV` blocks until every tag has a fetch in flight, so a
//! thread which failed before arming would hang it. [`spawn`] instead waits for
//! the thread to arm, or to exit with why it could not.

use super::queue;
use super::target;
use super::wake;
use anyhow::Context;

/// Serve the device. This returns once every tag of the queue has a fetch in
/// flight.
pub(super) fn spawn(
    dev_id: u32,
    cdev: std::fs::File,
    waker: wake::Waker,
    queue_depth: u16,
    target: target::Target,
    commands: std::sync::mpsc::Receiver<super::Command>,
) -> anyhow::Result<std::thread::JoinHandle<anyhow::Result<()>>> {
    let (armed, is_armed) = std::sync::mpsc::channel();

    let thread = std::thread::Builder::new()
        .name(format!("disk-{dev_id}"))
        .spawn(move || {
            () = mark_io_flusher().with_context(|| {
                format!(
                    "marking the owner of device {dev_id} an I/O flusher, which \
                     requires CAP_SYS_RESOURCE"
                )
            })?;
            let queue = queue::Queue::new(dev_id, cdev, waker, queue_depth, target.blocks())
                .with_context(|| format!("setting up the queue of device {dev_id}"))?;
            _ = armed.send(());

            serve(queue, target, commands).with_context(|| format!("serving device {dev_id}"))
        })
        .with_context(|| format!("spawning the owner of device {dev_id}"))?;

    // A thread which fails before arming drops `armed` unsent, and returns why.
    if let Err(std::sync::mpsc::RecvError) = is_armed.recv() {
        return Err(match thread.join() {
            Ok(Err(err)) => err,
            Ok(Ok(())) => panic!("the owner of device {dev_id} returned without serving it"),
            Err(_panic) => anyhow::anyhow!("the owner of device {dev_id} panicked"),
        });
    }
    Ok(thread)
}

/// Mark this thread part of the disk's I/O path, so its allocations never wait on
/// I/O and its writes are throttled only by the image's host device. Otherwise it
/// could block on writeback of dirty pages which only it can clean.
fn mark_io_flusher() -> std::io::Result<()> {
    // 57 is `PR_SET_IO_FLUSHER` (Linux 5.6+), which `libc` exports only for
    // Android targets, though the option is Linux's.
    // SAFETY: this `prctl` option reads no user memory.
    if unsafe { libc::prctl(57, 1, 0, 0, 0) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

fn serve(
    mut queue: queue::Queue,
    mut target: target::Target,
    commands: std::sync::mpsc::Receiver<super::Command>,
) -> std::io::Result<()> {
    let mut requests = Vec::new();
    let mut replies = Vec::new();

    // Keep serving until the kernel stops the queue, even after every handle
    // is dropped or a stop command fails.
    while !queue.stopped() {
        for command in commands.try_iter() {
            match command {
                super::Command::CloseAdmission(closed) => {
                    _ = closed.send(target.close_admission());
                }
                super::Command::ResumeAdmission(range) => target.resume_admission(range),
            }
        }
        // Admit the mutations the last wait handed over, and retry parked ones
        // after a channel wake or a command reopening admission.
        target.admit(&mut replies);
        target.compact();

        // `complete` only queues each reply, and `wait` submits them. So reads
        // answered after the last wait go out together with the mutations
        // admitted above.
        for (tag, reply) in replies.drain(..) {
            queue.complete(tag, reply);
        }
        () = queue.wait(&mut requests)?;

        for (tag, request) in requests.drain(..) {
            target.on_request(tag, request, &mut replies);
        }
    }
    Ok(())
}
