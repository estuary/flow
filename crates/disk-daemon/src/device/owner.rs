//! The owner thread of a promoted disk, the only one which touches its image and
//! may drive its ublk queue.
//!
//! ublk binds a queue to the task which issued its fetches and refuses commits
//! from any other, so the ring is built and served here rather than by the
//! caller. `START_DEV` blocks until every tag has a fetch in flight, so a
//! thread which failed before arming would hang it. Promotion instead waits for
//! the owner to arm, or to say why it could not.
//!
//! An image failure ends the owner. Its exit drops the sender of served
//! mutations, which ends the writer's stream, and closes the queue, so the kernel
//! fails every request it holds or is later sent.

use super::chunk;
use super::queue;
use super::target;
use super::ublk;
use anyhow::Context;

/// Serve device `dev_id` through its character device `cdev`, from the rebuilt
/// `target`, sending its served mutations to `served_tx`.
pub(super) fn spawn(
    dev_id: u32,
    cdev: std::fs::File,
    target: target::Target,
    served_tx: tokio::sync::mpsc::Sender<super::Mutation>,
) -> anyhow::Result<std::thread::JoinHandle<anyhow::Result<()>>> {
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();

    let thread = std::thread::Builder::new()
        .name("disk-owner".to_string())
        .spawn(move || run(dev_id, cdev, target, served_tx, ready_tx))
        .with_context(|| format!("spawning the owner of device {dev_id}"))?;

    if let Err(std::sync::mpsc::RecvError) = ready_rx.recv() {
        return Err(match thread.join() {
            Ok(Err(err)) => err,
            Ok(Ok(())) => panic!("the owner of device {dev_id} returned without serving it"),
            Err(_panic) => anyhow::anyhow!("the owner of device {dev_id} panicked"),
        });
    }
    Ok(thread)
}

/// The owner thread: arm the queue, then serve requests from `target` until the
/// kernel stops the device, or the image fails.
fn run(
    dev_id: u32,
    cdev: std::fs::File,
    mut target: target::Target,
    served_tx: tokio::sync::mpsc::Sender<super::Mutation>,
    ready_tx: std::sync::mpsc::Sender<()>,
) -> anyhow::Result<()> {
    // Names the device in every event the owner logs.
    let _span = tracing::info_span!("device", dev_id).entered();

    // Mark this thread part of the disk's I/O path, so its allocations never
    // wait on I/O and its writes are throttled only by the image's host device.
    // Otherwise it could block on writeback of dirty pages which only it can
    // clean. 57 is `PR_SET_IO_FLUSHER` (Linux 5.6+), which `libc` exports only
    // for Android targets, though the option is Linux's.
    //
    // SAFETY: this `prctl` option reads no user memory.
    if unsafe { libc::prctl(57, 1, 0, 0, 0) } != 0 {
        return Err(std::io::Error::last_os_error())
            .context("marking a disk's owner an I/O flusher, which requires CAP_SYS_RESOURCE");
    }
    let queue = queue::Queue::new(dev_id, cdev, ublk::QUEUE_DEPTH)
        .with_context(|| format!("setting up the queue of device {dev_id}"))?;
    _ = ready_tx.send(());

    queue.serve(|request| {
        let chunk = match request {
            queue::Request::Read { start, buf } => return target.read(start, buf),
            queue::Request::Write { start, data } => chunk::encode_write(start, &data),
            queue::Request::Punch(range) => chunk::encode_punch(range.start, range.len() as u32),
        };

        // Sent before returning, so a commit's syncfs waits for its writes to
        // reach the journal writer.
        for mutation in target.serve(chunk)? {
            // Fails only once the writer is gone, as at teardown.
            _ = served_tx.blocking_send(mutation);
        }
        Ok(())
    })
}
