//! The seam between an accepted device mutation and its durable copy.
//!
//! An owner offers each mutation's chunks here before it issues that mutation
//! against the image. Chunks are therefore queued in the order the image is
//! modified, which makes journal order equal replay order. A mutation is queued
//! whole, so backpressure never splits a device request across two deltas.
//!
//! The channel is bounded, and that bound is the device's backpressure. A
//! mutation which does not fit parks its request until the consumer takes some.
//! Taking a mutation is not the same as appending it. A consumer may hold what it
//! takes, so a disk's journal is created only once something is written.

use crate::proto::Chunk;
use crate::wake::Waker;

/// Offers one disk's mutations to its consumer. Held by that disk's owner, which
/// is a plain thread rather than a task, so it only ever offers without awaiting.
pub struct Capture(tokio::sync::mpsc::Sender<Vec<Chunk>>);

/// Takes one disk's mutations in the order the owner accepted them.
pub struct Captured {
    receiver: tokio::sync::mpsc::Receiver<Vec<Chunk>>,
    waker: Waker,
}

/// Create a channel holding `capacity` mutations, waking `waker` whenever a
/// parked owner may retry.
pub fn channel(capacity: usize, waker: Waker) -> (Capture, Captured) {
    assert!(
        capacity != 0,
        "a capture channel holds at least one mutation"
    );
    let (sender, receiver) = tokio::sync::mpsc::channel(capacity);

    (Capture(sender), Captured { receiver, waker })
}

impl Capture {
    /// Queue `chunks`, which are one mutation. They are queued whole, so
    /// backpressure never splits a device request across two deltas.
    ///
    /// Returns them if the channel is full. The caller then parks that request
    /// and retries when its waker fires.
    pub fn offer(&self, chunks: Vec<Chunk>) -> Result<(), Vec<Chunk>> {
        match self.0.try_send(chunks) {
            Ok(()) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Full(chunks)) => Err(chunks),
            // A released consumer is a disk which is being torn down. The owner
            // applies the mutation to its image whatever this returns, and nothing
            // will read these chunks again, so they are dropped rather than parking
            // a device which still has to be unmounted.
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_chunks)) => Ok(()),
        }
    }

    /// Whether the next offer will be accepted.
    ///
    /// Only the owner offers, so room it observes is room it still has. Horizon
    /// copies ask this rather than risk a refusal. A refused copy would have to
    /// be held while mutations of the same blocks flowed past it.
    pub fn has_room(&self) -> bool {
        self.0.capacity() != 0
    }

    /// Whether the consumer has taken every mutation offered to it.
    ///
    /// Only the owner offers, so an empty channel it observes is one it emptied.
    /// A serving owner never asks; this is how a case sees that the writer has
    /// taken what it offered.
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.0.capacity() == self.0.max_capacity()
    }
}

impl Captured {
    /// Take the next mutation, awaiting one if the queue is empty. `None` once
    /// the owner has dropped its [`Capture`] and the queue is drained.
    ///
    /// A dropped future has taken nothing, so a `select!` may race this against
    /// other work.
    pub async fn recv(&mut self) -> Option<Vec<Chunk>> {
        let chunks = self.receiver.recv().await?;
        () = self.wake_a_parked_owner();
        Some(chunks)
    }

    /// Take the next mutation if one is queued.
    pub fn try_recv(&mut self) -> Option<Vec<Chunk>> {
        let chunks = self.receiver.try_recv().ok()?;
        () = self.wake_a_parked_owner();
        Some(chunks)
    }

    /// Wake an owner which may be parked behind a full queue.
    ///
    /// The owner blocks in `submit_and_wait` with a read of the waker's eventfd
    /// armed, and parks a request only when an offer was refused — which happens
    /// only on a full queue. Nothing but this consumer removes a mutation, so the
    /// queue stays full from that refusal until the next take, and this take is
    /// therefore the one which must wake. The queue held `capacity` an instant ago
    /// exactly when it holds `capacity - 1` now, up to further offers the owner has
    /// already slipped in, so waking at `capacity - 1` or more cannot miss it.
    ///
    /// The other direction is free: an eventfd counts rather than latches, so a
    /// wake which lands before the owner parks is still there when it looks, and a
    /// wake with nothing parked behind it only costs one more trip around the ring.
    fn wake_a_parked_owner(&self) {
        if self.receiver.len() + 1 >= self.receiver.max_capacity() {
            self.waker.wake();
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Capture, Captured, channel};
    use crate::chunk::encode_punch;
    use crate::wake::Waker;

    fn pair(capacity: usize) -> (Capture, Captured) {
        channel(capacity, Waker::new().unwrap())
    }

    #[tokio::test]
    async fn test_offers_are_refused_at_capacity_and_taken_in_order() {
        let (capture, mut captured) = pair(2);

        capture.offer(vec![encode_punch(1, 1)]).unwrap();
        capture.offer(vec![encode_punch(2, 1)]).unwrap();

        let refused = capture.offer(vec![encode_punch(3, 1)]).unwrap_err();
        assert_eq!(refused, vec![encode_punch(3, 1)]);

        assert_eq!(captured.recv().await.unwrap(), vec![encode_punch(1, 1)]);
        capture.offer(refused).unwrap();

        assert_eq!(captured.recv().await.unwrap(), vec![encode_punch(2, 1)]);
        assert_eq!(captured.recv().await.unwrap(), vec![encode_punch(3, 1)]);
        assert_eq!(captured.try_recv(), None);
    }

    #[tokio::test]
    async fn test_dropping_the_owner_half_drains_and_then_ends() {
        let (capture, mut captured) = pair(4);
        capture.offer(vec![encode_punch(7, 3)]).unwrap();
        drop(capture);

        assert_eq!(captured.recv().await.unwrap(), vec![encode_punch(7, 3)]);
        assert_eq!(captured.recv().await, None);
    }

    /// The consumer takes one mutation at a time, so an owner which refills the
    /// queue as fast as it drains cannot grow what that consumer holds.
    #[test]
    fn test_one_mutation_is_taken_at_a_time_while_the_owner_refills() {
        const CAPACITY: usize = 2;
        const MUTATIONS: u32 = 16_384;
        let (capture, mut captured) = pair(CAPACITY);

        let owner = std::thread::spawn(move || {
            for block in 0..MUTATIONS {
                let mut chunks = vec![crate::chunk::encode_punch(block, 1)];

                loop {
                    match capture.offer(chunks) {
                        Ok(()) => break,
                        Err(refused) => chunks = refused,
                    }
                    std::thread::yield_now();
                }
            }
        });

        let mut blocks = Vec::new();

        while blocks.len() < MUTATIONS as usize {
            let Some(chunks) = captured.try_recv() else {
                std::thread::yield_now();
                continue;
            };
            blocks.extend(chunks.into_iter().map(|chunk| chunk.block));
        }
        owner.join().unwrap();

        assert_eq!(blocks, (0..MUTATIONS).collect::<Vec<_>>());
    }

    /// A consumer which is waiting on an empty channel wakes on the next offer,
    /// and again when the owner closes the channel behind it.
    #[tokio::test]
    async fn test_an_awaiting_receiver_wakes_on_the_next_offer() {
        let (capture, mut captured) = pair(1);

        let taker = tokio::spawn(async move {
            let first = captured.recv().await;
            (first, captured.recv().await)
        });
        tokio::task::yield_now().await;

        capture.offer(vec![encode_punch(9, 2)]).unwrap();
        drop(capture);

        assert_eq!(taker.await.unwrap(), (Some(vec![encode_punch(9, 2)]), None));
    }
}
