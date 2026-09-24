//! The seam between an accepted device mutation and its durable copy.
//!
//! A mutation is recorded once this channel accepts it. From then on it belongs to
//! the open delta, though it is durable only once the writer has appended it and a
//! prepare has confirmed it. The name follows Gazette's `recoverylog.Recorder`,
//! which likewise records a filesystem's changes to a journal for its playback to
//! rebuild.
//!
//! An owner offers each mutation's chunks here, and applies that mutation to the
//! image once they are accepted. Chunks are therefore queued in the order the image
//! is modified, which makes journal order equal replay order. A mutation is queued
//! whole, so backpressure never splits a device request across two deltas.
//!
//! The channel is bounded, and that bound is the device's backpressure. A
//! mutation which does not fit parks its request until the consumer takes some.
//! Taking a mutation is not the same as appending it. A consumer may hold what it
//! takes, so a disk's journal is created only once something is written.
//!
//! The owner is a thread parked on its ring rather than a task, so it polls for
//! room instead of awaiting it. A reservation the channel refuses leaves the
//! owner's [`std::task::Waker`] with the channel, which wakes it once the consumer
//! frees a slot or goes away.

use crate::proto::Chunk;

/// Offers one disk's mutations to its consumer. Held by that disk's owner.
pub struct Recorder {
    sender: tokio_util::sync::PollSender<Vec<Chunk>>,
    /// Woken when a reservation the channel refused may be tried again.
    waker: std::task::Waker,
}

/// Takes one disk's mutations in the order the owner accepted them. `recv` returns
/// `None` once the owner has dropped its [`Recorder`] and the queue is drained.
pub type Recorded = tokio::sync::mpsc::Receiver<Vec<Chunk>>;

/// Create a channel holding `capacity` mutations, waking `waker` whenever a
/// reservation it refused may be tried again.
pub fn channel(capacity: usize, waker: std::task::Waker) -> (Recorder, Recorded) {
    assert!(
        capacity != 0,
        "a recording channel holds at least one mutation"
    );
    let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
    let sender = tokio_util::sync::PollSender::new(sender);

    (Recorder { sender, waker }, receiver)
}

impl Recorder {
    /// Reserve room for one mutation, which the returned [`Permit`] queues whole,
    /// so backpressure never splits a device request across two deltas.
    ///
    /// Returns `None` if the channel is full. The refusal leaves the waker with the
    /// channel, and the caller reserves again once it fires. A reservation which is
    /// pending holds its place, so the next slot the consumer frees is this one's.
    ///
    /// Room found stays reserved until a permit sends into it, whatever the owner
    /// does in between. A permit dropped unsent leaves it to the next reservation.
    /// Only the owner reserves, so a held slot is never one another sender waits
    /// on.
    pub fn reserve(&mut self) -> Option<Permit<'_>> {
        let mut cx = std::task::Context::from_waker(&self.waker);

        match self.sender.poll_reserve(&mut cx) {
            std::task::Poll::Pending => None,
            std::task::Poll::Ready(Ok(())) => Some(Permit(self)),
            // A released consumer refuses nothing, per `Permit::send`.
            std::task::Poll::Ready(Err(_closed)) => Some(Permit(self)),
        }
    }

    /// Whether the consumer has taken every mutation offered to it.
    ///
    /// Only the owner offers, so an empty channel it observes is one it emptied. A
    /// slot a [`Recorder::reserve`] holds counts as taken. A serving owner never
    /// asks; this is how a case sees that the writer has taken what it offered.
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        let sender = self.sender.get_ref().expect("a recorder is never closed");
        sender.capacity() == sender.max_capacity()
    }
}

/// Room [`Recorder::reserve`] found for one mutation.
pub struct Permit<'a>(&'a mut Recorder);

impl Permit<'_> {
    /// Queue `chunks`, which are one mutation.
    pub fn send(self, chunks: Vec<Chunk>) {
        match self.0.sender.send_item(chunks) {
            Ok(()) => (),
            // A released consumer is a disk which is being torn down. The owner
            // applies the mutation to its image all the same, and nothing will
            // read these chunks again, so they are dropped rather than parking a
            // device which still has to be unmounted.
            Err(_closed) => (),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Recorded, Recorder, channel};
    use crate::chunk::encode_punch;
    use tokio::sync::mpsc::error::TryRecvError;

    fn pair(capacity: usize) -> (Recorder, Recorded) {
        channel(capacity, std::task::Waker::noop().clone())
    }

    #[tokio::test]
    async fn test_reservations_are_refused_at_capacity_and_taken_in_order() {
        let (mut recorder, mut recorded) = pair(2);

        recorder.reserve().unwrap().send(vec![encode_punch(1, 1)]);
        recorder.reserve().unwrap().send(vec![encode_punch(2, 1)]);
        assert!(recorder.reserve().is_none(), "a full channel found room");

        assert_eq!(recorded.recv().await.unwrap(), vec![encode_punch(1, 1)]);
        recorder.reserve().unwrap().send(vec![encode_punch(3, 1)]);

        assert_eq!(recorded.recv().await.unwrap(), vec![encode_punch(2, 1)]);
        assert_eq!(recorded.recv().await.unwrap(), vec![encode_punch(3, 1)]);
        assert_eq!(recorded.try_recv(), Err(TryRecvError::Empty));
    }

    /// A horizon copy may reserve room and then find nothing to copy. The slot it
    /// found is the next reservation's, and is not reserved twice.
    #[test]
    fn test_an_unsent_permit_leaves_its_slot_to_the_next() {
        let (mut recorder, mut recorded) = pair(1);

        assert!(recorder.reserve().is_some(), "an empty channel had no room");
        recorder.reserve().unwrap().send(vec![encode_punch(1, 1)]);
        assert!(recorder.reserve().is_none(), "a full channel found room");

        assert_eq!(recorded.try_recv().unwrap(), vec![encode_punch(1, 1)]);
        assert_eq!(recorded.try_recv(), Err(TryRecvError::Empty));
    }

    #[tokio::test]
    async fn test_dropping_the_owner_half_drains_and_then_ends() {
        let (mut recorder, mut recorded) = pair(4);
        recorder.reserve().unwrap().send(vec![encode_punch(7, 3)]);
        drop(recorder);

        assert_eq!(recorded.recv().await.unwrap(), vec![encode_punch(7, 3)]);
        assert_eq!(recorded.recv().await, None);
    }

    /// The consumer takes one mutation at a time, so an owner which refills the
    /// queue as fast as it drains cannot grow what that consumer holds.
    #[test]
    fn test_one_mutation_is_taken_at_a_time_while_the_owner_refills() {
        const CAPACITY: usize = 2;
        const MUTATIONS: u32 = 16_384;
        let (mut recorder, mut recorded) = pair(CAPACITY);

        let owner = std::thread::spawn(move || {
            for block in 0..MUTATIONS {
                let permit = loop {
                    if let Some(permit) = recorder.reserve() {
                        break permit;
                    }
                    std::thread::yield_now();
                };
                permit.send(vec![crate::chunk::encode_punch(block, 1)]);
            }
        });

        let mut blocks = Vec::new();

        while blocks.len() < MUTATIONS as usize {
            let Ok(chunks) = recorded.try_recv() else {
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
        let (mut recorder, mut recorded) = pair(1);

        let taker = tokio::spawn(async move {
            let first = recorded.recv().await;
            (first, recorded.recv().await)
        });
        tokio::task::yield_now().await;

        recorder.reserve().unwrap().send(vec![encode_punch(9, 2)]);
        drop(recorder);

        assert_eq!(taker.await.unwrap(), (Some(vec![encode_punch(9, 2)]), None));
    }

    /// Whether an owner was woken since it last asked.
    #[derive(Default)]
    struct Woken(std::sync::atomic::AtomicBool);

    impl std::task::Wake for Woken {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl Woken {
        fn take(&self) -> bool {
            self.0.swap(false, std::sync::atomic::Ordering::SeqCst)
        }
    }

    /// An owner admits a parked request only when it is woken. The take which
    /// frees a slot therefore wakes it, and so does a consumer which goes away:
    /// the channel then has room for every mutation, and a stop waits on what is
    /// parked.
    #[test]
    fn test_a_refused_reservation_is_woken_by_a_take_and_by_a_released_consumer() {
        let woken = std::sync::Arc::new(Woken::default());
        let (mut recorder, mut recorded) = channel(1, std::task::Waker::from(woken.clone()));

        recorder.reserve().unwrap().send(vec![encode_punch(1, 1)]);
        assert!(recorder.reserve().is_none(), "a full channel found room");
        assert!(!woken.take(), "a refusal woke the owner");

        recorded.try_recv().unwrap();
        assert!(
            woken.take(),
            "a take which freed a slot left the owner parked"
        );
        recorder.reserve().unwrap().send(vec![encode_punch(2, 1)]);

        assert!(recorder.reserve().is_none(), "a full channel found room");
        drop(recorded);
        assert!(woken.take(), "a released consumer left the owner parked");
        recorder.reserve().unwrap().send(vec![encode_punch(3, 1)]);
    }
}
