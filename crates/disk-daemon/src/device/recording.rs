use crate::proto;

/// Takes one disk's mutations in the order the owner accepted them. `recv` returns
/// `None` once the owner has dropped its [`Recorder`] and the queue is drained.
pub type Recorded = tokio::sync::mpsc::Receiver<Vec<proto::Chunk>>;

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

/// Offers one disk's mutations to its consumer. Held by that disk's owner.
pub struct Recorder {
    /// Bounded channel carrying whole mutations to the writer.
    sender: tokio_util::sync::PollSender<Vec<proto::Chunk>>,
    /// Woken when a reservation the channel refused may be tried again.
    waker: std::task::Waker,
}

impl Recorder {
    /// Reserve room for one whole mutation, or return `None` and register the
    /// waker. A pending reservation keeps its place for the next free slot.
    ///
    /// Dropping a permit unsent leaves its slot reserved for the next call.
    /// Only the owner reserves, so no other sender waits on that slot.
    pub fn reserve(&mut self) -> Option<Permit<'_>> {
        let mut cx = std::task::Context::from_waker(&self.waker);

        match self.sender.poll_reserve(&mut cx) {
            std::task::Poll::Pending => None,
            std::task::Poll::Ready(Ok(())) => Some(Permit(self)),
            // A released consumer refuses nothing, per `Permit::send`.
            std::task::Poll::Ready(Err(_closed)) => Some(Permit(self)),
        }
    }

    /// Whether the consumer has taken every queued mutation. A reserved slot
    /// counts as occupied.
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
    pub fn send(self, chunks: Vec<proto::Chunk>) {
        match self.0.sender.send_item(chunks) {
            Ok(()) => (),
            // Teardown must still admit unmount writes after the consumer drops;
            // these mutations will never commit.
            Err(_closed) => (),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Recorded, Recorder, channel};
    use crate::chunk;
    use crate::proto;
    use std::fmt::Write as _;
    use tokio::sync::mpsc;

    fn pair(capacity: usize) -> (Recorder, Recorded) {
        channel(capacity, std::task::Waker::noop().clone())
    }

    /// Records wakeups since the last `take`.
    #[derive(Default)]
    struct Woken(std::sync::atomic::AtomicBool);

    impl Woken {
        fn take(&self) -> bool {
            self.0.swap(false, std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl std::task::Wake for Woken {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum Step {
        /// Send a punch: (starting block, block count).
        Punch(u32, u32),
        /// Reserve room and drop the permit unsent.
        Reserve,
        Recv,
        TryRecv,
        DropRecorder,
        DropRecorded,
    }
    use Step::*;

    fn received(mutation: Option<Vec<proto::Chunk>>) -> String {
        let Some(chunks) = mutation else {
            return "ended".to_string();
        };
        let chunks: Vec<String> = chunks
            .iter()
            .map(|chunk| match &chunk.content {
                Some(proto::chunk::Content::Punch(_)) => {
                    format!("punch {:?}", chunk::covered_blocks(chunk))
                }
                _ => format!("{chunk:?}"),
            })
            .collect();
        format!("took [{}]", chunks.join(", "))
    }

    /// Trace channel operations and whether each wakes the owner.
    async fn run(capacity: usize, steps: &[Step]) -> String {
        let woken = std::sync::Arc::new(Woken::default());
        let (recorder, recorded) = channel(capacity, std::task::Waker::from(woken.clone()));
        let (mut recorder, mut recorded) = (Some(recorder), Some(recorded));
        let mut out = String::new();

        for &step in steps {
            let outcome = match step {
                Punch(block, blocks) => match recorder.as_mut().unwrap().reserve() {
                    Some(permit) => {
                        () = permit.send(vec![chunk::encode_punch(block, blocks)]);
                        "sent".to_string()
                    }
                    None => "refused".to_string(),
                },
                Reserve => match recorder.as_mut().unwrap().reserve() {
                    Some(_unsent) => "room".to_string(),
                    None => "refused".to_string(),
                },
                Recv => received(recorded.as_mut().unwrap().recv().await),
                TryRecv => match recorded.as_mut().unwrap().try_recv() {
                    Ok(chunks) => received(Some(chunks)),
                    Err(mpsc::error::TryRecvError::Empty) => "empty".to_string(),
                    Err(err) => format!("{err:?}"),
                },
                DropRecorder => {
                    recorder = None;
                    "dropped".to_string()
                }
                DropRecorded => {
                    recorded = None;
                    "dropped".to_string()
                }
            };
            let wake = if woken.take() { "woken" } else { "-" };
            let step = format!("{step:?}");

            writeln!(out, "{step:<14}{outcome:<20}{wake}").unwrap();
        }
        out
    }

    #[tokio::test]
    async fn test_reservations_are_refused_at_capacity_and_taken_in_order() {
        let trace = run(
            2,
            &[
                Punch(1, 1),
                Punch(2, 1),
                Reserve,
                Recv,
                Punch(3, 1),
                Recv,
                Recv,
                TryRecv,
            ],
        )
        .await;
        insta::assert_snapshot!(trace, @"
        Punch(1, 1)   sent                -
        Punch(2, 1)   sent                -
        Reserve       refused             -
        Recv          took [punch 1..2]   woken
        Punch(3, 1)   sent                -
        Recv          took [punch 2..3]   -
        Recv          took [punch 3..4]   -
        TryRecv       empty               -
        ");
    }

    /// A horizon copy may reserve room but find nothing to copy. The next
    /// reservation reuses that slot.
    #[tokio::test]
    async fn test_an_unsent_permit_leaves_its_slot_to_the_next() {
        let trace = run(1, &[Reserve, Punch(1, 1), Reserve, TryRecv, TryRecv]).await;
        insta::assert_snapshot!(trace, @"
        Reserve       room                -
        Punch(1, 1)   sent                -
        Reserve       refused             -
        TryRecv       took [punch 1..2]   woken
        TryRecv       empty               -
        ");
    }

    #[tokio::test]
    async fn test_dropping_the_owner_half_drains_and_then_ends() {
        let trace = run(4, &[Punch(7, 3), DropRecorder, Recv, Recv]).await;
        insta::assert_snapshot!(trace, @"
        Punch(7, 3)   sent                -
        DropRecorder  dropped             -
        Recv          took [punch 7..10]  -
        Recv          ended               -
        ");
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

        recorder
            .reserve()
            .unwrap()
            .send(vec![chunk::encode_punch(9, 2)]);
        drop(recorder);

        let (first, second) = taker.await.unwrap();
        let trace = format!("first:  {}\nsecond: {}", received(first), received(second));
        insta::assert_snapshot!(trace, @"
        first:  took [punch 9..11]
        second: ended
        ");
    }

    /// Freeing a slot or dropping the consumer must wake parked requests, which
    /// otherwise prevent device stop from completing.
    #[tokio::test]
    async fn test_a_refused_reservation_is_woken_by_a_take_and_by_a_released_consumer() {
        let trace = run(
            1,
            &[
                Punch(1, 1),
                // A refusal parks the owner without waking it.
                Reserve,
                TryRecv,
                Punch(2, 1),
                Reserve,
                // Dropping the consumer wakes the owner and releases backpressure.
                DropRecorded,
                Punch(3, 1),
            ],
        )
        .await;
        insta::assert_snapshot!(trace, @"
        Punch(1, 1)   sent                -
        Reserve       refused             -
        TryRecv       took [punch 1..2]   woken
        Punch(2, 1)   sent                -
        Reserve       refused             -
        DropRecorded  dropped             woken
        Punch(3, 1)   sent                -
        ");
    }
}
