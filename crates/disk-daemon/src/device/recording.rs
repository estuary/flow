use crate::proto;

/// One recorded mutation or horizon copy, which the writer appends as one record.
pub struct Mutation {
    /// The write, punch, or copy, encoded.
    pub chunks: Vec<proto::Chunk>,
    /// Whether this, the first mutation of its delta, opened a horizon.
    pub opens_horizon: bool,
}

/// Receives one disk's mutations from its [`Recorder`], in order. `recv` returns
/// `None` once the recorder is dropped and every mutation has been taken.
pub type Recorded = tokio::sync::mpsc::Receiver<Mutation>;

/// Create a channel holding `capacity` mutations, waking `waker` whenever a
/// reservation it refused may be tried again.
pub(super) fn channel(capacity: usize, waker: std::task::Waker) -> (Recorder, Recorded) {
    let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
    let sender = tokio_util::sync::PollSender::new(sender);

    (Recorder { sender, waker }, receiver)
}

/// Sends one disk's mutations to its [`Recorded`]. Held by that disk's owner.
pub(super) struct Recorder {
    sender: tokio_util::sync::PollSender<Mutation>,
    waker: std::task::Waker,
}

impl Recorder {
    /// Reserve room for one mutation, or return `None` if the channel is full.
    /// Dropping the [`Permit`] unsent keeps its room reserved for the next call.
    pub fn reserve(&mut self) -> Option<Permit<'_>> {
        let mut cx = std::task::Context::from_waker(&self.waker);

        match self.sender.poll_reserve(&mut cx) {
            std::task::Poll::Pending => None,
            std::task::Poll::Ready(Ok(()) | Err(_)) => Some(Permit(self)),
        }
    }
}

/// Room reserved for one mutation.
pub(super) struct Permit<'a>(&'a mut Recorder);

impl Permit<'_> {
    /// Queue one whole `mutation`.
    pub fn send(self, mutation: Mutation) {
        // Fails only once the consumer is gone, and the mutation is discarded.
        let _ = self.0.sender.send_item(mutation);
    }
}

#[cfg(test)]
mod test {
    use super::{Mutation, channel};
    use crate::chunk;
    use std::fmt::Write as _;
    use tokio::sync::mpsc;

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

    fn received(mutation: Option<Mutation>) -> String {
        match mutation {
            Some(Mutation { chunks, .. }) => {
                let blocks: Vec<_> = chunks.iter().map(chunk::covered_blocks).collect();
                format!("took {blocks:?}")
            }
            None => "ended".to_string(),
        }
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
                        () = permit.send(Mutation {
                            chunks: vec![chunk::encode_punch(block, blocks)],
                            opens_horizon: false,
                        });
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
                    Ok(mutation) => received(Some(mutation)),
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

            writeln!(out, "{step:<14}{outcome:<14}{wake}").unwrap();
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
        Punch(1, 1)   sent          -
        Punch(2, 1)   sent          -
        Reserve       refused       -
        Recv          took [1..2]   woken
        Punch(3, 1)   sent          -
        Recv          took [2..3]   -
        Recv          took [3..4]   -
        TryRecv       empty         -
        ");
    }

    /// A horizon copy may reserve room but find nothing to copy. The next
    /// reservation reuses that slot.
    #[tokio::test]
    async fn test_an_unsent_permit_leaves_its_slot_to_the_next() {
        let trace = run(1, &[Reserve, Punch(1, 1), Reserve, TryRecv, TryRecv]).await;
        insta::assert_snapshot!(trace, @"
        Reserve       room          -
        Punch(1, 1)   sent          -
        Reserve       refused       -
        TryRecv       took [1..2]   woken
        TryRecv       empty         -
        ");
    }

    #[tokio::test]
    async fn test_dropping_the_owner_half_drains_and_then_ends() {
        let trace = run(4, &[Punch(7, 3), DropRecorder, Recv, Recv]).await;
        insta::assert_snapshot!(trace, @"
        Punch(7, 3)   sent          -
        DropRecorder  dropped       -
        Recv          took [7..10]  -
        Recv          ended         -
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
        Punch(1, 1)   sent          -
        Reserve       refused       -
        TryRecv       took [1..2]   woken
        Punch(2, 1)   sent          -
        Reserve       refused       -
        DropRecorded  dropped       woken
        Punch(3, 1)   sent          -
        ");
    }
}
