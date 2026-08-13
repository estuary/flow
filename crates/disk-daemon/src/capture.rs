//! The seam between an accepted device mutation and its durable copy.
//!
//! An owner offers each mutation's chunks here *before* it issues that
//! mutation against the image, so the order chunks are queued is the order the
//! image is modified, which is what makes journal order equal replay order.
//!
//! The channel is bounded, and that bound is the device's backpressure: a
//! mutation whose chunks do not fit parks until the consumer takes some. The
//! consumer is the journal appender; a test collects instead. Taking a mutation
//! is not appending it, so a consumer may hold what it takes, which is what
//! lets a disk's journal be created only once something is written.

use crate::proto::Chunk;
use crate::wake::Waker;

/// Offers one disk's mutations to its consumer. Held by that disk's owner.
pub struct Capture(std::sync::Arc<Shared>);

/// Takes one disk's mutations in the order the owner accepted them.
pub struct Captured(std::sync::Arc<Shared>);

/// Create a channel holding `capacity` mutations, waking `waker` whenever a
/// parked owner may retry.
pub fn channel(capacity: usize, waker: Waker) -> (Capture, Captured) {
    assert!(
        capacity != 0,
        "a capture channel holds at least one mutation"
    );

    let shared = std::sync::Arc::new(Shared {
        state: std::sync::Mutex::new(State {
            queue: std::collections::VecDeque::new(),
            capacity,
            parked: false,
            closed: false,
        }),
        ready: std::sync::Condvar::new(),
        waker,
    });
    (Capture(shared.clone()), Captured(shared))
}

struct Shared {
    state: std::sync::Mutex<State>,
    ready: std::sync::Condvar,
    waker: Waker,
}

struct State {
    queue: std::collections::VecDeque<Vec<Chunk>>,
    capacity: usize,
    /// An offer was refused, so the next take must wake the owner.
    parked: bool,
    closed: bool,
}

impl Capture {
    /// Queue `chunks`, which are one mutation and are queued whole so that
    /// backpressure never splits a device request across two deltas.
    ///
    /// Returns them if the channel is full. The caller parks that request and
    /// retries when its waker fires.
    pub fn offer(&self, chunks: Vec<Chunk>) -> Result<(), Vec<Chunk>> {
        let mut state = self.0.state.lock().unwrap();

        if state.queue.len() == state.capacity {
            state.parked = true;
            return Err(chunks);
        }
        state.queue.push_back(chunks);
        drop(state);

        self.0.ready.notify_one();
        Ok(())
    }
}

impl Drop for Capture {
    fn drop(&mut self) {
        self.0.state.lock().unwrap().closed = true;
        self.0.ready.notify_one();
    }
}

impl Captured {
    /// Take the next mutation, blocking until one arrives. `None` once the
    /// owner has dropped its [`Capture`] and the queue is drained.
    pub fn recv(&self) -> Option<Vec<Chunk>> {
        let mut state = self.0.state.lock().unwrap();

        loop {
            if let Some(chunks) = self.take(&mut state) {
                return Some(chunks);
            }
            if state.closed {
                return None;
            }
            state = self.0.ready.wait(state).unwrap();
        }
    }

    /// Take the next mutation if one is queued.
    pub fn try_recv(&self) -> Option<Vec<Chunk>> {
        let mut state = self.0.state.lock().unwrap();
        self.take(&mut state)
    }

    fn take(&self, state: &mut State) -> Option<Vec<Chunk>> {
        let chunks = state.queue.pop_front()?;

        if std::mem::take(&mut state.parked) {
            self.0.waker.wake();
        }
        Some(chunks)
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

    #[test]
    fn test_offers_are_refused_at_capacity_and_taken_in_order() {
        let (capture, captured) = pair(2);

        capture.offer(vec![encode_punch(1, 1)]).unwrap();
        capture.offer(vec![encode_punch(2, 1)]).unwrap();

        let refused = capture.offer(vec![encode_punch(3, 1)]).unwrap_err();
        assert_eq!(refused, vec![encode_punch(3, 1)]);

        assert_eq!(captured.recv().unwrap(), vec![encode_punch(1, 1)]);
        capture.offer(refused).unwrap();

        assert_eq!(captured.recv().unwrap(), vec![encode_punch(2, 1)]);
        assert_eq!(captured.recv().unwrap(), vec![encode_punch(3, 1)]);
        assert_eq!(captured.try_recv(), None);
    }

    #[test]
    fn test_dropping_the_owner_half_drains_and_then_ends() {
        let (capture, captured) = pair(4);
        capture.offer(vec![encode_punch(7, 3)]).unwrap();
        drop(capture);

        assert_eq!(captured.recv().unwrap(), vec![encode_punch(7, 3)]);
        assert_eq!(captured.recv(), None);
    }

    #[test]
    fn test_a_blocked_receiver_wakes_on_the_next_offer() {
        let (capture, captured) = pair(1);

        let taker = std::thread::spawn(move || captured.recv());
        capture.offer(vec![encode_punch(9, 2)]).unwrap();

        assert_eq!(taker.join().unwrap(), Some(vec![encode_punch(9, 2)]));
    }
}
