use tokio::sync::mpsc;

/// A `BuildHasher` for `Producer`-keyed maps that passes through the
/// raw bytes as the hash value. Producer IDs are already uniformly
/// distributed random values, so rehashing them with SipHash is wasted work.
#[derive(Clone, Default)]
pub struct ProducerHasher;

impl std::hash::BuildHasher for ProducerHasher {
    type Hasher = ProducerHasherState;

    #[inline]
    fn build_hasher(&self) -> Self::Hasher {
        ProducerHasherState(0)
    }
}

/// Hasher state for [`ProducerHasher`]. Packs written bytes into a `u64`.
pub struct ProducerHasherState(u64);

impl std::hash::Hasher for ProducerHasherState {
    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }

    #[inline]
    fn write(&mut self, _bytes: &[u8]) {
        unreachable!("ProducerHasherState may only be used with Producer");
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
}

/// Map keyed by `Producer` using a passthrough hasher. Producer IDs are
/// already uniformly distributed random values, so we skip rehashing.
pub type ProducerMap<V> =
    std::collections::HashMap<proto_gazette::uuid::Producer, V, ProducerHasher>;

/// Re-export of `proto_flow::shuffle` so that dependents can refer to
/// protocol message types as `shuffle::proto::*`, avoiding the naming
/// conflict between this crate and the protobuf module.
pub use proto_flow::shuffle as proto;

pub mod binding;
mod client;
pub mod frontier;
pub mod log;
pub mod merge;
mod service;
mod session;
pub mod slice;

#[cfg(test)]
mod open_deadlock_tests;
#[cfg(test)]
pub(crate) mod testing;

/// Document passed JSON Schema validation.
/// Bit 15 of the flags u16, above the 10-bit UUID wire space (bits 0-9).
/// Shared by `slice::read::Meta::flags` and `log::block::BlockMeta::flags`.
///
/// Note that `uuid::build()` asserts flags fit in 10 bits, so accidental
/// round-trip (for example, in an actual document) is impossible.
pub const FLAGS_SCHEMA_VALID: u16 = 0x8000;

pub use binding::{Binding, Source};
pub use client::SessionClient;
pub use frontier::{Accounted, Frontier, JournalFrontier, ProducerFrontier};
pub use service::{DEFAULT_SHUFFLE_DISK_LIMIT_BYTES, Service};

/// Clock distance at which a producer's commits become ancient history.
///
/// A closed producer is pruned (forgotten) from a `(journal, binding)` entry
/// only once its `last_commit` trails the entry's newest commit by at least
/// this horizon ([`JournalFrontier::prune_stale_producers`]).
/// The runtime prunes a shard's committed frontier this way at recovery.
/// The session prunes its cumulative Frontier as it takes checkpoints.
///
/// A later backfill can re-extract a causal hint naming a pruned producer.
/// Its ACK can't resolve the hint: the journal is read past it, and the
/// producer is retired. Only the entry's `prune_floor` can discharge it
/// ([`JournalFrontier::accounts_for`]), and it always does:
///
/// 1. The hint is at-or-below the producer's pruned `last_commit`.
/// 2. If the session pruned the producer, it raised `prune_floor` to that
///    `last_commit`.
/// 3. If the producer was pruned before the session, it trailed its journal's
///    newest commit by at least this horizon. That commit was retained, and
///    the session seeds `prune_floor` to it less this horizon
///    ([`Frontier::seed_prune_floors`]).
///
/// Step 3 requires that no prior prune forgot a producer nearer than this
/// horizon. Otherwise its hints stall until [`CAUSAL_HINT_RESOLUTION_TIMEOUT`]
/// tears down the session. The next session resumes before the hinting ACK,
/// and repeats forever.
///
/// Checkpoints migrated from V1 are subject to step 3 as well. Gazette's
/// `message.Sequencer` pruned them per journal by clock alone at 24 hours.
/// That, and not any need of V2 itself, is why this horizon is 24 hours.
///
/// The horizon must also stay well above producer clock skew plus append delay.
/// `prune_floor` accounts for the clocks of every absent producer, including
/// one the entry has never read. A hint naming such a producer, at a clock
/// below the floor, is discharged though its ACK is unread.
pub const PRODUCER_STALENESS_HORIZON: std::time::Duration =
    std::time::Duration::from_secs(24 * 60 * 60);

/// Margin added to the clock which sets a read's gap floor
/// ([`slice::read::ReadState::take_gap_floor`]).
///
/// It covers producer clock skew only: offset order is not clock order, so an
/// ACK the gap removed may post-date that clock.
pub const CAUSAL_HINT_GAP_MARGIN: std::time::Duration = std::time::Duration::from_secs(10 * 60);

/// Return the current wall-clock time as a `uuid::Clock`.
///
/// This routine is aware of tokio paused/advanced test time when running
/// under a CurrentThread runtime in debug builds, using the same technique
/// as `tokens::now()`. It captures a `(std::time::Instant, SystemTime)` pair
/// once, then maps elapsed tokio time through that baseline.
fn now_clock() -> proto_gazette::uuid::Clock {
    if cfg!(debug_assertions)
        && tokio::runtime::Handle::try_current()
            .ok()
            .map(|h| h.runtime_flavor())
            == Some(tokio::runtime::RuntimeFlavor::CurrentThread)
    {
        static TIME_POINT: std::sync::LazyLock<(std::time::Instant, std::time::SystemTime)> =
            std::sync::LazyLock::new(|| (std::time::Instant::now(), std::time::SystemTime::now()));

        let (start_instant, start_system) = &*TIME_POINT;
        let elapsed = tokio::time::Instant::now()
            .duration_since(tokio::time::Instant::from_std(*start_instant));

        proto_gazette::uuid::Clock::from_time(*start_system + elapsed)
    } else {
        proto_gazette::uuid::Clock::from_time(std::time::SystemTime::now())
    }
}

fn new_channel<T>() -> (mpsc::Sender<T>, mpsc::Receiver<T>) {
    mpsc::channel::<T>(proto_grpc::CHANNEL_BUFFER)
}

/// Non-blocking channel send that enforces capacity invariants.
///
/// Returns an error on `Full` — the caller's design must guarantee
/// sufficient capacity. A Full here means a capacity invariant is
/// violated. The error propagates up to tear down the session (fail-fast).
///
/// Silently drops on `Closed` — the peer has exited. The caller will
/// discover a more informative error from the peer's rx stream.
fn verify_send<T>(tx: &mpsc::Sender<T>, value: T) -> anyhow::Result<()> {
    match tx.try_send(value) {
        Ok(()) => Ok(()),
        Err(mpsc::error::TrySendError::Closed(_)) => Ok(()),
        Err(mpsc::error::TrySendError::Full(_)) => {
            anyhow::bail!("verify_send: channel full; capacity invariant violated")
        }
    }
}

/// Await `futures`, returning their outputs in order, or else the first error
/// to occur (dropping the others).
///
/// We don't use `futures::future::try_join_all` because, beyond a small number
/// of futures (30, today), it yields their outputs in order: an error isn't
/// observed until every preceding future completes. That can hang our open
/// fan-outs: a Log responds Opened only once every Slice connects, so if one
/// open fails, an earlier open may wait forever and the error is never seen.
///
/// See also: issue #3245.
async fn try_join_all_eager<T>(
    futures: impl IntoIterator<Item = impl std::future::Future<Output = anyhow::Result<T>>>,
) -> anyhow::Result<Vec<T>> {
    let mut pending: futures::stream::FuturesUnordered<_> = futures
        .into_iter()
        .enumerate()
        .map(|(index, future)| async move { (index, future.await) })
        .collect();

    let mut outputs: Vec<Option<T>> = std::iter::repeat_with(|| None)
        .take(pending.len())
        .collect();

    while let Some((index, result)) = futures::StreamExt::next(&mut pending).await {
        outputs[index] = Some(result?);
    }
    Ok(outputs.into_iter().map(Option::unwrap).collect())
}

/// Build the error for a request stream that closed or spoke out of turn while
/// a handler was still opening. No legitimate request arrives before a handler
/// emits its `Opened` (the coordinator sends `resume_checkpoint`, and the
/// Session sends `Start`, only after observing `Opened`). So EOF, an error, or
/// any message means the client (or its RPC) went away: the handler aborts,
/// dropping its downstream open fan-out so that EOF teardown cascades to peers.
/// Deliberately involves no deadline — see the crate README "Shutdown" notes.
fn opening_aborted<T>(source: &str, peer: &str, msg: Option<tonic::Result<T>>) -> anyhow::Error {
    match msg {
        None => {
            anyhow::format_err!("{source} request stream from {peer} reached EOF during opening")
        }
        Some(Err(status)) => proto_grpc::status_to_anyhow(status).context(format!(
            "{source} request stream from {peer} errored during opening"
        )),
        Some(Ok(_)) => anyhow::format_err!(
            "{source} received an unexpected request from {peer} during opening (before Opened was sent)"
        ),
    }
}

/// Interval at which all actor event loops tick, ensuring per-loop tracing
/// instrumentation fires periodically even when no other events arrive.
/// The Session actor additionally uses ticks for mark-and-sweep detection
/// of stalled causal hint resolution, accumulating stalled ticks against
/// [`CAUSAL_HINT_RESOLUTION_TIMEOUT`].
const ACTOR_TICKER_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// Duration that causal hint resolution may stall — no progress against any
/// unresolved hint — before the Session actor tears down the session. It's
/// decoupled from [`ACTOR_TICKER_INTERVAL`] (the tracing cadence) and enforced
/// via mark-and-sweep over that many ticks. See `session::state::CheckpointState`.
const CAUSAL_HINT_RESOLUTION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15 * 60);

#[cfg(test)]
mod test {
    // An error is returned as it occurs, though preceding futures never
    // complete, and beyond the count at which `try_join_all` yields in order.
    #[tokio::test]
    async fn test_try_join_all_eager_short_circuits() {
        let futures = (0..40).map(|index| async move {
            match index {
                39 => anyhow::bail!("failed {index}"),
                _ => std::future::pending().await,
            }
        });
        let err = super::try_join_all_eager::<()>(futures).await.unwrap_err();
        assert_eq!(err.to_string(), "failed 39");

        let outputs = super::try_join_all_eager((0..40).map(|index| async move {
            tokio::task::yield_now().await;
            Ok(index)
        }))
        .await
        .unwrap();
        assert_eq!(outputs, (0..40).collect::<Vec<_>>());
    }
}
