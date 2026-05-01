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
mod service;
mod session;
mod slice;

#[cfg(test)]
pub(crate) mod testing;

/// Document passed JSON Schema validation.
/// Bit 15 of the flags u16, above the 10-bit UUID wire space (bits 0-9).
/// Shared by `slice::read::Meta::flags` and `log::block::BlockMeta::flags`.
///
/// Note that `uuid::build()` asserts flags fit in 10 bits, so accidental
/// round-trip (for example, in an actual document) is impossible.
pub const FLAGS_SCHEMA_VALID: u16 = 0x8000;

pub use binding::Binding;
pub use client::SessionClient;
pub use frontier::{Frontier, JournalFrontier, ProducerFrontier};
pub use service::Service;

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
    mpsc::channel::<T>(32)
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

// Map an anyhow::Error into a tonic::Status.
#[inline]
fn anyhow_to_status(err: anyhow::Error) -> tonic::Status {
    match err.downcast::<tonic::Status>() {
        Ok(status) => status,
        Err(err) => tonic::Status::unknown(format!("{err:?}")),
    }
}

// Map a tonic::Status into an anyhow::Error.
#[inline]
fn status_to_anyhow(status: tonic::Status) -> anyhow::Error {
    match status.code() {
        tonic::Code::Unknown => anyhow::anyhow!(status.message().to_owned()),
        _ => anyhow::Error::new(status),
    }
}

// verify is a convenience for building protocol error messages in a standard, structured way.
// You call verify to establish a Verify instance, which is then used to assert expectations
// over protocol requests or responses.
// If an expectation fails, it produces a suitable error message.
fn verify<'p>(
    source: &'static str,
    expect: &'static str,
    peer_endpoint: &'p str,
    peer_index: usize,
) -> Verify<'p> {
    Verify {
        source,
        expect,
        peer_endpoint,
        peer_index,
    }
}

struct Verify<'p> {
    source: &'static str,
    expect: &'static str,
    peer_endpoint: &'p str,
    peer_index: usize,
}

impl<'p> Verify<'p> {
    #[inline]
    fn ok<T>(&self, t: tonic::Result<T>) -> anyhow::Result<T> {
        match t {
            Ok(t) => Ok(t),
            Err(status) => Err(self.fail_status(status)),
        }
    }

    #[inline]
    fn eof<T: serde::Serialize>(&self, t: Option<tonic::Result<T>>) -> anyhow::Result<()> {
        match t {
            None => Ok(()),
            Some(Err(status)) => Err(self.fail_status(status)),
            Some(Ok(t)) => Err(self.fail(t)),
        }
    }

    #[inline]
    fn not_eof<T>(&self, t: Option<tonic::Result<T>>) -> anyhow::Result<T> {
        if let Some(t) = t {
            Ok(self.ok(t)?)
        } else {
            Err(self.fail(Option::<()>::None))
        }
    }

    #[must_use]
    #[cold]
    fn fail<T: serde::Serialize>(&self, t: T) -> anyhow::Error {
        let Self {
            source,
            expect,
            peer_endpoint,
            peer_index,
        } = self;

        let mut t = serde_json::to_string(&t).unwrap();
        t.truncate(4096);

        if t == "null" {
            anyhow::format_err!("unexpected {source} EOF (expected {expect})")
        } else {
            anyhow::format_err!(
                "{source} protocol error (expected {expect}) from {peer_endpoint}@{peer_index}: {t}"
            )
        }
    }

    #[must_use]
    #[cold]
    fn fail_status(&self, status: tonic::Status) -> anyhow::Error {
        let Self {
            source,
            expect,
            peer_endpoint,
            peer_index,
        } = self;

        status_to_anyhow(status).context(format!(
            "{source} error (expected {expect}) from {peer_endpoint}@{peer_index}"
        ))
    }
}

/// Interval at which all actor event loops tick, ensuring per-loop tracing
/// instrumentation fires periodically even when no other events arrive.
/// The Session actor additionally uses ticks for mark-and-sweep detection
/// of stalled causal hint resolution.
const ACTOR_TICKER_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
