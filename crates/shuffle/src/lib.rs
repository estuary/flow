use tokio::sync::mpsc;

mod binding;
mod client;
pub mod frontier;
pub mod log;
mod service;
mod session;
mod slice;

#[cfg(test)]
pub(crate) mod testing;

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
    #[must_use]
    #[inline]
    fn ok<T>(&self, t: tonic::Result<T>) -> anyhow::Result<T> {
        match t {
            Ok(t) => Ok(t),
            Err(status) => Err(self.fail_status(status)),
        }
    }

    #[must_use]
    #[inline]
    fn eof<T: serde::Serialize>(&self, t: Option<tonic::Result<T>>) -> anyhow::Result<()> {
        match t {
            None => Ok(()),
            Some(Err(status)) => Err(self.fail_status(status)),
            Some(Ok(t)) => Err(self.fail(t)),
        }
    }

    #[must_use]
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

        if t == "None" {
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
