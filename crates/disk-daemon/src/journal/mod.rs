//! One tenure's journal: what it may be, and how it is claimed.
//!
//! `spec.rs` is the live specification a disk may be served from, and the
//! recovery-floor label the daemon stores on it; `fence.rs` is the `author`
//! register, and the claim which installs a tenure's epoch in it.

use proto_gazette::uuid;

mod spec;

pub mod buffer;
pub mod fence;
pub mod playback;
pub mod replay;

/// Run `work`, failing if the tenure ends before it finishes.
///
/// Every broker call a tenure makes retries a transient error until it succeeds.
/// That is right while the disk is live, and wrong the moment the tenure is over.
/// A teardown which waited on an unreachable broker would hold the disk's device
/// and its mount for as long as the outage lasted, and a draining daemon would
/// leave both behind.
async fn until_ended<T>(
    ended: &tokio_util::sync::CancellationToken,
    what: &str,
    work: impl Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    ended.run_until_cancelled(work).await.unwrap_or_else(|| {
        Err(anyhow::Error::new(crate::Failure::Ended(format!(
            "the tenure ended while {what}"
        ))))
    })
}

fn uuid_bytes(producer: uuid::Producer, clock: uuid::Clock, flags: uuid::Flags) -> bytes::Bytes {
    bytes::Bytes::copy_from_slice(uuid::build(producer, clock, flags).as_bytes())
}
