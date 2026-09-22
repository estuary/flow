//! A disk journal's replay, as a task which runs until its tenure promotes it.
//!
//! A tenure which serves a disk immediately still runs this. It opens, the playback
//! backfills, and the promotion arrives at once. A tenure which stands by runs the
//! same playback and promotes later, perhaps hours later. There is one read path, and
//! a promotion is an action on top of it.
//!
//! The task holds the image and applies committed records to it. It holds an
//! unacknowledged delta rather than applying it, per [`super::buffer`], so the image
//! it hands over is at the committed edge and never ahead of it.
//!
//! A backfill reads the history of the journal. A tail then follows new records as
//! they arrive, and reaching that point is the one transition a client acts on: it
//! is reported as `Opened`, and a promotion from there costs a fence, the records
//! which arrive after it, and a mount.

use super::buffer::Buffer;
use super::replay::{self, Extent, Pass};
use crate::failure;
use crate::image::Image;
use anyhow::Context;

/// Times a read gap restarts a playback before its tenure fails.
///
/// A gap means the fragments this playback still needed were deleted, which happens
/// only when it falls more than a whole recovery range behind. One restart is a
/// playback which was unlucky. Three are a playback which cannot keep up with the
/// writer of its journal, and which would otherwise read forever without ever
/// becoming ready.
const RESTART_LIMIT: usize = 3;

/// What a playback hands to the tenure which promotes it.
pub(super) struct Handoff {
    pub(super) image: Image,
    /// Sequencing state of the read, which the promotion's own read continues.
    ///
    /// It must continue rather than start again. It holds each producer's clocks and
    /// the delta this playback is holding, so a fresh pass would not recognise the
    /// acknowledgement of that delta when the promotion reads it.
    pub(super) pass: Pass,
    /// Offset through which committed state is applied. The promotion reads from
    /// here, so nothing is read twice and nothing is skipped.
    pub(super) applied: i64,
}

/// The journal a playback reads, and the tenure whose end stops it.
///
/// These three are carried together because every read a playback makes needs all
/// three: the client and the name to issue it, and the token to give it up on.
/// [`super::Journal`] holds the same facts for the writer side.
pub(super) struct Source {
    pub(super) client: gazette::journal::Client,
    pub(super) journal: String,
    /// Cancelled once the tenure is over, which ends the playback wherever it is.
    pub(super) ended: tokio_util::sync::CancellationToken,
}

/// Where one pass over the journal begins, and the one transition it reports.
struct Progress {
    /// Offset the backfill seeks from. A restart after a [`replay::Gap`] moves it to
    /// the journal's current floor.
    floor: i64,
    /// Head the backfill reads to. It is the one a tenure's `Open` resolved, and
    /// zero for a journal with no content at all.
    head: i64,
    /// Taken when the backfill first reaches `head`. A playback which restarts does
    /// not retract that: `Opened` is sent once.
    caught_up: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Progress {
    /// Tell the tenure that the backfill has reached the head, if it has not been
    /// told already.
    fn report_caught_up(&mut self) {
        if let Some(signal) = self.caught_up.take() {
            _ = signal.send(());
        }
    }
}

/// A playback which is running.
pub struct Playback {
    /// Resolves when the backfill reaches the head. It is taken once it does, so a
    /// playback which caught up long ago does not report it twice.
    caught_up: Option<tokio::sync::oneshot::Receiver<()>>,
    stop: tokio_util::sync::CancellationToken,
    task: tokio::task::JoinHandle<anyhow::Result<Handoff>>,
}

impl Playback {
    /// Start a playback of `source` from `floor` into `image`.
    pub(super) fn start(
        source: Source,
        floor: i64,
        head: i64,
        image: Image,
        buffer: Buffer,
    ) -> Self {
        let (signal, caught_up) = tokio::sync::oneshot::channel();
        let stop = tokio_util::sync::CancellationToken::new();

        let progress = Progress {
            floor,
            head,
            caught_up: Some(signal),
        };
        let task = tokio::spawn(run(source, progress, image, buffer, stop.clone()));

        Self {
            caught_up: Some(caught_up),
            stop,
            task,
        }
    }

    /// Wait for the backfill to reach the journal head, which a tenure reports as
    /// `Opened`.
    ///
    /// This resolves once. Afterwards it reports the failure which ends the playback
    /// and otherwise never resolves, so a tenure may keep selecting on it and learn
    /// of a playback which dies while it stands by.
    pub async fn caught_up(&mut self) -> anyhow::Result<()> {
        if let Some(signal) = &mut self.caught_up {
            let reached = (&mut *signal).await.is_ok();
            self.caught_up = None;

            if reached {
                return Ok(());
            }
        }
        Err(self.failure().await)
    }

    /// The error a playback ended with.
    ///
    /// A playback which was not promoted cannot end well: a tail does not finish on
    /// its own, and only a promotion stops one.
    async fn failure(&mut self) -> anyhow::Error {
        match (&mut self.task).await {
            Ok(Ok(_handoff)) => anyhow::anyhow!("a playback ended without being promoted"),
            Ok(Err(err)) => err,
            Err(panic) => anyhow::anyhow!("the playback task panicked: {panic}"),
        }
    }

    /// Stop the playback at the head, and take what it holds.
    ///
    /// This is legal while the playback still backfills. The read is cancelled where
    /// it stands, and the promotion continues from the offset handed over.
    pub(super) async fn stop(self) -> anyhow::Result<Handoff> {
        let Self { stop, task, .. } = self;
        () = stop.cancel();

        match task.await {
            Ok(result) => result,
            Err(panic) => Err(anyhow::anyhow!("the playback task panicked: {panic}")),
        }
    }
}

/// Read the journal of `source` into `image` until the tenure promotes or ends.
///
/// A read gap discards the image and starts again from the journal's current floor.
/// That floor guarantees a read from it rebuilds the whole disk, so it is the only
/// repair, and it is always available. A partial image cannot be patched: a block
/// which was deallocated below the gap has no record above it to punch.
async fn run(
    source: Source,
    mut progress: Progress,
    mut image: Image,
    mut buffer: Buffer,
    stop: tokio_util::sync::CancellationToken,
) -> anyhow::Result<Handoff> {
    for restart in 0..=RESTART_LIMIT {
        let mut pass = Pass::new(buffer);

        let outcome = play(&source, &mut progress, &mut image, &mut pass, &stop).await;

        match outcome {
            Ok(()) => {
                return Ok(Handoff {
                    image,
                    applied: pass.applied_offset(),
                    pass,
                });
            }
            Err(err) if err.chain().any(|cause| cause.is::<replay::Gap>()) => {
                anyhow::ensure!(
                    restart != RESTART_LIMIT,
                    "the playback of {} read a deleted range {} times, so it cannot \
                     keep up with the writer of that journal: {err:#}",
                    source.journal,
                    RESTART_LIMIT + 1,
                );
                tracing::warn!(
                    journal = source.journal,
                    restart,
                    ?err,
                    "restarting a playback of a deleted range"
                );

                // Nothing of this image survives. A read from the new floor rebuilds
                // the disk, but only onto an image which holds nothing else.
                () = image.reset().context("discarding a playback's image")?;

                let (held, _floor, _horizon) = pass.into_parts();
                buffer = held;
                () = buffer.clear()?;
                progress.floor =
                    super::spec::current_floor(&source.client, &source.journal).await?;
            }
            Err(err) => return Err(err),
        }
    }
    unreachable!("the restart limit is checked within the loop")
}

/// Backfill to the head, then follow the journal until the tenure stops it.
async fn play(
    source: &Source,
    progress: &mut Progress,
    image: &mut Image,
    pass: &mut Pass,
    stop: &tokio_util::sync::CancellationToken,
) -> anyhow::Result<()> {
    let Source {
        client,
        journal,
        ended,
    } = source;

    // An empty journal holds nothing to replay, and reading one would wake a journal
    // which Gazette suspended: a disk which is never written must cost an etcd entry
    // and nothing more. Such a playback is current the moment it starts, and it stays
    // current by following nothing at all.
    //
    // Its tenure must therefore resolve the journal again when it promotes. A
    // primary may have formatted and committed the whole disk while this waited, and
    // nothing here would have seen it.
    if progress.head == 0 {
        () = progress.report_caught_up();
        () = ended_or_stopped(stop, ended).await?;

        return Ok(());
    }

    tokio::select! {
        result = replay::read(
            client,
            journal,
            progress.floor,
            Extent::Bounded(progress.head),
            image,
            pass,
        ) => {
            _ = result?;
        }
        () = stop.cancelled() => return Ok(()),
        () = ended.cancelled() => return Err(anyhow::Error::new(failure::Failure::Ended(
            "the tenure ended while its playback backfilled".to_string(),
        ))),
    }

    tracing::info!(
        journal,
        head = progress.head,
        applied = pass.applied_offset(),
        chunks = pass.applied_chunks(),
        "a playback reached the journal head",
    );
    () = progress.report_caught_up();

    tokio::select! {
        result = replay::read(client, journal, pass.applied_offset(), Extent::Tail, image, pass) => {
            _ = result?;
            anyhow::bail!("a tail of {journal} ended on its own, which it cannot do")
        }
        () = stop.cancelled() => Ok(()),
        () = ended.cancelled() => Err(anyhow::Error::new(failure::Failure::Ended(
            "the tenure ended while its playback tailed".to_string(),
        ))),
    }
}

/// Wait for the tenure to promote this playback, or to end.
async fn ended_or_stopped(
    stop: &tokio_util::sync::CancellationToken,
    ended: &tokio_util::sync::CancellationToken,
) -> anyhow::Result<()> {
    tokio::select! {
        () = stop.cancelled() => Ok(()),
        () = ended.cancelled() => Err(anyhow::Error::new(failure::Failure::Ended(
            "the tenure ended while its playback waited".to_string(),
        ))),
    }
}
