use super::rebuild;
use super::uncommitted;
use crate::image;

/// A playback which is running.
pub struct Playback {
    /// Resolves when the backfill reaches the head. It is taken once it does, so a
    /// playback which caught up long ago does not report it twice.
    caught_up: Option<tokio::sync::oneshot::Receiver<()>>,
    /// Requests a handoff of replay state to promotion.
    stop: tokio_util::sync::CancellationToken,
    /// Background replay task, returning its rebuild state.
    task: tokio::task::JoinHandle<anyhow::Result<rebuild::Rebuild>>,
}

impl Playback {
    /// Start a playback of `reading` from `floor` into `image`.
    pub(in crate::journal) fn start(
        reading: Reading,
        floor: i64,
        head: i64,
        image: image::Image,
        uncommitted: uncommitted::UncommittedDelta,
    ) -> Self {
        let (signal, caught_up) = tokio::sync::oneshot::channel();
        let stop = tokio_util::sync::CancellationToken::new();
        let rebuild = rebuild::Rebuild::new(image, uncommitted, floor);

        Self {
            caught_up: Some(caught_up),
            task: tokio::spawn(play(reading, head, signal, stop.clone(), rebuild)),
            stop,
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

    /// Stop playback at its current record boundary and return the rebuild.
    /// Promotion may interrupt backfill and continue from that boundary without
    /// losing sequencing state. Any partially read record is read again.
    pub(in crate::journal) async fn stop(self) -> anyhow::Result<rebuild::Rebuild> {
        let Self { stop, task, .. } = self;
        () = stop.cancel();

        match task.await {
            Ok(result) => result,
            Err(panic) => Err(anyhow::anyhow!("the playback task panicked: {panic}")),
        }
    }

    /// The error a playback ended with.
    ///
    /// A playback which was not promoted cannot end well: a tail does not finish on
    /// its own, and only a promotion stops one.
    async fn failure(&mut self) -> anyhow::Error {
        match (&mut self.task).await {
            Ok(Ok(_rebuild)) => anyhow::anyhow!("a playback ended without being promoted"),
            Ok(Err(err)) => err,
            Err(panic) => anyhow::anyhow!("the playback task panicked: {panic}"),
        }
    }
}

/// Read context shared across backfill and tailing. The cancellation token
/// belongs to the tenure; `Journal` holds the same context for writes.
pub(in crate::journal) struct Reading {
    /// Broker client used for backfill and tailing.
    pub(in crate::journal) client: gazette::journal::Client,
    /// Name of the disk journal being replayed.
    pub(in crate::journal) journal: String,
    /// Cancelled once the tenure is over, which ends the playback wherever it is.
    pub(in crate::journal) ended: tokio_util::sync::CancellationToken,
}

/// Backfill to the head resolved at `Open`, signal readiness, then tail until
/// promotion or tenure cancellation. A gap fails the tenure; see [`super`].
async fn play(
    reading: Reading,
    head: i64,
    caught_up: tokio::sync::oneshot::Sender<()>,
    stop: tokio_util::sync::CancellationToken,
    mut rebuild: rebuild::Rebuild,
) -> anyhow::Result<rebuild::Rebuild> {
    let Reading {
        client,
        journal,
        ended,
    } = &reading;

    // An empty journal can fully suspend and then refuse reads. Report it
    // ready and wait for promotion, which lists again and reads through its
    // fence to catch any writes made while this playback was parked.
    if head == 0 {
        _ = caught_up.send(());

        return tokio::select! {
            () = stop.cancelled() => Ok(rebuild),
            () = ended.cancelled() => Err(proto_grpc::status_to_anyhow(tonic::Status::unavailable(
                "the tenure ended while its playback waited".to_string(),
            ))),
        };
    }

    tokio::select! {
        result = super::read(client, journal, Some(head), &mut rebuild) => {
            () = result?;
        }
        () = stop.cancelled() => return Ok(rebuild),
        () = ended.cancelled() => return Err(proto_grpc::status_to_anyhow(tonic::Status::unavailable(
            "the tenure ended while its playback backfilled".to_string(),
        ))),
    }

    tracing::info!(
        journal,
        head,
        chunks = rebuild.applied_chunks(),
        "a playback reached the journal head",
    );
    _ = caught_up.send(());

    tokio::select! {
        result = super::read(client, journal, None, &mut rebuild) => {
            () = result?;
            anyhow::bail!("a tail of {journal} ended on its own, which it cannot do")
        }
        () = stop.cancelled() => Ok(rebuild),
        () = ended.cancelled() => Err(proto_grpc::status_to_anyhow(tonic::Status::unavailable(
            "the tenure ended while its playback tailed".to_string(),
        ))),
    }
}
