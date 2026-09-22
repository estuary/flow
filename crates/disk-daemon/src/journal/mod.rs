//! One tenure's journal: what it may be, how it is claimed, and how it is written.
//!
//! The files of this directory are:
//!
//! - `spec.rs` — the live journal specification a disk may be served from, and the
//!   recovery-floor label the daemon stores on it.
//! - `fence.rs` — the `author` register, and the claim which installs a tenure's
//!   epoch in it.
//! - `writer.rs` — the actor which appends a tenure's deltas, and the [`Writer`]
//!   handle its `Prepare` and `Acknowledge` reach it over.
//! - `playback.rs` — the replay task, which runs from `Open` until the promotion
//!   stops it.
//! - `replay.rs` — the read, and the rules by which records rebuild a disk.
//! - `buffer.rs` — the unacknowledged delta a replay holds rather than applies.
//!
//! This file is the phases a journal passes through. [`Opening`] is a journal which
//! is validated and being replayed, and which nothing has claimed. [`Claimed`] is one
//! this tenure holds, between the claim and the disk it goes on to serve. [`Journal`]
//! is what is carried through all of them: the name, the client, the appender, the
//! epoch, and the offsets a broker has confirmed.
//!
//! A tenure appends each mutation as its device accepts it. An acknowledgement
//! commits the delta, and the client makes that acknowledgement durable elsewhere
//! before it hands the acknowledgement back. The writer therefore holds the two
//! halves of a boundary apart. `prepare` finishes and confirms every data append,
//! then returns the acknowledgement's exact bytes. `acknowledge` appends those bytes
//! and awaits the broker's confirmation.
//!
//! Records reach the journal through the `publisher::Appender` which
//! `runtime-next` publishes its collection documents with. The writer hands it
//! one complete record at a time and checkpoints at that boundary, and the
//! appender decides when a batch of them becomes an append RPC. What is
//! particular to a disk stays here: claiming the journal and the recovery floor.
//!
//! The writer takes no mutation while an acknowledgement is outstanding. A tenure
//! stamps every delta with one producer, and Gazette sequences per producer: an
//! `ACK_TXN` commits the pending records of its producer whose clocks are at or
//! below its own and drops the rest, and an acknowledgement's clock is fixed at the
//! cut which built it. Records of the next delta which reached the journal ahead of
//! that acknowledgement would therefore be lost, so nothing of the next delta is
//! taken until it has landed. A device whose mutations nothing takes parks after a
//! queue depth of them, so a workload which writes heavily across a boundary waits
//! for `Acknowledge`, and a client keeps that interval short.
//!
//! A tenure holds one prepared delta at a time, per [`writer::Task::prepare`], so a
//! client is never handed a second acknowledgement while it holds an unconfirmed one.
//! This is the same rule the Gazette consumer framework applies between one
//! transaction's `StartCommit` and the last transaction's pending acknowledgement.
//!
//! The journal's specification is the caller's, and so is the journal. `Open` names
//! one which must already exist, and the daemon validates its live spec rather than
//! creating or converging it. A name nothing has created is what the tenure asked
//! for and no retry of it can succeed, so it fails the tenure as invalid. The daemon
//! never creates a journal, never deletes one, and writes exactly one field of a spec
//! it did not create: the recovery-floor label.
//!
//! A tenure claims its journal exactly once, at [`Opening::claim_journal`], and
//! claims it whether or not the journal holds anything. Every append behind that
//! claim checks the epoch it installed, so a writer serves only after the claim has
//! landed. A tenure's own setup — a fresh disk's `mkfs`, and the mount of any disk —
//! writes through that writer, and the tenure commits those writes itself before it
//! hands the mount to its client.

use crate::image::Image;
use crate::proto;
use anyhow::Context;
use proto_gazette::{fixed_framing, uuid};

mod spec;
mod writer;

pub mod buffer;
pub mod fence;
pub mod playback;
pub mod replay;

#[cfg(test)]
mod broker_test;

use spec::{Resolved, resolve};
pub use writer::Writer;

/// A tenure's journal before its disk exists.
///
/// Recovery is a step of its own. A disk with committed state must be rebuilt
/// before a device can be created over it, and its journal must be claimed before
/// it is read.
pub struct Opening {
    journal: Journal,
    appender: publisher::Appender,
}

/// A journal this tenure holds, before it serves the disk behind it.
///
/// The claim is a step of its own because it bounds a replay which is still
/// running: see [`Opening::claim_journal`]. What lies between the two is
/// [`Claimed::promote`], which finishes that replay and settles the offsets its
/// writer starts from.
pub struct Claimed {
    journal: Journal,
    appender: publisher::Appender,
    /// Offset of a horizon the replay left open, which the writer resumes rather
    /// than opening one of its own.
    horizon: Option<i64>,
}

/// What a recovery of a disk found in its journal.
pub struct Recovered {
    /// Whether the journal held committed state. False for a disk which is fresh,
    /// and whose filesystem the caller must format.
    pub recovered: bool,
    /// Blocks a horizon the replay left open still owes a copy, which the disk's
    /// owner resumes rather than opening a horizon of its own over whatever it
    /// finds allocated.
    ///
    /// This is `Some` exactly when [`Claimed`]'s own horizon offset is, and the
    /// two are the halves of one horizon: the writer completes it at that offset
    /// once the owner reports these blocks discharged.
    pub horizon: Option<crate::horizon::Horizon>,
}

impl Opening {
    /// Open `journal`, and read the head and recovery floor a replay of it starts
    /// from.
    ///
    /// Nothing is created here, and nothing is appended beyond the zero-byte probe.
    /// `ended` is the tenure's own cancellation, which every broker call of this
    /// journal gives up on: see `until_ended`.
    ///
    /// The journal must already exist. Its specification belongs to whoever created
    /// it, and this checks that live spec is one a disk could be recovered from —
    /// failing at `Open` rather than at the first write of a filesystem which is
    /// already mounted and already serving its client.
    pub async fn new(
        client: &gazette::journal::Client,
        journal: String,
        ended: tokio_util::sync::CancellationToken,
    ) -> anyhow::Result<Self> {
        let client = client.clone();

        // The author this reports is not kept. A standby opens once and claims much
        // later, and [`Opening::claim_journal`] reads the author of that moment.
        let Resolved { head, floor, .. } = resolve(&client, &journal, &ended).await?;

        // The claim is an append of its own, and checks the *prior* author (see
        // `fence`). Every record behind it checks the epoch that claim installed.
        let epoch = random_producer();
        let appender = publisher::Appender::new(client.clone(), journal.clone())
            .with_check_registers(fence::held_by(epoch));

        Ok(Self {
            journal: Journal {
                name: journal,
                client,
                ended,
                epoch,
                head,
                floor,
            },
            appender,
        })
    }

    /// Start the replay of this journal, which runs until [`Claimed::promote`] stops
    /// it.
    ///
    /// The replay seeks from the recovery floor of the journal's label. Zero reads
    /// from the first fragment the store still holds. It is a seek and never a
    /// filter, so a floor which is absent or behind costs work and cannot change
    /// what is rebuilt.
    ///
    /// This does not claim the journal, so another tenure may still be writing it.
    /// That is what makes a standby possible, and the claim in `promote` is what
    /// makes it safe: it bounds what the other writer can ever append, and the read
    /// which follows it converges on that bound.
    pub fn play(&mut self, image: Image, buffer: buffer::Buffer) -> playback::Playback {
        let source = playback::Source {
            client: self.journal.client.clone(),
            journal: self.journal.name.clone(),
            ended: self.journal.ended.clone(),
        };

        playback::Playback::start(source, self.seek(), self.journal.head, image, buffer)
    }

    /// Offset a replay of this journal seeks from.
    ///
    /// A floor above the head is not a floor of this journal at all, so it is ignored
    /// rather than trusted. Seeking past the records a disk needs loses them silently,
    /// while seeking from zero only costs the replay work. The next completed horizon
    /// writes the label again.
    fn seek(&mut self) -> i64 {
        if self.journal.floor <= self.journal.head {
            return self.journal.floor;
        }
        tracing::warn!(
            journal = self.journal.name,
            floor = self.journal.floor,
            head = self.journal.head,
            "ignoring a recovery floor which is above the journal's head",
        );
        self.journal.floor = 0;

        0
    }

    /// Take the journal from whoever holds it now, so that its head stops moving.
    ///
    /// This is the first half of a promotion, and it is separate because it does not
    /// wait for the replay. A promotion which fences before the replay is current
    /// bounds that replay: a head no other writer can move is one the replay
    /// converges on rather than chases.
    ///
    /// The journal is resolved again here, and nothing of what this tenure's own
    /// `Open` saw is used. A standby opens once and promotes much later, and the
    /// author goes stale in the interval: every fence since that open was another
    /// writer's and each was legitimate, so a claim against the author the open read
    /// fails although this tenure is the rightful writer — and if two standbies both
    /// hold that stale value, neither can promote at all. The compare-and-swap still
    /// arbitrates two promotions which race, because both compare against the value
    /// they each just read and only one installs itself over it.
    ///
    /// Every journal is claimed, whether or not it holds anything. A tenure serves a
    /// disk only behind its claim, and a fresh disk's own `mkfs` is a delta like any
    /// other, so there is no disk a tenure may write without first excluding whoever
    /// wrote the journal before it.
    ///
    /// A tenure claims once. The compare-and-swap is also the backstop for every
    /// race the listing and the probe could have lost: whatever the journal was when
    /// this tenure looked, only one epoch installs itself over the author it read.
    pub async fn claim_journal(self) -> anyhow::Result<Claimed> {
        let Self {
            mut journal,
            appender,
        } = self;

        let Resolved { prior, head, .. } =
            resolve(&journal.client, &journal.name, &journal.ended).await?;
        journal.head = head;

        () = until_ended(&journal.ended, "claiming", async {
            fence::claim(
                &journal.client,
                &journal.name,
                prior.as_deref(),
                journal.epoch,
                fence::record(journal.epoch),
            )
            .await
        })
        .await?;

        Ok(Claimed {
            journal,
            appender,
            horizon: None,
        })
    }
}

impl Claimed {
    /// Stop `playback` at the head, finish the replay at the fenced head, and report
    /// what it rebuilt. Each of `recovered_acks` is then appended, once the replay
    /// can say it is one it could honor: see [`check_recovered_ack`].
    ///
    /// [`Opening::claim_journal`] runs first and holds the journal, so nobody else can
    /// append past what this reads.
    ///
    /// Fresh and recovered are told apart by what the replay applied, and not by
    /// what the journal holds: a journal of nothing but fences and orphaned deltas
    /// is a disk which was never committed. A client holds a recovered
    /// acknowledgement only for a journal whose data appends a broker confirmed, so
    /// a replay which applies nothing alongside one is committed state which was
    /// destroyed. That is an error, and not a fresh disk which hides it.
    ///
    /// The claim comes before the read which finishes here and before the repair.
    /// Both of those read or change state which the previous writer must no longer
    /// be able to touch.
    ///
    /// A delta which the playback held and which is still unacknowledged is dropped
    /// here. Its records were never applied, so the image is already at the disk's
    /// committed edge, and dropping them is what keeps a promoted disk from running
    /// ahead of the client's own commit.
    pub async fn promote(
        &mut self,
        playback: playback::Playback,
        recovered_acks: Vec<bytes::Bytes>,
    ) -> anyhow::Result<(Image, Recovered)> {
        let playback::Handoff {
            mut image,
            mut pass,
            applied,
        } = playback.stop().await?;

        // The head is read after the claim, and from a broker which has just served
        // an append. Its index therefore covers every fragment below it, and the
        // claim means nobody else can append past it. That fixes the end of this
        // read and makes it fresh.
        let mut head = until_ended(&self.journal.ended, "promoting", async {
            let head = fence::probe(&self.journal.client, &self.journal.name)
                .await?
                .head;

            _ = replay::read(
                &self.journal.client,
                &self.journal.name,
                applied,
                replay::Extent::Bounded(head),
                &mut image,
                &mut pass,
            )
            .await?;

            anyhow::Ok(head)
        })
        .await?;

        // The pass has sequenced every delta any writer appended, so it can say which
        // acknowledgements it could honor. One it could not would fail this replay and
        // every later one, so it is refused before it is appended.
        for ack in &recovered_acks {
            () = check_recovered_ack(&pass, ack)?;
        }
        let repaired = !recovered_acks.is_empty();

        if repaired {
            for ack in recovered_acks {
                () = self
                    .journal
                    .append_ack(&mut self.appender, &ack)
                    .await
                    .context("repairing a recovered acknowledgement")?;
            }
            // The acknowledgements landed at `head`, and the flush which appended them
            // learned the head beyond them. They are read back through the same pass,
            // which is how it learns they committed what it held.
            let repaired_head = self.journal.head;

            () = until_ended(&self.journal.ended, "promoting", async {
                _ = replay::read(
                    &self.journal.client,
                    &self.journal.name,
                    head,
                    replay::Extent::Bounded(repaired_head),
                    &mut image,
                    &mut pass,
                )
                .await?;

                anyhow::Ok(())
            })
            .await?;
            head = repaired_head;
        }

        let (chunks, derived) = (pass.applied_chunks(), pass.derived_floor());
        let (held, _floor, opened) = pass.into_parts();

        // The offset is the writer's, and the blocks are the owner's.
        let (horizon_at, horizon) = match opened {
            Some(replay::Opened { at, blocks }) => (Some(at), Some(blocks)),
            None => (None, None),
        };

        if !held.is_empty() {
            tracing::info!(
                journal = self.journal.name,
                bytes = held.len(),
                "dropping the delta which the journal never acknowledged",
            );
        }

        tracing::info!(
            journal = self.journal.name,
            head,
            chunks,
            floor = ?derived,
            horizon = ?horizon_at,
            "replayed a disk from its journal",
        );

        // The acknowledgements repaired above prove a broker confirmed this
        // disk's data appends. A replay which applied nothing means those
        // records were destroyed, even though the journal's head outlived them.
        // The acknowledged records are the newest the disk has, so no floor this
        // daemon stored can seek past them.
        crate::ensure_valid!(
            !repaired || chunks != 0,
            "the tenure supplied recovered acknowledgements, but a replay of journal {} \
             applied nothing: its committed state was destroyed",
            self.journal.name,
        );

        self.journal.head = head;
        self.journal.floor = derived.unwrap_or(self.journal.floor);
        self.horizon = horizon_at;

        // A floor the replay derived is one an earlier tenure completed a horizon
        // for but could not store, because it died before it could. This tenure
        // stores it on that tenure's behalf, which is what makes the scheme
        // self-healing.
        if let Some(derived) = derived {
            () = self.journal.store_floor(derived).await;
        }

        Ok((
            image,
            Recovered {
                recovered: chunks != 0,
                horizon,
            },
        ))
    }
}

/// Refuse a recovered acknowledgement which a replay could not honor, before it is
/// appended. A journal which holds one fails every replay from then on.
fn check_recovered_ack(pass: &replay::Pass, ack: &[u8]) -> anyhow::Result<()> {
    let mut ack = bytes::BytesMut::from(ack);
    let record = match fixed_framing::unpack::<proto::DiskRecord>(&mut ack) {
        Ok(fixed_framing::Frame::Record { message, .. }) if ack.is_empty() => message,
        _ => {
            return Err(anyhow::Error::new(crate::Failure::Invalid(
                "a recovered acknowledgement is not one framed record".to_string(),
            )));
        }
    };
    let uuid = uuid::Uuid::from_slice(&record.uuid).map_err(|err| {
        crate::Failure::Invalid(format!(
            "a recovered acknowledgement carries no message UUID: {err}"
        ))
    })?;
    let (producer, clock, flags) = uuid::parse(uuid).map_err(|err| {
        crate::Failure::Invalid(format!(
            "a recovered acknowledgement carries a malformed UUID: {err}"
        ))
    })?;

    crate::ensure_valid!(
        flags.is_ack()
            && record.chunks.is_empty()
            && !record.opens_horizon
            && record.installs_epoch.is_empty(),
        "a recovered acknowledgement is not an ACK_TXN record which carries nothing else",
    );
    crate::ensure_valid!(
        pass.can_acknowledge(producer, clock),
        "the recovered acknowledgement of {producer:?} at {clock:?} commits a delta which later \
         records of this journal displaced, or rolls back what they committed",
    );
    Ok(())
}

/// The journal itself, and everything a tenure needs to append to it.
///
/// One of these is carried from [`Opening::new`] through the claim and the
/// promotion into the actor which serves the disk. The epoch a tenure appends
/// under, and the offsets a broker has confirmed to it, are the same facts at
/// every one of those phases.
struct Journal {
    name: String,
    /// Claims, probes, and labels the journal. These are one-shot operations over
    /// the journal itself rather than appends of its content.
    client: gazette::journal::Client,
    /// Cancelled once the tenure is over, which stops this writer appending and
    /// gives up whatever broker call is in flight.
    ended: tokio_util::sync::CancellationToken,
    /// Value this tenure installs in the journal's `author` register, which every
    /// append then checks. It is also the producer of every record this tenure
    /// stamps, except a fence, which carries a fresh producer of its own.
    epoch: uuid::Producer,
    /// Write head the broker last confirmed, which every flush learns again.
    /// Together with `floor` it gives the range a recovery of this disk would
    /// have to read.
    head: i64,
    floor: i64,
}

impl Journal {
    /// Hand one complete record to the appender, which batches it with the
    /// records around it and appends under the epoch this tenure claimed.
    ///
    /// The checkpoint is where a device which writes faster than its broker
    /// accepts finally waits, and until then this costs no broker round trip at
    /// all. A wait here stops the writer taking mutations, which fills the
    /// capture channel and parks the device: that channel stays the one seam at
    /// which a workload is slowed down.
    ///
    /// A retry re-appends identical bytes. An append which landed but reported a
    /// failure therefore duplicates its records, and a reader de-duplicates those
    /// by UUID.
    async fn append_record(
        &mut self,
        appender: &mut publisher::Appender,
        record: &proto::DiskRecord,
    ) -> anyhow::Result<()> {
        fixed_framing::encode(record, &mut appender.buffer);

        until_ended(&self.ended, "appending", async {
            Ok(appender.checkpoint().await?)
        })
        .await
    }

    /// Append `ack` exactly as it was prepared, and await the broker's
    /// confirmation of it.
    ///
    /// It is flushed rather than checkpointed, because the client is told its
    /// delta is committed only once this has landed — and because nothing of the
    /// next delta may enter the appender ahead of it. See `writer::Task::taking`.
    async fn append_ack(
        &mut self,
        appender: &mut publisher::Appender,
        ack: &bytes::Bytes,
    ) -> anyhow::Result<()> {
        appender.buffer.extend_from_slice(ack);

        self.flush(appender).await
    }

    /// Append everything the appender holds, await the broker's confirmation of
    /// it, and take this journal's head from what that confirmation reports.
    ///
    /// A flush is the only point at which the broker has answered for everything
    /// this tenure appended, and so it is where the head is learned. A retry
    /// re-appends identical bytes, so the daemon cannot count what the journal
    /// holds from what it wrote, and the broker's confirmation is the only
    /// truthful head. It is learned often enough: the one decision which reads it
    /// — whether a delta opens a recovery horizon — runs behind a flush of its
    /// own.
    async fn flush(&mut self, appender: &mut publisher::Appender) -> anyhow::Result<()> {
        // Taken before the flush, so that the appends it starts are what satisfy
        // it, and awaited after, where it reports the response of the last of them.
        let barrier = appender.barrier();

        () = until_ended(&self.ended, "appending", async {
            Ok(appender.flush().await?)
        })
        .await?;

        // The barrier is a broker wait like the flush, and the tenure may end
        // under either of them.
        let response = until_ended(&self.ended, "appending", async { Ok(barrier.await?) }).await?;

        if let Some(commit) = &response.commit {
            self.head = commit.end;
        }
        Ok(())
    }

    /// Store `floor` as this journal's recovery floor, and carry on if it cannot
    /// be stored.
    ///
    /// A floor which is not stored costs a later replay the work of reading below
    /// it, and nothing else. Failing a tenure over that would trade a durable
    /// disk for a cheaper recovery, so this only warns. The next horizon stores a
    /// floor again, and a recovery which derives one stores it too.
    async fn store_floor(&self, floor: i64) {
        if let Err(err) = spec::advance_floor(&self.client, &self.name, floor).await {
            tracing::warn!(
                journal = self.name,
                floor,
                ?err,
                "could not store the disk's recovery floor",
            );
        }
    }
}

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

/// Generate a fresh random Gazette `Producer` identity.
///
/// A copy of runtime-next's `new_producer`, which this crate does not depend on.
fn random_producer() -> uuid::Producer {
    let mut producer: [u8; 6] = rand::random();
    producer[0] |= 0x01; // Set multicast bit (mark as not a real MAC address).
    uuid::Producer::from_bytes(producer)
}

fn uuid_bytes(producer: uuid::Producer, clock: uuid::Clock, flags: uuid::Flags) -> bytes::Bytes {
    bytes::Bytes::copy_from_slice(uuid::build(producer, clock, flags).as_bytes())
}
