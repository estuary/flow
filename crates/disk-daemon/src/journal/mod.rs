//! One tenure's journal: what it may be, how it is claimed, and how it is written.
//!
//! The files of this directory are:
//!
//! - `spec.rs` — the live journal specification a disk may be served from, the
//!   recovery-floor label the daemon stores on it, and the write barrier which
//!   confirms its head at `Open`.
//! - `writer.rs` — the task which appends a tenure's deltas, and the [`Writer`]
//!   handle its `Prepare` and `Acknowledge` reach it over.
//! - `ledger.rs` — `Ledger`, what that writer owes and holds, as data: its state,
//!   the prepared delta, record stamping, when a horizon is asked for and completes,
//!   and which failure a request is told. It does no I/O.
//! - `playback.rs` — the replay task, which runs from `Open` until the promotion
//!   stops it.
//! - `replay.rs` — the read, and the rules by which records rebuild a disk.
//! - `reassembly.rs` — `Reassembly`, the records of a read reassembled from the
//!   content it streams, and the content a broker skipped. It does no I/O.
//! - `sequencer.rs` — `Sequencer`, which delta a replay holds and what each record
//!   does to it: holding, displacing, and committing, and which acknowledgements can
//!   be honored. It does no I/O; `replay::Pass` carries out what it decides.
//! - `uncommitted.rs` — `UncommittedDelta`, the unacknowledged delta a replay holds rather than
//!   applies.
//!
//! This file is the phases a journal passes through. [`Standby`] is a journal which
//! is validated and being replayed, and which nothing has claimed. [`Claimed`] is one
//! this tenure holds while its replay finishes. [`Promoted`] is one whose replay is
//! done, and which a writer serves from the offsets it settled. [`Journal`] is what is
//! carried through all of them: the name, the client, the epoch, and the offsets a
//! broker has confirmed.
//!
//! Records reach the journal through the `publisher::Appender` which
//! `runtime-next` publishes its collection documents with. The writer hands it
//! one complete record at a time and takes the appender's checkpoint at that
//! boundary, and the appender decides when a batch of them becomes an append RPC. What is
//! particular to a disk stays here: claiming the journal and the recovery floor.
//! How the writer holds a delta and its acknowledgement apart is `writer.rs`'s.
//!
//! The journal's specification is the caller's, and so is the journal. `Open` names
//! one which must already exist, and the daemon validates its live spec rather than
//! creating or converging it. A name nothing has created is what the tenure asked
//! for and no retry of it can succeed, so it fails the tenure as invalid. The daemon
//! never creates a journal, never deletes one, and writes exactly one field of a spec
//! it did not create: the recovery-floor label.
//!
//! A tenure claims its journal exactly once, at [`Standby::claim_journal`], as a
//! Gazette recovery log's recorder fences: it appends a fence record under a request
//! which unions its epoch into the journal's `author` register (see [`author`]) and
//! checks nothing, and every append after it under a request which checks for that
//! epoch. A union replaces every value the journal held for a label it names, so the
//! fence displaces whichever author came before, and Gazette orders it with every
//! other append: an earlier writer's append ordered after it fails its author check.
//! So a writer serves only after its claim has landed.
//!
//! The fence decides nothing about who should write. A recovery log leaves that to
//! etcd's shard assignment, and a disk leaves it to its client: a client which
//! promotes two tenures of one disk at once has the later fence win, and the earlier
//! tenure fail at its next append. A displaced tenure cannot take its journal back,
//! because it never claims again. Its client opens a new tenure, which is a new
//! claimant.
//!
//! The register is not commit authority. Etcd can lose register state independently
//! of journal contents, and an empty register set matches any selector, so a journal
//! whose registers were lost is writable again while its committed records stay
//! authoritative.

use crate::failure;
use crate::image::Image;
use crate::proto;
use anyhow::Context;
use proto_gazette::{broker, fixed_framing, uuid};

mod ledger;
mod reassembly;
mod sequencer;
mod spec;
mod writer;

pub mod playback;
pub mod replay;
pub mod uncommitted;

use spec::{Resolved, list, resolve};
pub use writer::Writer;

/// A tenure's journal before its disk exists.
///
/// Recovery is a step of its own. A disk with committed state must be rebuilt
/// before a device can be created over it, and its journal must be claimed before
/// it is read.
pub struct Standby {
    journal: Journal,
    appender: publisher::Appender,
}

/// A journal this tenure holds, while the replay of it finishes.
///
/// The claim is a step of its own because it bounds a replay which is still
/// running: see [`Standby::claim_journal`]. [`Claimed::promote`] then finishes that
/// replay.
pub struct Claimed {
    journal: Journal,
    appender: publisher::Appender,
}

/// A journal this tenure holds and has replayed, which [`Promoted::serve`] appends
/// the disk's deltas to.
///
/// Its offsets are the ones the promotion settled: the head its replay read to, the
/// floor that replay derived, and the horizon it left open.
pub struct Promoted {
    journal: Journal,
    appender: publisher::Appender,
    /// Offset of a horizon the replay left open, which the writer completes rather
    /// than opening one of its own. See [`Recovered::horizon`].
    horizon: Option<i64>,
}

/// What a recovery of a disk found in its journal.
pub struct Recovered {
    /// The disk's committed state, rebuilt.
    pub image: Image,
    /// Whether the journal held no committed state. A fresh disk's filesystem is the
    /// caller's to format.
    pub fresh: bool,
    /// Blocks a horizon the replay left open still owes a copy, which the disk's
    /// owner resumes rather than opening a horizon of its own over whatever it
    /// finds allocated.
    ///
    /// This is `Some` exactly when [`Promoted`]'s horizon offset is. The two are the
    /// halves of one horizon, held apart because the offset is the writer's and the
    /// blocks are the owner's: the writer completes the horizon at that offset once
    /// the owner reports these blocks discharged.
    pub horizon: Option<crate::horizon::Horizon>,
}

impl Standby {
    /// Open `journal`, and read the head and recovery floor a replay of it starts
    /// from.
    ///
    /// Nothing is created here, and nothing is appended beyond a zero-byte write
    /// barrier.
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

        let Resolved {
            head,
            floor,
            marked,
        } = resolve(&client, &journal, &ended).await?;

        // Nothing is appended before the claim, which is what sets the registers this
        // appender's requests carry, as a recovery log's recorder checks none until its
        // fence: see [`Standby::claim_journal`].
        let epoch = random_producer();
        let appender = publisher::Appender::new(client.clone(), journal.clone());

        Ok(Self {
            journal: Journal {
                name: journal,
                client,
                ended,
                epoch,
                head,
                floor,
                marked,
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
    /// what is rebuilt. One at or above the head was refused at `Open`.
    ///
    /// This does not claim the journal, so another tenure may still be writing it.
    /// That is what makes a standby possible, and [`Standby::claim_journal`] is what
    /// makes it safe.
    pub fn play(
        &self,
        image: Image,
        uncommitted: uncommitted::UncommittedDelta,
    ) -> playback::Playback {
        let reading = playback::Reading {
            client: self.journal.client.clone(),
            journal: self.journal.name.clone(),
            ended: self.journal.ended.clone(),
        };

        playback::Playback::start(
            reading,
            self.journal.floor,
            self.journal.head,
            image,
            uncommitted,
        )
    }

    /// Take the journal from whoever holds it now, so that its head stops moving.
    ///
    /// This is the first half of a promotion, and it is separate because it does not
    /// wait for the replay. A promotion which fences before the replay is current
    /// bounds that replay: a head no other writer can move is one the replay
    /// converges on rather than chases.
    ///
    /// A recovery log's player instead reads to the head before it injects its
    /// handoff, because that no-op must chain onto the log as read so far. A fence
    /// installs an epoch and chains onto nothing, so it can come first.
    ///
    /// The journal is listed again here, before the fence, so that a spec this
    /// tenure would refuse is refused before it fences anyone: see [`spec::list`].
    ///
    /// Every journal is claimed, whether or not it holds anything. A tenure serves a
    /// disk only behind its claim, and a fresh disk's own `mkfs` is a delta like any
    /// other, so there is no disk a tenure may write without first excluding whoever
    /// wrote the journal before it.
    ///
    /// The claim is an append through the tenure's own appender, as a recovery log's
    /// recorder appends its fence through the same `AppendService` as everything
    /// after it. The fence is written under a request which unions this tenure's
    /// [`author`] into the journal's registers and checks nothing, and every
    /// write after it under one which checks for that author. A failure of the
    /// fence's append fails the tenure, as a failed handoff fails a recovery log's
    /// playback.
    ///
    /// A tenure claims once. The fence is alone in its append, so the head its flush
    /// learns is the one just past it, which [`Claimed::promote`] reads through.
    pub async fn claim_journal(self) -> anyhow::Result<Claimed> {
        let Self {
            mut journal,
            mut appender,
        } = self;

        let listed = list(&journal.client, &journal.name, &journal.ended).await?;
        journal.marked = listed.marked;

        let author = author(journal.epoch);

        () = appender
            .set_request(broker::AppendRequest {
                journal: journal.name.clone(),
                union_registers: Some(author.clone()),
                ..Default::default()
            })
            .await?;

        // The fence installs the epoch, but its own producer differs from it. Were it
        // the epoch, the fence's wall-clock stamp would become that producer's last
        // commit, and a clock which stepped back before the tenure's first record would
        // have replay drop that record as a duplicate. A transient retry re-sends these
        // same bytes, and replay skips a repeat which landed as a duplicate of its UUID.
        let fence = proto::DiskRecord {
            uuid: uuid_bytes(
                random_producer(),
                uuid::Clock::from_time(std::time::SystemTime::now()),
                uuid::Flags::OUTSIDE_TXN,
            ),
            installs_epoch: bytes::Bytes::copy_from_slice(journal.epoch.as_bytes()),
            ..Default::default()
        };
        fixed_framing::encode(&fence, &mut appender.buffer);

        () = journal
            .flush(&mut appender)
            .await
            .with_context(|| format!("claiming {}", journal.name))?;

        () = appender
            .set_request(broker::AppendRequest {
                journal: journal.name.clone(),
                check_registers: Some(broker::LabelSelector {
                    include: Some(author),
                    exclude: None,
                }),
                ..Default::default()
            })
            .await?;

        // A standby, and above all one parked over an empty journal, reads nothing of
        // a head which regressed while it waited. Only this listing and the fence's
        // head can show it.
        () = spec::check_floor(&journal.name, &listed, journal.head)?;

        Ok(Claimed { journal, appender })
    }
}

impl Claimed {
    /// Stop `playback` at the head, finish the replay at the fenced head, and report
    /// what it rebuilt. Each of `recovered_acks` which commits the delta the replay
    /// holds is then appended. One the journal already committed is not: see
    /// [`sequencer::RecoveredAck`].
    ///
    /// [`Standby::claim_journal`] runs first and holds the journal, so nobody else can
    /// append past what this reads.
    ///
    /// Fresh and recovered are told apart by what the replay applied, and not by
    /// what the journal holds: a journal of nothing but fences and orphaned deltas
    /// is a disk which was never committed. Two things say a disk has committed
    /// state whatever its fragments hold: a recovered acknowledgement, which a
    /// client holds only for a delta whose data appends a broker confirmed, and the
    /// journal's floor label, which the writer stores once a delta first commits. A
    /// replay which applies nothing alongside either is committed state which was
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
        self,
        playback: playback::Playback,
        recovered_acks: Vec<bytes::Bytes>,
    ) -> anyhow::Result<(Promoted, Recovered)> {
        let Self {
            mut journal,
            mut appender,
        } = self;
        let playback::Handoff {
            mut image,
            mut pass,
            applied,
        } = playback.stop().await?;

        // The head is where the claim's fence committed, as a broker which had just
        // served that append confirmed it. Its index therefore covers every fragment
        // below it, and only this epoch, or a later fence which displaces it, may
        // append past it. That fixes the end of this read and makes it fresh.
        let mut head = journal.head;

        () = until_ended(&journal.ended, "promoting", async {
            _ = replay::read(
                &journal.client,
                &journal.name,
                applied,
                replay::Extent::Bounded(head),
                &mut image,
                &mut pass,
            )
            .await?;

            anyhow::Ok(())
        })
        .await?;

        // The pass has sequenced every delta any writer appended, so it can say what
        // each recovered acknowledgement does. Only one which commits the delta held is
        // appended. One the journal already committed has nothing left to do, and any
        // other would fail this replay and every later one, so it is refused.
        let mut repairs = Vec::new();

        for ack in &recovered_acks {
            let (producer, clock) = check_recovered_ack(ack, &journal.name)?;

            match pass.sequencer().recovered_ack(producer, clock) {
                sequencer::RecoveredAck::Commits => repairs.push(ack.clone()),
                sequencer::RecoveredAck::Committed { older: false } => (),
                sequencer::RecoveredAck::Committed { older: true } => tracing::warn!(
                    journal = journal.name,
                    ?producer,
                    ?clock,
                    "a recovered acknowledgement is older than its writer's last commit, \
                     so the client's own state is behind the disk",
                ),
                sequencer::RecoveredAck::Refused => {
                    return Err(anyhow::Error::new(failure::Failure::Invalid(format!(
                        "the recovered acknowledgement of {producer:?} at {clock:?} commits \
                         a delta which journal {} does not hold whole",
                        journal.name,
                    ))));
                }
            }
        }

        if !repairs.is_empty() {
            for ack in repairs {
                () = journal
                    .append_ack(&mut appender, &ack)
                    .await
                    .context("repairing a recovered acknowledgement")?;
            }
            // The acknowledgements landed at `head`, and the flush which appended them
            // learned the head beyond them. They are read back through the same pass,
            // which is how it learns they committed what it held.
            let repaired_head = journal.head;

            () = until_ended(&journal.ended, "promoting", async {
                _ = replay::read(
                    &journal.client,
                    &journal.name,
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
        let (uncommitted, _floor, opened) = pass.into_parts();

        let (horizon_at, horizon) = match opened {
            Some(replay::OpenHorizon { at, blocks }) => (Some(at), Some(blocks)),
            None => (None, None),
        };

        if !uncommitted.is_empty() {
            tracing::info!(
                journal = journal.name,
                bytes = uncommitted.len(),
                "dropping the delta which the journal never acknowledged",
            );
        }

        tracing::info!(
            journal = journal.name,
            head,
            chunks,
            floor = ?derived,
            horizon = ?horizon_at,
            "replayed a disk from its journal",
        );

        // A recovered acknowledgement proves a broker confirmed this disk's data
        // appends, and the floor label proves a delta of it committed. A replay which
        // applied nothing alongside either means those records were destroyed, even
        // though the journal outlived them. The acknowledged records are the newest
        // the disk has, so no floor this daemon stored can seek past them.
        failure::ensure_valid!(
            chunks != 0 || (recovered_acks.is_empty() && !journal.marked),
            "a replay of journal {} applied nothing, but {}: its committed state was \
             destroyed",
            journal.name,
            if recovered_acks.is_empty() {
                "its recovery-floor label says the disk committed state"
            } else {
                "the tenure supplied recovered acknowledgements"
            },
        );

        journal.head = head;
        journal.floor = derived.unwrap_or(journal.floor);

        // A floor the replay derived is one an earlier tenure completed a horizon
        // for but could not store, because it died before it could. This tenure
        // stores it on that tenure's behalf, which is what makes the scheme
        // self-healing.
        if let Some(derived) = derived {
            () = journal.store_floor(derived).await;
        }

        Ok((
            Promoted {
                journal,
                appender,
                horizon: horizon_at,
            },
            Recovered {
                image,
                fresh: chunks == 0,
                horizon,
            },
        ))
    }
}

/// Refuse a recovered acknowledgement which is not one framed `ACK_TXN` record of
/// `journal` that carries nothing else, and report whose it is and its clock.
///
/// An acknowledgement names the journal of the disk it commits a delta of, so one a
/// client recovered for some other disk is refused rather than judged against this
/// journal's writers, none of which it belongs to.
fn check_recovered_ack(ack: &[u8], journal: &str) -> anyhow::Result<(uuid::Producer, uuid::Clock)> {
    let mut ack = bytes::BytesMut::from(ack);
    let record = match fixed_framing::unpack::<proto::DiskRecord>(&mut ack) {
        Ok(fixed_framing::Frame::Record { message, .. }) if ack.is_empty() => message,
        _ => {
            return Err(anyhow::Error::new(failure::Failure::Invalid(
                "a recovered acknowledgement is not one framed record".to_string(),
            )));
        }
    };
    let uuid = uuid::Uuid::from_slice(&record.uuid).map_err(|err| {
        failure::Failure::Invalid(format!(
            "a recovered acknowledgement carries no message UUID: {err}"
        ))
    })?;
    let (producer, clock, flags) = uuid::parse(uuid).map_err(|err| {
        failure::Failure::Invalid(format!(
            "a recovered acknowledgement carries a malformed UUID: {err}"
        ))
    })?;

    failure::ensure_valid!(
        flags.is_ack()
            && record.chunks.is_empty()
            && !record.opens_horizon
            && record.installs_epoch.is_empty(),
        "a recovered acknowledgement is not an ACK_TXN record which carries nothing else",
    );
    failure::ensure_valid!(
        record.journal == journal,
        "a recovered acknowledgement of journal {:?} was handed to journal {journal}",
        record.journal,
    );
    Ok((producer, clock))
}

/// The journal itself, and everything a tenure needs to append to it.
///
/// One of these is carried from [`Standby::new`] through the claim and the
/// promotion into the writer task which serves the disk. The epoch a tenure appends
/// under, and the offsets a broker has confirmed to it, are the same facts at
/// every one of those phases.
struct Journal {
    name: String,
    /// Claims, lists, and labels the journal. These are one-shot operations over
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
    /// Whether the journal carries a recovery-floor label, which says the disk has
    /// committed state. The writer stores one once a delta first commits.
    marked: bool,
}

impl Journal {
    /// Hand one complete record to the appender, which batches it with the
    /// records around it and appends under the epoch this tenure claimed.
    ///
    /// The appender's checkpoint is where a device which writes faster than its broker
    /// accepts finally waits, and until then this costs no broker round trip at
    /// all. A wait here stops the writer taking mutations, which fills the
    /// recording channel and parks the device: that channel stays the one seam at
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
    /// The appender is flushed here rather than checkpointed, because the client is told its
    /// delta is committed only once this has landed — and because nothing of the
    /// next delta may enter the appender ahead of it. See `ledger::Ledger::taking`.
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
    async fn store_floor(&mut self, floor: i64) {
        match spec::advance_floor(&self.client, &self.name, floor).await {
            Ok(()) => self.marked = true,
            Err(err) => tracing::warn!(
                journal = self.name,
                floor,
                ?err,
                "could not store the disk's recovery floor",
            ),
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
        Err(anyhow::Error::new(failure::Failure::Ended(format!(
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

/// Registers which name `epoch` as a journal's `author`, as Go's `Author.Fence()`
/// returns them. The value is the epoch's producer in hex. A claim unions them in,
/// and every append after it checks them.
pub(crate) fn author(epoch: uuid::Producer) -> broker::LabelSet {
    let value: String = epoch
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();

    labels::build_set([("author", value.as_str())])
}

#[cfg(test)]
mod test {
    use super::{author, check_recovered_ack};
    use crate::{failure, proto};
    use proto_gazette::{fixed_framing, uuid};

    const JOURNAL: &str = "acmeCo/disk/one";

    fn producer(seed: u8) -> uuid::Producer {
        uuid::Producer::from_bytes([seed | 0x01, 0, 0, 0, 0, seed])
    }

    /// A clock `ticks` microseconds after the epoch.
    fn clock(ticks: u64) -> uuid::Clock {
        let mut clock = uuid::Clock::UNIX_EPOCH;
        for _ in 0..ticks {
            _ = clock.tick();
        }
        clock
    }

    fn record(seed: u8, ticks: u64, flags: uuid::Flags) -> proto::DiskRecord {
        proto::DiskRecord {
            uuid: super::uuid_bytes(producer(seed), clock(ticks), flags),
            ..Default::default()
        }
    }

    fn frame(records: &[proto::DiskRecord]) -> bytes::Bytes {
        let mut buf = bytes::BytesMut::new();
        for record in records {
            fixed_framing::encode(record, &mut buf);
        }
        buf.freeze()
    }

    #[test]
    fn test_an_epoch_is_its_author_register_in_hex() {
        let author = author(uuid::Producer([0x01, 0x23, 0x45, 0x67, 0x89, 0xab]));

        assert_eq!(author.labels.len(), 1);
        assert_eq!(author.labels[0].name, "author");
        assert_eq!(author.labels[0].value, "0123456789ab");
    }

    /// A recovered acknowledgement is judged against the journal only if it is exactly
    /// one framed `ACK_TXN` record of that journal which carries nothing else.
    /// Anything else is refused as invalid. Whether the journal already committed it,
    /// or it commits the delta a replay holds, is the sequencer's to say.
    #[test]
    fn test_a_recovered_acknowledgement_is_one_ack_of_this_journal() {
        use uuid::Flags;

        let ack = proto::DiskRecord {
            journal: JOURNAL.to_string(),
            ..record(0x10, 2, Flags::ACK_TXN)
        };

        let cases: [(&str, bytes::Bytes); 10] = [
            ("an acknowledgement", frame(std::slice::from_ref(&ack))),
            (
                "another journal's",
                frame(&[proto::DiskRecord {
                    journal: "acmeCo/disk/two".to_string(),
                    ..ack.clone()
                }]),
            ),
            (
                "one naming no journal",
                frame(&[proto::DiskRecord {
                    journal: String::new(),
                    ..ack.clone()
                }]),
            ),
            ("unframed bytes", bytes::Bytes::from_static(b"not a record")),
            ("two records", frame(&[ack.clone(), ack.clone()])),
            ("no UUID", frame(&[proto::DiskRecord::default()])),
            (
                "a malformed UUID",
                frame(&[proto::DiskRecord {
                    uuid: bytes::Bytes::from_static(&[0; 16]),
                    ..ack.clone()
                }]),
            ),
            (
                "a data record",
                frame(&[proto::DiskRecord {
                    journal: JOURNAL.to_string(),
                    ..record(0x10, 2, Flags::CONTINUE_TXN)
                }]),
            ),
            (
                "one carrying chunks",
                frame(&[proto::DiskRecord {
                    chunks: crate::chunk::encode_write(0, &bytes::Bytes::from(vec![1; 4096])),
                    ..ack.clone()
                }]),
            ),
            (
                "one opening a horizon",
                frame(&[proto::DiskRecord {
                    opens_horizon: true,
                    ..ack.clone()
                }]),
            ),
        ];

        let mut out = Vec::new();
        for (what, ack) in cases {
            let outcome = match check_recovered_ack(&ack, JOURNAL) {
                Ok((producer, clock)) => format!("judged, of {producer:?} at {clock:?}"),
                Err(err) => {
                    assert!(
                        matches!(
                            err.downcast_ref::<failure::Failure>(),
                            Some(failure::Failure::Invalid(_))
                        ),
                        "{what} was refused as other than invalid: {err:#}",
                    );
                    format!("refused: {err:#}")
                }
            };
            out.push(format!("{what:<22}{outcome}"));
        }
        insta::assert_snapshot!(out.join("\n"), @r#"
        an acknowledgement    judged, of Producer(11:00:00:00:00:10) at Clock(0s 2000ns)
        another journal's     refused: a recovered acknowledgement of journal "acmeCo/disk/two" was handed to journal acmeCo/disk/one
        one naming no journal refused: a recovered acknowledgement of journal "" was handed to journal acmeCo/disk/one
        unframed bytes        refused: a recovered acknowledgement is not one framed record
        two records           refused: a recovered acknowledgement is not one framed record
        no UUID               refused: a recovered acknowledgement carries no message UUID: invalid length: expected 16 bytes, found 0
        a malformed UUID      refused: a recovered acknowledgement carries a malformed UUID: UUID 00000000-0000-0000-0000-000000000000 is not a V1 UUID
        a data record         refused: a recovered acknowledgement is not an ACK_TXN record which carries nothing else
        one carrying chunks   refused: a recovered acknowledgement is not an ACK_TXN record which carries nothing else
        one opening a horizon refused: a recovered acknowledgement is not an ACK_TXN record which carries nothing else
        "#);
    }
}

#[cfg(test)]
mod broker_test {
    //! The writer against a real broker.
    //!
    //! These are the scenarios which are about what reaches a journal: which records a
    //! delta appends, in what order, under whose claim, and what a replay of them
    //! rebuilds. A data plane is expensive to start, so one test drives them all, each
    //! over a journal of its own, and holds each scenario's [`Recorder`] for the length
    //! of that scenario — dropping it closes the recording channel, which is how a
    //! tenure ends.
    //!
    //! What a client sees of all this is `tests/`, over the daemon as it ships.

    use crate::BLOCK_SIZE;
    use crate::chunk::{covered_blocks, encode_punch, encode_write};
    use crate::proto;
    use crate::recording::Recorder;
    use crate::test_support::broker::Fixture;
    use proto_gazette::broker;

    #[tokio::test]
    async fn test_a_writer_over_a_real_broker() {
        let fixture = Fixture::start().await;

        first_use_claims_the_journal(&fixture).await;
        a_replacement_writer_fences_the_first(&fixture).await;
        a_tenure_which_never_prepares_appends_only_its_fence(&fixture).await;
        a_repeated_fence_replays_as_one(&fixture).await;
        a_committed_delta_reads_back_as_its_chunks(&fixture).await;
        mutations_offered_while_a_commit_is_outstanding_wait_for_it(&fixture).await;
        a_large_delta_carries_one_record_per_mutation(&fixture).await;
        a_delta_which_spans_several_appends_keeps_its_records(&fixture).await;
        a_horizon_records_its_openers_own_offset(&fixture).await;
        an_abandoned_tenure_answers_its_client(&fixture).await;
        an_absent_journal_is_refused_at_open(&fixture).await;
        an_unrecoverable_journal_never_opens(&fixture).await;
        recovery_applies_only_committed_deltas(&fixture).await;
        a_recovered_acknowledgement_is_repaired(&fixture).await;
        a_stale_recovered_acknowledgement_is_refused(&fixture).await;
        an_orphaned_journal_recovers_nothing(&fixture).await;
        an_acknowledgement_which_landed_before_a_pending_delta_is_recovered(&fixture).await;
        a_first_commit_marks_the_floor_label(&fixture).await;
        a_marked_journal_without_committed_state_is_refused(&fixture).await;

        fixture.stop().await;
    }

    /// First use claims the journal. It installs the author register with a fence
    /// record, then appends its delta under that claim.
    async fn first_use_claims_the_journal(fixture: &Fixture) {
        let journal = "acmeCo/disk/first-use";
        let (mut recorder, writer) = fixture.open(journal).await.unwrap();

        recorder.reserve().unwrap().send(vec![encode_punch(3, 2)]);
        let ack = writer
            .prepare()
            .await
            .unwrap()
            .expect("the delta is not empty");
        () = writer.acknowledge(ack).await.unwrap();

        let records = fixture.read(journal).await;
        assert_eq!(records.len(), 3);

        let (producer, _clock, flags) = records[0].0;
        assert!(flags.is_outside(), "a fence is outside a transaction");
        assert_ne!(
            producer,
            writer.epoch(),
            "a fence has a producer of its own"
        );
        assert_eq!(records[0].1.installs_epoch, writer.epoch().as_bytes()[..]);
        assert!(records[0].1.chunks.is_empty());

        // A tenure stamps its delta and that delta's acknowledgement with its epoch,
        // which is also the value it installed in the author register.
        let (producer, _clock, flags) = records[1].0;
        assert!(flags.is_continue());
        assert_eq!(producer, writer.epoch());
        assert_eq!(records[1].1.chunks, vec![encode_punch(3, 2)]);

        let (producer, _clock, flags) = records[2].0;
        assert!(flags.is_ack());
        assert_eq!(producer, writer.epoch());
        assert!(records[2].1.chunks.is_empty());

        assert_eq!(
            fixture.author(journal).await.as_deref(),
            Some(super::author(writer.epoch()).labels[0].value.as_str()),
        );
    }

    /// A replacement tenure takes the author register. The first tenure then cannot
    /// append.
    async fn a_replacement_writer_fences_the_first(fixture: &Fixture) {
        let journal = "acmeCo/disk/contended";
        let (mut first_recorder, first) = fixture.open(journal).await.unwrap();

        first_recorder
            .reserve()
            .unwrap()
            .send(vec![encode_punch(0, 1)]);
        let ack = first.prepare().await.unwrap().unwrap();
        () = first.acknowledge(ack).await.unwrap();

        // The journal now holds a committed delta, so a replacement claims it as that
        // replacement recovers.
        let (_second_recorder, second, _blocks) =
            fixture.recover(journal, Vec::new()).await.unwrap();
        assert_ne!(first.epoch(), second.epoch());

        first_recorder
            .reserve()
            .unwrap()
            .send(vec![encode_punch(1, 1)]);
        let err = first.prepare().await.unwrap_err();

        // The broker's own error survives the appender, because a lost fence is
        // `ABORTED` to the client and everything else is not. A message which
        // merely mentioned the status would classify as `INTERNAL`.
        let cause = err
            .chain()
            .find_map(|cause| cause.downcast_ref::<gazette::Error>())
            .unwrap_or_else(|| panic!("expected a fenced-out append, got: {err:#}"));

        assert!(
            matches!(
                cause,
                gazette::Error::BrokerStatus(broker::Status::RegisterMismatch),
            ),
            "expected a fenced-out append, got: {err:#}",
        );
        // Every later request reports the failure which ended the tenure.
        let err = first.prepare().await.unwrap_err();
        assert!(format!("{err:#}").contains("tenure has failed"), "{err:#}");
    }

    /// A tenure which prepares nothing leaves its fence behind and nothing else. The
    /// claim is what makes it the journal's writer, and it happens whether or not the
    /// disk is ever written.
    async fn a_tenure_which_never_prepares_appends_only_its_fence(fixture: &Fixture) {
        let journal = "acmeCo/disk/untouched";
        let (recorder, writer) = fixture.open(journal).await.unwrap();

        let epoch = writer.epoch();
        assert_eq!(writer.prepare().await.unwrap(), None);
        drop((recorder, writer));

        let records = fixture.read(journal).await;
        assert_eq!(records.len(), 1, "{records:?}");

        let (_producer, _clock, flags) = records[0].0;
        assert!(flags.is_outside(), "a fence is outside a transaction");
        assert_eq!(records[0].1.installs_epoch, epoch.as_bytes()[..]);
    }

    /// A fence which lands twice, as a transient retry of one which had already landed
    /// does, is harmless. The epoch holds the journal, and a later replay skips the
    /// repeated fence as a duplicate.
    async fn a_repeated_fence_replays_as_one(fixture: &Fixture) {
        let journal = "acmeCo/disk/repeated-fence";
        () = fixture.create_journal(fixture.spec(journal)).await.unwrap();

        let epoch = super::random_producer();
        let fence = proto::DiskRecord {
            uuid: super::uuid_bytes(
                super::random_producer(),
                proto_gazette::uuid::Clock::from_time(std::time::SystemTime::now()),
                proto_gazette::uuid::Flags::OUTSIDE_TXN,
            ),
            installs_epoch: bytes::Bytes::copy_from_slice(epoch.as_bytes()),
            ..Default::default()
        };

        let first = fixture.append_fence(journal, epoch, &fence).await;
        assert_eq!(first, fixture.head(journal).await);

        let second = fixture.append_fence(journal, epoch, &fence).await;
        assert_eq!(second, fixture.head(journal).await);
        assert_eq!(second, 2 * first, "the same fence, appended again");

        assert_eq!(
            fixture.author(journal).await.as_deref(),
            Some(super::author(epoch).labels[0].value.as_str()),
        );

        // The journal holds two copies of one fence and nothing else, so a replay of
        // it is a fresh disk.
        let (_promoted, blocks) = fixture.promote(journal, Vec::new()).await.unwrap();
        assert_eq!(blocks, Vec::new());
    }

    /// A committed delta reads back as exactly the chunks which were recorded.
    async fn a_committed_delta_reads_back_as_its_chunks(fixture: &Fixture) {
        let journal = "acmeCo/disk/delta";
        let (mut recorder, writer) = fixture.open(journal).await.unwrap();

        let mutations = vec![
            encode_write(0, &bytes::Bytes::from(vec![0x11; 8192])),
            encode_write(2, &bytes::Bytes::from(vec![0; 4096])),
            vec![encode_punch(3, 4)],
        ];
        for mutation in &mutations {
            recorder.reserve().unwrap().send(mutation.clone());
        }

        let ack = writer.prepare().await.unwrap().unwrap();
        () = writer.acknowledge(ack.clone()).await.unwrap();

        let chunks: Vec<_> = fixture
            .read(journal)
            .await
            .into_iter()
            .flat_map(|(_uuid, record)| record.chunks)
            .collect();

        assert_eq!(chunks, mutations.concat());

        // A second commit is a protocol violation. The delta it acknowledged is
        // already committed.
        let err = writer.acknowledge(ack).await.unwrap_err();
        assert!(format!("{err:#}").contains("no prepared delta"), "{err:#}");
    }

    /// The writer takes no mutation while a client holds an acknowledgement. Those
    /// mutations would be records of the same producer as the acknowledgement, and
    /// Gazette's sequencer drops such records when they land ahead of it. They wait in
    /// the recording channel instead, and the writer takes them once the acknowledgement
    /// has landed, so the journal holds the second delta above the first delta's commit
    /// and a recovery of it holds both.
    async fn mutations_offered_while_a_commit_is_outstanding_wait_for_it(fixture: &Fixture) {
        let journal = "acmeCo/disk/outstanding";
        let (mut recorder, writer) = fixture.open(journal).await.unwrap();

        let epoch = writer.epoch();

        recorder.reserve().unwrap().send(write(1, 0xaa));
        let first = writer.prepare().await.unwrap().unwrap();

        // Mutations of the second delta, offered while the client holds the first
        // delta's acknowledgement. The writer leaves them in the channel.
        recorder.reserve().unwrap().send(write(2, 0xbb));
        recorder.reserve().unwrap().send(write(3, 0xcc));
        () = tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !recorder.is_empty(),
            "the writer took a mutation while an acknowledgement was outstanding",
        );

        () = writer.acknowledge(first).await.unwrap();
        () = drains(&recorder).await;

        let second = writer.prepare().await.unwrap().unwrap();
        () = writer.acknowledge(second).await.unwrap();
        drop((recorder, writer));

        // The journal reads back as: fence, delta one, its acknowledgement, delta two,
        // its acknowledgement. Nothing of delta two precedes delta one's commit, and
        // every record carries the tenure's epoch.
        let records = fixture.read(journal).await;
        let shape: Vec<_> = records
            .iter()
            .map(|((_producer, _clock, flags), _record)| {
                match (flags.is_outside(), flags.is_continue()) {
                    (true, _) => "fence",
                    (_, true) => "continue",
                    _ => "ack",
                }
            })
            .collect();

        assert_eq!(
            shape,
            vec!["fence", "continue", "ack", "continue", "continue", "ack"],
        );
        assert_eq!(records[3].1.chunks, write(2, 0xbb));
        assert_eq!(records[4].1.chunks, write(3, 0xcc));

        for ((producer, _clock, _flags), _record) in &records[1..] {
            assert_eq!(*producer, epoch);
        }

        let (_recorder, _writer, blocks) = fixture.recover(journal, Vec::new()).await.unwrap();
        assert_eq!(blocks, vec![(1, 0xaa), (2, 0xbb), (3, 0xcc)]);
    }

    /// A delta of many mutations carries exactly one record per mutation. Those records
    /// cover exactly the blocks the mutations wrote.
    async fn a_large_delta_carries_one_record_per_mutation(fixture: &Fixture) {
        let journal = "acmeCo/disk/bounded";
        const WRITES: usize = 8;

        let (mut recorder, writer) = fixture.open(journal).await.unwrap();
        let write = encode_write(0, &bytes::Bytes::from(vec![0x22; 128 * 1024]));

        for _ in 0..WRITES {
            recorder.reserve().unwrap().send(write.clone());
        }

        let ack = writer.prepare().await.unwrap().unwrap();
        () = writer.acknowledge(ack).await.unwrap();

        let records = fixture.read(journal).await;
        let mut blocks = Vec::new();
        let mut carrying = 0;

        for (_uuid, decoded) in &records {
            carrying += usize::from(!decoded.chunks.is_empty());
            blocks.extend(decoded.chunks.iter().flat_map(covered_blocks));
        }

        // Nothing splits a mutation, so each write is exactly one record.
        assert_eq!(carrying, WRITES);
        assert_eq!(
            blocks,
            std::iter::repeat_with(|| 0u32..32)
                .take(WRITES)
                .flatten()
                .collect::<Vec<_>>(),
        );
    }

    /// A delta larger than the appender's buffer threshold is appended in several
    /// RPCs, and the boundary between them changes nothing of what the journal
    /// holds: one record per mutation, in the order they were recorded, and a
    /// recovery which rebuilds the disk from them.
    async fn a_delta_which_spans_several_appends_keeps_its_records(fixture: &Fixture) {
        let journal = "acmeCo/disk/batched";

        // Enough of the disk, enough times over, to carry the delta past the
        // threshold at which the appender stops buffering and waits.
        const MUTATIONS: usize = 40;
        let each = crate::test_support::broker::BLOCKS as usize * BLOCK_SIZE as usize;
        assert!(MUTATIONS * each > publisher::Appender::BUFFER_FLUSH_THRESHOLD);

        let (mut recorder, writer) = fixture.open(journal).await.unwrap();

        for index in 0..MUTATIONS {
            let fill = (index + 1) as u8;
            recorder
                .reserve()
                .unwrap()
                .send(encode_write(0, &bytes::Bytes::from(vec![fill; each])));
        }

        let ack = writer.prepare().await.unwrap().unwrap();
        () = writer.acknowledge(ack).await.unwrap();
        drop((recorder, writer));

        // Each mutation is one record, and their order is the order they were
        // recorded in: the fill byte of each identifies which.
        let fills: Vec<u8> = fixture
            .read(journal)
            .await
            .into_iter()
            .filter_map(
                |(_uuid, decoded)| match decoded.chunks.first()?.content.as_ref()? {
                    proto::chunk::Content::Data(data) => Some(data[0]),
                    proto::chunk::Content::Punch(_) => None,
                },
            )
            .collect();

        assert_eq!(
            fills,
            (1..=MUTATIONS as u8).collect::<Vec<_>>(),
            "records were split, merged, or reordered across appends",
        );

        // The journal is past the threshold, so more than one append built it.
        assert!(fixture.head(journal).await as usize > publisher::Appender::BUFFER_FLUSH_THRESHOLD);

        let (_recorder, _writer, blocks) = fixture.recover(journal, Vec::new()).await.unwrap();

        assert_eq!(
            blocks,
            (0..crate::test_support::broker::BLOCKS)
                .map(|block| (block, MUTATIONS as u8))
                .collect::<Vec<_>>(),
            "the last mutation of the delta is what the disk holds",
        );
    }

    /// A horizon's offset is where its opening record begins, and stays exact
    /// where a delta is batched into several appends: the opening record is
    /// appended alone, so the broker's own `begin` for it is the horizon.
    ///
    /// The writer's primitives are driven directly, because opening a horizon
    /// otherwise needs a compactor, and a compactor needs a real device.
    async fn a_horizon_records_its_openers_own_offset(fixture: &Fixture) {
        let journal = "acmeCo/disk/horizon-offset";

        // Every append checks the epoch the claim installs, so the journal is promoted
        // as a tenure's is even though the scenario drives the writer's primitives
        // directly.
        let (promoted, _blocks) = fixture.promote(journal, Vec::new()).await.unwrap();

        // The writer's task, but not spawned: this drives its primitives itself.
        let mut task = promoted.into_task(None);

        let each = crate::test_support::broker::BLOCKS as usize * BLOCK_SIZE as usize;
        let write = || encode_write(0, &bytes::Bytes::from(vec![0x55; each]));

        // Records ahead of the horizon, batched by the appender.
        for _ in 0..8 {
            () = task.append_mutation(write(), false).await.unwrap();
        }
        () = task.flush().await.unwrap();

        // Cut, so that the next record is a delta's first and opens a horizon. The
        // cut's acknowledgement need not reach the journal for what this checks.
        let ack = task.ledger.cut(std::time::SystemTime::now(), false);
        _ = task.ledger.begin_acknowledge(false, &ack).unwrap();

        () = task.append_mutation(write(), true).await.unwrap();
        let horizon = task.ledger.horizon().expect("a horizon was opened");

        // More records behind it, which must not move what it recorded.
        for _ in 0..8 {
            () = task.append_mutation(write(), false).await.unwrap();
        }
        () = task.flush().await.unwrap();

        // The offset names the record which carries the flag, and a replay from
        // it reads that record first.
        let records = fixture.read_from(journal, horizon).await;
        let (_uuid, first) = records.first().expect("the horizon record is readable");

        assert!(first.opens_horizon, "the offset is not the opening record");
        assert_eq!(records.len(), 9, "the horizon skipped records behind it");
    }

    /// A tenure which is abandoned commits nothing more, and says so at once
    /// rather than waiting on a broker. Its appender is dropped with it, so no
    /// append RPC of that tenure outlives it.
    async fn an_abandoned_tenure_answers_its_client(fixture: &Fixture) {
        for (journal, has_delta) in [
            ("acmeCo/disk/abandoned-empty", false),
            ("acmeCo/disk/abandoned", true),
        ] {
            let (mut recorder, writer) = fixture.open(journal).await.unwrap();

            if has_delta {
                recorder.reserve().unwrap().send(write(6, 0x33));
                () = drains(&recorder).await;
            }
            () = writer.abandon();

            // Let the writer drop its appender and drain an unmount mutation
            // before a request arrives.
            recorder.reserve().unwrap().send(write(7, 0x44));
            () = drains(&recorder).await;

            let err = tokio::time::timeout(std::time::Duration::from_secs(10), writer.prepare())
                .await
                .expect("an abandoned tenure answers rather than waiting")
                .expect_err("an abandoned tenure commits nothing");

            assert!(format!("{err:#}").contains("the tenure ended"), "{err:#}");
        }
    }

    /// A journal nothing created is what the tenure asked for, and no retry of that
    /// `Open` could find one, because the daemon creates none.
    async fn an_absent_journal_is_refused_at_open(fixture: &Fixture) {
        let journal = "acmeCo/disk/never-created";

        let Err(err) = fixture.standby_uncreated(journal).await else {
            panic!("a journal which does not exist must not open");
        };
        assert!(format!("{err:#}").contains("does not exist"), "{err:#}");
        assert!(err.chain().any(|cause| matches!(
            cause.downcast_ref::<crate::failure::Failure>(),
            Some(crate::failure::Failure::Invalid(_)),
        )));
    }

    /// A journal whose live spec a disk could not be recovered from never opens, even
    /// where the spec the tenure supplied is perfectly good. The daemon validates the
    /// spec which exists rather than converging it onto the one it was handed, because
    /// the journal belongs to whoever applied it.
    async fn an_unrecoverable_journal_never_opens(fixture: &Fixture) {
        let journal = "acmeCo/disk/unrecoverable";

        // The daemon both appends to a disk journal and replays it.
        let mut staged = fixture.spec(journal);
        staged.flags = broker::journal_spec::Flag::ORdonly as u32;

        () = fixture.create_journal(staged).await.unwrap();

        let Err(err) = fixture.standby_uncreated(journal).await else {
            panic!("a journal this daemon cannot append to must not open");
        };
        assert!(format!("{err:#}").contains("must be read-write"), "{err:#}");
    }

    /// Recovery rebuilds the deltas which committed. It discards a delta whose
    /// acknowledgement never reached the journal.
    async fn recovery_applies_only_committed_deltas(fixture: &Fixture) {
        let journal = "acmeCo/disk/recovered";
        let (mut recorder, writer) = fixture.open(journal).await.unwrap();

        for (block, fill) in [(1, 0xaa), (2, 0xbb)] {
            recorder.reserve().unwrap().send(write(block, fill));
        }
        let ack = writer.prepare().await.unwrap().unwrap();
        () = writer.acknowledge(ack).await.unwrap();

        // A second delta, prepared but never committed. A tenure which crashed
        // between the two leaves this behind.
        recorder.reserve().unwrap().send(write(2, 0xcc));
        recorder.reserve().unwrap().send(write(3, 0xdd));
        _ = writer.prepare().await.unwrap().unwrap();
        drop((recorder, writer));

        let (_recorder, _writer, blocks) = fixture.recover(journal, Vec::new()).await.unwrap();
        assert_eq!(blocks, vec![(1, 0xaa), (2, 0xbb)]);
    }

    /// The client made an acknowledgement durable, but it never reached the journal.
    /// Recovery appends it verbatim, which commits the delta it acknowledges.
    async fn a_recovered_acknowledgement_is_repaired(fixture: &Fixture) {
        let journal = "acmeCo/disk/repaired";
        let (mut recorder, writer) = fixture.open(journal).await.unwrap();

        recorder.reserve().unwrap().send(write(4, 0x11));
        let ack = writer.prepare().await.unwrap().unwrap();
        drop((recorder, writer));

        let (_recorder, _writer, blocks) =
            fixture.recover(journal, vec![ack.clone()]).await.unwrap();

        assert_eq!(blocks, vec![(4, 0x11)]);

        // A second repair hands back the same acknowledgement, which the journal now
        // holds, so nothing is appended for it. A tenure which repeats a repair
        // therefore recovers the same disk.
        let (_recorder, _writer, blocks) = fixture.recover(journal, vec![ack]).await.unwrap();
        assert_eq!(blocks, vec![(4, 0x11)]);
    }

    /// A recovered acknowledgement which a replay could not honor is refused before it
    /// is appended. Its delta was displaced by a tenure which promoted without it, and a
    /// journal which held that acknowledgement would fail every replay from then on.
    async fn a_stale_recovered_acknowledgement_is_refused(fixture: &Fixture) {
        let journal = "acmeCo/disk/stale-ack";
        let (mut recorder, writer) = fixture.open(journal).await.unwrap();

        recorder.reserve().unwrap().send(write(1, 0x11));
        let stale = writer.prepare().await.unwrap().unwrap();
        drop((recorder, writer));

        // A tenure which promoted without that acknowledgement, and wrote past it.
        let (mut recorder, writer, blocks) = fixture.recover(journal, Vec::new()).await.unwrap();
        assert!(blocks.is_empty(), "{blocks:?}");

        recorder.reserve().unwrap().send(write(2, 0x22));
        let ack = writer.prepare().await.unwrap().unwrap();
        () = writer.acknowledge(ack).await.unwrap();
        drop((recorder, writer));

        let Err(err) = fixture.recover(journal, vec![stale]).await else {
            panic!("an acknowledgement a replay could not honor must not be appended");
        };
        assert!(
            err.chain().any(|cause| matches!(
                cause.downcast_ref::<crate::failure::Failure>(),
                Some(crate::failure::Failure::Invalid(_)),
            )),
            "{err:#}",
        );

        // Nothing of it reached the journal, so the disk still recovers.
        let (_recorder, _writer, blocks) = fixture.recover(journal, Vec::new()).await.unwrap();
        assert_eq!(blocks, vec![(2, 0x22)]);
    }

    /// A journal which a failed first use left content in holds no committed state, so
    /// its disk is fresh.
    async fn an_orphaned_journal_recovers_nothing(fixture: &Fixture) {
        let journal = "acmeCo/disk/orphaned";
        let (mut recorder, writer) = fixture.open(journal).await.unwrap();

        recorder.reserve().unwrap().send(write(5, 0x22));
        _ = writer.prepare().await.unwrap().unwrap();
        drop((recorder, writer));

        assert!(
            fixture.head(journal).await > 0,
            "the delta reached the journal"
        );

        let (_recorder, _writer, blocks) = fixture.recover(journal, Vec::new()).await.unwrap();
        assert!(blocks.is_empty(), "{blocks:?}");
    }

    /// A client hands back the acknowledgement its checkpoint holds, which already
    /// landed, after the next transaction's records reached the journal. That
    /// acknowledgement is already committed, so the promotion appends nothing for it,
    /// and the records after it are a delta nobody acknowledged.
    async fn an_acknowledgement_which_landed_before_a_pending_delta_is_recovered(
        fixture: &Fixture,
    ) {
        let journal = "acmeCo/disk/landed-ack";
        let (mut recorder, writer) = fixture.open(journal).await.unwrap();

        recorder.reserve().unwrap().send(write(1, 0xaa));
        let ack = writer.prepare().await.unwrap().unwrap();
        () = writer.acknowledge(ack.clone()).await.unwrap();

        // The next transaction writes, and the writer appends that write at once.
        let head = fixture.head(journal).await;
        recorder.reserve().unwrap().send(write(2, 0xbb));
        () = fixture.advances(journal, head).await;
        drop((recorder, writer));

        // The client failed before it committed the next transaction, so its checkpoint
        // still holds the acknowledgement it last committed.
        let (_recorder, _writer, blocks) = fixture.recover(journal, vec![ack]).await.unwrap();
        assert_eq!(blocks, vec![(1, 0xaa)]);

        let records = fixture.read(journal).await;
        let ((_producer, _clock, flags), last) = records.last().unwrap();
        assert!(
            flags.is_outside() && !last.installs_epoch.is_empty(),
            "the promotion appended nothing after its fence",
        );
    }

    /// A disk's first commit marks its journal's recovery-floor label, which is how a
    /// later promotion knows the disk holds committed state.
    async fn a_first_commit_marks_the_floor_label(fixture: &Fixture) {
        let journal = "acmeCo/disk/marked";
        let (mut recorder, writer) = fixture.open(journal).await.unwrap();
        assert_eq!(fixture.floor_label(journal).await, None);

        recorder.reserve().unwrap().send(write(1, 0xaa));
        let ack = writer.prepare().await.unwrap().unwrap();
        assert_eq!(
            fixture.floor_label(journal).await,
            None,
            "a prepared delta is not yet committed",
        );
        () = writer.acknowledge(ack).await.unwrap();

        assert_eq!(
            fixture.floor_label(journal).await.as_deref(),
            Some(crate::recovery_floor_value(0).as_str()),
        );
    }

    /// A journal whose label says the disk committed state, but which holds none, lost
    /// that state: its fragments were deleted. One which holds nothing at all has no
    /// head past its floor, and is refused at `Open`. One which still holds records,
    /// none of them committed, is refused at promotion rather than formatted as a fresh
    /// disk, whether or not the client hands back an acknowledgement.
    async fn a_marked_journal_without_committed_state_is_refused(fixture: &Fixture) {
        let journal = "acmeCo/disk/emptied";

        let mut spec = fixture.spec(journal);
        spec.labels = Some(labels::set_value(
            spec.labels.take().unwrap_or_default(),
            crate::DISK_RECOVERY_FLOOR,
            &crate::recovery_floor_value(0),
        ));
        () = fixture.create_journal(spec).await.unwrap();

        let Err(err) = fixture.recover(journal, Vec::new()).await else {
            panic!("a disk whose committed state is gone must not be formatted");
        };
        assert!(
            err.chain().any(|cause| matches!(
                cause.downcast_ref::<crate::failure::Failure>(),
                Some(crate::failure::Failure::Invalid(_)),
            )),
            "{err:#}",
        );
        assert!(
            format!("{err:#}").contains("recovery-floor label"),
            "{err:#}"
        );

        // A fence commits nothing, and puts the head past the floor.
        let epoch = super::random_producer();
        let fence = proto::DiskRecord {
            uuid: super::uuid_bytes(
                super::random_producer(),
                proto_gazette::uuid::Clock::from_time(std::time::SystemTime::now()),
                proto_gazette::uuid::Flags::OUTSIDE_TXN,
            ),
            installs_epoch: bytes::Bytes::copy_from_slice(epoch.as_bytes()),
            ..Default::default()
        };
        _ = fixture.append_fence(journal, epoch, &fence).await;

        // An acknowledgement of this journal, from a writer whose records are gone.
        let mut ack = bytes::BytesMut::new();
        proto_gazette::fixed_framing::encode(
            &proto::DiskRecord {
                uuid: super::uuid_bytes(
                    super::random_producer(),
                    proto_gazette::uuid::Clock::from_time(std::time::SystemTime::now()),
                    proto_gazette::uuid::Flags::ACK_TXN,
                ),
                journal: journal.to_string(),
                ..Default::default()
            },
            &mut ack,
        );

        for acks in [Vec::new(), vec![ack.freeze()]] {
            let Err(err) = fixture.recover(journal, acks).await else {
                panic!("a disk whose committed state is gone must not be formatted");
            };
            assert!(
                err.chain().any(|cause| matches!(
                    cause.downcast_ref::<crate::failure::Failure>(),
                    Some(crate::failure::Failure::Invalid(_)),
                )),
                "{err:#}",
            );
            assert!(format!("{err:#}").contains("applied nothing"), "{err:#}");
        }
    }

    /// One block of `fill`, as a device write of it encodes.
    fn write(block: u32, fill: u8) -> Vec<proto::Chunk> {
        encode_write(block, &bytes::Bytes::from(vec![fill; BLOCK_SIZE as usize]))
    }

    /// Wait for the writer to take every mutation the recording channel holds.
    ///
    /// The writer takes on a task of its own, so a scenario which asserts what it did
    /// with a mutation must first see that it has one.
    async fn drains(recorder: &Recorder) {
        for _ in 0..1000 {
            if recorder.is_empty() {
                return;
            }
            () = tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("the writer did not take the mutations offered to it");
    }
}
