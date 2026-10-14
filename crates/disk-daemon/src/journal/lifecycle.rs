//! Journal lifecycle as a typestate machine. Each transition consumes its
//! previous phase, preventing writes before fencing or service before replay.
//!
//! ```text
//! Standby::new ─► Standby    validate spec and confirm head at Open
//!                   │ play: backfill and tail without claiming
//!                   │ claim_journal: fence without waiting for replay
//!                   ▼
//!                 Claimed
//!                   │ promote: finish replay and repair a recovered ack
//!                   ▼
//!                 Promoted ─► serve: spawn the writer
//! ```
//!
//! The fence append unions a fresh author into the journal's `author` register,
//! replacing prior values. Subsequent appends check that author. Gazette orders
//! these appends, so earlier writers fail once fenced out.
//!
//! The client chooses which tenure to promote. Concurrent promotions may both
//! succeed; the later fence wins, and the earlier tenure fails at its next
//! append. A tenure claims once and cannot reclaim after losing its fence.
//!
//! Registers provide cooperative ownership, not commit authority: etcd can lose
//! registers independently of journal content, and empty registers match any
//! selector. Committed records remain authoritative.

use super::replay::{self, sequencer, uncommitted};
use super::{spec, writer};
use crate::device;
use crate::image;
use crate::proto;
use anyhow::Context;
use proto_gazette::{broker, fixed_framing, uuid};

/// A validated journal whose replay may run without claiming it.
pub struct Standby {
    journal: super::Journal,
    appender: publisher::Appender,
}

impl Standby {
    /// Validate the existing journal and resolve its head and recovery floor.
    /// Only a zero-byte barrier is appended. `ended` cancels broker calls.
    /// Validation happens before the disk is mounted and available to its client.
    pub async fn new(
        client: &gazette::journal::Client,
        journal: String,
        ended: tokio_util::sync::CancellationToken,
    ) -> anyhow::Result<Self> {
        let client = client.clone();

        let spec::Resolved {
            head,
            floor,
            marked,
        } = spec::resolve(&client, &journal, &ended).await?;

        // The claim sets this appender's registers before it writes any records.
        let author = random_producer();
        let appender = publisher::Appender::new(client.clone(), journal.clone());

        Ok(Self {
            journal: super::Journal {
                name: journal,
                client,
                ended,
                author,
                head,
                floor,
                marked,
            },
            appender,
        })
    }

    /// Replay from the recovery floor until [`Claimed::promote`] stops playback.
    /// This does not claim the journal, allowing another tenure to keep writing.
    /// The floor is a seek position, never a record filter; a missing or stale
    /// floor costs replay work. Zero starts at the first retained fragment.
    pub fn play(
        &self,
        image: image::Image,
        uncommitted: uncommitted::UncommittedDelta,
    ) -> replay::Playback {
        let reading = replay::Reading {
            client: self.journal.client.clone(),
            journal: self.journal.name.clone(),
            ended: self.journal.ended.clone(),
        };

        replay::Playback::start(
            reading,
            self.journal.floor,
            self.journal.head,
            image,
            uncommitted,
        )
    }

    /// Fence the previous writer before waiting for replay, giving promotion a
    /// fixed head to reach. Validate the live spec again before displacing anyone.
    ///
    /// Even an empty journal must be claimed: formatting is a recorded delta.
    /// The fence is flushed alone so [`Claimed::promote`] knows its exact end.
    /// A failed fence append ends the tenure, even if the append landed.
    pub async fn claim_journal(self) -> anyhow::Result<Claimed> {
        let Self {
            mut journal,
            mut appender,
        } = self;

        let listed = spec::list(&journal.client, &journal.name, &journal.ended).await?;
        journal.marked = listed.marked;

        let fence = fence(journal.author);

        () = appender
            .set_request(broker::AppendRequest {
                journal: journal.name.clone(),
                union_registers: Some(fence.clone()),
                ..Default::default()
            })
            .await
            .map_err(super::append_failure)?;

        // Use a separate producer so the fence's wall clock cannot make the
        // first delta look like a duplicate if time steps backward. Retries
        // reuse these bytes and are deduplicated by UUID.
        let record = proto::DiskRecord {
            uuid: super::uuid_bytes(
                random_producer(),
                uuid::Clock::from_time(std::time::SystemTime::now()),
                uuid::Flags::OUTSIDE_TXN,
            ),
            installs_author: bytes::Bytes::copy_from_slice(journal.author.as_bytes()),
            ..Default::default()
        };
        fixed_framing::encode(&record, &mut appender.buffer);

        () = journal
            .flush(&mut appender)
            .await
            .with_context(|| format!("claiming {}", journal.name))?;

        () = appender
            .set_request(broker::AppendRequest {
                journal: journal.name.clone(),
                check_registers: Some(broker::LabelSelector {
                    include: Some(fence),
                    exclude: None,
                }),
                ..Default::default()
            })
            .await
            .map_err(super::append_failure)?;

        // Playback may not observe a regressed head, especially while parked
        // on an empty journal. Check the new listing against the fenced head.
        () = spec::check_floor(&journal.name, &listed, journal.head)?;

        Ok(Claimed { journal, appender })
    }
}

/// A fenced journal with replay still to finish at the claimed head.
pub struct Claimed {
    journal: super::Journal,
    appender: publisher::Appender,
}

impl Claimed {
    /// Stop playback and finish replay through the fenced head. Append and replay
    /// `recovered_ack` only if it commits the held delta; skip an already committed
    /// acknowledgement and refuse one replay cannot honor.
    ///
    /// Drop any remaining unacknowledged delta before handing over the image, so
    /// the mounted disk cannot run ahead of the client's committed state.
    ///
    /// Fences and orphaned deltas alone describe a fresh disk. If a recovered ack
    /// or floor label proves committed state existed but replay applies nothing,
    /// fail rather than format over possible data loss.
    pub async fn promote(
        self,
        playback: replay::Playback,
        recovered_ack: Option<bytes::Bytes>,
    ) -> anyhow::Result<(Promoted, Recovered)> {
        let Self {
            mut journal,
            mut appender,
        } = self;
        let mut rebuild = playback.stop().await?;

        // The fence's broker-confirmed head bounds replay and ensures the broker
        // has indexed every fragment below it.
        let mut head = journal.head;

        () = super::until_ended(&journal.ended, "promoting", async {
            () = replay::read(&journal.client, &journal.name, Some(head), &mut rebuild).await?;

            anyhow::Ok(())
        })
        .await?;

        // Validate repair against the completed replay before appending an ack
        // that could make this and all future replays fail.
        let mut repair = None;

        if let Some(ack) = &recovered_ack {
            let (producer, clock) = check_recovered_ack(ack)?;

            match rebuild.sequencer().recovered_ack(producer, clock) {
                sequencer::RecoveredAck::Commits => repair = Some(ack.clone()),
                sequencer::RecoveredAck::Committed { older: false } => (),
                sequencer::RecoveredAck::Committed { older: true } => tracing::warn!(
                    journal = journal.name,
                    ?producer,
                    ?clock,
                    "a recovered acknowledgement is older than its writer's last commit, \
                     so the client's own state is behind the disk",
                ),
                sequencer::RecoveredAck::Refused => {
                    return Err(crate::invalid_argument(format!(
                        "the recovered acknowledgement of {producer:?} at {clock:?} commits \
                         a delta which journal {} does not hold whole",
                        journal.name,
                    )));
                }
            }
        }

        if let Some(ack) = repair {
            () = journal
                .append_ack(&mut appender, &ack)
                .await
                .context("repairing a recovered acknowledgement")?;

            // Read the repaired acknowledgement through the same sequencing state
            // to apply the delta it commits.
            let repaired_head = journal.head;

            () = super::until_ended(&journal.ended, "promoting", async {
                () = replay::read(
                    &journal.client,
                    &journal.name,
                    Some(repaired_head),
                    &mut rebuild,
                )
                .await?;

                anyhow::Ok(())
            })
            .await?;
            head = repaired_head;
        }

        let (chunks, derived) = (rebuild.applied_chunks(), rebuild.derived_floor());
        let (image, uncommitted, _floor, opened) = rebuild.into_parts();

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

        // Neither a valid floor nor a repaired ack can hide every committed
        // chunk. Missing chunks here must not be treated as a fresh disk.
        if chunks == 0 && (recovered_ack.is_some() || journal.marked) {
            return Err(crate::invalid_argument(format!(
                "a replay of journal {} applied nothing, but {}: its committed state was \
                 destroyed",
                journal.name,
                if recovered_ack.is_none() {
                    "its recovery-floor label says the disk committed state"
                } else {
                    "the tenure supplied a recovered acknowledgement"
                },
            )));
        }

        journal.head = head;
        journal.floor = derived.unwrap_or(journal.floor);

        // Persist any floor an earlier tenure completed but failed to store.
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

/// A journal this tenure holds and has replayed, which [`Promoted::serve`] appends
/// the disk's deltas to.
///
/// Its offsets are the ones the promotion settled: the head its replay read to, the
/// floor that replay derived, and the horizon it left open.
pub struct Promoted {
    /// Journal ownership and recovery offsets after promotion.
    journal: super::Journal,
    /// Fenced append pipeline passed to the writer.
    appender: publisher::Appender,
    /// Offset of a horizon the replay left open, which the writer completes rather
    /// than opening one of its own. See [`Recovered::horizon`].
    horizon: Option<i64>,
}

impl Promoted {
    /// Start the writer over this device's recording channel and compactor.
    /// Every append checks the author installed by the claim.
    pub fn serve(self, recorded: device::Recorded, compactor: device::Compactor) -> writer::Writer {
        self.into_actor(Some(compactor)).spawn(recorded)
    }

    /// [`Promoted::serve`], for a writer with no disk behind it. It appends what it
    /// is given and compacts nothing, because a compactor needs a real device.
    #[cfg(test)]
    pub fn serve_uncompacted(self, recorded: device::Recorded) -> writer::Writer {
        self.into_actor(None).spawn(recorded)
    }

    /// The writer's actor for this journal, at the start of a delta it has not yet
    /// taken anything into. Nothing runs until [`Promoted::serve`] spawns it.
    pub(super) fn into_actor(self, compactor: Option<device::Compactor>) -> writer::WriterActor {
        let Self {
            journal,
            appender,
            horizon,
        } = self;

        writer::WriterActor::new(journal, appender, horizon, compactor)
    }
}

/// What a recovery of a disk found in its journal.
pub struct Recovered {
    /// The disk's committed state, rebuilt.
    pub image: image::Image,
    /// Whether the journal held no committed state. A fresh disk's filesystem is the
    /// caller's to format.
    pub fresh: bool,
    /// Pending blocks of the horizon left open by replay. Present exactly when
    /// [`Promoted::horizon`] is: the owner resumes these blocks, while the writer
    /// keeps the opening offset and advances the floor after the discharging
    /// delta commits.
    pub horizon: Option<crate::horizon::Horizon>,
}

/// Registers which name `author` as a journal's author, as Go's `Author.Fence()`
/// returns them. The value is the author's producer in hex. A claim unions them in,
/// and every append after it checks them.
pub(crate) fn fence(author: uuid::Producer) -> broker::LabelSet {
    let value: String = author
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();

    labels::build_set([("author", value.as_str())])
}

/// Generate a fresh random Gazette `Producer` identity: a tenure's author, or a
/// fence record's own producer.
///
/// A copy of runtime-next's `new_producer`, which this crate does not depend on.
fn random_producer() -> uuid::Producer {
    let mut producer: [u8; 6] = rand::random();
    producer[0] |= 0x01; // Set multicast bit (mark as not a real MAC address).
    uuid::Producer::from_bytes(producer)
}

/// Refuse a recovered acknowledgement which is not one framed `ACK_TXN` record that
/// carries nothing else, and report whose it is and its clock.
fn check_recovered_ack(ack: &[u8]) -> anyhow::Result<(uuid::Producer, uuid::Clock)> {
    let mut ack = bytes::BytesMut::from(ack);
    let record = match fixed_framing::unpack::<proto::DiskRecord>(&mut ack) {
        Ok(fixed_framing::Frame::Record { message, .. }) if ack.is_empty() => message,
        _ => {
            return Err(crate::invalid_argument(
                "a recovered acknowledgement is not one framed record".to_string(),
            ));
        }
    };
    let uuid = uuid::Uuid::from_slice(&record.uuid).map_err(|err| {
        crate::invalid_argument(format!(
            "a recovered acknowledgement carries no message UUID: {err}"
        ))
    })?;
    let (producer, clock, flags) = uuid::parse(uuid).map_err(|err| {
        crate::invalid_argument(format!(
            "a recovered acknowledgement carries a malformed UUID: {err}"
        ))
    })?;

    if !flags.is_ack()
        || !record.chunks.is_empty()
        || record.opens_horizon
        || !record.installs_author.is_empty()
    {
        return Err(crate::invalid_argument(
            "a recovered acknowledgement is not an ACK_TXN record which carries nothing else"
                .to_string(),
        ));
    }
    Ok((producer, clock))
}

#[cfg(test)]
mod test {
    use super::{check_recovered_ack, fence};
    use crate::proto;
    use proto_gazette::{fixed_framing, uuid};

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
            uuid: crate::journal::uuid_bytes(producer(seed), clock(ticks), flags),
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
    fn test_an_author_fences_its_register_in_hex() {
        let fence = fence(uuid::Producer([0x01, 0x23, 0x45, 0x67, 0x89, 0xab]));

        let labels: Vec<String> = fence
            .labels
            .iter()
            .map(|label| format!("{}={}", label.name, label.value))
            .collect();
        insta::assert_snapshot!(labels.join("\n"), @"author=0123456789ab");
    }

    /// A recovered acknowledgement is judged against the journal only if it is exactly
    /// one framed `ACK_TXN` record which carries nothing else. Anything else is
    /// refused as invalid. Whether the journal already committed it, or it commits the
    /// delta a replay holds, is the sequencer's to say.
    #[test]
    fn test_a_recovered_acknowledgement_is_one_ack() {
        let ack = record(0x10, 2, uuid::Flags::ACK_TXN);

        let cases: [(&str, bytes::Bytes); 8] = [
            ("an acknowledgement", frame(std::slice::from_ref(&ack))),
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
                frame(&[record(0x10, 2, uuid::Flags::CONTINUE_TXN)]),
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
            let outcome = match check_recovered_ack(&ack) {
                Ok((producer, clock)) => format!("judged, of {producer:?} at {clock:?}"),
                Err(err) => {
                    assert!(
                        err.downcast_ref::<proto_grpc::StatusError>()
                            .map(|status| status.code())
                            == Some(tonic::Code::InvalidArgument),
                        "{what} was refused as other than invalid: {err:#}",
                    );
                    format!("refused: {err:#}")
                }
            };
            out.push(format!("{what:<22}{outcome}"));
        }
        insta::assert_snapshot!(out.join("\n"), @"
        an acknowledgement    judged, of Producer(11:00:00:00:00:10) at Clock(0s 2000ns)
        unframed bytes        refused: a recovered acknowledgement is not one framed record (InvalidArgument)
        two records           refused: a recovered acknowledgement is not one framed record (InvalidArgument)
        no UUID               refused: a recovered acknowledgement carries no message UUID: invalid length: expected 16 bytes, found 0 (InvalidArgument)
        a malformed UUID      refused: a recovered acknowledgement carries a malformed UUID: UUID 00000000-0000-0000-0000-000000000000 is not a V1 UUID (InvalidArgument)
        a data record         refused: a recovered acknowledgement is not an ACK_TXN record which carries nothing else (InvalidArgument)
        one carrying chunks   refused: a recovered acknowledgement is not an ACK_TXN record which carries nothing else (InvalidArgument)
        one opening a horizon refused: a recovered acknowledgement is not an ACK_TXN record which carries nothing else (InvalidArgument)
        ");
    }
}

#[cfg(test)]
mod broker_test {
    use crate::chunk;
    use crate::proto;
    use crate::test_support::broker;
    use proto_gazette::uuid;

    /// Render records with producer and author roles instead of random IDs
    /// to keep snapshots stable.
    fn render(
        records: &[(broker::UuidParts, proto::DiskRecord)],
        roles: &[(&str, uuid::Producer)],
    ) -> Vec<String> {
        records
            .iter()
            .map(|((producer, _clock, flags), record)| {
                let flags = if flags.is_outside() {
                    "outside"
                } else if flags.is_ack() {
                    "ack"
                } else {
                    "continue"
                };
                let mut carries = Vec::new();

                if !record.installs_author.is_empty() {
                    carries.push(format!(
                        "installs {}",
                        role(roles, &record.installs_author, "another author"),
                    ));
                }
                carries.extend(record.chunks.iter().map(render_chunk));

                if record.opens_horizon {
                    carries.push("opens a horizon".to_string());
                }
                let producer = role(roles, producer.as_bytes(), "another producer");

                format!("{flags:<10}{producer:<18}{}", carries.join(", "))
                    .trim_end()
                    .to_string()
            })
            .collect()
    }

    fn role<'a>(roles: &[(&'a str, uuid::Producer)], producer: &[u8], other: &'a str) -> &'a str {
        roles
            .iter()
            .find(|(_, known)| known.as_bytes()[..] == producer[..])
            .map_or(other, |(role, _)| *role)
    }

    fn render_chunk(chunk: &proto::Chunk) -> String {
        let blocks = chunk::covered_blocks(chunk);

        match &chunk.content {
            Some(proto::chunk::Content::Punch(_)) => format!("punch {blocks:?}"),
            Some(proto::chunk::Content::Data(data)) => match data.first() {
                Some(&fill) if data.iter().all(|&byte| byte == fill) => {
                    format!("write {blocks:?} of {fill:#04x}")
                }
                _ => format!("write {blocks:?} of {} bytes", data.len()),
            },
            None => format!("no content at {blocks:?}"),
        }
    }

    fn render_register(value: Option<String>, roles: &[(&str, uuid::Producer)]) -> String {
        let Some(value) = value else {
            return "unset".to_string();
        };
        roles
            .iter()
            .find(|(_, author)| super::fence(*author).labels[0].value == value)
            .map_or_else(|| format!("{value:?}"), |(role, _)| role.to_string())
    }

    fn render_blocks(blocks: &[(u32, u8)]) -> String {
        if blocks.is_empty() {
            return "none".to_string();
        }
        blocks
            .iter()
            .map(|(block, fill)| format!("{block}={fill:#04x}"))
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Format the error code and message seen by a tenure client.
    fn refusal(err: &anyhow::Error) -> String {
        match err.downcast_ref::<proto_grpc::StatusError>() {
            Some(status) => format!("{:?}: {err:#}", status.code()),
            None => format!("no code: {err:#}"),
        }
    }

    #[tokio::test]
    async fn test_a_journal_lifecycle_over_a_real_broker() {
        let fixture = broker::Fixture::start().await;

        first_use_claims_the_journal(&fixture).await;
        a_replacement_writer_fences_the_first(&fixture).await;
        a_tenure_which_never_prepares_appends_only_its_fence(&fixture).await;
        a_repeated_fence_replays_as_one(&fixture).await;
        an_absent_journal_is_refused_at_open(&fixture).await;
        an_unrecoverable_journal_never_opens(&fixture).await;
        recovery_applies_only_committed_deltas(&fixture).await;
        a_recovered_acknowledgement_is_repaired(&fixture).await;
        a_stale_recovered_acknowledgement_is_refused(&fixture).await;
        an_orphaned_journal_recovers_nothing(&fixture).await;
        an_acknowledgement_which_landed_before_a_pending_delta_is_recovered(&fixture).await;
        a_marked_journal_without_committed_state_is_refused(&fixture).await;

        fixture.stop().await;
    }

    /// First use claims the journal. It installs the author register with a fence
    /// record, then appends its delta under that claim.
    async fn first_use_claims_the_journal(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/first-use";
        let (mut recorder, writer) = fixture.open(journal).await.unwrap();

        recorder
            .reserve()
            .unwrap()
            .send(vec![chunk::encode_punch(3, 2)]);
        let ack = writer
            .prepare()
            .await
            .unwrap()
            .expect("the delta is not empty");
        () = writer.acknowledge(ack).await.unwrap();

        let roles = [("the author", writer.author())];
        let mut out = render(&fixture.read(journal).await, &roles);
        out.push(format!(
            "register  {}",
            render_register(fixture.author(journal).await, &roles),
        ));

        // The fence has its own producer; deltas and acknowledgements use the author.
        insta::assert_snapshot!(out.join("\n"), @"
        outside   another producer  installs the author
        continue  the author        punch 3..5
        ack       the author
        register  the author
        ");
    }

    /// A replacement tenure takes the author register. The first tenure then cannot
    /// append.
    async fn a_replacement_writer_fences_the_first(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/contended";
        let (mut first_recorder, first) = fixture.open(journal).await.unwrap();

        first_recorder
            .reserve()
            .unwrap()
            .send(vec![chunk::encode_punch(0, 1)]);
        let ack = first.prepare().await.unwrap().unwrap();
        () = first.acknowledge(ack).await.unwrap();

        // The journal now holds a committed delta, so a replacement claims it as that
        // replacement recovers.
        let (_second_recorder, second, _blocks) = fixture.recover(journal, None).await.unwrap();
        assert_ne!(first.author(), second.author());

        first_recorder
            .reserve()
            .unwrap()
            .send(vec![chunk::encode_punch(1, 1)]);

        // The append which lost the fence fails as `ABORTED`, which tells its client
        // that another tenure owns the disk, rather than as the appender's own code.
        // Every later request reports the failure which ended the tenure.
        let mut out = Vec::new();
        for what in ["fenced-out prepare", "later prepare"] {
            let err = first.prepare().await.unwrap_err();
            out.push(format!("{what:<20}{}", refusal(&err)));
        }
        insta::assert_snapshot!(out.join("\n"), @"
        fenced-out prepare  Aborted: unexpected broker status: RegisterMismatch (Aborted)
        later prepare       no code: the tenure is over
        ");
    }

    /// A tenure which prepares nothing leaves its fence behind and nothing else. The
    /// claim is what makes it the journal's writer, and it happens whether or not the
    /// disk is ever written.
    async fn a_tenure_which_never_prepares_appends_only_its_fence(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/untouched";
        let (recorder, writer) = fixture.open(journal).await.unwrap();

        let author = writer.author();
        let prepared = writer.prepare().await.unwrap();
        drop((recorder, writer));

        let mut out = vec![format!("prepared  {prepared:?}")];
        out.extend(render(
            &fixture.read(journal).await,
            &[("the author", author)],
        ));

        insta::assert_snapshot!(out.join("\n"), @"
        prepared  None
        outside   another producer  installs the author
        ");
    }

    /// A fence which lands twice, as a transient retry of one which had already landed
    /// does, is harmless. The author holds the journal, and a later replay skips the
    /// repeated fence as a duplicate.
    async fn a_repeated_fence_replays_as_one(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/repeated-fence";
        () = fixture.create_journal(fixture.spec(journal)).await.unwrap();

        let author = super::random_producer();
        let fence = proto::DiskRecord {
            uuid: crate::journal::uuid_bytes(
                super::random_producer(),
                proto_gazette::uuid::Clock::from_time(std::time::SystemTime::now()),
                proto_gazette::uuid::Flags::OUTSIDE_TXN,
            ),
            installs_author: bytes::Bytes::copy_from_slice(author.as_bytes()),
            ..Default::default()
        };

        // Each append adds the fence's 34 bytes: an 8-byte frame header, then its
        // 16-byte UUID and 6-byte author, each behind a 2-byte field header.
        let mut out = Vec::new();
        for what in ["a fence", "the same fence, again"] {
            let end = fixture.append_fence(journal, author, &fence).await;
            let head = fixture.head(journal).await;
            out.push(format!("{what:<23}ends at {end}, head {head}"));
        }
        let register = fixture.author(journal).await;
        out.push(format!(
            "{:<23}{}",
            "register",
            render_register(register, &[("the author", author)]),
        ));

        // The journal holds two copies of one fence and nothing else, so a replay of
        // it is a fresh disk.
        let (_promoted, blocks) = fixture.promote(journal, None).await.unwrap();
        out.push(format!(
            "{:<23}{}",
            "replayed blocks",
            render_blocks(&blocks)
        ));

        insta::assert_snapshot!(out.join("\n"), @"
        a fence                ends at 34, head 34
        the same fence, again  ends at 68, head 68
        register               the author
        replayed blocks        none
        ");
    }

    /// A journal nothing created is what the tenure asked for, and no retry of that
    /// `Open` could find one, because the daemon creates none.
    async fn an_absent_journal_is_refused_at_open(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/never-created";

        let Err(err) = fixture.standby_uncreated(journal).await else {
            panic!("a journal which does not exist must not open");
        };
        insta::assert_snapshot!(refusal(&err), @"InvalidArgument: journal acmeCo/disk/never-created does not exist or is not a disk journal, and the daemon creates none: whoever deploys this disk owns its journal's specification (InvalidArgument)");
    }

    /// Reject an unrecoverable live specification without changing it. The journal
    /// belongs to its creator.
    async fn an_unrecoverable_journal_never_opens(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/unrecoverable";

        // The daemon both appends to a disk journal and replays it.
        let mut staged = fixture.spec(journal);
        staged.flags = proto_gazette::broker::journal_spec::Flag::ORdonly as u32;

        () = fixture.create_journal(staged).await.unwrap();

        let Err(err) = fixture.standby_uncreated(journal).await else {
            panic!("a journal this daemon cannot append to must not open");
        };
        insta::assert_snapshot!(refusal(&err), @"InvalidArgument: journal acmeCo/disk/unrecoverable has flags 0x1, and a disk's journal must be read-write (InvalidArgument)");
    }

    /// Recovery rebuilds the deltas which committed. It discards a delta whose
    /// acknowledgement never reached the journal.
    async fn recovery_applies_only_committed_deltas(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/recovered";
        let (mut recorder, writer) = fixture.open(journal).await.unwrap();

        for (block, fill) in [(1, 0xaa), (2, 0xbb)] {
            recorder.reserve().unwrap().send(broker::write(block, fill));
        }
        let ack = writer.prepare().await.unwrap().unwrap();
        () = writer.acknowledge(ack).await.unwrap();

        // A second delta, prepared but never committed. A tenure which crashed
        // between the two leaves this behind.
        recorder.reserve().unwrap().send(broker::write(2, 0xcc));
        recorder.reserve().unwrap().send(broker::write(3, 0xdd));
        _ = writer.prepare().await.unwrap().unwrap();
        drop((recorder, writer));

        let (_recorder, _writer, blocks) = fixture.recover(journal, None).await.unwrap();
        assert_eq!(blocks, vec![(1, 0xaa), (2, 0xbb)]);
    }

    /// The client made an acknowledgement durable, but it never reached the journal.
    /// Recovery appends it verbatim, which commits the delta it acknowledges.
    async fn a_recovered_acknowledgement_is_repaired(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/repaired";
        let (mut recorder, writer) = fixture.open(journal).await.unwrap();

        recorder.reserve().unwrap().send(broker::write(4, 0x11));
        let ack = writer.prepare().await.unwrap().unwrap();
        drop((recorder, writer));

        let (_recorder, _writer, blocks) =
            fixture.recover(journal, Some(ack.clone())).await.unwrap();
        let mut out = vec![format!("repaired        {}", render_blocks(&blocks))];

        // A second repair hands back the same acknowledgement, which the journal now
        // holds, so nothing is appended for it. A tenure which repeats a repair
        // therefore recovers the same disk.
        let (_recorder, _writer, blocks) = fixture.recover(journal, Some(ack)).await.unwrap();
        out.push(format!("repaired again  {}", render_blocks(&blocks)));

        insta::assert_snapshot!(out.join("\n"), @"
        repaired        4=0x11
        repaired again  4=0x11
        ");
    }

    /// A recovered acknowledgement which a replay could not honor is refused before it
    /// is appended. Its delta was displaced by a tenure which promoted without it, and a
    /// journal which held that acknowledgement would fail every replay from then on.
    async fn a_stale_recovered_acknowledgement_is_refused(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/stale-ack";
        let (mut recorder, writer) = fixture.open(journal).await.unwrap();

        recorder.reserve().unwrap().send(broker::write(1, 0x11));
        let stale = writer.prepare().await.unwrap().unwrap();
        drop((recorder, writer));

        // A tenure which promoted without that acknowledgement, and wrote past it.
        let (mut recorder, writer, blocks) = fixture.recover(journal, None).await.unwrap();
        let mut out = vec![format!("promoted without it   {}", render_blocks(&blocks))];

        recorder.reserve().unwrap().send(broker::write(2, 0x22));
        let ack = writer.prepare().await.unwrap().unwrap();
        () = writer.acknowledge(ack).await.unwrap();
        drop((recorder, writer));

        // Normalize the acknowledgement's random author and clock for the snapshot.
        let (author, clock) = super::check_recovered_ack(&stale).unwrap();
        let Err(err) = fixture.recover(journal, Some(stale)).await else {
            panic!("an acknowledgement a replay could not honor must not be appended");
        };
        let refused = refusal(&err)
            .replace(&format!("{author:?}"), "<its author>")
            .replace(&format!("{clock:?}"), "<its clock>");
        out.push(format!("the stale ack         {refused}"));

        // Nothing of it reached the journal, so the disk still recovers.
        let (_recorder, _writer, blocks) = fixture.recover(journal, None).await.unwrap();
        out.push(format!("recovered after       {}", render_blocks(&blocks)));

        insta::assert_snapshot!(out.join("\n"), @"
        promoted without it   none
        the stale ack         InvalidArgument: the recovered acknowledgement of <its author> at <its clock> commits a delta which journal acmeCo/disk/stale-ack does not hold whole (InvalidArgument)
        recovered after       2=0x22
        ");
    }

    /// A journal which a failed first use left content in holds no committed state, so
    /// its disk is fresh.
    async fn an_orphaned_journal_recovers_nothing(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/orphaned";
        let (mut recorder, writer) = fixture.open(journal).await.unwrap();

        recorder.reserve().unwrap().send(broker::write(5, 0x22));
        _ = writer.prepare().await.unwrap().unwrap();
        drop((recorder, writer));

        assert!(
            fixture.head(journal).await > 0,
            "the delta reached the journal"
        );

        let (_recorder, _writer, blocks) = fixture.recover(journal, None).await.unwrap();
        assert!(blocks.is_empty(), "{blocks:?}");
    }

    /// A client hands back the acknowledgement its checkpoint holds, which already
    /// landed, after the next transaction's records reached the journal. That
    /// acknowledgement is already committed, so the promotion appends nothing for it,
    /// and the records after it are a delta nobody acknowledged.
    async fn an_acknowledgement_which_landed_before_a_pending_delta_is_recovered(
        fixture: &broker::Fixture,
    ) {
        let journal = "acmeCo/disk/landed-ack";
        let (mut recorder, writer) = fixture.open(journal).await.unwrap();
        let first = writer.author();

        recorder.reserve().unwrap().send(broker::write(1, 0xaa));
        let ack = writer.prepare().await.unwrap().unwrap();
        () = writer.acknowledge(ack.clone()).await.unwrap();

        // The next transaction writes, and the writer appends that write at once.
        let head = fixture.head(journal).await;
        recorder.reserve().unwrap().send(broker::write(2, 0xbb));
        () = fixture.advances(journal, head).await;
        drop((recorder, writer));

        // The client failed before it committed the next transaction, so its checkpoint
        // still holds the acknowledgement it last committed.
        let (_recorder, second, blocks) = fixture.recover(journal, Some(ack)).await.unwrap();
        let mut out = vec![format!("blocks    {}", render_blocks(&blocks))];

        // The promotion appended nothing after its fence.
        out.extend(render(
            &fixture.read(journal).await,
            &[
                ("the first author", first),
                ("the second author", second.author()),
            ],
        ));

        insta::assert_snapshot!(out.join("\n"), @"
        blocks    1=0xaa
        outside   another producer  installs the first author
        continue  the first author  write 1..2 of 0xaa
        ack       the first author
        continue  the first author  write 2..3 of 0xbb
        outside   another producer  installs the second author
        ");
    }

    /// A journal whose label says the disk committed state, but which holds none, lost
    /// that state: its fragments were deleted. One which holds nothing at all has no
    /// head past its floor, and is refused at `Open`. One which still holds records,
    /// none of them committed, is refused at promotion rather than formatted as a fresh
    /// disk, whether or not the client hands back an acknowledgement.
    async fn a_marked_journal_without_committed_state_is_refused(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/emptied";

        let mut spec = fixture.spec(journal);
        spec.labels = Some(labels::set_value(
            spec.labels.take().unwrap_or_default(),
            crate::DISK_RECOVERY_FLOOR,
            &crate::recovery_floor_value(0),
        ));
        () = fixture.create_journal(spec).await.unwrap();

        let Err(err) = fixture.recover(journal, None).await else {
            panic!("a disk whose committed state is gone must not be formatted");
        };
        let mut out = vec![format!("{:<20}{}", "nothing, at open", refusal(&err))];

        // A fence commits nothing, and puts the head past the floor.
        let author = super::random_producer();
        let fence = proto::DiskRecord {
            uuid: crate::journal::uuid_bytes(
                super::random_producer(),
                proto_gazette::uuid::Clock::from_time(std::time::SystemTime::now()),
                proto_gazette::uuid::Flags::OUTSIDE_TXN,
            ),
            installs_author: bytes::Bytes::copy_from_slice(author.as_bytes()),
            ..Default::default()
        };
        _ = fixture.append_fence(journal, author, &fence).await;

        // An acknowledgement of this journal, from a writer whose records are gone.
        let mut ack = bytes::BytesMut::new();
        proto_gazette::fixed_framing::encode(
            &proto::DiskRecord {
                uuid: crate::journal::uuid_bytes(
                    super::random_producer(),
                    proto_gazette::uuid::Clock::from_time(std::time::SystemTime::now()),
                    proto_gazette::uuid::Flags::ACK_TXN,
                ),
                ..Default::default()
            },
            &mut ack,
        );

        for (what, ack) in [
            ("a fence, no ack", None),
            ("a fence and an ack", Some(ack.freeze())),
        ] {
            let Err(err) = fixture.recover(journal, ack).await else {
                panic!("a disk whose committed state is gone must not be formatted");
            };
            out.push(format!("{what:<20}{}", refusal(&err)));
        }

        insta::assert_snapshot!(out.join("\n"), @"
        nothing, at open    InvalidArgument: journal acmeCo/disk/emptied's recovery-floor label is 0 but its head is 0, so records it committed at or above its floor are gone: its head regressed, or the label is not this journal's (InvalidArgument)
        a fence, no ack     InvalidArgument: a replay of journal acmeCo/disk/emptied applied nothing, but its recovery-floor label says the disk committed state: its committed state was destroyed (InvalidArgument)
        a fence and an ack  InvalidArgument: a replay of journal acmeCo/disk/emptied applied nothing, but the tenure supplied a recovered acknowledgement: its committed state was destroyed (InvalidArgument)
        ");
    }
}
