//! A journal's lifecycle, as a typestate machine. Each step consumes the one
//! before it, so a journal is not written before it's fenced, or served before
//! it's replayed:
//!
//! 1. `Standby::new` replays the journal to its head, handing each committed
//!    delta to the disk's device. The standby then keeps tailing it with
//!    `Standby::follow` until promoted.
//! 2. `Standby::promote` fences the previous writer, finishes replay, and gives
//!    back the sender of the device's replayed chunks.
//! 3. `Promoted::into_writer` hands the journal to a `writer::Writer`, which
//!    appends deltas.

use super::{replay, sequencer, uncommitted, writer};
use crate::device;
use crate::proto;
use anyhow::Context;
use proto_gazette::{broker, fixed_framing, uuid};

/// Replays and tails a journal, without writing to it.
pub struct Standby {
    /// The journal's identity, and the head and floor resolved at open.
    journal: super::Journal,
    /// The journal as replayed so far.
    replay: replay::Replay,
}

impl Standby {
    /// Replay `journal` into `replayed_tx` from its recovery floor, and return once
    /// replay reaches the journal's current head, or fail if the attachment is
    /// cancelled first.
    pub async fn new(
        client: &gazette::journal::Client,
        journal: String,
        cancel: tokio_util::sync::CancellationToken,
        replayed_tx: tokio::sync::mpsc::Sender<(proto::Chunk, bool)>,
        uncommitted: uncommitted::UncommittedDelta,
    ) -> anyhow::Result<Self> {
        let client = client.clone();

        let opening = async {
            // Retry transient failures, as confirming the head and replay do.
            let listing = loop {
                match client.get_journal(&journal).await {
                    Ok(listing) => break listing,
                    Err(err) if err.is_transient() => {
                        tracing::warn!(journal, %err, "listing failed (will retry)");
                        () = tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                    Err(err) => {
                        return Err(anyhow::Error::new(err).context(format!("listing {journal}")));
                    }
                }
            };
            let Some(listing) = listing else {
                return Err(crate::invalid_argument(format!(
                    "journal {journal} does not exist or is not a disk journal",
                )));
            };

            // Read the head after listing, not before. A writer stores a new floor only
            // after appending past it, so a floor listed first is always below a head
            // read afterwards.
            let head = super::confirm_head(&client, &journal).await?;
            let floor = super::listed_floor(&journal, listing, head)?;

            let mut replay = replay::Replay::new(replayed_tx, uncommitted, floor);

            // A journal with no content may be fully suspended, refusing all but
            // appends, so neither read nor tail it. Promotion's fence resumes it, and
            // its read through the fence catches any writes made meanwhile.
            if head != 0 {
                () = replay::read(&client, &journal, Some(head), &mut replay).await?;
            }

            anyhow::Ok(Self {
                journal: super::Journal {
                    name: journal,
                    client,
                    cancel: cancel.clone(),
                    author: random_producer(),
                    head,
                    floor,
                },
                replay,
            })
        };
        super::unless_cancelled(&cancel, opening).await
    }

    /// Tail the journal until reading it fails, and return that failure.
    pub async fn follow(&mut self) -> anyhow::Error {
        if self.journal.head == 0 {
            return std::future::pending().await;
        }
        let super::Journal { client, name, .. } = &self.journal;

        match replay::read(client, name, None, &mut self.replay).await {
            Ok(()) => unreachable!("a tailing read never ends"),
            Err(err) => err,
        }
    }

    /// Fence the previous writer, then finish replay through the fenced head, and
    /// give back the sender of the device's replayed chunks.
    pub async fn promote(
        self,
        recovered_ack: Option<bytes::Bytes>,
    ) -> anyhow::Result<(Promoted, tokio::sync::mpsc::Sender<(proto::Chunk, bool)>)> {
        let cancel = self.journal.cancel.clone();

        let promoting = async move {
            let Self {
                mut journal,
                mut replay,
            } = self;

            let mut appender = fence(&mut journal).await?;

            // Read through the end of the fence. The broker confirmed that offset, so
            // every fragment below it is readable.
            () = replay::read(
                &journal.client,
                &journal.name,
                Some(journal.head),
                &mut replay,
            )
            .await?;

            // Validate repair against the completed replay before appending an ack
            // that could make this and all future replays fail.
            let mut repair = None;

            if let Some(ack) = &recovered_ack {
                let Some((record, producer, clock)) = parse_ack(ack) else {
                    return Err(crate::invalid_argument(
                        "the recovered ack is not a single acknowledgement record".to_string(),
                    ));
                };

                match replay.sequencer().recovered_ack(producer, clock) {
                    sequencer::RecoveredAck::Commits => repair = Some((ack.clone(), record)),
                    sequencer::RecoveredAck::Committed => (),
                    sequencer::RecoveredAck::Refused => {
                        return Err(crate::invalid_argument(format!(
                            "the recovered ack (from {producer:?} at {clock:?}) does not match \
                             the uncommitted delta in journal {}",
                            journal.name,
                        )));
                    }
                }
            }

            if let Some((ack, record)) = repair {
                () = journal
                    .append_ack(&mut appender, &ack)
                    .await
                    .context("repairing a recovered acknowledgement")?;

                // The fence excludes other writers, so the ack landed where replay
                // stopped. Sequence it there, as later replays will.
                () = replay.record(&record, &ack).await?;
            }

            let (replayed_tx, opener, chunks) = replay.into_parts();

            if chunks == 0 && recovered_ack.is_some() {
                return Err(crate::invalid_argument(format!(
                    "replay of journal {} found no committed data, but a recovered ack \
                     shows there was some: the data was lost",
                    journal.name,
                )));
            }

            let promoted = Promoted {
                journal,
                appender,
                opener,
                fresh: chunks == 0,
            };
            anyhow::Ok((promoted, replayed_tx))
        };
        super::unless_cancelled(&cancel, promoting).await
    }
}

/// A replayed journal which this attachment holds, ready for its `writer::Writer`.
pub struct Promoted {
    /// Journal ownership and recovery offsets after promotion.
    journal: super::Journal,
    /// Fenced append pipeline passed to the writer.
    appender: publisher::Appender,
    /// Offset of the last committed record which opened a horizon, which the writer
    /// completes if the device holds it open still.
    opener: Option<i64>,
    /// Whether the journal held no committed data, so the disk must be formatted.
    fresh: bool,
}

impl Promoted {
    /// Whether the journal held no committed data, so the disk must be formatted.
    pub fn fresh(&self) -> bool {
        self.fresh
    }

    /// Start the writer of the device's served mutations, `served_rx`.
    pub fn into_writer(
        self,
        served_rx: tokio::sync::mpsc::Receiver<device::Mutation>,
    ) -> writer::Writer {
        let Self {
            journal,
            appender,
            opener,
            ..
        } = self;

        writer::Writer::spawn(journal, appender, opener, served_rx)
    }
}

/// Fence the previous writer, giving promotion a fixed head to reach, and return the
/// appender every later append goes through.
async fn fence(journal: &mut super::Journal) -> anyhow::Result<publisher::Appender> {
    // The fence appends alone, unioning this attachment's author into the
    // journal's registers.
    let mut fencer = publisher::Appender::new(journal.client.clone(), journal.name.clone())
        .with_union_registers(author_registers(journal.author));

    // Registers change only on a non-empty append, so the fence is a record.
    // Replay never sequences its zero clock.
    let record = proto::DiskRecord {
        uuid: super::uuid_bytes(
            journal.author,
            uuid::Clock::zero(),
            uuid::Flags::OUTSIDE_TXN,
        ),
        ..Default::default()
    };
    fixed_framing::encode(&record, &mut fencer.buffer);

    () = journal
        .flush(&mut fencer)
        .await
        .with_context(|| format!("fencing {}", journal.name))?;

    // The broker refuses later appends with REGISTER_MISMATCH once another fence
    // replaces this author in the journal's registers.
    let appender = publisher::Appender::new(journal.client.clone(), journal.name.clone())
        .with_check_registers(broker::LabelSelector {
            include: Some(author_registers(journal.author)),
            exclude: None,
        });

    Ok(appender)
}

/// Registers which name `author` as a journal's author, as its producer in hex.
fn author_registers(author: uuid::Producer) -> broker::LabelSet {
    let value: String = author
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();

    labels::build_set([("author", value.as_str())])
}

fn random_producer() -> uuid::Producer {
    let mut producer: [u8; 6] = rand::random();
    producer[0] |= 0x01; // Set multicast bit (mark as not a real MAC address).
    uuid::Producer::from_bytes(producer)
}

/// Parse `ack`, which must be exactly one `ACK_TXN` record, with its producer and
/// clock.
fn parse_ack(ack: &[u8]) -> Option<(proto::DiskRecord, uuid::Producer, uuid::Clock)> {
    let mut ack = bytes::BytesMut::from(ack);

    let Ok(fixed_framing::Frame::Record { message, .. }) =
        fixed_framing::unpack::<proto::DiskRecord>(&mut ack)
    else {
        return None;
    };
    let (producer, clock, flags) = uuid::parse(uuid::Uuid::from_slice(&message.uuid).ok()?).ok()?;

    (ack.is_empty() && flags.is_ack()).then_some((message, producer, clock))
}

#[cfg(test)]
mod test {
    use super::{author_registers, parse_ack};
    use crate::proto;
    use crate::test_support;
    use proto_gazette::uuid;

    #[test]
    fn test_an_author_fences_its_register_in_hex() {
        let registers = author_registers(uuid::Producer([0x01, 0x23, 0x45, 0x67, 0x89, 0xab]));

        let labels: Vec<String> = registers
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
        let a = test_support::producer('a');
        let ack = test_support::record(a, 2, uuid::Flags::ACK_TXN, None);
        let frame = |record: proto::DiskRecord| test_support::frame([&record]);

        let cases: [(&str, bytes::BytesMut); 6] = [
            ("an acknowledgement", frame(ack.clone())),
            (
                "unframed bytes",
                bytes::BytesMut::from(&b"not a record"[..]),
            ),
            ("two records", test_support::frame([&ack, &ack])),
            ("no UUID", frame(proto::DiskRecord::default())),
            (
                "a malformed UUID",
                frame(proto::DiskRecord {
                    uuid: bytes::Bytes::from_static(&[0; 16]),
                    ..ack.clone()
                }),
            ),
            (
                "a data record",
                frame(test_support::record(a, 2, uuid::Flags::CONTINUE_TXN, None)),
            ),
        ];

        let mut out = Vec::new();
        for (what, ack) in cases {
            let outcome = match parse_ack(&ack) {
                Some((_record, producer, clock)) => {
                    format!("judged, of {producer:?} at {clock:?}")
                }
                None => "refused".to_string(),
            };
            out.push(format!("{what:<22}{outcome}"));
        }
        insta::assert_snapshot!(out.join("\n"), @"
        an acknowledgement    judged, of Producer(61:00:00:00:00:61) at Clock(0s 2000ns)
        unframed bytes        refused
        two records           refused
        no UUID               refused
        a malformed UUID      refused
        a data record         refused
        ");
    }
}

#[cfg(test)]
mod broker_test {
    use crate::test_support;
    use crate::test_support::broker;

    /// Format the error code and message seen by an attachment client.
    fn refusal(err: &anyhow::Error) -> String {
        match err.downcast_ref::<proto_grpc::StatusError>() {
            Some(status) => format!("{:?}: {err:#}", status.code()),
            None => format!("no code: {err:#}"),
        }
    }

    #[tokio::test]
    async fn test_a_journal_lifecycle_over_a_real_broker() {
        let fixture = broker::Fixture::start().await;

        first_use_fences_the_journal(&fixture).await;
        a_replacement_writer_fences_the_first(&fixture).await;
        an_attachment_which_never_prepares_appends_only_its_fence(&fixture).await;
        an_absent_journal_is_refused_at_open(&fixture).await;
        recovery_applies_only_committed_deltas(&fixture).await;
        a_recovered_acknowledgement_is_repaired(&fixture).await;
        a_stale_recovered_acknowledgement_is_refused(&fixture).await;
        an_orphaned_journal_recovers_nothing(&fixture).await;
        an_acknowledgement_which_landed_before_a_pending_delta_is_recovered(&fixture).await;
        committed_state_which_is_gone_is_refused(&fixture).await;

        fixture.stop().await;
    }

    /// First use fences the journal, then appends its delta under that fence.
    async fn first_use_fences_the_journal(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/first-use";
        let (served_tx, writer) = fixture.open(journal).await.unwrap();

        served_tx
            .try_send(broker::mutation(test_support::punch_chunk(3, 2)))
            .unwrap();
        let ack = writer
            .prepare()
            .await
            .unwrap()
            .expect("the delta is not empty");
        () = writer.acknowledge(ack).await.unwrap();

        let out = broker::render(&fixture.read(journal).await);

        insta::assert_snapshot!(out, @"
        fence     author 1
        continue  author 1  blocks 3..5: punch
        ack       author 1
        ");
    }

    /// A replacement attachment takes the author register. The first attachment then
    /// cannot append.
    async fn a_replacement_writer_fences_the_first(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/contended";
        let (first_served_tx, first) = fixture.open(journal).await.unwrap();

        first_served_tx
            .try_send(broker::mutation(test_support::punch_chunk(0, 1)))
            .unwrap();
        let ack = first.prepare().await.unwrap().unwrap();
        () = first.acknowledge(ack).await.unwrap();

        // The journal now holds a committed delta, so a replacement fences it as that
        // replacement recovers.
        let (_second_served_tx, _second, _blocks) = fixture.recover(journal, None).await.unwrap();

        first_served_tx
            .try_send(broker::mutation(test_support::punch_chunk(1, 1)))
            .unwrap();

        // The append which lost the fence fails as `ABORTED`, which tells its client
        // that another attachment owns the disk. The failure is terminal.
        let mut out = Vec::new();
        for what in ["fenced-out prepare", "later prepare"] {
            let err = first.prepare().await.unwrap_err();
            out.push(format!("{what:<20}{}", refusal(&err)));
        }
        insta::assert_snapshot!(out.join("\n"), @"
        fenced-out prepare  Aborted: journal acmeCo/disk/contended registers do not match (Aborted)
        later prepare       Aborted: journal acmeCo/disk/contended registers do not match (Aborted)
        ");
    }

    /// An attachment which prepares nothing leaves its fence behind and nothing else.
    /// The fence is what makes it the journal's writer, and it happens whether or not
    /// the disk is ever written.
    async fn an_attachment_which_never_prepares_appends_only_its_fence(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/untouched";
        let (served_tx, writer) = fixture.open(journal).await.unwrap();

        let ack = writer.prepare().await.unwrap();
        drop((served_tx, writer));

        let mut out = vec![format!("prepared  {ack:?}")];
        out.push(broker::render(&fixture.read(journal).await));

        insta::assert_snapshot!(out.join("\n"), @"
        prepared  None
        fence     author 1
        ");
    }

    /// A journal nothing created is what the attachment asked for, and no retry of that
    /// `Open` could find one, because the daemon creates none.
    async fn an_absent_journal_is_refused_at_open(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/never-created";

        let Err(err) = fixture.standby_uncreated(journal).await else {
            panic!("a journal which does not exist must not open");
        };
        insta::assert_snapshot!(refusal(&err), @"InvalidArgument: journal acmeCo/disk/never-created does not exist or is not a disk journal (InvalidArgument)");
    }

    /// Recovery rebuilds the deltas which committed. It discards a delta whose
    /// acknowledgement never reached the journal.
    async fn recovery_applies_only_committed_deltas(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/recovered";
        let (served_tx, writer) = fixture.open(journal).await.unwrap();

        for (block, fill) in [(1, 0xaa), (2, 0xbb)] {
            served_tx.try_send(broker::write(block, fill)).unwrap();
        }
        let ack = writer.prepare().await.unwrap().unwrap();
        () = writer.acknowledge(ack).await.unwrap();

        // A second delta, prepared but never committed. An attachment which crashed
        // between the two leaves this behind.
        served_tx.try_send(broker::write(2, 0xcc)).unwrap();
        served_tx.try_send(broker::write(3, 0xdd)).unwrap();
        _ = writer.prepare().await.unwrap().unwrap();
        drop((served_tx, writer));

        let (_served_tx, _writer, blocks) = fixture.recover(journal, None).await.unwrap();
        assert_eq!(blocks, vec![(1, 0xaa), (2, 0xbb)]);
    }

    /// The client made an acknowledgement durable, but it never reached the journal.
    /// Recovery appends it verbatim, which commits the delta it acknowledges.
    async fn a_recovered_acknowledgement_is_repaired(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/repaired";
        let (served_tx, writer) = fixture.open(journal).await.unwrap();

        served_tx.try_send(broker::write(4, 0x11)).unwrap();
        let ack = writer.prepare().await.unwrap().unwrap();
        drop((served_tx, writer));

        let (_served_tx, _writer, blocks) =
            fixture.recover(journal, Some(ack.clone())).await.unwrap();
        let mut out = vec![format!(
            "repaired        {}",
            test_support::render_blocks(&blocks)
        )];

        // A second repair hands back the same acknowledgement, which the journal now
        // holds, so nothing is appended for it. An attachment which repeats a repair
        // therefore recovers the same disk.
        let (_served_tx, _writer, blocks) = fixture.recover(journal, Some(ack)).await.unwrap();
        out.push(format!(
            "repaired again  {}",
            test_support::render_blocks(&blocks)
        ));

        insta::assert_snapshot!(out.join("\n"), @"
        repaired        4..5 filled 0x11
        repaired again  4..5 filled 0x11
        ");
    }

    /// A recovered acknowledgement which a replay could not honor is refused before it
    /// is appended. Its delta was displaced by an attachment which promoted without it,
    /// and a journal which held that acknowledgement would fail every replay from then
    /// on.
    async fn a_stale_recovered_acknowledgement_is_refused(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/stale-ack";
        let (served_tx, writer) = fixture.open(journal).await.unwrap();

        served_tx.try_send(broker::write(1, 0x11)).unwrap();
        let stale = writer.prepare().await.unwrap().unwrap();
        drop((served_tx, writer));

        // An attachment which promoted without that acknowledgement, and wrote past it.
        let (served_tx, writer, blocks) = fixture.recover(journal, None).await.unwrap();
        let mut out = vec![format!(
            "promoted without it   {}",
            test_support::render_blocks(&blocks)
        )];

        served_tx.try_send(broker::write(2, 0x22)).unwrap();
        let ack = writer.prepare().await.unwrap().unwrap();
        () = writer.acknowledge(ack).await.unwrap();
        drop((served_tx, writer));

        // Normalize the acknowledgement's random author and clock for the snapshot.
        let (_record, author, clock) = super::parse_ack(&stale).unwrap();
        let Err(err) = fixture.recover(journal, Some(stale)).await else {
            panic!("an acknowledgement a replay could not honor must not be appended");
        };
        let refused = refusal(&err)
            .replace(&format!("{author:?}"), "<its author>")
            .replace(&format!("{clock:?}"), "<its clock>");
        out.push(format!("the stale ack         {refused}"));

        // Nothing of it reached the journal, so the disk still recovers.
        let (_served_tx, _writer, blocks) = fixture.recover(journal, None).await.unwrap();
        out.push(format!(
            "recovered after       {}",
            test_support::render_blocks(&blocks)
        ));

        insta::assert_snapshot!(out.join("\n"), @"
        promoted without it   none
        the stale ack         InvalidArgument: the recovered ack (from <its author> at <its clock>) does not match the uncommitted delta in journal acmeCo/disk/stale-ack (InvalidArgument)
        recovered after       2..3 filled 0x22
        ");
    }

    /// A journal which a failed first use left content in holds no committed state, so
    /// its disk is fresh.
    async fn an_orphaned_journal_recovers_nothing(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/orphaned";
        let (served_tx, writer) = fixture.open(journal).await.unwrap();

        served_tx.try_send(broker::write(5, 0x22)).unwrap();
        _ = writer.prepare().await.unwrap().unwrap();
        drop((served_tx, writer));

        assert!(
            fixture.head(journal).await > 0,
            "the delta reached the journal"
        );

        let (_served_tx, _writer, blocks) = fixture.recover(journal, None).await.unwrap();
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
        let (served_tx, writer) = fixture.open(journal).await.unwrap();

        served_tx.try_send(broker::write(1, 0xaa)).unwrap();
        let ack = writer.prepare().await.unwrap().unwrap();
        () = writer.acknowledge(ack.clone()).await.unwrap();

        // The next transaction writes, which the writer appends at once.
        let head = fixture.head(journal).await;
        served_tx.try_send(broker::write(2, 0xbb)).unwrap();
        () = fixture.advances(journal, head).await;
        drop((served_tx, writer));

        // The client failed before it committed the next transaction, so its checkpoint
        // still holds the acknowledgement it last committed.
        let (_served_tx, _second, blocks) = fixture.recover(journal, Some(ack)).await.unwrap();
        let mut out = vec![format!(
            "blocks    {}",
            test_support::render_blocks(&blocks)
        )];

        // The promotion appended nothing after its fence.
        out.push(broker::render(&fixture.read(journal).await));

        insta::assert_snapshot!(out.join("\n"), @"
        blocks    1..2 filled 0xaa
        fence     author 1
        continue  author 1  blocks 1..2: data len 4096 [0xaax4096]
        ack       author 1
        continue  author 1  blocks 2..3: data len 4096 [0xbbx4096]
        fence     author 2
        ");
    }

    /// A journal which holds no committed state recovers a fresh disk for a client with
    /// no acknowledgement to hand back. A client which hands one back committed state
    /// the journal no longer holds, as where its fragments were deleted, so promotion
    /// refuses rather than formatting over it.
    async fn committed_state_which_is_gone_is_refused(fixture: &broker::Fixture) {
        let journal = "acmeCo/disk/emptied";

        // An acknowledgement of this journal, from a writer whose records are gone.
        let ack = test_support::frame([&test_support::record(
            test_support::producer('b'),
            1,
            proto_gazette::uuid::Flags::ACK_TXN,
            None,
        )]);

        let mut out = Vec::new();
        for (what, ack) in [("no ack", None), ("an ack", Some(ack.freeze()))] {
            let outcome = match fixture.recover(journal, ack).await {
                Ok((_served_tx, _writer, blocks)) => {
                    format!("recovered {}", test_support::render_blocks(&blocks))
                }
                Err(err) => refusal(&err),
            };
            out.push(format!("{what:<8}{outcome}"));
        }
        insta::assert_snapshot!(out.join("\n"), @"
        no ack  recovered none
        an ack  InvalidArgument: replay of journal acmeCo/disk/emptied found no committed data, but a recovered ack shows there was some: the data was lost (InvalidArgument)
        ");
    }
}
