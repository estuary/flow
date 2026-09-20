//! The writer against a real broker.
//!
//! These are the scenarios which are about what reaches a journal: which records a
//! delta appends, in what order, under whose claim, and what a replay of them
//! rebuilds. A data plane is expensive to start, so one test drives them all, each
//! over a journal of its own, and holds each scenario's [`Capture`] for the length
//! of that scenario — dropping it closes the capture channel, which is how a
//! tenure ends.
//!
//! What a client sees of all this is `tests/`, over the daemon as it ships.

use crate::capture::Capture;
use crate::chunk::{covered_blocks, encode_punch, encode_write};
use crate::proto;
use crate::test_support::broker::Fixture;
use crate::{BLOCK_SIZE, journal::fence};
use proto_gazette::broker;

#[tokio::test]
async fn test_a_writer_over_a_real_broker() {
    let fixture = Fixture::start().await;

    first_use_claims_the_journal(&fixture).await;
    a_replacement_writer_fences_the_first(&fixture).await;
    a_tenure_which_never_prepares_appends_only_its_fence(&fixture).await;
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

    fixture.stop().await;
}

/// First use claims the journal. It installs the author register with a fence
/// record, then appends its delta under that claim.
async fn first_use_claims_the_journal(fixture: &Fixture) {
    let journal = "acmeCo/disk/first-use";
    let (capture, writer) = fixture.open(journal).await.unwrap();

    capture.offer(vec![encode_punch(3, 2)]).unwrap();
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
        Some(fence::value(writer.epoch()).as_str()),
    );
}

/// A replacement tenure takes the author register. The first tenure then cannot
/// append.
async fn a_replacement_writer_fences_the_first(fixture: &Fixture) {
    let journal = "acmeCo/disk/contended";
    let (first_capture, first) = fixture.open(journal).await.unwrap();

    first_capture.offer(vec![encode_punch(0, 1)]).unwrap();
    let ack = first.prepare().await.unwrap().unwrap();
    () = first.acknowledge(ack).await.unwrap();

    // The journal now holds a committed delta, so a replacement claims it as that
    // replacement recovers.
    let (_second_capture, second, _blocks) = fixture.recover(journal, Vec::new()).await.unwrap();
    assert_ne!(first.epoch(), second.epoch());

    first_capture.offer(vec![encode_punch(1, 1)]).unwrap();
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
    let (capture, writer) = fixture.open(journal).await.unwrap();

    let epoch = writer.epoch();
    assert_eq!(writer.prepare().await.unwrap(), None);
    drop((capture, writer));

    let records = fixture.read(journal).await;
    assert_eq!(records.len(), 1, "{records:?}");

    let (_producer, _clock, flags) = records[0].0;
    assert!(flags.is_outside(), "a fence is outside a transaction");
    assert_eq!(records[0].1.installs_epoch, epoch.as_bytes()[..]);
}

/// A journal nothing created is what the tenure asked for, and no retry of that
/// `Open` could find one, because the daemon creates none.
async fn an_absent_journal_is_refused_at_open(fixture: &Fixture) {
    let journal = "acmeCo/disk/never-created";

    let Err(err) = fixture.opening_uncreated(journal).await else {
        panic!("a journal which does not exist must not open");
    };
    assert!(format!("{err:#}").contains("does not exist"), "{err:#}");
    assert!(err.chain().any(|cause| matches!(
        cause.downcast_ref::<crate::Failure>(),
        Some(crate::Failure::Invalid(_)),
    )));
}

/// A committed delta reads back as exactly the chunks which were captured.
async fn a_committed_delta_reads_back_as_its_chunks(fixture: &Fixture) {
    let journal = "acmeCo/disk/delta";
    let (capture, writer) = fixture.open(journal).await.unwrap();

    let mutations = vec![
        encode_write(0, &bytes::Bytes::from(vec![0x11; 8192])),
        encode_write(2, &bytes::Bytes::from(vec![0; 4096])),
        vec![encode_punch(3, 4)],
    ];
    for mutation in &mutations {
        capture.offer(mutation.clone()).unwrap();
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
/// the capture channel instead, and the writer takes them once the acknowledgement
/// has landed, so the journal holds the second delta above the first delta's commit
/// and a recovery of it holds both.
async fn mutations_offered_while_a_commit_is_outstanding_wait_for_it(fixture: &Fixture) {
    let journal = "acmeCo/disk/outstanding";
    let (capture, writer) = fixture.open(journal).await.unwrap();

    let epoch = writer.epoch();

    capture.offer(write(1, 0xaa)).unwrap();
    let first = writer.prepare().await.unwrap().unwrap();

    // Mutations of the second delta, offered while the client holds the first
    // delta's acknowledgement. The writer leaves them in the channel.
    capture.offer(write(2, 0xbb)).unwrap();
    capture.offer(write(3, 0xcc)).unwrap();
    () = tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        !capture.is_empty(),
        "the writer took a mutation while an acknowledgement was outstanding",
    );

    () = writer.acknowledge(first).await.unwrap();
    () = drains(&capture).await;

    let second = writer.prepare().await.unwrap().unwrap();
    () = writer.acknowledge(second).await.unwrap();
    drop((capture, writer));

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

    let (_capture, _writer, blocks) = fixture.recover(journal, Vec::new()).await.unwrap();
    assert_eq!(blocks, vec![(1, 0xaa), (2, 0xbb), (3, 0xcc)]);
}

/// A delta of many mutations carries exactly one record per mutation. Those records
/// cover exactly the blocks the mutations wrote.
async fn a_large_delta_carries_one_record_per_mutation(fixture: &Fixture) {
    let journal = "acmeCo/disk/bounded";
    const WRITES: usize = 8;

    let (capture, writer) = fixture.open(journal).await.unwrap();
    let write = encode_write(0, &bytes::Bytes::from(vec![0x22; 128 * 1024]));

    for _ in 0..WRITES {
        capture.offer(write.clone()).unwrap();
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
/// holds: one record per mutation, in the order they were captured, and a
/// recovery which rebuilds the disk from them.
async fn a_delta_which_spans_several_appends_keeps_its_records(fixture: &Fixture) {
    let journal = "acmeCo/disk/batched";

    // Enough of the disk, enough times over, to carry the delta past the
    // threshold at which the appender stops buffering and waits.
    const MUTATIONS: usize = 40;
    let each = crate::test_support::broker::BLOCKS as usize * BLOCK_SIZE as usize;
    assert!(MUTATIONS * each > publisher::Appender::BUFFER_FLUSH_THRESHOLD);

    let (capture, writer) = fixture.open(journal).await.unwrap();

    for index in 0..MUTATIONS {
        let fill = (index + 1) as u8;
        capture
            .offer(encode_write(0, &bytes::Bytes::from(vec![fill; each])))
            .unwrap();
    }

    let ack = writer.prepare().await.unwrap().unwrap();
    () = writer.acknowledge(ack).await.unwrap();
    drop((capture, writer));

    // Each mutation is one record, and their order is the order they were
    // captured in: the fill byte of each identifies which.
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

    let (_capture, _writer, blocks) = fixture.recover(journal, Vec::new()).await.unwrap();

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
    let opening = fixture.opening(journal).await.unwrap();

    // Every append checks the epoch this installs, so the claim comes first even
    // though the scenario drives the writer's primitives directly.
    let claimed = opening.claim_journal().await.unwrap();

    // The actor, but not spawned: this drives its primitives itself.
    let mut task = claimed.into_task(None);

    let each = crate::test_support::broker::BLOCKS as usize * BLOCK_SIZE as usize;
    let write = || encode_write(0, &bytes::Bytes::from(vec![0x55; each]));

    // Records ahead of the horizon, batched by the appender.
    for _ in 0..8 {
        () = task.append_mutation(write(), false).await.unwrap();
    }
    () = task.flush().await.unwrap();

    // Cut, so that the next record is a delta's first and opens a horizon.
    task.delta_records = 0;

    () = task.append_mutation(write(), true).await.unwrap();
    let horizon = task.horizon.expect("a horizon was opened");

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
        let (capture, writer) = fixture.open(journal).await.unwrap();

        if has_delta {
            capture.offer(write(6, 0x33)).unwrap();
            () = drains(&capture).await;
        }
        () = writer.abandon();

        // Let the writer drop its appender and drain an unmount mutation
        // before a request arrives.
        capture.offer(write(7, 0x44)).unwrap();
        () = drains(&capture).await;

        let err = tokio::time::timeout(std::time::Duration::from_secs(10), writer.prepare())
            .await
            .expect("an abandoned tenure answers rather than waiting")
            .expect_err("an abandoned tenure commits nothing");

        assert!(format!("{err:#}").contains("the tenure ended"), "{err:#}");
    }
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

    let Err(err) = fixture.opening_uncreated(journal).await else {
        panic!("a journal this daemon cannot append to must not open");
    };
    assert!(format!("{err:#}").contains("must be read-write"), "{err:#}");
}

/// Recovery rebuilds the deltas which committed. It discards a delta whose
/// acknowledgement never reached the journal.
async fn recovery_applies_only_committed_deltas(fixture: &Fixture) {
    let journal = "acmeCo/disk/recovered";
    let (capture, writer) = fixture.open(journal).await.unwrap();

    for (block, fill) in [(1, 0xaa), (2, 0xbb)] {
        capture.offer(write(block, fill)).unwrap();
    }
    let ack = writer.prepare().await.unwrap().unwrap();
    () = writer.acknowledge(ack).await.unwrap();

    // A second delta, prepared but never committed. A tenure which crashed
    // between the two leaves this behind.
    capture.offer(write(2, 0xcc)).unwrap();
    capture.offer(write(3, 0xdd)).unwrap();
    _ = writer.prepare().await.unwrap().unwrap();
    drop((capture, writer));

    let (_capture, _writer, blocks) = fixture.recover(journal, Vec::new()).await.unwrap();
    assert_eq!(blocks, vec![(1, 0xaa), (2, 0xbb)]);
}

/// The client made an acknowledgement durable, but it never reached the journal.
/// Recovery appends it verbatim, which commits the delta it acknowledges.
async fn a_recovered_acknowledgement_is_repaired(fixture: &Fixture) {
    let journal = "acmeCo/disk/repaired";
    let (capture, writer) = fixture.open(journal).await.unwrap();

    capture.offer(write(4, 0x11)).unwrap();
    let ack = writer.prepare().await.unwrap().unwrap();
    drop((capture, writer));

    let (_capture, _writer, blocks) = fixture.recover(journal, vec![ack.clone()]).await.unwrap();

    assert_eq!(blocks, vec![(4, 0x11)]);

    // A second repair re-appends the same bytes, and Gazette de-duplicates those by
    // UUID. A tenure which repeats a repair therefore recovers the same disk.
    let (_capture, _writer, blocks) = fixture.recover(journal, vec![ack]).await.unwrap();
    assert_eq!(blocks, vec![(4, 0x11)]);
}

/// A recovered acknowledgement which a replay could not honor is refused before it
/// is appended. Its delta was displaced by a tenure which promoted without it, and a
/// journal which held that acknowledgement would fail every replay from then on.
async fn a_stale_recovered_acknowledgement_is_refused(fixture: &Fixture) {
    let journal = "acmeCo/disk/stale-ack";
    let (capture, writer) = fixture.open(journal).await.unwrap();

    capture.offer(write(1, 0x11)).unwrap();
    let stale = writer.prepare().await.unwrap().unwrap();
    drop((capture, writer));

    // A tenure which promoted without that acknowledgement, and wrote past it.
    let (capture, writer, blocks) = fixture.recover(journal, Vec::new()).await.unwrap();
    assert!(blocks.is_empty(), "{blocks:?}");

    capture.offer(write(2, 0x22)).unwrap();
    let ack = writer.prepare().await.unwrap().unwrap();
    () = writer.acknowledge(ack).await.unwrap();
    drop((capture, writer));

    let Err(err) = fixture.recover(journal, vec![stale]).await else {
        panic!("an acknowledgement a replay could not honor must not be appended");
    };
    assert!(
        err.chain().any(|cause| matches!(
            cause.downcast_ref::<crate::Failure>(),
            Some(crate::Failure::Invalid(_)),
        )),
        "{err:#}",
    );

    // Nothing of it reached the journal, so the disk still recovers.
    let (_capture, _writer, blocks) = fixture.recover(journal, Vec::new()).await.unwrap();
    assert_eq!(blocks, vec![(2, 0x22)]);
}

/// A journal which a failed first use left content in holds no committed state, so
/// its disk is fresh.
async fn an_orphaned_journal_recovers_nothing(fixture: &Fixture) {
    let journal = "acmeCo/disk/orphaned";
    let (capture, writer) = fixture.open(journal).await.unwrap();

    capture.offer(write(5, 0x22)).unwrap();
    _ = writer.prepare().await.unwrap().unwrap();
    drop((capture, writer));

    assert!(
        fixture.head(journal).await > 0,
        "the delta reached the journal"
    );

    let (_capture, _writer, blocks) = fixture.recover(journal, Vec::new()).await.unwrap();
    assert!(blocks.is_empty(), "{blocks:?}");
}

/// One block of `fill`, as a device write of it encodes.
fn write(block: u32, fill: u8) -> Vec<proto::Chunk> {
    encode_write(block, &bytes::Bytes::from(vec![fill; BLOCK_SIZE as usize]))
}

/// Wait for the writer to take every mutation the capture channel holds.
///
/// The writer takes on a task of its own, so a scenario which asserts what it did
/// with a mutation must first see that it has one.
async fn drains(capture: &Capture) {
    for _ in 0..1000 {
        if capture.is_empty() {
            return;
        }
        () = tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("the writer did not take the mutations offered to it");
}
