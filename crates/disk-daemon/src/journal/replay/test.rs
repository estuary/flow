use super::Pass;
use crate::BLOCK_SIZE;
use crate::chunk::{encode_punch, encode_write};
use crate::image::Image;
use crate::proto;
use crate::test_support;
use proto_gazette::{fixed_framing, uuid};

const BLOCKS: u32 = 64;

fn producer(seed: u8) -> uuid::Producer {
    uuid::Producer::from_bytes([seed | 0x01, 0, 0, 0, 0, seed])
}

/// A clock `ticks` microseconds after the epoch. Each case then reads as a
/// sequence of small numbers.
fn clock(ticks: u64) -> uuid::Clock {
    let mut clock = uuid::Clock::UNIX_EPOCH;
    for _ in 0..ticks {
        _ = clock.tick();
    }
    clock
}

fn record(
    producer: uuid::Producer,
    ticks: u64,
    flags: uuid::Flags,
    chunks: Vec<proto::Chunk>,
) -> proto::DiskRecord {
    proto::DiskRecord {
        uuid: bytes::Bytes::copy_from_slice(
            uuid::build(producer, clock(ticks), flags)
                .as_bytes()
                .as_slice(),
        ),
        chunks,
        opens_horizon: false,
        installs_epoch: bytes::Bytes::new(),
    }
}

/// `record` as the first of a delta which opens a horizon.
fn opens(record: proto::DiskRecord) -> proto::DiskRecord {
    proto::DiskRecord {
        opens_horizon: true,
        ..record
    }
}

fn write(producer: uuid::Producer, clock: u64, block: u32, fill: u8) -> proto::DiskRecord {
    record(
        producer,
        clock,
        uuid::Flags::CONTINUE_TXN,
        encode_write(block, &bytes::Bytes::from(vec![fill; BLOCK_SIZE as usize])),
    )
}

fn ack(producer: uuid::Producer, clock: u64) -> proto::DiskRecord {
    record(producer, clock, uuid::Flags::ACK_TXN, Vec::new())
}

fn fence(producer: uuid::Producer, clock: u64, installs: uuid::Producer) -> proto::DiskRecord {
    proto::DiskRecord {
        installs_epoch: bytes::Bytes::copy_from_slice(installs.as_bytes()),
        ..record(producer, clock, uuid::Flags::OUTSIDE_TXN, Vec::new())
    }
}

/// `record` framed exactly as the journal frames it. A pass keeps the journal's
/// own bytes of a held record and decodes them again when its delta commits, so
/// every case frames for real rather than standing in for these bytes.
fn frame(record: &proto::DiskRecord) -> bytes::BytesMut {
    let mut framed = bytes::BytesMut::new();
    fixed_framing::encode(record, &mut framed);

    framed
}

/// Journal offset at which the record at `index` begins.
fn offset_of(records: &[proto::DiskRecord], index: usize) -> i64 {
    records[..index]
        .iter()
        .map(|record| frame(record).len() as i64)
        .sum()
}

/// Replay `records` through a pass, which holds the delta in doubt until its
/// acknowledgement arrives. Returns the pass alongside each block's fill byte.
fn replay(dir: &tempfile::TempDir, records: &[proto::DiskRecord]) -> (Pass, Image, Vec<(u32, u8)>) {
    let mut image = Image::create(dir.path(), BLOCKS).unwrap();
    let mut pass = Pass::new(super::Buffer::create(dir.path()).unwrap());
    let mut offset = 0;

    for record in records {
        let framed = frame(record);

        _ = pass.record(record, &framed, offset, &mut image).unwrap();
        offset += framed.len() as i64;
    }
    let blocks = test_support::allocated(&image);

    (pass, image, blocks)
}

/// Blocks a replay of `records` leaves allocated, and their fill bytes.
fn replayed(dir: &tempfile::TempDir, records: &[proto::DiskRecord]) -> Vec<(u32, u8)> {
    replay(dir, records).2
}

/// A delta reaches the image only at its acknowledgement, and all of it lands
/// there at once.
#[test]
fn test_a_delta_applies_at_its_acknowledgement() {
    let dir = tempfile::tempdir().unwrap();
    let (a, f) = (producer(0x10), producer(0x20));

    let (pass, _image, blocks) = replay(
        &dir,
        &[
            fence(f, 1, a),
            write(a, 2, 3, 0xaa),
            write(a, 3, 4, 0xbb),
            ack(a, 4),
        ],
    );
    assert_eq!(blocks, vec![(3, 0xaa), (4, 0xbb)]);

    let (held, _floor, _horizon) = pass.into_parts();
    assert!(held.is_empty(), "the acknowledged delta was released");
}

/// The delta which is still in doubt stays held, and nothing of it reaches the
/// image. That is what keeps a standby's image at its client's committed edge.
#[test]
fn test_a_delta_still_in_doubt_is_held_and_not_applied() {
    let dir = tempfile::tempdir().unwrap();
    let a = producer(0x10);

    let (pass, _image, blocks) = replay(
        &dir,
        &[
            write(a, 1, 3, 0xaa),
            ack(a, 2),
            // A delta the tenure never acknowledged.
            write(a, 3, 4, 0xbb),
            write(a, 4, 5, 0xcc),
        ],
    );
    assert_eq!(blocks, vec![(3, 0xaa)]);

    let (held, _floor, _horizon) = pass.into_parts();
    assert!(!held.is_empty(), "the delta in doubt is still held");
}

/// A delta which a replacement tenure's records follow is abandoned. Those
/// records displace it, so it is dropped rather than carried for the rest of the
/// pass, and nothing of it is ever applied.
#[test]
fn test_a_delta_a_replacement_tenure_abandoned_is_not_applied() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b, f) = (producer(0x10), producer(0x30), producer(0x20));

    let (pass, _image, blocks) = replay(
        &dir,
        &[
            write(a, 1, 3, 0xaa),
            ack(a, 2),
            write(a, 3, 5, 0xcc),
            fence(f, 4, b),
            write(b, 5, 6, 0xee),
            ack(b, 6),
        ],
    );
    assert_eq!(blocks, vec![(3, 0xaa), (6, 0xee)]);

    let (held, _floor, _horizon) = pass.into_parts();
    assert!(held.is_empty(), "the displaced delta was dropped");
}

/// A promotion repairs the acknowledgement its client held before it appends
/// anything of its own, so the delta that fence displaced is still applied.
#[test]
fn test_a_delta_a_promotion_repaired_is_applied() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b, f) = (producer(0x10), producer(0x30), producer(0x20));

    assert_eq!(
        replayed(
            &dir,
            &[
                write(a, 1, 3, 0xaa),
                fence(f, 2, b),
                ack(a, 3),
                write(b, 4, 5, 0xcc),
                ack(b, 5),
            ],
        ),
        vec![(3, 0xaa), (5, 0xcc)],
    );
}

/// The range may begin within a delta. That delta's acknowledgement then
/// commits only the records which were in range.
#[test]
fn test_a_delta_which_begins_below_the_range_commits_what_is_in_it() {
    let dir = tempfile::tempdir().unwrap();
    let a = producer(0x10);

    assert_eq!(
        replayed(&dir, &[write(a, 5, 2, 0xaa), ack(a, 9)]),
        vec![(2, 0xaa)],
    );
}

/// At-least-once appends repeat records. Sequencing drops a repeat rather than
/// applying it a second time over a newer value.
#[test]
fn test_duplicate_records_are_not_applied_again() {
    let dir = tempfile::tempdir().unwrap();
    let a = producer(0x10);

    assert_eq!(
        replayed(
            &dir,
            &[
                write(a, 1, 2, 0xaa),
                write(a, 2, 2, 0xbb),
                write(a, 1, 2, 0xaa),
                ack(a, 3),
                ack(a, 3),
            ],
        ),
        vec![(2, 0xbb)],
    );
}

#[test]
fn test_a_punch_deallocates_what_an_earlier_delta_wrote() {
    let dir = tempfile::tempdir().unwrap();
    let a = producer(0x10);

    assert_eq!(
        replayed(
            &dir,
            &[
                write(a, 1, 8, 0xaa),
                write(a, 2, 9, 0xbb),
                ack(a, 3),
                record(a, 4, uuid::Flags::CONTINUE_TXN, vec![encode_punch(8, 1)]),
                ack(a, 5),
            ],
        ),
        vec![(9, 0xbb)],
    );
}

/// A record which opens a horizon snapshots the blocks allocated before its own
/// chunks apply. The acknowledgement of the delta which discharges the last of
/// those blocks moves the floor to that record. The horizon is held with its
/// delta until then, exactly as that delta's chunks are.
#[test]
fn test_a_discharged_horizon_derives_the_floor() {
    let dir = tempfile::tempdir().unwrap();
    let a = producer(0x10);

    let records = [
        write(a, 1, 3, 0xaa),
        write(a, 2, 4, 0xbb),
        ack(a, 3),
        // This delta opens a horizon over both blocks and rewrites them, which
        // discharges the horizon without any copy.
        opens(write(a, 4, 3, 0xcc)),
        write(a, 5, 4, 0xdd),
        ack(a, 6),
    ];
    let (pass, _image, blocks) = replay(&dir, &records);

    assert_eq!(blocks, vec![(3, 0xcc), (4, 0xdd)]);

    let (_held, floor, horizon) = pass.into_parts();

    assert!(horizon.is_none(), "the horizon completed");
    assert_eq!(
        floor.expect("a floor was derived"),
        offset_of(&records, 3),
        "the floor is the offset of the record which opened the horizon",
    );
}

/// The next tenure resumes a horizon the range leaves open. The pass holds both
/// halves of it: where it opened, and what it has left to discharge.
#[test]
fn test_an_open_horizon_outlives_the_pass() {
    let dir = tempfile::tempdir().unwrap();
    let a = producer(0x10);

    let records = [
        write(a, 1, 3, 0xaa),
        write(a, 2, 4, 0xbb),
        ack(a, 3),
        opens(write(a, 4, 3, 0xcc)),
        ack(a, 5),
    ];
    let (pass, _image, _blocks) = replay(&dir, &records);
    let (_held, floor, horizon) = pass.into_parts();
    let horizon = horizon.expect("a horizon is open");

    assert!(floor.is_none());
    assert_eq!(horizon.at, offset_of(&records, 3));
    assert_eq!(
        horizon.blocks.pending(),
        1,
        "the block it still owes a copy"
    );
}

/// A range may hold several horizons. Each one replaces the one before it, so
/// the floor is the last horizon which a delta discharged.
#[test]
fn test_a_later_horizon_replaces_an_earlier_one() {
    let dir = tempfile::tempdir().unwrap();
    let a = producer(0x10);

    let records = [
        write(a, 1, 3, 0xaa),
        write(a, 2, 4, 0xbb),
        ack(a, 3),
        opens(write(a, 4, 3, 0xcc)),
        ack(a, 5),
        opens(write(a, 6, 3, 0xdd)),
        write(a, 7, 4, 0xee),
        ack(a, 8),
    ];
    let (pass, _image, _blocks) = replay(&dir, &records);
    let (_held, floor, horizon) = pass.into_parts();

    assert!(horizon.is_none(), "the second horizon completed");
    assert_eq!(
        floor.expect("the second horizon completed"),
        offset_of(&records, 5),
    );
}

/// A horizon belongs to its delta. A horizon whose delta is never acknowledged
/// never existed, exactly as its chunks never applied.
#[test]
fn test_a_horizon_of_an_uncommitted_delta_does_not_exist() {
    let dir = tempfile::tempdir().unwrap();
    let a = producer(0x10);

    let (pass, _image, blocks) = replay(
        &dir,
        &[write(a, 1, 3, 0xaa), ack(a, 2), opens(write(a, 3, 4, 0xbb))],
    );

    assert_eq!(blocks, vec![(3, 0xaa)]);

    let (_held, floor, horizon) = pass.into_parts();
    assert!(horizon.is_none() && floor.is_none());
}

#[test]
fn test_malformed_records_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let mut image = Image::create(dir.path(), BLOCKS).unwrap();
    let (a, f) = (producer(0x10), producer(0x20));

    let cases: [(proto::DiskRecord, &str); 5] = [
        (
            proto::DiskRecord {
                uuid: bytes::Bytes::from_static(b"short"),
                ..write(a, 1, 0, 0xaa)
            },
            "no message UUID",
        ),
        (
            proto::DiskRecord {
                opens_horizon: true,
                ..ack(a, 1)
            },
            "does not begin a delta",
        ),
        (
            proto::DiskRecord {
                installs_epoch: bytes::Bytes::from_static(b"nope"),
                ..fence(f, 1, a)
            },
            "4 bytes of epoch",
        ),
        (
            proto::DiskRecord {
                chunks: vec![encode_punch(0, 1)],
                ..fence(f, 1, a)
            },
            "a fence carries 1 chunks",
        ),
        (
            proto::DiskRecord {
                chunks: vec![encode_punch(0, 1)],
                ..ack(a, 9)
            },
            "an acknowledgement carries 1 chunks",
        ),
    ];

    // Each case is the first record of its own pass, so no case is rejected for
    // the sequencing state another one left behind.
    for (record, expect) in cases {
        let err = Pass::new(super::Buffer::create(dir.path()).unwrap())
            .record(&record, &frame(&record), 0, &mut image)
            .unwrap_err();

        assert!(format!("{err:#}").contains(expect), "{expect}: {err:#}");
    }
}

/// Sequence `records` through one pass, and report the failure of the last of
/// them. Every record before it must be accepted.
fn refused(dir: &tempfile::TempDir, records: &[proto::DiskRecord]) -> String {
    let mut image = Image::create(dir.path(), BLOCKS).unwrap();
    let mut pass = Pass::new(super::Buffer::create(dir.path()).unwrap());

    let (last, accepted) = records.split_last().expect("a case has records");

    for (index, record) in accepted.iter().enumerate() {
        _ = pass
            .record(
                record,
                &frame(record),
                offset_of(records, index),
                &mut image,
            )
            .unwrap();
    }
    let err = pass
        .record(
            last,
            &frame(last),
            offset_of(records, accepted.len()),
            &mut image,
        )
        .unwrap_err();

    format!("{err:#}")
}

/// An acknowledgement cannot order two deltas whose records interleaved, so it
/// is rejected.
#[test]
fn test_an_interleaved_acknowledgement_is_an_ordering_error() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = (producer(0x10), producer(0x30));

    let err = refused(
        &dir,
        &[
            write(a, 1, 2, 0xaa),
            write(b, 2, 3, 0xbb),
            ack(b, 3),
            ack(a, 4),
        ],
    );
    assert!(err.contains("interleaved"), "{err}");
}

/// A delta which another producer's records displaced is not taken up again when
/// its own records resume. Its earlier records were dropped, so what follows is a
/// fragment of it: nothing of that fragment is held, and the acknowledgement which
/// follows is refused as an interleaved one rather than committing the fragment.
///
/// Only a writer which appended past a replacement's fence produces this, which the
/// `author` register prevents unless etcd lost it.
#[test]
fn test_a_displaced_delta_which_resumes_is_never_committed() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = (producer(0x10), producer(0x30));

    // `b` displaces and commits past `a`'s delta, and then `a` resumes.
    let displaced = [
        write(a, 1, 2, 0xaa),
        write(b, 2, 3, 0xbb),
        ack(b, 3),
        write(a, 4, 4, 0xcc),
    ];

    let (pass, _image, blocks) = replay(&dir, &displaced);
    assert_eq!(blocks, vec![(3, 0xbb)], "only the committed delta applied");

    let (held, _floor, _horizon) = pass.into_parts();
    assert!(held.is_empty(), "the fragment was held");

    let mut acknowledged = displaced.to_vec();
    acknowledged.push(ack(a, 5));

    let err = refused(&dir, &acknowledged);
    assert!(err.contains("interleaved"), "{err}");

    // A resumed fragment interleaves the delta held behind it too, exactly as any
    // other producer's record does, so that delta cannot be committed either.
    let err = refused(
        &dir,
        &[
            write(a, 1, 2, 0xaa),
            write(b, 2, 3, 0xbb),
            write(a, 3, 4, 0xcc),
            ack(b, 4),
        ],
    );
    assert!(err.contains("interleaved"), "{err}");
}

#[test]
fn test_an_acknowledgement_which_rolls_back_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let a = producer(0x10);

    let err = refused(
        &dir,
        &[
            write(a, 5, 2, 0xaa),
            ack(a, 6),
            write(a, 7, 3, 0xbb),
            ack(a, 6),
        ],
    );
    assert!(err.contains("rolls back"), "{err}");
}
