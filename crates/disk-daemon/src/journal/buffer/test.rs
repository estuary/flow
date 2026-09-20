use super::{Buffer, READ_BYTES};
use crate::image::Image;
use crate::proto;
use crate::test_support;
use crate::{BLOCK_SIZE, chunk};
use proto_gazette::{fixed_framing, uuid};

const BLOCKS: u32 = 256;

fn producer(seed: u8) -> uuid::Producer {
    uuid::Producer::from_bytes([seed | 0x01, 0, 0, 0, 0, seed])
}

/// A record of `producer`'s delta carrying `chunks`. Nothing here sequences
/// records, so one clock serves them all.
fn record(producer: uuid::Producer, chunks: Vec<proto::Chunk>) -> proto::DiskRecord {
    proto::DiskRecord {
        uuid: bytes::Bytes::copy_from_slice(
            uuid::build(producer, uuid::Clock::UNIX_EPOCH, uuid::Flags::CONTINUE_TXN)
                .as_bytes()
                .as_slice(),
        ),
        chunks,
        opens_horizon: false,
        installs_epoch: bytes::Bytes::new(),
    }
}

/// A record writing `blocks` blocks of `fill`, beginning at `block`.
fn write(producer: uuid::Producer, block: u32, blocks: u32, fill: u8) -> proto::DiskRecord {
    let data = bytes::Bytes::from(vec![fill; (blocks * BLOCK_SIZE) as usize]);

    record(producer, chunk::encode_write(block, &data))
}

/// A record whose chunk carries `bytes` bytes of `fill`, which is how a case
/// places its framing at an offset a block-aligned write could not reach.
fn sized(producer: uuid::Producer, block: u32, bytes: usize, fill: u8) -> proto::DiskRecord {
    record(
        producer,
        vec![proto::Chunk {
            block,
            content: Some(proto::chunk::Content::Data(bytes::Bytes::from(vec![
                fill;
                bytes
            ]))),
        }],
    )
}

/// Hold `records`, framed exactly as the journal frames them.
fn hold(buffer: &mut Buffer, producer: uuid::Producer, records: &[proto::DiskRecord]) {
    let mut framed = bytes::BytesMut::new();

    for record in records {
        framed.clear();
        fixed_framing::encode(record, &mut framed);
        buffer.push(producer, &framed).unwrap();
    }
}

/// A delta of many records, spanning many fills of the reader's buffer, applies
/// in the order it was held.
#[test]
fn test_a_many_record_delta_applies_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let mut image = Image::create(dir.path(), BLOCKS).unwrap();
    let mut buffer = Buffer::create(dir.path()).unwrap();
    let a = producer(0x10);

    // Each block is rewritten many times over, so only ordered application
    // leaves the last writer of each.
    let records: Vec<_> = (0..600u32).map(|i| write(a, i % 8, 1, i as u8)).collect();

    hold(&mut buffer, a, &records);
    assert!(
        buffer.len() > 8 * READ_BYTES as u64,
        "many buffer fills of delta"
    );

    assert_eq!(
        buffer.drain(&mut image, &mut None, None).unwrap(),
        records.len()
    );
    assert_eq!(
        test_support::allocated(&image),
        (0..8u32)
            .map(|block| (block, (592 + block) as u8))
            .collect::<Vec<_>>(),
    );
    assert!(buffer.is_empty());
    assert_eq!(buffer.len(), 0);
}

/// A record may be larger than the buffer it is read through, and is then read
/// whole. Framing decodes a record as a unit, and a chunk of the device's
/// maximum request size already exceeds the buffer.
#[test]
fn test_a_record_larger_than_the_read_buffer() {
    let dir = tempfile::tempdir().unwrap();
    let mut image = Image::create(dir.path(), BLOCKS).unwrap();
    let mut buffer = Buffer::create(dir.path()).unwrap();
    let a = producer(0x10);

    let blocks = 2 + READ_BYTES as u32 / BLOCK_SIZE;

    hold(
        &mut buffer,
        a,
        &[
            write(a, 0, 1, 0xaa),
            write(a, 1, blocks, 0xbb),
            write(a, 1 + blocks, 1, 0xcc),
        ],
    );
    assert_eq!(buffer.drain(&mut image, &mut None, None).unwrap(), 3);

    let mut expect = vec![(0, 0xaa)];
    expect.extend((1..1 + blocks).map(|block| (block, 0xbb)));
    expect.push((1 + blocks, 0xcc));

    assert_eq!(test_support::allocated(&image), expect);
}

/// A frame may cross a fill of the reader's buffer at any offset, including
/// within its own header. Sweeping the size of the first record walks the second
/// record's start across that edge, and the sweep asserts that it reached the
/// header case.
#[test]
fn test_a_record_may_straddle_the_read_buffer() {
    let dir = tempfile::tempdir().unwrap();
    let a = producer(0x10);
    let mut split_header = false;

    for bytes in READ_BYTES - 72..READ_BYTES + 8 {
        let mut image = Image::create(dir.path(), BLOCKS).unwrap();
        let mut buffer = Buffer::create(dir.path()).unwrap();

        let first = sized(a, 0, bytes, 0xaa);
        let mut framed = bytes::BytesMut::new();
        fixed_framing::encode(&first, &mut framed);

        split_header |=
            (READ_BYTES - fixed_framing::HEADER_LEN..READ_BYTES).contains(&framed.len());

        hold(
            &mut buffer,
            a,
            &[first, write(a, 40, 1, 0xbb), write(a, 41, 1, 0xcc)],
        );
        assert_eq!(buffer.drain(&mut image, &mut None, None).unwrap(), 3);

        let covered = bytes.div_ceil(BLOCK_SIZE as usize) as u32;
        let mut expect: Vec<_> = (0..covered).map(|block| (block, 0xaa)).collect();
        expect.extend([(40, 0xbb), (41, 0xcc)]);

        assert_eq!(
            test_support::allocated(&image),
            expect,
            "with {bytes} bytes of data"
        );
    }
    assert!(split_header, "a frame header straddled a buffer boundary");
}

/// Clearing punches the file without shrinking it. A shorter delta must stop
/// at its own end, even though the file still extends to the prior delta's end.
#[test]
fn test_a_drained_buffer_holds_the_next_delta() {
    let dir = tempfile::tempdir().unwrap();
    let mut image = Image::create(dir.path(), BLOCKS).unwrap();
    let mut buffer = Buffer::create(dir.path()).unwrap();
    let a = producer(0x10);

    hold(
        &mut buffer,
        a,
        &[write(a, 0, 2, 0xaa), write(a, 1, 1, 0xbb)],
    );
    let prior_len = buffer.len();
    assert_eq!(buffer.drain(&mut image, &mut None, None).unwrap(), 2);
    assert_eq!(buffer.len(), 0);

    hold(
        &mut buffer,
        a,
        &[write(a, 1, 1, 0xcc), write(a, 2, 1, 0xdd)],
    );
    assert!(buffer.len() < prior_len, "the next delta is shorter");
    assert_eq!(buffer.file.metadata().unwrap().len(), prior_len);
    assert_eq!(buffer.drain(&mut image, &mut None, None).unwrap(), 2);

    assert_eq!(
        test_support::allocated(&image),
        vec![(0, 0xaa), (1, 0xcc), (2, 0xdd)],
        "the second delta applied over the first",
    );
}

/// Hold malformed bytes and report the failure of draining them.
fn refused(dir: &tempfile::TempDir, parts: &[&[u8]]) -> String {
    let mut image = Image::create(dir.path(), BLOCKS).unwrap();
    let mut buffer = Buffer::create(dir.path()).unwrap();
    let a = producer(0x10);

    for part in parts {
        buffer.push(a, part).unwrap();
    }
    let err = buffer.drain(&mut image, &mut None, None).unwrap_err();
    assert!(test_support::allocated(&image).is_empty());

    format!("{err:#}")
}

#[test]
fn test_unframed_held_content_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let err = refused(&dir, &[b"held content with no magic word anywhere in it"]);

    assert!(err.contains("does not decode"), "{err}");
}

#[test]
fn test_a_delta_which_ends_within_a_record_is_refused() {
    let dir = tempfile::tempdir().unwrap();

    let mut framed = bytes::BytesMut::new();
    fixed_framing::encode(&write(producer(0x10), 0, 1, 0xaa), &mut framed);

    for len in [1, fixed_framing::HEADER_LEN - 1, framed.len() - 1] {
        let err = refused(&dir, &[&framed[..len]]);
        assert!(err.contains("ends within a record"), "{err}");
    }
}

/// A header states its payload's length, and that length is checked against the
/// bytes the file holds before anything is sized from it. The delta here runs
/// past the first buffer fill, so the check is reached with most of it unread.
#[test]
fn test_a_record_which_frames_more_than_is_held_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let a = producer(0x10);

    let mut claims = bytes::BytesMut::from(&fixed_framing::MAGIC[..]);
    claims.extend_from_slice(&u32::MAX.to_le_bytes());

    let mut beyond = bytes::BytesMut::new();
    while beyond.len() < READ_BYTES {
        fixed_framing::encode(&write(a, 0, 1, 0xaa), &mut beyond);
    }

    let err = refused(&dir, &[&claims, &beyond]);
    assert!(err.contains("more than the delta holds"), "{err}");
}
