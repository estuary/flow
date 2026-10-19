use crate::proto;
use proto_gazette::{fixed_framing, uuid};

pub mod broker;

/// A producer which [`name`] reads back as `name`, so that a trace can label it.
pub fn producer(name: char) -> uuid::Producer {
    let name = name as u8;
    uuid::Producer::from_bytes([name | 0x01, 0, 0, 0, 0, name])
}

/// The name a [`producer`] was built with.
pub fn name(producer: uuid::Producer) -> char {
    producer.as_bytes()[5] as char
}

/// A clock `ticks` microseconds after the epoch, so that a case reads as a sequence
/// of small numbers.
pub fn clock(ticks: u64) -> uuid::Clock {
    let mut clock = uuid::Clock::UNIX_EPOCH;
    for _ in 0..ticks {
        _ = clock.tick();
    }
    clock
}

/// A record of `producer` at `ticks`, carrying `chunks`.
pub fn record(
    producer: uuid::Producer,
    ticks: u64,
    flags: uuid::Flags,
    chunks: Vec<proto::Chunk>,
) -> proto::DiskRecord {
    proto::DiskRecord {
        uuid: bytes::Bytes::copy_from_slice(uuid::build(producer, clock(ticks), flags).as_bytes()),
        chunks,
        opens_horizon: false,
    }
}

/// The fence of `author`, as a promotion appends it.
pub fn fence(author: uuid::Producer) -> proto::DiskRecord {
    let uuid = uuid::build(author, uuid::Clock::zero(), uuid::Flags::OUTSIDE_TXN);

    proto::DiskRecord {
        uuid: bytes::Bytes::copy_from_slice(uuid.as_bytes()),
        ..Default::default()
    }
}

/// `records`, framed exactly as the journal frames them.
pub fn frame<'a>(records: impl IntoIterator<Item = &'a proto::DiskRecord>) -> bytes::BytesMut {
    let mut framed = bytes::BytesMut::new();
    for record in records {
        fixed_framing::encode(record, &mut framed);
    }
    framed
}

/// Render chunk ranges and run-length encoded data for compact snapshots.
pub fn render_chunk(chunk: &crate::proto::Chunk) -> String {
    let range = crate::chunk::covered_blocks(chunk);
    let what = match &chunk.content {
        Some(crate::proto::chunk::Content::Data(data)) if data.is_empty() => {
            "empty data (one zeroed block)".to_string()
        }
        Some(crate::proto::chunk::Content::Data(data)) => {
            format!("data len {} [{}]", data.len(), run_lengths(data))
        }
        Some(crate::proto::chunk::Content::Punch(blocks)) => format!("punch {blocks}"),
        None => "no content".to_string(),
    };
    format!("blocks {}..{}: {what}", range.start, range.end)
}

/// Render bytes as runs of one value, such as `0xabx4096 0x00x4`.
pub fn run_lengths(data: &[u8]) -> String {
    let mut runs: Vec<(u8, usize)> = Vec::new();
    for &byte in data {
        match runs.last_mut() {
            Some((value, count)) if *value == byte => *count += 1,
            _ => runs.push((byte, 1)),
        }
    }
    runs.iter()
        .map(|(value, count)| format!("{value:#04x}x{count}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Each allocated block of `image`, paired with the byte it is filled with: a
/// readable report of what a replay left allocated.
pub fn allocated(image: &crate::image::Image) -> Vec<(u32, u8)> {
    let mut block = vec![0u8; crate::BLOCK_SIZE as usize];

    image
        .allocated()
        .iter_ones()
        .map(|index| {
            let index = index as u32;
            image.read_at(index, &mut block).unwrap();
            (index, block[0])
        })
        .collect()
}

/// Render `blocks`, as [`allocated`] reports them, as runs of consecutive blocks
/// holding one fill byte.
pub fn render_blocks(blocks: &[(u32, u8)]) -> String {
    let mut runs: Vec<(std::ops::Range<u32>, u8)> = Vec::new();

    for &(block, fill) in blocks {
        match runs.last_mut() {
            Some((range, last)) if range.end == block && *last == fill => range.end += 1,
            _ => runs.push((block..block + 1, fill)),
        }
    }
    if runs.is_empty() {
        return "none".to_string();
    }
    runs.iter()
        .map(|(range, fill)| format!("{}..{} filled {fill:#04x}", range.start, range.end))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The recorded stream, replayed onto a fresh image, reproduces the image `served`:
/// the same bytes in the same allocated blocks. This is why the local image is
/// disposable.
pub fn assert_replays_identically(
    served: &crate::image::Image,
    mutations: &[Vec<crate::proto::Chunk>],
) {
    let mut replayed = crate::image::Image::create(&std::env::temp_dir(), served.blocks()).unwrap();

    for chunk in mutations.iter().flatten() {
        _ = replayed.apply(chunk).unwrap();
    }
    assert_eq!(
        served.allocated(),
        replayed.allocated(),
        "the replay tracked other blocks",
    );

    assert_eq!(
        first_difference(served, &replayed),
        None,
        "the replay differs from the image served",
    );
}

/// Block at which two images first differ, which names where a replay went wrong
/// instead of dumping either image.
fn first_difference(left: &crate::image::Image, right: &crate::image::Image) -> Option<u32> {
    let block_size = crate::BLOCK_SIZE as usize;
    let (mut left_buf, mut right_buf) = (vec![0u8; block_size], vec![0u8; block_size]);

    (0..left.blocks()).find(|&block| {
        () = left.read_at(block, &mut left_buf).unwrap();
        () = right.read_at(block, &mut right_buf).unwrap();
        left_buf != right_buf
    })
}
