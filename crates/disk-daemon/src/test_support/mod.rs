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

/// A record of `producer` at `ticks`, carrying `chunk`.
pub fn record(
    producer: uuid::Producer,
    ticks: u64,
    flags: uuid::Flags,
    chunk: Option<proto::Chunk>,
) -> proto::DiskRecord {
    proto::DiskRecord {
        uuid: bytes::Bytes::copy_from_slice(uuid::build(producer, clock(ticks), flags).as_bytes()),
        chunk,
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

/// A chunk writing `blocks` whole blocks of `fill` from `block`.
pub fn write_chunk(block: u32, blocks: u32, fill: u8) -> proto::Chunk {
    let data = vec![fill; (blocks * crate::BLOCK_SIZE) as usize];

    proto::Chunk {
        block,
        blocks,
        content: Some(proto::chunk::Content::Data(data.into())),
    }
}

/// A chunk punching `blocks` blocks from `block`.
pub fn punch_chunk(block: u32, blocks: u32) -> proto::Chunk {
    proto::Chunk {
        block,
        blocks,
        content: Some(proto::chunk::Content::Punch(proto::chunk::Punch {})),
    }
}

/// Render chunk ranges and run-length encoded data for compact snapshots.
pub fn render_chunk(chunk: &proto::Chunk) -> String {
    let what = match &chunk.content {
        Some(proto::chunk::Content::Data(data)) if data.is_empty() => "zeroed".to_string(),
        Some(proto::chunk::Content::Data(data)) => {
            format!("data len {} [{}]", data.len(), run_lengths(data))
        }
        Some(proto::chunk::Content::Punch(_)) => "punch".to_string(),
        None => "no content".to_string(),
    };
    format!(
        "blocks {}..{}: {what}",
        chunk.block,
        chunk.block + chunk.blocks
    )
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

/// The fill byte of each block which `chunks`, applied in order, leave allocated:
/// the first byte of the block, as a disk recovered from them would hold it.
pub fn fills<'a>(chunks: impl IntoIterator<Item = &'a proto::Chunk>) -> Vec<(u32, u8)> {
    let block_size = crate::BLOCK_SIZE as usize;
    let mut fills = std::collections::BTreeMap::new();

    for chunk in chunks {
        let range = chunk.block..chunk.block + chunk.blocks;

        match &chunk.content {
            // The blocks past the data are zero.
            Some(proto::chunk::Content::Data(data)) => {
                for (index, block) in range.enumerate() {
                    let fill = data.get(index * block_size).copied().unwrap_or(0);
                    _ = fills.insert(block, fill);
                }
            }
            Some(proto::chunk::Content::Punch(_)) => {
                for block in range {
                    _ = fills.remove(&block);
                }
            }
            None => (),
        }
    }
    fills.into_iter().collect()
}

/// Render `blocks`, as [`fills`] reports them, as runs of consecutive blocks
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
