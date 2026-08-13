//! Shaping a mutation's chunks into journal records, and records into appends.
//!
//! Both bounds exist for the same reason: Gazette serializes appends to a
//! journal, so an unbounded record or batch would block every later append of
//! that disk behind one large transaction.

use crate::proto::{Chunk, DiskRecord, chunk};
use gazette::journal::framing;
use prost::Message;

/// Bytes a record's UUID occupies: a field tag, a length, and sixteen bytes.
const UUID_LEN: usize = 18;

/// Largest encoding of a chunk's own fields around its data: the tags of its
/// block index and content, the block index, and two length prefixes.
const CHUNK_OVERHEAD: usize = 18;

/// Smallest record bound which can carry one block of data.
pub fn min_record_bytes(block_size: u32) -> usize {
    framing::HEADER_LEN + UUID_LEN + CHUNK_OVERHEAD + block_size as usize
}

/// Framed length of `record`.
pub fn framed_len(record: &DiskRecord) -> usize {
    framing::HEADER_LEN + record.encoded_len()
}

/// Distribute one mutation's `chunks` into the records which will carry them,
/// each framing to at most `max_record_bytes`.
///
/// A chunk which alone exceeds the bound is split at a block boundary. Replay
/// applies the parts in order and each covers only the blocks its own content
/// spans, so a split chunk applies exactly as the whole one would.
pub fn pack(chunks: Vec<Chunk>, block_size: u32, max_record_bytes: usize) -> Vec<Vec<Chunk>> {
    assert!(
        max_record_bytes >= min_record_bytes(block_size),
        "a record must be able to carry one block",
    );
    let budget = max_record_bytes - framing::HEADER_LEN - UUID_LEN;

    let mut records: Vec<Vec<Chunk>> = Vec::new();
    let mut used = 0;

    for chunk in chunks {
        for part in split(chunk, block_size, budget) {
            let len = entry_len(&part);

            if records.is_empty() || used + len > budget {
                records.push(Vec::new());
                used = 0;
            }
            used += len;
            records.last_mut().unwrap().push(part);
        }
    }
    records
}

/// Frames records into appends of at most `max_bytes`.
pub struct Batch {
    buf: bytes::BytesMut,
    max_bytes: usize,
}

impl Batch {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            buf: bytes::BytesMut::new(),
            max_bytes,
        }
    }

    /// Frame `record`, first returning the batch it does not fit within.
    pub fn push(&mut self, record: &DiskRecord) -> Option<bytes::Bytes> {
        let full = (!self.buf.is_empty() && self.buf.len() + framed_len(record) > self.max_bytes)
            .then(|| self.buf.split().freeze());

        framing::encode(record, &mut self.buf);
        full
    }

    /// Take what remains.
    pub fn take(&mut self) -> Option<bytes::Bytes> {
        (!self.buf.is_empty()).then(|| self.buf.split().freeze())
    }
}

/// Split `chunk` into parts which each fit `budget`.
fn split(chunk: Chunk, block_size: u32, budget: usize) -> Vec<Chunk> {
    let Some(chunk::Content::Data(data)) = &chunk.content else {
        return vec![chunk]; // A punch is a block index and a count.
    };
    if entry_len(&chunk) <= budget {
        return vec![chunk];
    }
    let block_size = block_size as usize;
    let per_part = (budget - CHUNK_OVERHEAD) / block_size * block_size;

    let mut parts = Vec::with_capacity(data.len().div_ceil(per_part));
    let mut block = chunk.block;

    for offset in (0..data.len()).step_by(per_part) {
        parts.push(Chunk {
            block,
            content: Some(chunk::Content::Data(
                data.slice(offset..std::cmp::min(offset + per_part, data.len())),
            )),
        });
        block += (per_part / block_size) as u32;
    }
    parts
}

/// Bytes a chunk occupies within its record, including its field tag and length.
fn entry_len(chunk: &Chunk) -> usize {
    let len = chunk.encoded_len();
    1 + prost::encoding::encoded_len_varint(len as u64) + len
}

#[cfg(test)]
mod test {
    use super::{Batch, framed_len, min_record_bytes, pack};
    use crate::chunk::{covered_blocks, encode_punch, encode_write};
    use crate::proto::DiskRecord;

    const BLOCK_SIZE: u32 = 4096;

    fn write(block: u32, blocks: u32, fill: u8) -> Vec<crate::proto::Chunk> {
        let data = bytes::Bytes::from(vec![fill; (blocks * BLOCK_SIZE) as usize]);
        encode_write(block, &data, BLOCK_SIZE)
    }

    fn record(chunks: Vec<crate::proto::Chunk>) -> DiskRecord {
        DiskRecord {
            uuid: bytes::Bytes::from_static(&[0u8; 16]),
            chunks,
            ..Default::default()
        }
    }

    #[test]
    fn test_records_hold_the_bound_and_cover_the_same_blocks() {
        let max_record_bytes = min_record_bytes(BLOCK_SIZE) * 4;

        // A large write, an all-zero write which encodes to empty-data chunks,
        // and a punch, which is the full range of mutation shapes.
        let mut chunks = write(0, 64, 0xab);
        chunks.extend(write(64, 3, 0));
        chunks.push(encode_punch(67, 9));

        let expect: Vec<_> = chunks
            .iter()
            .flat_map(|chunk| covered_blocks(chunk, BLOCK_SIZE))
            .collect();

        let records = pack(chunks, BLOCK_SIZE, max_record_bytes);
        assert!(records.len() > 1, "the write must span several records");

        let mut actual = Vec::new();
        for chunks in records {
            assert!(
                framed_len(&record(chunks.clone())) <= max_record_bytes,
                "record of {} chunks exceeds the bound",
                chunks.len(),
            );
            actual.extend(
                chunks
                    .iter()
                    .flat_map(|chunk| covered_blocks(chunk, BLOCK_SIZE)),
            );
        }
        assert_eq!(actual, expect);
    }

    #[test]
    fn test_a_chunk_within_the_bound_is_not_split() {
        let chunks = write(7, 1, 0xcd);
        let records = pack(chunks.clone(), BLOCK_SIZE, min_record_bytes(BLOCK_SIZE));

        assert_eq!(records, vec![chunks]);
    }

    #[test]
    fn test_batches_hold_the_bound() {
        // Uniformly sized records, so the batch count follows the arithmetic.
        let records: Vec<_> = (1..=10)
            .map(|block| record(write(block, 1, 0x01)))
            .collect();
        let max_bytes = framed_len(&records[0]) * 3;

        let mut batch = Batch::new(max_bytes);
        let mut appends = Vec::new();

        for record in &records {
            appends.extend(batch.push(record));
        }
        appends.extend(batch.take());

        assert_eq!(appends.len(), 4); // Three batches of three, then one.
        for append in &appends {
            assert!(
                append.len() <= max_bytes,
                "append of {} bytes",
                append.len()
            );
        }
        assert_eq!(
            appends.iter().map(bytes::Bytes::len).sum::<usize>(),
            records.iter().map(framed_len).sum::<usize>(),
        );
    }
}
