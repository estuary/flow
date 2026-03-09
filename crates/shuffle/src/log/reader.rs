use super::Lsn;
use super::block::{ArchivedBlock, ArchivedBlockProducer};
use crate::Frontier;
use anyhow::Context;
use proto_gazette::uuid::{Clock, Producer};
use std::collections::{HashSet, VecDeque};
use std::io::{Read, Seek};

/// Reader reads committed entries from local log files produced by `Writer`.
///
/// Given a checkpoint delta (`Frontier`), `read_frontier` returns a
/// `FrontierReader` that yields committed entries one at a time. Blocks are
/// tracked across checkpoints so that only uncommitted remainders are re-read,
/// and blocks with no producer overlap are skipped without I/O.
///
/// The reader is ephemeral: its state is not persisted. On any error the
/// session tears down (fail-fast), and segment files are discarded along with
/// all other session state.
pub struct Reader {
    // Base directory for all segment files of the log.
    directory: std::path::PathBuf,
    // Index of the read member, used to name its files.
    member_index: u32,
    // Member `flushed_lsn` of the latest Frontier, or Lsn::ZERO if new.
    // This is a read-through barrier: don't read blocks beyond this LSN.
    flushed_lsn: Lsn,
    // Live segments of the log.
    segments: VecDeque<Segment>,
    // Re-usable buffer, moved into FrontierReader during iteration.
    buf: Vec<u8>,
}

/// Segment is the processing and remainder state of a log segment file.
struct Segment {
    // Starting LSN of this Segment (block is always zero).
    base_lsn: Lsn,
    // Open file of the segment.
    file: std::fs::File,
    // Block remainders with uncommitted entries. Only non-empty remainders
    // are stored; fully consumed blocks are not tracked here.
    remainders: Vec<BlockRemainder>,
    // Total number of blocks read from this segment (consumed or not).
    block_count: usize,
    // File byte offset of the next expected block header,
    // were it to be extended.
    next_file_offset: u64,
}

/// Captures the uncommitted residual of a partially-consumed block.
///
/// Only blocks with uncommitted entries are tracked as remainders;
/// fully consumed blocks are removed from the segment's remainder list.
struct BlockRemainder {
    // Sparse index of:
    // (binding, journal_bid, producer_bid) -> first uncommitted Clock
    //
    // Where the first uncommitted Clock is the earliest tuple Clock observed
    // in prior reads of this block that couldn't be yielded (was uncommitted).
    //
    // The Clocks of a (binding, journal_bid, producer_bid) tuple are strictly
    // ordered (ascending) within the log, so this is the tuple's "resume" point
    // when the block is being re-read under a subsequent checkpoint.
    //
    // The index is sparse: if a block tuple is missing, then the tuple's entries
    // have been fully consumed from this block.
    first_clocks: Vec<(u16, u16, u16, Clock)>,
    // Distinct producers with uncommitted entries, sorted on Producer.
    //
    // This is built from `first_clocks` and will always align with the producers
    // of that index. However, here we store the full Producer value so that we
    // may test for intersection with Frontier producers without having to
    // physically re-read the block (in order to access its producer => producer_id map).
    producers: Vec<Producer>,
    // File byte offset of this block's header.
    file_offset: u64,
    // Uncompressed size of the block payload.
    raw_len: u32,
    // Compressed size, or 0 if uncompressed.
    lz4_len: u32,
}

/// Entry yielded by FrontierReader<'r>, borrowing from the in-memory block buffer.
pub struct Entry<'r> {
    // Binding index of the source collection.
    pub binding: u16,
    // Name of the journal this entry was read from.
    pub journal_name: &'r str,
    // Byte offset within the journal.
    pub offset: i64,
    // Producer that wrote this entry.
    pub producer: Producer,
    // Producer Clock of this entry.
    pub clock: Clock,
    // Packed prefix of the key under which the document was read and shuffled.
    pub packed_key_prefix: &'r [u8; 16],
    // Archived document content.
    pub doc: &'r doc::ArchivedEmbedded<'r>,
    // Document passed JSON schema validation.
    pub valid: bool,
}

/// Yields committed entries for a single frontier checkpoint.
///
/// Returned by `Reader::read_frontier`. Each call to `next` yields the next
/// committed entry; the returned `Entry` borrows from internal buffers and
/// must be dropped before calling `next` again (enforced by the `&mut self`
/// borrow). This is not a `std::iter::Iterator` because the yielded items
/// borrow from the iterator itself (a lending / streaming iterator).
pub struct FrontierReader<'r> {
    // Underlying reader whose segments and files we walk.
    reader: &'r mut Reader,
    // Frontier delta driving this iteration's visibility decisions.
    frontier: &'r Frontier,
    // Set of producers that committed in this Frontier delta.
    // Tested for intersaction with BlockRemainder::producers to skip IO.
    frontier_producers: HashSet<Producer>,
    // Current iteration phase (remainder blocks, tail blocks, or done).
    phase: Phase,
    // Block data buffer, moved out of Reader for the duration of iteration
    // to avoid borrow conflicts between segment file I/O and buffer access.
    buf: Vec<u8>,
    // Per-block scan state, rebuilt each time a block is loaded into `buf`.
    scan: BlockScan,
}

enum Phase {
    /// Walking existing blocks that have uncommitted remainders.
    Remainders { seg_idx: usize, block_idx: usize },
    /// Reading new blocks from the tail segment.
    Tail,
    /// Iteration complete.
    Done,
}

/// Cached state for yielding entries from a single loaded block.
struct BlockScan {
    /// Indices of committed entries within the archived block.
    committed: Vec<usize>,
    /// Next index into `committed` to yield.
    yield_idx: usize,
    /// Maps journal_bid to index into archived.journals.
    journal_bid_to_idx: Vec<usize>,
    /// Maps producer_bid to index into archived.producers.
    producer_bid_to_idx: Vec<usize>,
}

impl BlockScan {
    fn new() -> Self {
        Self {
            committed: Vec::new(),
            yield_idx: 0,
            journal_bid_to_idx: Vec::new(),
            producer_bid_to_idx: Vec::new(),
        }
    }

    fn has_pending(&self) -> bool {
        self.yield_idx < self.committed.len()
    }
}

impl Reader {
    /// Create a new Reader. No files are opened until `read_frontier`.
    pub fn new(directory: &std::path::Path, member_index: u32) -> Self {
        Self {
            directory: directory.to_owned(),
            member_index,
            segments: VecDeque::new(),
            buf: Vec::new(),
            flushed_lsn: Lsn::ZERO,
        }
    }

    /// Prepare to read committed entries for a frontier checkpoint.
    ///
    /// Validates the flushed LSN, opens the initial segment if needed,
    /// and returns a `FrontierReader` that lazily yields entries.
    pub fn read_frontier<'r>(
        &'r mut self,
        frontier: &'r Frontier,
    ) -> anyhow::Result<FrontierReader<'r>> {
        let flushed_lsn = frontier
            .flushed_lsn
            .get(self.member_index as usize)
            .copied()
            .with_context(|| {
                format!(
                    "flushed_lsn has {} entries but member_index is {}",
                    frontier.flushed_lsn.len(),
                    self.member_index,
                )
            })?;

        anyhow::ensure!(
            flushed_lsn >= self.flushed_lsn,
            "flushed_lsn {flushed_lsn:?} < previous {prev:?}",
            prev = self.flushed_lsn,
        );
        self.flushed_lsn = flushed_lsn;

        let committed_producers: HashSet<Producer> = frontier
            .journals
            .iter()
            .flat_map(|jf| jf.producers.iter().map(|p| p.producer))
            .collect();

        if self.segments.is_empty() {
            let file = open_segment(&self.directory, self.member_index, 1)?;
            self.segments.push_back(Segment {
                base_lsn: Lsn::new(1, 0),
                file,
                remainders: Vec::new(),
                block_count: 0,
                next_file_offset: 0,
            });
        }

        let buf = std::mem::take(&mut self.buf);

        Ok(FrontierReader {
            reader: self,
            frontier,
            frontier_producers: committed_producers,
            phase: Phase::Remainders {
                seg_idx: 0,
                block_idx: 0,
            },
            buf,
            scan: BlockScan::new(),
        })
    }
}

impl<'r> FrontierReader<'r> {
    /// Yield the next committed entry, or `None` when all entries for this
    /// frontier have been yielded.
    ///
    /// The returned `Entry` borrows from internal buffers. It must be dropped
    /// before calling `next` again (enforced by the borrow on `&mut self`).
    pub fn next(&mut self) -> Option<anyhow::Result<Entry<'_>>> {
        loop {
            if self.scan.has_pending() {
                return Some(Ok(self.yield_entry()));
            }
            match self.load_next_block() {
                Ok(true) => continue,
                Ok(false) => return None,
                Err(e) => {
                    self.phase = Phase::Done;
                    return Some(Err(e));
                }
            }
        }
    }

    /// Construct an Entry from the current block scan state.
    fn yield_entry(&mut self) -> Entry<'_> {
        let i = self.scan.committed[self.scan.yield_idx];
        self.scan.yield_idx += 1;

        let archived = rkyv::access::<ArchivedBlock, rkyv::rancor::Error>(&self.buf).unwrap();
        let meta = &archived.meta[i];
        let doc_entry = &archived.docs[i];

        let journal_bid: u16 = meta.journal_bid.to_native();
        let producer_bid: u16 = meta.producer_bid.to_native();

        let journal_name: &str =
            &archived.journals[self.scan.journal_bid_to_idx[journal_bid as usize]].name;
        let producer = Producer(
            archived.producers[self.scan.producer_bid_to_idx[producer_bid as usize]].producer,
        );

        // Safety: ArchivedEmbedded's lifetime parameter is meaningless
        // for archived data (same as ArchivedNode; see doc::archived).
        let doc: &doc::ArchivedEmbedded = unsafe {
            std::mem::transmute::<&doc::embedded::ArchivedEmbedded<'_>, &doc::ArchivedEmbedded>(
                &doc_entry.doc,
            )
        };

        Entry {
            binding: meta.binding.to_native(),
            journal_name,
            offset: doc_entry.offset.to_native(),
            producer,
            clock: Clock::from_u64(meta.clock.to_native()),
            packed_key_prefix: &doc_entry.packed_key_prefix,
            doc,
            valid: meta.flags.to_native() & 0x01 != 0,
        }
    }

    /// Advance to the next block that has committed entries.
    /// Returns Ok(true) if a block was loaded, Ok(false) if done.
    fn load_next_block(&mut self) -> anyhow::Result<bool> {
        loop {
            match self.phase {
                Phase::Remainders {
                    ref mut seg_idx,
                    ref mut block_idx,
                } => {
                    if *seg_idx >= self.reader.segments.len() {
                        self.phase = Phase::Tail;
                        continue;
                    }
                    if *block_idx >= self.reader.segments[*seg_idx].remainders.len() {
                        *seg_idx += 1;
                        *block_idx = 0;
                        continue;
                    }

                    let si = *seg_idx;
                    let bi = *block_idx;

                    let block = &self.reader.segments[si].remainders[bi];

                    // Skip check: no producer in this block committed in this delta.
                    if !block
                        .producers
                        .iter()
                        .any(|p| self.frontier_producers.contains(p))
                    {
                        *block_idx += 1;
                        continue;
                    }

                    // Snapshot prior state before re-reading block.
                    let file_offset = block.file_offset;
                    let raw_len = block.raw_len;
                    let lz4_len = block.lz4_len;
                    let prior_first_clocks = block.first_clocks.clone();

                    // Speculatively advance past this block. We'll undo the
                    // increment below if the remainder turns out empty
                    // (removing the entry shifts the next element into `bi`).
                    *block_idx += 1;

                    read_block_at(
                        &mut self.reader.segments[si].file,
                        file_offset,
                        raw_len,
                        lz4_len,
                        &mut self.buf,
                    )?;

                    let new_remainder =
                        self.scan_block(file_offset, raw_len, lz4_len, Some(&prior_first_clocks));

                    if let Some(remainder) = new_remainder {
                        self.reader.segments[si].remainders[bi] = remainder;
                    } else {
                        self.reader.segments[si].remainders.remove(bi);
                        // Undo the speculative increment: the next element
                        // slid into position `bi`.
                        if let Phase::Remainders {
                            ref mut block_idx, ..
                        } = self.phase
                        {
                            *block_idx -= 1;
                        }
                    }

                    if self.scan.has_pending() {
                        return Ok(true);
                    }
                    // Block had no newly-committed entries; continue to next.
                }
                Phase::Tail => {
                    return self.load_tail_block();
                }
                Phase::Done => return Ok(false),
            }
        }
    }

    /// Read new blocks from the tail segment up to flushed_lsn.
    fn load_tail_block(&mut self) -> anyhow::Result<bool> {
        loop {
            let seg_count = self.reader.segments.len();
            let seg = self.reader.segments.back_mut().unwrap();
            let next_block = seg.base_lsn.block() as u64 + seg.block_count as u64;
            let next_lsn = Lsn::new(seg.base_lsn.segment(), next_block as u16);

            if next_lsn > self.reader.flushed_lsn {
                self.phase = Phase::Done;
                return Ok(false);
            }

            let file_offset = seg.next_file_offset;
            match try_read_block_header(&mut seg.file, file_offset) {
                Ok(Some((raw_len, lz4_len))) => {
                    let payload_len = if lz4_len > 0 { lz4_len } else { raw_len } as usize;
                    read_block_payload(
                        &mut seg.file,
                        payload_len,
                        raw_len,
                        lz4_len,
                        &mut self.buf,
                    )?;
                    seg.next_file_offset =
                        file_offset + BLOCK_HEADER_LEN as u64 + payload_len as u64;
                    seg.block_count += 1;

                    // NLL: seg's last use is above; borrow ends here.
                    let seg_idx = seg_count - 1;

                    let new_remainder = self.scan_block(file_offset, raw_len, lz4_len, None);
                    if let Some(remainder) = new_remainder {
                        self.reader.segments[seg_idx].remainders.push(remainder);
                    }

                    if self.scan.has_pending() {
                        return Ok(true);
                    }
                    // No committed entries in this block; continue to next.
                }
                Ok(None) => {
                    // EOF: transition to next segment.
                    let next_segment_num = seg.base_lsn.segment() + 1;

                    // NLL: seg's last use is above; borrow ends here.
                    let file = open_segment(
                        &self.reader.directory,
                        self.reader.member_index,
                        next_segment_num,
                    )?;
                    self.reader.segments.push_back(Segment {
                        base_lsn: Lsn::new(next_segment_num, 0),
                        file,
                        remainders: Vec::new(),
                        block_count: 0,
                        next_file_offset: 0,
                    });
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Eagerly scan all entries in the loaded block, collecting committed
    /// entry indices. Returns the new remainder if there are uncommitted
    /// entries, or `None` if the block is fully consumed.
    fn scan_block(
        &mut self,
        file_offset: u64,
        raw_len: u32,
        lz4_len: u32,
        prior_first_clocks: Option<&[(u16, u16, u16, Clock)]>,
    ) -> Option<BlockRemainder> {
        let (committed, new_remainder, journal_bid_to_idx, producer_bid_to_idx) = {
            let archived = rkyv::access::<ArchivedBlock, rkyv::rancor::Error>(&self.buf).unwrap();

            let visibility = build_visibility_index(self.frontier, archived);

            let mut journal_bid_to_idx = vec![0usize; archived.journals.len()];
            for (idx, j) in archived.journals.iter().enumerate() {
                journal_bid_to_idx[u16::from(j.journal_bid) as usize] = idx;
            }
            let mut producer_bid_to_idx = vec![0usize; archived.producers.len()];
            for (idx, p) in archived.producers.iter().enumerate() {
                producer_bid_to_idx[u16::from(p.producer_bid) as usize] = idx;
            }

            let mut committed = Vec::new();
            let mut new_first_clocks: Vec<(u16, u16, u16, Clock)> = Vec::new();

            for i in 0..archived.meta.len() {
                let meta = &archived.meta[i];
                let binding: u16 = meta.binding.to_native();
                let journal_bid: u16 = meta.journal_bid.to_native();
                let producer_bid: u16 = meta.producer_bid.to_native();
                let clock = Clock::from_u64(meta.clock.to_native());
                let key = (binding, journal_bid, producer_bid);

                // Skip if already yielded in a prior checkpoint.
                if let Some(first_clocks) = prior_first_clocks {
                    match first_clocks.binary_search_by_key(&key, |e| (e.0, e.1, e.2)) {
                        Ok(idx) => {
                            if clock < first_clocks[idx].3 {
                                continue;
                            }
                        }
                        // Tuple fully consumed on a prior read.
                        Err(_) => continue,
                    }
                }

                // Check visibility.
                if let Ok(idx) = visibility.binary_search_by_key(&key, |e| (e.0, e.1, e.2)) {
                    if clock <= visibility[idx].3 {
                        committed.push(i);
                        continue;
                    }
                }

                // Not committed: track as remainder.
                match new_first_clocks.binary_search_by_key(&key, |e| (e.0, e.1, e.2)) {
                    Ok(_) => {} // Already tracked; first occurrence has the minimum clock.
                    Err(pos) => {
                        new_first_clocks.insert(pos, (binding, journal_bid, producer_bid, clock));
                    }
                }
            }

            let new_remainder = if new_first_clocks.is_empty() {
                None
            } else {
                let mut remaining_producers: Vec<Producer> = new_first_clocks
                    .iter()
                    .map(|&(_, _, pbid, _)| {
                        Producer(archived.producers[producer_bid_to_idx[pbid as usize]].producer)
                    })
                    .collect();
                remaining_producers.sort();
                remaining_producers.dedup();

                Some(BlockRemainder {
                    producers: remaining_producers,
                    first_clocks: new_first_clocks,
                    file_offset,
                    raw_len,
                    lz4_len,
                })
            };

            (
                committed,
                new_remainder,
                journal_bid_to_idx,
                producer_bid_to_idx,
            )
        };
        // archived is dropped; safe to mutate reader.segments.

        self.scan = BlockScan {
            committed,
            yield_idx: 0,
            journal_bid_to_idx,
            producer_bid_to_idx,
        };
        new_remainder
    }
}

impl<'r> Drop for FrontierReader<'r> {
    fn drop(&mut self) {
        // Return the buffer to the reader for reuse across checkpoints.
        self.reader.buf = std::mem::take(&mut self.buf);

        // GC fully-consumed front segments (no remainders, a following
        // segment exists).
        while self.reader.segments.len() > 1 {
            if !self
                .reader
                .segments
                .front()
                .map_or(false, |s| s.remainders.is_empty())
            {
                break;
            }
            let old = self.reader.segments.pop_front().unwrap();
            let path = segment_path(
                &self.reader.directory,
                self.reader.member_index,
                old.base_lsn.segment(),
            );
            drop(old.file);
            let _ = std::fs::remove_file(&path);
        }
    }
}

/// Open a segment file for reading.
fn open_segment(
    directory: &std::path::Path,
    member_index: u32,
    segment: u64,
) -> anyhow::Result<std::fs::File> {
    let path = segment_path(directory, member_index, segment);
    std::fs::File::open(&path).with_context(|| format!("opening log segment {path:?}"))
}

/// Build the segment file path using the writer's naming convention.
fn segment_path(
    directory: &std::path::Path,
    member_index: u32,
    segment: u64,
) -> std::path::PathBuf {
    let filename = format!("mem-{member_index:03}-seg-{segment:012x}.flog");
    directory.join(filename)
}

const BLOCK_HEADER_LEN: usize = 8;

/// Try to read a block header. Returns None on EOF, or (raw_len, lz4_len).
fn try_read_block_header(
    file: &mut std::fs::File,
    offset: u64,
) -> anyhow::Result<Option<(u32, u32)>> {
    file.seek(std::io::SeekFrom::Start(offset))?;

    let mut header = [0u8; BLOCK_HEADER_LEN];
    match file.read(&mut header) {
        Ok(0) => return Ok(None), // EOF
        Ok(n) if n < BLOCK_HEADER_LEN => {
            // Partial header — read the rest.
            file.read_exact(&mut header[n..])?;
        }
        Ok(_) => {}
        Err(e) => return Err(e.into()),
    }

    let raw_len = u32::from_be_bytes(header[0..4].try_into().unwrap());
    let lz4_len = u32::from_be_bytes(header[4..8].try_into().unwrap());
    Ok(Some((raw_len, lz4_len)))
}

/// Read block payload into buf, decompressing if needed. Assumes file position
/// is immediately after the header.
fn read_block_payload(
    file: &mut std::fs::File,
    payload_len: usize,
    raw_len: u32,
    lz4_len: u32,
    buf: &mut Vec<u8>,
) -> anyhow::Result<()> {
    if lz4_len > 0 {
        let mut compressed = vec![0u8; payload_len];
        file.read_exact(&mut compressed)?;
        buf.resize(raw_len as usize, 0);
        lz4::block::decompress_to_buffer(&compressed, Some(raw_len as i32), buf)
            .context("decompressing log block")?;
    } else {
        buf.resize(payload_len, 0);
        file.read_exact(buf)?;
    }
    Ok(())
}

/// Read a block from a known file offset (for re-reading blocks with remainder).
fn read_block_at(
    file: &mut std::fs::File,
    file_offset: u64,
    raw_len: u32,
    lz4_len: u32,
    buf: &mut Vec<u8>,
) -> anyhow::Result<()> {
    file.seek(std::io::SeekFrom::Start(
        file_offset + BLOCK_HEADER_LEN as u64,
    ))?;
    let payload_len = if lz4_len > 0 { lz4_len } else { raw_len } as usize;
    read_block_payload(file, payload_len, raw_len, lz4_len, buf)
}

/// Build a visibility index by merge-joining the frontier delta with the
/// block's journals and producers.
///
/// Returns a sorted `Vec<(binding, journal_bid, producer_bid, last_commit)>`
/// for binary search during entry scan.
fn build_visibility_index(
    frontier: &Frontier,
    archived: &ArchivedBlock<'_>,
) -> Vec<(u16, u16, u16, Clock)> {
    let mut result = Vec::new();

    // Block producers sorted by producer value.
    let block_producers: &[ArchivedBlockProducer] = &archived.producers;

    let mut block_journal_cursor = 0usize;
    let mut prev_binding: Option<u16> = None;

    for jf in &frontier.journals {
        // Reset block journal cursor when binding changes, because the same
        // journal name may appear under a different binding.
        if prev_binding != Some(jf.binding) {
            block_journal_cursor = 0;
            prev_binding = Some(jf.binding);
        }

        // Advance cursor to find matching journal name.
        while block_journal_cursor < archived.journals.len()
            && archived.journals[block_journal_cursor].name.as_str() < jf.journal.as_ref()
        {
            block_journal_cursor += 1;
        }

        if block_journal_cursor >= archived.journals.len()
            || archived.journals[block_journal_cursor].name.as_str() != jf.journal.as_ref()
        {
            continue; // Journal not in this block.
        }

        let block_journal_bid = u16::from(archived.journals[block_journal_cursor].journal_bid);

        // Merge-join frontier producers with block producers (both sorted by producer).
        let mut bp_cursor = 0usize;
        for fp in &jf.producers {
            while bp_cursor < block_producers.len()
                && Producer(block_producers[bp_cursor].producer) < fp.producer
            {
                bp_cursor += 1;
            }
            if bp_cursor < block_producers.len()
                && Producer(block_producers[bp_cursor].producer) == fp.producer
            {
                result.push((
                    jf.binding,
                    block_journal_bid,
                    u16::from(block_producers[bp_cursor].producer_bid),
                    fp.last_commit,
                ));
            }
        }
    }

    // Sort for binary search by (binding, journal_bid, producer_bid).
    result.sort_by_key(|&(b, j, p, _)| (b, j, p));
    result
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::log::block::BlockMeta;
    use crate::log::writer::Writer;
    use crate::testing::{jf, pf, producer};
    use proto_gazette::uuid;
    use std::collections::HashMap;

    /// Helper to build a frontier with given journals and flushed_lsn.
    fn test_frontier(journals: Vec<crate::JournalFrontier>, flushed_lsn: Vec<Lsn>) -> Frontier {
        Frontier::new(
            journals,
            flushed_lsn.into_iter().map(|l| l.as_u64()).collect(),
        )
        .unwrap()
    }

    /// Helper to write a block with entries and return the LSN.
    fn write_block(
        writer: &mut Writer,
        journals: HashMap<String, u16>,
        producers: HashMap<uuid::Producer, u16>,
        entries: Vec<(BlockMeta, i64, bytes::Bytes, bytes::Bytes)>,
    ) -> Lsn {
        writer.append_block(journals, producers, entries).unwrap()
    }

    fn make_doc_bytes(value: &str) -> bytes::Bytes {
        let alloc = doc::HeapNode::new_allocator();
        let node = doc::HeapNode::from_serde(&serde_json::json!({"v": value}), &alloc).unwrap();
        bytes::Bytes::from(node.to_archive().to_vec())
    }

    fn meta(binding: u16, journal_bid: u16, producer_bid: u16, clock: u64) -> BlockMeta {
        BlockMeta {
            binding,
            journal_bid,
            producer_bid,
            flags: 0x0001,
            clock,
        }
    }

    /// Collect entries from read_frontier into a vec of debug tuples.
    fn collect_entries(
        reader: &mut Reader,
        frontier: &Frontier,
    ) -> Vec<(u16, String, [u8; 6], u64, bool, i64)> {
        let mut result = Vec::new();
        let mut iter = reader.read_frontier(frontier).unwrap();
        while let Some(entry) = iter.next() {
            let entry = entry.unwrap();
            result.push((
                entry.binding,
                entry.journal_name.to_string(),
                entry.producer.0,
                entry.clock.as_u64(),
                entry.valid,
                entry.offset,
            ));
        }
        result
    }

    #[test]
    fn test_all_committed_on_first_read() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = Writer::new(dir.path(), 0).unwrap();

        let p1 = producer(0x01);
        let p2 = producer(0x03);
        let c1 = Clock::from_unix(100, 0);
        let c2 = Clock::from_unix(200, 0);

        let journals: HashMap<String, u16> = [("j/one".into(), 0u16)].into();
        let producers: HashMap<uuid::Producer, u16> = [(p1, 0u16), (p2, 1u16)].into();

        let entries = vec![
            (
                meta(0, 0, 0, c1.as_u64()),
                100i64,
                bytes::Bytes::from_static(b"key1____________"),
                make_doc_bytes("a"),
            ),
            (
                meta(0, 0, 1, c2.as_u64()),
                200i64,
                bytes::Bytes::from_static(b"key2____________"),
                make_doc_bytes("b"),
            ),
        ];
        write_block(&mut writer, journals, producers, entries);

        let frontier = test_frontier(
            vec![jf(
                "j/one",
                0,
                vec![pf(0x01, 100, 0, -100), pf(0x03, 200, 0, -200)],
            )],
            vec![Lsn::new(1, 0)],
        );

        let mut reader = Reader::new(dir.path(), 0);
        let result = collect_entries(&mut reader, &frontier);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, 0); // binding
        assert_eq!(result[0].1, "j/one");
        assert_eq!(result[0].3, c1.as_u64());
        assert_eq!(result[1].3, c2.as_u64());

        // Block should be fully consumed (no remainder tracked).
        assert!(reader.segments.back().unwrap().remainders.is_empty());
    }

    #[test]
    fn test_partial_commit_then_complete() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = Writer::new(dir.path(), 0).unwrap();

        let p1 = producer(0x01);
        let p2 = producer(0x03);
        let c1 = Clock::from_unix(100, 0);
        let c2 = Clock::from_unix(200, 0);

        let journals: HashMap<String, u16> = [("j/one".into(), 0u16)].into();
        let producers: HashMap<uuid::Producer, u16> = [(p1, 0u16), (p2, 1u16)].into();

        let entries = vec![
            (
                meta(0, 0, 0, c1.as_u64()),
                100i64,
                bytes::Bytes::from_static(b"key1____________"),
                make_doc_bytes("a"),
            ),
            (
                meta(0, 0, 1, c2.as_u64()),
                200i64,
                bytes::Bytes::from_static(b"key2____________"),
                make_doc_bytes("b"),
            ),
        ];
        write_block(&mut writer, journals, producers, entries);

        // First checkpoint: only P1 commits.
        let frontier1 = test_frontier(
            vec![jf("j/one", 0, vec![pf(0x01, 100, 0, -100)])],
            vec![Lsn::new(1, 0)],
        );

        let mut reader = Reader::new(dir.path(), 0);
        let result1 = collect_entries(&mut reader, &frontier1);
        assert_eq!(result1.len(), 1);
        assert_eq!(result1[0].3, c1.as_u64());

        // Block should have remainder with P2.
        assert_eq!(reader.segments.back().unwrap().remainders.len(), 1);
        assert_eq!(
            reader.segments.back().unwrap().remainders[0].producers,
            vec![p2]
        );

        // Second checkpoint: P2 commits.
        let frontier2 = test_frontier(
            vec![jf("j/one", 0, vec![pf(0x03, 200, 0, -200)])],
            vec![Lsn::new(1, 0)],
        );

        let result2 = collect_entries(&mut reader, &frontier2);
        assert_eq!(result2.len(), 1);
        assert_eq!(result2[0].3, c2.as_u64());

        // Block should be fully consumed (no remainder tracked).
        assert!(reader.segments.back().unwrap().remainders.is_empty());
    }

    #[test]
    fn test_skip_check_no_matching_producers() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = Writer::new(dir.path(), 0).unwrap();

        let p1 = producer(0x01);
        let c1 = Clock::from_unix(100, 0);

        let journals: HashMap<String, u16> = [("j/one".into(), 0u16)].into();
        let producers: HashMap<uuid::Producer, u16> = [(p1, 0u16)].into();

        let entries = vec![(
            meta(0, 0, 0, c1.as_u64()),
            100i64,
            bytes::Bytes::from_static(b"key1____________"),
            make_doc_bytes("a"),
        )];
        write_block(&mut writer, journals, producers, entries);

        // First checkpoint with a different producer (P2) that doesn't commit P1.
        let frontier1 = test_frontier(
            vec![jf("j/one", 0, vec![pf(0x03, 200, 0, -200)])],
            vec![Lsn::new(1, 0)],
        );

        let mut reader = Reader::new(dir.path(), 0);
        let result1 = collect_entries(&mut reader, &frontier1);
        // P1's entry is not committed (P2 committed but P1 didn't).
        // The block has P1 as remainder.
        assert_eq!(result1.len(), 0);
        assert_eq!(reader.segments.back().unwrap().remainders.len(), 1);

        // Second checkpoint: P2 commits again but P1 still doesn't.
        // The skip check should detect no overlap.
        let frontier2 = test_frontier(
            vec![jf("j/other", 0, vec![pf(0x03, 300, 0, -300)])],
            vec![Lsn::new(1, 0)],
        );
        let result2 = collect_entries(&mut reader, &frontier2);
        assert_eq!(result2.len(), 0);

        // Now commit P1.
        let frontier3 = test_frontier(
            vec![jf("j/one", 0, vec![pf(0x01, 100, 0, -100)])],
            vec![Lsn::new(1, 0)],
        );
        let result3 = collect_entries(&mut reader, &frontier3);
        assert_eq!(result3.len(), 1);
        assert!(reader.segments.back().unwrap().remainders.is_empty());
    }

    #[test]
    fn test_multiple_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = Writer::new(dir.path(), 0).unwrap();

        let p1 = producer(0x01);
        let c1 = Clock::from_unix(100, 0);
        let c2 = Clock::from_unix(200, 0);

        let journals: HashMap<String, u16> = [("j/one".into(), 0u16)].into();
        let producers: HashMap<uuid::Producer, u16> = [(p1, 0u16)].into();

        // Block 1.
        let entries1 = vec![(
            meta(0, 0, 0, c1.as_u64()),
            100i64,
            bytes::Bytes::from_static(b"key1____________"),
            make_doc_bytes("a"),
        )];
        write_block(&mut writer, journals.clone(), producers.clone(), entries1);

        // Block 2.
        let entries2 = vec![(
            meta(0, 0, 0, c2.as_u64()),
            200i64,
            bytes::Bytes::from_static(b"key2____________"),
            make_doc_bytes("b"),
        )];
        write_block(&mut writer, journals, producers, entries2);

        let frontier = test_frontier(
            vec![jf("j/one", 0, vec![pf(0x01, 200, 0, -200)])],
            vec![Lsn::new(1, 1)],
        );

        let mut reader = Reader::new(dir.path(), 0);
        let result = collect_entries(&mut reader, &frontier);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].3, c1.as_u64());
        assert_eq!(result[1].3, c2.as_u64());
    }

    #[test]
    fn test_clock_ordering_resume() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = Writer::new(dir.path(), 0).unwrap();

        let p1 = producer(0x01);
        let c1 = Clock::from_unix(100, 0);
        let c2 = Clock::from_unix(200, 0);
        let c3 = Clock::from_unix(300, 0);

        let journals: HashMap<String, u16> = [("j/one".into(), 0u16)].into();
        let producers: HashMap<uuid::Producer, u16> = [(p1, 0u16)].into();

        let entries = vec![
            (
                meta(0, 0, 0, c1.as_u64()),
                100i64,
                bytes::Bytes::from_static(b"key1____________"),
                make_doc_bytes("a"),
            ),
            (
                meta(0, 0, 0, c2.as_u64()),
                200i64,
                bytes::Bytes::from_static(b"key2____________"),
                make_doc_bytes("b"),
            ),
            (
                meta(0, 0, 0, c3.as_u64()),
                300i64,
                bytes::Bytes::from_static(b"key3____________"),
                make_doc_bytes("c"),
            ),
        ];
        write_block(&mut writer, journals, producers, entries);

        // Commit only up to c1.
        let frontier1 = test_frontier(
            vec![jf("j/one", 0, vec![pf(0x01, 100, 0, -100)])],
            vec![Lsn::new(1, 0)],
        );

        let mut reader = Reader::new(dir.path(), 0);
        let result1 = collect_entries(&mut reader, &frontier1);
        assert_eq!(result1.len(), 1);
        assert_eq!(result1[0].3, c1.as_u64());

        // Commit up to c3.
        let frontier2 = test_frontier(
            vec![jf("j/one", 0, vec![pf(0x01, 300, 0, -300)])],
            vec![Lsn::new(1, 0)],
        );
        let result2 = collect_entries(&mut reader, &frontier2);
        assert_eq!(result2.len(), 2);
        assert_eq!(result2[0].3, c2.as_u64());
        assert_eq!(result2[1].3, c3.as_u64());

        // Fully consumed (no remainder tracked).
        assert!(reader.segments.back().unwrap().remainders.is_empty());
    }

    #[test]
    fn test_multi_binding_same_journal() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = Writer::new(dir.path(), 0).unwrap();

        let p1 = producer(0x01);
        let c1 = Clock::from_unix(100, 0);
        let c2 = Clock::from_unix(200, 0);

        let journals: HashMap<String, u16> = [("j/shared".into(), 0u16)].into();
        let producers: HashMap<uuid::Producer, u16> = [(p1, 0u16)].into();

        // Two entries with the same journal but different bindings.
        let entries = vec![
            (
                BlockMeta {
                    binding: 0,
                    journal_bid: 0,
                    producer_bid: 0,
                    flags: 0,
                    clock: c1.as_u64(),
                },
                100i64,
                bytes::Bytes::from_static(b"key1____________"),
                make_doc_bytes("a"),
            ),
            (
                BlockMeta {
                    binding: 1,
                    journal_bid: 0,
                    producer_bid: 0,
                    flags: 0,
                    clock: c2.as_u64(),
                },
                200i64,
                bytes::Bytes::from_static(b"key2____________"),
                make_doc_bytes("b"),
            ),
        ];
        write_block(&mut writer, journals, producers, entries);

        // Frontier has the same journal under both bindings.
        let frontier = test_frontier(
            vec![
                jf("j/shared", 0, vec![pf(0x01, 100, 0, -100)]),
                jf("j/shared", 1, vec![pf(0x01, 200, 0, -200)]),
            ],
            vec![Lsn::new(1, 0)],
        );

        let mut reader = Reader::new(dir.path(), 0);
        let result = collect_entries(&mut reader, &frontier);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, 0); // binding 0
        assert_eq!(result[1].0, 1); // binding 1
    }

    #[test]
    fn test_flushed_lsn_monotonicity() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = Writer::new(dir.path(), 0).unwrap();

        let p1 = producer(0x01);
        let journals: HashMap<String, u16> = [("j/one".into(), 0u16)].into();
        let producers: HashMap<uuid::Producer, u16> = [(p1, 0u16)].into();
        let entries = vec![(
            meta(0, 0, 0, Clock::from_unix(100, 0).as_u64()),
            100i64,
            bytes::Bytes::from_static(b"key1____________"),
            make_doc_bytes("a"),
        )];
        write_block(&mut writer, journals, producers, entries);

        let frontier1 = test_frontier(
            vec![jf("j/one", 0, vec![pf(0x01, 100, 0, -100)])],
            vec![Lsn::new(1, 0)],
        );

        let mut reader = Reader::new(dir.path(), 0);
        {
            let mut iter = reader.read_frontier(&frontier1).unwrap();
            while iter.next().is_some() {}
        }

        // flushed_lsn goes backward: should fail.
        let frontier2 = test_frontier(vec![], vec![Lsn::new(0, 5)]);
        match reader.read_frontier(&frontier2) {
            Err(err) => assert!(
                format!("{err:?}").contains("flushed_lsn"),
                "expected monotonicity error, got: {err:?}"
            ),
            Ok(_) => panic!("expected monotonicity error"),
        }
    }
}
