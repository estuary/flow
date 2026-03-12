use super::{Reader, Segment, reader};
use crate::log::block;
use anyhow::Context;
use proto_gazette::uuid;
use std::collections::HashSet;
use std::collections::VecDeque;

pub struct FrontierScan {
    // Frontier delta being scanned.
    frontier: crate::Frontier,
    // Set of producers that committed in this Frontier delta.
    frontier_producers: HashSet<uuid::Producer>,
    // Reader powering the scan.
    reader: Reader,
    // Previously read blocks, that have unread (uncommitted) remainders.
    remainders: VecDeque<Remainder>,
    // Next index into reader.remainders to examine.
    remainders_idx: usize,
    // Current block's decompressed data (rkyv-aligned).
    block_buffer: rkyv::util::AlignedVec,
    // Committed entry indices within the current block.
    block_committed: Vec<u16>,
}

/// Sorted index of (binding, journal_bid, producer_bid) => first uncommitted Clock.
pub type ClockIndex = Vec<((u16, u16, u16), uuid::Clock)>;

/// A Remainder is a block that has an unconsumed residual,
/// which is being tracked for future consumption via a later Frontier.
///
/// Remainders are uncommon: in 99.9% of cases, blocks are fully consumed
/// on their very first read.
pub struct Remainder {
    // Arc to the segment file containing this block.
    segment: std::sync::Arc<Segment>,
    // File byte offset of this block's payload within `segment`.
    payload_offset: u32,
    // Uncompressed size of the block payload.
    raw_len: u32,
    // Compressed size, or 0 if uncompressed.
    lz4_len: u32,
    // Sparse index of:
    // (binding, journal_bid, producer_bid) -> first uncommitted Clock
    //
    // The Clocks of a (binding, journal_bid, producer_bid) tuple are strictly
    // ordered (ascending) within the log, so this is the tuple's "resume" point
    // when the block is being re-read under a subsequent checkpoint.
    //
    // The index is sparse: if a block tuple is missing, then the tuple's entries
    // have been fully consumed from this block.
    first_clocks: ClockIndex,
    // Distinct producers with uncommitted entries, sorted on Producer.
    //
    // Built from `first_clocks`. Stored here so we can test for intersection
    // with Frontier producers without re-reading the block.
    producers: Box<[uuid::Producer]>,
}

/// Iterator over committed entries within a single block.
///
/// Created by `FrontierScan::scan_block()`.
/// Borrows the block buffer and committed index from the parent FrontierScan.
pub struct BlockScan<'b> {
    block_buffer: &'b rkyv::util::AlignedVec,
    block_committed: &'b [u16],
    committed_idx: usize,
}

/// Entry yielded by BlockScan, borrowing from the in-memory block buffer.
pub struct Entry<'b> {
    /// Name of the journal this entry was read from.
    pub journal: &'b block::ArchivedBlockJournal,
    /// Producer that wrote this entry.
    pub producer: &'b block::ArchivedBlockProducer,
    /// Block metadata about this document:
    /// binding, block-internal IDs, flags, and clock.
    pub meta: &'b block::ArchivedBlockMeta,
    /// Block document content:
    /// offset, packed key prefix, and document object model.
    pub doc: &'b block::ArchivedBlockDoc<'b>,
}

impl FrontierScan {
    /// Create a new FrontierScan, consuming the Reader.
    ///
    /// Validates the flushed LSN and builds the frontier producer set.
    pub fn new(
        frontier: crate::Frontier,
        mut reader: Reader,
        remainders: VecDeque<Remainder>,
    ) -> anyhow::Result<Self> {
        let frontier_producers: HashSet<uuid::Producer> = frontier
            .journals
            .iter()
            .flat_map(|jf| jf.producers.iter().map(|p| p.producer))
            .collect();

        let flushed_lsn = frontier
            .flushed_lsn
            .get(reader.member_index() as usize)
            .copied()
            .with_context(|| {
                format!(
                    "flushed_lsn has {} entries but member_index is {}",
                    frontier.flushed_lsn.len(),
                    reader.member_index(),
                )
            })?;

        reader.set_flushed_lsn(flushed_lsn)?;

        Ok(Self {
            frontier,
            frontier_producers,
            reader,
            remainders,
            remainders_idx: 0,
            block_buffer: rkyv::util::AlignedVec::new(),
            block_committed: Vec::new(),
        })
    }

    /// Advance the scan forward by stepping to the next log block.
    /// Returns true if a block was read, or false if the scan is complete.
    ///
    /// This routine does not guarantee that the stepped-to block will
    /// have committed entries -- block_iter() may return an Iterator
    /// with no entries. While uncommon, callers should be aware of this
    /// and should simply advance to the next block.
    pub fn advance_block(&mut self) -> anyhow::Result<bool> {
        // Free a previous block buffer, if any, before we allocate a next one.
        // This allows the memory allocator to re-use the same physical memory.
        std::mem::drop(std::mem::take(&mut self.block_buffer));
        self.block_committed.clear();

        // Sequentially walk remainders to surface now-committed entries.
        while self.remainders_idx != self.remainders.len() {
            let Remainder {
                segment,
                payload_offset,
                raw_len,
                lz4_len,
                producers,
                first_clocks,
            } = &mut self.remainders[self.remainders_idx];

            // Trivially skip if no remaining producer of this block committed in this delta.
            if !producers
                .iter()
                .any(|p| self.frontier_producers.contains(p))
            {
                self.remainders_idx += 1;
                continue;
            }

            self.block_buffer = segment.read_block_payload(*payload_offset, *raw_len, *lz4_len)?;
            let block =
                unsafe { rkyv::access_unchecked::<block::ArchivedBlock>(&self.block_buffer) };

            let (committed, next_remainder) = scan_block(block, Some(first_clocks), &self.frontier);

            self.block_committed = committed;

            if let Some((next_producers, next_first_clocks)) = next_remainder {
                // Update Remainder in-place.
                *producers = next_producers;
                *first_clocks = next_first_clocks;
                self.remainders_idx += 1;
            } else {
                // Remainder is now fully consumed.
                // Next remainder assumes `remainders_idx` position.
                self.remainders.remove(self.remainders_idx);
            }

            return Ok(true);
        }

        // All remainders have been processed.
        // Read a next block from the tail of the log.
        let Some(reader::ReadBlock {
            segment,
            payload_offset,
            raw_len,
            lz4_len,
            block_buffer,
        }) = self.reader.read_next_block()?
        else {
            return Ok(false);
        };

        self.block_buffer = block_buffer;
        let block = unsafe { rkyv::access_unchecked::<block::ArchivedBlock>(&self.block_buffer) };

        let (committed, new_remainder) = scan_block(block, None, &self.frontier);
        self.block_committed = committed;

        if let Some((producers, first_clocks)) = new_remainder {
            self.remainders.push_back(Remainder {
                segment,
                payload_offset,
                raw_len,
                lz4_len,
                first_clocks,
                producers,
            });
            self.remainders_idx += 1; // Already read.
        }

        Ok(true)
    }

    /// Return a `BlockScan` iterator over committed entries in the current block.
    /// Note that the Iterator may be empty.
    pub fn block_iter(&self) -> BlockScan<'_> {
        BlockScan {
            block_buffer: &self.block_buffer,
            block_committed: &self.block_committed,
            committed_idx: 0,
        }
    }

    pub fn into_parts(self) -> (crate::Frontier, Reader, VecDeque<Remainder>) {
        (self.frontier, self.reader, self.remainders)
    }
}

impl<'b> Iterator for BlockScan<'b> {
    type Item = Entry<'b>;

    fn next(&mut self) -> Option<Entry<'b>> {
        if self.committed_idx == self.block_committed.len() {
            return None;
        }

        // Safety: caller asserts `block_buffer` is a valid ArchivedBlock.
        let block = unsafe { rkyv::access_unchecked::<block::ArchivedBlock>(self.block_buffer) };

        let entry_idx = self.block_committed[self.committed_idx] as usize;
        self.committed_idx += 1;

        let (meta, doc) = (&block.meta[entry_idx], &block.docs[entry_idx]);

        let journal_idx = block.journals_reverse[meta.journal_bid.to_native() as usize];
        let journal = &block.journals[journal_idx.to_native() as usize];

        let producer_idx = block.producers_reverse[meta.producer_bid.to_native() as usize];
        let producer = &block.producers[producer_idx.to_native() as usize];

        Some(Entry {
            journal,
            producer,
            meta,
            doc,
        })
    }
}

/// Scan all entries in a block, partitioning into committed indices and
/// remainder state. Returns committed indices and optionally (producers,
/// first_clocks) for the remainder.
fn scan_block(
    block: &block::ArchivedBlock<'_>,
    prior_first_clocks: Option<&ClockIndex>,
    frontier: &crate::Frontier,
) -> (Vec<u16>, Option<(Box<[uuid::Producer]>, ClockIndex)>) {
    let last_commits = build_visibility_index(frontier, block);

    let mut committed = Vec::new();
    let mut new_first_clocks: ClockIndex = Vec::new();

    for entry_idx in 0..block.meta.len() {
        let meta = &block.meta[entry_idx];

        let key = (
            meta.binding.to_native(),
            meta.journal_bid.to_native(),
            meta.producer_bid.to_native(),
        );
        let clock = uuid::Clock::from_u64(meta.clock.to_native());

        // Was the entry yielded in a prior checkpoint scan?
        if let Some(first_clocks) = prior_first_clocks {
            match first_clocks.binary_search_by_key(&&key, |(other, _)| other) {
                Ok(idx) => {
                    if clock < first_clocks[idx].1 {
                        continue;
                    }
                }
                Err(_) => continue, // Tuple fully consumed on a prior read.
            }
        }

        // Keys in `last_commits` are unique (see build_visibility_index).
        let visible = if last_commits.len() < VISIBILITY_LINEAR_SCAN_LIMIT {
            last_commits
                .iter()
                .any(|(other, last_commit)| &key == other && &clock <= last_commit)
        } else {
            last_commits
                .binary_search_by_key(&&key, |(other, _)| other)
                .ok()
                .map(|idx| clock <= last_commits[idx].1)
                .unwrap_or_default()
        };

        if visible {
            committed.push(entry_idx as u16);
            continue;
        }

        // Not committed: track as remainder.
        match new_first_clocks.binary_search_by_key(&&key, |(other, _)| other) {
            Ok(_) => {} // Already tracked; first occurrence has the minimum clock.
            Err(pos) => {
                new_first_clocks.insert(pos, (key, clock));
            }
        }
    }

    if new_first_clocks.is_empty() {
        return (committed, None); // Common case: block is fully committed.
    }

    // Map `new_first_clocks` into the set of Producer values it contains.
    let mut producers: Vec<uuid::Producer> = new_first_clocks
        .iter()
        .map(|&((_binding, _journal_bid, producer_bid), _first_clock)| {
            let producer_idx = block.producers_reverse[producer_bid as usize];
            uuid::Producer(block.producers[producer_idx.to_native() as usize].producer)
        })
        .collect();

    producers.sort();
    producers.dedup();

    (committed, Some((producers.into(), new_first_clocks)))
}

/// Build a visibility index by merge-joining the frontier delta with the
/// block's journals and producers.
///
/// Returns a sorted `Vec<((binding, journal_bid, producer_bid), last_commit)>`
/// with unique keys, for either linear or binary search during entry scan.
/// Keys are unique because Frontier journals are unique on (binding, name),
/// block journals have unique bids, and Frontier producers are unique per journal.
fn build_visibility_index(
    frontier: &crate::Frontier,
    block: &block::ArchivedBlock<'_>,
) -> Vec<((u16, u16, u16), uuid::Clock)> {
    // Frontier invariant: journals are ordered & unique by (binding, name).
    debug_assert!(
        frontier
            .journals
            .windows(2)
            .all(|w| (w[0].binding, &w[0].journal) < (w[1].binding, &w[1].journal))
    );

    // Block journals are sorted by name only (no binding dimension).
    // Build fat references once to avoid repeated rkyv inline string checks.
    let block_journals: Vec<(&str, u16)> = block
        .journals
        .iter()
        .map(|b| (b.name.as_str(), b.journal_bid.to_native()))
        .collect();

    let mut result = Vec::new();
    let mut bj_cursor = 0usize;
    let mut prev_binding: u16 = 0;

    for jf in &frontier.journals {
        // Frontier is sorted by (binding, name), but block journals are sorted
        // by name alone. The same journal name may appear under multiple bindings,
        // so we must re-scan block_journals from the start for each new binding.
        if prev_binding != jf.binding {
            bj_cursor = 0;
            prev_binding = jf.binding;
        }

        // Advance cursor to find matching journal name.
        while bj_cursor < block_journals.len() && block_journals[bj_cursor].0 < jf.journal.as_ref()
        {
            bj_cursor += 1;
        }

        if bj_cursor >= block_journals.len() || block_journals[bj_cursor].0 != jf.journal.as_ref() {
            continue; // Journal not in this block.
        }

        let journal_bid = block_journals[bj_cursor].1;

        // Frontier invariant: producers are ordered and unique.
        debug_assert!(
            jf.producers
                .windows(2)
                .all(|w| w[0].producer < w[1].producer)
        );

        // Merge-join frontier producers with block producers (both sorted by producer).
        let mut bp_cursor = 0usize;
        for fp in &jf.producers {
            while bp_cursor < block.producers.len()
                && block.producers[bp_cursor].producer < fp.producer.0
            {
                bp_cursor += 1;
            }
            if bp_cursor < block.producers.len()
                && block.producers[bp_cursor].producer == fp.producer.0
            {
                result.push((
                    (
                        jf.binding,
                        journal_bid,
                        block.producers[bp_cursor].producer_bid.to_native(),
                    ),
                    fp.last_commit,
                ));
            }
        }
    }

    result.sort();
    result
}

/// Threshold below which `scan_block` uses a linear scan of the visibility
/// index rather than binary search. For small indices, linear iteration over
/// cache-local memory outperforms the branch-heavy binary search.
const VISIBILITY_LINEAR_SCAN_LIMIT: usize = 32;

#[cfg(test)]
mod test {
    use super::*;
    use crate::log;
    use crate::log::reader::test_support::{collect_entries, make_frontier, pf_raw, write_block};
    use crate::testing::jf;
    use std::collections::VecDeque;

    #[test]
    fn test_new_validates_member_index() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = log::Writer::new(dir.path(), 0).unwrap();
        write_block(&mut writer, &[("j/one", 1, 0, 10)]);

        // member_index=0 with 1-entry flushed_lsn: ok.
        let frontier = make_frontier(
            &[log::Lsn::new(1, 0)],
            vec![jf("j/one", 0, vec![pf_raw(1, 10)])],
        );
        let reader = Reader::new(dir.path(), 0);
        FrontierScan::new(frontier, reader, VecDeque::new()).unwrap();

        // member_index=1 with 1-entry flushed_lsn: fails.
        let frontier = make_frontier(
            &[log::Lsn::new(1, 0)],
            vec![jf("j/one", 0, vec![pf_raw(1, 10)])],
        );
        let reader = Reader::new(dir.path(), 1);
        assert!(FrontierScan::new(frontier, reader, VecDeque::new()).is_err());
    }

    #[test]
    fn test_all_committed() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = log::Writer::new(dir.path(), 0).unwrap();
        write_block(
            &mut writer,
            &[
                ("j/one", 1, 0, 10),
                ("j/one", 1, 0, 20),
                ("j/one", 1, 0, 30),
            ],
        );

        let frontier = make_frontier(
            &[log::Lsn::new(1, 0)],
            vec![jf("j/one", 0, vec![pf_raw(1, 30)])],
        );
        let reader = Reader::new(dir.path(), 0);
        let scan = FrontierScan::new(frontier, reader, VecDeque::new()).unwrap();
        let (entries, scan) = collect_entries(scan);

        insta::assert_debug_snapshot!(entries, @r#"
        [
            (
                "j/one",
                1,
                0,
                10,
            ),
            (
                "j/one",
                1,
                0,
                20,
            ),
            (
                "j/one",
                1,
                0,
                30,
            ),
        ]
        "#);

        // No remainders.
        let (_, _, remainders) = scan.into_parts();
        assert!(remainders.is_empty());
    }

    #[test]
    fn test_remainder_consumed_next_frontier() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = log::Writer::new(dir.path(), 0).unwrap();
        write_block(
            &mut writer,
            &[
                ("j/one", 1, 0, 10),
                ("j/one", 1, 0, 20),
                ("j/one", 1, 0, 30),
                ("j/one", 1, 0, 40),
            ],
        );

        // Pass 1: commit through 10 only.
        let frontier1 = make_frontier(
            &[log::Lsn::new(1, 0)],
            vec![jf("j/one", 0, vec![pf_raw(1, 10)])],
        );
        let reader = Reader::new(dir.path(), 0);
        let scan = FrontierScan::new(frontier1, reader, VecDeque::new()).unwrap();
        let (entries1, scan) = collect_entries(scan);
        let (_, reader, remainders) = scan.into_parts();

        assert_eq!(entries1.len(), 1);
        assert_eq!(entries1[0].3, 10);
        assert_eq!(remainders.len(), 1);

        // Pass 2: commit through 20. Remainder is re-read but still has
        // unconsumed entries 30 and 40 (the in-place update path).
        let frontier2 = make_frontier(
            &[log::Lsn::new(1, 0)],
            vec![jf("j/one", 0, vec![pf_raw(1, 20)])],
        );
        let scan = FrontierScan::new(frontier2, reader, remainders).unwrap();
        let (entries2, scan) = collect_entries(scan);
        let (_, reader, remainders) = scan.into_parts();

        assert_eq!(entries2.len(), 1);
        assert_eq!(entries2[0].3, 20);
        assert_eq!(
            remainders.len(),
            1,
            "remainder should persist with entries 30-40"
        );

        // Pass 3: commit through 40. Remainder fully consumed.
        let frontier3 = make_frontier(
            &[log::Lsn::new(1, 0)],
            vec![jf("j/one", 0, vec![pf_raw(1, 40)])],
        );
        let scan = FrontierScan::new(frontier3, reader, remainders).unwrap();
        let (entries3, scan) = collect_entries(scan);

        assert_eq!(entries3.len(), 2);
        assert_eq!(entries3[0].3, 30);
        assert_eq!(entries3[1].3, 40);

        let (_, _, remainders) = scan.into_parts();
        assert!(remainders.is_empty());
    }

    #[test]
    fn test_remainder_skipped_unrelated_producer() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = log::Writer::new(dir.path(), 0).unwrap();
        // P1 and P3 in the same block.
        write_block(&mut writer, &[("j/one", 1, 0, 10), ("j/one", 3, 0, 20)]);

        // Pass 1: commit P1 only. P3's entry becomes remainder.
        let frontier1 = make_frontier(
            &[log::Lsn::new(1, 0)],
            vec![jf("j/one", 0, vec![pf_raw(1, 10)])],
        );
        let reader = Reader::new(dir.path(), 0);
        let scan = FrontierScan::new(frontier1, reader, VecDeque::new()).unwrap();
        let (entries1, scan) = collect_entries(scan);
        let (_, reader, remainders) = scan.into_parts();

        assert_eq!(entries1.len(), 1);
        assert_eq!(remainders.len(), 1);

        // Pass 2: frontier mentions P5 (not P3). Remainder should be skipped.
        let frontier2 = make_frontier(
            &[log::Lsn::new(1, 0)],
            vec![jf("j/one", 0, vec![pf_raw(5, 100)])],
        );
        let scan = FrontierScan::new(frontier2, reader, remainders).unwrap();
        let (entries2, scan) = collect_entries(scan);

        assert!(entries2.is_empty());
        // Remainder is still there (not consumed).
        let (_, _, remainders) = scan.into_parts();
        assert_eq!(remainders.len(), 1);
    }

    #[test]
    fn test_multiple_journals_selective_commit() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = log::Writer::new(dir.path(), 0).unwrap();
        write_block(&mut writer, &[("j/alpha", 1, 0, 10), ("j/beta", 3, 0, 20)]);

        // Commit only j/alpha with P1.
        let frontier = make_frontier(
            &[log::Lsn::new(1, 0)],
            vec![jf("j/alpha", 0, vec![pf_raw(1, 10)])],
        );
        let reader = Reader::new(dir.path(), 0);
        let scan = FrontierScan::new(frontier, reader, VecDeque::new()).unwrap();
        let (entries, scan) = collect_entries(scan);

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "j/alpha");

        let (_, _, remainders) = scan.into_parts();
        assert_eq!(remainders.len(), 1);
    }

    #[test]
    fn test_multi_binding_same_journal() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = log::Writer::new(dir.path(), 0).unwrap();
        // Block has "j/aaa" (sorts before "j/one") and "j/one" under two bindings.
        // "j/aaa" forces the cursor advancement loop when scanning for "j/one".
        write_block(
            &mut writer,
            &[
                ("j/aaa", 1, 0, 5),  // filler to force cursor skip
                ("j/one", 1, 0, 10), // binding 0
                ("j/one", 1, 1, 20), // binding 1
            ],
        );

        // Frontier includes journals that exercise all build_visibility_index paths:
        // - "j/missing" (binding 0): not in block => "journal not in block" continue
        // - "j/one" (binding 0): matches after cursor skips past "j/aaa" in block
        // - "j/one" (binding 1): different binding resets bj_cursor and re-scans
        // Frontier journals must be sorted by (binding, name).
        let frontier = make_frontier(
            &[log::Lsn::new(1, 0)],
            vec![
                jf("j/missing", 0, vec![pf_raw(1, 99)]),
                jf("j/one", 0, vec![pf_raw(1, 10)]),
                jf("j/one", 1, vec![pf_raw(1, 20)]),
            ],
        );
        let reader = Reader::new(dir.path(), 0);
        let scan = FrontierScan::new(frontier, reader, VecDeque::new()).unwrap();
        let (entries, scan) = collect_entries(scan);

        // Both bindings of "j/one" committed; "j/aaa" has no frontier match.
        assert_eq!(entries.len(), 2);
        assert_eq!((entries[0].2, entries[0].3), (0, 10));
        assert_eq!((entries[1].2, entries[1].3), (1, 20));

        // "j/aaa" entry is uncommitted remainder.
        let (_, _, remainders) = scan.into_parts();
        assert_eq!(remainders.len(), 1);
    }

    #[test]
    fn test_multiple_blocks_sequential() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = log::Writer::new(dir.path(), 0).unwrap();

        write_block(&mut writer, &[("j/one", 1, 0, 10)]);
        write_block(&mut writer, &[("j/one", 1, 0, 20)]);
        write_block(&mut writer, &[("j/one", 1, 0, 30)]);

        let frontier = make_frontier(
            &[log::Lsn::new(1, 2)],
            vec![jf("j/one", 0, vec![pf_raw(1, 30)])],
        );
        let reader = Reader::new(dir.path(), 0);
        let scan = FrontierScan::new(frontier, reader, VecDeque::new()).unwrap();
        let (entries, _) = collect_entries(scan);

        insta::assert_debug_snapshot!(entries, @r#"
        [
            (
                "j/one",
                1,
                0,
                10,
            ),
            (
                "j/one",
                1,
                0,
                20,
            ),
            (
                "j/one",
                1,
                0,
                30,
            ),
        ]
        "#);
    }
}
