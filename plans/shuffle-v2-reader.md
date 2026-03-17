# Log Reader

## Background

The shuffle crate writes documents into on-disk segment log files via `log::Writer`,
merging documents from all Slices in (priority DESC, adjusted_clock ASC) order.
The external coordinator polls `SessionClient::next_checkpoint()` to receive
successive `Frontier` deltas describing newly-committed transactions. Each delta
contains the journals and producers that committed since the last checkpoint,
along with a per-member `flushed_lsn` barrier.

The coordinator distributes each checkpoint to its member workers. Each member
must efficiently read committed entries from its local log files and process them
for downstream use (derivation transforms, materialization bindings, etc.).

This document proposes a `shuffle::log::reader` module for this purpose,
as the consumer-side complement to `log::Writer`.

## Objectives

- Read log segments and blocks up to `flushed_lsn` for a given member, yielding
  entries whose transactions are visible in the checkpoint's Frontier delta.
- Minimize I/O by tracking per-block consumption state, skipping fully-consumed
  blocks and blocks whose only remaining entries belong to producers absent from
  the current delta.
- Support garbage collection of fully-consumed segment files.
- Yield entries in log write order: block LSN order, then entry index within
  each block. This is the (priority DESC, adjusted_clock ASC) merge order
  imposed by the Writer's AppendHeap.
- Keep at most one block in memory at a time, leaning on the OS page cache.

## Key Insight: The Frontier Is a Delta

The `Frontier` returned by `next_checkpoint()` is a *delta*, not a full restatement.
It contains only the journals and producers that committed since the last
checkpoint, with their `last_commit` clock. Producers that made no new commits
are absent from the delta entirely. See the CheckpointPipeline (README §11) for
how deltas are produced.

Note that `hinted_commit` in `ProducerFrontier` is not relevant for log reading.
The reader uses only `last_commit` for visibility decisions.

This has two important consequences:
1. The visibility index for a block only needs entries for producers that appear
   in both the block AND the delta. A merge-join between two sorted structures.
2. Blocks lingering due to retired producers (whose transactions never committed)
   are trivially skipped: the retired producer won't appear in any future delta.

## Data Model

```
Reader
  directory: PathBuf
  member_index: u32
  segments: VecDeque<Segment>
  buf: Vec<u8>                       // reusable decompression buffer

Segment
  base_lsn: Lsn                     // segment number, block 0
  file: File
  blocks: Vec<Block>
  next_file_offset: u64             // byte offset of next unread block in file

Block
  lsn: Lsn
  remainder: BlockRemainder         // fully consumed if remainder.first_clocks is empty

BlockRemainder
  producers: Vec<Producer>           // sorted; external IDs for skip check without I/O
  first_clocks: Vec<(u16, u16, u16, Clock)>
                                     // sorted (binding, journal_bid, producer_bid, first_clock)
                                     // uses block-internal BIDs; sort order is for
                                     // binary search lookup, not semantic ordering
  file_offset: u64                   // byte position in segment file
  raw_len: u32                       // uncompressed size
  lz4_len: u32                       // compressed size (0 if uncompressed)
```

### Block Lifecycle

A `Block` is created when first read from the segment file. If all entries are
committed on that first read, the block is empty and permanently
skipped on future checkpoints. This is the common case (~99.9% of blocks).

If some entries are uncommitted, `remainder` captures:
- The `Producer` identities with uncommitted entries. This enables the skip
  check (step 1) to detect blocks with no overlap against the delta's producers
  *without any I/O* — the block need not be re-read from disk.
- Per `(binding, journal_bid, producer_bid)` the `first_clock`: the first
  uncommitted clock in this block for that tuple. This is the resume point
  for future reads.
- The file offset and lengths for efficient re-read without sequential scan.

When a future checkpoint commits all remaining entries, `remainder` becomes
`None` and the block is consumed.

### Clock Ordering Invariant

Entries for a specific `(binding, journal, producer)` tuple appear in strict
clock order within the log. This is locally true when scoped to a binding and
journal, even though clocks for the same producer across different journals
are only mostly ordered relative to each other.

This invariant makes `first_clock` correct as a resume point: all entries
before `first_clock` in a block were already yielded, and all entries from
`first_clock` onward are candidates for the current checkpoint.

### Segment Lifecycle

Segments form a `VecDeque`. New segments are discovered lazily: when reading
hits EOF of the current segment file, the reader opens the next segment file
(segment number + 1) and pushes a new `Segment` to the back. Reading never
advances past `flushed_lsn`, so any segment file referenced by `flushed_lsn`
is guaranteed to exist.

Garbage collection happens at segment transitions. When the reader steps past
the end of a segment (hits EOF and moves to the next), it checks whether all
blocks in the completed segment have `remainder = None`. If so, the segment
file is closed and deleted, and the `Segment` is removed from the `VecDeque`.

## Initialization

The Reader is created with a directory and member index, starting with an empty
`VecDeque<Segment>`. On the first `read_checkpoint` (which must have a `flushed_lsn`
at or greater than `Lsn(1, 0)`), we begin reading from byte zero of `Lsn(1, 0)`,
matching the Writer's starting LSN. Segment files are located using the same
naming convention as `log::Writer`: `mem-{member_index:03}-seg-{segment:012x}.flog`.

The reader is ephemeral — its state is not persisted across sessions. On any
error the entire session tears down (fail-fast), and segment files are discarded
along with all other session state. This makes segment deletion during GC safe:
there is no crash-recovery scenario where the reader would need to re-read a
deleted segment.

`read_checkpoint` will fail if `flushed_lsn` is empty, shorter than
`member_index`, or less than a prior member checkpoint value of `flushed_lsn`.

## Read Algorithm

### Per-Checkpoint Entry Point

```
read_checkpoint(frontier: &Frontier):
    flushed_lsn = frontier.flushed_lsn[member_index]
    committed_producers: HashSet<Producer> = collect from frontier

    walk segments and blocks in LSN order:
        for existing blocks: use metadata to skip or re-read
        for new blocks: read from file, create Block structs
        at EOF: transition to next segment, check GC
        stop when lsn > flushed_lsn
```

### Block Processing (Existing Block with Remainder)

```
1. Quick skip check:
   - If remainder.producers ∩ committed_producers is empty, skip.
     (No producer in this block committed in this checkpoint.)

2. Read block from file using cached (file_offset, raw_len, lz4_len).
   Decompress into reusable buffer if lz4_len > 0.

3. Access ArchivedBlock via rkyv zero-copy.

4. Build visibility index via sorted merge-join:
   - Block journals are pre-sorted on journal_name => journal_bid
   - Block producers are pre-sorted on producer_value => producer_bid
   - Merge-join with Frontier:
     - The Frontier is sorted by (journal, binding). Block journals are
       sorted by journal_name only (no binding). A journal name may appear
       under multiple bindings in the Frontier, grouped consecutively.
     - Walk Frontier entries linearly. For each entry, find the matching
       block journal by name (sorted merge scan with the block's journal list).
       Consecutive bindings of the same journal share the same cursor position.
     - For each (journal, binding, block_journal) match, merge-join the Frontier's
       producers (sorted by producer) with the block's producers
       (also sorted by producer). Each match emits:
         (binding, journal_bid, producer_bid) → last_commit
   - Result: sorted Vec for binary search during entry scan.

5. Scan entries in storage order:
   - Look up (binding, journal_bid, producer_bid) in first_clocks.
     If entry clock < first_clock, skip (already yielded).
   - Look up (binding, journal_bid, producer_bid) in visibility index.
     If found and clock <= last_commit, yield entry.
   - Track new first_clock per tuple: first uncommitted clock seen,
     upserting in-place, using i64::MAX as sentinel for "none".
     It's rare for us to ever insert into this map (all entries consumed on first read).

6. Update remainder:
   - Drain i64::MAX "none" entries from first_clocks
   - Rebuild producers from first_clocks.
   - If empty, set remainder = None (block fully consumed).
```

### Block Processing (New Block)

Same as above, except:
- No skip check (block hasn't been read before).
- Read sequentially from `segment.next_file_offset`.
- No `first_clocks` to filter against (index starts empty).
- After processing, push `Block` to `segment.blocks`.
- Update `segment.next_file_offset`.

### Segment Transition

When reading hits EOF of the current segment file:
1. Open segment file for `(segment_number + 1)`.
2. Push new `Segment` to back of `VecDeque`.
3. Check if all blocks of the completed segment have no remainder
   If yes: close file handle, delete the file, remove `Segment`
4. Continue reading from the new segment.

GC happens when re-reading as well.
On logical transition from a Segment with a just-completed remainder, to another Segment (also previously read),
we evaluate GC.


## Yielded Entry

We yield entries using a lending iterator pattern, providing the consumer with:

```
binding: u16
journal_name: &str         // resolved from block's delta-encoded journals
producer: Producer          // resolved from block's producer table
clock: u64
flags: u16                  // 0x0001 = schema validation passed
offset: i64                 // journal byte offset
packed_key_prefix: &[u8; 16]
doc: &ArchivedNode          // zero-copy from block buffer
```

These are borrowed from the in-memory block buffer, which remains valid
until the iterator is stepped again.

## Error Handling

Follows the shuffle crate's fail-fast model. Any I/O or decoding error
is returned as `anyhow::Error` and is expected to tear down the session.
No attempt is made to recover from partial reads or corrupt blocks.

## Synchronous I/O

The reader uses synchronous `std::fs::File` operations, matching the writer.
Local file I/O backed by the OS page cache is effectively instantaneous.
The caller may wrap `read_checkpoint` in `spawn_blocking` if needed.
