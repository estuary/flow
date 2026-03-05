# Queue File: RocksDB Design

## Background

The `shuffle` crate routes documents from Slices to per-member Queues for on-disk staging.
The Queue receives Enqueue requests containing documents and must support
efficient dequeue by the downstream consumer, which reads per-binding in
clock order across all journal×producer combinations.

Currently queue storage is a TODO.
This document describes a storage architecture for implementation.

### Processing Model

The Queue (RPC handler) and its consumer co-exist within a single process
(the member's reactor). Both obtain handles to a shared, open RocksDB
instance specific to that Queue through a central registry of the shuffle
Service, which manages per-Queue DB instance lifecycle.

The Queue hands off ordered Enqueue requests from Slices to the DB.
The consumer reads from the same DB after learning of available data via
NextCheckpoint. The consumer also informs the DB of data which has been
dequeued and can be deleted, for disk reclamation.

There is no RPC boundary between the consumer and the queue's storage:
all interactions are through the shared DB handle.

### Workload Characteristics and Objectives

Key characteristics of the workload:

**Write pattern**: Enqueue documents arrive in descending priority
across bindings, and mostly ascending clock within a binding
(micro jumps within a macro ascending clock sequence are expected).

Within a binding, Enqueues have all the characteristics of time-series data.
We want to exploit this in our tuning of RocksDB to reduce write amplification.

Further, bindings themselves are Zipfian: while many can exist,
a small fraction will be producing data at a given time and the long tail is nearly idle.

Finally, clocks are usually well-grouped globally:
it's typical that large sequential runs of clock values will be associated with
just one (journal, producer) author. This property falls out of how producers
assign clocks: they initialize a transaction clock using system time
(a larger jump of high clock bits) and then increment by-one for each document assignment.
In the pessimistic case, it's possible that two authors would sample close system times
and then write large-enough runs of clocks that they interleave, but this is rare.

**Read pattern**: Reads should be well optimized for quickly extracting
queued data, potentially in very large quantities.

The overwhelming majority of producers are long-lived
and commit frequent, short-lived transactions. We may Queue for extended
periods of time between consumer dequeues, and it's *extremely* common that
essentially ALL documents below a given low- and high-watermark clock are
included in a NextCheckpoint and eligible for dequeue.

Due to crash recovery, it's possible that the oldest runs of Clocks may
remain pending indefinitely.

Within a binding, it's crucial that we walk earlier versions of committed keys
before older versions. Given committed producers P1 and P2, we must read
a given key K at P1 @100, P2 @200, and then P1 @300.

This suggest an ideal strategy of dequeuing in binding order,
and by ascending clock within each binding,
filtering rows to determine whether a given producer and clock are visible
under the current NextCheckpoint Frontier.

Further, a reader walking in ascending clock order can easily identify ranges
and counts of clocks which are pending and eligible for deletion (versus pending),
putting it in an ideal position to author DeleteRange or SingleDelete cleanup operations.
It's also able to build metadata such as the first pending clock of a given
(journal, producer) author.

**Scale range**: Queue storage must scale from small, quick transactions
(10's of keys) to very large ones (tens of millions of keys, 100+ GB raw).

In practice, even at scale, the great majority of bindings and journals
are low throughput. Live data in the queue is from a smallish number of
active (binding, journal, producer) tuples at any given time.

**Back-pressure tolerant**: Processing overhead in scaling Queue storage
and the downstream consumer will back-pressure up to Queue RPCs,
and from there to Slice RPCs, and from there to journal reads.

This is absolutely intended: Queue storage is a large disk backed buffer,
but we want to bound its on-disk size and slow down journal reads which
are running _too_ far ahead of consumption.

While we'd like to keep write amplification low, amplification and
corresponding back-pressure is acceptable as a trade-off for accelerating
the bulk read path, which must be fast. As intuition for why, dequeue is merely
the start of a transaction lifecycle which encompasses further data movement
and processing, often in external systems. In contrast, shuffled reads happen
continuously, building out the next transaction in parallel to end-to-end
processing of a current transaction.

To maximize available buffering capacity, we want to use adaptive compression.
The smallest transactions would be uncompressed, and would progressively
use LZ4 → zstd as Queue data volume grows on disk.

**Ephemeral**: Queue state is discarded on session end (fail-fast recovery model).
We don't need durability.


## Mapping to Leveled RocksDB

### Why RocksDB

Queue storage needs an engine that can absorb high-throughput, semi-sorted
writes while supporting fast sequential scans and efficient range deletion.
RocksDB is a natural fit:

**Progressive compression via `compression_per_level`**:
Low-latency consumers will frequently dequeue small amounts of available data
which should do so with minimal overhead. Slow or periodic consumers will
accumulate large amounts of buffered data, where we must pay attention to
storage efficiency.

`compression_per_level` maps directly to this split: L0–L1 uncompressed for fast
read-back of fresh data, L2–L3 LZ4 for modest ratio at low CPU,
L4+ zstd for maximum compression of large amounts of buffered data.
Compaction depth is a near proxy for total disk usage, which is what we
actually want to adapt to.

**Bounded read amplification**: Leveled compaction maintains at most one SST
per level L1+ with overlapping key ranges. A sequential range scan over a
binding's clock range merges at most one file per level.

**Trivial move optimization**: When an L(n) SST has no overlapping keys in L(n+1),
RocksDB moves it down without rewriting (`kMinOverlappingRatio`). Our write
pattern (ascending clocks per binding) means fresh SSTs should often land in
empty key space, making trivial moves more common and reducing write
amplification for the hot path.

**MemTable as write buffer, no WAL**: With `write_buffer_size` capped (e.g. 256MB)
and WAL disabled, the MemTable serves as the write buffer. Writes are
immediately visible to readers without fsync cost. The MemTable's skiplist
supports lock-free concurrent reads with a single-writer mutex for inserts.
Low expected write contention - all Puts come from the merge task,
with occasional cleanups from the consume task.

**Shared resource management via `Env` and `Cache`**: Multiple Queue DBs
coexist on the same NVMe. A shared `Env` lets RocksDB coordinate thread pool
usage (compaction, flush) across instances, and a shared block `Cache`
provides a unified memory budget. This prevents any single Queue from
monopolizing I/O or memory at the expense of its neighbors.

**BlobDB for value outliers**: Documents vary widely in size (p50 ~16KB,
p99 ~1MB, max ~64MB). Values ≥1MB are stored in BlobDB blob files rather
than inline in SSTs. This eliminates write amplification for large outlier
values (only a ~20-byte reference is rewritten during compaction) while
keeping the common-case read path optimal (small/medium values inline in
SSTs, sequential block scan). Blob files are zstd-compressed regardless of
level, since outlier documents are infrequent and compress extremely
well. Blob GC is enabled; outlier-heavy blob files have few entries and
tend to become fully reclaimable in coarse commit-aligned batches.

### Data Model

#### Journal Mappings

The mapping column family stores bidirectional mappings between journal names
and compact 4-byte local IDs (`journal_id`). These IDs are monotonically
ascending integers assigned by the Queue, local and specific to that DB instance.

**Key layout** (two entries per journal, enabling both directions):

| Key | Value |
|---|---|
| `b"j" ++ journal_name` | `journal_id` (4 bytes, big-endian) |
| `b"J" ++ journal_id` (4 bytes, big-endian) | `journal_name` |

The Queue creates mappings for novel journals on first encounter during Enqueue.
The mapping CF is optimized for point lookups (high bloom filter bits-per-key,
small block size).

**In-memory caching**: Both Queue and consumer maintain in-memory caches of
journal mappings. The Queue populates its cache on insert; the consumer
populates lazily as it encounters novel `journal_id` values during iteration,
falling back to a point lookup in the mapping CF. In practice, the working set
of active journals is small relative to the total, so the cache hit rate is
near 100% in steady state.

#### Enqueues

The enqueue column family stores documents keyed for efficient per-binding,
clock-ordered iteration. It is optimized for sequential range scans, not
point lookups (large block size, no bloom filters, prefix compression).
BlobDB is enabled with `min_blob_size = 1MB` and `blob_compression_type = zstd`
to handle outlier documents without penalizing the common-case read path.

**Key**: 20 bytes, fixed-width, big-endian:

| Field | Bytes | Description |
|---|---|---|
| `binding` | 2 | Binding index |
| `clock` | 8 | Publication clock (UUID v1 timestamp) |
| `journal_id` | 4 | Local journal ID from mapping CF |
| `producer` | 6 | Raw producer ID (from UUID) |

The key sorts first by binding, then by ascending clock within a binding.
Ties on clock are broken by `(journal_id, producer)`, which provides a
stable, deterministic order but has no semantic significance — the consumer
just needs a total order to walk.

**Value**: variable-length, big-endian:

| Field | Size | Description |
|---|---|---|
| `offset` | 8 bytes | Journal byte offset of this document |
| `flags` | 1 byte | Flags of this enqueue ("valid", others TBD) |
| `key_len` | 3 bytes | Length of the key |
| `key` | `key_len` | Packed shuffle key (FDB tuple-encoding) |
| `doc` | remainder | `doc::ArchivedNode` bytes (rkyv encoding) |

Note that `flags` and `key_len` can be composed as a u32,
where `key_len` is the low three bytes.

**Range iteration**: The consumer scans `(binding, low_clock, 0, 0)` through
`(binding, high_clock, MAX, MAX)` to walk all documents for a binding within
a clock window. Within that window, every key is visited; filtering by
producer commit state happens in the consumer's iteration logic, not via
seek operations.

### Queue Lifecycle

The Queue is the RPC-facing writer. It merge-orders incoming Slice streams
by `(priority DESC, clock ASC)`, draining higher-priority streams first,
and writes Enqueue requests to the DB as they arrive.

**Enqueue path**:

1. For each document, resolve the journal name to a `journal_id`.
   Check the in-memory cache first; on miss, look up the mapping CF.
   If absent, assign the next monotonic `journal_id`, write both directions
   to the mapping CF, and cache.
2. Construct the 20-byte enqueue key: `(binding, clock, journal_id, producer)`.
3. `Put` the key and value directly to the enqueue CF. No WriteBatch
   buffering — the MemTable, operating without WAL, is the buffer
   and writes are immediately visible to the consumer.
4. Back-pressure: if the MemTable is full and a flush-to-L0 is in progress,
   `Put` will block indefinitely until space is available. This is the intended:
   it stalls the merge-order drain, which stalls Slice streams, which stalls journal reads,
   until the downstream consumer block clears or the task fails.

   We don't want to _synchronously_ block (we're in a tokio task), so we use
   `WriteOptions::set_no_slowdown()` in a retry loop and `tokio::time::sleep()`
   with exponential backoff (capped at 5s) if `Put` returns `Status::Incomplete`.

**Flush path**:

When a Queue reads `Flush`, it responds with `Flushed` immediately.
Note Queue's merge-order drain does not advance a Slice's stream until its latest
`Enqueue` has been popped and written, so a `Flush` implies all preceding Enqueues
have landed in the MemTable as `Put` calls. There is no additional work to perform,
as "visible" means "written to MemTable," and that happened synchronously during Enqueue.

**Ordering guarantee**: The Queue's merge-order drain ensures documents
arrive in `(priority DESC, clock ASC)` order across Slice streams. Within
the enqueue CF, key sort order is `(binding, clock, journal_id, producer)`,
which matches the desired read order. Since priority determines binding
grouping (higher priority bindings drain first), the write pattern is
roughly append-only within each binding — clocks are mostly ascending.
This minimizes overlap, increasing the likelihood of trivial moves as SSTs
move into deeper tree levels (or even L0->L1, if just one binding is active).

### Consumer Lifecycle

The consumer reads from the shared DB handle to dequeue committed documents
for downstream processing. It is triggered by NextCheckpoint, which provides
a frontier of `(journal, producer) → last_commit` per binding. The consumer
is the sole reader and the sole issuer of deletes.

#### Producer IDs

The consumer maintains an internal mapping of
`producer` (6 bytes) → `local_producer_id` (4 bytes)
for use in internal indexes. This mapping is ephemeral:
it's rebuilt lazily from observed keys and is never persisted.

The 4-byte shorthand matters for the consumer's sorted in-memory indexes
where `(journal_id, local_producer_id)` fits in 8 bytes instead of 16;
it has no bearing on the on-disk format.

(Aside: while the wire protocol uses 8-byte `sfixed64` for producers,
this is merely for protobuf convenience. Only the low 6 bytes are used)

#### Per-Binding State

The consumer maintains two in-memory indexes:

**Per-binding Commit Index**: Built from the NextCheckpoint delta, as an
index of `(journal_id, local_producer_id) → last_commit` for every
producer with committed data in a given binding.
This index is discarded and rebuilt on each binding of each NextCheckpoint.

**Low-Clock Index**: An index of `(binding, journal_id, local_producer_id) → signed_clock`,
tracking the lower bound of un-dequeued data for each producer using sign encoding
(mirroring the wire protocol's offset convention):
- **Positive (`+first_pending_clock`)**: The clock of the first pending document
  encountered during a prior dequeue scan. This is a tight floor on the first
  documents of the journal producer.
- **Negative (`-last_commit`)**: Fully dequeued through `|value|`; no pending
  documents existed beyond this point at the time of the last scan. The floor is
  `|value|` — new documents (committed or pending) must have clock > `|value|`
  due to Slice-side duplicate filtering.

This index is updated incrementally during dequeue and persists across dequeue cycles.
It may be truncated or discarded and then re-initialized without loss of correctness.

#### Dequeue Algorithm

On each dequeue cycle, for each binding in order:

1. **Build binding's Commit Index**: Mapped from NextCheckpoint.

2. **Compute `high_clock`**: `max(last_commit)` across all entries in the
   binding's Commit Index. As `last_commit` is an ACK_TXN clock which commits
   preceding CONTINUE_TXN clocks, or an OUTSIDE_TXN clock, no committed enqueue
   key can be beyond this ACK clock, so there's nothing useful to scan past it.

3. **Compute `low_clock`**: For each `(journal_id, local_producer_id)` in
   the binding's Commit Index, look up the Low-Clock Index entry for
   `(binding, journal_id, local_producer_id)`:
   - Positive entry → floor = entry value
   - Negative entry → floor = |entry|
   - Missing entry → floor = 0

   Then ratchet the entry to `-last_commit` if `last_commit > floor`,
   which reflects the producer's commit by establishing a new floor
   for a future dequeue cycle. Step 5 will overwrite back to a positive
   entry if pending documents are encountered during the scan.
   (Note that `last_commit < floor` is possible on rollback).

   Finally, `low_clock = min(floor)` (pre-ratchet value) across all entries.
   This skips past the prefix of already-dequeued data and un-compacted
   DeleteRange tombstones.

4. **Seek and scan**: Open an iterator at prefix `(binding, low_clock)` and
   scan until reaching a key beyond the prefix `(binding, high_clock)`,
   or EOF.

5. **For each key** `(binding, clock, journal_id, producer)`:
   - Resolve `producer` to a `local_producer_id` via the in-memory mapping
     (assign a new one on first encounter).
   - Look up `(journal_id, local_producer_id)` in the Commit Index.
     - **Found AND `clock ≤ last_commit`**: This document is committed.
       Emit it to the downstream consumer.
     - **Not found OR `clock > last_commit`**:
       The document is pending — skip it. If the Low-Clock Index entry for
       this `(binding, journal_id, local_producer_id)` is missing or negative
       (as set by step 3's ratchet), set it to `+clock` — this is the first
       pending clock. Leave positive entries unchanged; they already record
       an earlier pending clock from this or a prior scan.

6. **Track deletion runs**: As iteration proceeds, accumulate contiguous
   runs of keys that were dequeued (committed). They're eligible for deletion.
   - When a run ends (because we encounter a pending key, or reach EOF):
     - If the run has more than one key:
      `DeleteRange(run_start inclusive, run_end exclusive)` where `run_end` is:
       - a pending key, or
       - the key which terminated the scan, or
       - the prefix `(binding, high_clock+1)`
     - If the run is exactly one key: `SingleDelete(key)`
       (safe given assumed-correct exactly once shuffle sequencing behavior).
     - Invariant: a single key is deleted by DeleteRange or SingleDelete, not both.
   - Deletions are applied immediately to the DB at the completion of each run.

#### Low-Clock Index Properties

Both sign variants produce tight floors:
- **Positive entries** record the exact clock of the first un-dequeued document.
  The preceding scan was exhaustive, and Slice-side duplicate filtering prevents
  new documents from arriving with clocks ≤ max(`last_commit`, `max_continue`).
- **Negative entries** record the committed watermark beyond which no pending
  documents existed as-of the latest scan. New documents must have
  clock > `|value|` for the same reason.

Producers not in the current checkpoint and not encountered during iteration
are left unchanged — their entry still serves as a valid floor for future scans.

**Overshoot is impossible**: Positive entries record observed pending
positions; negative entries record the committed watermark. Neither can
advance past un-dequeued data.

**Undershoot is safe**: If an entry is missing or stale, the consumer scans
from an earlier point and re-encounters already-deleted keys (which RocksDB
skips via tombstones) or pending keys (which are correctly handled). This
is slower but correct.

**Eviction is safe**: The index can be bounded. An evicted entry
causes its producer's floor to default to zero, triggering a full
scan from the beginning of the binding.
