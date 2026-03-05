# Shuffle

Shuffle coordinates reading documents from Gazette journals across
distributed task shards, routing each document to the correct shard
based on its shuffle key, and delivering documents in globally-ordered
queues for downstream processing.

It serves derivation transforms, materialization bindings,
and ad-hoc collection reads. In each case, the problem is the same:
source collection journals are partitioned across brokers, but the
task processing those documents is sharded by a (potentially different)
key. Shuffle bridges that gap.

## Architecture

The system is built from three layered gRPC RPCs, defined in
`go/protocols/shuffle/shuffle.proto`, each implemented as an async actor
and forming a hierarchy. For M members, the system uses M Slice
streams and M² Queue streams (each Slice opens one Queue to every member):

```
Coordinator (external caller, typically runs on shard-000)
   |
   ▼
  Session (one per task)
     |
     ├─▶ Slice 0 ─┬─▶  Queue 0  (in-process)
     │            ├─▶  Queue 1  (remote)
     │            └─▶  Queue 2  (remote)
     ├─▶ Slice 1 ─┬─▶  Queue 0  (remote)
     │            ├─▶  Queue 1  (in-process)
     │            └─▶  Queue 2  (remote)
     └─▶ Slice 2 ─┬─▶  Queue 0  (remote)
                  ├─▶  Queue 1  (remote)
                  └─▶  Queue 2  (in-process)
```

**Session** (`session/`): Top-level coordinator-facing RPC. Opened by the
external coordinator (e.g. shard-000 of a derivation or materialization).
Manages the session lifecycle, routes discovered journals to Slices,
and aggregates progress into checkpoints.

**Slice** (`slice/`): Per-member RPC opened by the Session. Each Slice
watches journal listings for its assigned bindings, reads documents from
journals, sequences them, validates them, extracts shuffle keys, and routes
documents to the appropriate Queue(s) based on key hash.

**Queue** (`queue/`): Per-member RPC opened by each Slice. All Slices
targeting the same member join into a single QueueActor, which merges
documents across Slices in (priority DESC, adjusted_clock ASC) order
and writes them to local on-disk queue storage.

### Member Topology

A session has N **members**, each owning a disjoint range of the 2D
(key_hash, r_clock) space. Members tile the full `[0, 0xFFFFFFFF]` range
in both dimensions. Each member runs one Slice actor and one Queue actor.

### Dequeue

Dequeue is not modeled in the Shuffle protocol.
Instead, a consumer directly accesses queue storage and processes enqueued
documents up to the `last_commit` clocks reported on each checkpoint.

## Comparison with legacy shuffle implementation (`go/shuffle/`)

The legacy shuffle implementation has several limitations that motivated
this crate:

**Replay reads → conservative reads**: The legacy system reads optimistically
from the latest journal offset. When an uncommitted transaction later commits,
the system performs bounded "replay reads" to re-read that data, which is high
latency and performs poorly. This implementation instead reads conservatively
from the earliest `begin_offset` of any uncommitted producer in the checkpoint,
eliminating replay reads entirely. The tradeoff is potentially re-reading already-
committed data on startup if producers have long-running uncommitted transactions.

**Per-shard RPCs → shared streams**: The legacy system starts an RPC per
(shard, journal) pair, which doesn't scale: at M=10 shards with N=100k
journals, that's up to M×N = 1M concurrent RPCs, and each ACK is broadcast
to every shard. This implementation uses M + M² streams total (M Slice + M²
Queue), independent of journal count. At M=10 with N=100k, that's 110 streams
instead of 1M. Listing watches are also distributed across members (each
watches ~B/M bindings) rather than duplicated on every shard.

**In-memory staging → disk-backed queues**: The legacy system holds shuffled
documents in memory buffers, limiting how far reads can progress ahead of
downstream processing. This implementation writes to on-disk queue files,
allowing reads to run well ahead without memory pressure.

**Independent checkpoints → coordinated checkpoints**: The legacy system
maintains per-shard read offsets with no single "ready to process" checkpoint.
This implementation produces coordinated `NextCheckpoint` deltas that
represent data available across all shards, enabling coordinated multi-shard
transactions with idempotent recovery.

## End-to-End Walkthrough

What follows is a linear trace of a document's journey through the
shuffle system, from journal to enqueued output.

### 1. Session Open

The coordinator opens a Session RPC, providing the task spec (derivation,
materialization, or collection partitions), the member topology, and a
resume checkpoint frontier. The Session:

1. Parses the task into `Binding` structs — one per transform/binding —
   capturing the shuffle key, partition selector, priority, read delay,
   UUID pointer, and schema validator.
2. Opens a Slice RPC to every member (member 0 is in-process; others are
   remote gRPC calls).
3. Sends `Opened` to the coordinator, then reads the resume checkpoint
   (streamed as chunked `FrontierChunk` messages).
4. Sends `Start` to all Slices, which triggers journal listing watches.

### 2. Journal Discovery

Each Slice watches Gazette journal listings for its assigned bindings
(round-robin by `binding.index % member_count`). When a journal appears,
the Slice sends a `ListingAdded` response to the Session.

### 3. Read Routing

The Session receives `ListingAdded` and routes the journal to a member.
This routing is designed to minimize data movement by maximizing the likelihood
that the selected member will *also* be responsible queuing a journal document.
Exact routing depends on the binding's shuffle configuration:

- **Partition-field routing**: If the shuffle key is fully covered by
  partition fields, the key hash is computed statically from the
  journal's partition labels. The hash determines a single member.
- **Source-key routing**: If shuffling on the source collection key,
  the journal's key range narrows the candidate member set.
- **Lambda routing**: If the key is computed by a lambda, all members
  are candidates.

Within the candidate set, a stable hash of `(journal_name, read_suffix)`
selects the target member. The Session constructs a `StartRead` message
containing the journal spec, binding index, and the per-journal producer
checkpoint extracted from the resume frontier, then sends it to the
target Slice.

### 4. Journal Reading

The Slice receives `StartRead`, resolves the checkpoint into per-producer
state and a start offset (minimum uncommitted begin, or maximum committed
end), and initiates a Gazette streaming read. It first probes the journal
write head to determine whether the read is already tailing (caught up).

Read data arrives as `LinesBatch` chunks from Gazette, which are
transcoded via `simd_doc::SimdParser` into archived document nodes.
For each document, the Slice extracts UUID metadata (producer, clock,
flags) and validates the document against the binding's schema.

### 5. Ready-Read Heap and Clock Gating

Parsed documents enter a priority heap (`ReadyReadHeap`) ordered by
(priority DESC, adjusted_clock ASC), where `adjusted_clock = clock +
read_delay`. The Slice defers draining the heap until all pending reads
are tailing — this ensures no yet-to-resolve read could preempt the
current heap top.

Before processing the heap top, the Slice gates on wall-clock time: if
`adjusted_clock` is in the future (due to `read_delay`), the actor
sleeps until the clock catches up. This is how read delays impose
cross-transform ordering guarantees.

### 6. Document Sequencing

The top document is sequenced against per-producer state using
`uuid::sequence()`, which classifies it as one of:

- **ContinueBeginSpan / ContinueExtendSpan**: Part of an uncommitted
  transaction. Enqueued, no flush triggered.
- **OutsideCommit**: A single-document transaction. Enqueued and
  triggers a flush.
- **AckCommit / AckCleanRollback / AckDeepRollback**: Transaction
  boundary. Not enqueued, but triggers a flush. For ACK_TXN documents,
  causal hints are extracted.
- **Duplicates**: Already-seen documents. Silently dropped.

`notBefore` / `notAfter` bounds suppress enqueue but not flush/progress.

### 7. Key Extraction and Enqueue Routing

For enqueued documents, the Slice extracts the packed shuffle key,
computes its hash, and routes to target Queue member(s) using
`route_to_members()`. For read-only derivation transforms,
`filter_r_clocks` additionally filters by the rotated clock value,
distributing reads across members in the r_clock dimension.

The document, its packed key, metadata, and journal context are sent
as an `Enqueue` message to each target Queue. Journal names are
delta-encoded across consecutive sends to minimize wire overhead.

### 8. Queue Merge and Output

Each QueueActor receives Enqueue messages from all Slices. Received
enqueues are placed in a min-heap ordered by (priority DESC,
adjusted_clock ASC). The actor pops entries one at a time, writing
documents to its on-disk queue file in globally-merged order.

Back-pressure is enforced through HTTP/2 flow control: when the Queue
can't drain fast enough, Slice sends block, which blocks journal reads,
creating system-wide priority enforcement. High-priority, earlier-clock
documents flow through first.

### 9. Flush Cycle

When the Slice observes a commit (ACK or OUTSIDE_TXN), it marks the
flush as ready. On the next event loop iteration (if no flush is already
in-flight), the Slice:

1. Builds a `Frontier` from pending producer state and accumulated
   causal hints, then drains pending into settled.
2. Sends `Flush { seq }` to all Queue members.
3. Each Queue performs its durability IO and responds `Flushed { seq }`.
4. When all Queues respond, the flush cycle completes and the frontier
   is reduced into the Slice's accumulated progress.

Flush and progress reporting are deliberately decoupled for latency
pipelining: Slices flush autonomously after each commit without waiting
for the Session. Multiple flush cycles can complete while the Session
processes the previous checkpoint.

### 10. Progress Reporting

The Session maintains one outstanding `Progress` / `Progressed` cycle
per Slice. When the Slice has flushed progress available and a Progress
request pending, it sends the accumulated frontier as chunked
`FrontierChunk` messages.

### 11. Checkpoint Pipeline

The Session's `CheckpointPipeline` is a four-stage state machine that
promotes progress through: `progressed` → `unresolved` → `ready`.

**Causal hints** gate promotion. When a producer writes to journals
spanning multiple bindings within a single transaction, the ACK document
in one journal carries hints about commits expected in other journals.
Progress stays in `unresolved` until all hinted journals confirm the
producer committed — this prevents the checkpoint from advancing past
transactions that are only partially visible.

Once all hints resolve, the frontier promotes to `ready`. When the
coordinator sends `NextCheckpoint` and `ready` is non-empty, the Session
drains it as chunked `FrontierChunk` messages.

At startup, `resume_checkpoint` may contain unresolved hints from the
previous session. The `recovery_pending` flag gates promotion until the
coordinator consumes this recovery checkpoint, so that the very first
checkpoint is exactly the hinted frontier and no more (or less).

### 12. Coordinator Receives Checkpoint

The coordinator receives `NextCheckpoint` chunks, reassembles the
frontier, and processes the transaction: dequeuing documents from Queue
files up to each producer's `last_commit` clock. After completing
downstream processing, it merges the delta into its base checkpoint and
requests the next one.

## Key Types

- `Binding` (`binding.rs`): Per-binding shuffle configuration extracted
  from the task spec. Captures key extractors, partition selectors,
  priority, read delay, and schema.

- `Frontier` / `JournalFrontier` / `ProducerFrontier` (`frontier.rs`):
  Sorted, reducible representation of per-journal, per-producer progress.
  Supports causal hint resolution, chunked encode/decode, and draining.

- `SessionClient` (`client.rs`): Client wrapper for the Session RPC,
  providing structured open/next_checkpoint/close methods.

- `Service` (`service.rs`): gRPC service implementation that spawns
  Session, Slice, and Queue actors.

## Modules

- `session/`: Session actor, checkpoint pipeline, journal routing.
- `slice/`: Slice actor, journal reading, document sequencing, key
  extraction, Enqueue routing, flush/progress state machines.
  - `listing.rs`: Gazette journal listing subscriber.
  - `producer.rs`: Per-producer state tracking and flush frontier
    construction.
  - `read.rs`: ReadState, document metadata extraction, journal probing.
  - `routing.rs`: Clock rotation and member routing.
  - `heap.rs`: Priority heap for ready reads.
- `queue/`: Queue actor, enqueue merge heap, flush IO.
- `frontier.rs`: Frontier types, reduction, causal hint resolution,
  chunked encode/decode, and drain.
- `binding.rs`: Binding configuration, partition filtering.
