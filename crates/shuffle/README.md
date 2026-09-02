# Shuffle

Shuffle coordinates reading documents from Gazette journals across distributed
task shards, routing read documents to correct shards based on a document key,
merging documents into processing-order logs hosted at each task shard,
and reporting available transactional progress back upwards to an external coordinator.

Then, once each task shard is told by the coordinator of a specific checkpoint to
process (through a messaging mechanism which is NOT part of this crate),
shards are able to efficiently identify and extract transaction-visible documents
already available in their local log, as part of processing a distributed transaction.

Shuffles serve derivation transforms, materialization bindings,
and ad-hoc collection reads.

## Architecture

The system is built from three layered gRPC RPCs, defined in
`go/protocols/shuffle/shuffle.proto`, each implemented as an async actor
and forming a hierarchy. For M shards, the system uses M Slice
streams and M² Log streams (each Slice opens one Log RPC to every shard):

```
Coordinator (external caller, typically runs on shard-000)
   |
   ▼
  Session (one per task)
     |
     ├─▶ Slice 0 ─┬─▶  Log 0  (in-process)
     │            ├─▶  Log 1  (remote)
     │            └─▶  Log 2  (remote)
     ├─▶ Slice 1 ─┬─▶  Log 0  (remote)
     │            ├─▶  Log 1  (in-process)
     │            └─▶  Log 2  (remote)
     └─▶ Slice 2 ─┬─▶  Log 0  (remote)
                  ├─▶  Log 1  (remote)
                  └─▶  Log 2  (in-process)
```

**Session** (`session/`): Top-level coordinator-facing RPC. Opened by the
external coordinator (e.g. shard-000 of a derivation or materialization).
Manages the session lifecycle, routes discovered journals to Slices,
and aggregates progress into checkpoints.

**Slice** (`slice/`): Per-shard RPC opened by the Session. Each Slice
watches journal listings for its assigned bindings, reads documents from
journals, sequences them, validates them, extracts shuffle keys, and routes
documents to the appropriate Log RPC(s) based on key hash.

**Log** (`log/`): Per-shard RPC opened by each Slice. All Slices
targeting the same shard join into a single LogActor, which merges
documents across Slices in (priority DESC, adjusted_clock ASC) order
and writes them to local on-disk storage.

Once started, the distributed shuffle runs continuously to read journals,
transcode documents, map them to shards, and write them into on-disk log segments.
At the same time progress frontiers are reported upwards and aggregated at the
Session, which seeks to maintain a frequently-updated checkpoint of progress
available right now.

The Coordinator, an external application using a shuffled read, will then choose
its own cadence for polling the Session to fetch the next ready checkpoint.
It distributes the checkpoint amongst its shard workers, and each consumes
from its local log for downstream processing and reclaims space.

The recovery model is fail-fast: if a terminal error occurs with a component of
any shard, the entire topology is torn down and all logs are discarded,
to be rebuilt anew on a next Session.

### Shutdown

The topology tears down through a single path: the coordinator drops (EOFs) its
Session request stream, the Session propagates this to its Slices by closing
their request streams, and each Slice in turn to its Logs. Each actor observes
the EOF, then drains its downstream peers' EOFs before exiting.

This interacts subtly with disk back-pressure. When a Log engages back-pressure
(§8), it stops draining a Slice's `Append`s, which parks that Slice's request
stream — so the Log no longer polls it and cannot observe its EOF. A Slice whose
`Append` is parked at a back-pressured Log therefore can't drain that Log to EOF,
and shutdown wedges from the Log upward. Back-pressure normally releases only as
the downstream coordinator consumes the local log and reclaims segments — which
stops happening once the coordinator is shutting down.

A coordinator breaks this by relieving the back-pressure out-of-band: it removes
each shard's log segment files (`remove_shard_segments`). The co-located Log's
sealed-segment reclaim observes the unlinks, drops `disk_backlog_bytes` below
threshold, and releases back-pressure — so the parked Slice streams re-arm, reach
EOF, and the whole topology drains. Because this discards any log not yet
consumed, a coordinator does it only once it will request no further checkpoints,
then calls `SessionClient::close()` (which blocks until the Session→Slice→Log
topology has fully drained).

#### Teardown during the opening phase

The opening phase participates in the same EOF cascade. Each handler opens its
downstream peers and races that fan-out against its own request stream. It
short-circuits if any downstream open errors — dropping the sibling open futures,
which drops their request channels and cascades EOF down to those peers and
their peers in turn. Racing the request stream catches the *upstream* going
away mid-open.

The Log rendezvous is the subtle case. `M` Slices connect to each Log shard; the
invocation that connects last runs the `LogActor`, while the earlier `M-1` park.
A parked invocation must remain cancel-observant, so it selects its own
completion signal against `response_tx.closed()` (fired when its Slice
client drops its receiver) and reaps *only its own* slot under the `log_joins`
mutex (dropping the whole entry when it removed the last live slot),
so a stale partial rendezvous can't wedge the map or poison
a retry.

#### No deadlines, by design

There are deliberately **no** open-phase deadlines. Every transient failure
surfaces as an error that self-heals via `try_join_all` and the Go retry loop,
so a timeout would add flapping that would only paper over a true regression,
and makes the ensemble more difficult to debug (a stable wedge is far easier
to inspect).

### Authorization

When the `Service` is built with a `proto_grpc::Signer` (the sidecar; `None` in
`flowctl preview`), every remote shuffle hop carries a self-signed `SHUFFLE`
bearer scoped to the task-creation shard-id prefix. Peer sidecars verify it
for AuthN and AuthZ scoping to the requested task topology.

### Concepts

**Shard Topology**: A session has N **shards**, each owning a disjoint range of the 2D
(key_hash, r_clock) space. Shards tile the full `[0, 0xFFFFFFFF]` range
in both dimensions. Each shard runs one Slice RPC actor and one Log RPC actor.

**Span**: A single run of CONTINUE_TXN documents by one producer in one journal,
without an interleaving ACK. The producer's first CONTINUE_TXN *opens* a span and
further CONTINUEs extend it. It *closes* either by a committing ACK_TXN /
OUTSIDE_TXN — a **committing close**, of which an empty ACK is the degenerate case,
closing an empty span — or by a rollback, which closes by **discarding**. At most
one span is open per producer at any offset. Sequencing lives in `slice/producer.rs`.

**Cut**: The offset a journal was read through to produce a Frontier. It's never
represented explicitly, and is bounded from below only by the **cut floor**
`M = max|offset|` across the journal's producer entries.

**Open / Closed**: A producer's state at a cut: *open* if it has an open span at
the cut, else *closed*. The `offset` sign encoding of `frontier.rs` is a direct
readout — non-negative (`+begin` of the open span) ⇔ open; negative (the negated
end of the last committing close) ⇔ closed.

**Covered by**: The relation between a committing close — or any re-read
document — and a `last_commit` at-or-above its clock. A close covered by
`last_commit` is one already accounted for downstream, and a re-read covered by
`last_commit` sequences as a duplicate. Rollback closes need no coverage: a
re-read rollback re-discards, owing nothing downstream.

**Accounts for**: The umbrella over both arms of the Frontier invariant — an
entry accounts for its producer's history via covered committing closes plus a
located open span.

**Gapped**: A producer recovered *open* whose span's `+begin` lies below the
offset its journal read resumed from. The skipped range `[begin, resume)` is
recovered by a single bounded replay, triggered lazily by the producer's next
document (`slice/replay.rs`).

**Cumulative vs delta Frontier**: A *delta* carries only the journals and
producers which progressed since the last checkpoint. A *cumulative* Frontier
holds the complete reduction of every delta since genesis, leaving out nothing
which progressed. Durable resume checkpoints are cumulative, emitted `ready`
frontiers and peeks are deltas.

**The Frontier invariant**: A cumulative Frontier's entries always describe one
continuous read through its cut floor and account for every committing close and
open span below it — which is what makes it sound to resume from. Canonically
stated on `Frontier` in `frontier.rs`.

**Causal Hints**: ACKs are documents written to journals by a producer, and contain
a clock which closes that producer's open span of preceding lesser-clock documents,
in that same journal — a committing close or a rollback. An ACK closes nothing
in _other_ journals. However, producers frequently write multi-journal transactions,
and ACKs can contain "causal hints" that tell a reader that the ACK correlates with
related ACKs in specific journals. To support end-to-end multi journal transactions,
this implementation delays checkpoint visibility until the correlated committing
closes across read journals of the same cohort have all been read through.

**Cohorts**: Journals having the same priority and read-delay are grouped together
into cohorts, which is the unit of transaction visibility coordination: a hint's
correlated committing closes are only tracked within a cohort, allowing different
cohorts to make progress independently. For example, a binding read with an
explicit delay cannot gate a binding read in real time.

#### Terms to avoid

- *claim / claimed* — invokes confusing agency (claimed by whom?). Say "covered
  by", or a plain verb like "reported" or "took".
- *subsume / subsumable* — the same confusion. Say "covered by".
- *settle / settled / settling* — was used for both span closure and flush
  accounting. Say "close" / "closing document" for spans, "reported" for flush
  accounting.

## Comparison with legacy shuffle implementation (`go/shuffle/`)

The legacy shuffle implementation has several limitations that motivated
this crate:

**Optimistic replay → lazy gapped replay**: The legacy system reads
optimistically from the latest journal offset, so when an uncommitted
transaction later commits it must perform bounded "replay reads" to re-read
that data — a routine, high-latency part of steady-state reading. This
implementation reads forward and stages open spans into the log inline,
so steady-state reading never replays. Replay is confined to restart recovery:
a `(binding, journal)` read resumes from its checkpoint's cut floor
(`M = max|offset|` across producer entries), and a producer whose open span
begins before `M` is *gapped* — its skipped range is recovered by a single
bounded historical replay, triggered lazily by the producer's first newer
document (see `slice/replay.rs`).

**Per-shard RPCs → shared streams**: The legacy system starts an RPC per
(shard, journal) pair, which doesn't scale: at M=10 shards with N=100k
journals, that's up to M×N = 1M concurrent RPCs, and each ACK is broadcast
to every shard. This implementation uses M + M² streams total (M Slice RPCs + M²
Log RPCs), independent of journal count. At M=10 with N=100k, that's 110 streams
instead of 1M. Listing watches are also distributed across shards (each
watches ~B/M bindings) rather than duplicated on every shard.

**In-memory staging → disk-backed logs**: The legacy system holds shuffled
documents in memory buffers, limiting how far reads can progress ahead of
downstream processing. This implementation writes to on-disk log files,
allowing reads to run well ahead without memory pressure.

**Independent checkpoints → coordinated checkpoints**: The legacy system
maintains per-shard read offsets with no single "ready to process" checkpoint.
This implementation produces coordinated `NextCheckpoint` deltas that
represent data available across all shards, enabling coordinated multi-shard
transactions with idempotent recovery.

## Linear Walkthrough

What follows is a trace of a document's journey through the
shuffle system, from reading journals to appending to local shard log,
and back upwards through progress reporting.

### 1. Session Open

The coordinator opens a Session RPC, providing the task spec (derivation,
materialization, or collection partitions), the shard topology, and a
resume checkpoint frontier. The Session:

1. Parses the task into `Binding` structs — one per transform/binding —
   capturing the shuffle key, partition selector, priority, and read delay,
   plus `Source` structs — one per distinct source collection, following the
   spec's declared indirection. Schema validators and journal clients are built
   per Source, so bindings fanning in on one collection share them.
2. Opens a Slice RPC to every shard (shard 0 is in-process; others are
   remote gRPC calls).
3. Sends `Opened` to the coordinator, then reads the resume checkpoint
   `Frontier`.
4. Sends `Start` to all Slices, which triggers journal listing watches.

### 2. Journal Discovery

Each Slice watches Gazette journal listings for its assigned bindings
(round-robin by `binding.index % shard_count`). The Slice sends a
`ListingAdded` for each journal. The Slice sends one `ListingSnapshotComplete`
when a binding's initial snapshot is fully delivered, empty snapshots included.

### 3. Read Routing

The Session receives `ListingAdded` and routes the journal to a shard.
This routing is designed to minimize data movement by maximizing the likelihood
that the selected shard will *also* be responsible for storing the document in its local log.
Exact routing depends on the binding's shuffle configuration:

- **Partition-field routing**: If the shuffle key is fully covered by
  partition fields, the key hash is computed statically from the
  journal's partition labels. The hash determines a single shard.
- **Source-key routing**: If shuffling on the source collection key,
  the journal's key range narrows the candidate shard set.
- **Lambda routing**: If the key is computed by a lambda, all shards
  are candidates.

Within the candidate set, a stable hash of `(journal_name, read_suffix)`
selects the target shard. The Session constructs a `StartRead` message
containing the journal spec, binding index, and the per-journal producer
checkpoint extracted from the resume frontier, then sends it to the
target Slice.

Only one Slice lists each binding. When the Session has received a
`ListingSnapshotComplete` for every binding, it puts an `InitialReadsStarted` on
each Slice's `StartRead` queue. It therefore follows every initial `StartRead`,
and it opens the heap-drain gate.

### 4. Journal Reading

The Slice receives `StartRead`, resolves the checkpoint into per-producer
state and a start offset (the checkpoint's cut floor, `M = max|offset|` across
producer entries), and initiates a Gazette streaming read. A recovered producer
whose open span begins before `M` is marked *gapped* and its skipped range is
recovered later by a bounded replay (see `slice/replay.rs`).
It first probes the journal write head to determine whether the read is already
tailing (caught up).

Read data arrives as `LinesBatch` chunks from Gazette, which are
transcoded via `simd_doc::SimdParser` into archived document nodes.
For each document, the Slice extracts UUID metadata (producer, clock,
flags) and validates the document against the binding's schema.

### 5. Ready-Read Heap and Clock Gating

Parsed documents enter a priority heap (`ReadyReadHeap`) ordered by
(priority DESC, adjusted_clock ASC), where `adjusted_clock = clock +
read_delay`. The Slice does not drain until the Session's `InitialReadsStarted`
arrives, all pending reads are tailing, and no read still probes its write head.
No unstarted read and no unresolved read can then preempt the heap top.

A single non-tailing read therefore head-of-line-blocks the whole Slice's
drain, so I/O stalls on individual journals matter. A read is only
(re-)parked into `pending_reads` after a now-or-never poll fails to yield
its next batch (`park_or_process`); a read with content already buffered is
processed immediately rather than counted as blocked. Reads that genuinely
park while non-tailing are tracked in `stalled` and surfaced — by transition
— on the `stall` event track and the `shuffle_slice_stalled_reads` gauge, so
an operator can sample *which* journals are blocking and for how long.

Before processing the heap top, the Slice gates on wall-clock time: if
`adjusted_clock` is in the future (due to `read_delay`), the actor
sleeps until the clock catches up. This is how read delays impose
cross-transform ordering guarantees.

Each dequeue also records the document's clock on the
`shuffle_slice_last_source_published_at_time_seconds` gauge, labeled by
cohort: how far this shard's shuffled read has progressed in source
published-at time. `time() - gauge` is the shard's read lag, and max - min
across sibling shards is their skew — the measure of how tightly remapped
routing (§7) actually couples shards. Clocks are only comparable within a
cohort: bindings of differing priority or read delay legitimately diverge.

### 6. Document Sequencing

The top document is sequenced against per-producer state using
`uuid::sequence()`, which classifies it as one of:

- **ContinueBeginSpan / ContinueExtendSpan**: Opens or extends the producer's
  open span. Appended to a log, and no flush cycle is triggered.
- **OutsideCommit**: A single-document transaction — a committing close of an
  otherwise-empty span. Appended to a log and triggers a flush.
- **AckCommit / AckCleanRollback / AckDeepRollback**: Closes the producer's span:
  AckCommit is a committing close, while the rollbacks close by discarding.
  Not appended to a log, but triggers a flush.
  For ACK_TXN documents, causal hints are extracted.
- **Duplicates**: Already-seen documents, covered by the producer's `last_commit`.
  Silently dropped.

`notBefore` / `notAfter` bounds suppress log appends but not flush cycles and progress reporting.

### 7. Key Extraction and Append Routing

For Appended documents, the Slice extracts the packed shuffle key,
computes its hash, and routes to target Log shard(s) using
`route_to_shards()`. For read-only derivation transforms,
`filter_r_clocks` additionally filters by the rotated clock value,
distributing reads across shards in the r_clock dimension.

Key routing is deliberately "mostly solid": within `route_to_shards()`, a
small fixed fraction of keys (`routing::REMAP_THRESHOLD / REMAP_BUCKETS`,
selected by the key hash's low bits) route by a remapped hash — the hash's
16-bit halves swapped — landing them across the key space rather than in
their own range. This puts a
trickle of every Slice's documents into every Log, so a lagging Log's
clock-ordered merge (§8) back-pressures a Slice that has raced ahead,
bounding how far shards' wall-clocks can diverge within a transaction. The
remap is a pure function of the key hash, so every Slice agrees on it, and
it is internal to routing: the key hash that connectors and log entries
observe is always the true hash of the packed key.

The document, its packed key, metadata, and journal context are sent
as an `Append` message to each target Log. Journal names are
delta-encoded across consecutive sends to minimize wire overhead.

### 8. Log Merge and Output

Each LogActor receives Append messages from all Slices. Received
appends are placed in a min-heap ordered by (priority DESC,
adjusted_clock ASC). The actor pops entries one at a time, writing
documents to its on-disk log files in a globally-merged order.

Back-pressure is enforced through HTTP/2 flow control: when the Log actor
can't drain fast enough, Slice sends block, which blocks journal reads,
creating system-wide priority enforcement. High-priority, earlier-clock
documents flow through first.

### 9. Flush Cycle

When the Slice observes a commit (ACK or OUTSIDE_TXN), it marks the
flush as ready. On the next event loop iteration (if no flush is already
in-flight), the Slice:

1. Builds a `Frontier` from unreported producer state and accumulated
   causal hints, then drains `unreported` into `reported`.
2. Sends `Flush { cycle }` to all Log shards.
3. Each Log performs its durability IO and responds `Flushed { cycle }`.
4. When all Logs respond, the flush cycle completes and the frontier
   is reduced into the Slice's accumulated progress.

Flush and progress reporting are deliberately decoupled for latency
pipelining: Slices flush autonomously after each commit without waiting
for the Session. Multiple flush cycles can complete while the Session
processes the previous checkpoint.

### 10. Progress Reporting

The Session maintains one outstanding `Progress` / `Progressed` cycle
per Slice. When the Slice has flushed progress available and a Progress
request pending, it sends the accumulated frontier as a `Frontier`.

### 11. Checkpoint Pipeline

The Session's `CheckpointPipeline` is a four-stage state machine that
promotes progress through: `progressed` → `unresolved` → `ready`.

**Causal hints** gate promotion. When a producer writes to journals
spanning multiple bindings within a single transaction, the ACK document
in one journal carries hints about commits expected in other journals.
Progress stays in `unresolved` until all hinted journals confirm the
producer committed — this prevents the checkpoint from advancing past
transactions that are only partially visible.

Unaccounted `progressed` is held back behind `unresolved` so that newer
progress — which may itself add fresh hints — can't indefinitely starve
`unresolved` from fully resolving. Sequencing guarantees forward progress.

Once all hints resolve, the frontier promotes to `ready`. When the
coordinator sends `NextCheckpoint` and `ready` is non-empty, the Session
sends it as a single `Frontier` message.

Hints can be cyclic: `unresolved` may resolve hints carried by `progressed`.
Similarly, a re-enabled binding can re-visit hints which have long since
been resolved. `progressed` is therefore pruned (`Frontier::prune_hints`)
at promotion against `frontier::Completed` — the pipeline's accounting of
what is already done, written only on promotion to `ready`. It holds a
per-cohort ledger of producer commits, plus a per-binding maximum of
promoted progress which deems any clock at least
`PRODUCER_STALENESS_HORIZON` older to be completed. That horizon rule is the
dual of `runtime-next`'s committed-frontier prune, which forgets producers by
the same constant: it is what keeps a hint naming a forgotten producer
dischargeable at all.

#### Accounted-progress ratchet

While a checkpoint is held unresolved, each incoming delta is judged
(`Frontier::first_unaccounted`) against the boundary that checkpoint
defines. A delta is **accounted** when every producer commit and causal
hint it reports is already covered by one of the three authorities: the
pending frontier's own clocks and hints, a commit the producer's cohort
has completed, or the staleness horizon. Accounted progress folds into the
unresolved boundary, adopting the read's true offsets; unaccounted progress
is held back for the next boundary.

#### Idempotent recovery is a session of its own

At startup, `resume_checkpoint` may contain unresolved hints from the
previous session: a transaction it prepared but never durably committed,
which must be replayed exactly. Such a session does that replay and
nothing else. Three mechanisms, each self-gating on the resume
checkpoint, make it so:

- The Session starts reads only for `(journal, binding)` pairs carrying
  an unresolved hint (`Topology::build_start_read` returns None otherwise,
  gated on `resume_checkpoint.unresolved_hints`). Every other journal —
  including one newly listed mid-recovery — is the next session's work.
- Each such read parks the moment its own hints resolve
  (`slice::read::Park`): it drops its in-hand batch and its journal read
  stream at the resolving committing close, so no read tails on into post-crash
  content. Its `ReadState` survives, so the resolving flush delta still
  reaches the Session.
- `CheckpointPipeline::recovery_session` is sticky: newer `progressed`
  is never promoted, so the one checkpoint this session emits is exactly
  the hinted frontier, no more and no less, and the Session then
  quiesces.

The coordinator (`runtime-next`'s materialize leader) commits that
checkpoint and exits — deferring the transaction's own post-commit work
to the next session, which resumes it exactly as any crash restart
would. That restart is what bounds the replay's disk: the one unbounded
term, tailing past the hinted frontier while a lower-priority cohort
still resolves, is gone, so the replay fits within the same
`estuary.dev/shuffle-disk-limit` the crashed session ran under.

Recovery is the ratchet's primary use case. Without it, the one-shot
recovery session would discard every true replay offset in `progressed`,
leaving its checkpoint at the old cut floor so that the next session
re-reads the whole hinted span. Recovery has just one unresolved
generation, so its first unaccounted delta freezes the ratchet for the
rest of the session; conservative hint resolution then lands at the
ratcheted floor, and the next session re-reads only the novel tail.

#### Peeks of partial progress

In the recovery case, `unresolved` can carry hints whose resolution
requires reading tens of GB before `ready` becomes available. To avoid
keeping the coordinator idle (and log segments unscannable) during that
window, `take_ready` may emit a *peek* of `unresolved` instead: a
`Frontier` carrying `unresolved_hints == true` and zeroed byte deltas.
A peek is emitted only when `unresolved` has made progress — any
producer's `last_commit` advancing — since the last emission.

The same "did `unresolved` make progress?" signal disarms the `on_tick`
stall timeout: it fires only when no progress at all occurs between
two consecutive ticks, and only while a coordinator request is
outstanding — with nobody waiting on a checkpoint, zero progress is
routine.

A peek also carries `latest_backfill_begin` eagerly (cloned from
`unresolved`, which retains it for the eventual resolved `ready`), as
scan-classification metadata: a downstream materialization must observe a
backfill-truncation boundary before it scans any source or Loaded document
at or above that boundary's clock, so documents on opposite sides of the
boundary are never combined. The begin clock only becomes durable
checkpoint state once its causal hints resolve and it rides a fully-resolved
`ready`. `latest_backfill_complete` is surfaced the same way — eagerly on a peek
and durably on a resolved `ready` — but plays no part in classification.

### 12. Coordinator Receives Checkpoint

The coordinator receives `NextCheckpoint` chunks and reassembles a
`Frontier`. It may process the frontier (e.g. log scanning up to each
producer's `last_commit`) regardless of `unresolved_hints`. But for a
**transactional boundary** the coordinator must keep calling
`next_checkpoint()` until it receives a `Frontier` with
`unresolved_hints == false` — only then has the pipeline produced a
fully-resolved checkpoint.

After completing downstream processing on a fully-resolved frontier, the
coordinator merges the delta into its base checkpoint and requests the
next one.

## Key Types

- `Binding` / `Source` (`binding.rs`): Shuffle configuration extracted from
  the task spec. `Binding` captures per-binding key extractors, partition
  selectors, priority, and read delay; `Source` captures the per-collection
  schema, UUID pointer, and partition metadata.

- `Frontier` / `JournalFrontier` / `ProducerFrontier` (`frontier.rs`):
  Sorted, reducible representation of per-journal, per-producer progress.
  Supports causal hint resolution, chunked encode/decode, and draining.
  `Frontier` carries the canonical statement of the Frontier invariant.

- `SessionClient` (`client.rs`): Client wrapper for the Session RPC,
  providing structured open/next_checkpoint/close methods.

- `Service` (`service.rs`): gRPC service implementation that spawns
  Session, Slice, and Log actors.

## Modules

- `session/`: Session actor, checkpoint pipeline, journal routing.
- `slice/`: Slice actor, journal reading, document sequencing, key
  extraction, Append routing, flush/progress state machines.
  - `listing.rs`: Gazette journal listing subscriber.
  - `producer.rs`: Per-producer state tracking and flush frontier
    construction.
  - `read.rs`: ReadState (including its hinted-frontier park), document
    metadata extraction, journal probing.
  - `replay.rs`: Bounded historical replay of a gapped producer's span on
    restart recovery.
  - `routing.rs`: Clock rotation and shard routing.
  - `heap.rs`: Priority heap for ready reads.
- `log/`: Log actor, append merge heap, flush IO.
  - `log/block/`: Zero-copy types for working with segmented log blocks.
- `frontier.rs`: Frontier types, reduction, causal hint resolution,
  chunked encode/decode, and drain.
- `binding.rs`: Binding and Source configuration, partition filtering.
