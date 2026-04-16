## Shuffle V2 Truncation Design

This document is a shuffle-v2 extension of the higher-level truncation design
in `truncation.md`.

It describes the checkpoint metadata and filtering model for a shuffle-backed
coordinator which consumes shuffled documents from local on-disk logs after
receiving `NextCheckpoint`.

For the present implementation work, the target is `flowctl raw shuffle` as a
prototype coordinator. Integration into the actual runtime v2 coordinator is
still TBD and is out of scope here.

The central idea is:

- extend `NextCheckpoint` to carry checkpoint-level per-binding
  `latest_backfill_begin` metadata
- use that metadata while dequeuing committed source documents from each
  member's local shuffle log
- suppress stale pre-boundary source documents while preparing input for
  `doc::combine`
- add a stale-`Loaded` combiner rule so `Store.exists=true` is propagated

This places source-document truncation in the coordinator's dequeue path and
defines a narrow combiner rule for stale `Loaded` rows.

### Runtime Shape

The new shuffle crate now has the pieces needed for this design:

- `Session` returns `NextCheckpoint` frontier deltas to the coordinator.
- `Slice` journal readers parse backfill control messages while reading source
  journals and fold them into checkpoint metadata.
- `Log` writes shuffled source documents to per-member on-disk log files.
- backfill control messages are not appended to those local log files
- the coordinator reads those local log files out-of-band after receiving a
  checkpoint
- `flowctl raw shuffle` is already a prototype coordinator which:
  - requests `NextCheckpoint`
  - creates a `log::reader::FrontierScan` for each member
  - iterates committed log entries
  - feeds them into `doc::combine`

Source-document truncation happens during coordinator dequeue from local
shuffle logs, after the checkpoint metadata is known.

### Extend `NextCheckpoint`

`NextCheckpoint` should be extended to carry per-binding latest backfill begin
metadata for the checkpoint delta.

That metadata is derived by the shuffle session while it reads collection
journals. Control messages are parsed at journal-read time and summarized into
the checkpoint, rather than being forwarded into local shuffle log files for
the coordinator to read back later.

Today, the terminal `FrontierChunk` of a `NextCheckpoint` carries only:

- `flushed_lsn`, indexed by member

Under this design, the terminal `FrontierChunk` would additionally carry:

- `latest_backfill_begin`, keyed by binding
- `latest_backfill_complete`, keyed by binding

This terminal placement works because the coordinator already has to collect
the full `NextCheckpoint` response before it can start reading any local log
files. `FrontierScan` requires the checkpoint frontier together with the
terminal `flushed_lsn`, so the coordinator cannot begin scanning after
receiving only the non-terminal journal chunks. `latest_backfill_begin` and
`latest_backfill_complete` arrive in that same terminal chunk and are available
before any source document dequeue begins.

Example shape:

```proto
message BackfillBegin {
  uint32 binding = 1;
  fixed64 clock = 2;
}

message BackfillComplete {
  uint32 binding = 1;
  fixed64 clock = 2;
}

message FrontierChunk {
  repeated JournalFrontier journals = 1;
  repeated uint64 flushed_lsn = 2; // terminal chunk only
  repeated BackfillBegin latest_backfill_begin = 3; // terminal chunk only
  repeated BackfillComplete latest_backfill_complete = 4; // terminal chunk only
}
```

This metadata should ride with the checkpoint as whole-delta metadata, not in
per-journal frontier entries:

- `flushed_lsn` is already whole-checkpoint metadata
- `latest_backfill_begin[binding]` affects every journal of that binding
- `latest_backfill_complete[binding]` is connector-signaling state for that
  binding at checkpoint scope
- the coordinator wants one effective begin watermark per binding while
  dequeueing, not one watermark per journal row

Within one checkpoint delta, successive backfills for the same binding are
collapsed to the latest clock. Older begins in that same checkpoint are
subsumed by the latest one. Older completes in that same checkpoint are also
subsumed by the latest one.

The concrete runtime v2 coordinator implementation is still TBD and is out of
scope for the current work. This document only describes the checkpoint
metadata and filtering model that such a coordinator will need to implement.

### Coordinator State

The one durable coordinator requirement called for by this design is:

- `last_backfill_begin: Option<Clock>`

This value must be persisted per binding because source-document filtering in
later transactions depends on the most recently observed backfill-begin
boundary, even across restarts. `backfillComplete` does not clear that
boundary.

The coordinator will also need ordinary transient state while processing a
checkpoint, but the exact in-memory shape of that state is left unspecified
here.

### Processing One `NextCheckpoint`

For one checkpoint:

1. Request `NextCheckpoint`.
2. Read chunks until the terminal empty-`journals` chunk arrives.
3. Reassemble the frontier and terminal metadata, including `flushed_lsn`,
   `latest_backfill_begin`, and `latest_backfill_complete`.
4. Compute, for each binding:

```text
effective_begin = max(persisted last_backfill_begin, checkpoint.latest_backfill_begin[binding])
```

5. For each member, build a `FrontierScan` using the checkpoint frontier and
   that member's `Reader`.
6. Iterate committed log entries from the scan.
7. For each source document entry:
   - look at `entry.meta.binding`
   - look at `entry.meta.clock`
   - if `clock < effective_begin[binding]`, suppress it
   - otherwise, add it to the ordinary load/combine/store flow

The coordinator reads exactly the committed, transaction-visible source
documents for that checkpoint, and drops stale pre-boundary rows before they
ever reach the combiner.

`latest_backfill_complete` is not used in this filtering step.

### Log-Reader Filtering

The shuffle log already exposes the information needed to do this cheaply:

- `FrontierScan` uses the checkpoint frontier and `flushed_lsn` to read only
  visible log content
- each log entry already carries `binding`, `clock`, and `flags` in block
  metadata for ordinary source documents
- the runtime does not need to parse UUIDs back out of source document bodies
  just to compare clocks against a backfill boundary

Backfill control messages are handled earlier, by the journal readers that
construct the checkpoint metadata, and are not replayed from the local log.

So the hot decision becomes:

```text
if entry.meta.clock < effective_begin[entry.meta.binding] {
    suppress
} else {
    forward
}
```

That is exactly the kind of cheap per-entry filter that belongs here.

### Loaded Rows

Loaded rows follow a separate path from source rows.

They do not come from shuffle logs, so their UUID clock is inspected when
`Response::Loaded` is received.

For a loaded row:

- if there is no `effective_begin`, add it normally as `front=true`
- if `loaded_clock >= effective_begin`, add it normally as `front=true`
- if `loaded_clock < effective_begin`, add it as
  `front=true, stale_loaded=true`

This preserves destination existence for same-key post-backfill source rows.

### Combiner Handling

The combiner handles stale Loaded rows with one extra metadata bit.

Add one `Meta` bit such as `stale_loaded` and apply these same-key rules:

- ordinary source rows and ordinary Loaded rows leave the bit unset
- stale Loaded rows set `stale_loaded=true`
- entries with equal `stale_loaded` values use existing reduction behavior
- an ordinary row supersedes a stale Loaded row of the same key
- `front=true` is carried onto the surviving ordinary row
- payloads are never reduced across the stale-to-ordinary boundary
- a stale Loaded row that reaches drain without a same-key ordinary row is
  discarded instead of emitted

### Connector Signaling

Logically, each materialization transaction has the form:

1. zero or more `backfillBegin` notifications
2. regular filtered data
3. zero or more `backfillComplete` notifications

`backfillComplete` has no effect on source filtering. It matters only for
connector signaling and UX.

It is reasonable for the coordinator to derive:

- begin notifications from checkpoint `latest_backfill_begin`
- complete notifications from checkpoint `latest_backfill_complete`

Whether those notifications ride on `Flush`, `Acknowledge`, or some other
inter-transaction message is orthogonal to this source-filtering design.

### Recovery

The concrete recovery behavior of the runtime v2 coordinator is also TBD and is
out of scope for the current work.

The durable requirement from this design is:

- persist the finished checkpoint frontier
- persist the updated `last_backfill_begin` per binding
- do not clear the persisted begin on `backfillComplete`

On restart:

- resume shuffle from the persisted checkpoint
- restore the persisted `last_backfill_begin`
- continue filtering dequeued source rows and Loaded rows against that
  persisted boundary until a newer begin is reported

Whether the runtime also persists any in-flight checkpoint metadata for
idempotent recovery is an implementation choice and is not specified here.

### Optional Checkpoint Merging

One straightforward implementation is one shuffle `NextCheckpoint` to one
materialization transaction.

If the runtime later wants to merge multiple checkpoints into one larger
materialization transaction, then a checkpoint carrying a newer
`latest_backfill_begin` for any binding becomes a merge barrier:

- it must not be merged behind already-open regular data for that binding

Example:

- checkpoint `CP1` contains ordinary source documents for binding `B`
- checkpoint `CP2` carries `latest_backfill_begin[B] = T2`

If `CP1` and `CP2` are merged into one downstream transaction, then the
effective begin for binding `B` across that merged transaction is `T2`. Any
source document of `B` with `clock < T2` must be suppressed, including source
documents that came from `CP1`.

That means the coordinator cannot safely start reading and forwarding ordinary
data for `CP1` on binding `B`, and then later merge in `CP2` after discovering
the newer begin. Doing so would admit rows that should have been filtered out.

So a newer `latest_backfill_begin` can only be merged if the coordinator has
not yet opened regular data for that binding, or if it is using a different
batching strategy that buffers enough state to apply the merged effective begin
before forwarding rows.

That batching concern is secondary to the main design and can be deferred.

### Key Properties

- source truncation happens during coordinator dequeue from shuffle logs
- source-doc clock comparison is cheap because shuffle log metadata already
  carries binding and clock
- backfill control messages are parsed by journal readers and summarized into
  `NextCheckpoint` instead of being written to local shuffle logs
- `HeapEntry` stays unchanged for source rows
- this fits the v2 coordinator path already prototyped
  by `flowctl raw shuffle`
- `NextCheckpoint` must be extended with checkpoint-level backfill metadata
- the coordinator persists `last_backfill_begin` across finished checkpoints
- Loaded rows use their own clock-handling path
- the combiner applies the narrow `stale_loaded` rule

This design filters source documents at the point where the coordinator
dequeues committed shuffle-log entries through `FrontierScan`.
