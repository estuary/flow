# Collection Truncation and Inferred Deletions

## Problem Statement

When a capture connector performs a full refresh (re-queries all data from the
source), the materialization needs to know which rows in the destination are
stale — they existed before the refresh but were not re-captured, meaning
they've been deleted from the source. Currently, Flow has no mechanism to
communicate this, and materializations accumulate stale rows.

## Preamble: Intended Use

The immediate use case for this is for capture bindings which _exclusively_ use
a full refresh strategy, on some regular frequency to capture updates and
insertions to the source.

As described in later sections, the proposed solution will apply labeling to
collection journals to allow new readers to skip ahead of stale data prior to a
refresh. This will be universally supported for materializations and derivations
in the runtime, and not require connector-specific implementation.

Not all materializations or derivations will support actually deleting stale
data in the destination via the proposed mechanism, especially not at first. For
a strictly full refresh kind of capture, this is fine: A reader that skips ahead
to the start of the next backfill while reading from such a collection will by
definition get the complete set of data eventually. It might leave stale rows
from the prior backfill in the destination if the materialization hasn't been /
can't be updated to support truncation signals, but it should hopefully have
fewer than it would have prior to this solution being implemented, since stale
rows from older backfills are avoided entirely by skipping ahead to the freshest
restatement. It can't possibly have more.

This is arguably not the case for CDC-style captures, which emit deletion events
as well. Skipping ahead to the start of the next backfill can miss deletion
events, which means materializations that don't support truncations could
conceivably end up with _more_ stale rows than before. However, this isn't
entirely a new problem: presently, a CDC capture that has a backfill triggered
will "jump ahead" of reading its event log and might skip deletions on its own.

All of this is to say that initially, capture connectors should strive to
implement this solution for full refresh bindings. Good examples are batch SQL
captures, and many SaaS captures that don't have an incremental mode.
Implementations here will provide an immediate, unambiguous benefit. Future work
may also include CDC captures responding to source "truncate table" events, and
capture connectors in general having backfill counters incremented (user or
control-plane initiated backfills).

## Design Overview

**Control messages** bracket a full refresh of a capture binding. These are
written to collection journals alongside regular documents, but are detected and
handled separately by the runtime using a new UUID `CONTROL` flag (see UUID Flag
Extension below). Control messages are always immediately-committed
(`OUTSIDE_TXN` sequencing); they never participate in a `CONTINUE_TXN` span.

A `BackfillBegin` control message's **`truncatedAt`** timestamp divides journal
data into "before" (stale, to be superseded) and "after" (current).
Materialization connectors use `truncatedAt` to delete stale destination rows.

When a `BackfillBegin` is durably committed in capture state, the collection's
**journal labels** are also updated with `truncatedAt` as a truncation
timestamp. This allows new readers (e.g., a newly-created materialization) to
skip past stale pre-`truncatedAt` data entirely. `truncatedAt` reuses the
existing `begin_mod_time` mechanism, analogous to how `not_before` is used.

This initial design primarily covers support for captures & materializations.
Some changes to Dekaf will be necessary to handle these new control messages as
Kafka control records, as well as threading through the `truncatedAt` journal
label as the effective `not_before` for reads. Derivations will effectively
ignore the new control messages for now, but will respect the `truncatedAt`
journal labels to set their `not_before` for reads, via a common shuffle-layer
path.

## Conceptual Control Message Flow

### When a capture starts a new backfill:
- **Capture Connector**: Emits `BackfillBegin` message for one or more bindings
  as the **first message(s) for those bindings in the transaction**, followed by
  documents and a checkpoint. The runtime enforces this ordering — a
  `BackfillBegin` received after documents for the same binding in a transaction
  is an error.
- **Capture Runtime**:
  - Treats any connector checkpoint containing a `BackfillBegin` or
    `BackfillComplete` as a hard batching boundary. Connector checkpoints are
    never combined across a control-bearing checkpoint.
  - Publishes a `{"_meta": {"backfillBegin": true, ...}}` document to all
    journals of the collection before any ordinary documents of that isolated
    checkpoint. These are `CONTROL` documents (implicitly `OUTSIDE_TXN`) and
    are immediately committed.
  - Captures the actual UUID clock assigned to those published control
    documents. That clock is the authoritative `truncatedAt` timestamp.
  - Includes in the recovery log write:
    - The committed `truncatedAt` value for each binding / journal that
      received a `BackfillBegin`.
    - Pending label-update intents, so labels can still be applied if a crash
      happens after the recovery log write but before the broker `Apply`
      succeeds.
  - After the recovery log write is durable, updates journal labels with their
    new `truncatedAt` values. Compares the current label vs. expected and only
    applies if they differ, to minimize churn from task restarts.

### When a capture finishes a backfill:
- **Capture Connector**: Emits `BackfillComplete` message for one or more
  bindings, typically as the last message for those bindings before the
  checkpoint. This ordering is not enforced by the runtime.
- **Capture Runtime**:
  - References the persisted `truncatedAt` timestamp for the applicable journals
    and constructs a pending `{"_meta": {"backfillComplete": true,
    "truncatedAt": "<truncatedAt>", ...}}` control document publication.
  - Does **not** publish the `BackfillComplete` during ordinary checkpoint
    drain. Instead, it persists a pending post-commit complete-publication
    record alongside the connector checkpoint.
  - After the corresponding checkpoint commit is durable, publishes the
    `BackfillComplete` document(s) as `CONTROL`.
  - On success, clears the active backfill state and the pending complete
    publication record. On restart, any pending complete publications are
    retried until they succeed.

### When a materialization reads a `backfillBegin` document:
- **Materialize Runtime**:
  - Recognizes the control document from its UUID `CONTROL` flag. Parses the
    small JSON body to identify this as a `backfillBegin`.
  - Determines `truncatedAt` from the UUID clock (same as `flow_published_at`).
  - Resolves which binding the journal belongs to.
  - Queues a `BackfillBegin: <truncatedAt>` for that binding on the next Flush.
- **Materialization Connector**:
  - Reads `BackfillBegin` information from `Flush` message in
    `materialize-boilerplate`.
  - Surfaces `BackfillBegin` to individual connector implementations in their
    `Store` method. They can do whatever they want with it; generally though
    `BackfillBegin` won't be acted on.

### When a materialization reads a `backfillComplete` document:
- **Materialize Runtime**:
  - Recognizes the control document from its UUID `CONTROL` flag. Parses the
    small JSON body to identify this as a `backfillComplete` and extract
    `truncatedAt`.
  - Resolves which binding the journal belongs to.
  - Queues a `BackfillComplete: <truncatedAt>` for that binding on the next
    Flush.
- **Materialization Connector**:
  - Reads `BackfillComplete` information from `Flush` message in
    `materialize-boilerplate`.
  - Surfaces `BackfillComplete` to individual connector implementations in their
    `Store` method. This is where they'd apply a *"delete all older documents
    than `truncatedAt`, per `flow_published_at`"* operation.

### Nuances / Edge Cases:

**Ordering Guarantees**: The capture runtime uses combiners to combine and sort
by key all documents for a binding within a transaction. Control messages cannot
participate in this combining — they must be sequenced precisely relative to the
combined documents. Because the runtime may combine multiple connector
checkpoints into one runtime transaction, any connector checkpoint containing a
`BackfillBegin` or `BackfillComplete` is a hard boundary. The practical
consequences are:

- If a control-bearing connector checkpoint is encountered after ordinary
  checkpoints have already been buffered, the runtime drains the buffered work
  first and isolates the control-bearing checkpoint in the next Gazette
  transaction.
- `BackfillBegin` is written to collection journals *before* the ordinary
  documents of its isolated checkpoint. This is why the runtime enforces that
  `BackfillBegin` is the first message for a binding in a transaction.
- `BackfillComplete` is written to collection journals only *after* the
  isolated checkpoint has committed. It is never emitted inline after ordinary
  `CONTINUE_TXN` documents for the same producer.

**Capture emits `backfillComplete` without a corresponding `backfillBegin` (no
`truncatedAt` available)**: Generally this would be a connector logic error.
However, during a transition from "connector doesn't write these messages" ->
"now it does", there may be connectors in the middle of a backfill. To
accommodate this, the capture runtime ignores a `BackfillComplete` from the
connector if it has no persisted `truncatedAt` for that binding's journals.

**Materialization starts reading after `backfillBegin`, but before
`backfillComplete`**: The materialization reads the `backfillComplete` control
document and extracts `truncatedAt` from its body. It sends a
`BackfillComplete` to the connector, which truncates rows older than
`truncatedAt`. Since the materialization started reading after the backfill
began, all its data is post-`truncatedAt`, so the truncation is a no-op.

**Loaded documents superseded by truncation**: In a standard-updates
materialization, a destination document returned during `Load` may be
superseded by a newly backfilled journal document instead of being reduced with
it. Even in that case, the loaded document's existence in the destination must
still be preserved and reflected in the eventual `Store.exists=true`. This
allows connectors that don't yet implement truncation handling to still make
correct "update existing row" vs. "insert new row" decisions, even though the
loaded document's contents were discarded in favor of the post-`truncatedAt`
journal document.

**Partitioned Collections**: For collections partitioned into multiple journals,
a `backfillBegin` control message with the same timestamp will be written to all
of them. Likewise, a `backfillComplete` message (carrying the same `truncatedAt`
in its body) will be written to all of them.

Interleaved reading across journals can produce redundant truncations, but is
always correct. Consider two journals, `JournalA` and `JournalB`, both with two
backfill cycles (`backfillBegin@T00:00` then `backfillComplete(T00:00)`, then
`backfillBegin@T01:00` then `backfillComplete(T01:00)`), read in this order:
- `JournalA: {backfillBegin @ T00:00}`
- `JournalA: {backfillComplete(T00:00)}` — truncate at T00:00
- `JournalA: {backfillBegin @ T01:00}`
- `JournalB: {backfillBegin @ T00:00}`
- `JournalA: {backfillComplete(T01:00)}` — truncate at T01:00
- `JournalB: {backfillComplete(T00:00)}` — truncate at T00:00 (redundant, subsumed by T01:00)
- `JournalB: {backfillBegin @ T01:00}`
- `JournalB: {backfillComplete(T01:00)}` — truncate at T01:00 (redundant)

**Journal additions during a backfill**: If a new journal is added to a
collection after `backfillBegin` but before `backfillComplete`, the new journal
never receives a `backfillBegin`. This is handled by the journal label — the new
journal inherits the `truncatedAt` label, so the materialization never reads
pre-truncation data from it. Truncation actions taken by the materialization
will be redundant.

**Combined Capture Checkpoints**: Ordinary connector checkpoints continue to be
combined for efficiency. However, any checkpoint containing
`backfillBegin` or `backfillComplete` is isolated as its own capture-side
boundary. This prevents a `BackfillBegin` from being reordered ahead of older
buffered documents, and ensures a `BackfillComplete` is never published while
there are still pending `CONTINUE_TXN` documents for the same producer.

For example, if the connector emits:
- checkpoint C1: ordinary documents
- checkpoint C2: `backfillBegin`, documents
- checkpoint C3: ordinary documents
- checkpoint C4: `backfillComplete`

Then the runtime may still combine ordinary work around C1 and C3, but C2 and
C4 are isolated boundaries. Materialization still observes begin and complete in
journal order, and `backfillComplete` carries the persisted `truncatedAt` of the
most recent committed begin.

**Orphaned `backfillBegin` after crash**: Because `BackfillBegin` is a
`CONTROL` (immediately-committed) document published before the ordinary
documents of its isolated checkpoint commit, a crash can leave a visible begin
control document without
the corresponding ordinary documents ever committing. This is tolerated. Journal
labels are not updated until the begin state is durably committed in capture
state, and destination truncation does not happen until a later
`BackfillComplete` carrying the same persisted `truncatedAt` is published.

**Materialization Trickery with `notBefore`**: Say a collection has these control messages:
- `backfillBegin @ T00:00`
- ...first set of documents...
- `backfillComplete(T00:00)`
- `backfillBegin @ T01:00`
- ...second set of documents...
- `backfillComplete(T01:00)`

A materialization could read the `backfillBegin @ T00:00` and partially through
the first set of documents. Then a `notBefore` could be set to skip past the
first `backfillComplete` and `backfillBegin @ T01:00`, into the middle of the
second set of documents. The last `backfillComplete` carries `truncatedAt:
T01:00`. The materialization truncates at T01:00, correctly cleaning up the
first backfill's stale data. However, the second backfill's data is only
partially materialized due to the `notBefore` skip.

This is a data inconsistency, but it is no worse than the current behavior
without backfill signals — an explicit `notBefore` that skips data already
produces an incomplete view of the journal.

## Limitations

**Split capture shards**: There would need to be a coordination mechanism for a
capture (or derivation) task with multiple shards to ensure there is a quiescent
period where no shards are writing documents, and all synchronize initiating a
backfill after the `backfillBegin` control message has been written. This
doesn't seem immediately practical to accomplish.

The runtime will verify that the shard's key range covers the full space
(`key_begin == 0x00000000 && key_end == 0xffffffff`) and if it doesn't, a
`BackfillBegin` or `BackfillComplete` control message will be ignored.

*Additional Edge Case - Capture task is split, starts a backfill, and then merges
into a single shard before it is finished*: This would result in there being a
`BackfillComplete` message with no `BackfillBegin`. The `BackfillComplete` would
be ignored.

**Multiple captures writing to the same collection**: If two captures (A and B)
both target the same collection and Capture-A performs a full refresh, the
resulting truncation deletes all rows older than `truncatedAt` — including valid
rows written by Capture-B. Scoping truncation by source capture would require
provenance tracking (either in the destination schema or in the runtime), which
is not justified for this uncommon configuration. For now, backfill signals on
collections with multiple capture sources may produce incorrect deletions.

**Dekaf**: Dekaf reads collection journals directly and treats ACK documents as
Kafka control records — transmitted for offset tracking but invisible to Kafka
consumers. Backfill control messages will receive the same treatment:
`is_control` in `read.rs` is extended to detect `Flag_CONTROL` 
(`flags & 0x4 != 0`), and these documents are encoded as Kafka control records 
just like ACKs.

Dekaf currently derives `not_before` from the materialization binding spec.
It will be updated to also read the `truncatedAt` journal label from each
partition's `JournalSpec` (available at topology build time) and use
`max(binding.not_before, truncatedAt)` as the effective `not_before`. This
ensures new Dekaf readers skip pre-truncation data.

Beyond that, no additional support for Dekaf with respect to backfill control
messages is planned.

**Derivations**: Derivations read collection documents through the shuffle layer
and will encounter backfill control messages. The derive runtime must filter
these out (alongside ACKs) before passing documents to the derivation connector.
The Rust derive runtime already uses bitwise flag checks (`ACK_TXN & node !=
0`), so the filter extends to check for `Flag_CONTROL` as well.

Derivations do not participate in or propagate backfill signals in the initial
implementation; they simply ignore control messages. Future work might be done
to provide these to the derivation so it could act on backfill signals, like
clearing its state, or emitting backfill control messages of its own.

## Control Message Details

### UUID Flag Extension (Gazette Change)

Gazette message UUIDs have 10 bits reserved for flags. Currently only the lower
2 bits are used for transaction semantics (`OUTSIDE_TXN`, `CONTINUE_TXN`,
`ACK_TXN`). A new `CONTROL` flag is added in bit 2:

- **Bits 0-1**: Transaction semantics (unchanged)
- **Bit 2**: `Flag_CONTROL` — marks the message as an application control
  message. Gazette does not interpret control messages; it only ensures the flag
  does not affect sequencing.

**Invariant:** A `CONTROL` document is always `OUTSIDE_TXN` (the low 2 bits
are zero). The bit layout is:

```go
Flag_CONTROL = 0x4

// A control document's flags value is always exactly Flag_CONTROL (0x04):
//   bit 2      = 1 (CONTROL)
//   bits 0..1  = 0 (OUTSIDE_TXN, i.e., immediately committed)
```

Readers enforce this at parse time — a document with `Flag_CONTROL` set whose
transaction bits are not `OUTSIDE_TXN` is a protocol error. `BackfillBegin`
is published before the ordinary documents of its isolated connector
checkpoint; `BackfillComplete` is published only after that checkpoint has
committed.

Gazette's Sequencer must switch on `flags & 0x3` instead of the full flags
value. Existing code that interprets transaction semantics via exact equality
or switching on the full flag value must be updated to mask: for example,
`GetFlags(uuid) & 0x3 == Flag_ACK_TXN`. Code already using bitwise checks
(e.g., Rust derive runtime) works as-is.

The specific type of control message (backfill begin, backfill complete, or
future types) is determined by parsing the document body — Gazette is not
involved.

### Detection and Bypass

Because control messages have `Flag_CONTROL` set, the runtime detects them from
the UUID — before key extraction, schema validation, or reduction. They are
routed to runtime metadata handling on a separate path from regular documents,
the same way ACK documents are filtered by flag today. The runtime then parses
the small JSON body to determine the control message type and extract any
payload.

### Document Structure

Control messages include a JSON body that is not validated against the
collection schema. For `backfillBegin`, the body is informational — the
`truncatedAt` is derived from the UUID clock (same as `flow_published_at`):

```json
{
  "_meta": {
    "uuid": "...",
    "backfillBegin": true,
    "keyBegin": "00000000",
    "keyEnd": "ffffffff"
  }
}
```

For `backfillComplete`, the body is authoritative — the materialization runtime
parses `truncatedAt` from it:

```json
{
  "_meta": {
    "uuid": "...",
    "backfillComplete": true,
    "truncatedAt": "2025-01-15T12:00:00Z",
    "keyBegin": "00000000",
    "keyEnd": "ffffffff"
  }
}
```

The shard key range is in the body — the runtime always populates this from the
shard spec; connectors never deal with it. It's included to support future,
hypothetical work where multiple shards could engage in a synchronized backfill.
In the meantime, control messages from single-shard captures always carry the
full range.

## Journal Labels and New Readers

When committed `BackfillBegin` state is durably recorded, journal labels are
updated with a truncation timestamp (e.g., `estuary.dev/truncated-at`). The
shuffle layer reads this label from each journal's `LabelSet` (alongside
existing labels like `KeyBegin`/`KeyEnd`) and computes an effective
`notBefore` as `max(spec.notBefore, journal.truncatedAt)`.

### Label Update Intents

Journal label updates are not atomic with recovery log commits. To ensure labels
are always applied, "label update intents" are stored in RocksDB, alongside the
connector checkpoint.

Both paths share the same intent structure:

1. **During commit** (`recv_client_start_commit`): Label update intents are
   written to the RocksDB `WriteBatch` alongside the checkpoint and committed
   begin state. After the recovery log write is durable, the Go runtime in
   `StartCommit` applies the labels via broker `ApplyRequest`. Successful
   application clears only the pending label-update intent.
2. **During startup** (`RestoreCheckpoint`): The Go runtime receives the
   checkpoint from the Rust side via `Opened.runtime_checkpoint`. If pending
   label update intents are present, they are applied.

`BackfillComplete` does not clear or roll back the journal label. A later
`BackfillBegin` overwrites it with a newer `truncatedAt`.

This is a relatively heavy operation (etcd write via broker), but backfill
signals are infrequent.

## Protocol Changes

### Capture Protocol

New response message types:

```protobuf
message Response {
  // Signals the start of a backfill for a binding.
  message BackfillBegin {
    uint32 binding = 1;
  }
  BackfillBegin backfill_begin = 9;

  // Signals the end of a backfill for a binding.
  message BackfillComplete {
    uint32 binding = 1;
  }
  BackfillComplete backfill_complete = 10;
}
```

### Materialize Protocol

Backfill signals are placed on `Flush` because the runtime discovers them
during the Load phase (reading from journals):

```protobuf
message Request {
  message Flush {
    repeated BackfillBegin backfill_begin = 1;
    message BackfillBegin {
      uint32 binding = 1;
      google.protobuf.Timestamp truncated_at = 2;
    }

    repeated BackfillComplete backfill_complete = 2;
    message BackfillComplete {
      uint32 binding = 1;
      google.protobuf.Timestamp truncated_at = 2;
    }
  }
}
```

Both `backfill_begin` and `backfill_complete` carry `truncated_at`, so connectors
do not need to track state across transactions.
