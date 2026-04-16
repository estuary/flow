# Truncation + Shuffle-V2 Plan

## Summary
For higher-level descriptions, reference `plans/truncation.md` and plans `truncation-shuffle-v2.md`.

Implement truncation in three phases, with shuffle-v2 first:
1. Add committed backfill metadata and source filtering to shuffle-v2 and the `flowctl raw shuffle` prototype.
2. Add capture-side control-message production, durable truncation state, UUID flagging, and journal label updates.
3. Add direct-consumer and materialization plumbing for truncation semantics, while keeping legacy Go shuffle in compatibility-only mode.

Legacy Go shuffle is explicitly out of scope for truncation behavior. It only needs the minimum changes required so control docs do not break it. It will not propagate control docs and will not honor the `truncated-at` journal label.

## Public API / Interface Changes
- Add `Flag_CONTROL = 0x4` to UUID/message flags. A `CONTROL` document is always `OUTSIDE_TXN` (flags == `0x4`) and immediately committed; the runtime rejects any document that combines `CONTROL` with `CONTINUE_TXN` or `ACK_TXN`. `BackfillBegin` is published before the ordinary documents of its isolated checkpoint, while `BackfillComplete` is published only after that checkpoint commits.
- Add mirrored `estuary.dev/truncated-at` label constants in Go and Rust label packages.
- Extend `go/protocols/shuffle/shuffle.proto` terminal `FrontierChunk` with sparse per-binding:
  - `latest_backfill_begin`
  - `latest_backfill_complete`
- Extend `go/protocols/capture/capture.proto` with:
  - `Response.BackfillBegin { binding }`
  - `Response.BackfillComplete { binding }`
- Extend `go/protocols/materialize/materialize.proto` `Flush` with:
  - `backfill_begin[] { binding, truncated_at }`
  - `backfill_complete[] { binding, truncated_at }`
- Extend `go/protocols/runtime/runtime.proto` so the Rust capture runtime can surface active backfill state to the Go capture app on open and commit.
- Extend `doc::combine::Meta` with a `stale_loaded` bit.

## Implementation Changes
### 1. Shuffle-v2 first
- Extend shuffle-v2 frontier state and checkpoint reassembly so terminal `NextCheckpoint` results can carry `latest_backfill_begin` and `latest_backfill_complete` as binding-keyed `max(clock)` maps.
- Teach shuffle-v2 slice readers to detect `Flag_CONTROL` docs and parse `_meta.backfillBegin` / `_meta.backfillComplete`.
- Treat control docs as immediately committed metadata events. They do not participate in `CONTINUE_TXN` / `ACK_TXN` sequencing and therefore do not need staged rollback handling.
- Do not append control docs to local shuffle log segments.
- Fold committed control events into checkpoint frontier state and preserve begin and complete independently.
- Update `flowctl raw shuffle` to keep in-memory `last_backfill_begin` per binding across checkpoints, compute `effective_begin = max(last_backfill_begin, checkpoint.latest_backfill_begin[binding])`, and suppress source-log entries older than `effective_begin`.
- Advance in-memory `last_backfill_begin` only after each checkpoint is fully drained so filtering stays checkpoint-ordered.
- Keep the prototype limited to source filtering; loaded-row truncation remains out of scope until a real runtime-v2 coordinator exists.

### 2. Capture control docs and durable truncation state
- Extend the Rust capture runtime to accept `Captured`, `SourcedSchema`, `BackfillBegin`, `BackfillComplete`, and `Checkpoint` in one transaction stream.
- Treat any connector checkpoint containing `BackfillBegin` or `BackfillComplete` as a hard batching boundary: do not combine connector checkpoints across it.
- Enforce begin-before-data ordering for a binding within a connector checkpoint and ignore complete events that do not match persisted active backfill state.
- Persist active backfill state by source journal in capture RocksDB, including binding and the actual begin publish clock (`truncated_at`).
- Use that persisted state as the source of truth for:
  - synthesizing `BackfillComplete` control docs
  - reapplying `truncated-at` journal labels after restart
  - validating completion events
- Extend the internal runtime protocol so the Go capture app receives active-backfill state, pending label-update intents, and pending post-commit complete publications on shard open and commit handling.
- Apply `truncated-at` labels through broker `ApplyRequest` only after the corresponding begin state is durably committed, and reapply them idempotently on restore. `BackfillComplete` does not clear the label.
- Synthesize begin and complete control docs with `_meta.backfillBegin` / `_meta.backfillComplete` and publish them with `Flag_CONTROL` (i.e., `OUTSIDE_TXN` sequencing).
- Publish `BackfillBegin` control docs before ordinary docs of the isolated checkpoint, and capture the actual UUID clock assigned by the Go publisher as the authoritative `truncated_at`.
- For `BackfillComplete`, persist a pending post-commit publication record keyed by binding / `truncated_at`, then publish the control docs only after the checkpoint commit barrier resolves. Replay pending complete publications on restart until they succeed.
- Integrate post-commit complete publication into the capture app's commit future so the next poll / checkpoint acknowledgement does not race ahead of it.
- Extend the publication path so synthetic docs can request explicit UUID flags and report back their assigned UUID clocks instead of going through the ordinary data-doc path blindly.
- Audit exact transaction-flag equality checks in the capture, shuffle-v2, and legacy Go shuffle compatibility paths and convert them to masked checks where the new control bit would otherwise break behavior.

### 3. Consumers and compatibility
- Materialization runtime and protocol:
  - recognize backfill begin/complete notifications and queue them onto `Flush`
  - keep control docs out of ordinary combine/store flow
  - add `stale_loaded` handling so stale loaded rows can be discarded without losing `front=true` on a surviving ordinary row
  - preserve `Store.exists` behavior from surviving `front=true`
- Direct readers and Dekaf:
  - treat control docs as non-data records
  - compute effective `not_before = max(binding.not_before, journal.truncated_at)` from journal metadata
- Legacy Go shuffle compatibility only:
  - detect `Flag_CONTROL` at the read boundary and drop those documents before key extraction, coordinator broadcast, or subscriber delivery
  - update only the ACK-specific flag checks needed so the new control bit does not break existing transaction handling
  - do not propagate control docs downstream
  - do not read or apply the `truncated-at` label
  - do not modify existing `notBefore` behavior beyond what is required to keep the system running

## Test Plan
- Shuffle-v2 tests for terminal checkpoint metadata encoding, merge semantics, duplicate handling of outside-txn control docs, and absence of control docs from local log segments.
- `flowctl raw shuffle` tests for source suppression across checkpoints and repeated backfills on one binding.
- Capture tests for:
  - hard batching boundaries around control-bearing connector checkpoints
  - begin-before-data ordering
  - ignored invalid completes
  - orphaned begin control docs not causing label updates before durable begin-state commit
  - persisted active-backfill recovery after restart
  - replay of pending post-commit complete publications after restart
  - label reapplication
  - control-doc publication with `Flag_CONTROL` alone (flags == `0x4`)
- Materialization and combiner tests for:
  - stale loaded plus ordinary row for the same key
  - stale loaded row with no ordinary replacement
  - preservation of `Store.exists`
  - begin/complete notification delivery on `Flush`
- Dekaf/direct-reader tests for control-doc suppression and `truncated-at`-based effective `not_before`.
- Legacy Go shuffle compatibility tests proving:
  - control docs are ignored without panic or mis-shuffle
  - ACK handling still works after `Flag_CONTROL` is introduced
  - `truncated-at` labels are ignored and existing read behavior is unchanged
- Protocol generation and regression updates for Go protobufs, Rust generated protos, and `crates/proto-flow` snapshots.

## Assumptions and Defaults
- The intended high-level design doc is `plans/truncation.md`.
- Legacy Go shuffle-backed derivations and materializations are out of scope for truncation correctness; they only receive compatibility fixes.
- Truncation semantics are expected to be correct for shuffle-v2-based flows and direct readers that honor control docs or `truncated-at`.
- Durable runtime-v2 coordinator persistence of per-binding `last_backfill_begin` is deferred; the first implementation keeps that state only inside the `flowctl raw shuffle` prototype.
- Capture-side persisted begin state is the authoritative truncation record until the corresponding post-commit `BackfillComplete` publication succeeds and clears it.
- Orphaned `BackfillBegin` control docs are tolerated. Journal labels are driven only from durably committed begin state, not merely from the presence of a published control doc.
