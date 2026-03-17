# Shuffle Fuzz Test Requirements

## Purpose

Verify the shuffle crate's `Session → Slice → Log` pipeline correctly
implements transactional semantics under randomized multi-producer workloads,
including crash recovery. The fuzz test uses `quickcheck` to generate
randomized workloads and `e2e_support` to run them against a hermetic
DataPlane with real Gazette journals.

## Background

### Shuffle architecture

A shuffle session reads committed documents from Gazette journals, routes them
by key hash to Log members, and presents them to a downstream consumer
(e.g., a materialization) via `FrontierScan`. The hierarchy is:

```
Coordinator (the fuzz test driver, playing the role of a materialization reactor)
    ↓
Session (one per task, aggregates progress from Slices)
    ↓
Slice (one per member, reads from Gazette journals)
    ↓
Log (one per member, writes entries to on-disk segments)
```

The fuzz test parameterizes the member count (`N=1..3`). With `N=1` there is
one Slice and one Log; with `N>1`, documents are routed by key hash to
different members' Logs. This exercises both transactional correctness and
multi-member document routing within the same test cases.

### Transaction framing

Producers write documents into Gazette journals using UUID-based transaction
framing. Each producer commits to one style and does not mix them:

- **OUTSIDE_TXN**: Self-committing. The document is immediately visible as a
  committed transaction of one document. No ACK is needed.

- **CONTINUE_TXN + ACK**: Multi-document transactions. One or more
  `CONTINUE_TXN` documents are written (possibly across multiple journals),
  followed by an `ACK_TXN` that commits them all atomically.

A given producer ID uses exclusively one style. This is not a limitation of
shuffle itself, but a simplification for the fuzz test that avoids needing to
reason about interleaved single-doc and multi-doc commits from the same
producer.

### Clocks and ordering

Each document carries a UUID with a Lamport clock that increases monotonically
within a single producer. **Clocks do not provide ordering across producers.**
Two producers writing to disjoint journals are completely independent — shuffle
is not required to surface producer P1 at clock C1 before producer P2 at clock
C2, even if C1 < C2. Only within a single producer is clock ordering
meaningful.

### Causal hints and cross-journal transactions

When a CONTINUE_TXN transaction spans multiple journals, the ACK document
written to each journal carries **causal hints** referencing the other journals
in the transaction. For example, if producer P writes CONTINUE_TXN to journals
A and B, then commits:

- ACK in journal A carries a hint: "producer P committed at clock C in journal B"
- ACK in journal B carries a hint: "producer P committed at clock C in journal A"

The `CheckpointPipeline` in the Session will not promote a checkpoint to
`ready` until all causal hints are resolved — meaning the Session has observed
matching progress in the hinted journals. This guarantees that a single
`NextCheckpoint` response reflects the **complete** transaction across all
journals, never a partial view.

### Checkpoints are deltas

Each `NextCheckpoint` response from the Session is a **delta**, not a
cumulative snapshot. It contains only the journals and producers that
progressed since the last checkpoint. The consumer must `reduce()` each delta
into a cumulative base to build the full state:

```rust
round_frontier = round_frontier.reduce(delta);
```

`Frontier::reduce()` performs a sorted merge on `(journal, binding)`, then on
`producer`, taking the element-wise max of `last_commit`, `hinted_commit`, and
`offset`.

The Session may coalesce multiple commits into a single checkpoint delta, or
split them across multiple deltas — this is fundamentally racy and depends on
timing within the pipeline. The test driver must be prepared for either case.

### Recovery model

When a session "crashes" (closes) after processing transactions but before the
downstream consumer has fully applied them, the consumer can recover by
opening a new session with a **resume checkpoint** that encodes which
transactions need to be replayed.

The key mechanism is **hints projection**: the consumer takes the frontier from
the incomplete round (which has `last_commit` values for producers that
committed) and projects those `last_commit` values into `hinted_commit` fields
in a new frontier where everything else is zeroed. This projection is then
`reduce()`d into the cumulative recovery frontier:

```
projection = for each (binding, journal, producer) in round_frontier:
    ProducerFrontier {
        producer,
        last_commit: Clock::default(),      // zeroed
        hinted_commit: round.last_commit,   // projected
        offset: 0,                          // zeroed
    }
recovery = recovery.reduce(projection)
```

Because `reduce()` takes element-wise max, the recovery frontier now has:
- `last_commit` from prior rounds (the confirmed baseline thus far for ALL journals and producers)
- `hinted_commit` from the incomplete round (what needs to be replayed)

When the new session opens with this recovery frontier, the
`CheckpointPipeline` detects producers with `hinted_commit > last_commit` and
sets `recovery_pending = true`. This **blocks** new progress from being
promoted to `ready` until the recovery checkpoint is consumed. The first
`NextCheckpoint` from the new session reflects exactly the recovered
transaction — the same documents that were committed in the original session.

**Important**: This hints projection is a **client-side operation** that does
not exist in the shuffle crate. The fuzz test must implement it. It is not the
same as `Frontier::project_unresolved_hints()`, which is a server-side
operation that extracts already-unresolved hints from a resume checkpoint.

### Log segments and session lifecycle

Each session creates on-disk log segments via its Log actor. When a session is
**closed cleanly** (the consumer reads the Session EOF), the log segments are
deleted. This means:

- `FrontierScan` can only read segments from the **currently running** session.
- After closing a session and opening a new one, the old session's
  `flushed_lsn` values are meaningless. The recovery frontier's `flushed_lsn`
  must be reset to `vec![]` before opening the new session.
- The `Reader` and `Remainders` state from the old session is invalid and must
  be discarded (fresh `member_state`).

A single log directory can be reused across session restarts because the clean
close deletes old segments, and the new session creates fresh ones.

## Invariants Under Test

### 1. Completeness

Every document from a committed transaction must appear in the `FrontierScan`
output. No committed work is silently dropped.

### 2. Safety

Documents from uncommitted or rolled-back transactions must never appear in
`FrontierScan` output. The session must not leak partial transaction state.

### 3. Idempotent Recovery

When a session is closed and restarted from a recovery frontier with
unresolved hints, the first `NextCheckpoint` from the new session must produce
a frontier that, when scanned, yields exactly the same committed documents as
the original session's frontier for that round.

### 4. Cross-Journal Atomicity

Transactions spanning multiple journals must be atomic in a single
`NextCheckpoint` response: either all documents across all journals are
visible, or none are. The `CheckpointPipeline` enforces this by resolving all
causal hints before promoting a checkpoint to `ready`.

To maximally stress this invariant, the test driver should stop polling
`NextCheckpoint` as soon as it sees the minimum necessary progress (see
Polling Termination below), rather than waiting for all journals. If shuffle
incorrectly splits a multi-journal transaction across checkpoints, the scan
will see partial results and the oracle will catch the violation.

### 5. Rollback Isolation

After a rollback, the rolled-back producer's pending documents are discarded.
Subsequent reads must not surface them. The rollback ACK itself must not
introduce spurious documents.

After emitting a rollback at clock C, the producer is "retired": an expected
write invariant assumed by shuffle is that a rolled-back producer will never
again emit a clock larger than C (though it may emit duplicates of C). The
fuzz input generator must enforce this constraint.

## Test Design

### Configuration

- **Members**: 1–3 (randomized per test case). The key range `[0, u32::MAX]`
  is split evenly across members; all share the full r_clock range, the same
  gRPC endpoint, and the same log directory (segment files are isolated by
  member index in the filename: `mem-{index:03}-seg-{…}.flog`).
- **Bindings**: A single binding with multiple partitions, giving multiple
  journals under one binding index. The number of partitions should be
  sufficient to exercise cross-journal transactions (at least 4-5).
  Alternatively, multiple bindings of the same cohort (same `priority` and
  `read_delay`) may be used — the key requirement is multiple journals.
- **Binding config**: `priority=0`, `read_delay=0`, `filter_r_clocks=false`.
  Keep it simple; these features are orthogonal to transactional correctness.
- **Document shape**: Minimal. Each document embeds a per-producer action
  counter (sequential, starting at 0) that serves as an end-to-end integrity
  check. The oracle tracks `(producer, counter)` pairs and verifies they
  match `FrontierScan` output. Example:
  ```json
  {
    "_meta": {"uuid": "..."},
    "id": "<partition_value>",
    "category": "<partition_field>",
    "counter": 42
  }
  ```

### Fuzz Input

A quickcheck test case is a `Vec<Round>`. Between test cases,
a data-plane drops all journals and fragments for a clean slate.

#### Generation constraints

These must be enforced during `Arbitrary` generation (or via post-filter):

1. **No style mixing**: Each producer uses exclusively `OutsideTxn` actions or
   exclusively `Continue`/`AckCommit`/`AckRollback` actions.

2. **Valid transaction structure**: `AckCommit` and `AckRollback` are only
   valid when the producer has pending `Continue` actions. A `Continue`
   producer's actions within a round should form valid transaction(s):
   one or more `Continue` followed by an `AckCommit` or `AckRollback`.

3. **Retirement is permanent**: Once a producer emits `AckRollback`, it must
   never appear in any subsequent round. This is a write invariant that
   shuffle assumes. Violation would produce undefined behavior.

4. **Variable sizing**: The number of producers, actions per round, and
   partitions targeted should vary across test cases to maximize stress and
   exercise different race conditions.

#### Shrinking

Quickcheck's power comes from shrinking failing cases to minimal
reproductions. The `Arbitrary` impl should support shrinking that:

- Removes entire rounds
- Removes producers (and all their actions)
- Reduces the number of actions per producer
- Preserves all generation constraints (monotonic clocks, no post-rollback
  actions, valid transaction structure)

### Oracle

The oracle tracks the expected state of committed documents independently
from the shuffle pipeline. It maintains:

```rust
// Per producer: set of (counter, partition) pairs that are committed
committed: HashMap<ProducerId, Vec<(Counter, PartitionId)>>

// Per producer: whether the producer has been retired (rolled back)
retired: HashSet<ProducerId>
```

As the test driver executes actions:
- `OutsideTxn`: immediately adds `(counter, partition)` to committed
- `Continue`: adds `(counter, partition)` to a pending buffer
- `AckCommit`: moves all pending entries to committed
- `AckRollback`: discards all pending entries; marks producer as retired

After scanning a round's `FrontierScan` output, the oracle asserts:
- **Completeness**: Every `(producer, counter)` pair expected for this round
  appears in the scan output.
- **Safety**: No unexpected `(producer, counter)` pairs appear.
- **Integrity**: The document body's counter matches the expected value for
  that producer.

### Round Execution Flow

```
Setup:
  - Build catalog fixture and start DataPlane
  - Start shuffle gRPC server
  - Create log directory
  - Initialize: recovery = Frontier::default()
  - Open initial session with recovery as resume checkpoint

For each round:
  1. WRITE: Execute the round's actions via Publishers.
     - OutsideTxn producers: enqueue + flush
     - Continue producers: enqueue all Continues, then:
       - AckCommit: commit_intents() + build_transaction_intents() + write_intents()
       - AckRollback: write rollback ACK at last_commit clock
     - Update oracle state accordingly.

  2. INIT ROUND FRONTIER:
     round_frontier = Frontier {
         journals: vec![],
         flushed_lsn: recovery.flushed_lsn.clone(),
     }

  3. POLL CHECKPOINTS:
     Loop: call next_checkpoint(), reduce delta into round_frontier.
     Stop when the polling termination condition is met (see below).
     If the round has no commits, loop zero times (the round_frontier
     stays empty, which is a valid frontier that surfaces no documents).

  4. PROJECT HINTS INTO RECOVERY:
     Create a projection of round_frontier where each producer's
     last_commit becomes hinted_commit, with all other fields zeroed.
     Reduce this projection into recovery:
       recovery = recovery.reduce(projection)

  5. WRITE NEXT ROUND (if not last):
     Execute the next round's journal writes immediately. This creates
     an intentional race: the next round's data is in the journals, but
     the current round's frontier gates visibility. The FrontierScan in
     step 7 must see only the current round's committed documents.

  6. MAYBE CRASH AND RESTART (if round.crash is true):
     a. session.close() — reads EOF, ensures clean teardown, deletes
        log segments.
     b. recovery.flushed_lsn = vec![] — old session's LSNs are invalid.
     c. Open new session with recovery as resume checkpoint.
     d. Discard old member_state for all members (Reader, Remainders).
     e. Re-initialize round_frontier:
        round_frontier = Frontier {
            journals: vec![],
            flushed_lsn: recovery.flushed_lsn.clone(),
        }
     f. If recovery has unresolved hints (any producer with
        hinted_commit > last_commit): read first NextCheckpoint from
        the new session. This is the recovery checkpoint — it replaces
        round_frontier. (See "Why only with hints" below.)

  7. SCAN: For each member (0..N), drive FrontierScan with round_frontier
     and a member-specific Reader. Collect all entries across members into
     a single flat list. Assert against oracle expectations for this round.

  8. ACCUMULATE: recovery = recovery.reduce(round_frontier)

Close session.

Teardown:
  - Stop shuffle server, graceful_stop DataPlane.
```

### Polling Termination

The test driver stops polling `NextCheckpoint` when **every producer that
committed in this round** is visible in at least one journal of the
`round_frontier`. Specifically, for each committing producer P with expected
clock C:

> There exists at least one `(journal, binding)` in `round_frontier` where
> producer P has `last_commit >= C`.

We intentionally check the **minimum**: one journal per producer, not all
journals. This stresses the cross-journal atomicity invariant — if shuffle
correctly resolves causal hints, all journals for a multi-journal transaction
will appear together in the same checkpoint. If shuffle splits them, the scan
will catch the inconsistency.

**Why not wait for all journals?** Waiting for all journals per producer would
mask bugs where shuffle surfaces partial transactions. By checking the minimum,
we rely on the invariant rather than papering over potential violations.

**Why not use a single "sentinel" producer?** Clocks are Lamport timestamps,
not wall clocks. There is no ordering guarantee across producers. Producer P1
committing at clock 100 does not imply producer P2's clock 50 has been
surfaced. Each producer must be checked independently.

**Zero-commit rounds**: If no producers committed in this round (e.g., all
actions were Continues without Acks, or the round was empty), the termination
condition is trivially satisfied and we loop zero times. The `round_frontier`
stays as initialized (empty journals, recovery's `flushed_lsn`). This is a
valid frontier that `FrontierScan` will scan to produce zero documents.

### Why Crash Only Works With Hints

When a session opens with a resume checkpoint containing unresolved hints
(`hinted_commit > last_commit`), the `CheckpointPipeline` sets
`recovery_pending = true`. This **blocks** new progress from being promoted
to `ready`, ensuring the first `NextCheckpoint` is exactly the recovery
checkpoint — isolated from any new data in the journals.

Without unresolved hints, `recovery_pending` is false. The first
`NextCheckpoint` would include **all** progress, including documents from
the next round (which was already written in step 5). The test driver cannot
distinguish current-round recovery from next-round progress.

The test handles both cases correctly:
- **With hints**: Read the first `NextCheckpoint` as the recovery checkpoint
  (step 6f). It replaces `round_frontier` with the recovered state, which
  should match the original round's committed documents.
- **Without hints**: Skip the recovery `NextCheckpoint` (step 6f). The
  `round_frontier` stays empty (re-initialized in step 6e). Scanning produces
  zero documents, which is correct — there is nothing to recover. This still
  exercises the restart path (session close, new session open, fresh reader
  state) even without testing the recovery invariant.

### Why Not Zero the Recovery Frontier

An earlier draft proposed zeroing the entire recovery frontier after a restart.
This is wrong. The recovery frontier contains `JournalFrontier` entries from
**all prior rounds** — producer `last_commit` values, offsets, etc. Zeroing
it would lose the cumulative committed state, causing the new session to re-read
journals from the beginning and surface already-processed documents.

Only `flushed_lsn` should be reset to `vec![]` after a restart, because
`flushed_lsn` references on-disk log segments from the old session, which
were deleted on clean close. The journal/producer state (`last_commit`,
`hinted_commit`, `offset`) must be preserved — it tells the new session where
to resume reading in the Gazette journals.

### Why Write the Next Round Before Scanning

The test deliberately writes the next round's actions to journals (step 5)
before scanning the current round's frontier (step 7). This creates a race
that stresses shuffle's visibility guarantees:

- The Slice actors will pick up the next round's documents from the journals
  and route them to the Log actor, writing new log blocks.
- But `FrontierScan` is gated by `round_frontier`, whose `flushed_lsn`
  predates the next round's log writes. New blocks with higher LSNs are
  invisible to the scan.
- The oracle asserts that only the current round's committed documents appear.
  Any leakage from the next round would be caught.

This ordering is critical. If we scanned first and wrote the next round after,
we would lose this coverage.

### Why Not Mix OUTSIDE_TXN and CONTINUE/ACK Per Producer

While shuffle itself handles producers that mix transaction styles, the fuzz
test restricts each producer to one style. This simplification:

- Makes the oracle easier to reason about (no interleaving of self-committing
  and multi-doc commits from the same producer).
- Reduces the state space without sacrificing coverage of the critical
  invariants (completeness, safety, recovery, atomicity, rollback).
- Avoids edge cases in fuzz input generation where a producer might have
  pending CONTINUEs and also emit OUTSIDE_TXN documents.

Both styles are still exercised — just by different producers within the same
test case.

### DataPlane Reset Between Test Cases

`data_plane.reset()` is called between quickcheck test cases (not between
rounds). It deletes all journal specs and clears persisted fragments, ensuring
each test case starts with a clean slate.

Within a test case, journals persist across rounds — this is essential because
later rounds must see committed data from earlier rounds (the session reads
from journal offsets recorded in the recovery frontier). The session's clean
close (reading EOF) deletes log segments between restarts, restoring the log
directory to an empty state without affecting journal content.
