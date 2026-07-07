# Catalog Tests on runtime-next (Rust-only)

## Objective

Replace the Go-based catalog test flow (`flowctl-go test` + `go/testing/` +
the Testing RPC service) with a Rust-only implementation built on the V2
`runtime-next` crate and its host seams (Publisher / Shuffle / Logger
factories). The new implementation must be linkable as a crate:

- `flowctl test --source ...` runs catalog tests locally.
- The control-plane agent links the same crate to run publication tests,
  replacing its shell-out to `flowctl-go api test` and the temp-data-plane.

At completion, catalog tests require no Gazette broker, no etcd, no Go
consumer, and no `flowctl-go` binary.

## How V1 works today (normative reference for semantics)

Read these files before porting; they are the source of truth for behavior:

- `go/flowctl-go/cmd-test.go` — orchestration: temp data plane (etcd +
  gazette + Go consumer over unix sockets), build, activate, test, delete.
- `go/flowctl-go/cmd-api-test.go` — loads built `TestSpec`s, sorts them by
  step scope, polls shard readiness, runs each case, resets state between
  cases, writes `--snapshot` files of actual documents on failure.
- `go/testing/graph.go` — the dataflow graph and clock algorithm (detailed
  below). `go/testing/graph_test.go` and `action_test.go` are the reference
  test fixtures; port them as Rust unit tests.
- `go/testing/action.go` — the `RunTestCase` loop.
- `go/testing/driver.go` — `Stat` / `Ingest` / `Verify` / `Advance` against a
  live cluster; document combination and comparison semantics.
- `go/testing/clock.go` — `MinClock` / `MaxClock` / `ContainsClock` over
  `pb.Offsets` (journal → offset maps).
- `go/runtime/testing.go` — the Testing RPC service (`Ingest`,
  `AdvanceTime`, `ResetState`).
- `go/protocols/flow/flow.proto` — `TestSpec` (~line 590) and the `Testing`
  service (~line 741).

### The graph / scheduling algorithm (must be ported faithfully)

The `Graph` tracks, for a catalog of **derivations only** (V1 constructs it
as `NewGraph(nil /*captures*/, collections, nil /*materializations*/)` —
captures and materializations are excluded and never run during tests):

- `outputs`: task → collections it writes.
- `readers`: collection → list of `(task, transform suffix, read delay)`.
- `readThrough`: task → clock (per-journal offsets) it has read through.
- `writeClock`: global max write progress per journal.
- `pending`: `PendingStat { ReadyAt (synthetic TestTime), TaskName,
  ReadThrough }`.
- `atTime`: synthetic test time, advanced lazily.

Key operations:

- `CompletedIngest(collection, writeAt)`: max-reduce `writeClock`, then
  `projectWrite` — for each reader of the collection, compute its required
  read clock (write offsets filtered to the collection's journals, with the
  reader's suffix appended), skip if `readThrough[task]` already contains
  it (`ContainsClock` — this check is what terminates self-cycles), else
  enqueue/merge a `PendingStat` with `ReadyAt = atTime + readDelay`.
- `CompletedStat(task, readClock, writeAt)`: min-reduce into
  `readThrough[task]`, max-reduce `writeClock`, and `projectWrite` the
  task's outputs (cascades to downstream readers).
- `PopReadyStats()`: pop stats with `ReadyAt == atTime`; also report the
  delta to the next-ready time (for lazy time advancement).
- `HasPendingWrite(collection)`: BFS from pending stats through
  outputs/readers; true if any pending task transitively writes the
  collection. This gates verify steps.
- `CompletedAdvance(delta)`: `atTime += delta`.

The `RunTestCase` loop: snapshot `initial = writeClock`; repeatedly pop and
execute ready stats (cascading); run INGEST steps immediately; run VERIFY
steps only once `HasPendingWrite(collection)` is false; when neither steps
nor ready stats can progress, advance synthetic time to the next
`ReadyAt`; done when all steps are consumed and nothing is pending.

### V1 step semantics

- **Ingest** (`FlowTesting.Ingest`): fixture documents are first **combined
  by collection key** (write schema), then partition-routed and published;
  returns per-journal write heads.
- **Verify** (`ClusterDriver.Verify`): fetch all documents in the
  collection's journals in the window `(initial, writeClock]` — i.e. only
  documents written during this test case — filtered by the step's
  partition `LabelSelector`; **combine them by key** (read schema); compare
  against expected documents (which build-time validation guarantees are in
  collection-key order) using:
  - superset matching (actual ⊇ expected is a pass),
  - epsilon float comparison (string fast-path, then `f64` compare with
    FLT_EPSILON scaled to magnitude — see `driver.go compareNumbers`),
  - document UUIDs masked (V1 replaces `_meta.uuid` values with the
    placeholder `"flow-uuid"` before comparison),
  - failures report mismatched / missing / unexpected documents.
- **Reset** (between test cases): V1 sends `derive.Request.Reset` to every
  derivation connector, fire-and-forget (`go/runtime/derive.go`
  `ClearRegistersForTest`). Connector-internal state clears; read
  checkpoints, persisted state, and collection data all survive.
- **Advance**: bumps a synthetic publish-clock delta so read-delayed
  transforms become unblocked without wall-clock waits.

### V1 publication-test path (control plane)

`crates/control-plane-api/src/publications/builds.rs` (`test_catalog`,
~line 198): spawns a temp-data-plane via `flowctl-go`, activates each
derivation **with 3 splits** ("to try to catch shuffle errors"), then runs
`flowctl-go api test`. The `flowctl_go` binary path is threaded from
`crates/agent/src/main.rs`. Job logs stream to users via `logs_tx`.

## runtime-next building blocks (what the harness plugs into)

- **Host seams** (statically dispatched; implement all three):
  - `Publisher` / `PublisherFactory` — `crates/runtime-next/src/publish.rs`.
    Hot path is `publish_doc(binding, doc, uuid_ptr)`; commit boundary is
    `commit_intents()` / `write_intents()`. `NoopPublisher` in the same
    file is a reference impl.
  - `Logger` / `LoggerFactory` — `crates/runtime-next/src/logger.rs`.
    Synchronous; `LogEvent` is `#[non_exhaustive]`.
  - `ShuffleSession` / `ShuffleSessionFactory` —
    `crates/runtime-next/src/leader/shuffle.rs`. The leader calls
    `request_checkpoint()` / `recv_checkpoint() -> shuffle::Frontier`.
    Invariant: a yielded Frontier must reference log content already
    durably written to shard shuffle directories.
- **Session protocol** — `go/protocols/runtime/runtime.proto`, `Derive`
  message. Streams flow Controller → Shard, with Shard ↔ Leader relaying:
  `SessionLoop → Join → Joined → Task → Recover → Open → Opened →
  ⟨transactions: Load/Loaded, Flush/Flushed, Store/Stored,
  Persist/Persisted, WriteIntents⟩ → Stop → Stopped`. `Stop` is the
  precedent for controller-initiated control signals: Controller → Shard →
  Leader, with `Stopped` confirmations flowing back (see
  `crates/runtime-next/src/shard/derive/actor.rs` around the
  `Stop or CloseNow` verify, and the `stopping` flag in
  `crates/runtime-next/src/leader/derive/fsm.rs`).
- **Visibility model** — the shard-side scan admits a document iff its
  publication `clock <= last_commit` for its `(binding, journal, producer)`
  entry in the Frontier (`crates/shuffle/src/log/reader/scan.rs`,
  `build_visibility_index`). No wall clock is involved. Read delays
  (`crates/shuffle/src/binding.rs`, `read_delay`) are applied in the
  shuffle *service* path, which the harness bypasses — so **read-delay
  semantics are entirely the harness scheduler's responsibility**.
- **Fixture segment writer** — `crates/flowctl/src/preview/fixture.rs`.
  Writes `shuffle::log` segments directly: per source binding, it stamps a
  synthetic UUID from a monotonic `uuid::Clock` (producer
  `FIXTURE_PRODUCER`), validates, packs the shuffle key, and builds a
  Frontier whose per-(journal, binding) producer `last_commit` is the
  transaction's max clock. Currently **single-shard only**. Publication
  clocks advance globally across sessions so prior documents are never
  re-admitted. `Frontier.flushed_lsn` is already a per-shard `Vec`.
- **Preview drive loops** — `crates/flowctl/src/preview/{services.rs,
  driver.rs, derive_driver.rs, mod.rs}`. These drive the session protocol
  end-to-end for one task (tonic server hosting `runtime_next::Service` +
  leader, N synthetic shards, RocksDB/shuffle tempdirs). The harness
  generalizes this to many resident derivations.
- **Connector lifecycle** — `crates/runtime-next/src/shard/derive/
  {startup.rs, connector.rs}`: the connector (image container / local
  process / in-process derive-sqlite) starts during **session** startup and
  lives for the session. TypeScript derivations run as
  `ghcr.io/estuary/derive-typescript` image containers — expensive to
  start, which is why tests keep sessions (and thus containers) alive for
  the whole run and reset state in-band.
- **Reset support** — `derive.Request.Reset`
  (`go/protocols/derive/derive.proto` ~line 186) is a first-class,
  test-only connector message with no response; derive-sqlite implements
  it (`crates/derive-sqlite/src/connector.rs`, swaps in a fresh `:memory:`
  DB), as does derive-typescript. runtime-next currently has **no
  mechanism to send it** — Phase 1 adds one.
- **State patch wire format** — `crates/runtime-next/src/patches.rs`.
  A leading `null` patch means full replacement; used to persist a cleared
  connector state during Reset.

## Settled design decisions

1. **Faithful port of the Go graph algorithm** (not a simplified
   scheduler). It is small, proven, and its `ContainsClock` /
   `ReadyAt` machinery is required anyway for self-cycles and read
   delays. Clocks become offsets into the harness's in-memory collection
   store; single-process execution makes "Stat" synchronous and exact.
2. **Resident tasks + protocol-level Reset.** All derivations reachable
   from the test's ingested collections run as resident runtime-next
   sessions for the whole test run (containers stay warm, matching V1).
   Between test cases the harness drives a new `Reset` flow through the
   session protocol (Phase 1). Do NOT reset by cycling sessions — that
   would respawn connector containers and shifts reset semantics onto
   every connector's Open handling; `Request.Reset` is the narrow,
   already-supported contract.
3. **Multi-shard derivations.** Activate derivations with multiple splits
   (default 3, matching `test_catalog`) to exercise multi-shard data
   layout and catch shuffle/key-routing errors. The harness segment
   writer routes documents to shards by packed shuffle key, reusing the
   shuffle crate's own routing logic.
4. **New crate `crates/runtime-harness`** exposing roughly
   `run_tests(&build::Output, Options) -> Results`, linked by both
   `flowctl` and `control-plane-api`. Logs/progress go through a
   caller-provided sink (the Logger seam composes here), never stdout —
   the agent streams user-visible logs via `logs_tx`.
5. **Only derivations execute.** Captures and materializations are
   validated at build time but never run in tests, and are excluded from
   the graph (matches V1's `NewGraph(nil, collections, nil)`).
6. **`flowctl test`** becomes a new top-level local command (there is no
   Rust `Test` subcommand today; `flowctl catalog test` — a remote
   dry-run publish in `crates/flowctl/src/catalog/test.rs` — is
   unrelated and stays). Catalog build uses
   `local_specs::load_and_validate` (derivation connectors only; capture/
   materialize connector validation is noop'd — tests don't need it).

## Architecture of the harness

The crate has two layers. The `drive` layer is extracted from
`crates/flowctl/src/preview/` and is shared with preview (flowctl depends
on this crate); the test layer is new.

```
crates/runtime-harness
  ├─ drive/          Generic "run runtime-next tasks locally" layer,
  │                  extracted from flowctl::preview (~2,150 lines):
  │   ├─ driver.rs, derive_driver.rs, capture_driver.rs
  │   │              Session-protocol drive loops per task type (already
  │   │              generic over seam factories). Per-task options
  │   │              (sessions, max_transactions, delay, initial-state
  │   │              seeding) become an options struct; preview populates
  │   │              it from CLI flags, tests use defaults.
  │   ├─ services.rs Run: tonic hosting, tempdirs, debug port —
  │   │              parameterized over the caller's ShuffleSessionFactory
  │   │              (replacing preview's hardcoded factory enum).
  │   ├─ shards.rs   Synthetic shard topology (key-range splits).
  │   └─ segments.rs The lower half of preview's fixture.rs: the
  │                  segment/frontier writer (write_transaction, clock
  │                  stamping, key packing, frontier assembly) and the
  │                  channel-fed ShuffleSessionFactory
  │                  (FixtureOpener/FixtureCheckpoints). Generalized to
  │                  N shards with key-routing. This is the single shared
  │                  implementation of the durable-before-frontier and
  │                  clock-monotonicity invariants; both preview and the
  │                  test runner feed it.
  ├─ graph.rs        Port of go/testing/graph.go + clock.go.
  │                  Clocks: BTreeMap<(collection, partition), offset>
  │                  plus per-(task, transform) read suffixing as in V1.
  ├─ store.rs        CollectionStore: per (collection, logical partition)
  │                  append-log of committed docs + publication clocks.
  │                  Ingest and TestPublisher append; Verify and the
  │                  segment feeder read. Persists across test cases
  │                  within a run (parity: journals persist in V1).
  ├─ shuffle.rs      TestShuffle: thin wrapper over drive/segments.rs's
  │                  channel-fed ShuffleSessionFactory, one per
  │                  derivation. To execute a "Stat", the harness writes
  │                  pending source docs into the task's per-shard
  │                  shuffle-log directories (key-routed) and sends one
  │                  Frontier, then awaits the transaction's commit.
  ├─ publish.rs      TestPublisher: buffers publish_doc() per transaction;
  │                  on commit (commit_intents/write_intents) appends to
  │                  the CollectionStore with publication clocks from the
  │                  harness's global synthetic clock. publish_stats is a
  │                  no-op (or trace). update_clock() ticks the clock.
  ├─ runner.rs       Task runners: generalization of preview's
  │                  derive_driver — per derivation, a tonic server slot,
  │                  N shard streams + leader, RocksDB/shuffle tempdirs,
  │                  SessionLoop kept open for the whole run.
  ├─ steps.rs        Ingest (combine-by-key via doc::combine, partition
  │                  routing, append) and Verify (fetch window since
  │                  test-case start, partition-selector filter,
  │                  combine-by-key, compare).
  ├─ diff.rs         Comparator: superset match + scaled-epsilon floats +
  │                  UUID masking, with readable (colored) failure
  │                  rendering and FailedVerifies-equivalent reporting.
  └─ lib.rs          run_tests(): builds the graph from built specs,
                     starts runners for reachable derivations, executes
                     sorted test cases with Reset between them, returns
                     structured results.
```

Clock domains are unified: the graph's synthetic `TestTime`, the
`uuid::Clock` stamped on ingested/derived documents, and Frontier
`last_commit` values all derive from one global monotonic synthetic clock
(seeded like the fixture writer's `Clock::from_unix(1, 0)`). `Advance`
bumps it by the pending stat's delta. Read delays are implemented exactly
as V1's `ReadyAt`: delayed documents are simply not fed (not written into
a segment / not covered by a Frontier) until synthetic time reaches
`publication clock + delay`. Never rely on wall clock.

Multi-shard: shard topology is synthesized as in preview (`preview/
shards.rs`) with even key-range splits. The segment writer routes each
document per binding: extract the packed shuffle key (already done in
`fixture.rs`), map key-hash → owning shard range **using the same routing
function the shuffle crate's slice actors use** (find and reuse it; do not
reimplement). `shuffle: any` bindings must be distributed (e.g.
round-robin) rather than pinned to shard 0. Each shard gets its own log
directory and LSN; the Frontier assembles per-shard `flushed_lsn`.

## Implementation phases

Each phase is independently landable and verified. Run `cargo fmt` after
Rust changes.

### Phase 1 — `Reset` in the runtime-next session protocol

Add a Reset flow for **derive sessions only**, modeled on `Stop`:

1. `go/protocols/runtime/runtime.proto`: add to the `Derive` oneof:
   - `Reset` (Controller → Shard → Leader): request a connector reset.
   - A completion confirmation (Leader → Shards → Controller); either a
     distinct `ResetDone` or the same message flowing back, mirroring how
     `Stop`/`Stopped` pair. Pick clean field numbers; document direction
     conventions in comments like neighboring messages.
   - Regenerate: `mise run build:go-protobufs` and
     `mise run build:rust-protobufs`.
2. Shard side (`crates/runtime-next/src/shard/derive/actor.rs`): forward
   controller `Reset` to the leader (as `Stop` is forwarded). On receiving
   the leader's reset broadcast, forward `derive::Request{ reset }` on the
   established `connector_tx` (fire-and-forget; the connector protocol
   defines no response — per-stream ordering guarantees the connector
   processes it before any subsequent transaction's reads), then confirm
   to the leader.
3. Leader side (`crates/runtime-next/src/leader/derive/`): sequence the
   reset at a transaction boundary — Head FSM idle, Tail done, no
   transaction in flight (add a `reset_requested` input analogous to
   `close_requested` / `stopping`). Then:
   - broadcast reset to all shards; await confirmations;
   - clear leader-accumulated connector state and run a Persist cycle
     writing a null-replacement state patch (see `patches.rs`) so RocksDB
     agrees the state is empty (this is deliberately stronger than V1,
     which left persisted state stale);
   - confirm completion to the controller (via shards).
   If an out-of-band Persist cycle proves awkward in the FSM, folding the
   cleared state into the next transaction's Persist is acceptable, but
   the immediate persist is preferred (Reset is then durable standalone).
   Read frontiers must NOT be cleared — only connector state.
4. Tests: extend the existing FSM/actor snapshot tests
   (`fsm.rs`, `actor.rs` have inline test walk-throughs — follow that
   pattern) to cover: reset while idle; reset requested mid-transaction
   (deferred to the boundary); reset then further transactions. Verify
   derive-sqlite actually clears state across a Reset in an integration
   test if practical.

Materialize and capture messages are untouched.

### Phase 2 — harness crate: graph + scheduler port

Create the crate with the pure-logic core, no runtime-next dependency yet:

1. Port `graph.go`, `clock.go`, `action.go` to `graph.rs` (+ a
   `Driver`-like trait for Stat/Ingest/Verify/Advance so the loop is
   testable with a mock, as in `action_test.go`).
2. Port `graph_test.go`, `clock_test.go`, `action_test.go` fixtures as
   Rust unit tests (prefer `insta` snapshots per repo convention). These
   encode the semantics that matter: cascading stats, self-cycles
   terminating via `ContainsClock`, read-delay `ReadyAt` scheduling, lazy
   time advancement, `HasPendingWrite` BFS gating.
3. Build graph construction from built specs
   (`build::Output.built.built_collections` — derivations are collections
   with `derivation` set; skip disabled shard templates as V1 does).

### Phase 3 — seams, store, and driving one derivation

1. **Extraction first, behavior-preserving.** Move the `drive` layer
   (inventory in the Architecture section) out of
   `crates/flowctl/src/preview/` into the harness crate and migrate
   preview onto it **in the same change**, so no divergent copy ever
   exists. What stays in `flowctl::preview` (~1,200 lines): the CLI
   surface and orchestration in `mod.rs` (catalog build via
   `local_specs`, task resolution, flags), the stdout `PreviewPublisher`,
   the `PreviewLogger` (`--output-state`/`--output-apply` patch
   decoding), and the fixture *file format* — NDJSON parsing and
   eager/streaming session planning — which feeds the shared writer.
   Preview has no CI coverage; verify by running the `tests/soak/`
   fixtures through `flowctl preview` before and after the move (note:
   soak READMEs still reference the old `raw preview-next` name — update
   them in passing).
2. Generalize the shared segment writer to N shards (key-routing as
   described above). Lift preview's `--fixture` `--shards 1` restriction
   as part of this — the routing sits below the file-parsing layer
   preview keeps, so multi-shard fixture preview falls out of the shared
   writer (nice-to-have; deprioritize if it drags, but don't fork the
   writer to avoid it).
3. Implement `CollectionStore`, `TestPublisher`, `TestShuffle`, and the
   runner that keeps a derivation session resident.
4. Integration test (in-crate, no containers): a small catalog of
   derive-sqlite derivations, including a multi-hop chain and a
   self-cycle. Drive ingest → stat cascade → verify by hand; snapshot the
   store contents. Add a multi-shard case (3 splits) asserting documents
   route to distinct shards and combine correctly. While here, add a
   derive-sqlite smoke test of the drive layer itself (the extraction
   makes preview's machinery testable without a CLI invocation for the
   first time).

### Phase 4 — full test runner + `flowctl test`

1. Implement `run_tests`: load built `TestSpec`s
   (`build::Output.built.built_tests`), sort by step scope, start runners
   for derivations reachable from ingested collections, execute cases
   with the ported loop, send Reset between cases, collect structured
   results (pass/fail counts, per-failure diffs, optional snapshot dir of
   actual documents on failure — parity with `--snapshot`).
2. Ingest: combine fixture docs by collection key **before** appending
   (V1 parity — affects derivation input cardinality); route to logical
   partitions (reuse partition-field extraction from `assemble`/`doc`);
   validation of fixture docs already happened at build time
   (`crates/validation/src/test_step.rs`).
3. Verify: window is `(test-case-start offsets, current offsets]` per
   partition; apply the step's partition `LabelSelector` against the
   store's logical partitions; combine by key with the read schema; use
   the `diff.rs` comparator.
4. Add the `flowctl test` subcommand: `--source`, `--snapshot`,
   `--network`, log flags; builds via `local_specs::load_and_validate`;
   exits non-zero on failure with V1-style colored summary output.
5. Verification: `cargo run -p flowctl -- test --source examples/flow.yaml`
   must pass, and results must agree with
   `~/go/bin/flowctl-go test --source examples/flow.yaml`
   (the current `mise run ci:catalog-test`). The examples include
   TypeScript derivations, so this exercises image containers, read
   delays (citi-bike), and multi-hop chains. Also verify a deliberately
   broken expectation produces a readable diff.

### Phase 5 — control-plane linkage

1. Replace `test_catalog` in
   `crates/control-plane-api/src/publications/builds.rs`: drop the
   temp-data-plane spawn, socket waiting, activation loop, and
   `flowctl-go api test` shell-out; call the harness crate directly with
   the already-built `build::Output`, 3-way splits, and a log sink that
   feeds `logs_tx` (user-visible job logs — keep formatting comparable).
2. Remove the `flowctl_go` path threading if nothing else uses it
   (check `crates/agent/src/main.rs` and `publications/mod.rs`; the
   builds path may still shell out for other steps — verify before
   removing).
3. Verification: agent integration tests
   (`crates/agent/src/integration_tests/`) covering publications with
   tests, including a failing test surfacing errors to the publication.

### Phase 6 — deletion and CI switchover

1. Update `mise/tasks/ci/catalog-test` to invoke
   `cargo run -p flowctl -- test --source examples/flow.yaml` (or the
   built binary consistent with sibling CI tasks).
2. Delete, after confirming no remaining users (grep first):
   - `go/testing/` (graph, driver, action, clock + tests),
   - the Testing RPC service: `go/runtime/testing.go`, the `Testing`
     service + `IngestRequest`/`AdvanceTimeRequest`/`ResetStateRequest`
     messages in `go/protocols/flow/flow.proto` (regen protobufs), and
     the `--flow.test-apis` consumer flag,
   - `flowctl-go` commands `test` / `api test` (and `temp-data-plane`
     only if nothing else uses it — it may serve other local workflows;
     check `local/` and CI),
   - `ClearRegistersForTest` in `go/runtime/derive.go`.
3. Keep `derive.Request.Reset` in the connector protocol — it is the
   mechanism the V2 flow drives.
4. Update READMEs: the harness crate needs one (purpose, key types,
   entry points); touch `crates/runtime-next/README.md` for the Reset
   flow and `go/` docs that reference the old test flow.

## Parity checklist (acceptance criteria)

- [ ] Examples suite passes with results identical to `flowctl-go test`.
- [ ] Verify combines documents by key (read schema) before comparison;
      one document per key.
- [ ] Superset match: actual documents may carry extra fields.
- [ ] Numbers compare with scaled epsilon (port `compareNumbers`).
- [ ] Document UUIDs are masked before comparison.
- [ ] Expected docs are consumed in collection-key order (build-time
      validated; the comparator walks both lists in order as V1 does).
- [ ] Verify only sees documents written during the current test case.
- [ ] Verify honors partition selectors over logical partitions.
- [ ] Ingest combines by key before writing.
- [ ] Read delays gate via synthetic time; no wall-clock sleeps anywhere.
- [ ] Self-referential derivations reach fixed-point (ContainsClock).
- [ ] Multi-hop chains fully quiesce before verify (HasPendingWrite).
- [ ] Reset between test cases clears connector state (and, improving on
      V1, persisted state) but not read frontiers or collection data.
- [ ] Connector containers start at most once per derivation per run.
- [ ] Derivations run with 3 splits; documents route by shuffle key.
- [ ] Test cases execute sorted by step scope.
- [ ] Failure output: readable colored diffs; optional snapshot dir;
      non-zero exit; agent path streams failures to publication logs.

## Open items for the implementer

- Exact `Reset`/`ResetDone` message shape and field numbers in the
  `Derive` oneof; whether materialize gets reserved numbers for symmetry.
- Locate the canonical key-hash → shard-range routing function in
  `crates/shuffle` (slice actor path) and expose it for reuse; decide
  `shuffle: any` distribution (round-robin suggested).
- Whether the leader's Reset persist is an immediate cycle or folded into
  the next transaction's Persist (immediate preferred).
- The preview split is settled (see Architecture and Phase 3): the
  `drive` layer moves, preview keeps its CLI surface, seam impls, and
  fixture file format. Whether `drive` eventually becomes its own crate
  separate from the test runner is left open — start with one crate
  (`runtime-harness`), two modules; if it later splits, the natural
  naming is `runtime-harness` (drive layer) + `catalog-tests` (test
  runner). Split only if dependency weight in `control-plane-api`
  becomes a concern.
- `doc::diff` exists but is exact-match oriented; decide whether to
  extend it or write the comparator fresh in the harness crate.
- Stats: V1 tests ignore ops stats; `publish_stats` should no-op, but
  consider tracing them for debuggability.
