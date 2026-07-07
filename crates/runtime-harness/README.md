# runtime-harness

Runs Flow **catalog tests** locally on the V2 `runtime-next` runtime — with no
Gazette broker, etcd, Go consumer, or `flowctl-go` binary. It replaces the
Go-based test flow (`flowctl-go test` + `go/testing/` + the Testing RPC).

Linked by both:
- `flowctl` — the `flowctl test --source ...` command.
- `control-plane-api` — publication tests (replacing the temp-data-plane +
  `flowctl-go api test` shell-out).

Logs and progress flow through a caller-provided sink, never stdout — the agent
streams user-visible logs via `logs_tx`.

## Layers

- **Scheduler** (`clock.rs`, `graph.rs`, `action.rs`) — a faithful port of the
  V1 Go dataflow graph and clock algorithm (`go/testing/graph.go`, `clock.go`,
  `action.go`). Semantics are the source of truth for behavior; the ports keep
  the string-journal conventions and clock reductions verbatim so the reference
  unit tests port over directly.
- **Drive layer** (`drive/`) — extracted from `flowctl::preview` and shared with
  it: the session-protocol drive loops (`driver.rs`, `derive_driver.rs`,
  `capture_driver.rs`), tonic hosting + tempdirs (`services.rs`), the synthetic
  shard topology (`shards.rs`), and the shuffle-log segment/frontier writer plus
  its channel-fed `ShuffleSessionFactory` (`segments.rs`). Generic over the three
  runtime-next host seams (`PublisherFactory` / `LoggerFactory` /
  `ShuffleSessionFactory`); preview and the test runner each install their own.
- **Test seams + runner** — `store.rs` (`CollectionStore`), `partitions.rs`
  (logical-partition routing / labels), `publish.rs` (`TestPublisher`, routes
  derived docs to partition journals in the store), `logger.rs` (`TestLogger`,
  turns the leader's committing `Persist` into the runner's transaction-commit
  signal, and streams ops logs to a caller handler), and `runner.rs`
  (`DerivationRunner`, one derivation resident for the whole run, driven
  Stat-by-Stat, with `reset()` driving the runtime-next Reset flow).
- **Steps + entry point** — `steps.rs` (Ingest combine-by-key + partition route;
  Verify window + partition-selector filter + combine + `diff.rs` compare) and
  `run.rs` (`run_tests`: build the graph, start resident runners, execute test
  cases sorted by scope with a Reset between each, and return per-case outcomes).

## Key types

- `graph::Graph` — the dataflow graph. Tracks **derivations only** (captures and
  materializations are validated at build time but never run during tests, and
  are excluded from the graph — matching V1's `NewGraph(nil, collections, nil)`).
  `Graph::from_built_collections` constructs it from built specs.
- `graph::TestTime` — synthetic test time (nanoseconds), advanced lazily; no
  relation to wall clock. Read delays are realized entirely by the scheduler
  gating stats on `TestTime`.
- `action::Driver` — the abstract executor of Stat / Ingest / Verify / Advance,
  so `run_test_case` is testable with a mock and reusable by the live runner.
- `runner::DerivationRunner` — hosts a derivation's leader + N synthetic shards
  over one long-lived SessionLoop (connector starts once, stays warm) and runs
  exactly one transaction per `stat()`: it feeds the newly-readable source
  documents into per-shard shuffle logs (key-routed by `segments`), pushes one
  checkpoint frontier, and awaits the leader's committing `Persist`.
- `run::run_tests(&tables::Validations, run::Options)` — the entry point linked
  by `flowctl test` and `control-plane-api`. `Options` carries the connector
  network, image-derivation shard count (`splits`, default 3), optional snapshot
  dir, and a clonable `logger::LogHandler` ops-log sink. Returns `TestResults`
  (per-case pass/fail + rendered diffs). The caller installs a process rustls
  crypto provider first (the loopback stack dials over rustls).

## Non-obvious details

- **Remote-authoritative derivations must be single-shard.** derive-sqlite (and
  any connector whose checkpoint lives in its endpoint) reports connector state
  at Opened, which the runtime-next leader rejects on non-zero shards. So
  `run_tests` runs such derivations with one shard and image derivations (e.g.
  derive-typescript) with three splits (matching V1's `test_catalog`). The
  in-crate integration tests are all derive-sqlite, so they validate the runner
  single-shard; multi-shard *routing* is validated at the segment-writer level
  and multi-shard *execution* against the TypeScript examples.
- The runner detects a transaction commit via the leader's `LogEvent::Persist`
  (one committing `Persist` per transaction); derived documents are appended to
  the store during the drain (`publish_doc`), strictly before that signal, so a
  Stat observes all its output resident when its transaction reports done.
- **Feeding is per binding, not per source collection.** A stat's `read_through`
  entries carry the transform's checkpoint suffix, which the runner maps to a
  single binding and feeds only that binding (`segments::write_transaction_for_bindings`).
  So a source read by two transforms with *different* read delays feeds each
  independently — the delayed transform sees a document only when its own (later)
  stat fires, matching a live shuffle read. (Preview keeps the collection-fan-out
  `segments::write_transaction`, which delivers a fixture doc to every binding.)
- **A multi-shard writer authors segments by shard index.** Each shard's reader
  reconstructs its segment filenames as `mem-{shard_index:03}-seg-…`, so
  `ShardWriter::new(dir, shard_index)` must pass the matching index or the reader
  can't find the segments. Ingest / the publisher route to partition journals
  (`{collection}/{field=value}/…/pivot=00`); Verify matches those journals'
  `estuary.dev/field/*` labels against the step's partition selector.

- Clocks (`clock::Clock`) are per-journal offsets. In the harness these index
  the in-memory collection store rather than broker journals, but the reduction
  semantics (`min` / `max` / `contains`) are identical to Gazette's. Read
  progress clocks carry a `;{journal_read_suffix}` suffix per transform.
- `contains_clock` is what terminates self-cycles: a task stops re-stat-ing once
  its read clock contains its own projected write clock.
- `has_pending_write` is a BFS over outputs/readers that gates VERIFY steps until
  a multi-hop chain has fully quiesced.
