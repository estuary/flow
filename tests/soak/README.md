# V2 runtime soak test

Continuous soak-test infrastructure exercising the full V2 runtime
(`plans/runtime-v2/plan.md`) end to end, under the `test/soak/` prefix: a capture
emitting a self-checking workload, the `accounts` derivation that reconstructs and
checks it, and two materializations of that result — `materialize-postgres` (`views`)
for at-rest verification and a standalone Python connector (`ledger`) that re-verifies
the same invariants in flight while driving the materialize-specific V2 joints (Loads,
max-keys, connector-state scatter/gather).

## The workload

`source-soak` models a population of **accounts** in its shard's key range and emits
**double-entry transfers** between them. Every document is a self-describing checkpoint
of one account's state, so any lost, duplicated, reordered, or causally-torn document
surfaces as a downstream contradiction. Each document carries (see
`capture/events.schema.json` for the field-level contract):

- **`id` + `seq`** — the collection key `[/id, /seq]` makes an append-only log; a gap or
  duplicate in an id's `seq` is a direct exactly-once violation.
- **`set`** — one `add`/`remove`/`intersect` op over members `a..h` (an 8-bit mask),
  replayed downstream. `set` is associative but **not commutative**, so it also probes
  in-order reduction. (`SET_MEMBERS` is the one hand-synced copy of the schema's enum.)
- **`balanceDelta` + `transfer`** — one leg of a matched pair (`S`'s `−amount`, `R`'s
  `+amount`). Since every transfer is matched, **`Σ balanceDelta` over all accounts is
  always zero** — a global conservation invariant.
- **`oracle`** — the connector's authoritative post-event truth (`set`, `balance`,
  `seq`) for this id; downstream reductions must converge to it.

### Account windows, splits, and cross-collection routing

An account `id` is an integer in `[key_begin, key_begin + idRange)`, where `key_begin` is
the shard's owned key range (from shard labels when published; an even u32 split under
`preview-next --shards N`). Transfers stay within a shard's window, so each shard's
conservation is self-contained. On a shard **split** both children fork the parent's
state but own disjoint windows: the low child keeps `key_begin` and its accounts; the
high child gets a fresh window and (after pruning inherited out-of-window ids on `Open`)
starts new accounts at `seq 0`. No account is ever emitted by two shards.

Each document routes to `id % len(collections)`, so every event of a given account lands
in a **single** collection. Estuary guarantees strict key+clock ordering *within* a
collection but **not across** collections — within a capture txn the runtime assigns UUID
clocks in combiner `(binding, key)` order, not emission order. So the order-sensitive
`set` reduction is well-defined only because an id's events stay in one collection. The
**cross-collection** probe is instead **conservation**: a transfer's two legs are
distinct ids that generally route to different collections, so `Σ balanceDelta = 0`
requires reading both at a causally-consistent cut within one transaction. A torn read
(one leg committed without the other) breaks it — a signal resting only on
**transactional atomicity**, which Estuary *does* guarantee across collections.

## Layout

One file per task, each under a component directory that also homes that task's
connector. Every file imports just the upstream specs it sources from, so each can be fed
to `flowctl raw preview-next` alone, building only what it needs. Full chain: `source`
(capture) → `events/{alpha,beta,gamma}` → `accounts` (derivation) → `{views, ledger}`
(materializations); every task carries `enable-runtime-v2`, and top-level `flow.yaml`
imports all of them for a whole-chain publish.

- **`capture/`** — `flow.yaml` (`test/soak/source` plus the three `events/*` it writes,
  `local:`, no image); `source_soak/` (the Pydantic-only connector — `models.py` wire
  types + `EndpointConfig`, `__main__.py` serve loop / `Accounts` oracle / transfer
  producer); `events.schema.json` (the **single source of truth** for the wire contract,
  loaded by the connector *and* referenced by every events collection); `source-soak`
  (poetry-venv launcher).
- **`derivation/`** — `flow.yaml` defines `accounts` and imports `../capture/flow.yaml`.
  Its TypeScript module (`accounts.ts`) and schema (`accounts.schema.json`) live at the
  soak root.
- **`materialization/`** — `views.flow.yaml` (`materialize-postgres`) and
  `ledger.flow.yaml` (`local:` soak connector), each importing `../derivation/`, plus
  `flow.yaml` importing both. `materialize_soak/` is the sibling connector;
  `materialize-soak` is its launcher.

The wrappers are self-locating, since the reactor spawns `local:` connectors from `$HOME`.

## The capture and its state

State is **global** (per account) and persisted as a merge-patch of the ids touched since
the prior checkpoint:

```json
{ "seq": {"<id>": <next-seq>}, "mask": {"<id>": <0..255>}, "balance": {"<id>": <signed>} }
```

On `Open` the connector resumes from this state and prunes out-of-window ids, so emission
continues without gaps or oracle divergence across restarts, crashes, spec updates,
disable/enable, and splits. (It requests explicit acknowledgements only to sanity-check
the checkpoint→commit→ack join; resumption rests on state, not on acks.)

## Downstream: the `accounts` derivation

`accounts` (`accounts.ts`) is an **active TypeScript verifier**: it reconstructs each
account *inside* the derivation and compares to the connector's `oracle`, so a
contradiction is signalled at the source rather than inferred from reduced output. It
unions the three event logs on `/id` via three transforms and runs three probes:

1. **Union + in-order reduction** — replaying `set` ops in delivery order reconstructs
   membership; correctness proves within-collection `seq`-order delivery (a reorder
   surfaces as `reconstructed.set ≠ oracle.set`).
2. **Expected-vs-oracle (stateful, cross-session)** — each read reduces into a persisted
   per-account accumulator (`seq`, membership, balance) compared to the event's oracle.
   The accumulator round-trips through connector state, so the **`seq`-contiguity check
   spans session boundaries** — the exactly-once probe.
3. **Conservation (stateless, per-txn)** — each shard sums `balanceDelta` over the events
   it processed *this transaction* (reset each txn, never cumulative). A two-round `Flush`
   scatter/gather sums every shard's per-txn delta and asserts the global is **exactly
   zero**. A transfer's legs generally hash to different shards (and live in different
   collections), so this is simultaneously the cross-shard and cross-collection
   torn-read probe.

### State, sharding, and the Flush handshake

State holds two namespaces in one task-level singleton:

```jsonc
{ "accounts": {"<id>": {"seq": <int>, "set": [...], "balance": <int>}},
  "deltas":   {"<keyBegin>": <int>} }   // per-txn; written each txn, ignored on Open (cruft)
```

- **`shuffle: {key: [/id]}` on every transform is load-bearing** — it gives each id a
  single owning shard so the accumulator is coherent. `shuffle: any` shreds it;
  `shuffle: lambda` is rejected by derive-typescript.
- **`accounts` is keyed `/id`, stateful and persisted.** The leader broadcasts the full
  task-level state to every shard on `Open`, so each loads the union of all ids; ids it
  doesn't own are inert (never read, never re-persisted), which makes the map split-safe.
  **Never sum this map for conservation** (it double-counts inert entries) — use the
  per-txn delta.
- **Two-round Flush.** Round 1 publishes the touched docs and returns
  `deltas: {[keyBegin]: delta}` with `more: true`. Round 2 receives the concatenated array
  of every shard's round-1 state, sums all `deltas` to a `global`, and — if non-zero —
  publishes a violation sentinel, emits an ERROR, and returns `more: false`. The array is
  **concatenated, not merged** (`patches.rs` `extend_state_patches`;
  `leader/derive/fsm.rs`); the accounts patch is contributed in round 1 only (every
  iteration's state is persisted, so contributing it twice would merge twice).

### Output and signalling

Reconstruction docs `{id, seq, set, balance, oracle, ok, mismatch?}` and per-shard
violation sentinels share the `[/id]` keyspace. Sentinels use a **negative id**
(`-(keyBegin+1)`) so they never collide. Root reduce is `lastWriteWins`: each id is
published by one owning shard in increasing-`seq` order, so the latest write is the
highest-`seq` truth. Violations are **published and logged, never thrown** — the soak
keeps running and accumulates evidence.

Design points worth keeping in mind:

- **Per-txn delta, not cumulative — strictly sharper.** A cumulative sum would
  self-correct and mask a transient defect (e.g. one capture txn split across two
  derivation txns); a per-txn delta that must be zero *every* txn catches it at once.
- **Exactly `== 0`, no tolerance.** V2 never splits a capture transaction's documents
  across derivation transactions, so both legs are always present together.
- **Splits Just Work.** A split subdivides `[keyBegin, keyEnd)` into covering child
  ranges, so every id still routes to one child; the `/id`-keyed map is split-safe (a
  migrated id's accumulator is already in the broadcast state) and `deltas` are
  self-contained. Exercising a mid-run split is deferred validation.
- **Built-in connector image is resolved at build time**, so Validate and the runtime
  compile against the same interface. `derive-image-tag: local` pins the locally-built
  `:local`; otherwise an `enable-runtime-v2` task resolves to `:stable`, a V1 task to
  `:dev` (`crates/validation/src/derivation.rs`).

## Downstream: the PostgreSQL materialization (`test/soak/views`)

Materializes `accounts` into PostgreSQL two ways (one runtime-next task,
`enable-runtime-v2`, single shard), turning the derivation's in-flight checks into
queries against tables *at rest*:

- **`soak_accounts` — standard (merge).** One row per id holding the latest
  reconstruction. `Σ balance` over real ids is exactly 0 (both legs reduce in one
  derivation txn; the materialization commits whole txns; MVCC only ever shows a
  committed, balanced snapshot).
- **`soak_accounts_delta` — delta-updates.** Skips loads and **appends** each txn's
  reduced doc — a per-account history. The latest row per id reconstructs the standard
  view, and oracle matching holds on **every** historical row.

The connector projects every field as a column, flattening nested objects with `/`
(`"oracle/balance"`, `"mismatch/..."`, etc. — double-quote the `/` names in SQL). The
standard table has an `id` PK and a `flow_document` (`json`); the delta table has neither
(delta-updates never loads or reduces). A defect anywhere downstream surfaces as a
non-zero balance sum, an `ok = false` row, a sentinel (negative id), or a standard/delta
disagreement. (`ok` is the derivation's own verdict; the queries below also re-derive
oracle matching from the columns, catching a divergence introduced *by* materialization.)

## Downstream: the soak materialization (`test/soak/ledger`)

A standalone Python `local:` connector materializing `accounts` a second way, checking
the invariants in flight inside the connector and over the materialize transaction loop —
the Load path, max-keys, and the connector-state scatter/gather. It is
**recovery-log-authoritative with no external store** — the materialized table *is* its
connector state: per-id docs live in memory, persist as a merge patch in each
`StartedCommit` (landing in shard zero's RocksDB via the leader's Persist), and recover
whole on `Open`.

It sources `accounts` (keyed `/id`), not the raw `events/*` (keyed `[/id, /seq]`):
a materialization shuffles on its source key and can't re-key, so events would scatter an
id across shards and shred per-account state. State is the store:

```jsonc
{ "standard": {"<id>": <full reduced doc>},   // served on Load; baseline for the per-txn delta
  "deltas":   {"<keyBegin>": <signed delta>} } // scatter cruft, overwritten each txn, ignored on Open
```

On `Open` the leader broadcasts full task-level state (`standard` = union of all ids;
unowned ids inert and split-safe; **never sum it** — use the per-txn delta). Memory is
bounded by the **account population**: `standard` is one entry per id, and per-txn working
sets are dropped at `StartedCommit`.

### The V2 joints it exercises

| Joint | How |
| --- | --- |
| **Load / Loaded / max-keys** | The standard binding serves real Loads; with the load optimization on (default) the runtime suppresses Loads via max-keys and the connector probes the `exists` flag against its store. `forceLoads: true` (→ `Opened.disableLoadOptimization`) forces a Load per key. |
| **Three-phase state scatter/gather** | `Flushed` / `StartedCommit` / `Acknowledged` each scatter a `ConnectorState`; the runtime concatenates all shards' contributions (tab-framed JSON array, *not* a deep merge — `patches.rs`) and feeds them back at the next phase. Conservation uses the `StartedCommit → Acknowledge` round. |
| **Shard-zero consolidation** | The `standard` patch flows through the leader's Persist into shard zero's RocksDB; non-zero shards have no recovery log and acquire state via the `Open` broadcast. |

### The probes

1. **Conservation (cross-shard, per-txn).** During Store the standard binding sums
   `Σ(stored − loaded prior balance)` over its ids — needing both the **Load** and the
   **gather**. The delta is scattered in `StartedCommit`; at `Acknowledge` the connector
   sums every shard's delta and asserts the global is **exactly zero**. This gather is
   **post-commit** — the materialize protocol's earliest same-txn cross-shard gather is at
   `Acknowledge` (`Flushed` fires before the Stores). So the connector *detects* a
   violation immediately after the offending commit and logs it; it never blocks the
   commit (soak philosophy: accumulate evidence, keep running).
2. **Oracle integrity (per doc).** A faithfully transported doc is self-consistent: its
   recomputed oracle match equals its own `ok`. Disagreement = a materialization defect
   (ERROR); a self-consistent `ok = false` is an upstream defect (relayed WARN).
3. **seq monotonicity at the sink** — `lastWriteWins` must only advance an id's `seq`.
4. **`exists`-flag (max-keys)** — the runtime must not claim a key exists the connector
   can't serve.
5. **Standard vs delta agreement** — the two bindings must converge doc-for-doc each txn
   (`lastWriteWins`, so both resolve to that txn's highest-`seq` doc).
6. **Sentinel relay** — a negative-id derivation sentinel reaching the sink re-surfaces as
   ERROR.
7. **Exactly-once across sessions** — `standard` is persisted and recovered, so the
   monotonicity and oracle checks span restart / crash / spec-update / split.

There is no published collection or SQL table, so violations surface as **structured
ERROR ops logs** (`source: materialize-soak`). Verify a run by grepping its ops logs for
`ERROR`.

## Schema inference

The soak is *gated* on schema inference: the `test/soak/events/*` collections take an
inferred `readSchema`, so `accounts` and everything downstream cannot read a document until
inference has propagated end to end. **The chain running at all is the proof it works** — if
inference breaks, `accounts` never comes up.

### Design

Each `events/*` collection keeps `events.schema.json` as its **writeSchema** and adds a
**readSchema** of `allOf: [{$ref: flow://write-schema}, {$ref: flow://inferred-schema}]`.
Inference propagates six hops: runtime emits `inferred schema updated` → L1 rollup
(`ops/rollups/L1/public/<data-plane>/inferred-schemas`) → L2 → the `stats-view`
materialization writes the `inferred_schemas` control-plane table → a DB trigger schedules
the collection controller → it republishes the collection with the real schema inlined.

`source-soak` marks the stable fields (`id`, `ts`, `set`, `oracle`) as sourced via a partial
`SourcedSchema` and leaves the growing ones (`seq`, `balanceDelta`, `transfer`) to doc-driven
inference, so those bounds churn as magnitudes grow while the sourced fields stay put (see
`source_soak/__main__.py` for how and why). `test/soak/accounts` keeps an explicit schema and
produces no `inferred_schemas` row — both expected; see `derivation/flow.yaml`.

### Lifecycle — churn is expected

On a fresh publish, `flow://inferred-schema` is a placeholder that fails every read, so
`accounts` **crash-loops for ~1–3 minutes** until the first inference lands, then recovers.
After that, widening windows recur: as doc-inferred bounds cross 10× brackets the schema
widens, `accounts` briefly fails read validation, the controller republishes, and the task
recovers. Early on `transfer.from`/`transfer.to` (account ids across `[0, idRange)`) churn
the most. The exactly-once and conservation checks hold across every cycle — a
spec-update-under-load probe.

The healthy signature during any window is `source document of transform ... is invalid`.
Any *other* error, or a window that never closes, is a defect.

### Verification

```bash
# FLOW_PG_URL is ambient via mise (this stack's Postgres).
PG() { psql "$FLOW_PG_URL" -tAc "$@"; }

# Inferred-schema rows: expect exactly the three events/* (NOT accounts — see above). None
# at all ⇒ the runtime→L1→L2→materialize path is broken (the motivating bug class); rows
# present but accounts never converges ⇒ the controller-republish / downstream-activation hop.
PG "SELECT collection_name, md5 FROM inferred_schemas WHERE collection_name LIKE 'test/soak/%';"

# Built read schema of an events collection, once converged:
PG "SELECT built_spec->'readSchema' FROM live_specs WHERE catalog_name = 'test/soak/events/alpha';"
#   - must NOT contain 'inferredSchemaIsNotAvailable' (placeholder gone)
#   - 'seq' under flow://inferred-schema has a 'maximum' (doc-inferred, widened)
#   - 'id' and 'oracle/seq' have no bounds (sourced, stable)
```

(The L1 collection `ops/rollups/L1/public/<data-plane>/inferred-schemas` carries the same
`test/soak/*` documents, but reading it needs `ops/` access — use the `inferred_schemas`
table above, which `stats-view` materializes it into.)

### Caveats and knobs

- **Preview ordering.** `flowctl preview` (the profile is ambient via mise) of the derivation or ledger inlines
  the inferred schema from the control plane, so it only works **after** the published stack
  has converged; on a fresh stack it inlines the placeholder and every read fails. Capture
  preview is unaffected (captures don't read). Re-publishing `test/soak/` bumps the collection
  generation and discards stale inference, so the loop re-runs from the placeholder.
- **Forcing widening cycles.** With defaults, bracket crossings take hours. Lower `idRange`
  (e.g. to `50`) to concentrate events on fewer accounts and force crossings within minutes.

## Setup

The poetry project lives at the **repo root** (not here in `tests/soak/`). Install Python
dependencies into its in-project venv (`.venv`, gitignored; the root `poetry.toml` pins it
in-project so the wrapper finds it):

```bash
# From the repo root:
poetry install --no-root
```

**Materialization target.** `views` writes to the local stack's own Supabase Postgres —
addressed by the stable network alias **`db`** (db/user/password all `postgres`) — in a
dedicated `soak` schema, wiped by `supabase db reset` on each stack start (ephemeral —
re-published next run). It needs no extra wiring: both at Validate time and at runtime the
connector runs on the stack's `supabase_network_<stack>` Docker network and reaches `db:5432`
directly — see `local/README.md` ("Connectors run on the Supabase Docker network").

## Running

`FLOW_STACK_DIR`, `FLOWCTL_PROFILE`, and `flowctl` itself are ambient inside the checkout
via mise (see `local/README.md` if your shell lacks them).

```bash
# One-time per stack reset: (re-)provision the `test` tenant, then publish the chain.
mise run local:test-tenant --tenant test --user alice@example.com
set -a; . "$FLOW_STACK_DIR/test-tenant-test.env"; set +a
flowctl catalog publish --source tests/soak/flow.yaml --auto-approve

# Read connector ops logs (set SSL_CERT_FILE first, or you'll see TLS UnknownIssuer):
export SSL_CERT_FILE=~/flow-local/ca.crt
flowctl logs --task test/soak/ledger --since 30m | grep -i error

# Read a task's per-transaction ops stats:
flowctl raw stats --task test/soak/accounts --since 5m -o json
```

A published capture runs continuously. Disable it by republishing with
`shards: {disable: true}`, or `flowctl catalog delete --prefix test/soak/
--dangerous-auto-approve` (the flag skips the interactive confirmation, which
aborts in non-TTY shells).

### Verifying

#### Reported Violations

Derivation `test/soak/accounts` and materialization `test/soak/ledger` both self-verify
report violations through their task logs. `../accounts` additionally reports violations
as published documents.

#### Materialized Tables

Once `views` is running, the `soak` schema fills in the stack's Supabase Postgres (`db`);
query it through the host-mapped port. All checks hold *continuously* (MVCC always hands a query a
committed, balanced snapshot), so there's no need to pause the capture.

```bash
PG() { psql "$FLOW_PG_URL" -tAc "$@"; }

# 1. Conservation (standard): real balances sum to exactly 0.
PG "SELECT COALESCE(SUM(balance),0) FROM soak.soak_accounts WHERE id >= 0;"             # => 0

# 2. Oracle-vs-computed (standard): every reduced {seq,set,balance} equals the oracle.
PG "SELECT count(*) FROM soak.soak_accounts WHERE id >= 0
       AND (ok IS NOT TRUE OR balance <> \"oracle/balance\"
         OR seq <> \"oracle/seq\" OR (\"set\")::jsonb <> (\"oracle/set\")::jsonb);"      # => 0

# 3. No violation sentinels (negative id) reached the sink.
PG "SELECT id, \"violation/sum\" FROM soak.soak_accounts WHERE id < 0;"                 # => (none)

# 4. Conservation (delta): latest balance per id, then sum.
PG "SELECT COALESCE(SUM(balance),0) FROM
       (SELECT DISTINCT ON (id) id, balance FROM soak.soak_accounts_delta
          WHERE id >= 0 ORDER BY id, seq DESC) latest;"                                # => 0

# 5. Oracle-vs-computed on EVERY delta row (not just the latest).
PG "SELECT count(*) FROM soak.soak_accounts_delta WHERE id >= 0
       AND (ok IS NOT TRUE OR balance <> \"oracle/balance\"
         OR seq <> \"oracle/seq\" OR (\"set\")::jsonb <> (\"oracle/set\")::jsonb);"      # => 0

# 6. Standard vs delta agree: latest delta snapshot per id matches the standard row.
PG "SELECT s.id FROM soak.soak_accounts s
       JOIN LATERAL (SELECT balance, seq FROM soak.soak_accounts_delta d
                       WHERE d.id = s.id ORDER BY seq DESC LIMIT 1) d ON true
       WHERE s.id >= 0 AND (s.balance <> d.balance OR s.seq <> d.seq);"                # => (none)
```

### Debugging

derive/materialize also register `leader.*` handlers on the sidecar
(`curl -s localhost:${FLOW_SIDECAR_PORT}/debug/handlers.json | jq '.live[].kind'` confirms V2 routing).