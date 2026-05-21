# `flowctl raw preview-next` as a runtime-next E2E harness

This is a hands-on guide for using `flowctl raw preview-next` as a
repeatable end-to-end test of the `runtime-next` + `leader` + `shuffle`
stack against a local Postgres database. It assumes the runtime-v2
branch is checked out and built.

`flowctl preview` is the legacy harness against the existing runtime
crate; the runtime-next harness lives under `raw` while the new stack
is in development. They share most flags and the same test spec format.

**Scope.** `preview-next` runs `runtime-next` + `leader` + `shuffle`
*in-process inside flowctl* (its own tonic server, sharing flowctl's
process-global `service_kit::Registry`). It does **not** go through the
reactor or the runtime-sidecar process. Pass `--debug-port <port>` to
mount the same handler dashboard the sidecar exposes on `--admin-port`,
served loopback-only on `127.0.0.1:<port>/`. Both captures and
materializations are supported (the capture path goes through
`runtime_next::shard::Service::spawn_capture` — leaderless, identical to
what the reactor's `captureAppV2` drives in production; `--shards N`
fans out N independent capture shards in parallel). To exercise the full reactor path (including the sidecar
admin surface), publish a task to a local data plane with
`shards: { flags: { enable-runtime-v2: "true" } }` (the
`estuary.dev/flag/enable-runtime-v2` shard label that `useRuntimeV2` in
`go/runtime/flow_consumer.go` checks); the sidecar's handler dashboard
is then at `http://127.0.0.1:<base_port+61>/`. Note connectors on the
local stack run on the `supabase_network_flow` Docker network — see
`local/README.md` for the endpoint-address implications.

Capture runtime-v2 inside the **reactor** process registers handlers on
the per-shard task service rather than on the Rust runtime sidecar
admin surface; the sidecar dashboard surfaces only Shuffle Leader /
Shuffle handlers. `preview-next --debug-port` is the one place where
capture handlers (kind `shard.capture`) show up on a service-kit
dashboard.

Minimal local capture shape:

```yaml
captures:
  acmeCo/hello-world:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-hello-world:dev
        config:
          rate: 2
    interval: 5s
    shards:
      logLevel: info
      flags:
        enable-runtime-v2: "true"
    bindings:
      - resource:
          name: greetings
          prefix: "Hello {}!"
        target: acmeCo/events

collections:
  acmeCo/events:
    schema:
      $schema: "http://json-schema.org/draft/2020-12/schema"
      type: object
      properties:
        ts:
          type: string
          format: date-time
        message:
          type: string
      required: [ts, message]
    key: [/ts]
```

Useful validation loop after publishing:

```bash
cargo run -p flowctl -- --profile local catalog publish --source ./capture.flow.yaml

journalctl --user -u flow-reactor@local-cluster-8099.service --since '10 min ago' --no-pager \
  | rg -i 'acmeCo/hello-world|created partition|servePrimary failed|not authorized|transaction|stats'

set -a
. ~/flow-local/env/reactor-local-cluster-8099.env
set +a

~/go/bin/flowctl-go journals list \
  --selector 'name:prefix=acmeCo/events/' \
  --format table

timeout 15 ~/go/bin/flowctl-go journals read \
  --selector 'name=acmeCo/events/14d588f6a580018e/pivot=00' \
  --output - \
  --file-root ~/flow-local/fragments
```

**Exactly-once probe.** `source-hello-world` emits `"Hello N!"` for increasing
`N` and persists `N` as connector state, so the counter **resumes rather than
restarting at 0** across a reactor restart, spec update, or disable + re-enable.
Read the collection back and assert `N` has no gaps or duplicates — a cheap
check that runtime-v2 carries connector checkpoint state across sessions exactly
once. (Also reproducible in-process via `preview-next --sessions 2,2,2` against
a persistent shard-zero RocksDB tempdir.)

## One-time setup

Done once per workstation. Skip the steps you've already completed.

### 1. Local Postgres

A Postgres reachable at `localhost:5432` with `postgres / postgres`
credentials (this matches the dev `supabase` instance the repo already
ships with). Quick verify:

```bash
psql postgresql://postgres:postgres@localhost:5432/postgres -c 'SELECT 1;'
```

### 2. Build a native materialize-postgres binary

The published `ghcr.io/estuary/materialize-postgres:dev` image only
ships `linux/amd64`. On ARM hosts you can build the connector natively
from the sibling `connectors` repo. We use `local:` mode in the spec to
drive the binary directly, avoiding container plumbing entirely:

```bash
cd /home/johnny/estuary/connectors/materialize-postgres
go build -o /tmp/materialize-postgres .
```

Re-run when the connector source changes. Any other `materialize-*`
connector under `connectors/` works the same way.

### 3. Build flowctl

From this repo:

```bash
cd /home/johnny/estuary/flow
cargo build -p flowctl --bin flowctl
```

The resulting binary is at `/home/johnny/cargo-target/debug/flowctl`.

## Repeatable E2E run

### 1. The harness spec

Live at `/tmp/preview-test/local.flow.yaml` (or wherever you keep it).
The `local:` endpoint plus `protobuf: true` skips Docker, runs the
connector as a child process.

```yaml
materializations:
  test/preview/wiki:
    endpoint:
      local:
        command:
          - /tmp/materialize-postgres
        config:
          address: localhost:5432
          user: postgres
          credentials:
            auth_type: UserPassword
            password: postgres
          database: postgres
          schema: public
        protobuf: true
    shards:
      logLevel: info
    bindings:
      - source: demo/wikipedia/recentchange-sampled
        resource:
          table: preview_wiki
          schema: public
```

Notes:
- The `--name` you'll pass to `flowctl raw preview-next` is the
  materialization name — `test/preview/wiki` here.
- Source is a real production collection. flowctl auths reads via your
  flowctl token (`~/.flowctl/config-default.yaml`). Materializations require
  a logged-in token; captures don't (they don't read journals, and the
  in-process Leader / Shuffle stack is not constructed for the capture path).
- Pick a `resource.table` name that's unique per scenario you're
  exercising — leftover state from prior runs (the checkpoint table
  `flow_checkpoints_v1` and the per-binding table) will block
  re-validation. See **Reset Postgres state** below.

### 2. Reset Postgres state

Each fresh run requires the bindings table absent (the connector
refuses to bind a new materialization onto a pre-existing table) and
the materialization checkpoint table clean (otherwise `Apply` is a
re-attach against stale state):

```bash
psql postgresql://postgres:postgres@localhost:5432/postgres -c '
  DROP TABLE IF EXISTS public.preview_wiki,
                       public.flow_checkpoints_v1
  CASCADE;'
```

If you change the binding's `table` in the spec, drop the *old* table
too — the connector will create the new one but refuses to overwrite
either.

### 3. Run the harness

The minimal invocation:

```bash
cd /tmp/preview-test
RUST_BACKTRACE=1 RUST_LOG=h2=info,info /home/johnny/cargo-target/debug/flowctl raw preview-next \
    --source ./local.flow.yaml \
    --name test/preview/wiki \
    --sessions=-1 \
    --timeout 60s \
    2> preview.stderr
```

Flags worth knowing:
- `--sessions=-1` — one unbounded session (default is also one
  unbounded session). Use `--sessions 2,2,2` to exercise cross-session
  recovery: three sessions of two transactions each, against a single
  persistent shard-zero RocksDB tempdir.
- `--shards N` — synthetic N-shard topology. For materializations N=1
  (default) hits the fast-path Join consensus and N≥2 exercises full
  multi-shard rendezvous. The `materialize-postgres` spec above is not a
  valid N>1 materialization workload: each shard drives an independent
  connector transaction against the same table. Use a connector/spec
  designed for multi-shard materialization before treating N>1 results as
  runtime signal. Captures with N≥2 fan out N independent shards (own
  connector, RocksDB, publisher) — runtime-correct, but the connector
  must honor `Open.range.key_begin/key_end` to split work meaningfully.
  Most capture connectors (including `source-hello-world`) ignore the
  range and will simply duplicate output across shards.
- `--timeout 60s` — graceful stop trigger. Set high enough that the
  close-policy can fire on whatever your source produces.
- `--log-json` — JSON ops logs to stderr. Off by default; useful when
  feeding the run into log tooling.
- `--debug-port 9999` — loopback HTTP port hosting the service-kit
  admin dashboard. Off by default. Mirror of the runtime-sidecar's
  `--admin-port`: handler inventory at `/`, JSON snapshot at
  `/debug/handlers.json`, per-handler trace overrides via
  `POST /debug/handlers/{id}/level/{level}`, Prometheus scrape at
  `/metrics`. Per-handler trace overrides take effect because flowctl
  installs the service-kit `layer_filter` + `event::layer` against a
  process-global registry (see `crates/flowctl/src/main.rs`).

Per-transaction observability is via `tracing` to stderr (see the
`Publisher::Preview` arm in `crates/runtime-next/src/publish.rs`):

- `connector applied` — emitted by the leader's apply loop with the
  connector's `action_description`, the iteration number, and any
  applied connector-state patches (one per loop iteration).
- `transaction stats` — emitted once per committed transaction with the
  full `ops::Stats` document (per-binding docs/bytes counts, etc).

These events are at info level. Filter further with
`RUST_LOG=runtime_next::publish=info` if you want only these and nothing
else.

### 4. Inspect what landed in Postgres

Standard psql against the dev DB. The connector creates a checkpoint
metadata table alongside the binding table:

```bash
# All flow tables
psql postgresql://postgres:postgres@localhost:5432/postgres \
    -c '\dt public.flow_*' \
    -c '\dt public.preview_wiki'

# Row count + sample rows
psql postgresql://postgres:postgres@localhost:5432/postgres \
    -c 'SELECT count(*) FROM public.preview_wiki;'

psql postgresql://postgres:postgres@localhost:5432/postgres \
    -c 'SELECT title, "user", wiki, type, timestamp
        FROM public.preview_wiki
        ORDER BY timestamp DESC
        LIMIT 10;'

# Per-binding committed checkpoint position (one row per shard)
psql postgresql://postgres:postgres@localhost:5432/postgres \
    -c 'SELECT * FROM public.flow_checkpoints_v1;'
```

A passing run leaves you with:
- `preview_wiki` populated with N rows where N = shuffled documents
  combined per transaction × number of committed transactions.
- `flow_checkpoints_v1` with one row containing this shard's last
  committed Frontier.

## What's exercised by each scenario

| Scenario                                   | Validates                                                                 |
|--------------------------------------------|---------------------------------------------------------------------------|
| `--sessions=-1 --timeout 60s`              | Single open session, transactions close on `maxTxnDuration` / data volume |
| `--shards 4 --sessions=-1 --timeout 60s`   | Multi-shard Join consensus, fan-out shuffle, leader cross-shard reduce with a multi-shard-safe connector/spec |
| `--sessions 2,2,2`                         | Cross-session recovery — sessions 2 and 3 see non-empty `L:Recover`       |
| (Ctrl-C mid-session)                       | Clean tonic-server shutdown, tempdirs removed, no port left bound         |
| `--name <a-capture>` against a capture spec | Capture session runs; `Joined → Opened → … → Stopped` ladder              |
| `--debug-port 9999`                        | Loopback dashboard at `http://127.0.0.1:9999/` lists live handlers; `POST /debug/handlers/{id}/level/trace` flips one handler to TRACE |
| `--name <derivation>` against a derivation | Error path: "runtime-next preview supports captures and materializations only…" |

## Known issues / current state (as of branch `johnny/runtime-v2`)

- A single connector log line at startup renders as nested ANSI — it
  comes from the legacy `runtime` crate's build-time validation path,
  which doesn't set `LOG_FORMAT=json`. All runtime-next per-shard
  connector logs render cleanly.
