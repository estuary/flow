# `flowctl preview` as a runtime-next E2E harness

This is a hands-on guide for using `flowctl preview` as a repeatable
end-to-end test of the `runtime-next` + `leader` + `shuffle` stack
against a local Postgres database. It assumes the runtime-v2 branch is
checked out and built.

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
- The `--name` you'll pass to `flowctl preview` is the materialization
  name — `test/preview/wiki` here.
- Source is a real production collection. flowctl auths reads via your
  flowctl token (`~/.flowctl/config-default.yaml`).
- Pick a `resource.table` name that's unique per scenario you're
  exercising — leftover state from prior runs (the materialization
  metadata tables `flow_materializations_v2`, `flow_checkpoints_v1`,
  and the per-binding table) will block re-validation. See **Reset
  Postgres state** below.

### 2. Reset Postgres state

Each fresh run requires the bindings table absent (the connector
refuses to bind a new materialization onto a pre-existing table) and
the materialization metadata tables clean (otherwise `Apply` is a
re-attach against stale state):

```bash
psql postgresql://postgres:postgres@localhost:5432/postgres -c '
  DROP TABLE IF EXISTS public.preview_wiki,
                       public.flow_materializations_v2,
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
/home/johnny/cargo-target/debug/flowctl preview \
    --source ./local.flow.yaml \
    --name test/preview/wiki \
    --output-state \
    --output-apply \
    --sessions=-1 \
    --timeout 60s \
    2> preview.stderr
```

Flags worth knowing:
- `--sessions=-1` — one unbounded session (default is also one
  unbounded session). Use `--sessions 2,2,2` to exercise cross-session
  recovery: three sessions of two transactions each, against a single
  persistent shard-zero RocksDB tempdir.
- `--shards N` — synthetic N-shard topology. N=1 (default) hits the
  fast-path Join consensus; N≥2 exercises full multi-shard rendezvous.
- `--timeout 60s` — wall-clock kill switch. Set high enough that the
  close-policy can fire on whatever your source produces.
- `--output-apply` — emits one `["applied",{...}]` line at session
  start.
- `--output-state` — emits one `["connectorState",{...}]` line per
  committed transaction (the State Update Wire Format payload from
  L:StartedCommit).
- `--log-json` — JSON ops logs to stderr. Off by default; useful when
  feeding the run into log tooling.
- `RUST_LOG=info,shuffle=debug,leader=debug,runtime-next=debug` for
  cross-stack debug. `leader::materialize::startup=debug` is enough to
  see L:Recover payloads on session 2+.

### 4. Inspect what landed in Postgres

Standard psql against the dev DB. The connector creates two metadata
tables alongside the binding table:

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

# Materialization metadata (the connector's own bookkeeping)
psql postgresql://postgres:postgres@localhost:5432/postgres \
    -c 'SELECT * FROM public.flow_materializations_v2;'
```

A passing run leaves you with:
- `preview_wiki` populated with N rows where N = shuffled documents
  combined per transaction × number of committed transactions.
- `flow_checkpoints_v1` with one row containing this shard's last
  committed Frontier.
- `flow_materializations_v2` recording the binding's resource state.

## What's exercised by each scenario

| Scenario                                   | Validates                                                                 |
|--------------------------------------------|---------------------------------------------------------------------------|
| `--sessions=-1 --timeout 60s`              | Single open session, transactions close on `maxTxnDuration` / data volume |
| `--shards 4 --sessions=-1 --timeout 60s`   | Multi-shard Join consensus, fan-out shuffle, leader cross-shard reduce    |
| `--sessions 2,2,2`                         | Cross-session recovery — sessions 2 and 3 see non-empty `L:Recover`       |
| (Ctrl-C mid-session)                       | Clean tonic-server shutdown, tempdirs removed, no port left bound         |
| `--name <a-capture>` against a capture spec | Error path: "runtime-next preview supports materializations only…"        |

## Known issues / current state (as of branch `johnny/runtime-v2`)

- **Apply line image is empty.** The local-mode connector is a binary,
  not an OCI image, so `Opened.connector_image` is the empty string.
  Output is `["applied",{"image":""}]`. Cosmetic.
- **Connector logs render as nested ANSI.** Operationally harmless;
  comes from the connector binary's own logger inside our `ops::Log`
  envelope.