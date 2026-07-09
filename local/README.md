# Local Stack Architecture & Operations

This directory holds the systemd unit templates and helper scripts that
`mise run local:*` glues together into a working Estuary stack. The interesting
machinery is in `local/systemd/` (unit templates) and `mise/tasks/local/`
(drivers that emit env files and drop-ins, then `systemctl --user start`).

**Scope.** This README is the primary reference for how the local stack is wired.
Keep it current as the stack evolves. Task-specific docs (e.g. `tests/soak/README.md`)
should link to the relevant section here rather than restate it.

> **The one thing to know:** run `mise run local:stack`, then
> `mise run local:stack-info`. Everything below is scoped to *this checkout*
> automatically — you don't compute ports or pick names. `local:stack-info`
> prints this stack's ports, units, and ready-to-paste commands.

## Stacks: many independent platforms on one machine

A **stack** is one instance of the whole local platform (control plane + data
planes), keyed 1:1 to a checkout — the primary clone *or* a linked git worktree.
Multiple stacks run side-by-side on one dev VM, each with its own build
artifacts, database, etcd, fragments, ports, and flowctl profile, so separate
users/agents can develop and QA distinct issues in parallel.

**Every checkout follows identical rules — there is no canonical/special stack.**
Each stack has:

- a **name** = sanitized checkout basename (lowercase, `[^a-z0-9-] → -`,
  ≤ 40 chars; an empty name or one containing the token `cluster` is remapped to
  `stack-<index>`). The primary clone at `~/estuary/flow` is stack **`flow`**;
  a worktree at `.../worktrees/fix-auth` is stack **`fix-auth`**.
- an **index** `i ∈ 0..15` = the lowest free slot in the registry at first use.
  A fresh machine's first checkout (e.g. a CI runner) lands on index 0, but
  allocation is sticky: a sole surviving checkout keeps whatever slot it was
  first given, even after earlier checkouts release theirs.
- a **data plane** named **`local-<name>-cluster`** (exposed as `FLOW_CLUSTER`).

> **Why data planes are named `local-<name>-cluster`, not `<name>-cluster`.** The
> control plane only accepts data-plane names whose last segment starts with a
> known provider token — `local-`, `aws-`, `gcp-`, … (`parse_data_plane_name` in
> `crates/control-plane-api`). So every local data plane must start with `local-`.
> `FLOW_CLUSTER` is what `local:stop` globs on to scope a stack's data-plane
> units; the `cluster`-token ban on names keeps those globs collision-free. (The
> 40-char name cap keeps `broker-local-<name>-cluster` within a 63-char DNS
> label.)

### The registry and `stack-env`

`~/flow-local/stacks.tsv` is the machine-global registry — one line per stack:
`index<TAB>name<TAB>abs-checkout-root`, guarded by `flock ~/flow-local/stacks.lock`.

`mise/tasks/local/stack-env` resolves the current checkout root, looks up or
**allocates on first use** (lowest free index, name = basename), and emits the
stack's `FLOW_*` / `CARGO_TARGET_DIR` / `GOBIN` / `CGO_LDFLAGS` variables. It is
wired into mise's env via `[env] _.source = "./mise/tasks/local/stack-env"` in
`mise.toml`, so those variables are ambient for everything mise runs.

**mise is mandatory (fail-fast).** stack-env is the single source of stack
identity, and it hard-fails on any error (registry unwritable, all 16 indexes
taken) with a one-line actionable message — mise then aborts the command rather
than silently defaulting the env. The happy path is silent; the first allocation
for a checkout prints one line to stderr. Consequently:

- **Inside mise** (`mise run <task>`, `mise exec -- <cmd>`) the env is always
  present. `mise run local:*`, CI, and tests are covered.
- **In a non-mise shell**, wrap commands: `eval "$(mise run local:stack-env)"`
  once, or run under `mise exec -- <cmd>`. Task scripts assert the env is present
  (`ensure_stack_env`) and refuse to run when invoked outside mise.
- **Bare `cargo build`** outside the mise env writes to the checkout's own
  `./target/` — isolated, but slow (no sccache/mold/prebuilt-rocksdb) and
  invisible to the stack's systemd units. Build via mise. (There is deliberately
  no `.cargo/config.toml` pinning `target-dir`: doing so without the rest of the
  env causes fingerprint churn in the shared per-stack dir.)

The first mise use in a fresh worktree allocates its slot (cheap, idempotent,
sticky across stop/start). `mise run local:stack-release` frees the slot.

## The port rule is load-bearing

**`base(i) = 10000 + 1000·i`, index `i ∈ 0..15`.** All ports live in
10000–25999 — entirely below Linux's ephemeral range (32768+), so no port
reservation is needed. The **muscle memory** is that the **last three digits
identify the service uniformly across all stacks** (x0011 is always a Postgres,
x0299 is always plane-0's reactor) and the **thousands digit is the stack index**
(stack 0's agent is 10020, stack 1's is 11020).

Offsets within a stack's 1000-wide block:

| Offset | Service |
| --- | --- |
| +00 / +01 | etcd client / peer |
| +05 | dashboard (`site_url`; UI runs separately) |
| +10 / +11 / +12 / +13 | Supabase api / db / studio / mailpit |
| +20 / +21 | agent / config-encryption |
| +25 | bigtable emulator |
| +30 / +31 | dekaf upstream Kafka / KRaft controller |
| +40 | data-plane-controller (dev-facing) |
| +200 + 100·p | data plane `p ∈ 0..7` (`FLOW_PLANE_BASE` = base+200) |

Not stack-scoped: cockpit :9090 (machine-global).

Within a data plane's 100-wide block: brokers count up from `base+0`; reactors
count *down* from `base+99` (so a single reactor lands on `base+99` — e.g. stack
`flow`'s sole reactor is **10299**); dekaf on `base+50/51/52`; runtime-sidecar
gRPC `base+60` / admin `base+61`. `local:data-plane` rejects any base that isn't
one of the stack's 8 valid plane bases (`FLOW_PLANE_BASE + 100·p`).

Dekaf is **opt-in**: `local:stack --dekaf`.

### Convenience exports (so nobody computes a port)

stack-env exports, in addition to the `FLOW_PORT_*` numbers:

- `FLOW_PG_URL=postgresql://postgres:postgres@localhost:${FLOW_PORT_SUPABASE_DB}/postgres`
- `FLOW_AGENT_URL=http://agent.flow.localhost:${FLOW_PORT_AGENT}`
- `FLOWCTL_PROFILE=${FLOW_STACK_NAME}` — flowctl's `--profile` flag reads this env
  var, so **bare `flowctl` inside a checkout targets that checkout's stack** with
  no flag. An explicit `--profile` still wins.

`SSL_CERT_FILE` is **not** exported ambiently (it would replace the trust store
for unrelated tools). Export it per-command: `export SSL_CERT_FILE=~/flow-local/ca.crt`
(`local:stack-info` prints the line).

## Unit naming: everything is a template instance

All control-plane units are templated on the **stack name**, and data-plane units
on `<dp>-<port>` (dp = `local-<stack>-cluster`), so two stacks never collide in
systemd's machine-global unit namespace:

```
flow-control-plane@<stack>.target
  ├─ flow-supabase@<stack>.service        (db/api/studio/mailpit, edge functions)
  ├─ flow-config-encryption@<stack>.service
  ├─ flow-control-agent@<stack>.service   (agent + config-encryption)
  └─ (bigtable, etcd are their own @<stack> services)

flow-etcd@<stack>.service                 (single-node, per-stack data dir + ports)
flow-bigtable@<stack>.service             (container flow-bigtable-<stack>)
flow-dekaf-kafka@<stack>.service          (container dekaf-kafka-<stack>, host net)

flow-plane@<dp>.target                     one per data plane (dp = local-<stack>-cluster)
  ├─ flow-gazette@<dp>-<port>             broker(s),  base+0..9
  ├─ flow-reactor@<dp>-<port>             reactor(s), base+99..90
  ├─ flow-runtime-sidecar@<dp>            Rust sidecar, base+60 (gRPC) / +61 (admin)
  ├─ flow-dekaf@<dp>                      Kafka shim,  base+50/51/52 (only with --dekaf)
  └─ flow-plane-link@<dp>                 oneshot: POSTs to agent to register
```

`local:stack` boots the control plane plus a `local-<stack>-cluster` data plane
at `FLOW_PLANE_BASE` with 4 brokers, 1 reactor, and link, then publishes
`ops-catalog/local-view.bundle.json` and prints the stack card. Add `--dekaf`
for the Kafka shim.

Useful incantations (stack `flow`, index 0, shown; `local:stack-info` prints the
exact names/ports for your stack):

```bash
systemctl --user list-dependencies flow-control-plane@flow.target
systemctl --user list-dependencies flow-plane@local-flow-cluster.target
journalctl --user -u flow-gazette@local-flow-cluster-10200 -f
journalctl --user -u flow-reactor@local-flow-cluster-10299 -f
journalctl --user -u flow-runtime-sidecar@local-flow-cluster -f

# Sidecar admin surface (loopback only), e.g. raise handler 0 to TRACE:
curl -s localhost:10261/debug/handlers.json | jq
curl -s -X POST localhost:10261/debug/handlers/0/level/trace
```

## Topology is built from drop-ins, not static units

The template units in `local/systemd/` are deliberately minimal — they carry no
ports, binary paths, or specific instances. Tasks write out:

- per-instance **env files** in the flat, shared `~/flow-local/env/`
  (`<svc>-<instance>.env`), each self-contained: the common build/runtime vars
  are inlined into every file (`emit_common_vars` in `mise/tasks/local/lib.sh`),
  so units load exactly **one** env file.
- **drop-ins** under `~/.config/systemd/user/`: `flow-plane@<dp>.target.d/wants-*.conf`
  (`Wants=`) and `flow-<svc>@<i>.service.d/plane.conf` (`PartOf=`/`After=`,
  including the cross-instance link to `flow-etcd@<stack>` /
  `flow-dekaf-kafka@<stack>` / `flow-control-agent@<stack>`).

Binaries are referenced through env vars, not hardcoded paths:
`ExecStart=sh -c 'exec ${CARGO_TARGET_DIR}/debug/agent …'`,
`sh -c 'exec ${GOBIN}/flowctl-go serve consumer'`. `CARGO_TARGET_DIR` is
per-stack (`~/cargo-target/<name>`) and `GOBIN` is `${CARGO_TARGET_DIR}/go-bin`,
so a running stack can only ever execute binaries built from *its own* worktree.

**Unit templates are always re-linked** from the invoking checkout at stack
start (+ `daemon-reload`). Templates are branch-stable but their symlinks live in
a machine-global namespace, so the **most recently started stack's templates
win**; restart other stacks after changing a template. Keep templates minimal to
make divergence rare.

## `local:stop` is scoped; `local:stack-release` frees the slot

`local:stop` is a stack-scoped guillotine — it stops only *this* stack's units,
removes only this stack's drop-ins, env files, and runtime state
(`~/flow-local/<name>/{etcd,fragments,builds}` and `test-tenant-*.env`), and
`reset-failed`s only this stack. It **never** `rm -r ~/.config/systemd/user`,
leaves the shared TLS material and unit template symlinks in place, and
**retains the registry entry** (allocation is sticky).

It deliberately **keeps `~/flow-local/<name>/.supabase-started`**: the supabase
postgres volume survives `supabase stop`, so that sentinel is what makes the
next start run `db reset` for a clean, freshly-migrated database. (Removing it
would drop the next start onto the first-start path — no reset — silently
resurrecting stale catalog state from the surviving volume.)

```bash
mise run local:stop            # stop + wipe THIS stack's state; keep its slot
mise run local:stack-release   # stop, then free the registry index for reuse
```

Independence: with two stacks up, stopping one leaves the other's units, DB,
etcd, fragments, and flowctl profile untouched. `systemctl --user restart
flow-supabase@<stack>` wipes only that stack's DB (see below).

Also note: `flow-plane-link@<dp>.service` has an `ExecStop=` that deletes
`live_specs` + the data plane row from Postgres. That fires whenever the link
service stops — even on a tidy `systemctl stop flow-plane@<dp>.target`.

## Supabase: first start vs every subsequent start

`flow-supabase@<stack>.service` is unusual. Its `ExecStartPre`s:

1. `supabase start --exclude edge-runtime` (idempotent, starts Docker containers)
2. **If `${FLOW_STACK_DIR}/.supabase-started` exists**, `supabase db reset` —
   wipes and re-applies migrations
3. `touch ${FLOW_STACK_DIR}/.supabase-started`

Then `ExecStart` runs `supabase functions serve` as the foreground process.
`ExecStopPost` runs `supabase stop`. `project_id` and every port come from
`config.toml` `env()` refs resolved against this stack's ambient `FLOW_*` vars,
so the containers are `supabase_db_<stack>`, on Docker network
`supabase_network_<stack>`.

The implication: **every restart after the first one wipes the database.** To
preserve state, delete the sentinel beforehand or don't restart.

## Secrets you might think are real but aren't

- **`super-secret-jwt-token-with-at-least-32-characters-long`** — the Supabase
  JWT secret. The pre-minted `SYSTEM_USER_TOKEN` in `plane-link-*.env` is a JWT
  signed with it for the `support@estuary.dev` system user.
- **`AGE-SECRET-KEY-1UX6ZHA…`** — SOPS age key used by reactor and dekaf; its
  public counterpart `age1z2qskpk…` is the config-encryption `KMS_KEY`.
- **`key-<data-plane-name>`**, base64 — the HMAC auth key shared by brokers,
  reactors, sidecar, and the agent (e.g. for `local-flow-cluster`, the base64 of
  `key-local-flow-cluster`).

None are real; they're checked into the repo. Assume the same for anything else
hardcoded in mise tasks.

## TLS — one CA, one wildcard, shared across stacks

`mise/tasks/local/tls-cert` generates a single self-signed CA and server cert in
`~/flow-local/` with SAN `DNS:*.flow.localhost,DNS:flow.localhost,IP:127.0.0.1`.
It is **shared by all stacks** (the wildcard covers `broker-<any-dp>.flow.localhost`,
`etcd.flow.localhost`, etc.). `.flow.localhost` resolves to loopback without
`/etc/hosts` (systemd-resolved, RFC 6761). Ports on those hostnames distinguish
stacks.

## Where state lives

Shared (all stacks): `~/flow-local/ca.{crt,key}`, `server.{crt,key}` (TLS);
`~/flow-local/env/*.env` (per-instance env files, filenames carry the
stack/instance); `~/flow-local/stacks.tsv` (registry).

Per stack, under `~/flow-local/<name>/` (= `${FLOW_STACK_DIR}`):

| Path | What |
| --- | --- |
| `etcd/` | Etcd data dir |
| `fragments/<dp>/` | Gazette journal fragments (file-only mode) |
| `builds/` | Catalog builds emitted by the agent |
| `test-tenant-<tenant>.env` | flowctl credentials from `local:test-tenant` |
| `.supabase-started` | Sentinel that triggers `db reset` on next supabase start |

Build artifacts are per-stack too: `~/cargo-target/<name>/` (Rust) and
`~/cargo-target/<name>/go-bin/` (Go). Concurrent `cargo build` in two checkouts
doesn't serialize on a shared target-dir lock. sccache, the Go build cache, and
`~/rocksdb-<version>` stay shared (content-addressed / read-only).

## flowctl against a stack

`local:control-plane` writes `~/.config/flowctl/<stack>.json` (write-if-absent,
to preserve any login tokens) with this stack's `agent_url`, `pg_url`,
`dashboard_url`, `config_encryption_url`, and the well-known local
`pg_public_token`. Because `FLOWCTL_PROFILE=<stack>` is ambient, **bare `flowctl`
inside the checkout targets this stack** — no `--profile` needed.

You must `export SSL_CERT_FILE=~/flow-local/ca.crt` (per command) or you'll see
TLS violations on broker/reactor calls.

## Connectors run on the Supabase Docker network

The **reactor** sets `FLOW_NETWORK=supabase_network_<stack>` (per stack), and the
data plane spawns every connector container onto that network — at runtime *and*
at build / discover / Validate time. The agent proxies connectors to the data
plane (`ProxyConnectors`), so they land on the reactor's network regardless of
the agent's own config. A connector endpoint config can therefore point straight
at this stack's Supabase Postgres (`db:5432` inside the network — the
container-internal port is always 5432 regardless of the host-published port);
other services must be `docker network connect supabase_network_<stack>`'d first.

## Provisioning tenant credentials

```bash
mise run local:test-tenant --tenant acmeCo --user alice@example.com
# Credentials are written to ${FLOW_STACK_DIR}/test-tenant-<tenant>.env
# (the default tenant is 'test'). Source it, then:
source ~/flow-local/<stack>/test-tenant-acmeCo.env
flowctl catalog list        # FLOWCTL_PROFILE is ambient
```

flowctl picks up `FLOW_AUTH_TOKEN` (a refresh token), which takes precedence over
loaded config.

## Driving a local runtime QA loop

Keep the stack stateful while iterating. After changing reactor-side Go or Rust,
a reactor restart is usually enough:

```bash
systemctl --user restart flow-reactor@local-flow-cluster-10299.service
```

A full `local:stop && local:stack` is useful when topology or DB state is
suspect, but it deletes this stack's etcd, fragments, env files, and drop-ins.

Prefer `flowctl` as the primary tool. `flowctl raw gazctl-env --help` generates an
environment for direct `flowctl-go`/`gazctl` interaction with journals/shards.

Status can lag across layers; cross-check reactor logs, connector containers,
Etcd, and the collection/ops journals (use this stack's etcd port — see
`local:stack-info`; stack `flow`'s is 10000):

```bash
journalctl --user -u flow-reactor@local-flow-cluster-10299.service --since '10 min ago' --no-pager
docker ps --filter 'label=task-name=acmeCo/hello-world'

ETCDCTL_API=3 etcdctl --endpoints=http://etcd.flow.localhost:10000 \
  get /flow/local-flow-cluster/items/... -w json | jq -r '.kvs[0].value' | base64 -d | strings | head
```

Capture runtime-v2 runs in the reactor process, so capture work does not appear
in the sidecar's `/debug/handlers.json`.

## Forwarding a stack to your laptop

`mise run vm:port-forward <host> [stack]` is dual-mode (see the task for detail).
With a single stack on the host you can omit `[stack]`. It:

- **remaps** the classic laptop ports to this stack's real ports for
  fixed-address clients (saved psql/Studio/Mailpit bookmarks, dashboard
  dev-server `.env`): laptop `5431→api`, `5432→db`, `5433→studio`, `5434→mailpit`,
  `8675→agent`, `8765→config-enc`. This is the only place classic ports survive,
  and belongs to one stack at a time.
- **identity-forwards** this stack's real ports (api, db, agent, config-enc,
  plane-0 brokers/reactors/sidecar-admin/dekaf, cockpit 9090) for
  advertisement-following clients (`flowctl --profile <stack>` doing data-plane
  work over `*.flow.localhost`).

Use `--no-remap` to forward a second stack alongside (identity sets never
collide; the classic remap set belongs to one stack).

## How many stacks fit?

RAM bounds concurrency: roughly 2.5–4 GiB per full stack, so a 16 GiB VM runs 2,
maybe 3 — not the 16-slot cap. CPU contention between concurrent cargo builds is
inherent. Bigger VM shapes are the fix. Stale registry entries from deleted
worktrees hold a slot until `local:stack-release`, but are otherwise harmless.

## Standalone helpers

`local/ops-publication.sh <bundle>` emits the SQL that `local:stack` uses to
publish an ops bundle (targets `ops/dp/public/${FLOW_CLUSTER}`).
`local/install-connector.sh <image> [tag]` adds a connector to this stack's DB.
Both require the ambient `FLOW_*` vars (run inside a mise context).
