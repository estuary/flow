# Local Stack Architecture & Operations

This directory holds the systemd units and helper scripts that `mise run local:*`
glues together into a working Estuary stack. The interesting machinery is in
`local/systemd/` (unit files) and `mise/tasks/local/` (drivers that emit env
files and dropins, then `systemctl --user start`).

## Topology

```
flow-supabase.service ──┐
flow-config-encryption ─┤      flow-control-plane.target
flow-control-agent.service ─┘   (8675 agent, 8765 config-encryption)

flow-etcd.service  (single-node, http://etcd.flow.localhost:2379)

flow-plane@<dp>.target ───────────  one per data plane
  ├─ flow-gazette@<dp>-<port>            broker(s),  base_port + 0..9
  ├─ flow-reactor@<dp>-<port>            reactor(s), base_port + 99..90
  ├─ flow-runtime-sidecar@<dp>           Rust sidecar, base_port + 60 (gRPC) / +61 (admin HTTP)
  ├─ flow-dekaf@<dp>                     Kafka shim,  base_port + 50/51/52
  └─ flow-plane-link@<dp>                oneshot: POSTs to agent to register

flow-dekaf-kafka.service                 Confluent Kafka in Docker, fixed :29092
```

`local:stack` boots the control plane plus a `local-cluster` data plane at
port-block 8000 with 4 brokers, 1 reactor, dekaf, and link, and then publishes
`ops-catalog/local-view.bundle.json` into it.

## The port scheme is load-bearing

Each data plane gets a 100-wide block starting on a multiple of 100; the
per-service offsets within it (annotated in the topology diagram above) are
fixed by the mise tasks, and the data-plane script rejects a base port that
isn't `% 100 == 0`. One surprising consequence worth internalizing:
reactors count *down* from the end of the block, so a "single reactor"
lands on `block+99` — `local-cluster`'s sole reactor is at **8099**, not 8004.

`mise/tasks/vm/port-forward` hardcodes three reserved blocks — **8000–8099**,
**10000–10099**, **10100–10199** — so those are your three forwardable data
planes from a VM.

The dockerized Kafka that backs Dekaf consumer groups lives outside any block,
on **:29092**, and is shared across all data planes.

## Topology is built from drop-ins, not static units

The template units in `local/systemd/` are deliberately minimal — they don't
know about any specific data plane. mise tasks write out dropins under
`~/.config/systemd/user/`:

- `flow-plane@<dp>.target.d/wants-*.conf` — `Wants=` to pull each instance in
- `flow-<svc>@<i>.service.d/plane.conf` — `PartOf=`/`After=` to bind to the target

`PartOf=` is what makes `systemctl --user stop flow-plane@local-cluster.target`
cascade-stop every broker, reactor, sidecar, dekaf, and link. `Wants=` does
**not** cascade in reverse — starting the target pulls children up, but
starting a child doesn't pull the target up.

Useful incantations:

```bash
systemctl --user list-dependencies flow-plane@local-cluster.target
systemctl --user list-units 'flow-*'
journalctl --user -u flow-gazette@local-cluster-8000 -f
journalctl --user -u flow-runtime-sidecar@local-cluster -f

# Sidecar admin surface (loopback only): live Leader/Shuffle handlers, plus a
# runtime trace-level control. E.g. raise handler 0 to TRACE, then clear it
# (the level route is a POST — it mutates process state):
curl -s localhost:8061/debug/handlers.json | jq
curl -s -X POST localhost:8061/debug/handlers/0/level/trace
curl -s -X POST localhost:8061/debug/handlers/0/level/off
```

## You don't have to run the whole stack

`local:stack` is a convenience that chains `local:control-plane` →
`local:data-plane local-cluster 8000 ...` → bundled-catalog publish. Each
piece is its own task and pulls only what it needs via `#MISE depends=`:

| Task | Pulls in | When you want it |
| --- | --- | --- |
| `local:supabase` | (just supabase) | Hacking migrations / pgTAP; running Rust integration tests |
| `local:control-plane` | supabase, config-encryption, agent | Agent / controller work without any data plane |
| `local:data-plane <name> <port>` | etcd, brokers, reactor, runtime-sidecar | A second data plane alongside `local-cluster` (use port-block 10000 or 10100), or a data plane with no control plane at all if you skip `--link` |
| `local:data-plane-controller` | supabase | Dry-run controller convergence loop |

There's no "down" for an individual task — use `systemctl --user stop` on the
specific unit (or `flow-plane@<dp>.target` for a whole data plane). `local:stop`
is the "burn it all" button.

## `local:stop` is a guillotine

It does much more than `systemctl stop`:

```
systemctl --user stop 'flow-*'
rm -r ~/.config/systemd/user                 # ALL unit symlinks + dropins
rm -r ~/flow-local/{builds,env,etcd,fragments}
```

So journal fragments, Etcd state, generated env files, and the topology dropins
all vanish. If you want to bounce a service without nuking state, stop it
directly. If you want to keep state across a planned restart, stop the target
but leave `~/flow-local/` and `~/.config/systemd/user/` alone.

Also note: `flow-plane-link@.service` has an `ExecStop=` that deletes
`live_specs` + the data plane row from Postgres. That fires whenever the link
service stops — even on a tidy `systemctl stop flow-plane@<dp>.target`.

## Supabase: first start vs every subsequent start

`flow-supabase.service` is unusual. Its `ExecStartPre`s:

1. `supabase start --exclude edge-runtime` (idempotent, starts Docker containers)
2. **If `~/flow-local/.supabase-started` exists**, `supabase db reset` — wipes
   and re-applies migrations
3. `touch ~/flow-local/.supabase-started`

Then `ExecStart` runs `supabase functions serve` as the foreground process.
`ExecStopPost` runs `supabase stop` to tear down the Docker containers.

The implication: **every restart after the first one wipes the database**. If
you `systemctl restart flow-supabase`, you lose all catalog state. If you want
to preserve it, delete the flag file beforehand or just don't restart.

## Secrets you might think are real but aren't

- **`super-secret-jwt-token-with-at-least-32-characters-long`** — the Supabase
  JWT secret. The pre-minted `SYSTEM_USER_TOKEN` baked into `plane-link-*.env`
  is just a JWT signed with it for user `ffffffff-ffff-ffff-ffff-ffffffffffff`
  (the `support@estuary.dev` system user), with exp in 2055.
- **`AGE-SECRET-KEY-1UX6ZHA...`** — SOPS age key used by both reactor and
  dekaf. Its public counterpart `age1z2qskpk...` is the `KMS_KEY` the
  config-encryption service is configured with. Anything sops-encrypted by the
  local stack opens with `export SOPS_AGE_KEY=AGE-SECRET-KEY-1UX...`.
- **`key-<data-plane-name>`**, base64-encoded — the HMAC auth key shared by
  brokers, reactors, runtime-sidecar, and the agent (via the link payload).
  For `local-cluster` that's `a2V5LWxvY2FsLWNsdXN0ZXI=`.

If you ever wonder "is this real" — none of these are; they're checked into
the repo. If something else is hardcoded in mise tasks, assume the same.

## TLS — one CA, one wildcard, everywhere

`mise/tasks/local/tls-cert` generates a single self-signed CA and a single
server cert in `~/flow-local/` with SAN `DNS:*.flow.localhost,DNS:flow.localhost,IP:127.0.0.1`.
The cert is reused by every broker, reactor, and sidecar. Hosts use names like
`broker-<dp>.flow.localhost` and `reactor-<dp>.flow.localhost` precisely so
they validate against the wildcard. Certs are long-lived (365 days).

`.flow.localhost` works without `/etc/hosts` entries: systemd-resolved
implements RFC 6761 and resolves any `*.localhost` to `::1`/`127.0.0.1`.

## Where state lives

| Path | What |
| --- | --- |
| `~/flow-local/ca.{crt,key}`, `server.{crt,key}` | TLS material |
| `~/flow-local/env/*.env` | Generated systemd env files, one per instance |
| `~/flow-local/etcd/` | Etcd data dir |
| `~/flow-local/fragments/<dp>/` | Gazette journal fragments (file-only mode) |
| `~/flow-local/builds/` | Catalog builds emitted by the agent |
| `~/flow-local/.supabase-started` | Sentinel that triggers `db reset` on next supabase start |
| `~/.config/systemd/user/flow-*` | Linked unit files + drop-in topology |

Binaries: the unit files run `~/cargo-target/debug/<crate>` because
`CARGO_TARGET_DIR=~/cargo-target` is set by mise, not under the repo. Most
units have an `ExecStartPre=cargo build -p <crate>` so a unit restart picks up
your latest source — handy when iterating.

## Driving a local runtime QA loop

For runtime work, keep the local stack stateful while you iterate. A full
`mise run local:stop && mise run local:stack` is useful when topology or
database state is suspect, but it also deletes Etcd state, local fragments,
generated env files, and the systemd drop-ins. After changing reactor-side Go
or Rust code, a reactor restart is usually enough (takes a few minutes):

```bash
systemctl --user restart flow-reactor@local-cluster-8099.service
```

Strongly prefer `flowctl` as the primary tool for interacting with the platform.

If needed, use `flowctl raw gazctl-env --help` to generate an environment
suitable for `flowctl-go` or `gazctl`, which allows for direct interaction
with journals and shards.

Status comes from several layers, and one layer can lag another. After a task
has failed a few times, `flowctl catalog status <task>` may continue reporting
the last failure while the controller and reactor work through retry backoff.
Cross-check with reactor logs, connector containers, Etcd, and the actual
collection or ops journals:

```bash
systemctl --user is-active \
  flow-control-plane.target \
  flow-plane@local-cluster.target \
  flow-reactor@local-cluster-8099.service \
  flow-runtime-sidecar@local-cluster.service

journalctl --user -u flow-reactor@local-cluster-8099.service --since '10 min ago' --no-pager

docker ps --filter 'label=task-name=acmeCo/hello-world'
```

The labels in a built spec are not always the labels the reactor is actually
running with. The controller overlays live shard labels such as `range`,
`logs-journal`, and `stats-journal` into Etcd. Inspect the live shard spec when
authorization, stats, or logs look inconsistent with the built catalog:

```bash
ETCDCTL_API=3 etcdctl \
  --endpoints=http://etcd.flow.localhost:2379 \
  get /flow/local-cluster/items/capture/acmeCo/hello-world/14d588f6a580018e/00000000-00000000 \
  -w json | jq -r '.kvs[0].value' | base64 -d | strings | head -100
```

The runtime sidecar admin endpoint is useful for materializations and
derivations driven by the Rust sidecar. Capture runtime-v2 runs in the reactor
process, so do not expect capture work to appear in `/debug/handlers.json`.

## Provisioning tenant credentials

After data plane(s) are started, register a new tenant and credentials:
```bash
mise run local:test-tenant --tenant acmeCo --user alice@example.com
# Credentials are written per-tenant as ~/flow-local/test-tenant-<tenant>.env
# (the exact path is printed at the end).
# The default tenant is 'test' -> ~/flow-local/test-tenant-test.env.
source ~/flow-local/test-tenant-acmeCo.env
flowctl --profile local catalog list
```

flowctl picks up and uses `FLOW_AUTH_TOKEN` (a refresh token),
which takes precedence over loaded config.

## flowctl against the local stack

`flowctl --profile local` flips a single boolean in flowctl's config that
swaps every URL to a localhost equivalent:

| | local | default |
| --- | --- | --- |
| agent | `http://localhost:8675` | `https://api.estuary.dev` |
| postgrest | `http://localhost:5431/rest/v1` | hosted |
| dashboard | `http://localhost:3000` | hosted |
| config-encryption | `http://localhost:8765` | hosted |

The `--profile` flag also controls which config file in
`~/.config/flowctl/` is loaded.

## Connectors run on the Supabase Docker network

Reactor and agent set `FLOW_NETWORK=supabase_network_flow` (see
`mise/tasks/local/reactor`), so connector containers — at build/discover time
and at runtime — run on that Docker network, not the default bridge. The handy
consequence: a connector endpoint config can point straight at the Supabase
Postgres, which sits on the same network as `supabase_db_flow` and answers to
`db:5432`. Any *other* service a connector needs has to be `docker network
connect supabase_network_flow`'d first, and then addressed by container name.

## What `local:stack` is actually publishing

After the data plane comes up, `local:stack` opens a psql session and writes a
draft + publication of `ops-catalog/local-view.bundle.json` to the
`ops/dp/public/local-cluster` data plane, as the system user
`support@estuary.dev`. This is what makes `flowctl --profile local catalog list`
return the `ops/rollups/...` and `ops/dp/public/local-cluster/...` specs.
`local/ops-publication.sh` is the standalone version of that same SQL if you
want to publish your own bundle the same way.
