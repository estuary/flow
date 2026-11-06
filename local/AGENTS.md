# Running a local stack in a dev VM — agent notes

`local/README.md` is the reference for how the stack is *wired*. This file is the
operational companion: the order to do things in, and the failure modes that are
slow to diagnose because their symptom points somewhere other than their cause.

Everything here was learned by bringing a stack up from scratch in a Lima VM on an
Apple Silicon host and running a connector in it.

## Bringing up a VM

```bash
mise run vm:create-lima <name>    # from the flow checkout; see --cpu / --memory
```

Two properties worth knowing before you start:

- **Nothing of the host is visible inside the VM.** It is created with
  `--mount-none`, so source, patches, and binaries are either cloned inside or
  copied in with `limactl copy`, which takes `<vm>:`-prefixed paths on either side
  (guest paths without a leading `/` are relative to the guest user's home):

  ```bash
  limactl copy ./patch.diff <vm>:/tmp/patch.diff   # host -> guest
  limactl copy -r ./some-dir <vm>:/tmp/            # -r for directories
  limactl copy <vm>:/etc/os-release .              # guest -> host
  ```
- **On Apple Silicon the guest is arm64 while connector containers run amd64**
  (`--platform=linux/amd64`, hard-coded in `crates/runtime/src/container.rs`).
  Rosetta runs them, but anything built *for* a container has to be x86_64 — which
  is a good reason to test connectors through a `local:` endpoint instead.

### If `create-post` fails, the VM is left half-bootstrapped

`create-lima` clones the repo and then runs `vm:create-post`, which calls
`mise install`. mise resolves every `github:*` tool through the GitHub API, and
unauthenticated requests are limited **per source IP** — so this fails when the
machine's quota is already spent, which in practice means several VM creations or
other GitHub-hitting tooling running at once:

```
mise WARN Failed to resolve tool version list for github:supabase/cli:
  HTTP status client error (403 Forbidden) …
github response: {"message":"API rate limit exceeded for <ip>. …"}
[vm:create-post] ERROR task failed
```

It fails **after** the VM exists and the repo is cloned, so you are left with a VM
that looks healthy and is missing supabase, etcd, sops, mold and sccache — plus
everything `create-post` does after `mise install`: the `usermod` calls, `rustup
default stable`, the swapfile. The follow-on errors all point at the wrong thing:

| What you see | Actual cause |
| --- | --- |
| `sh: 1: supabase: not found`, then `Job for flow-supabase@<stack>.service failed` | mise tools missing |
| `permission denied … /var/run/docker.sock` and `Docker Desktop is a prerequisite for local development` | user not in `docker` group |
| `journalctl --user`: `No journal files were opened due to insufficient permissions` | user not in `systemd-journal` group |

`mise ls --current | grep missing` confirms it — an empty result is what you want.

To recover, or to pre-empt it when you know the quota is tight, supply a token so
mise's lookups are authenticated. Check the quota this machine has left with:

```bash
curl -s https://api.github.com/rate_limit | jq '.rate'
```

Then re-run the install and the rest of `create-post`, piping the token via stdin so
it never lands in process args or on disk:

```bash
gh auth token | limactl shell <vm> -- bash -lc \
  'read -r TOK; cd ~/estuary/flow && MISE_GITHUB_TOKEN="$TOK" GITHUB_TOKEN="$TOK" mise install'
limactl shell <vm> -- bash -lc 'cd ~/estuary/flow && mise run vm:create-post'
limactl stop <vm> && limactl start <vm>
```

**Restart the VM afterwards, don't just reconnect.** `limactl shell --reconnect` does
*not* pick up the group membership `create-post` adds; `id -Gn` still showed only the
primary group. Verify with `docker ps` and `journalctl --user -n1`.

## Starting the stack

```bash
mise run local:stack && mise run local:stack-info
```

- **If supabase times out during `start-pre`, raise its timeout.**
  `local/systemd/flow-supabase@.service` sets `TimeoutStartSec=300`, and on a fresh
  VM that budget has to cover `supabase start` pulling ~10 images. The failure is
  `start-pre operation timed out`, with the journal a wall of `Pull complete` lines,
  and `ExecStopPost` then tears the containers down. Pulled layers stay cached so
  retries get further, but the clean fix is a drop-in:

  ```bash
  mkdir -p ~/.config/systemd/user/flow-supabase@<stack>.service.d
  printf '[Service]\nTimeoutStartSec=1800\n' > ~/.config/systemd/user/flow-supabase@<stack>.service.d/timeout.conf
  systemctl --user daemon-reload
  ```

- **Do not "pre-start" supabase by hand naively.** `supabase/config.toml` sets
  `project_id = "env(FLOW_SUPABASE_PROJECT_ID)"`, and that variable comes from the
  unit's env file — *not* from mise's stack env. Running `mise exec -- supabase
  start` leaves it unset, the CLI falls back to your username, and you get a whole
  second project (`supabase_db_<user>`, `supabase_kong_<user>`, …) competing for the
  same ports while the unit hangs in `start-pre`. If you do want to pre-pull:

  ```bash
  set -a; . ~/flow-local/env/supabase-<stack>.env; set +a
  cd $FLOW_ROOT && supabase start --exclude edge-runtime
  ```

- **Restart units after anything regenerates their env files.** `local:stack` leaves
  an already-running unit alone, so a unit can keep a stale `PATH` or a stale
  `CONFIG_ENCRYPTION_URL` indefinitely. Confirm what a process actually has:

  ```bash
  PID=$(systemctl --user show -p MainPID --value flow-reactor@<dp>-<port>.service)
  tr '\0' '\n' < /proc/$PID/environ | grep <VAR>
  ```

## Getting a tenant and publishing

```bash
mise run local:test-tenant                       # tenant `test/`, owned by alice@example.com
source ~/flow-local/<stack>/test-tenant-test.env # flowctl credentials
mise exec -- ./local/install-connector.sh ghcr.io/estuary/materialize-postgres
mise exec -- flowctl catalog publish --source my.flow.yaml --auto-approve
```

- `publish` refuses to run non-interactively without `--auto-approve`.
- The onboarding API rejects a user that already holds grants, so each seeded user
  (alice/bob/carol/dave) can own at most one tenant.
- **Write the endpoint config against the connector's current schema.** Publishing
  runs it through config-encryption, which validates and rejects with e.g.
  `Missing required property: credentials` — materialize-postgres now wants
  `credentials: {auth_type: UserPassword, password: …}` rather than a flat
  `password`. Get the schema from `connector_tags.endpoint_spec_schema` or
  `flowctl raw spec`.
- **A materialization needs the runtime-v2 shard flag**, or its shard dies with
  `runtime-v2 feature flag is unset but this shard is running the V2 materialize
  runtime`:

  ```yaml
  shards:
    logLevel: info                    # connector containers default to warn
    flags:
      enable-runtime-v2: "true"
  ```

### Prefer a `local:` endpoint over an image when testing a connector

Point the task at the connector binary instead of a published image:

```yaml
endpoint:
  local:
    command: ["/path/to/materialize-postgres"]   # absolute path
    config: { ... }                              # plaintext is fine
    protobuf: true                               # see below
```

The runtime execs the command directly (`crates/runtime{,-next}/src/local_connector.rs`),
so nothing in the container path applies: no image to build or push, no
`install-connector.sh` registration, no `--platform=linux/amd64`, and no
`flow-connector-init`. On an arm64 VM that leaves a plain native `go build` inside
the VM, with no cross-compiling and no arch to match.

- Permitted only when the runtime is `Plane::Local`. `mise/tasks/local/reactor` sets
  `FLOW_ALLOW_LOCAL=true`, so it works in a dev stack and is rejected anywhere else
  with `Local connectors are not permitted in this context`.
- **Set `protobuf: true` for Go connectors.** The runtime defaults to the JSON codec,
  while the `source-`/`materialize-boilerplate` connectors default `FLOW_RUNTIME_CODEC`
  to `proto`. Otherwise the two disagree on framing from the first request.
- **Use an absolute path.** The reactor is what execs it — both for Validate/Apply
  during a publish (the agent proxies those to the data plane) and for the running
  shard — so resolution depends on that unit's `PATH`, not your shell's.
- Discovery stays image-only (the control plane checks `connector_tags` first), so a
  `local:` endpoint covers validate/apply/run but not discover.

## Where to look for what

| Question | Where |
| --- | --- |
| What did a task's connector log? | `flowctl logs --task <name>` (needs `export SSL_CERT_FILE=~/flow-local/ca.crt`) |
| What did the build/validate step log? | the `build>` lines in `flowctl catalog publish` output |
| What did a unit do? | `journalctl --user -u flow-<svc>@<instance>` |
| What is in a built spec? | `select built_spec from live_specs where catalog_name = …` |
| What did the data plane actually receive? | the build database: `~/flow-local/<stack>/builds/<build-id>`, a SQLite file whose `built_materializations.spec` column is **proto**, not JSON |
| Which ports does this stack use? | `mise run local:stack-info` |
