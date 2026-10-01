# connector

Everything involved in running a Flow connector — extracting and unsealing its
endpoint configuration, injecting IAM credentials, dispatching to a docker
image / local subprocess / in-process connector, pumping its logs, and tearing
it down in the right order — behind exactly one protocol,
`connector.Connector` (`go/protocols/connector/connector.proto`).

One `Service` serves that protocol three ways:

- **in-process**, via `Service::spawn_connector`, with no wire hop and no
  protobuf ser/deser — how `runtime-next`'s shards drive their connectors;
- **over gRPC on a task's UDS**, via `Service::into_tonic_service`, registered
  by every `TaskService` alongside the `Shard` service;
- **over gRPC on the reactor's public address**, through the Go pass-through
  proxy (`go/runtime/connector_proxy_v2.go`), so a `PROXY_CONNECTOR` bearer can
  drive a connector from outside the reactor.

All paths use the same served stream. Clients route through
`proto_grpc::connector::Router`: `ServiceRouter` spawns an in-process stream and
`EndpointRouter` dials a gRPC endpoint.

`runtime-next` depends on this crate, never the reverse. The `runtime` crate
owns its container, image, and local-connector implementation independently.

## Protocol contract

- The **first** request sets `start` **and** exactly one protocol request. That
  request determines the connector type for the life of the stream. The service
  extracts and unseals its endpoint configuration and authorizes its task
  identity against the bearer's claims.
- Every **later** request sets exactly one protocol request of that same type
  and MUST NOT set `start`.
- `Started` is the first protocol response and carries the connector's Spec
  response. Logs may precede it, so clients read until `Started`. Later items
  are logs or protocol responses of the established type.
- `Start.execution` is the connector's execution, unset for ordinary. Every
  request, first or later, which embeds the task's built spec (Apply, Open)
  must match that spec's `execution`. `Started.execution` echoes it, unset
  when ordinary, and `proto_grpc::connector::start` rejects a `Started` which
  doesn't.
- A failure before the connector runs ends the stream with a `Status` and no
  `Started`.
- After protocol responses end, stderr is drained before EOF or a terminal
  status.
- Opening a connector eagerly spawns its handler. The request stream is
  authoritative for asynchronous teardown: clients close it when abandoning a
  session. Response sends are best-effort, but dropping the response stream
  is also a teardown signal: it's raced against both connector startup and the
  running session.

## Connector process environment

Image and local connectors receive the same runtime-owned environment contract.
These values override same-named entries of a local connector's model-provided
`env`; connectors may rely on them regardless of how they are launched.

- `CONNECTOR_MOUNT` names the connector mount described below.
- `LOG_FORMAT` is always `json`.
- `LOG_LEVEL` is the requested task log level. When unspecified,
  it defaults to `info` on development planes and `warn` elsewhere.

In-process connectors do not receive a process environment.

## Connector mount

Every image and local connector run receives a **connector mount**, which is
a defined file layout of resources handed to the connector for its use.
It exists only under the V2 runtime; the V1 `runtime` crate is untouched.

- `CONNECTOR_MOUNT` names the directory, which MUST exist though MAY be empty.
  A connector tests for the files it wants.
- Image connectors bind-mount the directory **read-only**. Future writable
  areas (scratch, persisted state) will be nested read-write mounts within it.
- An image mount additionally contains `flow-connector-init` (its entrypoint)
  and `image-inspect.json` (its `--image-inspect-json-path`).
- `task-update.json` is present only where the `Service` holds a `TaskUpdate`
  and the run has a task identity — not for a `<spec>` Spec, and not under
  `Service::new_local` (`flowctl preview`). Its shape is exactly:

  ```json
  {"token": "…", "control_plane_url": "…", "config_encryption_url": "…"}
  ```

- `token` bears `TASK_UPDATE` scoped to the task, and is periodically re-minted
  and rewritten **in place**. A connector therefore MUST re-read
  `task-update.json` at each use.

## Secrets and task update

- A task's `secrets` stanza maps secret names to the JSON pointers they merge
  at. A sibling of the task may merge wherever the task chooses. Any other
  secret reaches a connector only under the **image rule**: the image's
  `dev.estuary.secrets` label is a JSON object of
  `<prefix>/connectors/<repository>/<leaf>` names to pointers, and the task
  must name the same pointer the image declares. The vendor, and not the task
  author, decides where a vendor secret lands.
- What remains is connector development policy: a connector author must not
  send a declared location's value anywhere other than its intended service,
  such as to a host or URL the task configures.
- A connector updates its task by calling `/task/set-secret` and
  `/task/update-config` with the `task-update.json` credential
  (`tests/secrets/connector/source_secrets/rotation.py` is the reference). Such
  a connector MUST NOT also emit legacy `configUpdate` log events: a legacy
  update carries no `secrets` stanza, and publishing it clears the task's
  stanza. That's intended for legacy connectors, whose update is a `sops`
  config that cannot publish beside a stanza. It is connector development
  policy, and not enforced by the runtime.

## Key types

| Type / item                     | Role                                                                     |
| ------------------------------- | ------------------------------------------------------------------------ |
| `Service`                       | The gRPC service, and its in-process `spawn_connector` entry point        |
| `ServiceRouter`, `Service::new_local` | The in-process router and local-context pair                       |
| `LOCAL_ISSUER`                  | Issuer of local self-signed bearers; public for served test fixtures      |
| `proto_grpc::connector`         | Client routing, identity, bearer, and stream helpers                      |
| `LogSink` / `LogDest`           | Routes connector logs and container lifecycle records                     |
| `protocol::start`               | The start pipeline every connector goes through                          |
| `protocol::Protocol`            | Per-protocol trait: Spec request, RPC, and endpoint extraction             |
| `protocol::StartContext`        | Plain data a start needs: plane, network, logging, task, process, secrets, execution, VMM |
| `Vmm`, `Vmm::from_env` | VMM capability and configuration from `CONNECTOR_VMM_*` |
| `vmm::vmm_for` | Admission decision for requested execution |
| `vmm::plan::plan` | Pure plan of a VMM launch |
| `vmm::launch::start` | VMM lifecycle, image admission, credential mount, and recovery |
| `vmm::release::recover` | Releases resources whose durable owner is dead |
| `vmm::check_spec_execution` | Keeps execution and egress fixed through every session request |
| `TaskUpdate`                    | Signer and URLs by which a connector updates its config and secrets |
| `protocol::Endpoint`            | Normalized endpoint: image, local subprocess, or in-process connector     |
| `protocol::create_connector_mount` | Creates the per-run connector mount; see above                        |
| `Guard`                         | Host resources of one run: container process, refresh task, mount        |

## Layout

```
src/
├── lib.rs        # ServiceRouter re-export, LogSink, Started, RuntimeProtocol
├── service.rs    # Service / ServiceImpl, spawn_connector, tonic Connector impl
├── router.rs     # ServiceRouter and the local bearer issuer
├── serve.rs      # per-stream: authn/authz, extract, start, pump, teardown
├── protocol.rs   # Protocol trait, StartContext, start pipeline, connector mount
├── policy.rs     # pure product policy: image/secret admission, usage, token lifetimes
├── image.rs      # Estuary image declarations and image-endpoint connection
├── capture.rs    # Protocol impl: capture endpoints and RPC
├── derive.rs     # Protocol impl: derive endpoints and RPC, incl. derive-sqlite
├── materialize.rs# Protocol impl: materialize endpoints and RPC, incl. Dekaf
├── vmm.rs        # VMM configuration and admission
├── vmm/plan.rs   # Pure launch planning
├── vmm/launch.rs # Claimed launch and credential lifecycle
├── vmm/record.rs # Durable ownership and locks
├── vmm/release.rs# Resource release and recovery
└── container.rs  # Docker/Podman pull, inspect/run capabilities, dial
tests/
└── e2e.rs        # served streams: loopback gRPC, EndpointRouter, in-process
```

## Non-obvious details

- **Either stream ends a session.** Opening eagerly spawns the handler, which
races a closed request stream against a dropped response stream at every stage,
including connector startup. Only the handler's return releases the run's
`Guard`, so a client which walks away from a wedged connector must not be able
to leave it running.

- **The connector request stream begins with an internal Spec exchange.** The
  client's initial request follows after its configuration is unsealed, then
  subsequent client requests pass directly to the transport.
  `Started` carries the `Spec` response so the client can use it as well.

- **A local data plane's `*.localhost` services get a host-gateway mapping.**
  A `.localhost` name denotes the loopback of whoever resolves it, which inside
  an image container is the container. The `TaskUpdate` URLs are mapped to the
  host so a connector can reach them; only exact `.localhost` names, and only
  in a local plane, so ordinary DNS keeps its configured resolution.

- **Invalid later requests close connector input and terminate the session.**
The handler owns request validation, so malformed input cannot disappear as a
clean connector EOF.

- **Logs and protocol responses share one bounded response channel.** This
gives them the same backpressure and ordering. Log-producer lifetime signals
that stderr has been read through, which the handler awaits before terminal
EOF or status. A client which does not drain responses may park the handler.

- **Dropping a `Guard` kills `docker run`, then frees the connector mount.**
  The `docker run` child is SIGKILLed and the token refresh signaled to stop
  *before* the mount they use is removed. Killing `docker run` closes its
  stderr so its log pump can finish before the stream terminates; a local
  subprocess instead couples process and stderr completion in
  `connector_init::rpc::bidi`, so its `Guard` holds no process — only the
  mount which must outlive it.
  Note that killing `docker run` does NOT stop the container, only its log proxy.
  `flow-connector-init` self-exits after 10s without a received RPC.

- **The minted selector always admits `<spec>`.** `connector_bearer` scopes to
  `{task-type: [type], task-name: [name, <spec>]}`, so one bearer shape serves
  both a session's Open and a unary Spec — which names no task. Tokens are one
  minute and are checked at stream open only (no `expiry_guard`), consistent
  with the leader and shuffle bearers. A non-Spec request which carries the
  `<spec>` sentinel is rejected.

- **`Start.sqlite_vfs_uri` is runtime-internal.** It's set only by an in-process
  shard hosting a recorded recovery log, and only for a `Sqlite` derivation;
  any other connector type rejects it as `InvalidArgument`.

- **Every start is admitted by `vmm_for`, before anything runs.** It sees the
  requested execution, the endpoint, the protocol, and the service's VMM
  capability, after endpoint extraction and before any image is pulled or
  process started, and returns the `Execution` the start's image connector
  runs under. A VMM request must name an eligible image (by repository,
  whatever its tag or digest: today only `ghcr.io/estuary/derive-python` as a
  derivation), on a service with VMM capability. It is then launched in a VMM
  or fails; it never falls back to an ordinary container. A local or
  in-process endpoint cannot request it. A start whose execution declares
  `egress` without a VMM is refused first, whatever its endpoint: only a VMM
  enforces egress, and a declaration is never quietly dropped. A VMM start's
  declared hosts are validated here too, before anything is pulled.
  `Started.execution` is set only once the VMM's connector-init is dialed and
  the Spec exchanged.

- **A session's execution is fixed at its start.** `serve::pump` checks later
  built specs against `Started.execution` before forwarding them. Even
  narrower egress would differ from the running VMM's policy, so a mismatch
  ends the session with `InvalidArgument`.

- **VMM configuration is read once, and checked whole.** `Vmm::from_env`
  reads nothing else when `CONNECTOR_VMM_IMAGE` is unset or empty. Otherwise
  `CONNECTOR_VMM_STATE_DIR` is required: an absolute host path without a
  comma, at most 72 bytes so that `<dir>/fv_<16 hex>/sock/init.sock` fits a
  Unix socket address. `CONNECTOR_VMM_DISK_MIB` (2048),
  `CONNECTOR_VMM_MEMORY_OVERHEAD_MIB` (256) and `CONNECTOR_VMM_PODMAN`
  (`podman`, the rootful engine however this process reaches it) take
  defaults. A malformed value fails service construction. Whether the host
  can actually run a VMM is a question for each launch, not for configuration.

- **A VMM container gets an ordinary connector's limits.** `--memory` and
  `--cpus` are `CONNECTOR_MEMORY_LIMIT` and `CONNECTOR_CPU_LIMIT`, verbatim.
  The guest is sized within them: RAM is the limit's whole MiB less the
  overhead, and vCPUs are the CPU limit rounded up. The memory limit is read
  as podman reads `--memory` (go-units' `RAMInBytes`: fractions, exponents,
  `b`/`ib` units, one optional space), so the guest fits the cap the engine
  actually applies; only a hexadecimal number is refused. The CPU limit is a
  decimal number of CPUs.

- **A VMM connector reaches its own hosts, its image's, and its task's.**
  Each eligible connector carries default hosts (Python's package index, for
  `derive-python`), the image may add more as a JSON array of names in its
  `dev.estuary.egress-hosts` label, and the task may add more in its
  execution's `egress.hosts`. `vmm::plan::Egress` is their union, in that
  order and without repeats, each name remembered with where it came from.
  A missing, blank or empty label adds nothing; anything else that isn't
  valid names is refused, on every plane. Names follow
  [`crates/egress`](../egress/README.md), whose `public_policy` writes the
  VMM's `policy.json`. Ordinary launches never read the label.

- **Whether a task is held to names at all is the plane's, unless it
  declares egress.** A task which declares egress, even with no hosts, is
  held to the union above on every plane. One which declares none is held to
  it on a public plane, while a private or local plane writes
  `egress::any_public_policy`: any public destination, by name or address,
  but still never a destination the VMM's baseline excludes. The plane is the
  service's own; nothing a task sends chooses it. Each launch logs which
  applies, and every permitted name with its source, to the task's logs.

- **A VMM launch is planned before anything is created.** `vmm::plan::plan`
  turns the configuration and the connector's image inspection into the
  policy, the `fv_<16 hex>` state directories and socket, the files the
  connector mount must hold, and three podman lines: `boundary verify` from
  the VMM image, the VMM's own `fvm<12 hex>` bridge network, and its `run`.
  It refuses an image which exposes TCP ports, which nothing could reach. The
  lines are those of `crates/connector-vmm-tests`'s `src/launch.rs` plus
  `--platform`, the usage labels, the owner label (below) and
  `--cgroup-parent`, which the reference leaves to a launcher, and with the
  configured limits as written. The plan `podman create`s the container the
  reference `podman run`s, with the same arguments, and the launch then
  `podman start --attach`es it by ID; `plan_reproduces_the_reference_lines`
  holds them to it. `LOG_LEVEL` defaults to `info` on local planes and `warn`
  elsewhere.

- **A VMM launch runs as one task, which holds its ownership record.**
  `vmm::launch` first releases what dead launches left (below), pulls and
  inspects the image with `CONNECTOR_VMM_PODMAN` (skipping the pull of a
  `:local` tag, as ordinary launches do), and plans. It then claims its id by
  making `<STATE_DIR>/fv_<id>.owner` (a new id if it exists), makes the
  connector mount and `fv_<id>` state directory, writes the planned files,
  runs `boundary verify`, creates the network (a new id and record, at most
  three tries, if that fails), creates the container, and starts it. Pulls,
  inspection, verification, the readiness wait and the dial race the start's
  abandonment; the commands which create the network and container are never
  interrupted, so each has finished before teardown looks for what it made.

- **A launch owns only what it proves it made, and its record says so.** The
  record is written, `flock`ed and synced as an unnamed `O_TMPFILE`, then
  linked under its name, so it is never seen unlocked or empty, and its claim
  is exclusive. It is append-only JSON lines: a claim naming the id, a random
  owner token and the connector mount, then a line marking each directory
  made by exclusive `mkdir`, written before anything is put in it. The network
  and container carry the token as their `dev.estuary.vmm-owner` label, which
  podman stores as it creates them, and are found by it and nothing else: a
  name never proves ownership. A directory that already exists is not marked,
  and so never removed. A record which reads as anything other than what a
  launch writes (another version, an id or mount of the wrong shape, a
  repeated or unknown line) directs no removal at all, and is kept and
  reported; a last line without its newline is an append that never finished.

- **The record's lock is the owner's liveness, PIDs aside.** The kernel drops
  a `flock` with the last descriptor of its open file, however its process
  ends, across PID namespaces and containers sharing the host filesystem. The
  commands which create the network and container (`fenced` in `vmm::launch`)
  are given that open file as stdin, so the lock is held as long as any of
  them runs: a podman client survives its launcher's death, sudo's too, and
  may still create. Whoever takes the lock therefore finds what is final. This
  assumes `CONNECTOR_VMM_PODMAN` keeps its stdin open until its command is done
  (as `exec`, `sudo` and podman do) and a local engine: a remote podman service
  may finish a request after its client has gone, which can leak, but never
  misattribute, a resource. `STATE_DIR` must be a local filesystem with
  `flock` and `O_TMPFILE`, and persistent if records are to survive a reboot.

- **Release is one state machine, for teardown and recovery alike.**
  `vmm::release` finds the containers labeled with the record's token and
  removes them by ID; if that query or removal fails, nothing else is touched,
  since the mount, state and network may still be in use. Then networks by the
  label, removed by name (podman 4.9 skips its in-use check when given an ID);
  then the marked mount and state. The record is removed, under its lock, only
  once all of them are gone. Otherwise it is kept unchanged and let go, and a
  later release, which repeats every check, retries it.

- **Teardown stops the VMM, then releases, and the session waits for it.** A
  dropped `Guard`, an abandoned start, or a failure at any step leads to the
  same teardown: `podman rm --force` by the ID `create` printed (killing a
  client does not stop its container, and a client run through sudo outlives a
  kill of sudo), at most ten seconds for the `start --attach` client to leave,
  then the release. What remains is an error log to the session and to
  tracing, naming the record kept for it. The task holds a clone of the
  session's log sink, so the stream's terminal status or EOF follows teardown.
  If the task is dropped instead, with its runtime, nothing runs: its lock goes,
  and its record is recovered.

- **Every VMM launch first recovers its state directory.** Before its own
  claim, a launch tries the lock of every `fv_<16 hex>.owner` beneath
  `STATE_DIR`, skipping those held, by a live owner or another releaser, and
  releases the rest. Their failures go to tracing alone, not to the session,
  whose task they are not. There is no other trigger: what a dead launch left
  waits for the next VMM launch sharing its `STATE_DIR`. connector-init ends an
  idle VMM within seconds of its last RPC, and `--rm` then removes its
  container, so what usually waits is a network, mount, state and record.

- **Nothing without a record is touched.** `fv_*` containers, networks and
  state directories left by a launcher that wrote no records, and mounts not
  named `mount-fv_<id>`, are not this crate's to judge. With every launcher
  stopped, an operator may remove `fv_*` containers and networks lacking a
  `dev.estuary.vmm-owner` label, `STATE_DIR/fv_*` directories lacking a
  matching `.owner`, and `connector-mounts-*/mount-*` directories not named
  `mount-fv_*`.

- **The host is checked where the launcher shares it.** Before any network:
  `CONNECTOR_VMM_STATE_DIR` must be a directory, the launch's `scratch/` must
  take an `O_TMPFILE` sized to `CONNECTOR_VMM_DISK_MIB` (sparse, then closed),
  a static `flow-connector-init` must be found beside this program or on `PATH`
  (as the VMM's guest runs it; there is no fallback image), and
  `boundary verify` from the VMM image must pass, uncached, immediately before
  the network is created. `/dev/kvm` is left to podman, whose `/dev` may not be
  this process's. A refused plan is `InvalidArgument`, an unready host
  `FailedPrecondition`, and a failed pull, network, run, readiness or dial an
  ordinary launch's error.

- **A VMM's connector mount follows the shared contract.** It is
  `mount-fv_<id>` in `$TMPDIR/connector-mounts-<euid>`, both mode 0711,
  holding `flow-connector-init` (0555) and `image-inspect.json` (0444), bound
  read-only at its own path into the container and the guest and named by
  `CONNECTOR_MOUNT`. TMPDIR, like the state directory, must therefore be the
  same path for this process and the engine. Ordinary launches still mount two
  temporaries.

- **A VMM is reached only through `init.sock`.** Its `Container` has no
  address, ports or mapped ports, and carries the image's usage rate.

- **Image admission identifies images by repository.** The public-plane
  refusal of Python derivations applies to every tag, digest, and bare
  spelling of `ghcr.io/estuary/derive-python`, for ordinary launches. A VMM
  launch skips ordinary image admission: its eligibility is narrower, and the
  VMM is the protection the refusal stands in for.

- **A response-stream error is terminal.** Panics in the spawned handler are
converted into a terminal gRPC status rather than appearing as clean EOF.
