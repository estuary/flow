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

VMM execution's task fields are described for users in
[`docs/connector-vmm/tasks.md`](../../docs/connector-vmm/tasks.md), and its
host preparation, configuration, recovery and rollout for operators in
[`docs/connector-vmm/operating.md`](../../docs/connector-vmm/operating.md).

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
- The host parent `$TMPDIR/connector-mounts-<euid>` is owned by the launcher
  and mode 0700. Each child is mode 0711, with credentials mode 0444, so an
  image-defined UID can read its explicit bind mount. Local connectors access
  the child directly as the launcher UID. The engine must be able to bind
  children beneath the private host parent.
- Host paths are not secrets: bridge names, ownership records, engine argv,
  container metadata and debug logs can disclose them. The private ancestor
  denies other host UIDs access even with the exact path. It does not isolate
  processes running as the launcher UID, host root, or users controlling the
  engine. Workloads receive only their own directory through mount isolation.
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
| `vmm::launch::start` | VMM lifecycle, image admission, and credential mount |
| `Vmm::spawn_recovery`, `VmmRecovery` | A process's background recovery of its `STATE_DIR`, until dropped |
| `Vmm::recover`, `Vmm::recovery_metrics` | One recovery pass now; the process's pass metrics |
| `Vmm::drain_launches` | Bounded wait for this process's launches, teardown included |
| `vmm::release::recover` | One pass: releases resources whose durable owner is dead, and labeled resources whose record is gone |
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
├── vmm/release.rs# Resource release, and one recovery pass
├── vmm/recovery.rs # Recovery's schedule, reporting and metrics
└── container.rs  # Docker/Podman pull, inspect/run capabilities, dial
tests/
├── e2e.rs        # served streams: loopback gRPC, EndpointRouter, in-process
└── fixtures/execution.rs # request shapes shared with the pump unit tests
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
  The internal Spec carries the *sealed* configuration, so a connector may
  describe itself from its non-secret portions.

- **A local data plane's `*.localhost` services get a host-gateway mapping.**
  A `.localhost` name denotes the loopback of whoever resolves it, which inside
  an image container is the container. The `TaskUpdate` URLs are mapped to the
  host so a connector can reach them; only exact `.localhost` names, and only
  in a local plane, so ordinary DNS keeps its configured resolution.

- **A local plane publishes the network ports of a built spec.**
  These are the `network_ports` of the initial request's spec (Apply or Open),
  plus `flow-connector-init`. Requests lacking a built spec publish only
  `flow-connector-init`. Validation builds a spec's ports from the
  `Started.container.advertised_ports` which its image `EXPOSE`s, or from a
  custom connector's `spec.networkPorts`. A VMM's ingress is the same built
  spec's ports (below).

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
  runs under. A VMM request must name an eligible image of its own protocol,
  by exact repository whatever its tag or digest: `capture-python`,
  `derive-python` and `materialize-python` of `ghcr.io/estuary/`. It must
  declare `egress`, and run on a service with VMM capability. It is then
  launched in a VMM or fails; it never falls back to an ordinary container.
  A local or in-process endpoint cannot request it. A start whose execution
  declares `egress` without a VMM is refused first, whatever its endpoint:
  only a VMM enforces egress, and a declaration is never quietly dropped. A
  VMM start's declared hosts and CIDRs are validated here too, before
  anything is pulled, and `policy::egress_cidrs` refuses any CIDR on a public
  plane. Catalog validation checks only their syntax: it does not know the
  plane. Each refusal here, and an unready host's (below), is marked by
  `proto_grpc::connector::refuse_execution`. A connector's own error status
  may have any code, so the mark is how a client such as `flowctl generate`
  tells a refusal from it.
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

- **Engine subprocesses omit platform authority.** `container::engine_command`
  removes `CONSUMER_AUTH_KEYS`, `BROKER_AUTH_KEYS`, `SOPS_AGE_KEY` and
  `FLOW_AUTH_TOKEN` from ordinary and VMM engine clients, including operator
  wrappers. Other environment settings remain inherited for engine selection,
  registry authentication, remote connections and wrapper configuration.
  This reduces secret copies in subprocesses; it does not restrict rootful
  engine authority or change VMM isolation. `container/engine_tests.rs` covers
  invocation paths.

- **A VMM container gets an ordinary connector's limits.** `--memory` is
  `CONNECTOR_MEMORY_LIMIT`, parsed once into bytes and passed as that decimal
  byte count; `--cpus` is `CONNECTOR_CPU_LIMIT`, verbatim. The guest is sized
  within them: RAM is the cap's whole MiB less the overhead, and vCPUs are the
  CPU limit rounded up. The VMM path accepts whole bytes or a decimal number
  suffixed `m` or `g`. The CPU limit is a decimal number of CPUs.

- **A VMM connector reaches its own hosts, its image's, and its task's.**
  Each eligible connector carries default hosts (Python's package index), the
  image may add more as a JSON array of names in its
  `dev.estuary.egress-hosts` label, and the task may add more in its
  execution's `egress.hosts`. `vmm::plan::Egress` is their union, in that
  order and without repeats, each name remembered with where it came from.
  A missing, blank or empty label adds nothing; anything else that isn't
  valid names is refused, on every plane. Names follow
  [`crates/egress`](../egress/README.md), whose `public_policy` writes the
  VMM's `policy.json`. Ordinary launches never read the label.

- **Empty egress permits nothing, on every plane.** Egress declaring no
  host and no CIDR (`egress: {}`) is planned as no names and no CIDRs, so the
  VMM's resolver refuses every name and no address is reachable: not the
  connector's defaults, nor its image's label. Any other declaration is held
  to the union above. Each launch logs which applies, and every permitted
  name with its source, to the task's logs.

- **A task's CIDRs open private destinations, guest and host alike.** On a
  private or local plane, `egress.cidrs` is written into the policy as
  `declaredCidrs`, and the launch's host grant carries a `--cidr` for each
  range, because the host boundary drops private destinations for every VMM
  otherwise. The grant is the one described below for ingress, so it is
  installed, recorded and revoked with it, and a launch with CIDRs but no
  declared port still has one.

- **A VMM launch is planned before anything is created.** `vmm::plan::plan`
  turns the configuration and the connector's image inspection into the
  policy, the `fv_<16 hex>` state directories and socket, the files the
  connector mount must hold, and four podman lines: `boundary verify` from
  the VMM image, the VMM's own `fvm<12 hex>` bridge network, its private rootfs
  volume, and its `run`. The volume uses the inspected image ID so a concurrent
  pull cannot replace the code after its metadata and policy were read. It uses
  Podman's `image` volume driver for a private storage container, mounted
  read-only at `/rootfs`. A `type=image` mount shares the image's mount across
  launches, and Podman 4.9 unmounts it even when removing a container that never
  started, breaking a sibling's pending OCI mount. Usage labels keep the
  requested image reference.
  The lines are those of `crates/connector-vmm-tests`'s `src/launch.rs` plus
  `--platform`, the usage labels, the owner label (below) and
  `--cgroup-parent`, which the reference leaves to a launcher, and with the
  configured limits as written. The plan `podman create`s the container the
  reference `podman run`s, with the same arguments, and the launch then
  `podman start --attach`es it by ID; `plan_reproduces_the_reference_lines`
  holds them to it. `LOG_LEVEL` defaults to `info` on local planes and `warn`
  elsewhere.

- **A VMM launch runs as one task, which holds its ownership record.**
  `vmm::launch` pulls and
  inspects the image with `CONNECTOR_VMM_PODMAN` (skipping the pull of a
  `:local` tag, as ordinary launches do), and plans. It then claims its id by
  making `<STATE_DIR>/fv_<id>.owner` (a new id if it exists), makes the
  connector mount and `fv_<id>` state directory, writes the planned files,
  runs `boundary verify`, creates the network (a new id and record, at most
  three tries, if that fails), creates the rootfs volume and container, and
  starts it. Pulls,
  inspection, verification and the readiness dial race the start's
  abandonment; the commands which create the network, volume and container are never
  interrupted, so each client has exited before teardown looks for what it
  made.

- **A launch owns only what it proves it made, and its record says so.** The
  record is written at mode 0600, `flock`ed and synced as an unnamed `O_TMPFILE`, then
  linked under its name, so it is never seen unlocked or empty, and its claim
  is exclusive. It is append-only JSON lines: a claim naming the id, a random
  owner token and the connector mount, then a line marking each directory
  made by exclusive `mkdir`, written before anything is put in it. The network
  rootfs volume and container carry the token as their `dev.estuary.vmm-owner` label, which
  podman stores as it creates them, and are found by it and nothing else: a
  name never proves ownership. Each also carries its `STATE_DIR` as its
  `dev.estuary.vmm-state-dir` label, and is named for the launch, as `fv_<id>`.
  So are the containers of its boundary runs: the ingress grant's
  `fv_<id>-ingress`, and `fv_<id>-verify` and `fv_<id>-revoke`, which `--rm`
  removes only if they start, and which a release names and labels from its
  record. A directory that already exists is not marked,
  and so never removed. Recovery also writes records, for grants (below):
  a claim naming the token and the ID of the grant's image, and no mount,
  which marks nothing. A record which reads as anything other than these (another
  version, an id or mount of the wrong shape, a repeated or unknown line)
  directs no removal at all, and is kept and reported; a last line without
  its newline is an append that never finished.

- **The record's lock is the owner's liveness, PIDs aside.** The kernel drops
  a `flock` with the last descriptor of its open file, however its process
  ends, across PID namespaces and containers sharing the host filesystem. The
  lock says nothing of the creates the owner asked for. A podman client
  survives its launcher's death, sudo's too, and the podman service finishes
  a request after its client has gone. So whoever takes the lock may release
  a launch before its last create lands, and a later recovery pass removes
  what that create made (below). `STATE_DIR` must be a local
  filesystem with `flock` and `O_TMPFILE`, and persistent if records are to
  survive a reboot.

- **Release is one state machine, for teardown and recovery alike.**
  `vmm::release` finds the containers labeled with the record's token and
  removes them by ID; if that query or removal fails, nothing else is touched,
  since the mount, state and network may still be in use. The ingress grant's
  container is kept once it exits, so it is among them. Its grant is revoked
  next, by its owner token: a start of the grant's container which podman has
  yet to serve then fails, rather than grant again. A failed revoke keeps the
  record. Then networks by the
  label, removed by name (podman 4.9 skips its in-use check when given an ID);
  then rootfs volumes by the same label, removed without force; then the marked
  mount and state. The record is removed, under its lock, only
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

- **A stopped shard's VMM waits for recovery.** A reactor stops a shard by
  dropping its task service, and that service's runtime with it, so the
  launch's task is dropped rather than torn down. The VMM ends within seconds
  of its RPC (connector-init's idle watchdog) and `--rm` removes its
  container, whose bridge goes with it, but its network definition, state,
  record and connector mount wait for recovery. A Validate or Spec, whose
  session ends before its runtime, is torn down by its owner. An exiting
  flowctl waits up to 30 seconds for its own launches' teardowns
  (`Vmm::drain_launches`); one killed, or past that, leaves them to recovery.

- **Recovery runs for the process lifetime.** `Vmm::spawn_recovery`
  runs one pass at once, then another 30 seconds after each ends, so passes
  never overlap, until its `VmmRecovery` is dropped, which abandons the pass
  under way. A process runs one, from whatever lives as long as it does: a
  reactor's connector proxy task service (`TaskServiceConfig.vmm_recovery`),
  not a shard's, so it runs with no shards at all; and a VMM-capable flowctl
  invocation, at best, until it exits. A pass first makes the checks of
  `STATE_DIR` a launch makes, and touches nothing if they fail. It tries the
  lock of every `fv_<16 hex>.owner` beneath `STATE_DIR`, skipping those
  held, by a live owner or another releaser, and releases the rest. It then
  lists the containers, networks and volumes labeled with its `STATE_DIR`,
  and removes each whose launch's record is gone: a podman service may create
  one after the release that removed its record. Each kind is listed before
  its records are looked for. A launch claims its record before its first
  create, so a live launch's resource always has its record. A record of any
  content keeps its resources, and is not read. Processes sharing a
  `STATE_DIR` each recover it; whichever takes a lock releases that record.

- **A grant whose record is gone is recorded before it is removed.** A grant's
  container may be created, and started, after the release of its launch has
  revoked its token and removed its record; a remote `podman run` sends its
  create and start apart. Its labels are then the grant's only evidence.
  `release::release_grant` inspects it, checks its `STATE_DIR` label and
  token, and claims a record of the launch's name naming that token and the
  ID of the container's image, atomically and locked. The image ID preserves
  revocation across tag updates. Release removes the container before
  revoking permissions, so pending starts fail. It removes the record after
  cleanup succeeds. A competing claimant leaves the grant to the record's
  owner. Until recording succeeds, pruning must preserve the helper as the
  grant's only evidence.

- **Recovery reports to its process, and counts its problems.** What a pass
  leaves (a resource it couldn't remove or list, a malformed record) goes to
  tracing, not to any session: it may be no live task's. Each problem is
  logged when first seen, then again only once ten minutes have passed while
  it persists. `Vmm::recovery_metrics` counts failed passes, the problems the
  last pass left, and when a pass last left none; a reactor exports them
  (`go/bindings/metrics.go`). Neither fails a launch or a readiness check.

- **Nothing without a record or the `STATE_DIR` label is touched.** `fv_*`
  containers, networks and state directories left by a launcher that wrote no
  records, or no `dev.estuary.vmm-state-dir` label, and mounts not named
  `mount-fv_<id>`, are not this crate's to judge. With every launcher
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

- **A VMM's connector mount follows the shared contract.** Its mode-0711
  `mount-fv_<id>` directory lives beneath the launcher's private mode-0700
  `$TMPDIR/connector-mounts-<euid>` parent. The child is bound read-only at
  its own path into the container and at `/connector-mount` in the guest,
  which `CONNECTOR_MOUNT` names. TMPDIR must name the same directory for the
  launcher and engine. The shared parent helper rejects links, another user's
  directory and directories writable by others, then sets mode 0700.
  When credentials are configured, the launch writes and rotates
  `task-update.json` there until teardown starts. `STATE_DIR` must also be a
  real directory owned by the launcher, mode 0700. Recovery uses the same UID
  and record format across launcher restarts.

- **VMM control uses `init.sock`; container networking uses the built spec's TCP ports.**
  A VMM's ingress is the `network_ports` of its initial request's built spec
  (Apply or Open), less connector-init's, whatever its image exposes. A
  request lacking a built spec (Spec, Discover, Validate) has none. A VMM with
  ports reports its container IPv4 address, and the image's advertised ports
  as an ordinary container does; one without reports no address. The host
  proxy dials that address; the VMM forwards to the guest. Ports are not
  published on host interfaces. Host permissions are
  keyed by the VMM bridge and declared port, owned by the launch token, and
  recorded with the VMM image used to revoke them during teardown or recovery.
  Ingress and its replies work independently of egress policy. Connector-init's
  reserved port 49092 is never authorized. The control socket's parent has mode
  0700, restricting access to the launcher's host UID and root.

- **A VMM is ready when connector-init's health is SERVING.** The launch
  dials `init.sock` and calls standard `grpc.health.v1.Health/Check` for the
  whole server, retrying failures every 100ms within one 60-second deadline.
  The `start --attach` client's exit ends the wait, and its error carries the
  client's exit status. Stderr is only logging.

- **A dial left unanswered for 1s is abandoned for another.** libkrun holds a
  dial made before the guest listens, unanswered, until its vsock reaper
  closes it 5s later. connector-init's watchdog ends it 5s after it starts
  serving unless a connector RPC has begun, so waiting such a dial out leaves
  the Spec too little time: the VMM exits before readiness, or drops the Spec.

- **VMM release deadlines use a paused clock in tests.** `vmm/release.rs`
  checks each command's deadline and snapshots failure-state transitions.
  The KVM suite checks real command termination and subsequent recovery.
- **VMM readiness is tested without KVM.** `vmm/launch/readiness_tests.rs`
  exercises the real launcher with a scripted engine and Unix socket peers,
  checking startup, cancellation, the dial's deadline, a health which is not
  SERVING, a dial held unanswered as libkrun holds one, and resource cleanup.
  A scripted peer also serves derive Spec: health flapping from NOT_SERVING
  to SERVING is redialed, and a guest which dies once SERVING, before or
  while its first Spec is held, fails that Spec and is released.

- **Image admission identifies images by repository.** The public-plane
  refusal of Python captures, derivations and materializations applies to
  every tag, digest, and bare spelling of their repositories, for ordinary
  launches. A VMM launch admits them through VMM eligibility, and applies the
  same image declarations, usage-rate validation, and task-secret checks as
  ordinary execution.

- **A response-stream error is terminal.** Panics in the spawned handler are
converted into a terminal gRPC status rather than appearing as clean EOF.
