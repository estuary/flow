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

`proto_flow::connector::Protocol` states each protocol's messages and how
they're wrapped.

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
  must match that spec's `execution`.
- A failure before the connector runs ends the stream with a `Status` and no
  `Started`.
- Clients close the request stream to abandon a session. Response sends are
  best-effort, and dropping the response stream also ends the session.

## The connector's side of the contract

Every endpoint is dispatched to something which serves the same
`connector.Connector` messages, under its own first-request rule:

- **Image** connectors run `flow-connector-init` as their entrypoint, which
  serves the contract on its port. See `crates/connector-init/README.md`.
- **Local** connectors run `connector_init::serve` in-process, over the same
  subprocess framing and logging code.
- **In-process** connectors (derive-sqlite, Dekaf) run
  `connector_init::serve::run` through `protocol::in_process`.

The service opens each with `{start, <internal Spec request>}`. The Spec
carries the *sealed* configuration, so a connector may describe itself from its
non-secret portions. The first response is `Started { codec, spec }`, whose Spec
the client may use as well. Unsealing needs the Spec's `config_schema_json`, so
the service unseals only then, and sends the client's rewritten initial request.

The crate interface is the trust boundary. `flow-connector-init` runs in the
connector's container, so whatever it sends is treated like connector output:

- the service overwrites every `Started` field but `codec` and `spec`;
- every `Log` frame passes through `policy::sanitize_connector_log`, which
  also repairs whatever of it would fail to serialize;
- every other frame is forwarded as-is, including a second `Started` or a frame
  of another protocol. Clients validate what they receive:
  `proto_grpc::connector::next` rejects any non-`Log` frame that isn't of the
  stream's protocol.

`Start` carries no secrets. Secrets reach a connector only by unsealing into
its initial request.

## Task logs and application logs

Task logs are `ops::Log` frames: the connector's stderr, records from
`connector_init::serve` such as the exit status, and this crate's own records
through `LogSink`. `tracing` is application logging, and never reaches task logs.

`docker run`'s stderr is application logging. It carries container-runtime
errors and `flow-connector-init`'s `tracing` output, which `container::pump_stderr`
emits as warnings. It's capped at `container::STDERR_BUDGET` bytes, past which
the session fails with `ResourceExhausted`.

## Connector process environment

Image and local connectors receive the same runtime-owned environment contract.
These values override same-named entries of a local connector's model-provided
`env`; connectors may rely on them regardless of how they are launched.
In-process connectors do not receive a process environment.

- `CONNECTOR_MOUNT` names the connector mount described below. It's the only
  variable this crate sets.
- `LOG_FORMAT` is always `json`, and `LOG_LEVEL` is the stream's
  `start.log_level`. `connector_init::serve` sets both. When `log_level` is
  unspecified, this crate defaults it to `info` on development planes and
  `warn` elsewhere.

## Connector mount

Every image and local connector run receives a **connector mount**, which is
a defined file layout of resources handed to the connector for its use.

- `CONNECTOR_MOUNT` names the directory, which MUST exist though MAY be empty.
  A connector tests for the files it wants.
- Image connectors bind-mount the directory **read-only**. Writable areas
  are read-write mounts nested within it:
  - `scratch/` is present under VMM execution only. The image's user and
    group own it, mode 0770. It is on the VM's scratch disk, as are `/tmp`
    and `/var/tmp`, and `CONNECTOR_VMM_DISK_MIB` bounds all three together.
    The disk is discarded with the VM.
  - `persist/` is reserved for persistent storage, which no run has yet.
    The directory is absent until then.
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
| `LogSink` / `LogDest`           | Routes this crate's own task logs, such as image pulls and container starts |
| `protocol::start`               | The start pipeline every connector goes through, dispatching on endpoint and execution |
| `protocol::Protocol`            | Per-protocol endpoint extraction, over `proto_flow::connector::Protocol`  |
| `connector_init::serve`         | The connector's side of the contract, for image and local connectors      |
| `protocol::StartContext`        | Plain data a start needs: plane, network, logging, task, process, secrets, VMM |
| `image::connect`                | Ordinary image start: pull, inspect, admit, and run its container         |
| `Vmm`, `Vmm::from_env` | VMM capability and configuration from `CONNECTOR_VMM_*` |
| `vmm::vmm_for` | Admission decision for requested execution |
| `vmm::plan::plan` | Pure plan of a VMM launch |
| `vmm_argv` (`vmm::argv`) | Pure podman lines of a VMM launch, shared with `crates/connector-vmm-tests` |
| `vmm::launch::start` | VMM lifecycle, image admission, and credential mount |
| `flow-connector-vmm-prune` | Host command for one cleanup pass under a state-directory lock |
| `vmm::release::recover` | One pass: releases resources whose durable owner is dead, and labeled resources whose record is gone |
| `TaskUpdate`                    | Signer and URLs by which a connector updates its config and secrets |
| `protocol::Endpoint`            | Normalized endpoint: image, local subprocess, or in-process connector     |
| `protocol::create_connector_mount` | Creates an ordinary run's connector mount; see above                  |
| `Guard`                         | Host resources of one run: container process or VMM launch, and an ordinary run's refresh task and mount |

## Layout

```
src/
├── lib.rs        # ServiceRouter re-export, LogSink, Started, RuntimeProtocol
├── service.rs    # Service / ServiceImpl, spawn_connector, tonic Connector impl
├── router.rs     # ServiceRouter and the local bearer issuer
├── serve.rs      # per-stream: authn/authz, extract, start, pump, teardown
├── protocol.rs   # Protocol trait, StartContext, start pipeline, connector mount
├── policy.rs     # pure product policy: image/secret admission, usage, token lifetimes
├── image.rs      # Estuary image declarations and ordinary image connection
├── capture.rs    # Protocol impl: capture endpoints
├── derive.rs     # Protocol impl: derive endpoints, incl. derive-sqlite
├── materialize.rs# Protocol impl: materialize endpoints, incl. Dekaf
├── vmm.rs        # VMM configuration and admission
├── vmm/argv.rs   # Pure podman lines of a launch
├── vmm/plan.rs   # Pure launch planning
├── vmm/launch.rs # Claimed launch and credential lifecycle
├── vmm/record.rs # Durable ownership and locks
├── vmm/release.rs# Resource release, and one recovery pass
├── container.rs  # Docker/Podman pull, inspect/run capabilities, dial
└── test_util.rs  # `test_util` feature: request shapes shared by unit tests and e2e.rs
tests/
└── e2e.rs        # served streams: loopback gRPC, EndpointRouter, in-process
```

## Non-obvious details

- **Either stream ends a session.** Opening eagerly spawns the handler, which
races a closed request stream against a dropped response stream at every stage,
including connector startup. Only the handler's return releases the run's
`Guard`, so a client which walks away from a wedged connector must not be able
to leave it running.

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
The connector's side of the contract validates them, and fails its stream with
the terminal status this service forwards. Malformed input cannot disappear as
a clean connector EOF.

- **Logs and protocol responses share one bounded response channel.** This
gives them the same backpressure. Connector logs travel in-band, and precede
the connector's terminal EOF or status. `LogSink` producer lifetime signals
that this crate's own logs are written, which the handler awaits before
terminal EOF or status. A client which does not drain responses may park the
handler.

- **Dropping a `Guard` kills `docker run`, then frees the connector mount.**
  The `docker run` child is SIGKILLed and the token refresh signaled to stop
  *before* the mount is removed. A local subprocess is instead killed by
  dropping its `connector_init::serve` stream, which the handler drops ahead
  of its `Guard`. Its `Guard` holds no process — only the mount.
  Teardown is started in this order and isn't awaited, so a connector which
  is still exiting may see its mount removed.
  Note that killing `docker run` does NOT stop the container. Ending the
  session's stream does: `flow-connector-init` serves one stream per container
  and exits once it's done. It self-exits after 5–10s if no stream arrives.

- **The minted selector always admits `<spec>`.** `connector_bearer` scopes to
  `{task-type: [type], task-name: [name, <spec>]}`, so one bearer shape serves
  both a session's Open and a unary Spec — which names no task. Tokens are one
  minute and are checked at stream open only (no `expiry_guard`), consistent
  with the leader and shuffle bearers. A non-Spec request which carries the
  `<spec>` sentinel is rejected.

- **`Start.sqlite_vfs_uri` is runtime-internal.** It's set only by an in-process
  shard hosting a recorded recovery log, and only for a `Sqlite` derivation;
  any other connector type rejects it as `InvalidArgument`.

- **A response-stream error is terminal.** Panics in the spawned handler are
converted into a terminal gRPC status rather than appearing as clean EOF.

## VMM execution

A task with `vmm: true` runs its connector in a micro-VM, launched as a
rootful podman container of `flow-connector-vmm`
([`crates/connector-vmm`](../connector-vmm/README.md)). Each step names the
module that documents it.

VMMs run only on Linux hosts, though the crate builds elsewhere for
`flowctl`. Off Linux, admission is unchanged, `vmm::launch::start` refuses
each start as `FAILED_PRECONDITION` before it pulls or creates anything.

1. **Configure.** `Vmm::from_env` reads `CONNECTOR_VMM_*` once.
   `CONNECTOR_VMM_STATE_DIR` enables VMM execution. The image is
   `ghcr.io/estuary/connector-vmm:<FLOW_VERSION>` (`vmm::DEFAULT_IMAGE`),
   the tag `ci:docker-images` publishes for the same build, unless
   `CONNECTOR_VMM_IMAGE` names another for development.
2. **Admit.** `vmm::vmm_for` decides each start's `Execution` before
   anything is pulled. A VMM start names an eligible Python image of its own
   protocol, declares `egress`, and never falls back to an ordinary
   container. Egress without a VMM is refused. Refusals carry
   `proto_grpc::connector::refuse_execution`'s mark. A stream's first Apply
   or Open must carry the requested execution in its built spec. The
   execution then holds for the stream.
3. **Plan.** `vmm::plan::plan` is the pure plan of a launch: its id, paths,
   podman lines (`vmm::argv`), and egress. Egress is exactly the task's
   `egress`; neither the connector nor its image adds a host, and
   `egress: {}` permits nothing. The guest is sized within
   `CONNECTOR_MEMORY_LIMIT` and `CONNECTOR_CPU_LIMIT` (`vmm::memory_bytes`).
4. **Launch.** `vmm::launch::start` runs one task per launch. It claims
   `<STATE_DIR>/fv_<id>.owner`, resolves the VMM image to an ID, runs
   `boundary verify`, creates the network, volume and container, waits for
   connector-init's readiness byte on stderr, and dials `init.sock`.
   `protocol::start` opens `connector.Connector` over it, as for an ordinary
   container. The VMM's stderr follows `container::pump_stderr`'s rule.
5. **Own.** `vmm::record` is the durable ownership record. Its `flock` is
   the owner's liveness. Resources carry the launch's token and `STATE_DIR`
   as labels, and nothing without a record or that label is touched.
6. **Release.** `vmm::release` is one state machine for teardown and
   recovery. A failed removal keeps the record for a later pass.
7. **Prune.** `flow-connector-vmm-prune` releases abandoned records and
   labeled resources whose record is gone.

Ingress is the built spec's `network_ports`, granted on the host by the
launch's token and revoked with the image its record names. The connector
mount follows the contract above, at `/connector-mount` in the guest.

`vmm/launch/readiness_tests.rs` drives the launcher against a scripted
engine without KVM, and `vmm/release.rs` tests release with a paused clock.
[`crates/connector-vmm-tests`](../connector-vmm-tests/README.md) runs real
VMs.
