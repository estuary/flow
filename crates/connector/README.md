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
| `protocol::start`               | The start pipeline every connector goes through, dispatching on endpoint and execution |
| `image::connect`                | Ordinary image start: pull, inspect, admit, and run its container         |
| `protocol::Protocol`            | Per-protocol trait: Spec request, RPC, and endpoint extraction             |
| `protocol::StartContext`        | Plain data a start needs: plane, network, logging, task, process, secrets, execution, VMM |
| `Vmm`, `Vmm::from_env` | VMM capability and configuration from `CONNECTOR_VMM_*` |
| `vmm::vmm_for` | Admission decision for requested execution |
| `vmm::plan::plan` | Pure plan of a VMM launch |
| `vmm_argv` (`vmm::argv`) | Pure podman lines of a VMM launch, shared with `crates/connector-vmm-tests` |
| `vmm::launch::start` | VMM lifecycle, image admission, and credential mount |
| `Vmm::spawn_recovery`, `VmmRecovery` | A process's background recovery of its `STATE_DIR`, until dropped |
| `Vmm::recover`, `Vmm::recovery_metrics` | One recovery pass now; the process's pass metrics |
| `Vmm::drain_launches` | Bounded wait for this process's launches, teardown included |
| `vmm::release::recover` | One pass: releases resources whose durable owner is dead, and labeled resources whose record is gone |
| `vmm::check_spec_execution` | Keeps execution and egress fixed through every session request |
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
├── capture.rs    # Protocol impl: capture endpoints and RPC
├── derive.rs     # Protocol impl: derive endpoints and RPC, incl. derive-sqlite
├── materialize.rs# Protocol impl: materialize endpoints and RPC, incl. Dekaf
├── vmm.rs        # VMM configuration and admission
├── vmm/argv.rs   # Pure podman lines of a launch
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

- **A response-stream error is terminal.** Panics in the spawned handler are
converted into a terminal gRPC status rather than appearing as clean EOF.

## VMM execution

A task with `vmm: true` runs its connector in a micro-VM, launched as a
rootful podman container of `flow-connector-vmm`
([`crates/connector-vmm`](../connector-vmm/README.md)). Each step names the
module that documents it.

1. **Configure.** `Vmm::from_env` reads `CONNECTOR_VMM_*` once.
   `CONNECTOR_VMM_STATE_DIR` enables VMM execution. The image is
   `ghcr.io/estuary/connector-vmm:<FLOW_VERSION>` (`vmm::DEFAULT_IMAGE`),
   the tag `ci:docker-images` publishes for the same build, unless
   `CONNECTOR_VMM_IMAGE` names another for development.
2. **Admit.** `vmm::vmm_for` decides each start's `Execution` before
   anything is pulled. A VMM start names an eligible Python image of its own
   protocol, declares `egress`, and never falls back to an ordinary
   container. Egress without a VMM is refused. Refusals carry
   `proto_grpc::connector::refuse_execution`'s mark.
   `vmm::check_spec_execution` holds every later built spec to the session's
   execution.
3. **Plan.** `vmm::plan::plan` is the pure plan of a launch: its id, paths,
   podman lines (`vmm::argv`), and egress. Egress is the union of the
   connector's default hosts, the image's `dev.estuary.egress-hosts` label
   and the task's `egress.hosts`; `egress: {}` permits nothing. The guest is
   sized within `CONNECTOR_MEMORY_LIMIT` and `CONNECTOR_CPU_LIMIT`
   (`vmm::memory_bytes`).
4. **Launch.** `vmm::launch::start` runs one task per launch. It claims
   `<STATE_DIR>/fv_<id>.owner`, resolves the VMM image to an ID, runs
   `boundary verify`, creates the network, volume and container, waits for
   connector-init's readiness byte on stderr, and dials `init.sock`.
5. **Own.** `vmm::record` is the durable ownership record. Its `flock` is
   the owner's liveness. Resources carry the launch's token and `STATE_DIR`
   as labels, and nothing without a record or that label is touched.
6. **Release.** `vmm::release` is one state machine for teardown and
   recovery. A failed removal keeps the record for a later pass.
7. **Recover.** `Vmm::spawn_recovery` runs passes for the process's life
   (`vmm::recovery`), releasing what dead owners left, including labeled
   resources whose record is gone.

Ingress is the built spec's `network_ports`, granted on the host by the
launch's token and revoked with the image its record names. The connector
mount follows the contract above, at `/connector-mount` in the guest.
`container::engine_command` removes platform credentials from every engine
subprocess's environment.

`vmm/launch/readiness_tests.rs` drives the launcher against a scripted
engine without KVM, and `vmm/release.rs` tests release with a paused clock.
[`crates/connector-vmm-tests`](../connector-vmm-tests/README.md) runs real
VMs.
