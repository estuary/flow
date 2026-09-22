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
  and MUST NOT set `start`. Later requests pass straight through.
- `Started` is the first protocol response and carries the connector's Spec
  response. Logs may precede it, so clients read until `Started`. Later items
  are logs or protocol responses of the established type.
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
| `protocol::StartContext`        | Plain data a start needs: plane, network, logging, task, process, secrets |
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

- **A response-stream error is terminal.** Panics in the spawned handler are
converted into a terminal gRPC status rather than appearing as clean EOF.
