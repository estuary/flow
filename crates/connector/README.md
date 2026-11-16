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
It exists only under the V2 runtime; the V1 `runtime` crate is untouched.

- `CONNECTOR_MOUNT` names the directory, which MUST exist though MAY be empty.
  A connector tests for the files it wants.
- Image connectors bind-mount the directory **read-only**. Writable areas, like
  `persist/`, are nested read-write mounts within it.
- `persist/` is present only if `Start.volume` is set.
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
| `protocol::start`               | The start pipeline every connector goes through                          |
| `protocol::Protocol`            | Per-protocol endpoint extraction, over `proto_flow::connector::Protocol`  |
| `connector_init::serve`         | The connector's side of the contract, for image and local connectors      |
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
├── capture.rs    # Protocol impl: capture endpoints
├── derive.rs     # Protocol impl: derive endpoints, incl. derive-sqlite
├── materialize.rs# Protocol impl: materialize endpoints, incl. Dekaf
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

- **A local data plane's `*.localhost` services get a host-gateway mapping.**
  A `.localhost` name denotes the loopback of whoever resolves it, which inside
  an image container is the container. The `TaskUpdate` URLs are mapped to the
  host so a connector can reach them; only exact `.localhost` names, and only
  in a local plane, so ordinary DNS keeps its configured resolution.

- **A local plane publishes the network ports of a built spec.**
  These are the `network_ports` of the initial request's spec (Apply or Open),
  plus `flow-connector-init`. Requests lacking a built spec publish only
  `flow-connector-init`. Validation builds a spec's ports from the
  `Started.container.advertised_ports` which its image `EXPOSE`s.

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
