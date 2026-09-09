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
  session. Response sends are best-effort, and dropping the response stream
  does not necessarily release held resources.

## Key types

| Type / item                     | Role                                                                     |
| ------------------------------- | ------------------------------------------------------------------------ |
| `Service`                       | The gRPC service, and its in-process `spawn_connector` entry point        |
| `ServiceRouter`, `Service::new_local` | The in-process router and local-context pair                       |
| `LOCAL_ISSUER`                  | Issuer of local self-signed bearers; public for served test fixtures      |
| `proto_grpc::connector`         | Client routing, identity, bearer, and stream helpers                      |
| `LogSink` / `LogDest`           | Routes connector logs and container lifecycle records                     |
| `flow_runtime_protocol`         | Image inspection, for callers which only need an image's protocol         |
| `protocol::start`               | The start pipeline every connector goes through                          |
| `protocol::Protocol`            | Per-protocol trait: Spec request, RPC, and endpoint extraction             |
| `protocol::StartContext`        | Plain data a start needs: plane, network, logging, task, process, secrets |
| `Rotation`                      | Signer and URLs by which a connector rotates what it manages; None locally |
| `protocol::Endpoint`            | Normalized endpoint: image, local subprocess, or in-process connector     |

## Layout

```
src/
├── lib.rs        # ServiceRouter re-export, LogSink, Started, RuntimeProtocol
├── service.rs    # Service / ServiceImpl, spawn_connector, tonic Connector impl
├── router.rs     # ServiceRouter and the local bearer issuer
├── serve.rs      # per-stream: authn/authz, extract, start, pump, teardown
├── protocol.rs   # Protocol trait, StartContext, and the one start pipeline
├── capture.rs    # Protocol impl: capture endpoints and RPC
├── derive.rs     # Protocol impl: derive endpoints and RPC, incl. derive-sqlite
├── materialize.rs# Protocol impl: materialize endpoints and RPC, incl. Dekaf
├── container.rs  # docker pull / inspect / run, connector-init dial, Guard
└── tests.rs      # authz, stream semantics, and end-to-end coverage
```

## Non-obvious details

- **The request stream owns lifetime.** Opening eagerly spawns the handler.
Connectors may have server-side resources that release only when the client
closes its requests, so response receiver closure alone does not promise
cancellation.

- **The connector request stream begins with an internal Spec exchange.** The
  client's initial request follows after its configuration is unsealed, then
  subsequent client requests pass directly to the transport.
  `Started` carries the `Spec` response so the client can use it as well.

- **Endpoint configuration resolves through `unseal::resolve`.** That one joint
  chooses between whole-document `sops` unsealing and a merge-patched `secrets`
  stanza. `protocol::start` wraps it. A task-less Spec passes its configuration
  through untouched, and IAM injection follows every other outcome.
  Plaintext values are never cached by this crate.

- **A task may name a secret which isn't its sibling, if its image vouches for
  it.** The image rule admits `<prefix>/<repo>/<leaf>`, where `<repo>` is the
  running image with its tag or digest stripped: it's how a vendor's OAuth
  client secret reaches every task using that connector. An image opts in by
  listing full secret names in a `dev.estuary.secrets` label.

  Two enforcement points, for two different failure modes.
  `container::check_image_secrets` runs between `docker inspect` and `docker
  run`, so a stanza the control-plane will refuse never reaches a container;
  it also checks every *declared* name against the image rule, used or not, so
  a typo'd label fails at start rather than whenever some task first depends
  on it. `container::check_local_secrets` is the same check for a local or
  in-process connector, which has no image and so gets siblings only -- run in
  `protocol::start` before anything is pulled or spawned.

  The real enforcement is the control-plane's: `protocol::start` captures the
  repository before `connect` consumes the endpoint, and threads it into each
  `SecretResolver::decrypt`, which attests it as `estuary.dev/image-name` in
  the decrypt JWT's selector. Being absent is meaningful -- it's how the
  control-plane tells a local connector from an image one.

- **A connector rotates its own credentials through injected environment.**
  A reactor holds `Rotation`: the data-plane signer, plus the control-plane and
  config-encryption URLs. `protocol::start` mints an
  `AUTHORIZE | TASK_UPDATE` token scoped to the task, and passes
  `FLOW_ROTATION_TOKEN`, `FLOW_CONTROL_API` and `FLOW_CONFIG_ENCRYPTION_URL` to
  `docker run --env` and to the local subprocess alike. Environment rather than
  a mounted file: this host already holds the key which signed the token, so
  `docker inspect` here is not a new boundary.

  Only a session's token carries `estuary.dev/build`, which
  `/task/update-config` requires -- a connector a publication behind must not
  clobber a model it hasn't seen -- and only a session gets the week-long
  lifetime; the unary kinds get ten minutes. A session's `token_restart_at` is
  the earlier of the IAM and rotation deadlines, so the session re-establishes,
  and both credentials re-mint, before either lapses.

  `Service::new_local` (flowctl, tests) holds no `Rotation` and injects
  nothing, which is how a connector knows to rotate in memory only.

  A container does not share its host's view of the network, so the two URLs
  are rewritten on their way into an image connector: `CONNECTOR_URL_REWRITE`
  is one `<from-suffix>=<to-host>` (`container::url_rewrite`) which
  `container::rewrite_url` applies to a matching URL host, replacing it whole
  -- the container resolves exactly one name for its host, and the port is what
  tells the rewritten services apart. It is how a local stack's
  `*.flow.localhost` services stay reachable from inside a container, and it is
  unset in every deployed data plane. Local and in-process connectors run on
  the host and are handed the URLs unchanged.

- **Invalid later requests close connector input and terminate the session.**
The handler owns request validation, so malformed input cannot disappear as a
clean connector EOF.

- **Logs and protocol responses share one bounded response channel.** This
gives them the same backpressure and ordering. Log-producer lifetime signals
that stderr has been read through, which the handler awaits before terminal
EOF or status. A client which does not drain responses may park the handler.

- **Dropping an image connector's `Guard` kills `docker run`.** This closes
  stderr so its log pump can finish before the stream terminates. Local
  connectors couple process and stderr completion in `connector_init::rpc::bidi`.

- **The minted selector always admits `<spec>`.** `connector_bearer` scopes to
  `{task-type: [type], task-name: [name, <spec>]}`, so one bearer shape serves
  both a session's Open and a unary Spec — which names no task. Tokens are one
  minute and are checked at stream open only (no `expiry_guard`), consistent
  with the leader and shuffle bearers.

- **`Start.sqlite_vfs_uri` is runtime-internal.** It's set only by an in-process
  shard hosting a recorded recovery log, and only for a `Sqlite` derivation;
  any other connector type rejects it as `InvalidArgument`.

- **A response-stream error is terminal.** Panics in the spawned handler are
converted into a terminal gRPC status rather than appearing as clean EOF.
