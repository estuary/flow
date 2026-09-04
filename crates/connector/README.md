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

All three paths run the identical `Verified<Claims>` through one `serve`.
Clients use `proto_grpc::connector::Router`: `ServiceRouter` opens an
in-process service and `EndpointRouter` dials a gRPC endpoint.

`runtime-next` depends on this crate, never the reverse. The `runtime` crate
owns its container, image, and local-connector implementation independently.

## Protocol contract

- The **first** request sets `start` **and** exactly one protocol request. That
  request determines the connector type for the life of the stream, and is the
  only one inspected: its endpoint configuration is extracted and unsealed, and
  its task identity is authorized against the bearer's claims.
- Every **later** request sets exactly one protocol request of that same type
  and MUST NOT set `start`. Later requests pass straight through.
- The first *protocol* response is `Started`, carrying the connector's Spec
  response. Logs of a connector which is
  still starting — an image pull runs for minutes — stream ahead of it, so a
  client reads until `Started` rather than expecting it first. Every response
  after it is exactly one of `log` or a protocol response of the stream's type.
- A failure before the connector runs ends the stream with a `Status` and no
  `Started`.
- The stream ends only after **both** the connector's protocol responses and
  its stderr have been read through, so a terminal `Status` is always the
  stream's last word and is preceded by every log the connector emitted.

## Key types

| Type / item                     | Role                                                                     |
| ------------------------------- | ------------------------------------------------------------------------ |
| `Service`                       | The gRPC service, and its in-process `spawn_connector` entry point        |
| `ServiceRouter`, `Service::new_local` | The in-process router and local-context pair                       |
| `LOCAL_ISSUER`                  | Issuer of local self-signed bearers; public for served test fixtures      |
| `proto_grpc::connector`         | Client routing, identity, bearer, and stream helpers                      |
| `LogSink` / `LogDest`           | Where connector logs and container lifecycle records go                   |
| `flow_runtime_protocol`         | Image inspection, for callers which only need an image's protocol         |

## Layout

```
src/
├── lib.rs        # ServiceRouter re-export, LogSink, RuntimeProtocol
├── service.rs    # Service / ServiceImpl, spawn_connector, tonic Connector impl
├── router.rs     # ServiceRouter and the local bearer issuer
├── serve.rs      # per-stream: authn/authz, extract, start, pump, teardown
├── capture.rs    # capture start + extract_endpoint
├── derive.rs     # derive start + extract_endpoint
├── materialize.rs# materialize start + extract_endpoint
├── container.rs  # docker pull / inspect / run, connector-init dial, Guard
├── image.rs      # image connector: container + protocol RPC
├── local.rs      # local subprocess connector
└── tests.rs      # identity, authz matrix, loopback and in-process end-to-end
```

## Non-obvious details

- **The response channel is bounded (16) and shared** by protocol responses and
  connector logs, so a stalled consumer back-pressures the connector — by
  design. Nothing buffers logs anywhere else: a `LogSink` send awaits that
  channel's permit, and `connector_init::rpc::bidi` awaits the sink in turn, so
  a chatty connector blocks on its own stderr.

- **The last `LogSink` clone dropping is the read-through signal.** Every clone
  lives inside a log pump, and a stream's sink holds a `oneshot::Sender` it
  never sends on — so its drop is what `serve` awaits during `draining`, ahead
  of the stream's last word. That, and not the log path itself, is why a sink
  is an `Arc`.

- **Teardown is SIGKILL-driven.** Dropping the container `Guard` kills the
  `docker run` client, which closes the container's stderr, which lets its log
  pump emit "stopped connector container" and release the last `LogSink`. Only
  then does the stream terminate. A client which hangs up — in-process, by
  dropping its receiver — cancels the stream's start-and-pump outright, which
  drops the Guard the same way; nothing waits on the connector to notice that
  its request stream ended. A local connector needs no `Guard`:
  `connector_init::rpc::bidi` already reads its subprocess's stderr through
  before the stream's terminal item.

- **Every start exchanges Spec.** Capture and materialize use its
  `config_schema_json` for the sops overlay and IAM extraction. `Started`
  carries the response so the client can use it without another request.

- **The minted selector always admits `<spec>`.** `connector_bearer` scopes to
  `{task-type: [type], task-name: [name, <spec>]}`, so one bearer shape serves
  both a session's Open and a unary Spec — which names no task. Tokens are one
  minute and are checked at stream open only (no `expiry_guard`), consistent
  with the leader and shuffle bearers.

- **`Start.sqlite_vfs_uri` is runtime-internal.** It's set only by an in-process
  shard hosting a recorded recovery log, and only for a `Sqlite` derivation;
  any other connector type rejects it as `InvalidArgument`.

- **Only the `spawn_*` adapters send `Err`,** and only after `serve` returns —
  the same rule as `runtime-next`'s shard service. So a response stream's `Err`
  is unambiguously the whole stream's outcome.

- **Secrets-stanza resolution is not here yet.** `unseal::overlay` /
  `unseal::decrypt_sops` handle the sops case; a secrets arm belongs alongside
  them in each protocol's `start`.
