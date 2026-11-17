# connector-init

`flow-connector-init` is bind-mounted as the entrypoint of every image
connector. It starts the connector's real entrypoint (from the image's inspect
JSON) and serves it over gRPC on `--port`. It's versioned with the reactor,
which ships it, rather than with connector images. It's a static musl binary
with light dependencies, because it runs inside arbitrary amd64 images.

This crate is the untrusted, in-container half of running a connector. The
`connector` crate is the trusted reactor half, and depends on this one.
Anything this crate sends is treated like connector output.

## Services

- **`connector.Connector`**, served by `serve::serve` and used by the V2
  runtime (`crates/connector`). It serves exactly one stream per process, and
  rejects a second. The process exits once that stream is done.
- **The per-protocol services** (`capture.Connector`, `derive.Connector`,
  `materialize.Connector`), used by the V1 runtime (`crates/runtime`). Each RPC
  starts a new connector process through `rpc::bidi`. An idle watchdog exits
  the process 5–10s after the last RPC, and also covers a `connector.Connector`
  stream which never arrives.

## `connector.Connector` contract

- The first request sets `start` and a Spec request, and the connector's Spec
  response is returned as `Started { codec, spec }`. Any other first request
  is rejected.
- Later requests are protocol requests of the first request's protocol, and
  must not set `start`. An invalid one fails the stream.
- Connector stdout returns as protocol responses, and stderr as `Log`s.
  Connector logs aren't ordered against its responses, as they're written to
  separate pipes.
- `serve` writes its own task-relevant events as `Log`s, filtered by
  `start.log_level`. They're ordered relative to the responses it forwards.
- Every log precedes the stream's EOF or terminal status. A failed exit's
  terminal status carries the connector's last log.

`serve` sets the connector's `LOG_LEVEL` and `LOG_FORMAT` from `start`. It
checks the protocol version of the Spec it returns in `Started`.

`serve` is generic over `proto_flow::connector::Protocol`, which states each
protocol's messages, how they're wrapped, and the shape of its Spec exchange.
It runs the connector through `rpc::spawn`, as V1's services do by way of
`rpc::bidi`.

Frozen `derive-{typescript,python}:dev` images serve one request per process.
`inspect::Image::is_frozen_derive` detects them from the image's `RepoTags`.
`serve` then runs their Spec in a process of its own, and a second process
serves the stream.

## Key types and entry points

| Item              | Role                                                                 |
| ----------------- | -------------------------------------------------------------------- |
| `run` / `Args`    | The binary's server: binds `--port`, then writes a readiness byte    |
| `serve::serve`    | Serves one `connector.Connector` stream over connector processes     |
| `serve::run`      | Returns a connector's Spec as `Started`, then pumps the stream       |
| `rpc::spawn`      | Runs a connector process over requests and responses                 |
| `Codec`           | The connector's stdin/stdout framing: length-prefixed proto, or JSON |
| `inspect::Image`  | Parsed `docker image inspect` output                                 |

## Logging

`serve` runs in-process within the reactor and `flowctl` (for local
connectors), so it leaves process-global state alone. It never installs or
reloads a `tracing` subscriber or filter. Only `main.rs` does.

The binary's `tracing` filter is fixed at boot: `LOG_LEVEL` if set, which only
V1 does, and otherwise `info`. It writes stderr as `ops::Log` JSON, which V1
decodes into task logs. Under V2, that stderr is application logging which the
reactor emits as `tracing::warn!`.
