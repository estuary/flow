# connector-init

`flow-connector-init` is bind-mounted as the entrypoint of every image
connector. It starts the connector's real entrypoint (from the image's inspect
JSON) and serves it over gRPC on `--port` or `--vsock-port`. It's versioned with the reactor,
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
- In a VMM, `serve` also writes each DNS refusal of the VMM's resolver as a
  `Log` while the connector runs (`refusals.rs`). The resolver reports facts,
  such as the refused name, and `refusals::render` words them.
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
| `run` / `Args`    | Binds `--port` or `--vsock-port`, then writes a readiness byte       |
| `serve::serve`    | Serves one `connector.Connector` stream over connector processes     |
| `serve::run`      | Returns a connector's Spec as `Started`, then pumps the stream       |
| `rpc::spawn`      | Runs a connector process over requests and responses                 |
| `Codec`           | The connector's stdin/stdout framing: length-prefixed proto, or JSON |
| `inspect::Image`  | Parsed image inspect output, with the ID that pins VMM roots         |

## Transports

Exactly one of `--port` (TCP, `0.0.0.0`) or `--vsock-port` (AF_VSOCK,
`VMADDR_CID_ANY`) is given; clap enforces the pair. Containers use TCP. The
connector VMM boots the image in a guest and uses vsock, which the VMM maps to
a Unix socket on the host, so the runtime dials a path instead of a port.
Both listeners serve the same services, with the same one-stream lifetime.

The listener is bound before anything else can fail, and only then is a single
space written to stderr. That byte is an ordinary container's readiness
signal, and nothing else here may write a line beginning with a space. That
includes usage errors, so `main` re-frames clap's output behind the binary's
name rather than letting clap print its indented argument lists.

The VMM launcher waits for the same byte before dialing the Unix socket that
libkrun forwards to the guest's vsock listener.

A VMM also passes `--resolver-vsock-port`. A served stream dials it at the
host, where libkrun forwards it to the resolver's refusals socket. A failed
dial leaves refusals unreported: they're feedback, and the resolver enforces
egress regardless.

## Logging

`serve` runs in-process within the reactor and `flowctl` (for local
connectors), so it leaves process-global state alone. It never installs or
reloads a `tracing` subscriber or filter. Only `main.rs` does.

The binary's `tracing` filter is fixed at boot: `LOG_LEVEL` if set, which only
V1 does, and otherwise `info`. It writes stderr as `ops::Log` JSON, which V1
decodes into task logs. Under V2, that stderr is application logging which the
reactor emits as `tracing::warn!`.

## Testing

`src/main.rs` checks in a subprocess that a listener bind failure exits with
status 1 and reports its cause.

`src/vsock_test.rs` serves a `connector.Connector` stream over AF_VSOCK
against an in-process server. It's a unit test because an integration test
would make Cargo build a glibc `flow-connector-init` into `target/debug`,
which launchers pass over (`locate_bin::locate_static`) but plain `PATH`
lookups find ahead of the musl build connector containers need.

It dials CID 1, which needs the `vsock_loopback` kernel module. Without it
the test skips, unless `CONNECTOR_VMM_KVM` is set, in which case a missing
loopback is a failure; `mise run ci:connector-vmm-kvm` sets it.
`sudo modprobe vsock_loopback` enables it.
