# connector-init

`flow-connector-init` is the entrypoint the runtime injects into every
connector container. It listens for the runtime, and for each gRPC stream it
receives it spawns the image's own entrypoint and proxies the
capture / derive / materialize protocol over the connector's stdin and stdout.
The image never knows it is behind a proxy; the runtime never knows which
codec the image speaks.

## Roadmap

- `src/main.rs`: CLI. Installs the `ops::Log` tracing layer (stderr, always
  structured) and a current-thread Tokio runtime, then calls `run`.
- `src/lib.rs`: `Args` and `run`. Binds the listener, writes the readiness
  byte, parses the image inspection, and serves the three services and
  `tonic-health`'s standard `grpc.health.v1.Health` until the `watchdog` sees
  no connector RPC for a while.
- `src/inspect.rs`: the `docker inspect` output the launcher writes into the
  container; yields the image's real argv and its `FLOW_RUNTIME_CODEC`.
- `src/rpc.rs`: the generic bidirectional and unary proxies over a child
  process, and the stderr log pump.
- `src/codec.rs`: protobuf or newline-delimited JSON framing on the child's
  pipes.
- `src/{capture,derive,materialize}.rs`: one `Proxy` per protocol, each a thin
  binding of `rpc` to its generated tonic service.
- `src/vsock_test.rs`: a Spec RPC over AF_VSOCK against an in-process server
  (Linux only). A unit test because an integration test would make Cargo
  build a glibc `flow-connector-init` into `target/debug`, which launchers
  pass over (`locate_bin::locate_static`) but plain `PATH` lookups find ahead
  of the musl build connector containers need.

## Transports

Exactly one of `--port` (TCP, `0.0.0.0`) or `--vsock-port` (AF_VSOCK,
`VMADDR_CID_ANY`) is given; clap enforces the pair. Containers use TCP. The
connector VMM boots the image in a guest and uses vsock, which the VMM maps to
a Unix socket on the host, so the runtime dials a path instead of a port.

The listener is bound before anything else can fail, and only then is a single
space written to stderr. That byte is an ordinary container's readiness
signal, and nothing else here may write a line beginning with a space. That
includes usage errors, so `main` re-frames clap's output behind the binary's
name rather than letting clap print its indented argument lists.

A VMM's launcher ignores the byte, since anything in the guest may write to
its console, and waits instead for the server's health (`""`) to answer
SERVING. Health is served only with the proxies, once the image is parsed, so
that answer means init serves the connector's protocol. It runs no connector.

## Testing

`src/health_test.rs` runs `run` over TCP with an entrypoint that cannot be
executed: health is SERVING, and a capture RPC fails to start the connector.

`src/vsock_test.rs` dials CID 1, which needs the `vsock_loopback` kernel module.
Without it the test skips, unless `CONNECTOR_VMM_KVM` is set, in which case a
missing loopback is a failure; `mise run ci:connector-vmm-kvm` sets it.
`sudo modprobe vsock_loopback` enables it.
