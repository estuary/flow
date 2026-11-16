# proto-grpc

Hand-written gRPC plumbing and generated tonic clients and servers for Flow
protocols. Generated modules are checked into `src/` and each client/server is
enabled by its matching Cargo feature.

The crate root provides authentication and authorization (`Metadata`,
`Signer`, `Authenticator`, `Authorizer`), bounded status conversion and
protocol verification helpers, shared `CHANNEL_BUFFER` and `MAX_MESSAGE_SIZE`
limits, and `dial_channel` with the workspace's HTTP, HTTPS, and Unix-socket
defaults. Caches of dialed Channels drop them after `CHANNEL_CACHE_MAX_AGE`, to
rotate long-lived HTTP/2 connections and release connections to peers that
have left the topology.

`status_to_anyhow` carries a status across an error chain as a `StatusError`,
and `anyhow_to_status` maps one back: verbatim if the status *is* the error,
or -- under `context` layers, which inform a status without changing its
outcome -- keeping its code while folding those layers into its message. A
status which reaches a chain by any other route (a bare `?` on a
`tonic::Result`) converts to Unknown and renders as tonic's field dump, which
is how an unmapped conversion announces itself.

`proto_grpc::chunk` carries messages of the shuffle, runtime, and connector
protocols which exceed one gRPC frame. Every server and client of those
services wraps its received stream in `chunk::join`, and its sent stream in
`chunk::split_requests` or `chunk::split_responses`. In-process paths skip
them. A message larger than `chunk::MAX_FRAME` is sent as chunk messages of
its own type. `MAX_FRAME` never chunks yet, so that every receiver can
reassemble before any sender chunks. Go's
`runtime.Chunks` and `runtime.RecvChunked` mirror the module for the Shard
service's Go controller, and module docs describe the protocol.

With `connector_client`, `proto_grpc::connector` also provides the client side
of `connector.Connector`: the object-safe `Router` seam, `EndpointRouter`,
request identity and bearer minting, the `start` / `next` stream helpers, and a
`unary` adapter which consumes logs, `Started`, one protocol response, and EOF.
`start` (and so `unary`) requires `Started.execution` to echo the requested
`Start.execution` whole: a service which predates a requested execution, or a
part of one such as its egress, ignores it, and would otherwise start the
connector without it.
