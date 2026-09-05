# proto-grpc

Hand-written gRPC plumbing and generated tonic clients and servers for Flow
protocols. Generated modules are checked into `src/` and each client/server is
enabled by its matching Cargo feature.

The crate root provides authentication and authorization (`Metadata`,
`Signer`, `Authenticator`, `Authorizer`), bounded status conversion and
protocol verification helpers, shared `CHANNEL_BUFFER` and `MAX_MESSAGE_SIZE`
limits, and `dial_channel` with the workspace's HTTP, HTTPS, and Unix-socket
defaults.

`status_to_anyhow` carries a status across an error chain as a `StatusError`,
and `anyhow_to_status` maps one back: verbatim if the status *is* the error,
or -- under `context` layers, which inform a status without changing its
outcome -- keeping its code while folding those layers into its message. A
status which reaches a chain by any other route (a bare `?` on a
`tonic::Result`) converts to Unknown and renders as tonic's field dump, which
is how an unmapped conversion announces itself.

With `connector_client`, `proto_grpc::connector` also provides the client side
of `connector.Connector`: the object-safe `Router` seam, `EndpointRouter`,
request identity and bearer minting, the `start` / `next` stream helpers, and a
`unary` adapter which consumes logs, `Started`, one protocol response, and EOF.
