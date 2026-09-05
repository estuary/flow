# proto-grpc

Hand-written gRPC plumbing and generated tonic clients and servers for Flow
protocols. Generated modules are checked into `src/` and each client/server is
enabled by its matching Cargo feature.

The crate root provides authentication and authorization (`Metadata`,
`Signer`, `Authenticator`, `Authorizer`), bounded status conversion and
protocol verification helpers, shared `CHANNEL_BUFFER` and `MAX_MESSAGE_SIZE`
limits, and `dial_channel` with the workspace's HTTP, HTTPS, and Unix-socket
defaults.

With `connector_client`, `proto_grpc::connector` also provides the client side
of `connector.Connector`: the object-safe `Router` seam, `EndpointRouter`,
request identity and bearer minting, and the `start` / `next` stream helpers.
