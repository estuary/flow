# proto-grpc

Shared Rust gRPC protocol support for Flow services.

The crate owns generated tonic modules and their feature gates, bearer-token
authentication helpers, bounded status conversion, protocol verification
helpers, shared `CHANNEL_BUFFER` and `MAX_MESSAGE_SIZE` limits, and
`dial_channel` with the workspace's HTTP, HTTPS, and Unix-socket defaults.

Start in `src/lib.rs` for generated-module exports and feature gates,
`src/auth.rs` for authentication and authorization, `src/status.rs` for error
translation and protocol expectations, and `src/dial.rs` for transport setup.
