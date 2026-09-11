# proto-grpc

Shared Rust gRPC protocol support for Flow services.

The crate owns generated tonic modules and their feature gates, bearer-token
authentication helpers, bounded status conversion, protocol verification
helpers, shared `CHANNEL_BUFFER` and `MAX_MESSAGE_SIZE` limits, and
`dial_channel` with the workspace's HTTP, HTTPS, and Unix-socket defaults.

`status_to_anyhow` carries a status across an error chain as a `StatusError`,
and `anyhow_to_status` maps one back: verbatim if the status *is* the error,
or -- under `context` layers, which inform a status without changing its
outcome -- keeping its code while folding those layers into its message. A
status which reaches a chain by any other route (a bare `?` on a
`tonic::Result`) converts to Unknown and renders as tonic's field dump, which
is how an unmapped conversion announces itself.

Start in `src/lib.rs` for generated-module exports and feature gates,
`src/auth.rs` for authentication and authorization, `src/status.rs` for error
translation and protocol expectations, and `src/dial.rs` for transport setup.
