//! Check that connector-init's `--vsock-port` serves `connector.Connector`
//! over AF_VSOCK, the transport libkrun maps to a host Unix socket.
//!
//! The test binds and dials CID 1 (VMADDR_CID_LOCAL), which requires the
//! `vsock_loopback` module. Where it isn't loaded the test fails, naming
//! `mise run bootstrap:vsock-loopback`, which loads it.
//!
//! A unit test rather than an integration test under `tests/`: Cargo builds a
//! package's binaries for its integration tests, which would put a glibc
//! `flow-connector-init` in `$CARGO_TARGET_DIR/debug`. `locate_bin::locate`
//! and $PATH lookups would find it ahead of the musl build that connector
//! containers need.

use crate::serve::test::{SPEC, first};
use anyhow::Context;
use proto_flow::connector;

// Guest AF_VSOCK port the connector VMM maps to its host-side Unix socket.
const VSOCK_PORT: u32 = 49092;

#[tokio::test]
async fn connector_over_vsock() {
    let probe = bind_probe().await.unwrap_or_else(|err| {
        panic!(
            "AF_VSOCK loopback failed its probe ({err:#}); \
             `mise run bootstrap:vsock-loopback` loads the vsock_loopback module it needs"
        )
    });

    let dir = tempfile::tempdir().unwrap();
    let inspect_path = dir.path().join("image-inspect.json");
    std::fs::write(
        &inspect_path,
        serde_json::to_vec(&serde_json::json!([{
            "Id": "synthetic-image",
            "Config": {
                "Cmd": null,
                "Entrypoint": ["/bin/sh", "-c", format!("read spec; echo '{SPEC}'")],
                "Labels": {"FLOW_RUNTIME_CODEC": "json"},
                "Env": [],
            }
        }]))
        .unwrap(),
    )
    .unwrap();

    std::mem::drop(probe); // Free the port for the server.

    let args = <crate::Args as clap::Parser>::try_parse_from([
        "flow-connector-init".to_string(),
        format!("--image-inspect-json-path={}", inspect_path.display()),
        format!("--vsock-port={VSOCK_PORT}"),
    ])
    .unwrap();
    let server = tokio::spawn(crate::run(args, "warn".to_string()));

    let channel = tonic::transport::Endpoint::from_static("http://[::1]:0")
        .connect_with_connector(tower::service_fn(|_: tonic::transport::Uri| async {
            Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(dial().await?))
        }))
        .await
        .unwrap();

    let mut responses = proto_grpc::connector::connector_client::ConnectorClient::new(channel)
        .connector(futures::stream::once(async { first("warn") }))
        .await
        .unwrap()
        .into_inner();

    let started = responses.message().await.unwrap().unwrap();
    assert!(matches!(
        started.kind,
        Some(connector::response::Kind::Started(_))
    ));
    assert!(responses.message().await.unwrap().is_none());

    // `run` returns once its one stream is done, as it does over TCP.
    tokio::time::timeout(std::time::Duration::from_secs(3), server)
        .await
        .expect("run returns after its stream")
        .unwrap()
        .unwrap();
}

/// Dial the in-process server, retrying while it binds. The binary signals
/// readiness on stderr, which isn't observable in-process.
async fn dial() -> std::io::Result<tokio_vsock::VsockStream> {
    let addr = tokio_vsock::VsockAddr::new(tokio_vsock::VMADDR_CID_LOCAL, VSOCK_PORT);
    let mut attempts = 0;
    loop {
        match tokio_vsock::VsockStream::connect(addr).await {
            Ok(stream) => return Ok(stream),
            Err(error) if attempts == 50 => return Err(error),
            Err(_) => attempts += 1,
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// Learn whether AF_VSOCK loopback works here, by binding the test's port and
/// then dialing it at CID 1. Binding alone is not enough: a host with some
/// other vsock transport binds fine and only fails the connect. Returns the
/// listener so the caller can hold the port until it starts the server.
async fn bind_probe() -> anyhow::Result<tokio_vsock::VsockListener> {
    let listener = tokio_vsock::VsockListener::bind(tokio_vsock::VsockAddr::new(
        tokio_vsock::VMADDR_CID_ANY,
        VSOCK_PORT,
    ))
    .context("binding the test's vsock port")?;

    // Only the handshake matters; the connected stream is dropped immediately.
    tokio_vsock::VsockStream::connect(tokio_vsock::VsockAddr::new(
        tokio_vsock::VMADDR_CID_LOCAL,
        VSOCK_PORT,
    ))
    .await
    .context("dialing it at CID 1")?;

    Ok(listener)
}
