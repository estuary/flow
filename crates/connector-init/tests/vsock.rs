//! Check that connector-init's `--vsock-port` serves its gRPC services over
//! AF_VSOCK, the transport libkrun maps to a host Unix socket.
//!
//! The test binds and dials CID 1 (VMADDR_CID_LOCAL), which requires the
//! `vsock_loopback` module. Where it isn't loaded the test skips rather than
//! fails, because the transport is unreachable from userspace at all. The
//! opt-in KVM suite sets `CONNECTOR_VMM_KVM` and then a missing loopback is a
//! failure: that job exists to prove the transport, not to skip it.

use futures::TryStreamExt;

// Guest AF_VSOCK port the connector VMM maps to its host-side Unix socket.
const VSOCK_PORT: u32 = 49092;

// Version that connector-init requires of a connector's Spec response.
const EXPECT_PROTOCOL: u32 = 3032023;

#[tokio::test]
async fn spec_rpc_over_vsock() {
    let Some(probe) = bind_probe().await else {
        let strict = std::env::var_os("CONNECTOR_VMM_KVM").is_some_and(|v| !v.is_empty());
        assert!(
            !strict,
            "AF_VSOCK loopback is unavailable and CONNECTOR_VMM_KVM is set; run `sudo modprobe vsock_loopback`"
        );
        eprintln!("skipping: AF_VSOCK loopback is unavailable (vsock_loopback not loaded)");
        return;
    };

    let dir = tempfile::tempdir().unwrap();

    // The "connector" is `cat` over a canned response: it writes one
    // newline-delimited JSON response and exits without reading its stdin.
    let expect = proto_flow::capture::Response {
        kind: Some(proto_flow::capture::response::Kind::Spec(Box::new(
            proto_flow::capture::response::Spec {
                protocol: EXPECT_PROTOCOL,
                config_schema_json: "{}".into(),
                resource_config_schema_json: "{}".into(),
                documentation_url: "https://example.com".to_string(),
                ..Default::default()
            },
        ))),
        ..Default::default()
    };
    let response_path = dir.path().join("response.json");
    let mut response_json = serde_json::to_vec(&expect).unwrap();
    response_json.push(b'\n');
    std::fs::write(&response_path, &response_json).unwrap();

    let inspect_path = dir.path().join("image-inspect.json");
    std::fs::write(
        &inspect_path,
        serde_json::to_vec(&serde_json::json!([{
            "Config": {
                "Cmd": null,
                "Entrypoint": ["/bin/cat", response_path],
                "Labels": {"FLOW_RUNTIME_CODEC": "json"},
                "Env": [],
            }
        }]))
        .unwrap(),
    )
    .unwrap();

    std::mem::drop(probe); // Free the port for the child.

    // Served in-process rather than by spawning the package's binary: a
    // `CARGO_BIN_EXE_` reference would build a glibc `flow-connector-init` into
    // `$CARGO_TARGET_DIR/debug`, where `locate_bin` and $PATH lookups prefer it
    // over the musl build that connector containers require.
    let args = <connector_init::Args as clap::Parser>::try_parse_from([
        "flow-connector-init".to_string(),
        format!("--image-inspect-json-path={}", inspect_path.display()),
        format!("--vsock-port={VSOCK_PORT}"),
    ])
    .unwrap();
    let server = tokio::spawn(connector_init::run(args, "warn".to_string()));

    let channel = tonic::transport::Endpoint::from_static("http://[::1]:0")
        .connect_with_connector(tower::service_fn(|_: tonic::transport::Uri| async {
            Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(dial().await?))
        }))
        .await
        .unwrap();

    let mut client = proto_grpc::capture::connector_client::ConnectorClient::new(channel);

    let request = proto_flow::capture::Request {
        kind: Some(proto_flow::capture::request::Kind::Spec(
            proto_flow::capture::request::Spec {
                config_json: "{}".into(),
                ..Default::default()
            },
        )),
        ..Default::default()
    };
    let responses = client
        .capture(futures::stream::once(async { request }))
        .await
        .unwrap()
        .into_inner();

    let responses: Vec<_> = responses.try_collect().await.unwrap();
    assert_eq!(responses, vec![expect]);

    server.abort();
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
/// other vsock transport binds fine and only fails the connect, which is the
/// case that must skip rather than fail. Returns the listener so the caller can
/// hold the port until it spawns.
async fn bind_probe() -> Option<tokio_vsock::VsockListener> {
    let listener = tokio_vsock::VsockListener::bind(tokio_vsock::VsockAddr::new(
        tokio_vsock::VMADDR_CID_ANY,
        VSOCK_PORT,
    ))
    .ok()?;

    // Only the handshake matters; the connected stream is dropped immediately.
    tokio_vsock::VsockStream::connect(tokio_vsock::VsockAddr::new(
        tokio_vsock::VMADDR_CID_LOCAL,
        VSOCK_PORT,
    ))
    .await
    .ok()?;

    Some(listener)
}
