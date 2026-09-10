//! End-to-end check that `flow-connector-init --vsock-port` serves its gRPC
//! services over AF_VSOCK, the transport libkrun maps to a host Unix socket.
//!
//! The test binds and dials CID 1 (VMADDR_CID_LOCAL), which requires the
//! `vsock_loopback` module. Where it isn't loaded the test skips rather than
//! fails, because the transport is unreachable from userspace at all.

use futures::TryStreamExt;

// AF_VSOCK port used by the runtime, per the sandbox spike contract.
const VSOCK_PORT: u32 = 49092;

// Version that connector-init requires of a connector's Spec response.
const EXPECT_PROTOCOL: u32 = 3032023;

#[tokio::test]
async fn spec_rpc_over_vsock() {
    let Some(probe) = bind_probe() else {
        eprintln!("skipping: AF_VSOCK loopback is unavailable (vsock_loopback not loaded)");
        return;
    };

    let dir = tempfile::tempdir().unwrap();

    // The "connector" is `cat` over a canned response: it writes one
    // newline-delimited JSON response and exits without reading its stdin.
    let expect = proto_flow::capture::Response {
        spec: Some(proto_flow::capture::response::Spec {
            protocol: EXPECT_PROTOCOL,
            config_schema_json: "{}".into(),
            resource_config_schema_json: "{}".into(),
            documentation_url: "https://example.com".to_string(),
            ..Default::default()
        }),
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

    let mut cmd = async_process::Command::new(env!("CARGO_BIN_EXE_flow-connector-init"));
    cmd.arg(format!(
        "--image-inspect-json-path={}",
        inspect_path.display()
    ))
    .arg(format!("--vsock-port={VSOCK_PORT}"))
    .stderr(async_process::Stdio::piped());

    let mut child: async_process::Child = cmd.spawn().unwrap().into();
    let mut stderr = child.stderr.take().unwrap();

    // Readiness: a single space on stderr, written once the listener is bound.
    let mut ready = [0u8; 1];
    tokio::io::AsyncReadExt::read_exact(&mut stderr, &mut ready)
        .await
        .expect("connector-init exited before signaling readiness");
    assert_eq!(&ready, b" ");

    let channel = tonic::transport::Endpoint::from_static("http://[::1]:0")
        .connect_with_connector(tower::service_fn(|_: tonic::transport::Uri| async {
            let stream = tokio_vsock::VsockStream::connect(tokio_vsock::VsockAddr::new(
                tokio_vsock::VMADDR_CID_LOCAL,
                VSOCK_PORT,
            ))
            .await?;
            Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
        }))
        .await
        .unwrap();

    let mut client = proto_grpc::capture::connector_client::ConnectorClient::new(channel);

    let request = proto_flow::capture::Request {
        spec: Some(proto_flow::capture::request::Spec {
            config_json: "{}".into(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let responses = client
        .capture(futures::stream::once(async { request }))
        .await
        .unwrap()
        .into_inner();

    let responses: Vec<_> = responses.try_collect().await.unwrap();
    assert_eq!(responses, vec![expect]);
}

/// Bind the test's vsock port to learn whether AF_VSOCK loopback works here.
/// Returns the listener so the caller can hold the port until it spawns.
fn bind_probe() -> Option<tokio_vsock::VsockListener> {
    tokio_vsock::VsockListener::bind(tokio_vsock::VsockAddr::new(
        tokio_vsock::VMADDR_CID_ANY,
        VSOCK_PORT,
    ))
    .ok()
}
