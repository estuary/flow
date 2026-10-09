//! `run`'s health is SERVING although its connector cannot run at all, as a
//! connector RPC over the same channel shows.

#[tokio::test]
async fn health_is_serving_without_running_the_connector() {
    let dir = tempfile::tempdir().unwrap();
    let inspect_path = dir.path().join("image-inspect.json");
    std::fs::write(
        &inspect_path,
        serde_json::to_vec(&serde_json::json!([{
            "Config": {
                "Cmd": null,
                "Entrypoint": [dir.path().join("absent")],
                "Labels": {},
                "Env": [],
            }
        }]))
        .unwrap(),
    )
    .unwrap();

    // `run` binds the port itself, so it's found free and then let go.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let args = <crate::Args as clap::Parser>::try_parse_from([
        "flow-connector-init".to_string(),
        format!("--image-inspect-json-path={}", inspect_path.display()),
        format!("--port={port}"),
    ])
    .unwrap();

    let client = async {
        let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let health = tonic_health::pb::health_client::HealthClient::new(channel.clone())
            .check(tonic_health::pb::HealthCheckRequest::default())
            .await
            .map(|response| response.into_inner().status())
            .map_err(|status| status.to_string());

        let spec = proto_flow::capture::Request {
            kind: Some(proto_flow::capture::request::Kind::Spec(Default::default())),
            ..Default::default()
        };
        let connector = proto_grpc::capture::connector_client::ConnectorClient::new(channel)
            .capture(futures::stream::once(std::future::ready(spec)))
            .await
            .map(|_| ())
            .map_err(|status| status.message().to_string());
        (health, connector)
    };

    // Polled first, `run` has bound its port before the client dials it.
    let (health, connector) = tokio::select! {
        biased;
        ended = crate::run(args, "warn".to_string()) => panic!("connector-init ended: {ended:?}"),
        outcome = client => outcome,
    };
    assert_eq!(
        health,
        Ok(tonic_health::pb::health_check_response::ServingStatus::Serving)
    );
    let connector = connector.expect_err("the connector cannot be run");
    assert!(
        connector.starts_with("could not start connector entrypoint"),
        "{connector}"
    );
}
