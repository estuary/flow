//! End-to-end tests of the `connector.Connector` contract: a served stream
//! driven over a loopback gRPC server, through an `EndpointRouter`, and
//! in-process.
//!
//! Every connector started here is either in-process (derive-sqlite, Dekaf) or
//! a `/bin/sh` subprocess, so nothing needs Docker.

use connector::proto;
use futures::StreamExt;
use proto_grpc::connector::{Router as _, SPEC_TASK_NAME};
use serde_json::json;
use tokio::sync::mpsc;

// ----------------------------------------------------------------- fixtures --

/// A local `Service` and its router, for the many tests which need a working
/// pair and nothing more.
fn local_service() -> (connector::Service, connector::ServiceRouter) {
    connector::Service::new_local(String::new(), service_kit::Registry::new())
}

/// Requests are written as JSON because their typed form buries what each test
/// varies under layers of `Option`, `Box`, and `..Default::default()`.
fn request(fixture: serde_json::Value) -> proto::Request {
    serde_json::from_value(fixture).expect("fixture is a valid connector::Request")
}

/// Set `start` on a first request. `sqlite_vfs_uri` is runtime-internal: only
/// an in-process caller may set it, and only for a Sqlite derivation.
fn with_start(mut fixture: serde_json::Value, sqlite_vfs_uri: &str) -> serde_json::Value {
    fixture["start"] = json!({"logLevel": "debug", "sqliteVfsUri": sqlite_vfs_uri});
    fixture
}

/// A `local:` endpoint running `script` under `/bin/sh`. Callers which care
/// set `config` or `env` on the result.
fn sh(script: &str) -> serde_json::Value {
    json!({"command": ["/bin/sh", "-c", script], "config": {}})
}

fn derive_open(collection: &str) -> serde_json::Value {
    json!({"derive": {"open": {"collection": {
        "name": collection,
        "derivation": {
            "connectorType": "SQLITE",
            "config": {"migrations": []},
        },
    }}}})
}

/// A first request validating `acmeCo/materialization` against Dekaf, whose
/// `{variant, config}` wrapper the pipeline splits before startup.
fn dekaf_validate(config: serde_json::Value) -> proto::Request {
    request(with_start(
        json!({"materialize": {"validate": {
            "name": "acmeCo/materialization",
            "connectorType": "DEKAF",
            "config": {"variant": "test", "config": config},
        }}}),
        "",
    ))
}

// ------------------------------------------------------------------ harness --

/// Drive a Connector RPC over a loopback gRPC server under the bearer its own
/// first request calls for, which is what an honest client presents.
async fn drive_loopback(requests: Vec<proto::Request>) -> Vec<tonic::Result<proto::Response>> {
    let (task_type, task_name) = honest_identity(&requests);
    drive_loopback_as(task_type, &task_name, requests).await
}

/// Drive a Connector RPC over a loopback gRPC server under a bearer named
/// here rather than derived, for the cases which deliberately mismatch.
async fn drive_loopback_as(
    task_type: ops::TaskType,
    task_name: &str,
    requests: Vec<proto::Request>,
) -> Vec<tonic::Result<proto::Response>> {
    let (service, router) = local_service();
    let metadata =
        proto_grpc::connector::connector_bearer(router.signer(), task_type, task_name).unwrap();

    let (endpoint, server) = serve(service).await;
    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();

    let responses =
        match proto_grpc::connector::connector_client::ConnectorClient::with_interceptor(
            channel, metadata,
        )
        .connector(futures::stream::iter(requests))
        .await
        {
            Ok(response) => response.into_inner().collect::<Vec<_>>().await,
            Err(status) => vec![Err(status)],
        };

    server.abort();
    responses
}

/// Drive a Connector RPC through an `EndpointRouter` which dials `service`.
async fn drive_endpoint_router(
    service: connector::Service,
    signer: proto_grpc::Signer,
    requests: Vec<proto::Request>,
) -> Vec<tonic::Result<proto::Response>> {
    _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let (task_type, task_name) = honest_identity(&requests);
    let (endpoint, server) = serve(service).await;
    let router = proto_grpc::connector::EndpointRouter::new(endpoint, signer);

    let responses =
        collect_receiver(router.open(task_type, &task_name, request_receiver(requests))).await;
    server.abort();
    responses
}

/// Drive a Connector RPC in-process, returning its responses.
async fn drive_router(
    router: &connector::ServiceRouter,
    task_type: ops::TaskType,
    task_name: &str,
    requests: Vec<proto::Request>,
) -> Vec<tonic::Result<proto::Response>> {
    collect_receiver(router.open(task_type, task_name, request_receiver(requests))).await
}

/// Identity the first request calls for, or a task-less Spec where the request
/// under test is malformed enough to have none.
fn honest_identity(requests: &[proto::Request]) -> (ops::TaskType, String) {
    let Some(kind) = requests.first().and_then(|request| request.kind.as_ref()) else {
        return (ops::TaskType::Capture, SPEC_TASK_NAME.to_string());
    };
    match proto_grpc::connector::task_identity(kind) {
        Ok((task_type, task_name)) => (task_type, task_name.to_string()),
        Err(_) => (
            proto_grpc::connector::task_type(kind),
            SPEC_TASK_NAME.to_string(),
        ),
    }
}

/// Serve `service` on an ephemeral loopback port, returning its endpoint and
/// the task to abort once the stream under test is done.
async fn serve(service: connector::Service) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());

    let server = tokio::spawn(async move {
        _ = tonic::transport::Server::builder()
            .add_service(service.into_tonic_service())
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });
    (endpoint, server)
}

fn request_receiver(requests: Vec<proto::Request>) -> mpsc::Receiver<proto::Request> {
    let (request_tx, request_rx) = mpsc::channel(proto_grpc::CHANNEL_BUFFER);
    for request in requests {
        request_tx
            .try_send(request)
            .expect("test requests fit the channel");
    }
    request_rx
}

async fn collect_receiver(
    mut response_rx: mpsc::Receiver<tonic::Result<proto::Response>>,
) -> Vec<tonic::Result<proto::Response>> {
    let mut responses = Vec::new();
    while let Some(response) = response_rx.recv().await {
        responses.push(response);
    }
    responses
}

/// Render one response stream as a line-per-response block, so that the
/// *order* of `Started`, logs, protocol responses, and a terminal Status is
/// what a snapshot asserts on.
fn render(responses: Vec<tonic::Result<proto::Response>>) -> String {
    render_lines(responses).join("\n")
}

/// Render several labelled streams as one block, which reads more directly
/// than a nested `Debug` of the same strings.
fn render_all<'a>(
    outcomes: impl IntoIterator<Item = (&'a str, Vec<tonic::Result<proto::Response>>)>,
) -> String {
    outcomes
        .into_iter()
        .map(|(label, responses)| format!("{label}:\n  {}", render_lines(responses).join("\n  ")))
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_lines(responses: Vec<tonic::Result<proto::Response>>) -> Vec<String> {
    responses
        .into_iter()
        .map(|response| match response {
            Err(status) => format!("Status({:?}): {}", status.code(), status.message()),
            Ok(proto::Response { kind: None }) => "empty".to_string(),
            Ok(proto::Response {
                kind: Some(response),
            }) => match response {
                proto::response::Kind::Started(started) => format!(
                    "Started(codec={:?}, container={}, process={}, spec={})",
                    proto::response::started::Codec::try_from(started.codec).unwrap(),
                    started.container.is_some(),
                    started.process.is_some(),
                    match started.spec {
                        Some(proto::response::started::Spec::Capture(_)) => "capture",
                        Some(proto::response::started::Spec::Derive(_)) => "derive",
                        Some(proto::response::started::Spec::Materialize(_)) => "materialize",
                        None => "missing",
                    },
                ),
                proto::response::Kind::Log(log) => render_log(log),
                proto::response::Kind::Capture(r) => format!("Capture({})", variant(&r)),
                proto::response::Kind::Derive(r) => format!("Derive({})", variant(&r)),
                proto::response::Kind::Materialize(r) => format!("Materialize({})", variant(&r)),
            },
        })
        .collect()
}

/// A log with its fields, ordered, so that identifiers this crate reports are
/// asserted alongside the message which carries them.
fn render_log(log: ops::Log) -> String {
    let fields: std::collections::BTreeMap<&str, serde_json::Value> = log
        .fields_json_map
        .iter()
        .map(|(key, value)| {
            (
                key.as_str(),
                serde_json::from_slice(value).expect("log field is JSON"),
            )
        })
        .collect();

    let level = log.level().as_str_name();
    if fields.is_empty() {
        format!("Log({level}): {}", log.message)
    } else {
        format!(
            "Log({level}): {} {}",
            log.message,
            serde_json::to_string(&fields).unwrap(),
        )
    }
}

/// Name the single set field of a protocol response.
fn variant(response: &impl serde::Serialize) -> String {
    let serde_json::Value::Object(map) = serde_json::to_value(response).unwrap() else {
        unreachable!("a protocol response is an object")
    };
    map.into_iter()
        .map(|(key, _value)| key)
        .collect::<Vec<_>>()
        .join("+")
}

// ----------------------------------------------------------------- sessions --

/// The same derive-sqlite session over each transport: `Started` leads, and
/// the connector's responses follow in order. The wire session threads a
/// trailing Spec; the in-process one threads a recorded VFS path, which only
/// it may set.
#[tokio::test]
async fn derive_sqlite_session_over_every_transport() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_uri = dir.path().join("derive.db").to_string_lossy().into_owned();

    let (service, router) = local_service();
    let over_wire = drive_endpoint_router(
        service,
        router.signer().clone(),
        vec![
            request(with_start(derive_open("acmeCo/derivation"), "")),
            request(json!({"derive": {"spec": {
                "connectorType": "SQLITE",
                "config": {"migrations": []},
            }}})),
        ],
    )
    .await;

    let in_process = drive_router(
        &router,
        ops::TaskType::Derivation,
        "acmeCo/derivation",
        vec![request(with_start(
            derive_open("acmeCo/derivation"),
            &vfs_uri,
        ))],
    )
    .await;

    insta::assert_snapshot!(render_all([("wire", over_wire), ("in-process", in_process)]), @"
    wire:
      Started(codec=Proto, container=false, process=false, spec=derive)
      Derive(opened)
      Derive(spec)
    in-process:
      Started(codec=Proto, container=false, process=false, spec=derive)
      Derive(opened)
    ");
}

/// A first request which names a `local:` derivation connector, running
/// `script`.
fn local_derive_spec(script: &str) -> proto::Request {
    request(with_start(
        json!({"derive": {"spec": {
            "connectorType": "LOCAL",
            "config": sh(script),
        }}}),
        "",
    ))
}

/// A `local:` subprocess connector which writes to stderr and then exits
/// non-zero: its logs precede the terminal Status, which is the stream's last
/// word. Logs race `Started` -- they're sunk as they're read, and this
/// connector writes immediately -- so only their position relative to the
/// Status is asserted.
#[tokio::test]
async fn local_connector_logs_precede_its_status() {
    let rendered = render_lines(
        drive_loopback(vec![local_derive_spec(
            "echo 'a first line' >&2; echo 'a second line' >&2; exit 7",
        )])
        .await,
    );

    assert!(
        rendered.iter().any(|r| r.contains("a first line")),
        "{rendered:?}",
    );
    assert!(
        rendered.last().unwrap().starts_with("Status("),
        "the terminal Status is the stream's last word: {rendered:?}",
    );
    let last_log = rendered.len() - 2;
    assert!(rendered[last_log].starts_with("Log("), "{rendered:?}");
}

/// A client which goes away while its connector is still starting -- before
/// the connector has answered the internal Spec -- releases the handler, and
/// with it the connector. The handler owns `request_rx`, so its completion is
/// observed as the closing of `request_tx`.
#[tokio::test]
async fn a_dropped_response_stream_ends_a_starting_connector() {
    let (service, router) = local_service();
    let metadata = proto_grpc::connector::connector_bearer(
        router.signer(),
        ops::TaskType::Derivation,
        SPEC_TASK_NAME,
    )
    .unwrap();

    // The connector announces itself on stderr, then hangs without ever
    // answering the Spec and without reading stdin, so only cancellation can
    // end its session. `exec` matters: a forked `sleep` would outlive the
    // killed shell and hold its inherited stderr open, parking the blocking
    // read of our log pump.
    let (request_tx, request_rx) = mpsc::channel(1);
    request_tx
        .try_send(local_derive_spec(
            "echo 'connector is up' >&2; exec sleep 60",
        ))
        .unwrap();
    let mut response_rx = service.spawn_connector(metadata, request_rx);

    // The connector's log places the handler within `start`, awaiting a Spec.
    _ = tokio::time::timeout(std::time::Duration::from_secs(10), response_rx.recv())
        .await
        .expect("the connector's log arrives")
        .expect("the stream is open");

    std::mem::drop(response_rx);

    tokio::time::timeout(std::time::Duration::from_secs(10), request_tx.closed())
        .await
        .expect("the starting connector is released with its client");
}

/// Over the wire, a caller which drops both halves of its session releases a
/// connector which is still starting and has gone silent. Only the relay of
/// `EndpointRouter` observes the caller leaving, and it must pass that on as
/// an HTTP/2 stream reset.
#[tokio::test]
async fn a_wire_caller_which_leaves_ends_a_silent_starting_connector() {
    // As in `a_dropped_response_stream_ends_a_starting_connector`.
    let responses =
        leave_a_silent_wire_connector("echo 'connector is up' >&2; exec sleep 60").await;
    assert!(responses[0].starts_with("Log("), "{responses:?}");
}

/// As above, for a connector which has `Started` and then gone silent.
#[tokio::test]
async fn a_wire_caller_which_leaves_ends_a_silent_started_connector() {
    let responses = leave_a_silent_wire_connector(
        r#"echo '{"spec":{"protocol":3032023,"configSchema":{},"resourceConfigSchema":{}}}'; exec sleep 60"#,
    )
    .await;
    assert!(
        responses.last().unwrap().starts_with("Started("),
        "{responses:?}"
    );
}

/// Run `script` as a `local:` derive connector behind a loopback
/// `EndpointRouter`, read responses until it logs to stderr or reports
/// `Started`, and then drop both halves of the session. Returns the rendered
/// responses read, after asserting that the connector's handler exits.
async fn leave_a_silent_wire_connector(script: &str) -> Vec<String> {
    _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let registry = service_kit::Registry::new();
    let (service, router) = connector::Service::new_local(String::new(), registry.clone());

    let requests = vec![local_derive_spec(script)];
    let (task_type, task_name) = honest_identity(&requests);
    let (endpoint, server) = serve(service).await;
    let router = proto_grpc::connector::EndpointRouter::new(endpoint, router.signer().clone());

    let (request_tx, request_rx) = mpsc::channel(1);
    for request in requests {
        request_tx.try_send(request).unwrap();
    }
    let mut response_rx = router.open(task_type, &task_name, request_rx);

    let mut responses = Vec::new();
    while !responses
        .last()
        .is_some_and(|r: &String| r.starts_with("Log(") || r.starts_with("Started("))
    {
        let response = tokio::time::timeout(std::time::Duration::from_secs(10), response_rx.recv())
            .await
            .expect("the connector responds")
            .expect("the stream is open");
        responses.extend(render_lines(vec![response]));
    }
    assert_eq!(registry.snapshot().live.len(), 1, "{responses:?}");

    std::mem::drop(request_tx);
    std::mem::drop(response_rx);

    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !registry.snapshot().live.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the silent connector is released with its caller");

    server.abort();
    responses
}

/// Opening a channel eagerly starts its handler, even if responses are never
/// received: the invalid request below is consumed without a reader.
#[tokio::test]
async fn opening_a_channel_starts_its_handler() {
    let (service, router) = local_service();
    let metadata = proto_grpc::connector::connector_bearer(
        router.signer(),
        ops::TaskType::Derivation,
        SPEC_TASK_NAME,
    )
    .unwrap();

    let (request_tx, request_rx) = mpsc::channel(1);
    request_tx.try_send(proto::Request::default()).unwrap();
    let response_rx = service.spawn_connector(metadata, request_rx);

    tokio::time::timeout(std::time::Duration::from_secs(1), request_tx.closed())
        .await
        .expect("the spawned handler consumes the invalid request");

    drop(request_tx);
    insta::assert_snapshot!(render(collect_receiver(response_rx).await), @"Status(InvalidArgument): the first Connector request must set `start` and exactly one protocol request");
}

// ------------------------------------------------------------ client input --

/// Malformed client input is rejected, whether it arrives before anything is
/// started or after `Started`. The last row matters most: the handler owns
/// request validation, so invalid input cannot disappear as the clean EOF of
/// an in-process connector.
#[tokio::test]
async fn invalid_client_input_is_rejected() {
    let sqlite_open = || request(with_start(derive_open("acmeCo/derivation"), ""));

    let mut outcomes = Vec::new();
    for (label, requests) in [
        (
            "no start on the first request",
            vec![request(derive_open("acmeCo/derivation"))],
        ),
        (
            "no protocol request on the first request",
            vec![request(json!({"start": {"logLevel": "debug"}}))],
        ),
        ("a second start", vec![sqlite_open(), sqlite_open()]),
        (
            "a later request of another protocol",
            vec![sqlite_open(), request(json!({"capture": {"spec": {}}}))],
        ),
        (
            "an empty request, racing a started connector's EOF",
            vec![
                dekaf_validate(json!({"token": "plaintext"})),
                proto::Request::default(),
            ],
        ),
    ] {
        outcomes.push((label, drive_loopback(requests).await));
    }

    insta::assert_snapshot!(render_all(outcomes), @"
    no start on the first request:
      Status(InvalidArgument): the first Connector request must set `start` and exactly one protocol request
    no protocol request on the first request:
      Status(InvalidArgument): the first Connector request must set `start` and exactly one protocol request
    a second start:
      Started(codec=Proto, container=false, process=false, spec=derive)
      Status(InvalidArgument): only the first Connector request may set `start`
    a later request of another protocol:
      Started(codec=Proto, container=false, process=false, spec=derive)
      Status(InvalidArgument): every Connector request must set exactly one protocol request, of the type established by the first request
    an empty request, racing a started connector's EOF:
      Started(codec=Proto, container=false, process=false, spec=materialize)
      Status(InvalidArgument): every Connector request must set exactly one protocol request, of the type established by the first request
    ");
}

/// `sqlite_vfs_uri` is runtime-internal, and rejected before a connector is
/// started: by the wire path for every connector, because it is an unvalidated
/// path which the reactor would open and create, and in-process for every
/// connector but the Sqlite derivation which reads it. `policy.rs` covers the
/// decisions themselves.
#[tokio::test]
async fn sqlite_vfs_uri_is_runtime_internal() {
    let dir = tempfile::tempdir().unwrap();
    let vfs_uri = dir.path().join("probe.db");

    let over_wire = drive_loopback(vec![request(with_start(
        derive_open("acmeCo/derivation"),
        &vfs_uri.to_string_lossy(),
    ))])
    .await;

    let (_service, router) = local_service();
    let in_process = drive_router(
        &router,
        ops::TaskType::Derivation,
        SPEC_TASK_NAME,
        vec![request(with_start(
            json!({"derive": {"spec": {
                "connectorType": "LOCAL",
                "config": sh("exit 0"),
            }}}),
            &vfs_uri.to_string_lossy(),
        ))],
    )
    .await;

    insta::assert_snapshot!(render_all([("wire", over_wire), ("in-process", in_process)]), @"
    wire:
      Status(InvalidArgument): Start.sqlite_vfs_uri is runtime-internal and may not be set by a remote client
    in-process:
      Status(InvalidArgument): Start.sqlite_vfs_uri may only be set for a Sqlite derivation connector
    ");

    assert!(!vfs_uri.exists(), "the reactor did not create {vfs_uri:?}");
}

/// A bearer admits exactly its own task on the `Service` which issued it, and
/// no connector is started otherwise: not for another Service's key, not for a
/// sibling task, and not for the `<spec>` sentinel named by a wire request,
/// which any bearer at all would authorize. `proto-grpc`'s `router.rs` covers
/// the label scope itself.
#[tokio::test]
async fn a_bearer_is_scoped_to_its_task_and_its_service() {
    let (service, _router) = local_service();
    let (_other, other_router) = local_service();

    let foreign_key = drive_endpoint_router(
        service,
        other_router.signer().clone(),
        vec![request(with_start(derive_open("acmeCo/derivation"), ""))],
    )
    .await;

    let other_task = drive_loopback_as(
        ops::TaskType::Derivation,
        "acmeCo/other",
        vec![request(with_start(derive_open("acmeCo/derivation"), ""))],
    )
    .await;

    let sentinel = drive_loopback_as(
        ops::TaskType::Capture,
        "acmeCo/other-task",
        vec![request(with_start(
            json!({"capture": {"validate": {
                "name": SPEC_TASK_NAME,
                "connectorType": "IMAGE",
                "config": {"image": "busybox:latest", "config": {}},
            }}}),
            "",
        ))],
    )
    .await;

    insta::assert_snapshot!(
        render_all([
            ("another service's key", foreign_key),
            ("another task's bearer", other_task),
            ("the <spec> sentinel", sentinel),
        ]),
        @"
    another service's key:
      Status(Unauthenticated): failed to verify token: InvalidSignature
    another task's bearer:
      Status(PermissionDenied): token is not authorized for {estuary.dev/task-name=acmeCo/derivation,estuary.dev/task-type=derivation}
    the <spec> sentinel:
      Status(InvalidArgument): `<spec>` is reserved for Spec requests and cannot name a task
    "
    );
}
