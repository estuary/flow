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

/// A local `Service` and its router which resolve no secrets, for the many
/// tests which never reference one.
fn local_service() -> (connector::Service, connector::ServiceRouter) {
    connector::Service::new_local(
        String::new(),
        None,
        service_kit::Registry::new(),
        std::sync::Arc::new(flow_client_next::secret_resolver::NoOp),
    )
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

/// A task template carrying the build which a session's task update token pins.
fn shard_template() -> serde_json::Value {
    json!({"labels": {"labels": [{"name": labels::BUILD, "value": "1122334455667788"}]}})
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
            "shardTemplate": shard_template(),
        },
    }}}})
}

/// A first request opening `acmeCo/capture` over a `local:` endpoint.
fn capture_open(endpoint: serde_json::Value, secrets: serde_json::Value) -> proto::Request {
    request(with_start(
        json!({"capture": {"open": {"capture": {
            "name": "acmeCo/capture",
            "connectorType": "LOCAL",
            "config": endpoint,
            "secrets": secrets,
            "shardTemplate": shard_template(),
        }}}}),
        "",
    ))
}

/// A first request validating `acmeCo/materialization` against Dekaf, whose
/// `{variant, config}` wrapper the pipeline splits before startup.
fn dekaf_validate(config: serde_json::Value, secrets: serde_json::Value) -> proto::Request {
    request(with_start(
        json!({"materialize": {"validate": {
            "name": "acmeCo/materialization",
            "connectorType": "DEKAF",
            "config": {"variant": "test", "config": config},
            "secrets": secrets,
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

/// A log with its fields, ordered, so that identifiers this crate reports --
/// a resolved secret's lifecycle id, for one -- are asserted alongside the
/// message which carries them.
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

// ------------------------------------------------------------------ secrets --

/// Resolves `acmeCo/password` for the tests which declare it, and panics for
/// those asserting that a rejection happens *before* resolution.
struct SecretResolver {
    reachable: bool,
}

#[tonic::async_trait]
impl flow_client_next::SecretResolver for SecretResolver {
    async fn decrypt(
        &self,
        task_type: ops::TaskType,
        task_name: &str,
        _image: Option<&str>,
        name: models::Secret,
    ) -> anyhow::Result<models::authorizations::SecretDecryption> {
        assert!(self.reachable, "resolution must not be reached");
        match task_type {
            ops::TaskType::Capture => assert_eq!(task_name, "acmeCo/capture"),
            ops::TaskType::Materialization => assert_eq!(task_name, "acmeCo/materialization"),
            _ => panic!("unexpected task type"),
        }
        assert_eq!(name.as_str(), "acmeCo/password");

        Ok(models::authorizations::SecretDecryption {
            value: Some(json!("resolved")),
            secret_id: Some(models::Id::new([1, 2, 3, 4, 5, 6, 7, 8])),
            retry_millis: 0,
        })
    }
}

fn resolving_router(reachable: bool) -> connector::ServiceRouter {
    let (_service, router) = connector::Service::new_local(
        String::new(),
        None,
        service_kit::Registry::new(),
        std::sync::Arc::new(SecretResolver { reachable }),
    );
    router
}

/// Secret resolution happens after Spec, but before the connector receives its
/// initial Open. The connector sees the resolved config while sealedConfig
/// remains the as-published, non-secret baseline, and the lifecycle id is
/// observable in the preceding INFO log.
#[tokio::test]
async fn resolves_secrets_and_preserves_the_published_open_config() {
    // Each `case` fails with a distinct message, so a regression names itself.
    let script = r#"
read spec_request
echo '{"spec":{"protocol":3032023,"configSchema":true,"resourceConfigSchema":true,"documentationUrl":"https://example.test/docs"}}'
read open_request
case "$open_request" in
  *'"config":{"actual":"resolved","base":"published"}'*) ;;
  *) echo 'resolved configuration was not provided' >&2; exit 7 ;;
esac
case "$open_request" in
  *'"sealedConfig":{"base":"published"}'*) ;;
  *) echo 'published baseline was not preserved' >&2; exit 7 ;;
esac
echo '{"opened":{}}'
"#;

    let mut endpoint = sh(script);
    endpoint["config"] = json!({"base": "published"});

    let responses = drive_router(
        &resolving_router(true),
        ops::TaskType::Capture,
        "acmeCo/capture",
        vec![capture_open(
            endpoint,
            json!({"acmeCo/password": "/actual"}),
        )],
    )
    .await;

    insta::assert_snapshot!(render(responses), @r#"
    Log(info): resolved task secret {"secret":"acmeCo/password","secretId":"0102030405060708"}
    Started(codec=Json, container=false, process=false, spec=capture)
    Capture(opened)
    "#);
}

/// A config which both carries a `sops` key and declares a `secrets` stanza is
/// rejected before resolution, which an unreachable resolver proves. Dekaf's
/// wrapper is split before startup, so the same check applies to its *inner*
/// configuration.
#[tokio::test]
async fn rejects_a_sops_key_together_with_a_secrets_stanza() {
    let router = resolving_router(false);
    let secrets = json!({"acmeCo/password": "/actual"});

    // The connector answers the internal Spec, so a rejection is the config
    // check's and not a startup failure's.
    let sops_endpoint = || {
        let mut endpoint =
            sh("read request; echo '{\"spec\":{\"configSchema\":true}}'; read forever");
        endpoint["config"] = json!({"sops": null});
        endpoint
    };

    let outer = drive_router(
        &router,
        ops::TaskType::Capture,
        "acmeCo/capture",
        vec![capture_open(sops_endpoint(), secrets.clone())],
    )
    .await;

    let inner = drive_router(
        &router,
        ops::TaskType::Materialization,
        "acmeCo/materialization",
        vec![dekaf_validate(json!({"sops": null}), secrets)],
    )
    .await;

    insta::assert_snapshot!(render_all([("outer", outer), ("dekaf inner", inner)]), @"
    outer:
      Status(InvalidArgument): endpoint configuration has a top-level `sops` key and cannot also use a `secrets` stanza
    dekaf inner:
      Status(InvalidArgument): endpoint configuration has a top-level `sops` key and cannot also use a `secrets` stanza
    ");
}

/// Dekaf's `{variant, config}` wrapper is split before startup, so the
/// pipeline resolves the *inner* configuration and Dekaf validates its token
/// from the request slot -- whether that token was published in plaintext or
/// resolved from a secret.
#[tokio::test]
async fn dekaf_resolves_inner_configuration() {
    let router = resolving_router(true);

    let mut outcomes = Vec::new();
    for (label, config, secrets) in [
        ("plaintext token", json!({"token": "plaintext"}), json!({})),
        (
            "resolved secret",
            json!({}),
            json!({"acmeCo/password": "/token"}),
        ),
    ] {
        let responses = drive_router(
            &router,
            ops::TaskType::Materialization,
            "acmeCo/materialization",
            vec![dekaf_validate(config, secrets)],
        )
        .await;
        outcomes.push((label, responses));
    }

    insta::assert_snapshot!(render_all(outcomes), @r#"
    plaintext token:
      Started(codec=Proto, container=false, process=false, spec=materialize)
      Materialize(validated)
    resolved secret:
      Log(info): resolved task secret {"secret":"acmeCo/password","secretId":"0102030405060708"}
      Started(codec=Proto, container=false, process=false, spec=materialize)
      Materialize(validated)
    "#);
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
    let (service, router) = connector::Service::new_local(
        String::new(),
        None,
        registry.clone(),
        std::sync::Arc::new(flow_client_next::secret_resolver::NoOp),
    );

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
                dekaf_validate(json!({"token": "plaintext"}), json!({})),
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

// ----------------------------------------------------------------- mount --

/// Every local connector gets a `CONNECTOR_MOUNT` directory, whether or not
/// anything is in it. `task-update.json` is in it only where the Service holds
/// a `TaskUpdate` -- and its absence is how a connector knows to rotate in
/// memory only, which is every `Service::new_local` context.
#[tokio::test]
async fn provides_task_update_through_the_connector_mount() {
    // Each check fails with its own message, so a regression names itself
    // rather than surfacing as an opaque non-zero exit.
    let script = r#"
read spec_request
echo '{"spec":{"protocol":3032023,"configSchema":true,"resourceConfigSchema":true,"documentationUrl":"https://example.test/docs"}}'
read open_request
if [ -z "$CONNECTOR_MOUNT" ]; then echo 'CONNECTOR_MOUNT is unset' >&2; exit 7; fi
if [ ! -d "$CONNECTOR_MOUNT" ]; then echo 'CONNECTOR_MOUNT is not a directory' >&2; exit 7; fi
if [ "$LOG_FORMAT" != json ]; then echo "LOG_FORMAT is $LOG_FORMAT" >&2; exit 7; fi
if [ "$LOG_LEVEL" != debug ]; then echo "LOG_LEVEL is $LOG_LEVEL" >&2; exit 7; fi

file="$CONNECTOR_MOUNT/task-update.json"
if [ "$EXPECT_TASK_UPDATE" = yes ]; then
  if [ ! -f "$file" ]; then echo 'task-update.json is missing' >&2; exit 7; fi
  for property in '"token"' '"control_plane_url"' '"config_encryption_url"'; do
    if ! grep -q "$property" "$file"; then
      echo "task-update.json has no $property" >&2; exit 7
    fi
  done
elif [ -e "$file" ]; then
  echo 'task-update.json is present' >&2; exit 7
fi
echo '{"opened":{}}'
"#;

    let run = async |router: &connector::ServiceRouter, expect: &str| {
        let mut endpoint = sh(script);
        endpoint["env"] = json!({
            "CONNECTOR_MOUNT": "/configured/connector/mount",
            "EXPECT_TASK_UPDATE": expect,
            "LOG_FORMAT": "configured-format",
            "LOG_LEVEL": "error",
        });

        drive_router(
            router,
            ops::TaskType::Capture,
            "acmeCo/capture",
            vec![capture_open(endpoint, json!({}))],
        )
        .await
    };

    let (_service, local) = local_service();

    // A failed check fails the connector, so an identical pair of clean
    // sessions is what both expectations being met looks like.
    let outcomes = [
        (
            "with task update",
            run(
                &task_update_router(std::time::Duration::from_secs(600)),
                "yes",
            )
            .await,
        ),
        ("new_local has an empty mount", run(&local, "no").await),
    ];

    insta::assert_snapshot!(render_all(outcomes), @"
    with task update:
      Started(codec=Json, container=false, process=false, spec=capture)
      Capture(opened)
    new_local has an empty mount:
      Started(codec=Json, container=false, process=false, spec=capture)
      Capture(opened)
    ");
}

/// The mounted credential is refreshed *in place*, which a running connector
/// observes by re-reading the file it already read once. A connector which
/// cached the first token would hold an expiring one; this is the behavior
/// that makes re-reading correct.
#[tokio::test]
async fn refreshes_the_mounted_token_in_place() {
    // Claims are second-granular, so two mints within one second are the same
    // token: the sleep must cross a second boundary, not merely a tick.
    let script = r#"
read spec_request
echo '{"spec":{"protocol":3032023,"configSchema":true,"resourceConfigSchema":true,"documentationUrl":"https://example.test/docs"}}'
read open_request
file="$CONNECTOR_MOUNT/task-update.json"
token() { sed -n 's/.*"token":"\([^"]*\)".*/\1/p' "$file"; }

before=$(token)
if [ -z "$before" ]; then echo 'task-update.json has no token' >&2; exit 7; fi
sleep 2
after=$(token)
if [ -z "$after" ]; then echo 'task-update.json has no token after refresh' >&2; exit 7; fi
if [ "$before" = "$after" ]; then echo 'the token was not refreshed' >&2; exit 7; fi
echo '{"opened":{}}'
"#;

    let responses = drive_router(
        &task_update_router(std::time::Duration::from_millis(200)),
        ops::TaskType::Capture,
        "acmeCo/capture",
        vec![capture_open(sh(script), json!({}))],
    )
    .await;

    insta::assert_snapshot!(render(responses), @"
    Started(codec=Json, container=false, process=false, spec=capture)
    Capture(opened)
    ");
}

/// A router over a Service holding a `TaskUpdate`, mirroring
/// `Service::new_local` which deliberately holds none.
fn task_update_router(refresh_interval: std::time::Duration) -> connector::ServiceRouter {
    let key: [u8; 32] = rand::random();

    let mut task_update = connector::TaskUpdate::new(
        proto_grpc::Signer::new(
            "fqdn.example.com".to_string(),
            tokens::jwt::EncodingKey::from_secret(b"a reactor's data-plane key"),
        ),
        url::Url::parse("https://control.example.com/").unwrap(),
        url::Url::parse("https://config-encryption.example.com/").unwrap(),
    );
    task_update.refresh_interval = refresh_interval;

    let service = connector::Service::new(
        connector::Plane::Local,
        String::new(),
        None,
        proto_grpc::Authenticator::new(
            connector::LOCAL_ISSUER.to_string(),
            vec![tokens::jwt::DecodingKey::from_secret(&key)],
        ),
        None,
        service_kit::Registry::new(),
        std::sync::Arc::new(flow_client_next::secret_resolver::NoOp),
        Some(task_update),
    );
    connector::ServiceRouter::new(
        service,
        proto_grpc::Signer::new(
            connector::LOCAL_ISSUER.to_string(),
            tokens::jwt::EncodingKey::from_secret(&key),
        ),
    )
}

mod execution {
    use super::*;
    use proto_flow::{capture, derive, flow, materialize};

    fn vmm_fixture() -> connector::Vmm {
        connector::Vmm {
            image: "ghcr.io/estuary/connector-vmm:dev".to_string(),
            podman: "/nonexistent/connector-vmm-tests/podman".to_string(),
            state_dir: "/nonexistent/connector-vmm-tests/state".to_string(),
            disk_mib: 2048,
            memory_limit: "1g".to_string(),
            cpu_limit: "2".to_string(),
            guest_memory_mib: 768,
            vcpus: 2,
            cgroup_parent: None,
        }
    }

    fn start(sqlite_vfs_uri: &str) -> proto::request::Start {
        proto::request::Start {
            log_level: ops::LogLevel::Debug as i32,
            sqlite_vfs_uri: sqlite_vfs_uri.to_string(),
            ..Default::default()
        }
    }

    fn derive_open(collection: &str) -> proto::request::Kind {
        super::request(super::derive_open(collection)).kind.unwrap()
    }

    fn derive_request(request: derive::Request) -> proto::Request {
        proto::Request {
            start: Some(start("")),
            kind: Some(proto::request::Kind::Derive(request)),
        }
    }

    const PYTHON_IMAGE: &str = "ghcr.io/estuary/derive-python:stable";

    fn vmm_execution() -> Option<flow::ConnectorExecution> {
        Some(flow::ConnectorExecution {
            vmm: true,
            egress: None,
        })
    }

    fn egress_execution(vmm: bool, hosts: &[&str]) -> Option<flow::ConnectorExecution> {
        Some(flow::ConnectorExecution {
            vmm,
            egress: Some(flow::connector_execution::Egress {
                hosts: hosts.iter().map(ToString::to_string).collect(),
            }),
        })
    }

    fn start_with(execution: Option<flow::ConnectorExecution>) -> proto::request::Start {
        proto::request::Start {
            execution,
            ..start("")
        }
    }

    fn vmm_service(capable: bool) -> (connector::Service, connector::ServiceRouter) {
        let vmm = capable.then(vmm_fixture);
        connector::Service::new_local(
            String::new(),
            vmm,
            service_kit::Registry::new(),
            std::sync::Arc::new(flow_client_next::secret_resolver::NoOp),
        )
    }

    /// Every first-request shape of every protocol, as a connector whose endpoint
    /// is `connector_type` and `config_json`. Requests which embed the task's
    /// built spec (Apply and Open) carry `spec_execution` within it.
    fn every_first_request(
        connector_type: fn(ops::TaskType) -> i32,
        config_json: &bytes::Bytes,
        spec_execution: Option<flow::ConnectorExecution>,
    ) -> Vec<(
        &'static str,
        ops::TaskType,
        &'static str,
        proto::request::Kind,
    )> {
        use ops::TaskType::{Capture, Derivation, Materialization};
        let (task, spec_task) = ("acmeCo/task", SPEC_TASK_NAME);
        let config_json = config_json.clone();

        let capture_spec = flow::CaptureSpec {
            name: task.to_string(),
            connector_type: connector_type(Capture),
            config_json: config_json.clone(),
            execution: spec_execution.clone(),
            shard_template: serde_json::from_value(super::shard_template()).unwrap(),
            ..Default::default()
        };
        let collection_spec = flow::CollectionSpec {
            name: task.to_string(),
            derivation: Some(Box::new(flow::collection_spec::Derivation {
                connector_type: connector_type(Derivation),
                config_json: config_json.clone(),
                execution: spec_execution.clone(),
                shard_template: serde_json::from_value(super::shard_template()).unwrap(),
                ..Default::default()
            })),
            ..Default::default()
        };
        let materialization_spec = flow::MaterializationSpec {
            name: task.to_string(),
            connector_type: connector_type(Materialization),
            config_json: config_json.clone(),
            execution: spec_execution.clone(),
            shard_template: serde_json::from_value(super::shard_template()).unwrap(),
            ..Default::default()
        };

        let capture = |kind| {
            proto::request::Kind::Capture(capture::Request {
                kind: Some(kind),
                ..Default::default()
            })
        };
        let derive = |kind| {
            proto::request::Kind::Derive(derive::Request {
                kind: Some(kind),
                ..Default::default()
            })
        };
        let materialize = |kind| {
            proto::request::Kind::Materialize(materialize::Request {
                kind: Some(kind),
                ..Default::default()
            })
        };

        vec![
            (
                "capture Spec",
                Capture,
                spec_task,
                capture(capture::request::Kind::Spec(capture::request::Spec {
                    connector_type: connector_type(Capture),
                    config_json: config_json.clone(),
                })),
            ),
            (
                "capture Discover",
                Capture,
                task,
                capture(capture::request::Kind::Discover(Box::new(
                    capture::request::Discover {
                        name: task.to_string(),
                        connector_type: connector_type(Capture),
                        config_json: config_json.clone(),
                        ..Default::default()
                    },
                ))),
            ),
            (
                "capture Validate",
                Capture,
                task,
                capture(capture::request::Kind::Validate(Box::new(
                    capture::request::Validate {
                        name: task.to_string(),
                        connector_type: connector_type(Capture),
                        config_json: config_json.clone(),
                        ..Default::default()
                    },
                ))),
            ),
            (
                "capture Apply",
                Capture,
                task,
                capture(capture::request::Kind::Apply(Box::new(
                    capture::request::Apply {
                        capture: Some(capture_spec.clone()),
                        ..Default::default()
                    },
                ))),
            ),
            (
                "capture Open",
                Capture,
                task,
                capture(capture::request::Kind::Open(Box::new(
                    capture::request::Open {
                        capture: Some(capture_spec),
                        ..Default::default()
                    },
                ))),
            ),
            (
                "derive Spec",
                Derivation,
                spec_task,
                derive(derive::request::Kind::Spec(derive::request::Spec {
                    connector_type: connector_type(Derivation),
                    config_json: config_json.clone(),
                })),
            ),
            (
                "derive Validate",
                Derivation,
                task,
                derive(derive::request::Kind::Validate(Box::new(
                    derive::request::Validate {
                        connector_type: connector_type(Derivation),
                        config_json: config_json.clone(),
                        collection: Some(flow::CollectionSpec {
                            name: task.to_string(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                ))),
            ),
            (
                "derive Open",
                Derivation,
                task,
                derive(derive::request::Kind::Open(Box::new(
                    derive::request::Open {
                        collection: Some(collection_spec),
                        ..Default::default()
                    },
                ))),
            ),
            (
                "materialize Spec",
                Materialization,
                spec_task,
                materialize(materialize::request::Kind::Spec(
                    materialize::request::Spec {
                        connector_type: connector_type(Materialization),
                        config_json: config_json.clone(),
                    },
                )),
            ),
            (
                "materialize Validate",
                Materialization,
                task,
                materialize(materialize::request::Kind::Validate(Box::new(
                    materialize::request::Validate {
                        name: task.to_string(),
                        connector_type: connector_type(Materialization),
                        config_json: config_json.clone(),
                        ..Default::default()
                    },
                ))),
            ),
            (
                "materialize Apply",
                Materialization,
                task,
                materialize(materialize::request::Kind::Apply(Box::new(
                    materialize::request::Apply {
                        materialization: Some(materialization_spec.clone()),
                        ..Default::default()
                    },
                ))),
            ),
            (
                "materialize Open",
                Materialization,
                task,
                materialize(materialize::request::Kind::Open(Box::new(
                    materialize::request::Open {
                        materialization: Some(materialization_spec),
                        ..Default::default()
                    },
                ))),
            ),
        ]
    }

    fn image_connector_type(task_type: ops::TaskType) -> i32 {
        match task_type {
            ops::TaskType::Capture => flow::capture_spec::ConnectorType::Image as i32,
            ops::TaskType::Derivation => {
                flow::collection_spec::derivation::ConnectorType::Image as i32
            }
            ops::TaskType::Materialization => {
                flow::materialization_spec::ConnectorType::Image as i32
            }
            _ => unreachable!(),
        }
    }

    fn local_connector_type(task_type: ops::TaskType) -> i32 {
        match task_type {
            ops::TaskType::Capture => flow::capture_spec::ConnectorType::Local as i32,
            ops::TaskType::Derivation => {
                flow::collection_spec::derivation::ConnectorType::Local as i32
            }
            ops::TaskType::Materialization => {
                flow::materialization_spec::ConnectorType::Local as i32
            }
            _ => unreachable!(),
        }
    }

    async fn drive_each(
        router: &connector::ServiceRouter,
        start: proto::request::Start,
        requests: Vec<(
            &'static str,
            ops::TaskType,
            &'static str,
            proto::request::Kind,
        )>,
    ) -> Vec<String> {
        let mut rows = Vec::new();
        for (label, task_type, task_name, kind) in requests {
            let responses = drive_router(
                router,
                task_type,
                task_name,
                vec![proto::Request {
                    start: Some(start.clone()),
                    kind: Some(kind),
                }],
            )
            .await;
            rows.push(format!("{label}: {}", render_lines(responses).join(" | ")));
        }
        rows
    }

    /// Every protocol refuses VMM execution of an ineligible connector, and of
    /// any connector on a service without the capability, before starting it. An
    /// eligible connector on a capable service is launched in a VMM, which here
    /// fails at the fixture's state directory: never as an ordinary container.
    #[tokio::test]
    async fn vmm_requests_never_start_a_connector() {
        let config = serde_json::json!({"image": PYTHON_IMAGE, "config": {}});
        let config: bytes::Bytes = config.to_string().into();

        let mut rows = Vec::new();
        for capable in [false, true] {
            let (_service, router) = vmm_service(capable);
            let requests = every_first_request(image_connector_type, &config, vmm_execution());

            for row in drive_each(&router, start_with(vmm_execution()), requests).await {
                rows.push(format!("capable={capable} {row}"));
            }
        }

        insta::assert_snapshot!(rows.join("\n"), @r#"
    capable=false capture Spec: Status(InvalidArgument): connector image 'ghcr.io/estuary/derive-python:stable' is not eligible for VMM execution as a capture
    capable=false capture Discover: Status(InvalidArgument): connector image 'ghcr.io/estuary/derive-python:stable' is not eligible for VMM execution as a capture
    capable=false capture Validate: Status(InvalidArgument): connector image 'ghcr.io/estuary/derive-python:stable' is not eligible for VMM execution as a capture
    capable=false capture Apply: Status(InvalidArgument): connector image 'ghcr.io/estuary/derive-python:stable' is not eligible for VMM execution as a capture
    capable=false capture Open: Status(InvalidArgument): connector image 'ghcr.io/estuary/derive-python:stable' is not eligible for VMM execution as a capture
    capable=false derive Spec: Status(FailedPrecondition): this data plane does not support VMM execution
    capable=false derive Validate: Status(FailedPrecondition): this data plane does not support VMM execution
    capable=false derive Open: Status(FailedPrecondition): this data plane does not support VMM execution
    capable=false materialize Spec: Status(InvalidArgument): connector image 'ghcr.io/estuary/derive-python:stable' is not eligible for VMM execution as a materialization
    capable=false materialize Validate: Status(InvalidArgument): connector image 'ghcr.io/estuary/derive-python:stable' is not eligible for VMM execution as a materialization
    capable=false materialize Apply: Status(InvalidArgument): connector image 'ghcr.io/estuary/derive-python:stable' is not eligible for VMM execution as a materialization
    capable=false materialize Open: Status(InvalidArgument): connector image 'ghcr.io/estuary/derive-python:stable' is not eligible for VMM execution as a materialization
    capable=true capture Spec: Status(InvalidArgument): connector image 'ghcr.io/estuary/derive-python:stable' is not eligible for VMM execution as a capture
    capable=true capture Discover: Status(InvalidArgument): connector image 'ghcr.io/estuary/derive-python:stable' is not eligible for VMM execution as a capture
    capable=true capture Validate: Status(InvalidArgument): connector image 'ghcr.io/estuary/derive-python:stable' is not eligible for VMM execution as a capture
    capable=true capture Apply: Status(InvalidArgument): connector image 'ghcr.io/estuary/derive-python:stable' is not eligible for VMM execution as a capture
    capable=true capture Open: Status(InvalidArgument): connector image 'ghcr.io/estuary/derive-python:stable' is not eligible for VMM execution as a capture
    capable=true derive Spec: Status(FailedPrecondition): CONNECTOR_VMM_STATE_DIR /nonexistent/connector-vmm-tests/state: No such file or directory (os error 2)
    capable=true derive Validate: Status(FailedPrecondition): CONNECTOR_VMM_STATE_DIR /nonexistent/connector-vmm-tests/state: No such file or directory (os error 2)
    capable=true derive Open: Status(FailedPrecondition): CONNECTOR_VMM_STATE_DIR /nonexistent/connector-vmm-tests/state: No such file or directory (os error 2)
    capable=true materialize Spec: Status(InvalidArgument): connector image 'ghcr.io/estuary/derive-python:stable' is not eligible for VMM execution as a materialization
    capable=true materialize Validate: Status(InvalidArgument): connector image 'ghcr.io/estuary/derive-python:stable' is not eligible for VMM execution as a materialization
    capable=true materialize Apply: Status(InvalidArgument): connector image 'ghcr.io/estuary/derive-python:stable' is not eligible for VMM execution as a materialization
    capable=true materialize Open: Status(InvalidArgument): connector image 'ghcr.io/estuary/derive-python:stable' is not eligible for VMM execution as a materialization
    "#);
    }

    #[tokio::test]
    async fn vmm_requests_of_local_connectors_never_run_them() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("ran");
        let config = serde_json::json!({
            "command": ["/bin/sh", "-c", format!("touch {}", marker.display())],
            "config": {},
        });
        let config: bytes::Bytes = config.to_string().into();
        let (_service, router) = vmm_service(true);

        let requests = every_first_request(local_connector_type, &config, vmm_execution());
        let rows = drive_each(&router, start_with(vmm_execution()), requests).await;

        for row in &rows {
            assert!(
                row.ends_with(
                    ": Status(InvalidArgument): VMM execution requires an image connector"
                ),
                "{row}"
            );
        }
        assert_eq!(rows.len(), 12);
        assert!(!marker.exists(), "a refused local connector ran");

        // An in-process connector is refused the same way.
        let responses = drive_router(
            &router,
            ops::TaskType::Derivation,
            "acmeCo/derivation",
            vec![proto::Request {
                start: Some(start_with(vmm_execution())),
                kind: Some(derive_open("acmeCo/derivation")),
            }],
        )
        .await;
        insta::assert_debug_snapshot!(render_lines(responses), @r#"
    [
        "Status(InvalidArgument): Start.execution ConnectorExecution { vmm: true, egress: None } differs from the execution ConnectorExecution { vmm: false, egress: None } of the task's built spec",
    ]
    "#);

        let responses = drive_router(
            &router,
            ops::TaskType::Derivation,
            SPEC_TASK_NAME,
            vec![proto::Request {
                start: Some(start_with(vmm_execution())),
                kind: Some(proto::request::Kind::Derive(derive::Request {
                    kind: Some(derive::request::Kind::Spec(derive::request::Spec {
                        connector_type: flow::collection_spec::derivation::ConnectorType::Sqlite
                            as i32,
                        config_json: r#"{"migrations":[]}"#.into(),
                    })),
                    ..Default::default()
                })),
            }],
        )
        .await;
        insta::assert_debug_snapshot!(render_lines(responses), @r#"
    [
        "Status(InvalidArgument): VMM execution requires an image connector",
    ]
    "#);
    }

    #[tokio::test]
    async fn apply_and_open_must_start_with_the_built_spec_execution() {
        let config = serde_json::json!({"image": PYTHON_IMAGE, "config": {}});
        let config: bytes::Bytes = config.to_string().into();
        let (_service, router) = vmm_service(true);
        let embeds_spec = |label: &str| label.ends_with(" Apply") || label.ends_with(" Open");

        let mut rows = Vec::new();
        for (start, spec) in [(None, vmm_execution()), (vmm_execution(), None)] {
            let requests = every_first_request(image_connector_type, &config, spec.clone())
                .into_iter()
                .filter(|(label, ..)| embeds_spec(label))
                .collect();

            for row in drive_each(&router, start_with(start.clone()), requests).await {
                rows.push(format!(
                    "start vmm={} spec vmm={} {row}",
                    start.is_some(),
                    spec.is_some()
                ));
            }
        }

        insta::assert_snapshot!(rows.join("\n"), @r"
    start vmm=false spec vmm=true capture Apply: Status(InvalidArgument): Start.execution ConnectorExecution { vmm: false, egress: None } differs from the execution ConnectorExecution { vmm: true, egress: None } of the task's built spec
    start vmm=false spec vmm=true capture Open: Status(InvalidArgument): Start.execution ConnectorExecution { vmm: false, egress: None } differs from the execution ConnectorExecution { vmm: true, egress: None } of the task's built spec
    start vmm=false spec vmm=true derive Open: Status(InvalidArgument): Start.execution ConnectorExecution { vmm: false, egress: None } differs from the execution ConnectorExecution { vmm: true, egress: None } of the task's built spec
    start vmm=false spec vmm=true materialize Apply: Status(InvalidArgument): Start.execution ConnectorExecution { vmm: false, egress: None } differs from the execution ConnectorExecution { vmm: true, egress: None } of the task's built spec
    start vmm=false spec vmm=true materialize Open: Status(InvalidArgument): Start.execution ConnectorExecution { vmm: false, egress: None } differs from the execution ConnectorExecution { vmm: true, egress: None } of the task's built spec
    start vmm=true spec vmm=false capture Apply: Status(InvalidArgument): Start.execution ConnectorExecution { vmm: true, egress: None } differs from the execution ConnectorExecution { vmm: false, egress: None } of the task's built spec
    start vmm=true spec vmm=false capture Open: Status(InvalidArgument): Start.execution ConnectorExecution { vmm: true, egress: None } differs from the execution ConnectorExecution { vmm: false, egress: None } of the task's built spec
    start vmm=true spec vmm=false derive Open: Status(InvalidArgument): Start.execution ConnectorExecution { vmm: true, egress: None } differs from the execution ConnectorExecution { vmm: false, egress: None } of the task's built spec
    start vmm=true spec vmm=false materialize Apply: Status(InvalidArgument): Start.execution ConnectorExecution { vmm: true, egress: None } differs from the execution ConnectorExecution { vmm: false, egress: None } of the task's built spec
    start vmm=true spec vmm=false materialize Open: Status(InvalidArgument): Start.execution ConnectorExecution { vmm: true, egress: None } differs from the execution ConnectorExecution { vmm: false, egress: None } of the task's built spec
    ");
    }

    /// Declared egress, even with no hosts, is refused by every protocol of an
    /// ordinary execution, whether its connector is an image, a local command or
    /// in-process, before the connector starts. A capable service changes nothing.
    #[tokio::test]
    async fn egress_without_a_vmm_never_starts_a_connector() {
        const REFUSAL: &str = "Status(InvalidArgument): the task declares egress, which ordinary \
                           execution cannot enforce; egress is enforced only by VMM execution";

        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("ran");
        let local = serde_json::json!({
            "command": ["/bin/sh", "-c", format!("touch {}", marker.display())],
            "config": {},
        });
        let local: bytes::Bytes = local.to_string().into();
        let image =
            serde_json::json!({"image": "ghcr.io/estuary/source-hello-world:dev", "config": {}});
        let image: bytes::Bytes = image.to_string().into();

        for capable in [false, true] {
            let (_service, router) = vmm_service(capable);
            for execution in [
                egress_execution(false, &[]),
                egress_execution(false, &["api.acmeco.example"]),
            ] {
                for (connector_type, config) in [
                    (image_connector_type as fn(ops::TaskType) -> i32, &image),
                    (local_connector_type, &local),
                ] {
                    let requests = every_first_request(connector_type, config, execution.clone());
                    let rows = drive_each(&router, start_with(execution.clone()), requests).await;

                    assert_eq!(rows.len(), 12);
                    for row in &rows {
                        assert!(row.ends_with(REFUSAL), "capable={capable} {row}");
                    }
                }

                let responses = drive_router(
                    &router,
                    ops::TaskType::Derivation,
                    SPEC_TASK_NAME,
                    vec![proto::Request {
                        start: Some(start_with(execution.clone())),
                        kind: Some(proto::request::Kind::Derive(derive::Request {
                            kind: Some(derive::request::Kind::Spec(derive::request::Spec {
                                connector_type:
                                    flow::collection_spec::derivation::ConnectorType::Sqlite as i32,
                                config_json: r#"{"migrations":[]}"#.into(),
                            })),
                            ..Default::default()
                        })),
                    }],
                )
                .await;
                assert_eq!(
                    render_lines(responses),
                    [REFUSAL],
                    "capable={capable} in-process"
                );
            }
        }
        assert!(!marker.exists(), "a refused local connector ran");
    }

    #[tokio::test]
    async fn apply_and_open_must_start_with_the_built_spec_egress() {
        let config = serde_json::json!({"image": PYTHON_IMAGE, "config": {}});
        let config: bytes::Bytes = config.to_string().into();
        let (_service, router) = vmm_service(true);

        let mut rows = Vec::new();
        for (start, spec) in [
            (vmm_execution(), egress_execution(true, &[])),
            (egress_execution(true, &[]), vmm_execution()),
            (
                egress_execution(true, &["API.acmeco.example"]),
                egress_execution(true, &["api.acmeco.example"]),
            ),
        ] {
            let requests = every_first_request(image_connector_type, &config, spec.clone())
                .into_iter()
                .filter(|(label, ..)| label.ends_with(" Apply") || label.ends_with(" Open"))
                .collect();

            rows.extend(drive_each(&router, start_with(start.clone()), requests).await);
        }

        insta::assert_snapshot!(rows.join("\n"), @r#"
    capture Apply: Status(InvalidArgument): Start.execution ConnectorExecution { vmm: true, egress: None } differs from the execution ConnectorExecution { vmm: true, egress: Some(Egress { hosts: [] }) } of the task's built spec
    capture Open: Status(InvalidArgument): Start.execution ConnectorExecution { vmm: true, egress: None } differs from the execution ConnectorExecution { vmm: true, egress: Some(Egress { hosts: [] }) } of the task's built spec
    derive Open: Status(InvalidArgument): Start.execution ConnectorExecution { vmm: true, egress: None } differs from the execution ConnectorExecution { vmm: true, egress: Some(Egress { hosts: [] }) } of the task's built spec
    materialize Apply: Status(InvalidArgument): Start.execution ConnectorExecution { vmm: true, egress: None } differs from the execution ConnectorExecution { vmm: true, egress: Some(Egress { hosts: [] }) } of the task's built spec
    materialize Open: Status(InvalidArgument): Start.execution ConnectorExecution { vmm: true, egress: None } differs from the execution ConnectorExecution { vmm: true, egress: Some(Egress { hosts: [] }) } of the task's built spec
    capture Apply: Status(InvalidArgument): Start.execution ConnectorExecution { vmm: true, egress: Some(Egress { hosts: [] }) } differs from the execution ConnectorExecution { vmm: true, egress: None } of the task's built spec
    capture Open: Status(InvalidArgument): Start.execution ConnectorExecution { vmm: true, egress: Some(Egress { hosts: [] }) } differs from the execution ConnectorExecution { vmm: true, egress: None } of the task's built spec
    derive Open: Status(InvalidArgument): Start.execution ConnectorExecution { vmm: true, egress: Some(Egress { hosts: [] }) } differs from the execution ConnectorExecution { vmm: true, egress: None } of the task's built spec
    materialize Apply: Status(InvalidArgument): Start.execution ConnectorExecution { vmm: true, egress: Some(Egress { hosts: [] }) } differs from the execution ConnectorExecution { vmm: true, egress: None } of the task's built spec
    materialize Open: Status(InvalidArgument): Start.execution ConnectorExecution { vmm: true, egress: Some(Egress { hosts: [] }) } differs from the execution ConnectorExecution { vmm: true, egress: None } of the task's built spec
    capture Apply: Status(InvalidArgument): Start.execution ConnectorExecution { vmm: true, egress: Some(Egress { hosts: ["API.acmeco.example"] }) } differs from the execution ConnectorExecution { vmm: true, egress: Some(Egress { hosts: ["api.acmeco.example"] }) } of the task's built spec
    capture Open: Status(InvalidArgument): Start.execution ConnectorExecution { vmm: true, egress: Some(Egress { hosts: ["API.acmeco.example"] }) } differs from the execution ConnectorExecution { vmm: true, egress: Some(Egress { hosts: ["api.acmeco.example"] }) } of the task's built spec
    derive Open: Status(InvalidArgument): Start.execution ConnectorExecution { vmm: true, egress: Some(Egress { hosts: ["API.acmeco.example"] }) } differs from the execution ConnectorExecution { vmm: true, egress: Some(Egress { hosts: ["api.acmeco.example"] }) } of the task's built spec
    materialize Apply: Status(InvalidArgument): Start.execution ConnectorExecution { vmm: true, egress: Some(Egress { hosts: ["API.acmeco.example"] }) } differs from the execution ConnectorExecution { vmm: true, egress: Some(Egress { hosts: ["api.acmeco.example"] }) } of the task's built spec
    materialize Open: Status(InvalidArgument): Start.execution ConnectorExecution { vmm: true, egress: Some(Egress { hosts: ["API.acmeco.example"] }) } differs from the execution ConnectorExecution { vmm: true, egress: Some(Egress { hosts: ["api.acmeco.example"] }) } of the task's built spec
    "#);
    }

    /// Task hosts are validated before anything is pulled: an invalid one is
    /// refused by a capable service before its launch reaches the state
    /// directory, where the fixture's launch otherwise fails.
    #[tokio::test]
    async fn invalid_task_hosts_are_refused_before_a_launch() {
        let config = serde_json::json!({"image": PYTHON_IMAGE, "config": {}});
        let (_service, router) = vmm_service(true);

        let mut rows = Vec::new();
        for hosts in [
            &["api.acmeco.example", "*.com"][..],
            &["api.acmeco.example:443"][..],
            &["api.acmeco.example", "*.svc.acmeco.example"][..],
        ] {
            let responses = drive_router(
                &router,
                ops::TaskType::Derivation,
                SPEC_TASK_NAME,
                vec![proto::Request {
                    start: Some(start_with(egress_execution(true, hosts))),
                    kind: Some(proto::request::Kind::Derive(derive::Request {
                        kind: Some(derive::request::Kind::Spec(derive::request::Spec {
                            connector_type: flow::collection_spec::derivation::ConnectorType::Image
                                as i32,
                            config_json: config.to_string().into(),
                        })),
                        ..Default::default()
                    })),
                }],
            )
            .await;
            rows.push(format!(
                "{hosts:?}: {}",
                render_lines(responses).join(" | ")
            ));
        }

        insta::assert_snapshot!(rows.join("\n"), @r#"
    ["api.acmeco.example", "*.com"]: Status(InvalidArgument): task egress.hosts "*.com" is a wildcard over the public suffix "com", in the Public Suffix List's ICANN section; names beneath a public suffix belong to unrelated registrants
    ["api.acmeco.example:443"]: Status(InvalidArgument): task egress.hosts "api.acmeco.example:443" has a label outside [a-z0-9-]: "example:443"
    ["api.acmeco.example", "*.svc.acmeco.example"]: Status(FailedPrecondition): CONNECTOR_VMM_STATE_DIR /nonexistent/connector-vmm-tests/state: No such file or directory (os error 2)
    "#);
    }

    #[tokio::test]
    async fn a_wire_client_requests_vmm_execution() {
        let (service, router) = vmm_service(true);
        let config = serde_json::json!({"image": PYTHON_IMAGE, "config": {}});

        let responses = drive_endpoint_router(
            service,
            router.signer().clone(),
            vec![proto::Request {
                start: Some(start_with(vmm_execution())),
                kind: Some(proto::request::Kind::Derive(derive::Request {
                    kind: Some(derive::request::Kind::Spec(derive::request::Spec {
                        connector_type: flow::collection_spec::derivation::ConnectorType::Image
                            as i32,
                        config_json: config.to_string().into(),
                    })),
                    ..Default::default()
                })),
            }],
        )
        .await;

        insta::assert_debug_snapshot!(render_lines(responses), @r#"
    [
        "Status(FailedPrecondition): CONNECTOR_VMM_STATE_DIR /nonexistent/connector-vmm-tests/state: No such file or directory (os error 2)",
    ]
    "#);
    }

    /// The public-plane refusal of Python derivations is lifted for a VMM launch
    /// alone: an ordinary start is refused before anything is pulled, while a
    /// VMM start goes on to its launch, which fails here at the fixture's state
    /// directory.
    #[tokio::test]
    async fn public_python_runs_only_in_a_vmm() {
        let key: [u8; 32] = rand::random();
        let service = connector::Service::new(
            connector::Plane::Public,
            String::new(),
            Some(vmm_fixture()),
            proto_grpc::Authenticator::new(
                connector::LOCAL_ISSUER.to_string(),
                vec![tokens::jwt::DecodingKey::from_secret(&key)],
            ),
            None,
            service_kit::Registry::new(),
            std::sync::Arc::new(flow_client_next::secret_resolver::NoOp),
            None,
        );
        let signer = proto_grpc::Signer::new(
            connector::LOCAL_ISSUER.to_string(),
            tokens::jwt::EncodingKey::from_secret(&key),
        );
        let router = connector::ServiceRouter::new(service, signer);
        let config = serde_json::json!({"image": PYTHON_IMAGE, "config": {}});

        let mut rows = Vec::new();
        for execution in [None, vmm_execution()] {
            let responses = drive_router(
                &router,
                ops::TaskType::Derivation,
                SPEC_TASK_NAME,
                vec![proto::Request {
                    start: Some(start_with(execution.clone())),
                    kind: Some(proto::request::Kind::Derive(derive::Request {
                        kind: Some(derive::request::Kind::Spec(derive::request::Spec {
                            connector_type: flow::collection_spec::derivation::ConnectorType::Image
                                as i32,
                            config_json: config.to_string().into(),
                        })),
                        ..Default::default()
                    })),
                }],
            )
            .await;
            rows.push(format!(
                "vmm={}: {}",
                execution.is_some(),
                render_lines(responses).join(" | ")
            ));
        }

        insta::assert_snapshot!(rows.join("\n"), @r#"
    vmm=false: Status(Unknown): Python derivations may only run in private data-planes
    vmm=true: Status(FailedPrecondition): CONNECTOR_VMM_STATE_DIR /nonexistent/connector-vmm-tests/state: No such file or directory (os error 2)
    "#);
    }

    #[tokio::test]
    async fn explicitly_ordinary_execution_starts_ordinarily() {
        let (_service, router) = vmm_service(true);

        let responses = drive_router(
            &router,
            ops::TaskType::Derivation,
            "acmeCo/derivation",
            vec![proto::Request {
                start: Some(start_with(Some(flow::ConnectorExecution {
                    vmm: false,
                    egress: None,
                }))),
                kind: Some(derive_open("acmeCo/derivation")),
            }],
        )
        .await;

        let started = responses.iter().find_map(|response| match response {
            Ok(proto::Response {
                kind: Some(proto::response::Kind::Started(started)),
            }) => Some(started),
            _ => None,
        });
        assert_eq!(started.expect("connector started").execution, None);
        insta::assert_debug_snapshot!(render_lines(responses), @r#"
    [
        "Started(codec=Proto, container=false, process=false, spec=derive)",
        "Derive(opened)",
    ]
    "#);
    }

    fn later_sqlite_open(execution: Option<flow::ConnectorExecution>) -> proto::Request {
        let mut kind = derive_open("acmeCo/derivation");
        let proto::request::Kind::Derive(derive::Request {
            kind: Some(derive::request::Kind::Open(open)),
            ..
        }) = &mut kind
        else {
            unreachable!("derive_open is a derive Open");
        };
        let collection = open.collection.as_mut().unwrap();
        collection.derivation.as_mut().unwrap().execution = execution;

        proto::Request {
            start: None,
            kind: Some(kind),
        }
    }

    /// Even a VMM-capable service cannot change a running connector's execution.
    #[tokio::test]
    async fn an_ordinary_session_refuses_a_later_spec_of_another_execution() {
        let (_service, router) = vmm_service(true);
        let ordinary = Some(flow::ConnectorExecution::default());
        let sqlite_spec = derive_request(derive::Request {
            kind: Some(derive::request::Kind::Spec(derive::request::Spec {
                connector_type: flow::collection_spec::derivation::ConnectorType::Sqlite as i32,
                config_json: r#"{"migrations":[]}"#.into(),
            })),
            ..Default::default()
        });
        let valid = vec![
            sqlite_spec,
            later_sqlite_open(None),
            later_sqlite_open(ordinary.clone()),
        ];

        let mut rows = Vec::new();
        for (label, start, later) in [
            ("start unset", None, valid.clone()),
            ("start ordinary", ordinary.clone(), valid),
            ("vmm", None, vec![later_sqlite_open(vmm_execution())]),
            (
                "egress []",
                None,
                vec![later_sqlite_open(egress_execution(false, &[]))],
            ),
            (
                "egress [api]",
                ordinary.clone(),
                vec![later_sqlite_open(egress_execution(
                    false,
                    &["api.acmeco.example"],
                ))],
            ),
        ] {
            let mut requests = vec![proto::Request {
                start: Some(start_with(start)),
                kind: Some(derive_open("acmeCo/derivation")),
            }];
            requests.extend(later);

            let responses = drive_router(
                &router,
                ops::TaskType::Derivation,
                "acmeCo/derivation",
                requests,
            )
            .await;
            rows.push(format!("{label}: {}", render_lines(responses).join(" | ")));
        }

        insta::assert_snapshot!(rows.join("\n"), @r#"
    start unset: Started(codec=Proto, container=false, process=false, spec=derive) | Derive(opened) | Derive(spec) | Derive(opened) | Derive(opened)
    start ordinary: Started(codec=Proto, container=false, process=false, spec=derive) | Derive(opened) | Derive(spec) | Derive(opened) | Derive(opened)
    vmm: Started(codec=Proto, container=false, process=false, spec=derive) | Status(InvalidArgument): Start.execution ConnectorExecution { vmm: false, egress: None } differs from the execution ConnectorExecution { vmm: true, egress: None } of the task's built spec
    egress []: Started(codec=Proto, container=false, process=false, spec=derive) | Status(InvalidArgument): Start.execution ConnectorExecution { vmm: false, egress: None } differs from the execution ConnectorExecution { vmm: false, egress: Some(Egress { hosts: [] }) } of the task's built spec
    egress [api]: Started(codec=Proto, container=false, process=false, spec=derive) | Status(InvalidArgument): Start.execution ConnectorExecution { vmm: false, egress: None } differs from the execution ConnectorExecution { vmm: false, egress: Some(Egress { hosts: ["api.acmeco.example"] }) } of the task's built spec
    "#);
    }

    fn with_previous_specs(
        mut kind: proto::request::Kind,
        execution: &Option<flow::ConnectorExecution>,
    ) -> proto::request::Kind {
        use proto::request::Kind;

        let capture = || flow::CaptureSpec {
            execution: execution.clone(),
            ..Default::default()
        };
        let materialization = || flow::MaterializationSpec {
            execution: execution.clone(),
            ..Default::default()
        };

        match &mut kind {
            Kind::Capture(capture::Request {
                kind: Some(capture::request::Kind::Validate(validate)),
                ..
            }) => validate.last_capture = Some(capture()),
            Kind::Capture(capture::Request {
                kind: Some(capture::request::Kind::Apply(apply)),
                ..
            }) => apply.last_capture = Some(capture()),
            Kind::Derive(derive::Request {
                kind: Some(derive::request::Kind::Validate(validate)),
                ..
            }) => {
                validate.last_collection = Some(flow::CollectionSpec {
                    derivation: Some(Box::new(flow::collection_spec::Derivation {
                        execution: execution.clone(),
                        ..Default::default()
                    })),
                    ..Default::default()
                })
            }
            Kind::Materialize(materialize::Request {
                kind: Some(materialize::request::Kind::Validate(validate)),
                ..
            }) => validate.last_materialization = Some(materialization()),
            Kind::Materialize(materialize::Request {
                kind: Some(materialize::request::Kind::Apply(apply)),
                ..
            }) => apply.last_materialization = Some(materialization()),
            _ => {}
        }
        kind
    }

    /// Immediate connector EOF must not hide a ready request's execution mismatch.
    async fn pump_later<P: connector::protocol::Protocol>(
        execution: flow::ConnectorExecution,
        request: proto::Request,
    ) -> (usize, anyhow::Result<()>) {
        let (connector_tx, mut connector_rx) = mpsc::channel(1);
        let (response_tx, _response_rx) = mpsc::channel(1);
        let started = connector::Started::<P> {
            started: proto::Response::default(),
            connector_tx,
            connector_rx: futures::stream::empty().boxed(),
            guard: None,
            execution,
        };

        let requests = futures::stream::iter([Ok::<_, tonic::Status>(request)]);
        let result = connector::serve::pump(requests, started, &response_tx).await;

        let mut forwarded = 0;
        while connector_rx.try_recv().is_ok() {
            forwarded += 1;
        }
        (forwarded, result)
    }

    /// Drive `pump` directly to cover VMM sessions without launching a VMM.
    #[tokio::test]
    async fn later_requests_must_match_the_session_execution() {
        const EMBEDS_SPEC: &[&str] = &[
            "capture Apply",
            "capture Open",
            "derive Open",
            "materialize Apply",
            "materialize Open",
        ];
        let config = serde_json::json!({"image": PYTHON_IMAGE, "config": {}});
        let config: bytes::Bytes = config.to_string().into();
        let hosts = &["api.acmeco.example", "*.svc.acmeco.example"];
        let previous = egress_execution(true, &["previous.acmeco.example"]);

        let sessions = [
            ("ordinary", None),
            ("vmm", vmm_execution()),
            ("vmm egress [api, *.svc]", egress_execution(true, hosts)),
        ];
        let laters = [
            ("unset", None),
            ("ordinary", Some(flow::ConnectorExecution::default())),
            ("vmm", vmm_execution()),
            ("egress []", egress_execution(false, &[])),
            ("vmm egress []", egress_execution(true, &[])),
            ("vmm egress [api, *.svc]", egress_execution(true, hosts)),
            ("vmm egress [api]", egress_execution(true, &hosts[..1])),
        ];

        let mut rows = Vec::new();
        for (session_name, session) in &sessions {
            for (later_name, later) in &laters {
                let mut refused = Vec::new();

                for (label, task_type, _task_name, kind) in
                    every_first_request(image_connector_type, &config, later.clone())
                {
                    let request = proto::Request {
                        start: None,
                        kind: Some(with_previous_specs(kind, &previous)),
                    };
                    let session = session.clone().unwrap_or_default();

                    let (forwarded, result) = match task_type {
                        ops::TaskType::Capture => {
                            pump_later::<connector::capture::Capture>(session, request).await
                        }
                        ops::TaskType::Derivation => {
                            pump_later::<connector::derive::Derive>(session, request).await
                        }
                        ops::TaskType::Materialization => {
                            pump_later::<connector::materialize::Materialize>(session, request)
                                .await
                        }
                        _ => unreachable!(),
                    };
                    match result {
                        Ok(()) => assert_eq!(forwarded, 1, "{label}"),
                        Err(err) => {
                            assert_eq!(forwarded, 0, "{label}");
                            let status = err.downcast_ref::<proto_grpc::StatusError>().unwrap();
                            assert_eq!(status.code(), tonic::Code::InvalidArgument, "{label}");
                            refused.push(label);
                        }
                    }
                }

                assert!(
                    refused.is_empty() || refused == EMBEDS_SPEC,
                    "session {session_name} later {later_name}: {refused:?}"
                );
                rows.push(format!(
                    "session {session_name:<23} later {later_name:<23} => {}",
                    if refused.is_empty() {
                        "forwarded"
                    } else {
                        "refused"
                    },
                ));
            }
        }

        insta::assert_snapshot!(rows.join("\n"), @r"
    session ordinary                later unset                   => forwarded
    session ordinary                later ordinary                => forwarded
    session ordinary                later vmm                     => refused
    session ordinary                later egress []               => refused
    session ordinary                later vmm egress []           => refused
    session ordinary                later vmm egress [api, *.svc] => refused
    session ordinary                later vmm egress [api]        => refused
    session vmm                     later unset                   => refused
    session vmm                     later ordinary                => refused
    session vmm                     later vmm                     => forwarded
    session vmm                     later egress []               => refused
    session vmm                     later vmm egress []           => refused
    session vmm                     later vmm egress [api, *.svc] => refused
    session vmm                     later vmm egress [api]        => refused
    session vmm egress [api, *.svc] later unset                   => refused
    session vmm egress [api, *.svc] later ordinary                => refused
    session vmm egress [api, *.svc] later vmm                     => refused
    session vmm egress [api, *.svc] later egress []               => refused
    session vmm egress [api, *.svc] later vmm egress []           => refused
    session vmm egress [api, *.svc] later vmm egress [api, *.svc] => forwarded
    session vmm egress [api, *.svc] later vmm egress [api]        => refused
    ");
    }
}
