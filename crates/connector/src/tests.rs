//! Cross-cutting tests of the `connector.Connector` contract: first-request
//! identity, authorization, and end-to-end streams driven both over a loopback
//! gRPC server and in-process.
//!
//! Every connector these tests start is either in-process (derive-sqlite) or a
//! `/bin/sh` subprocess, so nothing here needs Docker.

use crate::proto;
use futures::StreamExt;
use proto_flow::{capture, derive, flow, materialize};
use proto_grpc::connector::Router as _;
use tokio::sync::mpsc;

/// A local `Service` and its router which resolve no secrets, for the many
/// tests which never reference one.
fn local_service() -> (crate::Service, crate::ServiceRouter) {
    crate::Service::new_local(
        String::new(),
        service_kit::Registry::new(),
        std::sync::Arc::new(flow_client_next::secret_resolver::NoOp),
    )
}

// ---------------------------------------------------------------- identity --

fn capture_spec() -> proto::request::Kind {
    proto::request::Kind::Capture(capture::Request {
        kind: Some(capture::request::Kind::Spec(
            capture::request::Spec::default(),
        )),
        ..Default::default()
    })
}

fn derive_open(collection: &str) -> proto::request::Kind {
    proto::request::Kind::Derive(derive::Request {
        kind: Some(derive::request::Kind::Open(Box::new(
            derive::request::Open {
                collection: Some(flow::CollectionSpec {
                    name: collection.to_string(),
                    derivation: Some(Box::new(flow::collection_spec::Derivation {
                        connector_type: flow::collection_spec::derivation::ConnectorType::Sqlite
                            as i32,
                        config_json: r#"{"migrations":[]}"#.into(),
                        ..Default::default()
                    })),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ))),
        ..Default::default()
    })
}

/// Every row of the identity table maps to its `(task type, task name)`.
#[test]
fn task_identity_maps_every_request_shape() {
    let capture_of = |request| {
        proto_grpc::connector::task_identity(&proto::request::Kind::Capture(request))
            .map(|(t, n)| (t, n.to_string()))
    };
    let derive_of = |request| {
        proto_grpc::connector::task_identity(&proto::request::Kind::Derive(request))
            .map(|(t, n)| (t, n.to_string()))
    };
    let materialize_of = |request| {
        proto_grpc::connector::task_identity(&proto::request::Kind::Materialize(request))
            .map(|(t, n)| (t, n.to_string()))
    };

    let capture_spec = flow::CaptureSpec {
        name: "acmeCo/capture".to_string(),
        ..Default::default()
    };
    let materialization_spec = flow::MaterializationSpec {
        name: "acmeCo/materialization".to_string(),
        ..Default::default()
    };
    let collection_spec = flow::CollectionSpec {
        name: "acmeCo/derivation".to_string(),
        ..Default::default()
    };

    let rows = [
        capture_of(capture::Request {
            kind: Some(capture::request::Kind::Spec(Default::default())),
            ..Default::default()
        }),
        capture_of(capture::Request {
            kind: Some(capture::request::Kind::Discover(Box::new(
                capture::request::Discover {
                    name: "acmeCo/capture".to_string(),
                    ..Default::default()
                },
            ))),
            ..Default::default()
        }),
        capture_of(capture::Request {
            kind: Some(capture::request::Kind::Validate(Box::new(
                capture::request::Validate {
                    name: "acmeCo/capture".to_string(),
                    ..Default::default()
                },
            ))),
            ..Default::default()
        }),
        capture_of(capture::Request {
            kind: Some(capture::request::Kind::Apply(Box::new(
                capture::request::Apply {
                    capture: Some(capture_spec.clone()),
                    ..Default::default()
                },
            ))),
            ..Default::default()
        }),
        capture_of(capture::Request {
            kind: Some(capture::request::Kind::Open(Box::new(
                capture::request::Open {
                    capture: Some(capture_spec),
                    ..Default::default()
                },
            ))),
            ..Default::default()
        }),
        derive_of(derive::Request {
            kind: Some(derive::request::Kind::Spec(Default::default())),
            ..Default::default()
        }),
        derive_of(derive::Request {
            kind: Some(derive::request::Kind::Validate(Box::new(
                derive::request::Validate {
                    collection: Some(collection_spec.clone()),
                    ..Default::default()
                },
            ))),
            ..Default::default()
        }),
        derive_of(derive::Request {
            kind: Some(derive::request::Kind::Open(Box::new(
                derive::request::Open {
                    collection: Some(collection_spec),
                    ..Default::default()
                },
            ))),
            ..Default::default()
        }),
        materialize_of(materialize::Request {
            kind: Some(materialize::request::Kind::Spec(Default::default())),
            ..Default::default()
        }),
        materialize_of(materialize::Request {
            kind: Some(materialize::request::Kind::Validate(Box::new(
                materialize::request::Validate {
                    name: "acmeCo/materialization".to_string(),
                    ..Default::default()
                },
            ))),
            ..Default::default()
        }),
        materialize_of(materialize::Request {
            kind: Some(materialize::request::Kind::Apply(Box::new(
                materialize::request::Apply {
                    materialization: Some(materialization_spec.clone()),
                    ..Default::default()
                },
            ))),
            ..Default::default()
        }),
        materialize_of(materialize::Request {
            kind: Some(materialize::request::Kind::Open(Box::new(
                materialize::request::Open {
                    materialization: Some(materialization_spec),
                    ..Default::default()
                },
            ))),
            ..Default::default()
        }),
    ];
    let rows: Vec<(String, String)> = rows
        .into_iter()
        .map(|row| {
            let (task_type, name) = row.unwrap();
            (task_type.as_str_name().to_string(), name)
        })
        .collect();

    insta::assert_debug_snapshot!(rows, @r#"
    [
        (
            "capture",
            "<spec>",
        ),
        (
            "capture",
            "acmeCo/capture",
        ),
        (
            "capture",
            "acmeCo/capture",
        ),
        (
            "capture",
            "acmeCo/capture",
        ),
        (
            "capture",
            "acmeCo/capture",
        ),
        (
            "derivation",
            "<spec>",
        ),
        (
            "derivation",
            "acmeCo/derivation",
        ),
        (
            "derivation",
            "acmeCo/derivation",
        ),
        (
            "materialization",
            "<spec>",
        ),
        (
            "materialization",
            "acmeCo/materialization",
        ),
        (
            "materialization",
            "acmeCo/materialization",
        ),
        (
            "materialization",
            "acmeCo/materialization",
        ),
    ]
    "#);
}

/// A request naming no operation, or one whose named operation is missing its
/// spec, has no identity to authorize and is `InvalidArgument`.
#[test]
fn task_identity_rejects_shapeless_requests() {
    let err = proto_grpc::connector::task_identity(&proto::request::Kind::Capture(
        capture::Request::default(),
    ))
    .unwrap_err();
    assert_eq!(
        err.downcast_ref::<proto_grpc::StatusError>()
            .unwrap()
            .code(),
        tonic::Code::InvalidArgument,
    );

    let err = proto_grpc::connector::task_identity(&proto::request::Kind::Materialize(
        materialize::Request {
            kind: Some(materialize::request::Kind::Apply(Default::default())),
            ..Default::default()
        },
    ))
    .unwrap_err();
    let status = err.downcast_ref::<proto_grpc::StatusError>().unwrap();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        status.message(),
        "`apply` missing required `materialization`"
    );
}

// --------------------------------------------------------------------- authz --

struct StubSecretResolver;

#[tonic::async_trait]
impl flow_client_next::SecretResolver for StubSecretResolver {
    async fn decrypt(
        &self,
        task_type: ops::TaskType,
        task_name: &str,
        name: models::Secret,
    ) -> anyhow::Result<models::authorizations::SecretDecryption> {
        match task_type {
            ops::TaskType::Capture => assert_eq!(task_name, "acmeCo/capture"),
            ops::TaskType::Materialization => assert_eq!(task_name, "acmeCo/materialization"),
            _ => panic!("unexpected task type"),
        }
        assert_eq!(name.as_str(), "acmeCo/password");

        Ok(models::authorizations::SecretDecryption {
            value: Some(models::RawValue::from_str(r#""resolved""#).unwrap()),
            secret_id: Some(models::Id::new([1, 2, 3, 4, 5, 6, 7, 8])),
            retry_millis: 0,
        })
    }
}

struct PanickingSecretResolver;

#[tonic::async_trait]
impl flow_client_next::SecretResolver for PanickingSecretResolver {
    async fn decrypt(
        &self,
        _task_type: ops::TaskType,
        _task_name: &str,
        _name: models::Secret,
    ) -> anyhow::Result<models::authorizations::SecretDecryption> {
        panic!("mixed configurations must be rejected before resolution")
    }
}

fn authorize(
    service: &crate::Service,
    metadata: &proto_grpc::Metadata,
    task_type: ops::TaskType,
    task_name: &str,
) -> tonic::Result<()> {
    let verified = service
        .authenticator
        .authenticate(&metadata.0, proto_flow::capability::PROXY_CONNECTOR)?;

    proto_grpc::Authorizer::from_verified(verified)
        .authorize(proto_grpc::connector::task_label_set(task_type, task_name))?;
    Ok(())
}

#[test]
fn minted_bearers_authorize_their_task_and_a_spec() {
    let (service, router) = local_service();
    let metadata = proto_grpc::connector::connector_bearer(
        router.signer(),
        ops::TaskType::Capture,
        "acmeCo/foo",
    )
    .unwrap();

    // The named task, and a Spec of the same type, are both in scope.
    authorize(&service, &metadata, ops::TaskType::Capture, "acmeCo/foo").unwrap();
    authorize(
        &service,
        &metadata,
        ops::TaskType::Capture,
        crate::SPEC_TASK_NAME,
    )
    .unwrap();

    // A sibling task sharing a name prefix is denied, as is another tenant.
    for name in ["acmeCo/foobar", "acmeCo/fo", "otherCo/foo"] {
        assert_eq!(
            authorize(&service, &metadata, ops::TaskType::Capture, name)
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied,
            "{name}",
        );
    }

    // The right name of the wrong task type is denied.
    for task_type in [ops::TaskType::Derivation, ops::TaskType::Materialization] {
        assert_eq!(
            authorize(&service, &metadata, task_type, "acmeCo/foo")
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied,
        );
    }
}

#[test]
fn authentication_requires_the_capability_and_the_issuer() {
    use proto_flow::capability::{PROXY_CONNECTOR, SHUFFLE};

    // A Service over a known key, so each case below varies exactly one of
    // key, issuer, and capability against an otherwise-valid bearer.
    let (key, issuer) = (b"a known key".as_slice(), "data-plane.example");
    let service = crate::Service::new(
        crate::Plane::Local,
        String::new(),
        proto_grpc::Authenticator::new(
            issuer.to_string(),
            vec![tokens::jwt::DecodingKey::from_secret(key)],
        ),
        None,
        service_kit::Registry::new(),
        std::sync::Arc::new(flow_client_next::secret_resolver::NoOp),
    );
    let signer = |issuer: &str, key: &[u8]| {
        proto_grpc::Signer::new(
            issuer.to_string(),
            tokens::jwt::EncodingKey::from_secret(key),
        )
    };
    let bearer = |signer: &proto_grpc::Signer, capability: u32| {
        let selector = proto_gazette::broker::LabelSelector {
            include: Some(proto_grpc::connector::task_label_set(
                ops::TaskType::Capture,
                "acmeCo/foo",
            )),
            exclude: None,
        };
        let token = signer
            .sign(
                capability,
                "acmeCo/foo".to_string(),
                selector,
                tokens::TimeDelta::minutes(1),
            )
            .unwrap();
        proto_grpc::Metadata::new()
            .with_bearer_token(&token)
            .unwrap()
    };
    let outcome = |metadata: &proto_grpc::Metadata| {
        authorize(&service, metadata, ops::TaskType::Capture, "acmeCo/foo")
            .map_err(|status| (status.code(), status.message().to_string()))
    };

    // Control: the right key, issuer, and capability.
    outcome(&bearer(&signer(issuer, key), PROXY_CONNECTOR)).unwrap();

    // No bearer at all.
    assert_eq!(
        outcome(&proto_grpc::Metadata::new()).unwrap_err().0,
        tonic::Code::Unauthenticated,
    );

    // The right key and issuer, but a capability other than PROXY_CONNECTOR:
    // the bearer is authentic, and denied.
    assert_eq!(
        outcome(&bearer(&signer(issuer, key), SHUFFLE))
            .unwrap_err()
            .0,
        tonic::Code::PermissionDenied,
    );

    // The right key and capability, but another issuer.
    assert_eq!(
        outcome(&bearer(&signer("other.example", key), PROXY_CONNECTOR)).unwrap_err(),
        (
            tonic::Code::Unauthenticated,
            "unknown token issuer \"other.example\"".to_string(),
        ),
    );

    // The right issuer and capability, but signed with another key.
    assert_eq!(
        outcome(&bearer(&signer(issuer, b"another key"), PROXY_CONNECTOR))
            .unwrap_err()
            .0,
        tonic::Code::Unauthenticated,
    );
}

/// An `EndpointRouter` routes to its endpoint and mints a bearer which the
/// `Service` behind that endpoint authenticates and authorizes — the same
/// contract of a service router over a dialed endpoint.
#[test]
fn an_endpoint_router_mints_for_the_service_it_names() {
    let key = b"a reactor's data-plane key".as_slice();
    let service = crate::Service::new(
        crate::Plane::Local,
        String::new(),
        proto_grpc::Authenticator::new(
            crate::LOCAL_ISSUER.to_string(),
            vec![tokens::jwt::DecodingKey::from_secret(key)],
        ),
        None,
        service_kit::Registry::new(),
        std::sync::Arc::new(flow_client_next::secret_resolver::NoOp),
    );
    let router = proto_grpc::connector::EndpointRouter::new(
        "unix:/run/reactor.sock".to_string(),
        proto_grpc::Signer::new(
            crate::LOCAL_ISSUER.to_string(),
            tokens::jwt::EncodingKey::from_secret(key),
        ),
    );

    assert_eq!(router.endpoint(), "unix:/run/reactor.sock");
    let metadata = proto_grpc::connector::connector_bearer(
        &proto_grpc::Signer::new(
            crate::LOCAL_ISSUER.to_string(),
            tokens::jwt::EncodingKey::from_secret(key),
        ),
        ops::TaskType::Derivation,
        "acmeCo/derivation",
    )
    .unwrap();
    authorize(
        &service,
        &metadata,
        ops::TaskType::Derivation,
        "acmeCo/derivation",
    )
    .unwrap();

    // A signer over another key mints a bearer the same service rejects.
    let _other = proto_grpc::connector::EndpointRouter::new(
        "unix:/run/reactor.sock".to_string(),
        proto_grpc::Signer::new(
            crate::LOCAL_ISSUER.to_string(),
            tokens::jwt::EncodingKey::from_secret(b"another key"),
        ),
    );
    let metadata = proto_grpc::connector::connector_bearer(
        &proto_grpc::Signer::new(
            crate::LOCAL_ISSUER.to_string(),
            tokens::jwt::EncodingKey::from_secret(b"another key"),
        ),
        ops::TaskType::Derivation,
        "acmeCo/derivation",
    )
    .unwrap();

    assert_eq!(
        authorize(
            &service,
            &metadata,
            ops::TaskType::Derivation,
            "acmeCo/derivation",
        )
        .unwrap_err()
        .code(),
        tonic::Code::Unauthenticated,
    );
}

// -------------------------------------------------------------- end-to-end --

/// Drive a Connector RPC over a loopback gRPC server, returning its responses.
async fn drive_loopback(
    service: crate::Service,
    metadata: proto_grpc::Metadata,
    requests: Vec<proto::Request>,
) -> Vec<tonic::Result<proto::Response>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());

    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(service.into_tonic_service())
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );

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

async fn drive_endpoint_router(
    service: crate::Service,
    signer: proto_grpc::Signer,
    task_name: &str,
    requests: Vec<proto::Request>,
) -> Vec<tonic::Result<proto::Response>> {
    _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(service.into_tonic_service())
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let router = proto_grpc::connector::EndpointRouter::new(endpoint, signer);
    let task_type = proto_grpc::connector::task_type(
        requests[0].kind.as_ref().expect("first request has a kind"),
    );
    let request_rx = request_receiver(requests);
    let responses = collect_receiver(router.open(task_type, task_name, request_rx)).await;
    server.abort();
    responses
}

/// Drive a Connector RPC in-process, returning its responses.
async fn drive_router(
    router: &crate::ServiceRouter,
    task_type: ops::TaskType,
    task_name: &str,
    requests: Vec<proto::Request>,
) -> Vec<tonic::Result<proto::Response>> {
    use proto_grpc::connector::Router;

    collect_receiver(router.open(task_type, task_name, request_receiver(requests))).await
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

/// Render responses as compact strings a snapshot can assert on, so that the
/// *order* of `Started`, logs, protocol responses, and a terminal Status is
/// what's under test.
fn render(responses: Vec<tonic::Result<proto::Response>>) -> Vec<String> {
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
                proto::response::Kind::Log(log) => {
                    format!("Log({}): {}", log.level().as_str_name(), log.message)
                }
                proto::response::Kind::Capture(r) => format!("Capture({})", variant(&r)),
                proto::response::Kind::Derive(r) => format!("Derive({})", variant(&r)),
                proto::response::Kind::Materialize(r) => {
                    format!("Materialize({})", variant(&r))
                }
            },
        })
        .collect()
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

fn start(sqlite_vfs_uri: &str) -> proto::request::Start {
    proto::request::Start {
        log_level: ops::LogLevel::Debug as i32,
        sqlite_vfs_uri: sqlite_vfs_uri.to_string(),
    }
}

/// Secret resolution happens after Spec, but before the connector receives its
/// initial Open. The connector sees the resolved config while sealedConfig
/// remains the as-published, non-secret baseline, and the lifecycle id is
/// observable in the preceding INFO log.
#[tokio::test]
async fn resolves_secrets_and_preserves_the_published_open_config() {
    let (_service, router) = crate::Service::new_local(
        String::new(),
        service_kit::Registry::new(),
        std::sync::Arc::new(StubSecretResolver),
    );

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
    let endpoint = serde_json::json!({
        "command": ["/bin/sh", "-c", script],
        "config": {"base": "published"},
    });
    let capture = flow::CaptureSpec {
        name: "acmeCo/capture".to_string(),
        connector_type: flow::capture_spec::ConnectorType::Local as i32,
        config_json: endpoint.to_string().into(),
        secrets: [("/actual".to_string(), "acmeCo/password".to_string())]
            .into_iter()
            .collect(),
        ..Default::default()
    };

    let responses = drive_router(
        &router,
        ops::TaskType::Capture,
        "acmeCo/capture",
        vec![proto::Request {
            start: Some(start("")),
            kind: Some(proto::request::Kind::Capture(capture::Request {
                kind: Some(capture::request::Kind::Open(Box::new(
                    capture::request::Open {
                        capture: Some(capture),
                        ..Default::default()
                    },
                ))),
                ..Default::default()
            })),
        }],
    )
    .await;

    let log = responses
        .iter()
        .find_map(|response| match response.as_ref().ok()?.kind.as_ref()? {
            proto::response::Kind::Log(log) if log.message == "resolved task secret" => Some(log),
            _ => None,
        })
        .expect("resolution log is present");
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
    insta::assert_debug_snapshot!(fields, @r###"
    {
        "secret": String("acmeCo/password"),
        "secretId": String("0102030405060708"),
    }
    "###);
    insta::assert_debug_snapshot!(render(responses), @r###"
    [
        "Log(info): resolved task secret",
        "Started(codec=Json, container=false, process=false, spec=capture)",
        "Capture(opened)",
    ]
    "###);
}

#[tokio::test]
async fn rejects_a_sops_key_together_with_a_secrets_stanza() {
    let (_service, router) = crate::Service::new_local(
        String::new(),
        service_kit::Registry::new(),
        std::sync::Arc::new(PanickingSecretResolver),
    );
    let endpoint = serde_json::json!({
        "command": [
            "/bin/sh",
            "-c",
            "read request; echo '{\"spec\":{\"configSchema\":true}}'; read forever",
        ],
        "config": {"sops": null},
    });
    let capture = flow::CaptureSpec {
        name: "acmeCo/capture".to_string(),
        connector_type: flow::capture_spec::ConnectorType::Local as i32,
        config_json: endpoint.to_string().into(),
        secrets: [("/actual".to_string(), "acmeCo/password".to_string())]
            .into_iter()
            .collect(),
        ..Default::default()
    };

    let responses = drive_router(
        &router,
        ops::TaskType::Capture,
        "acmeCo/capture",
        vec![proto::Request {
            start: Some(start("")),
            kind: Some(proto::request::Kind::Capture(capture::Request {
                kind: Some(capture::request::Kind::Open(Box::new(
                    capture::request::Open {
                        capture: Some(capture),
                        ..Default::default()
                    },
                ))),
                ..Default::default()
            })),
        }],
    )
    .await;

    insta::assert_debug_snapshot!(render(responses), @r###"
    [
        "Status(InvalidArgument): endpoint configuration has a top-level `sops` key and cannot also use a `secrets` stanza",
    ]
    "###);
}

// Dekaf's `{variant, config}` wrapper is split before startup, so the pipeline
// resolves the *inner* configuration and Dekaf validates its token from the
// request slot.
#[tokio::test]
async fn dekaf_resolves_inner_configuration() {
    let (_service, router) = crate::Service::new_local(
        String::new(),
        service_kit::Registry::new(),
        std::sync::Arc::new(StubSecretResolver),
    );
    let mut outcomes = Vec::new();
    for (config, use_secrets) in [
        (serde_json::json!({"token": "plaintext"}), false),
        (serde_json::json!({}), true),
        (serde_json::json!({"sops": null}), true),
    ] {
        let responses = drive_router(
            &router,
            ops::TaskType::Materialization,
            "acmeCo/materialization",
            vec![proto::Request {
                start: Some(start("")),
                kind: Some(proto::request::Kind::Materialize(materialize::Request {
                    kind: Some(materialize::request::Kind::Validate(Box::new(
                        materialize::request::Validate {
                            name: "acmeCo/materialization".to_string(),
                            connector_type: flow::materialization_spec::ConnectorType::Dekaf as i32,
                            config_json: serde_json::json!({
                                "variant": "test",
                                "config": config,
                            })
                            .to_string()
                            .into(),
                            secrets: if use_secrets {
                                [("/token".to_string(), "acmeCo/password".to_string())]
                                    .into_iter()
                                    .collect()
                            } else {
                                Default::default()
                            },
                            ..Default::default()
                        },
                    ))),
                    ..Default::default()
                })),
            }],
        )
        .await;
        outcomes.push(render(responses));
    }
    insta::assert_debug_snapshot!(outcomes, @r###"
    [
        [
            "Started(codec=Proto, container=false, process=false, spec=materialize)",
            "Materialize(validated)",
        ],
        [
            "Log(info): resolved task secret",
            "Started(codec=Proto, container=false, process=false, spec=materialize)",
            "Materialize(validated)",
        ],
        [
            "Status(InvalidArgument): endpoint configuration has a top-level `sops` key and cannot also use a `secrets` stanza",
        ],
    ]
    "###);
}

fn derive_request(request: derive::Request) -> proto::Request {
    proto::Request {
        start: None,
        kind: Some(proto::request::Kind::Derive(request)),
    }
}

/// derive-sqlite over a loopback server: Spec, then Validate, then an Open
/// which threads a recorded VFS path. `Started` leads, and the connector's
/// responses follow in order.
#[tokio::test]
async fn loopback_derive_sqlite_session() {
    let (service, router) = local_service();

    let responses = drive_endpoint_router(
        service,
        router.signer().clone(),
        "acmeCo/derivation",
        vec![
            proto::Request {
                start: Some(start("")),
                kind: Some(derive_open("acmeCo/derivation")),
            },
            derive_request(derive::Request {
                kind: Some(derive::request::Kind::Spec(derive::request::Spec {
                    connector_type: flow::collection_spec::derivation::ConnectorType::Sqlite as i32,
                    config_json: r#"{"migrations":[]}"#.into(),
                })),
                ..Default::default()
            }),
        ],
    )
    .await;

    insta::assert_debug_snapshot!(render(responses), @r#"
    [
        "Started(codec=Proto, container=false, process=false, spec=derive)",
        "Derive(opened)",
        "Derive(spec)",
    ]
    "#);
}

/// The same session driven in-process, with a locally minted bearer,
/// produces the identical stream.
#[tokio::test]
async fn in_process_derive_sqlite_session() {
    let (_service, router) = local_service();

    let dir = tempfile::tempdir().unwrap();
    let vfs_uri = dir.path().join("derive.db").to_string_lossy().into_owned();

    let responses = drive_router(
        &router,
        ops::TaskType::Derivation,
        "acmeCo/derivation",
        vec![proto::Request {
            start: Some(start(&vfs_uri)),
            kind: Some(derive_open("acmeCo/derivation")),
        }],
    )
    .await;

    insta::assert_debug_snapshot!(render(responses), @r#"
    [
        "Started(codec=Proto, container=false, process=false, spec=derive)",
        "Derive(opened)",
    ]
    "#);
}

/// A `local:` subprocess connector which writes to stderr and then exits
/// non-zero: its logs precede the terminal Status, which is the stream's last
/// word. Logs race `Started` — they're sunk as they're read, and this
/// connector writes immediately — so only their position relative to the
/// Status is asserted.
#[tokio::test]
async fn loopback_local_connector_logs_precede_its_status() {
    let (service, router) = local_service();
    let metadata = proto_grpc::connector::connector_bearer(
        router.signer(),
        ops::TaskType::Derivation,
        "acmeCo/derivation",
    )
    .unwrap();

    let config = serde_json::json!({
        "command": [
            "/bin/sh",
            "-c",
            "echo 'a first line' >&2; echo 'a second line' >&2; exit 7",
        ],
        "config": {},
    });

    let responses = drive_loopback(
        service,
        metadata,
        vec![proto::Request {
            start: Some(start("")),
            kind: Some(proto::request::Kind::Derive(derive::Request {
                kind: Some(derive::request::Kind::Spec(derive::request::Spec {
                    connector_type: flow::collection_spec::derivation::ConnectorType::Local as i32,
                    config_json: config.to_string().into(),
                })),
                ..Default::default()
            })),
        }],
    )
    .await;

    let rendered = render(responses);
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

/// `start` on a request after the first is rejected, and the rejection lands
/// after `Started` (the connector was already running).
#[tokio::test]
async fn a_second_start_is_invalid_argument() {
    let (service, router) = local_service();
    let metadata = proto_grpc::connector::connector_bearer(
        router.signer(),
        ops::TaskType::Derivation,
        "acmeCo/derivation",
    )
    .unwrap();

    let responses = drive_loopback(
        service,
        metadata,
        vec![
            proto::Request {
                start: Some(start("")),
                kind: Some(derive_open("acmeCo/derivation")),
            },
            proto::Request {
                start: Some(start("")),
                kind: Some(derive_open("acmeCo/derivation")),
            },
        ],
    )
    .await;

    let rendered = render(responses);
    assert!(
        rendered
            .last()
            .unwrap()
            .starts_with("Status(InvalidArgument): only the first Connector request"),
        "{rendered:?}",
    );
}

/// A request of another protocol, after the first, is also rejected.
#[tokio::test]
async fn a_mismatched_protocol_request_is_invalid_argument() {
    let (service, router) = local_service();
    let metadata = proto_grpc::connector::connector_bearer(
        router.signer(),
        ops::TaskType::Derivation,
        "acmeCo/derivation",
    )
    .unwrap();

    let responses = drive_loopback(
        service,
        metadata,
        vec![
            proto::Request {
                start: Some(start("")),
                kind: Some(derive_open("acmeCo/derivation")),
            },
            proto::Request {
                start: None,
                kind: Some(capture_spec()),
            },
        ],
    )
    .await;

    let rendered = render(responses);
    assert!(
        rendered
            .last()
            .unwrap()
            .starts_with("Status(InvalidArgument): every Connector request"),
        "{rendered:?}",
    );
}

/// A bearer signed with another Service's key never reaches `Started`: no
/// connector is started at all.
#[tokio::test]
async fn a_bad_bearer_never_starts_a_connector() {
    let (service, _router) = local_service();
    let (_other, other_router) = local_service();

    let responses = drive_endpoint_router(
        service,
        other_router.signer().clone(),
        "acmeCo/derivation",
        vec![proto::Request {
            start: Some(start("")),
            kind: Some(derive_open("acmeCo/derivation")),
        }],
    )
    .await;

    insta::assert_debug_snapshot!(render(responses), @r#"
    [
        "Status(Unauthenticated): failed to verify token: InvalidSignature",
    ]
    "#);
}

/// A bearer for one task cannot open another task's connector.
#[tokio::test]
async fn a_bearer_of_another_task_is_denied() {
    let (service, router) = local_service();
    let metadata = proto_grpc::connector::connector_bearer(
        router.signer(),
        ops::TaskType::Derivation,
        "acmeCo/other",
    )
    .unwrap();

    let responses = drive_loopback(
        service,
        metadata,
        vec![proto::Request {
            start: Some(start("")),
            kind: Some(derive_open("acmeCo/derivation")),
        }],
    )
    .await;

    let rendered = render(responses);
    assert!(
        rendered[0].starts_with("Status(PermissionDenied)"),
        "{rendered:?}",
    );
}

/// `sqlite_vfs_uri` is runtime-internal and belongs only to a Sqlite
/// derivation: it's rejected for every other connector.
#[tokio::test]
async fn sqlite_vfs_uri_is_rejected_for_other_connectors() {
    let (service, router) = local_service();
    let metadata = proto_grpc::connector::connector_bearer(
        router.signer(),
        ops::TaskType::Capture,
        "acmeCo/capture",
    )
    .unwrap();

    let responses = drive_loopback(
        service,
        metadata,
        vec![proto::Request {
            start: Some(start("/tmp/nope.db")),
            kind: Some(capture_spec()),
        }],
    )
    .await;

    let rendered = render(responses);
    assert!(
        rendered[0].starts_with("Status(InvalidArgument): Start.sqlite_vfs_uri"),
        "{rendered:?}",
    );

    // A derivation of any connector type but Sqlite is rejected the same way
    // before its connector is started.
    let (service, router) = local_service();
    let metadata = proto_grpc::connector::connector_bearer(
        router.signer(),
        ops::TaskType::Derivation,
        "acmeCo/derivation",
    )
    .unwrap();
    let config = serde_json::json!({"command": ["/bin/sh", "-c", "exit 0"], "config": {}});

    let responses = drive_loopback(
        service,
        metadata,
        vec![proto::Request {
            start: Some(start("/tmp/nope.db")),
            kind: Some(proto::request::Kind::Derive(derive::Request {
                kind: Some(derive::request::Kind::Spec(derive::request::Spec {
                    connector_type: flow::collection_spec::derivation::ConnectorType::Local as i32,
                    config_json: config.to_string().into(),
                })),
                ..Default::default()
            })),
        }],
    )
    .await;

    let rendered = render(responses);
    assert!(
        rendered[0].starts_with("Status(InvalidArgument): Start.sqlite_vfs_uri"),
        "{rendered:?}",
    );
}

/// A first request missing `start`, or missing a protocol request, is rejected
/// before anything is started.
#[tokio::test]
async fn a_malformed_first_request_is_invalid_argument() {
    for request in [
        proto::Request {
            start: None,
            kind: Some(derive_open("acmeCo/derivation")),
        },
        proto::Request {
            start: Some(start("")),
            kind: None,
        },
    ] {
        let (service, router) = local_service();
        let metadata = proto_grpc::connector::connector_bearer(
            router.signer(),
            ops::TaskType::Derivation,
            "acmeCo/derivation",
        )
        .unwrap();

        let rendered = render(drive_loopback(service, metadata, vec![request]).await);
        assert!(
            rendered[0].starts_with("Status(InvalidArgument): the first Connector request"),
            "{rendered:?}",
        );
    }
}

// ------------------------------------------------------------ channel lifetime --

/// Opening a channel eagerly starts its handler, even if responses are not received.
#[tokio::test]
async fn opening_a_channel_starts_its_handler() {
    let (service, router) = local_service();
    let metadata = proto_grpc::connector::connector_bearer(
        router.signer(),
        ops::TaskType::Derivation,
        "acmeCo/derivation",
    )
    .unwrap();

    let (request_tx, request_rx) = mpsc::channel(1);
    request_tx.try_send(proto::Request::default()).unwrap();
    let response_rx = service.spawn_connector(metadata, request_rx);

    tokio::time::timeout(std::time::Duration::from_secs(1), request_tx.closed())
        .await
        .expect("the spawned handler consumes the invalid request");

    drop(request_tx);
    let rendered = render(collect_receiver(response_rx).await);
    assert_eq!(rendered.len(), 1);
    assert!(
        rendered[0].contains(
            "the first Connector request must set `start` and exactly one protocol request"
        ),
        "{rendered:?}"
    );
}

/// Invalid client input wins over clean EOF from an in-process connector.
#[tokio::test]
async fn an_invalid_request_cannot_be_swallowed_as_eof() {
    let (_service, router) = local_service();
    let config = serde_json::json!({"variant": "test", "config": {}});
    let responses = drive_router(
        &router,
        ops::TaskType::Materialization,
        crate::SPEC_TASK_NAME,
        vec![
            proto::Request {
                start: Some(start("")),
                kind: Some(proto::request::Kind::Materialize(materialize::Request {
                    kind: Some(materialize::request::Kind::Spec(
                        materialize::request::Spec {
                            connector_type: flow::materialization_spec::ConnectorType::Dekaf as i32,
                            config_json: config.to_string().into(),
                        },
                    )),
                    ..Default::default()
                })),
            },
            proto::Request::default(),
        ],
    )
    .await;

    insta::assert_debug_snapshot!(render(responses), @r#"
    [
        "Started(codec=Proto, container=false, process=false, spec=materialize)",
        "Status(InvalidArgument): every Connector request must set exactly one protocol request, of the type established by the first request",
    ]
    "#);
}
