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
        err.downcast_ref::<tonic::Status>().unwrap().code(),
        tonic::Code::InvalidArgument,
    );

    let err = proto_grpc::connector::task_identity(&proto::request::Kind::Materialize(
        materialize::Request {
            kind: Some(materialize::request::Kind::Apply(Default::default())),
            ..Default::default()
        },
    ))
    .unwrap_err();
    let status = err.downcast_ref::<tonic::Status>().unwrap();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        status.message(),
        "`apply` missing required `materialization`"
    );
}

// --------------------------------------------------------------------- authz --

/// A locally minted bearer, and the [`Service`] which checks it.
fn local_pair() -> (crate::Service, proto_grpc::Signer) {
    let key: [u8; 32] = rand::random();
    let service = crate::Service::new(
        crate::Plane::Local,
        String::new(),
        proto_grpc::Authenticator::new(
            crate::LOCAL_ISSUER.to_string(),
            vec![tokens::jwt::DecodingKey::from_secret(&key)],
        ),
        None,
        service_kit::Registry::new(),
    );
    let signer = proto_grpc::Signer::new(
        crate::LOCAL_ISSUER.to_string(),
        tokens::jwt::EncodingKey::from_secret(&key),
    );
    (service, signer)
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
    let (service, signer) = local_pair();
    let metadata =
        proto_grpc::connector::connector_bearer(&signer, ops::TaskType::Capture, "acmeCo/foo")
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
    let (request_tx, request_rx) = tokio::sync::mpsc::channel(16);
    for request in requests {
        request_tx.send(request).await.unwrap();
    }
    drop(request_tx);
    let responses = router
        .open(task_type, task_name, request_rx)
        .collect()
        .await;
    server.abort();
    responses
}

/// Drive a Connector RPC in-process, returning its responses.
async fn drive_in_process(
    service: &crate::Service,
    metadata: proto_grpc::Metadata,
    requests: Vec<proto::Request>,
) -> Vec<tonic::Result<proto::Response>> {
    let response_rx =
        service.spawn_connector(metadata, futures::stream::iter(requests).map(Ok).boxed());

    tokio_stream::wrappers::ReceiverStream::new(response_rx)
        .collect::<Vec<_>>()
        .await
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
    let (service, signer) = local_pair();

    let dir = tempfile::tempdir().unwrap();
    let vfs_uri = dir.path().join("derive.db").to_string_lossy().into_owned();

    let responses = drive_endpoint_router(
        service,
        signer,
        "acmeCo/derivation",
        vec![
            proto::Request {
                start: Some(start(&vfs_uri)),
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
    let (service, router) = local_pair();
    let metadata = proto_grpc::connector::connector_bearer(
        &router,
        ops::TaskType::Derivation,
        "acmeCo/derivation",
    )
    .unwrap();

    let responses = drive_in_process(
        &service,
        metadata,
        vec![proto::Request {
            start: Some(start("")),
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
    let (service, router) = local_pair();
    let metadata = proto_grpc::connector::connector_bearer(
        &router,
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
    let (service, router) = local_pair();
    let metadata = proto_grpc::connector::connector_bearer(
        &router,
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
    let (service, router) = local_pair();
    let metadata = proto_grpc::connector::connector_bearer(
        &router,
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
    let (service, _signer) = local_pair();
    let (_other, other_signer) = local_pair();

    let responses = drive_endpoint_router(
        service,
        other_signer,
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
    let (service, router) = local_pair();
    let metadata =
        proto_grpc::connector::connector_bearer(&router, ops::TaskType::Derivation, "acmeCo/other")
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
    let (service, router) = local_pair();
    let metadata =
        proto_grpc::connector::connector_bearer(&router, ops::TaskType::Capture, "acmeCo/capture")
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

    // A derivation of any connector type but Sqlite is rejected the same way,
    // by `derive::start`, before its connector is spawned.
    let (service, router) = local_pair();
    let metadata = proto_grpc::connector::connector_bearer(
        &router,
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
        let (service, router) = local_pair();
        let metadata = proto_grpc::connector::connector_bearer(
            &router,
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
