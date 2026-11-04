use super::{collector, common};
use opentelemetry_proto::tonic::{resource::v1 as resource, trace::v1 as trace};
use prost::Message as _;
use std::sync::{Arc, Mutex};

#[test]
fn upstream_parses_otel_headers() {
    let endpoint: url::Url = "https://otlp.example.test/otlp/v1/traces".parse().unwrap();
    // Base64 credentials contain `+`, `/`, and `=`, which are not percent-encoded.
    let upstream = super::Upstream::new(
        endpoint.clone(),
        " authorization = Basic%20dXNlcjp+/w== ,x-scope-orgid=acmeCo,",
    )
    .unwrap();
    insta::assert_debug_snapshot!(upstream.headers, @r#"
    {
        "authorization": "Basic dXNlcjp+/w==",
        "x-scope-orgid": "acmeCo",
    }
    "#);

    assert!(super::Upstream::new(endpoint, "authorization").is_err());
}

#[derive(Default)]
struct Backend {
    status: Mutex<u16>,
    received: Mutex<Vec<(Option<String>, collector::ExportTraceServiceRequest)>>,
}

#[sqlx::test(migrations = "../../supabase/migrations")]
async fn forwards_stamped_exports(pool: sqlx::PgPool) {
    let backend = Arc::new(Backend::default());
    let router = axum::Router::new().route(
        "/v1/traces",
        axum::routing::post({
            let backend = backend.clone();
            move |headers: axum::http::HeaderMap, body: axum::body::Bytes| async move {
                let authorization = headers
                    .get(axum::http::header::AUTHORIZATION)
                    .map(|value| value.to_str().unwrap().to_owned());
                let request = collector::ExportTraceServiceRequest::decode(body).unwrap();
                backend
                    .received
                    .lock()
                    .unwrap()
                    .push((authorization, request));
                axum::http::StatusCode::from_u16(*backend.status.lock().unwrap()).unwrap()
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/v1/traces", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

    let server = crate::test_server::TestServer::start_with_otlp_upstream(
        pool.clone(),
        crate::test_server::snapshot(pool, false).await,
        super::Upstream::new(endpoint, "authorization=Basic%20dGVzdA==").unwrap(),
    )
    .await;
    let token = server.make_access_token(
        "11111111-2222-3333-4444-555555555555".parse().unwrap(),
        Some("alice@example.test"),
    );

    let string = |key: &str, value: &str| common::KeyValue {
        key: key.to_owned(),
        value: Some(common::AnyValue {
            value: Some(common::any_value::Value::StringValue(value.to_owned())),
        }),
    };
    let export = collector::ExportTraceServiceRequest {
        resource_spans: vec![
            trace::ResourceSpans {
                resource: Some(resource::Resource {
                    attributes: vec![
                        string("service.name", "flowctl"),
                        string("enduser.id", "forged"),
                        string("user.email", "forged@example.test"),
                        string("user.name", "forged"),
                    ],
                    ..Default::default()
                }),
                scope_spans: vec![trace::ScopeSpans {
                    spans: vec![trace::Span {
                        name: "catalog list".to_owned(),
                        attributes: vec![
                            string("enduser.id", "forged"),
                            string("user_agent.original", "flowctl-dev"),
                        ],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            trace::ResourceSpans::default(),
        ],
    }
    .encode_to_vec();
    let gzip = |body: &[u8]| {
        use std::io::Write as _;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(body).unwrap();
        encoder.finish().unwrap()
    };

    let cases = [
        ("missing token", None, None, export.clone(), 200),
        (
            "not protobuf",
            Some(&token),
            None,
            b"not protobuf".to_vec(),
            200,
        ),
        ("gzip", Some(&token), Some("gzip"), gzip(&export), 200),
        (
            "decompressed body over limit",
            Some(&token),
            Some("gzip"),
            gzip(&vec![0; 2 * 1024 * 1024]),
            200,
        ),
        ("backend throttles", Some(&token), None, export.clone(), 429),
        (
            "backend rejects credentials",
            Some(&token),
            None,
            export,
            401,
        ),
    ];
    let client = reqwest::Client::new();
    let mut outcomes = Vec::new();
    for (name, token, encoding, body, backend_status) in cases {
        *backend.status.lock().unwrap() = backend_status;
        let mut request = client
            .post(server.base_url().join("/otlp/v1/traces").unwrap())
            .header(reqwest::header::CONTENT_TYPE, "application/x-protobuf")
            .body(body);
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        if let Some(encoding) = encoding {
            request = request.header(reqwest::header::CONTENT_ENCODING, encoding);
        }
        let status = request.send().await.unwrap().status().as_u16();
        let forwarded = backend.received.lock().unwrap().len();
        outcomes.push((name, status, forwarded));
    }
    insta::assert_debug_snapshot!(outcomes, @r#"
    [
        (
            "missing token",
            401,
            0,
        ),
        (
            "not protobuf",
            400,
            0,
        ),
        (
            "gzip",
            200,
            1,
        ),
        (
            "decompressed body over limit",
            413,
            1,
        ),
        (
            "backend throttles",
            429,
            2,
        ),
        (
            "backend rejects credentials",
            502,
            3,
        ),
    ]
    "#);

    let received = backend.received.lock().unwrap();
    let (authorization, request) = &received[0];
    let render = |attributes: &[common::KeyValue]| -> Vec<String> {
        attributes
            .iter()
            .map(
                |kv| match kv.value.as_ref().and_then(|value| value.value.as_ref()) {
                    Some(common::any_value::Value::StringValue(value)) => {
                        format!("{}={value}", kv.key)
                    }
                    other => format!("{}={other:?}", kv.key),
                },
            )
            .collect()
    };
    let resources: Vec<_> = request
        .resource_spans
        .iter()
        .map(|resource_spans| {
            let spans: Vec<_> = resource_spans
                .scope_spans
                .iter()
                .flat_map(|scope_spans| &scope_spans.spans)
                .map(|span| (span.name.as_str(), render(&span.attributes)))
                .collect();
            (
                resource_spans
                    .resource
                    .as_ref()
                    .map(|resource| render(&resource.attributes)),
                spans,
            )
        })
        .collect();
    insta::assert_debug_snapshot!((authorization, resources), @r#"
    (
        Some(
            "Basic dGVzdA==",
        ),
        [
            (
                Some(
                    [
                        "service.name=flowctl",
                    ],
                ),
                [
                    (
                        "catalog list",
                        [
                            "user_agent.original=flowctl-dev",
                            "enduser.id=11111111-2222-3333-4444-555555555555",
                        ],
                    ),
                ],
            ),
            (
                None,
                [],
            ),
        ],
    )
    "#);
}
