use crate::graphql::*;
use opentelemetry_proto::tonic::collector::trace::v1 as collector;
use opentelemetry_proto::tonic::trace::v1 as trace;
use prost::Message as _;
use std::sync::{Arc, Mutex};
use tracing::Instrument as _;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/draft/delete-mutation.graphql",
    response_derives = "Debug"
)]
struct DeleteDraftMutation;

/// Requests which reach the stub agent-api.
#[derive(Default)]
struct Received {
    traceparents: Vec<Option<String>>,
    exports: Vec<(Option<String>, collector::ExportTraceServiceRequest)>,
}

#[tokio::test(flavor = "multi_thread")]
async fn exports_command_and_graphql_spans() {
    // SAFETY: nextest runs each test in its own process, and no other thread reads the environment.
    unsafe {
        std::env::set_var(
            "TRACEPARENT",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        )
    };

    let received = Arc::new(Mutex::new(Received::default()));
    let router = axum::Router::new()
        .route(
            "/api/graphql",
            axum::routing::post({
                let received = received.clone();
                move |headers: axum::http::HeaderMap| async move {
                    let traceparent = headers
                        .get("x-estuary-traceparent")
                        .map(|value| value.to_str().unwrap().to_owned());
                    received.lock().unwrap().traceparents.push(traceparent);
                    axum::Json(serde_json::json!({"errors": [{"message": "draft not found"}]}))
                }
            }),
        )
        .route(
            "/otlp/v1/traces",
            axum::routing::post({
                let received = received.clone();
                move |headers: axum::http::HeaderMap, body: bytes::Bytes| async move {
                    let authorization = headers
                        .get(axum::http::header::AUTHORIZATION)
                        .map(|value| value.to_str().unwrap().to_owned());
                    let request = collector::ExportTraceServiceRequest::decode(body).unwrap();
                    received
                        .lock()
                        .unwrap()
                        .exports
                        .push((authorization, request));
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rest = flow_client_next::rest::Client::new(
        &format!("http://{}", listener.local_addr().unwrap())
            .parse()
            .unwrap(),
        "flowctl-test",
    );
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

    let target = super::Target::default();
    let (layer, provider) = super::layer(target.clone());
    // Nextest runs each test in its own process, so a global default is safe. Unlike a scoped default, it also covers spawned tasks.
    tracing_subscriber::registry().with(layer).init();
    let user_tokens = tokens::fixed(Ok(flow_client_next::user_auth::UserToken {
        access_token: Some("test-token".to_owned()),
        refresh_token: None,
    }))
    .ready_owned()
    .await;
    target.set(rest.clone(), user_tokens);

    let matches = <crate::Cli as clap::CommandFactory>::command()
        .try_get_matches_from(["flowctl", "--profile", "acmeCo", "draft", "delete"])
        .unwrap();
    let result = async {
        // Neither spans from other crates nor log events are exported.
        tracing::info_span!(target: "runtime", "connector")
            .in_scope(|| tracing::info!("connector log"));
        post_graphql::<DeleteDraftMutation>(
            &rest,
            Some("test-token"),
            delete_draft_mutation::Variables { id: Id::zero() },
        )
        .await
    }
    .instrument(super::command_span(&matches))
    .await;
    insta::assert_snapshot!(format!("{:#}", result.unwrap_err()), @"graphql query errors: [<query>:0:0: draft not found]");

    tokio::task::spawn_blocking(move || provider.shutdown().unwrap())
        .await
        .unwrap();

    let received = received.lock().unwrap();
    let [(authorization, export)] = received.exports.as_slice() else {
        panic!("expected one export, got {}", received.exports.len());
    };
    let [resource_spans] = export.resource_spans.as_slice() else {
        panic!("expected one resource");
    };
    let hex = |bytes: &[u8]| bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
    // Values vary by build and platform, and the SDK doesn't order attributes.
    let resource: std::collections::BTreeSet<_> = resource_spans
        .resource
        .iter()
        .flat_map(|resource| &resource.attributes)
        .map(|kv| kv.key.as_str())
        .collect();
    let spans: Vec<_> = resource_spans
        .scope_spans
        .iter()
        .flat_map(|scope_spans| &scope_spans.spans)
        .collect();
    let summary: Vec<_> = spans
        .iter()
        .map(|span| {
            let parent = spans
                .iter()
                .find(|parent| parent.span_id == span.parent_span_id)
                .map_or_else(|| hex(&span.parent_span_id), |parent| parent.name.clone());
            let status = span.status.as_ref().map(|status| {
                (
                    trace::status::StatusCode::try_from(status.code)
                        .unwrap()
                        .as_str_name(),
                    status.message.as_str(),
                )
            });
            (
                span.name.as_str(),
                trace::span::SpanKind::try_from(span.kind)
                    .unwrap()
                    .as_str_name(),
                hex(&span.trace_id),
                parent,
                status,
            )
        })
        .collect();
    insta::assert_debug_snapshot!((authorization, resource, summary), @r#"
    (
        Some(
            "Bearer test-token",
        ),
        {
            "host.arch",
            "os.type",
            "service.name",
            "service.version",
        },
        [
            (
                "DeleteDraftMutation",
                "SPAN_KIND_CLIENT",
                "4bf92f3577b34da6a3ce929d0e0e4736",
                "flowctl draft delete",
                Some(
                    (
                        "STATUS_CODE_ERROR",
                        "graphql query errors: [<query>:0:0: draft not found]",
                    ),
                ),
            ),
            (
                "flowctl draft delete",
                "SPAN_KIND_INTERNAL",
                "4bf92f3577b34da6a3ce929d0e0e4736",
                "00f067aa0ba902b7",
                Some(
                    (
                        "STATUS_CODE_UNSET",
                        "",
                    ),
                ),
            ),
        ],
    )
    "#);

    // The GraphQL request carried the context of its own span.
    let graphql = spans
        .iter()
        .find(|span| span.name == "DeleteDraftMutation")
        .unwrap();
    assert_eq!(
        received.traceparents,
        vec![Some(format!(
            "00-{}-{}-01",
            hex(&graphql.trace_id),
            hex(&graphql.span_id)
        ))]
    );
}
