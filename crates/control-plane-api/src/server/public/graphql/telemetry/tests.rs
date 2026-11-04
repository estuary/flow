use super::*;
use crate::server::telemetry::tests::{attribute, capture};

struct Query;
struct Tenant;

#[async_graphql::Object]
impl Query {
    async fn tenant(&self, name: Option<String>) -> Tenant {
        let _ = name;
        Tenant
    }

    async fn denied(&self) -> async_graphql::Result<bool> {
        Err(async_graphql::Error::new_with_source(
            super::super::authorization::AuthorizationDenied {
                capability: models::authz::Capability::EditBilling,
                message: "not allowed".to_owned(),
            },
        ))
    }

    async fn retry(&self) -> async_graphql::Result<bool> {
        Err(crate::ApiError::AuthZRetry(crate::AuthZRetry {
            original_uri: "/api/graphql".parse().unwrap(),
            started: tokens::DateTime::UNIX_EPOCH,
            failed: tokens::DateTime::UNIX_EPOCH,
            retry_after: tokens::DateTime::UNIX_EPOCH,
            status: tonic::Status::permission_denied("stale authorization"),
        })
        .into())
    }

    async fn failed(&self) -> async_graphql::Result<bool> {
        Err(async_graphql::Error::new("resolver failed"))
    }
}

#[async_graphql::Object]
impl Tenant {
    async fn name(&self) -> &str {
        "acmeCo"
    }
}

fn schema()
-> async_graphql::Schema<Query, async_graphql::EmptyMutation, async_graphql::EmptySubscription> {
    async_graphql::Schema::build(
        Query,
        async_graphql::EmptyMutation,
        async_graphql::EmptySubscription,
    )
    .extension(Tracing)
    .extension(super::super::authorization::AuthorizationErrors)
    .finish()
}

// Models the HTTP entry span required by `export_filter`.
fn entry_span() -> tracing::Span {
    tracing::info_span!("http.server", otel.kind = "server")
}

#[tokio::test]
async fn root_fields_and_operation_names() {
    let (_provider, exporter) = capture();
    let schema = schema();
    for request in [
        async_graphql::Request::new(
            r#"query TenantName($name: String) { first: tenant(name: $name) { name } second: tenant(name: "acmeCo/") { name } __typename }"#,
        )
        .variables(async_graphql::Variables::from_json(
            serde_json::json!({"name": "acmeCo/"}),
        )),
        async_graphql::Request::new("{ tenant { name } }"),
    ] {
        let response = schema.execute(request).instrument(entry_span()).await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
    }

    let spans = exporter.get_finished_spans().unwrap();
    let mut summary: Vec<_> = spans
        .iter()
        .map(|span| {
            let parent = spans
                .iter()
                .find(|parent| parent.span_context.span_id() == span.parent_span_id)
                .map(|parent| parent.name.to_string());
            (
                span.name.to_string(),
                parent,
                attribute(span, "graphql.operation.name"),
            )
        })
        .collect();
    summary.sort();
    insta::assert_debug_snapshot!(summary, @r#"
    [
        (
            "Query.tenant",
            Some(
                "graphql.request",
            ),
            None,
        ),
        (
            "Query.tenant",
            Some(
                "graphql.request",
            ),
            None,
        ),
        (
            "Query.tenant",
            Some(
                "graphql.request",
            ),
            None,
        ),
        (
            "graphql.request",
            Some(
                "http.server",
            ),
            None,
        ),
        (
            "graphql.request",
            Some(
                "http.server",
            ),
            Some(
                "TenantName",
            ),
        ),
        (
            "http.server",
            None,
            None,
        ),
        (
            "http.server",
            None,
            None,
        ),
    ]
    "#);
}

#[tokio::test]
async fn errors_are_recorded_on_the_request_span() {
    let (_provider, exporter) = capture();
    let schema = schema();
    let denied = schema.execute("{ denied }").instrument(entry_span()).await;
    assert!(
        denied.errors[0]
            .source::<super::super::authorization::AuthorizationDenied>()
            .is_some()
    );
    assert_eq!(
        denied.errors[0].extensions.as_ref().unwrap().get("code"),
        Some(&async_graphql::Value::from("FORBIDDEN"))
    );
    let retry = schema.execute("{ retry }").instrument(entry_span()).await;
    assert!(matches!(
        retry.errors[0].source::<crate::ApiError>(),
        Some(crate::ApiError::AuthZRetry(_))
    ));
    for request in [
        "{ tenant { name } failed }",
        "{ unknownField }",
        "{ tenant { name } }",
    ] {
        schema.execute(request).instrument(entry_span()).await;
    }

    let spans = exporter.get_finished_spans().unwrap();
    let requests: Vec<_> = spans
        .iter()
        .filter(|span| span.name == "graphql.request")
        .map(|span| {
            let events: Vec<_> = span
                .events
                .iter()
                .map(|event| {
                    let attributes: Vec<_> = event
                        .attributes
                        .iter()
                        .filter(|kv| {
                            kv.key.as_str().starts_with("exception.")
                                || kv.key.as_str().starts_with("graphql.")
                        })
                        .map(|kv| format!("{}={}", kv.key, kv.value))
                        .collect();
                    (event.name.to_string(), attributes)
                })
                .collect();
            (
                matches!(span.status, opentelemetry::trace::Status::Error { .. }),
                events,
            )
        })
        .collect();
    insta::assert_debug_snapshot!(requests, @r#"
    [
        (
            true,
            [
                (
                    "exception",
                    [
                        "exception.message=not allowed. Requires capabilities: billing or admin",
                        "graphql.error.code=FORBIDDEN",
                        "graphql.error.path=[\"denied\"]",
                    ],
                ),
            ],
        ),
        (
            true,
            [
                (
                    "exception",
                    [
                        "exception.message=PermissionDenied: stale authorization",
                        "graphql.error.path=[\"retry\"]",
                    ],
                ),
            ],
        ),
        (
            true,
            [
                (
                    "exception",
                    [
                        "exception.message=resolver failed",
                        "graphql.error.path=[\"failed\"]",
                    ],
                ),
            ],
        ),
        (
            true,
            [
                (
                    "exception",
                    [
                        "exception.message=Unknown field \"unknownField\" on type \"Query\".",
                        "graphql.error.path=[]",
                    ],
                ),
            ],
        ),
        (
            false,
            [],
        ),
    ]
    "#);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_loader_batches_keep_their_request_parent() {
    let (_provider, exporter) = capture();
    struct Loader;
    impl async_graphql::dataloader::Loader<usize> for Loader {
        type Value = usize;
        type Error = std::convert::Infallible;
        async fn load(
            &self,
            keys: &[usize],
        ) -> Result<std::collections::HashMap<usize, usize>, Self::Error> {
            async {
                tokio::task::yield_now().await;
                Ok(keys.iter().map(|key| (*key, *key)).collect())
            }
            .instrument(tracing::info_span!("batch"))
            .await
        }
    }
    let mut handles = Vec::new();
    for _ in 0..2 {
        let parent = tracing::info_span!(parent: None, "request", otel.kind = "server");
        let loader = parent.in_scope(|| super::super::request_data_loader(Loader));
        handles.push(tokio::spawn(
            async move {
                let result = tokio::join!(
                    loader
                        .load_one(1)
                        .instrument(tracing::info_span!("first.resolver")),
                    loader
                        .load_one(2)
                        .instrument(tracing::info_span!("second.resolver"))
                );
                assert_eq!(result, (Ok(Some(1)), Ok(Some(2))));
            }
            .instrument(parent),
        ));
    }
    for handle in handles {
        handle.await.unwrap();
    }
    let spans = exporter.get_finished_spans().unwrap();
    let batches: Vec<_> = spans.iter().filter(|span| span.name == "batch").collect();
    assert_eq!(batches.len(), 2);
    assert_ne!(
        batches[0].span_context.trace_id(),
        batches[1].span_context.trace_id()
    );
    for batch in batches {
        let parent = spans
            .iter()
            .find(|span| span.span_context.span_id() == batch.parent_span_id)
            .unwrap();
        assert_eq!(parent.name, "request");
        assert_eq!(
            parent.span_context.trace_id(),
            batch.span_context.trace_id()
        );
    }
}
