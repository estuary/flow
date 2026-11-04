use opentelemetry::trace::TracerProvider as _;
use tracing_subscriber::{Layer as _, layer::SubscriberExt as _};

pub(crate) fn capture() -> (
    opentelemetry_sdk::trace::SdkTracerProvider,
    opentelemetry_sdk::trace::InMemorySpanExporter,
) {
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let subscriber = tracing_subscriber::registry().with(
        tracing_opentelemetry::layer()
            .with_tracer(provider.tracer("test"))
            .with_filter(super::export_filter()),
    );
    // Nextest runs each test in its own process, so a global default is safe. Unlike a scoped default, it also covers spawned tasks.
    tracing::subscriber::set_global_default(subscriber).unwrap();
    (provider, exporter)
}

pub(crate) fn attribute(span: &opentelemetry_sdk::trace::SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|value| value.key.as_str() == key)
        .map(|value| value.value.to_string())
}

#[test]
fn export_filter_scopes_to_requests() {
    let (_provider, exporter) = capture();
    let _background = tracing::info_span!("background").entered();
    tracing::info!("outside a request");
    {
        let _request = tracing::info_span!("request", otel.kind = "server").entered();
        let _child = tracing::info_span!("child").entered();
        tracing::debug_span!("verbose").in_scope(|| tracing::info!("inside a verbose span"));
        tracing::debug!("too verbose");
        tracing::error!("inside a request");
    }

    let spans = exporter.get_finished_spans().unwrap();
    let summary: Vec<_> = spans
        .iter()
        .map(|span| {
            let parent = spans
                .iter()
                .find(|parent| parent.span_context.span_id() == span.parent_span_id)
                .map(|parent| parent.name.to_string());
            let events: Vec<_> = span
                .events
                .iter()
                .map(|event| event.name.to_string())
                .collect();
            (span.name.to_string(), parent, events)
        })
        .collect();
    insta::assert_debug_snapshot!(summary, @r#"
    [
        (
            "child",
            Some(
                "request",
            ),
            [
                "inside a verbose span",
                "inside a request",
            ],
        ),
        (
            "request",
            None,
            [],
        ),
    ]
    "#);
}

#[test]
fn http_span_records_bounded_metadata() {
    let (_provider, exporter) = capture();
    let user_agents = [
        Some(axum::http::HeaderValue::from_static("flowctl-v0.6.15")),
        Some(axum::http::HeaderValue::from_static(
            "  Custom Agent/9 (verbatim)  ",
        )),
        None,
        Some(axum::http::HeaderValue::from_bytes(&[0xff]).unwrap()),
        Some(axum::http::HeaderValue::from_str(&"a".repeat(513)).unwrap()),
    ];
    for user_agent in &user_agents {
        let mut request = axum::http::Request::builder()
            .method("POST")
            .uri("/api/graphql")
            .body(axum::body::Body::empty())
            .unwrap();
        if let Some(value) = user_agent {
            request
                .headers_mut()
                .insert(axum::http::header::USER_AGENT, value.clone());
        }
        let span = super::http_span(&request);
        super::http_response(
            &axum::http::Response::builder()
                .status(503)
                .body(axum::body::Body::empty())
                .unwrap(),
            std::time::Duration::ZERO,
            &span,
        );
    }
    let spans = exporter.get_finished_spans().unwrap();
    let user_agents: Vec<_> = spans
        .iter()
        .map(|span| attribute(span, "user_agent.original"))
        .collect();
    insta::assert_debug_snapshot!(user_agents, @r#"
    [
        Some(
            "flowctl-v0.6.15",
        ),
        Some(
            "  Custom Agent/9 (verbatim)  ",
        ),
        None,
        None,
        None,
    ]
    "#);
    for span in &spans {
        assert_eq!(span.name, "POST unknown");
        assert_eq!(span.span_kind, opentelemetry::trace::SpanKind::Server);
        assert_eq!(
            attribute(span, "http.response.status_code").as_deref(),
            Some("503")
        );
        assert!(matches!(
            span.status,
            opentelemetry::trace::Status::Error { .. }
        ));
    }
}

#[test]
fn http_span_continues_only_estuary_trace_context() {
    let (_provider, exporter) = capture();
    let context = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    for header in ["x-estuary-traceparent", "traceparent"] {
        let request = axum::http::Request::builder()
            .header(header, context)
            .body(axum::body::Body::empty())
            .unwrap();
        drop(super::http_span(&request));
    }
    let spans = exporter.get_finished_spans().unwrap();
    let contexts: Vec<_> = spans
        .iter()
        .map(|span| {
            (
                span.span_context.trace_id().to_string(),
                span.parent_span_id.to_string(),
            )
        })
        .collect();
    assert_eq!(
        contexts[0],
        (
            "4bf92f3577b34da6a3ce929d0e0e4736".to_owned(),
            "00f067aa0ba902b7".to_owned()
        )
    );
    assert_ne!(contexts[1].0, contexts[0].0);
    assert_eq!(
        spans[1].parent_span_id,
        opentelemetry::trace::SpanId::INVALID
    );
}

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(path = "../../fixtures", scripts("data_planes"))
)]
async fn router_failures_nested_work_and_retry_keep_their_contract(pool: sqlx::PgPool) {
    let (_provider, exporter) = capture();
    let user_id = crate::tenants::provision_test_tenant(
        &pool,
        "acmeTrace",
        "trace@example.test",
        serde_json::json!({}),
    )
    .await;
    let draft_id: models::Id =
        sqlx::query_scalar("INSERT INTO drafts(user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let (publication_id, logs_token): (models::Id, uuid::Uuid) = sqlx::query_as(
        r#"INSERT INTO publications(user_id, draft_id, job_status)
           VALUES ($1, $2, '{"type":"success"}') RETURNING id, logs_token"#,
    )
    .bind(user_id)
    .bind(draft_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO internal.log_lines(token, stream, log_line) VALUES ($1, 'stdout', 'a log line')")
        .bind(logs_token).execute(&pool).await.unwrap();
    let server = crate::test_server::TestServer::start(
        pool.clone(),
        crate::test_server::snapshot(pool.clone(), false).await,
    )
    .await;
    let token = server.make_access_token(user_id, Some("trace@example.test"));
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let endpoint = server.base_url().join("api/graphql").unwrap();
    let mut statuses = Vec::new();
    let response = client
        .post(endpoint.clone())
        .bearer_auth("invalid.token.signature")
        .json(&serde_json::json!({"query": "{ __typename }"}))
        .send()
        .await
        .unwrap();
    statuses.push(response.status().as_u16());
    let _ = response.bytes().await.unwrap();
    let response = client
        .post(endpoint.clone())
        .bearer_auth(&token)
        .header("content-type", "application/json")
        .body("invalid json")
        .send()
        .await
        .unwrap();
    statuses.push(response.status().as_u16());
    let _ = response.bytes().await.unwrap();
    let denied = serde_json::json!({"query": "{ tenant(name: \"outsideTrace/\") { name } }"});
    let response = client
        .post(endpoint.clone())
        .bearer_auth(&token)
        .json(&denied)
        .send()
        .await
        .unwrap();
    statuses.push(response.status().as_u16());
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["errors"][0]["extensions"]["code"], "FORBIDDEN");
    let response = client.post(endpoint).bearer_auth(&token)
        .json(&serde_json::json!({
            "query": "query TenantBillingAndLogs($id: Id!) { tenant(name: \"acmeTrace/\") { billing { plan { __typename } paymentMethods { id } } } publication(id: $id) { logs { edges { node { line } } } } }",
            "variables": {"id": publication_id},
        })).send().await.unwrap();
    statuses.push(response.status().as_u16());
    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body.get("errors").is_none(), "{body}");
    assert_eq!(
        body["data"]["publication"]["logs"]["edges"][0]["node"]["line"],
        "a log line"
    );

    let data = crate::snapshot::try_fetch(&pool, &mut Default::default())
        .await
        .unwrap();
    let stale = crate::Snapshot::new(tokens::now() - chrono::TimeDelta::minutes(1), data);
    let revoke = stale.revoke.clone();
    let stale_server =
        crate::test_server::TestServer::start(pool, tokens::fixed(Ok(stale)).ready_owned().await)
            .await;
    let response = client
        .post(stale_server.base_url().join("api/graphql").unwrap())
        .bearer_auth(token)
        .json(&denied)
        .send()
        .await
        .unwrap();
    statuses.push(response.status().as_u16());
    assert!(response.headers().contains_key("location"));
    assert!(revoke.is_cancelled());
    let _ = response.bytes().await.unwrap();
    assert_eq!(statuses, [401, 400, 200, 200, 307]);

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let spans = exporter.get_finished_spans().unwrap();
            if spans
                .iter()
                .filter(|span| span.name == "POST /api/graphql")
                .count()
                >= 5
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    assert_eq!(
        spans
            .iter()
            .filter(|span| span.name == "POST /api/graphql")
            .map(|span| attribute(span, "http.response.status_code").unwrap())
            .collect::<Vec<_>>(),
        ["401", "400", "200", "200", "307"]
    );
    // Every request but the first presents a token which verifies.
    let caller = Some(user_id.to_string());
    assert_eq!(
        spans
            .iter()
            .filter(|span| span.name == "POST /api/graphql")
            .map(|span| attribute(span, "enduser.id"))
            .collect::<Vec<_>>(),
        [None, caller.clone(), caller.clone(), caller.clone(), caller]
    );
    let requests: Vec<_> = spans
        .iter()
        .filter(|span| span.name == "graphql.request")
        .map(|span| {
            let codes: Vec<_> = span
                .events
                .iter()
                .flat_map(|event| &event.attributes)
                .filter(|kv| kv.key.as_str() == "graphql.error.code")
                .map(|kv| kv.value.to_string())
                .collect();
            (attribute(span, "graphql.operation.name"), codes)
        })
        .collect();
    insta::assert_debug_snapshot!(requests, @r#"
    [
        (
            None,
            [
                "FORBIDDEN",
            ],
        ),
        (
            Some(
                "TenantBillingAndLogs",
            ),
            [],
        ),
        (
            None,
            [],
        ),
    ]
    "#);
    // Annotated nested resolvers sit under their root field.
    let mut parents: Vec<_> = spans
        .iter()
        .map(|span| {
            let parent = spans
                .iter()
                .find(|parent| parent.span_context.span_id() == span.parent_span_id)
                .map(|parent| parent.name.to_string());
            (span.name.to_string(), parent)
        })
        .collect();
    parents.sort();
    insta::assert_debug_snapshot!(parents, @r#"
    [
        (
            "POST /api/graphql",
            None,
        ),
        (
            "POST /api/graphql",
            None,
        ),
        (
            "POST /api/graphql",
            None,
        ),
        (
            "POST /api/graphql",
            None,
        ),
        (
            "POST /api/graphql",
            None,
        ),
        (
            "Publication.logs",
            Some(
                "QueryRoot.publication",
            ),
        ),
        (
            "QueryRoot.publication",
            Some(
                "graphql.request",
            ),
        ),
        (
            "QueryRoot.tenant",
            Some(
                "graphql.request",
            ),
        ),
        (
            "QueryRoot.tenant",
            Some(
                "graphql.request",
            ),
        ),
        (
            "QueryRoot.tenant",
            Some(
                "graphql.request",
            ),
        ),
        (
            "Tenant.billing",
            Some(
                "QueryRoot.tenant",
            ),
        ),
        (
            "TenantBilling.paymentMethods",
            Some(
                "QueryRoot.tenant",
            ),
        ),
        (
            "TenantBilling.plan",
            Some(
                "QueryRoot.tenant",
            ),
        ),
        (
            "graphql.request",
            Some(
                "POST /api/graphql",
            ),
        ),
        (
            "graphql.request",
            Some(
                "POST /api/graphql",
            ),
        ),
        (
            "graphql.request",
            Some(
                "POST /api/graphql",
            ),
        ),
    ]
    "#);
}
