use opentelemetry::propagation::TextMapPropagator as _;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

/// Selects the spans and events agent-api exports: entry-point spans, which declare `otel.kind`, and everything at INFO or above beneath one.
/// Work outside a request, such as controllers and automations, has no entry-point span above it and is not exported.
pub fn export_filter<S>() -> impl tracing_subscriber::layer::Filter<S>
where
    S: tracing::Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    tracing_subscriber::filter::dynamic_filter_fn(
        |metadata, cx: &tracing_subscriber::layer::Context<'_, S>| {
            // This filter's context only sees spans the filter enabled.
            *metadata.level() <= tracing::Level::INFO
                && (metadata.fields().field("otel.kind").is_some() || cx.lookup_current().is_some())
        },
    )
    .with_max_level_hint(tracing_subscriber::filter::LevelFilter::INFO)
}

pub(super) fn http_span(request: &axum::http::Request<axum::body::Body>) -> tracing::Span {
    let route = request
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .map_or("unknown", |path| path.as_str());
    let method = request.method().as_str();
    let user_agent = request
        .headers()
        .get(axum::http::header::USER_AGENT)
        .filter(|value| value.len() <= 512)
        .and_then(|value| value.to_str().ok());
    let span = tracing::info_span!(
        "http.server",
        otel.name = %format_args!("{method} {route}"),
        otel.kind = "server",
        http.request.method = method,
        http.route = route,
        http.response.status_code = tracing::field::Empty,
        user_agent.original = user_agent,
        enduser.id = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
    );
    // Cloud Run replaces a client's `traceparent` with Google's own context, so Estuary clients send theirs in this header instead.
    if let Some(value) = request
        .headers()
        .get("x-estuary-traceparent")
        .and_then(|value| value.to_str().ok())
    {
        let carrier =
            std::collections::HashMap::from([("traceparent".to_owned(), value.to_owned())]);
        let parent = opentelemetry_sdk::propagation::TraceContextPropagator::new()
            .extract_with_context(&opentelemetry::Context::new(), &carrier);
        // Trace export is optional, so this may have no OpenTelemetry layer.
        let _ = span.set_parent(parent);
    }
    span
}

pub(super) fn http_response(
    response: &axum::http::Response<axum::body::Body>,
    _latency: std::time::Duration,
    span: &tracing::Span,
) {
    span.record("http.response.status_code", response.status().as_u16());
    if response.status().is_server_error() {
        span.record("otel.status_code", "ERROR");
    }
}

#[cfg(test)]
pub(crate) mod tests;
