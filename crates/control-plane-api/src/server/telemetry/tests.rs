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
