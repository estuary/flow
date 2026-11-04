use anyhow::Context as _;
use opentelemetry::trace::TracerProvider as _;
use tracing_subscriber::{Layer as _, layer::SubscriberExt as _};

pub fn init(
    format: super::LogFormat,
) -> anyhow::Result<Option<opentelemetry_sdk::trace::SdkTracerProvider>> {
    // The SDK defaults to a localhost endpoint, so export only when one is configured.
    let provider = if std::env::var_os("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").is_some()
        || std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT").is_some()
    {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .build()
            .context("failed to build OTLP trace exporter")?;
        let mut resource = opentelemetry_sdk::Resource::builder()
            .with_service_name("agent-api")
            .with_attributes([
                opentelemetry::KeyValue::new("service.version", env!("FLOW_VERSION")),
                opentelemetry::KeyValue::new(
                    "service.instance.id",
                    uuid::Uuid::new_v4().to_string(),
                ),
            ]);
        if let Ok(revision) = std::env::var("K_REVISION") {
            resource =
                resource.with_attribute(opentelemetry::KeyValue::new("faas.version", revision));
        }
        Some(
            opentelemetry_sdk::trace::SdkTracerProvider::builder()
                .with_batch_exporter(exporter)
                .with_resource(resource.build())
                .build(),
        )
    } else {
        None
    };

    let traces = provider.as_ref().map(|provider| {
        tracing_opentelemetry::layer()
            .with_tracer(provider.tracer("agent-api"))
            .with_filter(control_plane_api::server::export_filter())
    });
    let subscriber = tracing_subscriber::registry().with(traces);
    // Use reasonable defaults for printing structured logs to stderr.
    let logs = tracing_subscriber::fmt::layer();
    let filter = tracing_subscriber::EnvFilter::from_default_env();
    if format == super::LogFormat::Json {
        tracing::subscriber::set_global_default(subscriber.with(logs.json().with_filter(filter)))
    } else {
        let no_color = matches!(std::env::var("NO_COLOR"), Ok(v) if v == "1");
        tracing::subscriber::set_global_default(
            subscriber.with(logs.with_ansi(!no_color).with_filter(filter)),
        )
    }
    .expect("setting tracing default failed");

    Ok(provider)
}

/// Configures client trace forwarding from `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`
/// and `OTEL_EXPORTER_OTLP_TRACES_HEADERS`.
pub fn otlp_upstream() -> anyhow::Result<Option<control_plane_api::server::public::otlp::Upstream>>
{
    let Ok(endpoint) = std::env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") else {
        return Ok(None);
    };
    let endpoint = endpoint
        .parse()
        .context("invalid OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")?;
    let headers = std::env::var("OTEL_EXPORTER_OTLP_TRACES_HEADERS").unwrap_or_default();
    let upstream = control_plane_api::server::public::otlp::Upstream::new(endpoint, &headers)
        .context("failed to configure OTLP trace forwarding")?;
    Ok(Some(upstream))
}
