use anyhow::{Context, Result};
use opentelemetry::{global, trace::TracerProvider, KeyValue};
use opentelemetry_otlp::{WithExportConfig, WithTonicConfig};
use opentelemetry_sdk::{
    trace::{RandomIdGenerator, Sampler, TracerProvider as SdkTracerProvider},
    Resource,
};
use std::time::Duration;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::Registry;

pub struct OtelConfig {
    pub endpoint: String,
    pub service_name: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub sample_rate: f64,
}

fn create_resource(service_name: &str) -> Resource {
    Resource::new(vec![
        KeyValue::new("service.name", service_name.to_string()),
        KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
    ])
}

pub fn init_tracer_provider(
    config: &OtelConfig,
) -> Result<OpenTelemetryLayer<Registry, opentelemetry_sdk::trace::Tracer>> {
    let mut builder = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_tls_config(tonic::transport::ClientTlsConfig::new().with_native_roots())
        .with_endpoint(&config.endpoint)
        .with_timeout(Duration::from_secs(10));

    // Add authentication if provided
    if let (Some(username), Some(password)) = (&config.username, &config.password) {
        let auth_header = format!(
            "Basic {}",
            base64::encode(format!("{}:{}", username, password))
        );
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert(
            "authorization",
            auth_header
                .parse()
                .context("Failed to parse authorization header")?,
        );
        builder = builder.with_metadata(metadata);
    }

    let exporter = builder
        .build()
        .context("Failed to build OTLP span exporter")?;

    let tracer_provider = SdkTracerProvider::builder()
        .with_sampler(Sampler::TraceIdRatioBased(config.sample_rate))
        .with_id_generator(RandomIdGenerator::default())
        .with_resource(create_resource(&config.service_name))
        .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
        .build();

    let tracer = tracer_provider.tracer("dekaf");
    global::set_tracer_provider(tracer_provider.clone());

    let telemetry_layer = OpenTelemetryLayer::new(tracer);

    Ok(telemetry_layer)
}
