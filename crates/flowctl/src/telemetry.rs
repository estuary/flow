//! Traces each flowctl command, and exports its spans to agent-api's `/otlp/v1/traces`.
//!
//! Only flowctl's own spans are exported: the command span from `command_span`, and a client span for each GraphQL request. Log events are not exported, since during `preview` they include connector logs.

use opentelemetry::propagation::TextMapPropagator as _;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_proto::tonic::collector::trace::v1 as collector;
use opentelemetry_proto::transform::common::tonic::ResourceAttributesWithSchema;
use prost::Message as _;
use std::sync::{Arc, OnceLock};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use tracing_subscriber::Layer as _;

/// Timeout for each span export request.
const EXPORT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

/// Where spans are exported. `Cli::run` sets it once it has resolved the agent URL and the user's tokens, which happens after tracing starts.
#[derive(Clone, Default)]
pub struct Target(Arc<OnceLock<Agent>>);

struct Agent {
    rest: flow_client_next::rest::Client,
    user_tokens: Arc<dyn tokens::Watch<flow_client_next::user_auth::UserToken>>,
    runtime: tokio::runtime::Handle,
}

impl Target {
    /// Starts exporting spans, unless `OTEL_SDK_DISABLED=true`.
    pub(crate) fn set(
        &self,
        rest: flow_client_next::rest::Client,
        user_tokens: Arc<dyn tokens::Watch<flow_client_next::user_auth::UserToken>>,
    ) {
        if std::env::var("OTEL_SDK_DISABLED").is_ok_and(|value| value.eq_ignore_ascii_case("true"))
        {
            return;
        }
        let _ = self.0.set(Agent {
            rest,
            user_tokens,
            runtime: tokio::runtime::Handle::current(),
        });
    }
}

/// Returns a layer which records flowctl's spans, and the provider which `main` shuts down to export the final batch. Spans are recorded whether or not they're exported, so that GraphQL requests always carry their trace context.
pub fn layer<S>(
    target: Target,
) -> (
    impl tracing_subscriber::Layer<S>,
    opentelemetry_sdk::trace::SdkTracerProvider,
)
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    // Values follow OpenTelemetry's semantic conventions, which name macOS and x86-64 / ARM64 differently than Rust.
    let os = match std::env::consts::OS {
        "macos" => "darwin",
        os => os,
    };
    let arch = match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        arch => arch,
    };
    // An empty builder ignores `OTEL_RESOURCE_ATTRIBUTES` and `OTEL_SERVICE_NAME`, which a user may have set for their own services.
    let resource = opentelemetry_sdk::Resource::builder_empty()
        .with_service_name("flowctl")
        .with_attributes([
            opentelemetry::KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            opentelemetry::KeyValue::new("os.type", os),
            opentelemetry::KeyValue::new("host.arch", arch),
        ])
        .build();

    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(Exporter {
            target,
            resource: Default::default(),
        })
        .with_resource(resource)
        .build();
    let layer = tracing_opentelemetry::layer()
        .with_tracer(provider.tracer("flowctl"))
        .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
            metadata.is_span()
                && *metadata.level() <= tracing::Level::INFO
                && metadata.target().split("::").next() == Some("flowctl")
        }));

    (layer, provider)
}

/// Creates a span named for the command, such as `flowctl catalog list`.
/// A valid `TRACEPARENT` supplies its parent; `TRACESTATE` supplies optional
/// trace state. Otherwise it starts a new trace.
pub fn command_span(matches: &clap::ArgMatches) -> tracing::Span {
    let mut name = String::from("flowctl");
    let mut matches = matches;
    while let Some((subcommand, next)) = matches.subcommand() {
        name.push(' ');
        name.push_str(subcommand);
        matches = next;
    }
    let span = tracing::info_span!(
        "command",
        otel.name = name,
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
    );

    let carrier: std::collections::HashMap<String, String> =
        [("traceparent", "TRACEPARENT"), ("tracestate", "TRACESTATE")]
            .into_iter()
            .filter_map(|(key, var)| Some((key.to_owned(), std::env::var(var).ok()?)))
            .collect();
    let parent = opentelemetry_sdk::propagation::TraceContextPropagator::new()
        .extract_with_context(&opentelemetry::Context::new(), &carrier);
    // This fails only when the span is not recorded.
    let _ = span.set_parent(parent);
    span
}

/// Returns the W3C `traceparent` of `span`, if it's recorded.
pub(crate) fn traceparent(span: &tracing::Span) -> Option<String> {
    let mut carrier = std::collections::HashMap::new();
    opentelemetry_sdk::propagation::TraceContextPropagator::new()
        .inject_context(&span.context(), &mut carrier);
    carrier.remove("traceparent")
}

struct Exporter {
    target: Target,
    resource: ResourceAttributesWithSchema,
}

impl std::fmt::Debug for Exporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Exporter").finish_non_exhaustive()
    }
}

impl opentelemetry_sdk::trace::SpanExporter for Exporter {
    async fn export(
        &self,
        batch: Vec<opentelemetry_sdk::trace::SpanData>,
    ) -> opentelemetry_sdk::error::OTelSdkResult {
        // agent-api requires a token, so spans are dropped before `Cli::run` sets the target, and for anonymous users.
        let Some(agent) = self.target.0.get() else {
            return Ok(());
        };
        let Some(token) = (agent.user_tokens.token().result().ok())
            .and_then(|tokens| tokens.access_ref().map(str::to_owned))
        else {
            return Ok(());
        };

        let body = collector::ExportTraceServiceRequest {
            resource_spans:
                opentelemetry_proto::transform::trace::tonic::group_spans_by_resource_and_scope(
                    batch,
                    &self.resource,
                ),
        }
        .encode_to_vec();
        let request = agent
            .rest
            .http_client
            .post(agent.rest.base_url.join("/otlp/v1/traces").unwrap())
            .bearer_auth(token)
            .header(reqwest::header::CONTENT_TYPE, "application/x-protobuf")
            .timeout(EXPORT_TIMEOUT)
            .body(body);

        // The batch processor exports from its own thread, and reqwest requires the tokio runtime.
        let result = agent
            .runtime
            .spawn(async move { request.send().await })
            .await;
        // A failure is logged at DEBUG rather than returned, because the batch processor prints returned errors to the user.
        match result {
            Ok(Ok(response)) if response.status().is_success() => {}
            Ok(Ok(response)) => {
                tracing::debug!(status = %response.status(), "agent-api rejected exported spans")
            }
            Ok(Err(error)) => tracing::debug!(%error, "failed to export spans"),
            Err(error) => tracing::debug!(%error, "failed to export spans"),
        }
        Ok(())
    }

    fn set_resource(&mut self, resource: &opentelemetry_sdk::Resource) {
        self.resource = resource.into();
    }
}

#[cfg(test)]
mod test;
