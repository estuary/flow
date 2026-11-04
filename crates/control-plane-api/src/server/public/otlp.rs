//! Forwards OTLP/HTTP trace exports from Estuary clients to agent-api's trace backend.
//!
//! Clients authenticate with their usual bearer token, and agent-api holds the backend's credentials.
//! Only `http/protobuf` is accepted. Each span records the verified caller as `enduser.id`.

use opentelemetry_proto::tonic::collector::trace::v1 as collector;
use opentelemetry_proto::tonic::common::v1 as common;
use prost::Message as _;
use std::sync::Arc;

/// Trace backend endpoint and authentication headers.
pub struct Upstream {
    client: reqwest::Client,
    endpoint: url::Url,
    headers: reqwest::header::HeaderMap,
}

impl Upstream {
    /// `headers` has the format of `OTEL_EXPORTER_OTLP_HEADERS`: comma-separated `key=value` pairs with percent-encoded values.
    pub fn new(endpoint: url::Url, headers: &str) -> anyhow::Result<Self> {
        use anyhow::Context as _;

        let headers = headers
            .split_terminator(',')
            .map(|entry| {
                let (name, value) = entry
                    .split_once('=')
                    .context("expected a comma-separated list of `key=value` headers")?;
                let value = percent_encoding::percent_decode_str(value.trim()).decode_utf8()?;
                Ok((name.trim().parse()?, value.parse()?))
            })
            .collect::<anyhow::Result<_>>()?;
        // Matches the default OTLP export timeout.
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()?;

        Ok(Self {
            client,
            endpoint,
            headers,
        })
    }
}

pub(crate) async fn handle_post_traces(
    axum::extract::State(app): axum::extract::State<Arc<crate::App>>,
    env: crate::Envelope,
    body: axum::body::Bytes,
) -> Result<axum::response::Response, crate::ApiError> {
    let claims = env.claims()?;
    let Some(upstream) = &app.otlp_upstream else {
        return Err(tonic::Status::unimplemented("trace export is not configured").into());
    };
    let mut request = collector::ExportTraceServiceRequest::decode(body).map_err(|err| {
        tonic::Status::invalid_argument(format!("invalid OTLP trace export: {err}"))
    })?;
    stamp(&mut request, claims);

    let response = upstream
        .client
        .post(upstream.endpoint.clone())
        .headers(upstream.headers.clone())
        .header(reqwest::header::CONTENT_TYPE, "application/x-protobuf")
        .body(request.encode_to_vec())
        .send()
        .await
        .map_err(|err| {
            tonic::Status::unavailable(format!("failed to forward traces: {}", err.without_url()))
        })?;

    let status = response.status();
    if matches!(
        status,
        axum::http::StatusCode::UNAUTHORIZED | axum::http::StatusCode::FORBIDDEN
    ) {
        // The backend rejected agent-api's credentials, which the client can't fix.
        tracing::error!(%status, "trace backend rejected forwarded traces");
        return Ok(axum::response::IntoResponse::into_response(
            axum::http::StatusCode::BAD_GATEWAY,
        ));
    }
    // Pass through the backend's `ExportTraceServiceResponse` or error `Status`, along with any retryable status code.
    let mut builder = axum::http::Response::builder().status(status);
    if let Some(content_type) = response.headers().get(reqwest::header::CONTENT_TYPE) {
        builder = builder.header(reqwest::header::CONTENT_TYPE, content_type);
    }
    Ok(builder
        .body(axum::body::Body::from_stream(response.bytes_stream()))
        .unwrap())
}

/// Removes `enduser.*` and `user.*` from resource and span attributes, then sets
/// each span's `enduser.id` to the verified caller's user ID. Agent-api records
/// the same ID on its HTTP spans, so one query matches spans from either source.
fn stamp(request: &mut collector::ExportTraceServiceRequest, claims: &crate::ControlClaims) {
    let enduser_id = common::KeyValue {
        key: "enduser.id".to_owned(),
        value: Some(common::AnyValue {
            value: Some(common::any_value::Value::StringValue(
                claims.sub.to_string(),
            )),
        }),
    };
    let is_identity =
        |kv: &common::KeyValue| kv.key.starts_with("enduser.") || kv.key.starts_with("user.");

    for resource_spans in &mut request.resource_spans {
        if let Some(resource) = &mut resource_spans.resource {
            resource.attributes.retain(|kv| !is_identity(kv));
        }
        for span in resource_spans
            .scope_spans
            .iter_mut()
            .flat_map(|scope_spans| &mut scope_spans.spans)
        {
            span.attributes.retain(|kv| !is_identity(kv));
            span.attributes.push(enduser_id.clone());
        }
    }
}

#[cfg(test)]
mod tests;
