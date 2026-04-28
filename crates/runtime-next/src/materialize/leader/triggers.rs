//! Trigger compilation, variable construction, and webhook delivery.
//!
//! This module is the single source of truth for materialize trigger logic.
//! It is consumed by:
//!   - The leader actor, which fires triggers post-commit via the Shuffle
//!     Leader protocol.
//!   - The legacy `runtime` crate (via re-export from
//!     `runtime/src/materialize/triggers.rs`), preserving its existing
//!     per-shard firing behavior.
//!
//! Inputs are POD primitives (`TriggerInputs`) so this module has no
//! coupling to either runtime's task/transaction types.

use anyhow::Context;
use models::TriggerVariables;
use proto_gazette::uuid::Clock;

/// Pre-compiled trigger templates and their associated configs.
pub struct CompiledTriggers {
    pub configs: Vec<models::TriggerConfig>,
    registry: handlebars::Handlebars<'static>,
}

impl std::fmt::Debug for CompiledTriggers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledTriggers")
            .field("configs", &self.configs.len())
            .finish_non_exhaustive()
    }
}

impl CompiledTriggers {
    /// Compile all trigger payload templates into a shared Handlebars registry.
    pub fn compile(configs: Vec<models::TriggerConfig>) -> anyhow::Result<Self> {
        let mut registry = handlebars::Handlebars::new();
        registry.set_strict_mode(true);
        registry.register_escape_fn(handlebars::no_escape);

        for (index, config) in configs.iter().enumerate() {
            registry
                .register_template_string(&Self::template_name(index), &config.payload_template)
                .with_context(|| format!("compiling trigger {index} template"))?;
        }

        Ok(Self { configs, registry })
    }

    /// Render the template for trigger `index` with the given context.
    pub fn render(&self, index: usize, context: &serde_json::Value) -> anyhow::Result<String> {
        self.registry
            .render(&Self::template_name(index), context)
            .with_context(|| format!("rendering trigger {index} template"))
    }

    fn template_name(index: usize) -> String {
        format!("trigger_{index}")
    }
}

/// Decrypt the SOPS-encrypted `triggers_json` blob from a materialization
/// spec and compile its templates. Returns `None` if `triggers_json` is empty.
pub async fn decrypt_and_compile(triggers_json: &[u8]) -> anyhow::Result<Option<CompiledTriggers>> {
    if triggers_json.is_empty() {
        return Ok(None);
    }

    let mut triggers: models::Triggers =
        serde_json::from_slice(triggers_json).context("parsing triggers JSON")?;

    // Strip HMAC-excluded fields before decryption (they were stripped
    // during encryption so SOPS HMAC doesn't cover them), then restore.
    let originals = models::triggers::strip_hmac_excluded_fields(&mut triggers);
    let stripped = models::RawValue::from_value(
        &serde_json::to_value(&triggers).context("serializing stripped triggers")?,
    );

    let mut decrypted: models::Triggers = serde_json::from_str(
        unseal::decrypt_sops(&stripped)
            .await
            .context("decrypting triggers_json")?
            .get(),
    )
    .context("parsing decrypted triggers JSON")?;

    models::triggers::restore_hmac_excluded_fields(&mut decrypted, originals);

    Ok(Some(
        CompiledTriggers::compile(decrypted.config).context("compiling trigger templates")?,
    ))
}

/// Inputs to `trigger_variables`. POD primitives so callers in different
/// crates can supply them from their own task/transaction shapes.
pub struct TriggerInputs<'a> {
    /// Collection names of bindings that received documents this transaction.
    pub collection_names: &'a [String],
    pub materialization_name: &'a str,
    pub connector_image: &'a str,
    /// First-document time of the transaction (used as `run_id`).
    pub started_at: std::time::SystemTime,
    /// Min over all bindings' first-source clocks; `None` if no docs were read.
    pub first_source_clock_min: Option<Clock>,
    /// Max over all bindings' last-source clocks; `None` if no docs were read.
    pub last_source_clock_max: Option<Clock>,
}

/// Compute trigger variables from POD inputs.
pub fn trigger_variables(inputs: &TriggerInputs<'_>) -> TriggerVariables {
    let flow_published_at_min = inputs
        .first_source_clock_min
        .map(|c| clock_to_rfc3339(&c))
        .unwrap_or_default();

    let flow_published_at_max = inputs
        .last_source_clock_max
        .map(|c| clock_to_rfc3339(&c))
        .unwrap_or_default();

    let run_id = time::OffsetDateTime::from(inputs.started_at)
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default();

    TriggerVariables {
        collection_names: inputs.collection_names.to_vec(),
        connector_image: inputs.connector_image.to_string(),
        materialization_name: inputs.materialization_name.to_string(),
        flow_published_at_min,
        flow_published_at_max,
        run_id,
    }
}

fn clock_to_rfc3339(clock: &Clock) -> String {
    let (seconds, nanos) = clock.to_unix();
    let ts = time::OffsetDateTime::from_unix_timestamp(seconds as i64)
        .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
    let ts = ts + time::Duration::nanoseconds(nanos as i64);
    ts.format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}

/// Fire all configured triggers using the given variables.
pub async fn fire_pending_triggers(
    compiled: &CompiledTriggers,
    variables: &TriggerVariables,
    client: &reqwest::Client,
) -> anyhow::Result<()> {
    let started_at = std::time::Instant::now();

    send_webhooks(
        compiled,
        variables,
        client,
        std::time::Duration::from_secs(1),
    )
    .await
    .map_err(|err| {
        tracing::error!(%err, "trigger webhook delivery failed");
        err
    })
    .context("trigger webhook delivery failed")?;

    tracing::info!(
        num_triggers = compiled.configs.len(),
        elapsed_ms = started_at.elapsed().as_millis() as u64,
        "trigger webhooks delivered successfully",
    );

    Ok(())
}

/// Render and send all configured trigger webhooks concurrently.
pub async fn send_webhooks(
    compiled: &CompiledTriggers,
    variables: &TriggerVariables,
    client: &reqwest::Client,
    base_backoff: std::time::Duration,
) -> anyhow::Result<()> {
    if compiled.configs.is_empty() {
        return Ok(());
    }

    let rendered: Vec<String> = compiled
        .configs
        .iter()
        .enumerate()
        .map(|(index, trigger)| {
            let context = models::build_template_context(variables, &trigger.headers);
            compiled.render(index, &context)
        })
        .collect::<anyhow::Result<_>>()?;

    let futures: Vec<_> = compiled
        .configs
        .iter()
        .zip(rendered)
        .enumerate()
        .map(|(index, (trigger, body))| {
            send_single_webhook(index, trigger, body, client, base_backoff)
        })
        .collect();

    let results = futures::future::join_all(futures).await;

    let errors: Vec<String> = results
        .into_iter()
        .filter_map(|r| r.err().map(|e| e.to_string()))
        .collect();

    if errors.is_empty() {
        Ok(())
    } else {
        anyhow::bail!("{} trigger(s) failed: {}", errors.len(), errors.join("; "))
    }
}

async fn send_single_webhook(
    index: usize,
    trigger: &models::TriggerConfig,
    body: String,
    client: &reqwest::Client,
    base_backoff: std::time::Duration,
) -> anyhow::Result<()> {
    let total_attempts = trigger.max_attempts.max(1);

    let mut last_err = String::new();

    for attempt in 0..total_attempts {
        if attempt > 0 {
            // Exponential backoff capped at 30 seconds.
            let delay = base_backoff
                .saturating_mul(2u32.saturating_pow(attempt - 1))
                .min(std::time::Duration::from_secs(30));
            tokio::time::sleep(delay).await;
        }

        let method = match trigger.method {
            models::HttpMethod::POST => reqwest::Method::POST,
            models::HttpMethod::PUT => reqwest::Method::PUT,
            models::HttpMethod::PATCH => reqwest::Method::PATCH,
        };
        let has_content_type = trigger
            .headers
            .keys()
            .any(|k| k.eq_ignore_ascii_case("content-type"));

        let mut request = client
            .request(method, &trigger.url)
            .timeout(trigger.timeout)
            .body(body.clone());

        if !has_content_type {
            request = request.header(reqwest::header::CONTENT_TYPE, "application/json");
        }
        for (name, value) in &trigger.headers {
            request = request.header(name, value);
        }

        match request.send().await {
            Ok(response) if response.status().is_success() => {
                return Ok(());
            }
            Ok(response) => {
                let status = response.status();
                let response_body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "<failed to read body>".to_string());

                last_err = format!("HTTP {status}: {response_body}");

                // 4xx errors (other than 408 Request Timeout and 429 Too Many
                // Requests) indicate a client-side problem that won't resolve
                // on retry — fail immediately.
                if status.is_client_error()
                    && status != reqwest::StatusCode::REQUEST_TIMEOUT
                    && status != reqwest::StatusCode::TOO_MANY_REQUESTS
                {
                    anyhow::bail!(
                        "trigger {index} ({}) received non-retryable {status}: {response_body}",
                        trigger.url,
                    );
                }

                tracing::warn!(
                    trigger_index = index,
                    url = %trigger.url,
                    %status,
                    attempt = attempt + 1,
                    total_attempts,
                    "trigger webhook received non-success response, will retry"
                );
            }
            Err(err) => {
                last_err = err.to_string();
                tracing::warn!(
                    trigger_index = index,
                    url = %trigger.url,
                    error = %err,
                    attempt = attempt + 1,
                    total_attempts,
                    "trigger webhook request failed, will retry"
                );
            }
        }
    }

    anyhow::bail!(
        "trigger {index} ({}) exhausted {total_attempts} attempts: {last_err}",
        trigger.url,
    )
}

#[cfg(test)]
mod test {
    use super::*;
    use models::TriggerVariables;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    async fn start_mock_server(
        app: axum::Router,
    ) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        (addr, handle)
    }

    fn make_trigger_with_url(url: &str, template: &str) -> models::TriggerConfig {
        models::TriggerConfig {
            url: url.to_string(),
            method: models::HttpMethod::POST,
            headers: Default::default(),
            payload_template: template.to_string(),
            timeout: std::time::Duration::from_secs(5),
            max_attempts: 3,
        }
    }

    #[test]
    fn render_template() {
        let mut trigger = make_trigger_with_url(
            "https://example.com/webhook",
            r#"{
  "event": "materialization_transaction_completed",
  "connector": "{{connector_image}}",
  "collections": [{{#each collection_names}}"{{this}}"{{#unless @last}}, {{/unless}}{{/each}}],
  "materialization": "{{materialization_name}}",
  "flow_published_at_min": "{{flow_published_at_min}}",
  "flow_published_at_max": "{{flow_published_at_max}}",
  "run_id": "{{run_id}}",
  "auth": "{{headers.Authorization}}"
}"#,
        );
        trigger
            .headers
            .insert("Authorization".to_string(), "Bearer my-secret".to_string());
        let compiled = CompiledTriggers::compile(vec![trigger.clone()]).unwrap();
        let context =
            models::build_template_context(&TriggerVariables::placeholder(), &trigger.headers);
        let rendered = compiled.render(0, &context).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        insta::assert_json_snapshot!("rendered-template", parsed);
    }

    #[test]
    fn trigger_variables_snapshot() {
        let collection_names = vec![
            "acmeCo/collection-a".to_string(),
            "acmeCo/collection-b".to_string(),
        ];
        let mut vars = trigger_variables(&TriggerInputs {
            collection_names: &collection_names,
            materialization_name: "acmeCo/my-materialization",
            connector_image: "ghcr.io/estuary/materialize-postgres:dev",
            started_at: std::time::SystemTime::UNIX_EPOCH,
            first_source_clock_min: Some(Clock::from_unix(500, 0)),
            last_source_clock_max: Some(Clock::from_unix(4000, 0)),
        });
        vars.run_id = "2024-06-15T12:30:00.000Z".to_string();

        insta::assert_json_snapshot!("trigger-variables", vars);
    }

    #[tokio::test]
    async fn webhook_retry_behavior() {
        struct Case {
            status: u16,
            fail_times: u32,
            max_attempts: u32,
            expect_success: bool,
            expect_calls: u32,
        }

        let cases = vec![
            // Happy path — succeeds immediately.
            Case {
                status: 200,
                fail_times: 0,
                max_attempts: 3,
                expect_success: true,
                expect_calls: 1,
            },
            // Retryable — succeeds after 2 failures.
            Case {
                status: 429,
                fail_times: 2,
                max_attempts: 3,
                expect_success: true,
                expect_calls: 3,
            },
            Case {
                status: 500,
                fail_times: 2,
                max_attempts: 3,
                expect_success: true,
                expect_calls: 3,
            },
            // Retryable — exhausts retries.
            Case {
                status: 500,
                fail_times: 9,
                max_attempts: 2,
                expect_success: false,
                expect_calls: 2,
            },
            // Non-retryable — fails immediately.
            Case {
                status: 400,
                fail_times: 9,
                max_attempts: 3,
                expect_success: false,
                expect_calls: 1,
            },
        ];

        for case in cases {
            let call_count = Arc::new(AtomicU32::new(0));
            let count_clone = call_count.clone();
            let fail_times = case.fail_times;
            let status = case.status;

            let app = axum::Router::new().route(
                "/webhook",
                axum::routing::post(move || {
                    let count = count_clone.clone();
                    async move {
                        if count.fetch_add(1, Ordering::SeqCst) < fail_times {
                            axum::http::StatusCode::from_u16(status).unwrap()
                        } else {
                            axum::http::StatusCode::OK
                        }
                    }
                }),
            );

            let (addr, _handle) = start_mock_server(app).await;
            let mut trigger =
                make_trigger_with_url(&format!("http://{addr}/webhook"), r#"{"event": "test"}"#);
            trigger.max_attempts = case.max_attempts;

            let compiled = CompiledTriggers::compile(vec![trigger]).unwrap();
            let result = send_webhooks(
                &compiled,
                &TriggerVariables::placeholder(),
                &reqwest::Client::new(),
                std::time::Duration::ZERO,
            )
            .await;

            assert_eq!(
                result.is_ok(),
                case.expect_success,
                "status {status}: expected success={}, got {result:?}",
                case.expect_success
            );
            assert_eq!(
                call_count.load(Ordering::SeqCst),
                case.expect_calls,
                "status {status}: expected {} calls",
                case.expect_calls
            );
        }
    }
}
