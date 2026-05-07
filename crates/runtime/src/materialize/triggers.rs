use super::{Task, Transaction};
use anyhow::Context;
use models::TriggerVariables;
use proto_gazette::uuid::Clock;

/// Pre-compiled trigger templates and their associated configs.
pub struct CompiledTriggers {
    pub configs: Vec<models::TriggerConfig>,
    registry: handlebars::Handlebars<'static>,
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

/// Compute trigger variables from the current transaction state and task metadata.
pub fn trigger_variables(
    task: &Task,
    txn: &Transaction,
    connector_image: &str,
) -> TriggerVariables {
    // Collect collection names from bindings that have received documents.
    let collection_names: Vec<String> = txn
        .stats
        .iter()
        .map(|(index, _)| task.bindings[*index as usize].collection_name.clone())
        .collect();

    let materialization_name = task.shard_ref.name.clone();

    // Compute min of first_source_clock and max of last_source_clock across bindings.
    let first_clocks = txn
        .stats
        .values()
        .map(|s| s.first_source_clock)
        .filter(|c| *c != Clock::default());
    let last_clocks = txn
        .stats
        .values()
        .map(|s| s.last_source_clock)
        .filter(|c| *c != Clock::default());

    let flow_published_at_min = first_clocks
        .min()
        .map(|c| clock_to_rfc3339(&c))
        .unwrap_or_default();

    let flow_published_at_max = last_clocks
        .max()
        .map(|c| clock_to_rfc3339(&c))
        .unwrap_or_default();

    let run_id = time::OffsetDateTime::from(txn.started_at)
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default();

    TriggerVariables {
        collection_names,
        connector_image: connector_image.to_string(),
        materialization_name,
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

    use super::{Task, Transaction};
    use crate::materialize::Binding;
    use proto_gazette::uuid::Clock;

    fn mock_task(binding_names: &[&str]) -> Task {
        Task {
            bindings: binding_names
                .iter()
                .map(|name| Binding {
                    collection_name: name.to_string(),
                    delta_updates: false,
                    journal_read_suffix: String::new(),
                    key_extractors: Vec::new(),
                    read_schema_json: bytes::Bytes::new(),
                    ser_policy: doc::SerPolicy::noop(),
                    state_key: String::new(),
                    store_document: false,
                    value_plan: doc::ExtractorPlan::new(&[]),
                    uuid_ptr: json::Pointer::empty(),
                })
                .collect(),
            shard_ref: ops::ShardRef {
                kind: ops::TaskType::Materialization as i32,
                name: "acmeCo/my-materialization".to_string(),
                key_begin: "00000000".to_string(),
                r_clock_begin: "00000000".to_string(),
                build: "test-build".to_string(),
            },
        }
    }

    #[test]
    fn trigger_variables_snapshot() {
        let task = mock_task(&["acmeCo/collection-a", "acmeCo/collection-b"]);
        let mut txn = Transaction::new();
        txn.started = true;

        let stats = txn.stats.entry(0).or_default();
        stats.right.docs_total = 5;
        stats.first_source_clock = Clock::from_unix(1000, 0);
        stats.last_source_clock = Clock::from_unix(3000, 0);

        let stats = txn.stats.entry(1).or_default();
        stats.right.docs_total = 3;
        stats.first_source_clock = Clock::from_unix(500, 0);
        stats.last_source_clock = Clock::from_unix(4000, 0);

        let mut vars = trigger_variables(&task, &txn, "ghcr.io/estuary/materialize-postgres:dev");
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
