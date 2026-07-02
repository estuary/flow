use anyhow::Context;
use models::TriggerVariables;
use std::collections::BTreeMap;

/// Stable identity for a trigger config, used to key debounce state so it
/// survives config reordering or additions across sessions.
pub fn config_key(config: &models::TriggerConfig) -> String {
    format!("{:?} {}", config.method, config.url)
}

/// Pre-compiled trigger templates and their associated configs.
pub struct CompiledTriggers {
    pub configs: Vec<models::TriggerConfig>,
    /// Maps a config's stable `config_key` to its index in `configs`.
    key_index: BTreeMap<String, usize>,
    registry: handlebars::Handlebars<'static>,
}

impl CompiledTriggers {
    /// Compile all trigger payload templates into a shared Handlebars registry.
    pub fn compile(configs: Vec<models::TriggerConfig>) -> anyhow::Result<Self> {
        let mut registry = handlebars::Handlebars::new();
        registry.set_strict_mode(true);
        registry.register_escape_fn(handlebars::no_escape);

        let mut key_index = BTreeMap::new();
        for (index, config) in configs.iter().enumerate() {
            registry
                .register_template_string(&Self::template_name(index), &config.payload_template)
                .with_context(|| format!("compiling trigger {index} template"))?;

            // Debounce state is keyed by `config_key` (method + URL), so
            // multiple configs sharing a key aren't supported: the first config
            // wins and later duplicates never fire.
            let key = config_key(config);
            if key_index.contains_key(&key) {
                service_kit::event!(
                    tracing::Level::WARN,
                    "trigger",
                    trigger_key = key.clone(),
                    trigger_index = index,
                    "duplicate trigger configs share a method and URL; only the first will fire",
                );
            } else {
                key_index.insert(key, index);
            }
        }

        Ok(Self {
            configs,
            key_index,
            registry,
        })
    }

    /// Stable keys of all configured triggers, one per config.
    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.key_index.keys().map(String::as_str)
    }

    /// Resolve a stable `config_key` to its config index, or None if no
    /// current config matches (e.g. the trigger was removed on republish).
    pub fn index_for_key(&self, key: &str) -> Option<usize> {
        self.key_index.get(key).copied()
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

impl std::fmt::Debug for CompiledTriggers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledTriggers").finish()
    }
}

/// Decode persisted trigger parameters into the per-config to_fire map.
/// A legacy single-window blob (persisted by a pre-debounce build, or by the
/// V1 runtime ahead of a migration) fans out to every configured trigger,
/// matching its original fire-all semantics.
pub fn decode_to_fire(
    compiled: &CompiledTriggers,
    bytes: &[u8],
) -> anyhow::Result<BTreeMap<String, TriggerVariables>> {
    match serde_json::from_slice::<models::triggers::PersistedTriggerParams>(bytes)
        .context("decoding trigger to_fire JSON")?
    {
        models::triggers::PersistedTriggerParams::PerConfig(map) => Ok(map),
        models::triggers::PersistedTriggerParams::Single(variables) => Ok(compiled
            .keys()
            .map(|key| (key.to_string(), variables.clone()))
            .collect()),
    }
}

/// Fire the due subset of triggers. `to_fire` maps a config's stable
/// `config_key` to the accumulated window to deliver for that config.
pub async fn fire_pending_triggers(
    compiled: &CompiledTriggers,
    to_fire: &BTreeMap<String, TriggerVariables>,
    client: &reqwest::Client,
) -> anyhow::Result<()> {
    let started_at = std::time::Instant::now();

    send_webhooks(compiled, to_fire, client, std::time::Duration::from_secs(1))
        .await
        .context("trigger webhook delivery failed")?;

    service_kit::event!(
        tracing::Level::INFO,
        "leader",
        num_triggers = to_fire.len(),
        elapsed_ms = started_at.elapsed().as_millis() as u64,
        "trigger webhooks delivered successfully",
    );

    Ok(())
}

/// Render and send the due trigger webhooks concurrently, each with its own
/// accumulated window.
pub async fn send_webhooks(
    compiled: &CompiledTriggers,
    to_fire: &BTreeMap<String, TriggerVariables>,
    client: &reqwest::Client,
    base_backoff: std::time::Duration,
) -> anyhow::Result<()> {
    let mut rendered = Vec::with_capacity(to_fire.len());
    for (key, variables) in to_fire {
        let Some(index) = compiled.index_for_key(key) else {
            // The config was removed on a republish while a delivery was still
            // pending; drop it rather than fail the task.
            service_kit::event!(
                tracing::Level::WARN,
                "trigger",
                trigger_key = key.clone(),
                "pending trigger has no matching config; dropping",
            );
            continue;
        };
        let trigger = &compiled.configs[index];
        let context = models::build_template_context(variables, &trigger.headers);
        rendered.push((index, trigger, compiled.render(index, &context)?));
    }

    let futures: Vec<_> = rendered
        .into_iter()
        .map(|(index, trigger, body)| {
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

                service_kit::event!(
                    tracing::Level::WARN,
                    "trigger",
                    trigger_index = index,
                    url = trigger.url.clone(),
                    status = status.as_u16(),
                    attempt,
                    total_attempts,
                    "trigger webhook received non-success response, will retry",
                );
            }
            Err(err) => {
                last_err = err.to_string();
                service_kit::event!(
                    tracing::Level::WARN,
                    "trigger",
                    trigger_index = index,
                    url = trigger.url.clone(),
                    error = service_kit::event::lazy(move || err.to_string()),
                    attempt,
                    total_attempts,
                    "trigger webhook request failed, will retry",
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
            interval: None,
        }
    }

    // A legacy single-window blob (pre-debounce persisted format) fans out to
    // every configured trigger; the current map format passes through as-is.
    #[test]
    fn decode_to_fire_handles_both_persisted_formats() {
        let cfg_a = make_trigger_with_url("https://a", "{}");
        let cfg_b = make_trigger_with_url("https://b", "{}");
        let compiled = CompiledTriggers::compile(vec![cfg_a.clone(), cfg_b.clone()]).unwrap();

        let variables = TriggerVariables::placeholder();
        let legacy_blob = serde_json::to_vec(&variables).unwrap();
        let fanned = decode_to_fire(&compiled, &legacy_blob).unwrap();
        assert_eq!(
            fanned,
            [
                (config_key(&cfg_a), variables.clone()),
                (config_key(&cfg_b), variables.clone()),
            ]
            .into(),
        );

        let map: BTreeMap<String, TriggerVariables> =
            [(config_key(&cfg_a), variables.clone())].into();
        let map_blob = serde_json::to_vec(&map).unwrap();
        assert_eq!(decode_to_fire(&compiled, &map_blob).unwrap(), map);
    }

    // Two configs sharing method+URL would collide in the debounce accumulator,
    // so multiple configs sharing a key aren't supported: the first wins and
    // later duplicates never fire.
    #[tokio::test]
    async fn duplicate_config_keys_first_wins() {
        let bodies = Arc::new(std::sync::Mutex::new(Vec::new()));
        let app = {
            let bodies = bodies.clone();
            axum::Router::new().route(
                "/hook",
                axum::routing::post(move |body: String| {
                    let bodies = bodies.clone();
                    async move {
                        bodies.lock().unwrap().push(body);
                        axum::http::StatusCode::OK
                    }
                }),
            )
        };
        let (addr, _handle) = start_mock_server(app).await;
        let url = format!("http://{addr}/hook");

        let first = make_trigger_with_url(&url, r#"{"which":"first"}"#);
        let second = make_trigger_with_url(&url, r#"{"which":"second"}"#);
        let compiled = CompiledTriggers::compile(vec![first.clone(), second]).unwrap();

        let to_fire: BTreeMap<String, TriggerVariables> =
            [(config_key(&first), TriggerVariables::placeholder())].into();
        send_webhooks(
            &compiled,
            &to_fire,
            &reqwest::Client::new(),
            std::time::Duration::ZERO,
        )
        .await
        .unwrap();

        assert_eq!(
            *bodies.lock().unwrap(),
            vec![r#"{"which":"first"}"#.to_string()],
            "exactly one delivery, using the first config's template",
        );
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
            let to_fire: BTreeMap<String, TriggerVariables> = compiled
                .keys()
                .map(|k| (k.to_string(), TriggerVariables::placeholder()))
                .collect();
            let result = send_webhooks(
                &compiled,
                &to_fire,
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

    // A `to_fire` map fires each configured trigger once with its own window,
    // and an entry whose key matches no current config (removed/edited on
    // republish) is skipped without failing the batch.
    #[tokio::test]
    async fn fires_map_and_skips_unknown_key() {
        let hits = Arc::new(AtomicU32::new(0));
        let app = {
            let (ha, hb) = (hits.clone(), hits.clone());
            axum::Router::new()
                .route(
                    "/a",
                    axum::routing::post(move || {
                        let h = ha.clone();
                        async move {
                            h.fetch_add(1, Ordering::SeqCst);
                            axum::http::StatusCode::OK
                        }
                    }),
                )
                .route(
                    "/b",
                    axum::routing::post(move || {
                        let h = hb.clone();
                        async move {
                            h.fetch_add(1, Ordering::SeqCst);
                            axum::http::StatusCode::OK
                        }
                    }),
                )
        };
        let (addr, _handle) = start_mock_server(app).await;

        let cfg_a = make_trigger_with_url(&format!("http://{addr}/a"), r#"{"t":"a"}"#);
        let cfg_b = make_trigger_with_url(&format!("http://{addr}/b"), r#"{"t":"b"}"#);
        let compiled = CompiledTriggers::compile(vec![cfg_a.clone(), cfg_b.clone()]).unwrap();

        let mut to_fire: BTreeMap<String, TriggerVariables> = BTreeMap::new();
        to_fire.insert(config_key(&cfg_a), TriggerVariables::placeholder());
        to_fire.insert(config_key(&cfg_b), TriggerVariables::placeholder());
        // No matching config: must be dropped, not fired and not an error.
        to_fire.insert(
            "POST http://removed/x".to_string(),
            TriggerVariables::placeholder(),
        );

        let result = send_webhooks(
            &compiled,
            &to_fire,
            &reqwest::Client::new(),
            std::time::Duration::ZERO,
        )
        .await;

        assert!(
            result.is_ok(),
            "unknown key must not fail the batch: {result:?}"
        );
        assert_eq!(
            hits.load(Ordering::SeqCst),
            2,
            "both configured triggers fire; the unknown key is skipped",
        );
    }
}
