use super::outcome;
use anyhow::Context;

/// Credentials are deliberately excluded from Debug, including the agent's CLI dump.
#[derive(Default, clap::Args)]
pub struct Config {
    #[clap(long, env = "REDDIT_CONVERSION_TOKEN", requires = "reddit_pixel_id")]
    pub reddit_conversion_token: Option<String>,
    #[clap(long, env = "REDDIT_PIXEL_ID", requires = "reddit_conversion_token")]
    pub reddit_pixel_id: Option<String>,
    #[clap(
        long,
        env = "LINKEDIN_CONVERSION_TOKEN",
        requires = "linkedin_conversion_id"
    )]
    pub linkedin_conversion_token: Option<String>,
    #[clap(
        long,
        env = "LINKEDIN_CONVERSION_ID",
        requires = "linkedin_conversion_token"
    )]
    pub linkedin_conversion_id: Option<u64>,
    #[clap(long, env = "LINKEDIN_API_VERSION", default_value = "202605")]
    pub linkedin_api_version: String,
}

pub struct Reporter {
    config: Config,
    client: reqwest::Client,
}

impl Reporter {
    pub fn new(config: Config) -> anyhow::Result<Self> {
        Ok(Self {
            config,
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .redirect(reqwest::redirect::Policy::none())
                .user_agent("estuary-flow/signup-attribution")
                .build()
                .context("building signup attribution HTTP client")?,
        })
    }
}

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct DeliveryStatus {
    #[serde(flatten)]
    pub task: super::TaskStatus,
    #[serde(default)]
    pub complete: bool,
}

#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Provider {
    Reddit,
    Linkedin,
}

pub type State = std::collections::BTreeMap<Provider, DeliveryStatus>;

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct Click {
    provider: Provider,
    click_id: String,
}

fn clicks(metadata: &serde_json::Value) -> Vec<Click> {
    let mut clicks: Vec<Click> = Vec::new();
    if let Some(values) = metadata
        .pointer("/signupAttribution/adClicks")
        .and_then(|v| v.as_array())
    {
        for value in values.iter().take(16) {
            let Ok(click) = serde_json::from_value::<Click>(value.clone()) else {
                continue;
            };
            if click.click_id.trim().is_empty()
                || click.click_id.len() > 256
                || clicks.iter().any(|kept| kept.provider == click.provider)
            {
                continue;
            }
            clicks.push(click);
        }
    }
    clicks
}

// A click's client-supplied clickedAt is attribution context, not the conversion
// time. Retries use tenant creation time and ID, including when a worker dies
// after delivery but before checkpointing.
fn payload(
    click: &Click,
    event_id: &str,
    created_at: chrono::DateTime<chrono::Utc>,
    config: &Config,
) -> serde_json::Value {
    match click.provider {
        Provider::Reddit => serde_json::json!({"events": [{
            "event_at": created_at.timestamp_millis(),
            "action_source": "WEBSITE",
            "type": {"tracking_type": "SIGN_UP"},
            "click_id": click.click_id,
            "metadata": {"conversion_id": event_id}
        }]}),
        Provider::Linkedin => serde_json::json!({
            "conversion": format!("urn:lla:llaPartnerConversion:{}", config.linkedin_conversion_id.expect("configured LinkedIn conversion")),
            "conversionHappenedAt": created_at.timestamp_millis(),
            "eventId": event_id,
            "user": {"userIds": [{
                "idType": "LINKEDIN_FIRST_PARTY_ADS_TRACKING_UUID",
                "idValue": click.click_id
            }]}
        }),
    }
}

impl Reporter {
    fn request(
        &self,
        click: &Click,
        event_id: &str,
        created_at: chrono::DateTime<chrono::Utc>,
    ) -> Option<reqwest::RequestBuilder> {
        let request = match click.provider {
            Provider::Reddit => {
                let token = self.config.reddit_conversion_token.as_ref()?;
                let pixel = self.config.reddit_pixel_id.as_ref()?;
                let mut url = url::Url::parse("https://ads-api.reddit.com/api/v3/pixels/").unwrap();
                url.path_segments_mut()
                    .unwrap()
                    .pop_if_empty()
                    .push(pixel)
                    .push("conversion_events");
                self.client.post(url).bearer_auth(token)
            }
            Provider::Linkedin => {
                let token = self.config.linkedin_conversion_token.as_ref()?;
                self.config.linkedin_conversion_id?;
                self.client
                    .post("https://api.linkedin.com/rest/conversionEvents")
                    .bearer_auth(token)
                    .header("LinkedIn-Version", &self.config.linkedin_api_version)
                    .header("X-Restli-Protocol-Version", "2.0.0")
            }
        };
        Some(request.json(&payload(click, event_id, created_at, &self.config)))
    }
}

fn record_result(
    status: &mut DeliveryStatus,
    result: Result<reqwest::StatusCode, reqwest::Error>,
    now: chrono::DateTime<chrono::Utc>,
) -> outcome::Outcome {
    // Response bodies may echo click IDs. Persist only the HTTP status, and never
    // log requests or transport errors that could contain identifiers.
    match result {
        Ok(code) if code.is_success() => {
            status.complete = true;
            status.task = super::TaskStatus::default();
            return outcome::Outcome::Idle;
        }
        Ok(code) => {
            status.task.last_error = Some(format!("conversion API returned HTTP {code}"));
            if code.is_client_error() && !matches!(code.as_u16(), 401 | 403 | 408 | 429) {
                status.complete = true;
                status.task.next_retry = None;
                return outcome::Outcome::Idle;
            }
        }
        Err(_) => status.task.last_error = Some("conversion API transport failure".to_string()),
    }
    status.task.failures = status.task.failures.saturating_add(1);
    let backoff = super::retry_backoff(status.task.failures);
    status.task.next_retry = Some(now + chrono::Duration::from_std(backoff).unwrap());
    outcome::Outcome::WaitForRetry(backoff)
}

pub async fn reconcile(
    state: &mut State,
    pool: &sqlx::PgPool,
    task_id: models::Id,
    reporter: &Reporter,
) -> anyhow::Result<outcome::Outcome> {
    let (tenant_id, created_at, metadata): (
        models::Id,
        chrono::DateTime<chrono::Utc>,
        serde_json::Value,
    ) = sqlx::query_as(
        "SELECT id, created_at, metadata FROM tenants WHERE controller_task_id = $1",
    )
    .bind(task_id)
    .fetch_one(pool)
    .await
    .context("fetching signup attribution")?;
    deliver(state, &metadata, tenant_id, created_at, reporter).await
}

async fn deliver(
    state: &mut State,
    metadata: &serde_json::Value,
    tenant_id: models::Id,
    created_at: chrono::DateTime<chrono::Utc>,
    reporter: &Reporter,
) -> anyhow::Result<outcome::Outcome> {
    let event_id = format!("tenant-signup-{tenant_id}");
    let mut outcome = outcome::Outcome::Idle;
    for click in clicks(metadata) {
        let status = state.entry(click.provider).or_default();
        if status.complete {
            continue;
        }
        let now = chrono::Utc::now();
        // Bound retry lifetime, including disabled integrations. Reddit rejects
        // events older than seven days; use the same operational window for both.
        if now >= created_at + chrono::Duration::days(7) {
            status.complete = true;
            status.task.next_retry = None;
            status.task.last_error = Some("signup reporting window expired".to_string());
            continue;
        }
        if let Some(next_retry) = status.task.next_retry.filter(|at| *at > now) {
            outcome = outcome.combine(outcome::Outcome::WaitForRetry(
                (next_retry - now).to_std().unwrap(),
            ));
            continue;
        }
        let Some(request) = reporter.request(&click, &event_id, created_at) else {
            // Configuration may arrive on a later deployment. Do not mark an
            // unconfigured provider as delivered or require a manual task wake.
            let delay = std::time::Duration::from_secs(900);
            status.task.last_error = Some("conversion reporting is not configured".to_string());
            status.task.next_retry = Some(now + chrono::Duration::minutes(15));
            outcome = outcome.combine(outcome::Outcome::WaitForRetry(delay));
            continue;
        };
        let result = request.send().await.map(|response| response.status());
        outcome = outcome.combine(record_result(status, result, now));
    }
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> Config {
        Config {
            reddit_conversion_token: Some("test-reddit-token".to_string()),
            reddit_pixel_id: Some("test-pixel".to_string()),
            linkedin_conversion_token: Some("test-linkedin-token".to_string()),
            linkedin_conversion_id: Some(123),
            linkedin_api_version: "202605".to_string(),
        }
    }

    #[test]
    fn provider_requests() {
        let reporter = Reporter::new(config()).unwrap();
        let now = chrono::DateTime::from_timestamp_millis(1_790_000_000_000).unwrap();
        let mut requests = Vec::new();
        for provider in [Provider::Reddit, Provider::Linkedin] {
            let click = clicks(&serde_json::json!({"signupAttribution": {"adClicks": [{
                "provider": provider,
                "clickId": "invented-click",
                "clickedAt": "2026-09-01T00:00:00Z"
            }]}}))
            .pop()
            .unwrap();
            let request = reporter
                .request(&click, "tenant-signup-test", now)
                .unwrap()
                .build()
                .unwrap();
            assert_eq!(
                request.headers()["authorization"],
                match provider {
                    Provider::Reddit => "Bearer test-reddit-token",
                    Provider::Linkedin => "Bearer test-linkedin-token",
                }
            );
            requests.push(serde_json::json!({
                "url": request.url().as_str(),
                "method": request.method().as_str(),
                "version": request.headers().get("LinkedIn-Version").map(|v| v.to_str().unwrap()),
                "restli": request.headers().get("X-Restli-Protocol-Version").map(|v| v.to_str().unwrap()),
                "body": serde_json::from_slice::<serde_json::Value>(request.body().unwrap().as_bytes().unwrap()).unwrap()
            }));
        }
        insta::assert_json_snapshot!(requests);
    }

    #[test]
    fn independent_retries_and_restart() {
        let now = chrono::Utc::now();
        let mut state = State::default();
        assert!(matches!(
            record_result(
                state.entry(Provider::Reddit).or_default(),
                Ok(reqwest::StatusCode::OK),
                now
            ),
            outcome::Outcome::Idle
        ));
        assert!(matches!(
            record_result(
                state.entry(Provider::Linkedin).or_default(),
                Ok(reqwest::StatusCode::TOO_MANY_REQUESTS),
                now
            ),
            outcome::Outcome::WaitForRetry(_)
        ));
        let mut restored: State =
            serde_json::from_value(serde_json::to_value(&state).unwrap()).unwrap();
        assert!(restored[&Provider::Reddit].complete);
        assert!(!restored[&Provider::Linkedin].complete);
        assert_eq!(
            restored[&Provider::Linkedin].task.next_retry,
            Some(now + chrono::Duration::seconds(60))
        );
        record_result(
            restored.get_mut(&Provider::Linkedin).unwrap(),
            Ok(reqwest::StatusCode::CREATED),
            now,
        );
        assert!(
            restored
                .values()
                .all(|s| s.complete && s.task.last_error.is_none())
        );
    }

    #[test]
    fn rejected_events_and_retryable_errors() {
        let now = chrono::Utc::now();
        for code in [400, 401, 403, 408, 422, 429, 500, 503] {
            let mut status = DeliveryStatus::default();
            record_result(
                &mut status,
                Ok(reqwest::StatusCode::from_u16(code).unwrap()),
                now,
            );
            assert_eq!(status.complete, matches!(code, 400 | 422));
            assert_eq!(status.task.next_retry.is_some(), !status.complete);
        }
    }

    #[tokio::test]
    async fn disabled_backoff_complete_and_expired() {
        let reporter = Reporter::new(Config::default()).unwrap();
        let metadata = serde_json::json!({"signupAttribution": {"adClicks": [
            {"provider": "REDDIT", "clickId": "invented-reddit-click"},
            {"provider": "LINKEDIN", "clickId": "invented-linkedin-click"}
        ]}});
        let now = chrono::Utc::now();
        let mut state = State::default();
        assert!(matches!(
            deliver(&mut state, &metadata, models::Id::zero(), now, &reporter)
                .await
                .unwrap(),
            outcome::Outcome::WaitForRetry(_)
        ));
        assert!(state.values().all(|s| !s.complete && s.task.failures == 0));
        let before = serde_json::to_value(&state).unwrap();
        deliver(&mut state, &metadata, models::Id::zero(), now, &reporter)
            .await
            .unwrap();
        assert_eq!(before, serde_json::to_value(&state).unwrap());
        // Delivered providers stay complete even if configuration is later removed.
        state.get_mut(&Provider::Reddit).unwrap().complete = true;
        let reddit = serde_json::to_value(&state[&Provider::Reddit]).unwrap();
        assert!(matches!(
            deliver(
                &mut state,
                &metadata,
                models::Id::zero(),
                now - chrono::Duration::days(8),
                &reporter
            )
            .await
            .unwrap(),
            outcome::Outcome::Idle
        ));
        assert_eq!(
            reddit,
            serde_json::to_value(&state[&Provider::Reddit]).unwrap()
        );
        assert!(state[&Provider::Linkedin].complete);
        assert_eq!(
            state[&Provider::Linkedin].task.last_error.as_deref(),
            Some("signup reporting window expired")
        );
        let legacy: super::super::TenantControllerState = serde_json::from_str("{}").unwrap();
        assert!(legacy.signup_attribution.is_empty());
    }

    #[test]
    fn tolerates_unrecognized_and_malformed_metadata() {
        let metadata = serde_json::json!({"signupAttribution": {"adClicks": [
            {"provider": "FUTURE", "clickId": "unknown"},
            {"provider": "REDDIT", "clickId": " "},
            {"provider": "REDDIT", "clickId": "invented-click"},
            {"provider": "REDDIT", "clickId": "duplicate"},
            {"provider": "LINKEDIN", "clickId": "x".repeat(257)},
            {"provider": "LINKEDIN", "clickId": "invented-click"}
        ]}});
        let parsed = clicks(&metadata);
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].click_id, "invented-click");
        assert_eq!(parsed[1].provider, Provider::Linkedin);
        assert!(clicks(&serde_json::json!({})).is_empty());
    }
}
