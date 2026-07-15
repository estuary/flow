//! Inbound Stripe webhook receiver.
//!
//! This handler deliberately departs from the crate's usual public-API shape (a
//! JWT `Envelope` plus a typed `Json<T>` body). Stripe does not present a bearer
//! token; it signs each delivery with an HMAC-SHA256 over the *raw* request
//! bytes, carried in the `Stripe-Signature` header. So we take the body as
//! `axum::body::Bytes` — round-tripping it through JSON first would change the
//! bytes and break verification — and authenticate via the endpoint's signing
//! secret before trusting anything in the payload.
//!
//! We subscribe to a single event, `setup_intent.succeeded`, and use it to wake
//! the tenant's `TenantController` — the same reconciliation the
//! `setBillingPaymentMethod` GraphQL mutation triggers. The tenant is recovered
//! from the SetupIntent's metadata, which `BillingProvider::create_setup_intent`
//! stamps at creation time.

use std::sync::Arc;

/// Webhook signing secret for local development and tests only. Generated once
/// and committed so the value is stable and reusable across the test suite and
/// the local stack.
///
/// This is NOT a production secret — it is world-readable in the source tree.
/// Production must set `STRIPE_WEBHOOK_SECRET` explicitly; when it is unset the
/// handler fails closed rather than fall back to this value (see
/// `crate::App::stripe_webhook_secret`).
pub const DEV_WEBHOOK_SECRET: &str =
    "whsec_0c1ef79638919be66761253b6f15896fa30ddd38fcff54c85b66de122ae36933";

/// Handle a Stripe webhook delivery: verify the signature, then wake the tenant
/// controller on `setup_intent.succeeded`. Returns `200` for both handled and
/// intentionally-ignored events (so Stripe stops retrying), and `400` when the
/// signature does not verify.
pub async fn handle_post_stripe_webhook(
    axum::extract::State(app): axum::extract::State<Arc<crate::App>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<axum::http::StatusCode, crate::ApiError> {
    let Some(secret) = app.stripe_webhook_secret.as_deref() else {
        // Fail closed: without a configured secret we can't authenticate the
        // request, and we refuse to trust the source-tree dev fixture in an
        // environment that hasn't opted into it. A 500 makes the
        // misconfiguration loud rather than silently dropping deliveries.
        tracing::error!(
            "received a Stripe webhook but STRIPE_WEBHOOK_SECRET is not configured; rejecting"
        );
        return Err(tonic::Status::internal("stripe webhook secret is not configured").into());
    };

    let signature = headers
        .get("Stripe-Signature")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    let payload = std::str::from_utf8(&body)
        .map_err(|_| tonic::Status::invalid_argument("webhook body was not valid UTF-8"))?;

    let event = stripe::Webhook::construct_event(payload, signature, secret).map_err(|err| {
        // A bad signature or stale timestamp is the expected shape of a forged
        // or replayed request. Log at debug and return 400 so Stripe treats it
        // as a permanent rejection rather than retrying.
        tracing::debug!(?err, "rejected Stripe webhook with an invalid signature");
        tonic::Status::invalid_argument("invalid webhook signature")
    })?;

    handle_event(&app, event).await?;
    Ok(axum::http::StatusCode::OK)
}

/// Act on a verified event. Only `setup_intent.succeeded` carrying tenant
/// metadata triggers work; every other event is acknowledged and ignored.
async fn handle_event(app: &crate::App, event: stripe::Event) -> Result<(), crate::ApiError> {
    if event.type_ != stripe::EventType::SetupIntentSucceeded {
        tracing::debug!(event_type = %event.type_, "ignoring unsubscribed Stripe event");
        return Ok(());
    }

    let stripe::EventObject::SetupIntent(setup_intent) = event.data.object else {
        tracing::warn!("setup_intent.succeeded event did not carry a SetupIntent object");
        return Ok(());
    };

    let Some(tenant) = setup_intent
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get(billing_types::TENANT_METADATA_KEY))
    else {
        // SetupIntents created outside our flow (or before we began stamping the
        // tenant) won't carry it. Nothing to reconcile; ack and move on.
        tracing::info!(
            setup_intent = %setup_intent.id,
            "setup_intent.succeeded without tenant metadata; ignoring"
        );
        return Ok(());
    };

    wake_tenant_controller(&app.pg_pool, tenant).await?;
    tracing::info!(
        %tenant,
        setup_intent = %setup_intent.id,
        "woke tenant controller from setup_intent.succeeded",
    );
    Ok(())
}

/// Wake the tenant's controller so it reconciles the billing change. Mirrors the
/// wake performed by the `setBillingPaymentMethod` mutation; the SQL function
/// lazily creates the controller task on first use.
async fn wake_tenant_controller(pool: &sqlx::PgPool, tenant: &str) -> Result<(), crate::ApiError> {
    sqlx::query!("SELECT internal.wake_tenant_controller($1::TEXT)", tenant)
        .execute(pool)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_server::{self, TestServer};
    use std::collections::HashMap;

    /// Wrap `object` in an `Event` of the given `type_`, with valid IDs. Stripe's
    /// `EventId`/resource-id types reject the empty-string defaults on
    /// deserialization, so tests must supply prefixed ids that round-trip.
    fn event(type_: stripe::EventType, object: stripe::EventObject) -> stripe::Event {
        stripe::Event {
            id: "evt_test".parse().unwrap(),
            type_,
            data: stripe::NotificationEventData {
                object,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// A SetupIntent event object, optionally stamping `tenant` into its metadata
    /// (as `create_setup_intent` does in production).
    fn setup_intent(tenant: Option<&str>) -> stripe::EventObject {
        let metadata = tenant.map(|tenant| {
            HashMap::from([(
                billing_types::TENANT_METADATA_KEY.to_string(),
                tenant.to_string(),
            )])
        });
        stripe::EventObject::SetupIntent(stripe::SetupIntent {
            id: "seti_test".parse().unwrap(),
            metadata,
            ..Default::default()
        })
    }

    /// A `setup_intent.succeeded` event carrying the given tenant metadata.
    fn setup_intent_succeeded(tenant: Option<&str>) -> stripe::Event {
        event(
            stripe::EventType::SetupIntentSucceeded,
            setup_intent(tenant),
        )
    }

    /// Reproduce Stripe's `Stripe-Signature` header: `t=<ts>,v1=<hex hmac>`,
    /// where the HMAC-SHA256 is computed over `"<ts>.<payload>"`.
    fn stripe_signature(secret: &str, timestamp: i64, payload: &str) -> String {
        use hmac::Mac;
        let mut mac =
            hmac::Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes()).expect("valid key");
        mac.update(format!("{timestamp}.{payload}").as_bytes());
        let v1 = hex::encode(mac.finalize().into_bytes());
        format!("t={timestamp},v1={v1}")
    }

    async fn post_webhook(
        server: &TestServer,
        signature: &str,
        payload: &str,
    ) -> reqwest::Response {
        reqwest::Client::new()
            .post(server.base_url().join("/api/v1/stripe/webhook").unwrap())
            .header("Stripe-Signature", signature)
            .body(payload.to_string())
            .send()
            .await
            .expect("webhook request")
    }

    /// Sign `event` with `DEV_WEBHOOK_SECRET` (the secret the test server is
    /// configured with) at the current time, then POST it.
    async fn post_signed(server: &TestServer, event: &stripe::Event) -> reqwest::Response {
        let payload = serde_json::to_string(event).unwrap();
        let timestamp = chrono::Utc::now().timestamp();
        let signature = stripe_signature(DEV_WEBHOOK_SECRET, timestamp, &payload);
        post_webhook(server, &signature, &payload).await
    }

    async fn start(pool: &sqlx::PgPool) -> TestServer {
        TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn setup_intent_succeeded_wakes_tenant_controller(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let tenant = format!("whtest{}", uuid::Uuid::new_v4().simple());
        crate::directives::beta_onboard::provision_test_tenant(
            &pool,
            &tenant,
            &format!("{tenant}@example.test"),
            serde_json::json!({"full_name": "webhook admin"}),
        )
        .await;
        let server = start(&pool).await;
        let tenant_key = format!("{tenant}/");

        let response = post_signed(&server, &setup_intent_succeeded(Some(&tenant_key))).await;
        assert_eq!(response.status(), 200);

        let has_controller: bool = sqlx::query_scalar(
            "SELECT controller_task_id IS NOT NULL FROM tenants WHERE tenant = $1",
        )
        .bind(&tenant_key)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            has_controller,
            "setup_intent.succeeded should have woken (and created) the tenant controller task"
        );
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn rejects_invalid_signature(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let server = start(&pool).await;

        let event = setup_intent_succeeded(Some("acmeCo/"));
        let payload = serde_json::to_string(&event).unwrap();
        let timestamp = chrono::Utc::now().timestamp();
        // Sign with the wrong secret: the HMAC won't verify against the server's.
        let signature = stripe_signature("whsec_not_the_configured_secret", timestamp, &payload);

        let response = post_webhook(&server, &signature, &payload).await;
        assert_eq!(response.status(), 400);
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn ignores_unsubscribed_event(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let server = start(&pool).await;

        // A validly-signed event of a type we don't subscribe to is acked (200)
        // and does no work. The handler short-circuits on the event type before
        // inspecting the object, so any valid object suffices here.
        let unsubscribed = event(stripe::EventType::CustomerUpdated, setup_intent(None));
        let response = post_signed(&server, &unsubscribed).await;
        assert_eq!(response.status(), 200);
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn ignores_setup_intent_without_tenant_metadata(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let server = start(&pool).await;

        // Validly signed, correct event type, but no tenant metadata: nothing to
        // reconcile, so it is acked (200) without touching the database.
        let response = post_signed(&server, &setup_intent_succeeded(None)).await;
        assert_eq!(response.status(), 200);
    }
}
