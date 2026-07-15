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

use stripe::WebhookError;

/// Handle a Stripe webhook delivery: verify the signature, then wake the tenant
/// controller on `setup_intent.succeeded`. Returns `200` for both handled and
/// intentionally-ignored events (so Stripe stops retrying), and `400` when the
/// signature does not verify, with the exception of if the event doesn't parse
/// it could mean that they updated an event so in that case we also return a
/// 200 and ignore the event.
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
        tracing::warn!(
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

    let event = match stripe::Webhook::construct_event(payload, signature, secret) {
        Err(err) => {
            tracing::debug!(?err, "rejected Stripe webhook with an invalid signature");
            match err {
                WebhookError::BadParse(inner) => {
                    tracing::error!(?inner, "Failed to parse stripe event");
                    return Ok(axum::http::StatusCode::OK);
                }
                _ => (),
            }
            return Err(tonic::Status::invalid_argument("invalid webhook signature").into());
        }
        Ok(value) => value,
    };

    handle_event(&app, event).await?;
    Ok(axum::http::StatusCode::OK)
}

/// Act on a verified event. Only `setup_intent.succeeded` carrying tenant
/// metadata triggers work; every other event is acknowledged and ignored.
async fn handle_event(app: &crate::App, event: stripe::Event) -> Result<(), crate::ApiError> {
    if event.type_ != stripe::EventType::SetupIntentSucceeded {
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

    // `wake_tenant_controller` no-ops for an unknown tenant, so a `setup_intent`
    // whose metadata names a tenant that no longer exists is harmless here.
    match crate::server::wake_tenant_controller(&app.pg_pool, tenant).await {
        Ok(made_wait_for_controller) => {
            if !made_wait_for_controller {
                tracing::warn!(
                    ?tenant,
                    "Failed to wake tenant controller, tenant not found"
                );
                return Ok(());
            }
        }
        Err(err) => {
            tracing::warn!(
                ?err,
                "Received an error message while trying to wait tenant controller"
            );
            return Ok(());
        }
    };
    tracing::info!(
        %tenant,
        setup_intent = %setup_intent.id,
        "woke tenant controller from setup_intent.succeeded",
    );
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::test_server::{self, TestServer};

    /// Webhook signing secret used by the test suite. `test_server` configures
    /// the app with this value so tests can sign payloads against a stable,
    /// committed secret. It is test-only: nothing wires it into the running
    /// `agent` binary (which requires `STRIPE_WEBHOOK_SECRET` and otherwise fails
    /// closed — see `crate::App::stripe_webhook_secret`), and it is world-readable
    /// here, so it must never be treated as a production secret.
    pub(crate) const DEV_WEBHOOK_SECRET: &str =
        "whsec_0c1ef79638919be66761253b6f15896fa30ddd38fcff54c85b66de122ae36933";

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
        let metadata = tenant.map(billing_types::tenant_metadata);
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

    /// Count the `{"type":"wake"}` messages sitting in the tenant controller's
    /// task inbox. `send_to_task` appends `[from_id, message]` to
    /// `internal.tasks.inbox`, so we unnest the inbox and match the message's
    /// `type`. Returns 0 when the tenant has no controller task yet.
    async fn controller_wake_count(pool: &sqlx::PgPool, tenant: &str) -> i64 {
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*) FILTER (WHERE elem -> 1 ->> 'type' = 'wake')
            FROM tenants te
            LEFT JOIN internal.tasks t ON t.task_id = te.controller_task_id
            LEFT JOIN LATERAL unnest(t.inbox) AS elem ON true
            WHERE te.tenant = $1
            "#,
        )
        .bind(tenant)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn setup_intent_succeeded_wakes_tenant_controller(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let tenant = format!("whtest{}", uuid::Uuid::new_v4().simple());
        crate::server::public::graphql::billing::test_util::provision_test_tenant(&pool, &tenant)
            .await;
        let server = start(&pool).await;
        let tenant_key = format!("{tenant}/");

        // Capture the wake count before the webhook so the assertion measures the
        // wake this webhook enqueues, independent of any wake that provisioning
        // (or its DB triggers) may already have delivered.
        let wakes_before = controller_wake_count(&pool, &tenant_key).await;

        let response = post_signed(&server, &setup_intent_succeeded(Some(&tenant_key))).await;
        assert_eq!(response.status(), 200);

        let wakes_after = controller_wake_count(&pool, &tenant_key).await;
        assert_eq!(
            wakes_after,
            wakes_before + 1,
            "setup_intent.succeeded should enqueue exactly one wake message \
             (before={wakes_before}, after={wakes_after})"
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
        // and does no work. Use a realistic customer.updated payload (a Customer
        // object), which is what Stripe would actually deliver for this type.
        let customer = stripe::EventObject::Customer(stripe::Customer {
            id: "cus_test".parse().unwrap(),
            ..Default::default()
        });
        let unsubscribed = event(stripe::EventType::CustomerUpdated, customer);
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

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn unknown_tenant_does_not_create_controller(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let server = start(&pool).await;

        // A validly-signed event naming a well-formed tenant that does not exist
        // is acked (200) but must NOT create an orphan controller task: the wake
        // is gated on the tenant existing. TENANT_CONTROLLER is task_type 12.
        let response = post_signed(&server, &setup_intent_succeeded(Some("ghosttenant/"))).await;
        assert_eq!(response.status(), 200);

        let controller_tasks: i64 = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM internal.tasks WHERE task_type = 12",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            controller_tasks, 0,
            "no controller task should be created for an unknown tenant"
        );
    }
}
