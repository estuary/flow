//! `POST /api/v1/agui` — the AG-UI (agentic) protocol endpoint.
//!
//! This handler is the policy boundary: it authenticates the request, applies
//! agentic authorization (stubbed for the spike), selects the LLM provider, and
//! then delegates the entire protocol to the pure `agui` crate. Because the
//! response is an SSE stream, only pre-stream failures (auth, unconfigured
//! provider) surface as plain HTTP errors; anything after the stream begins is
//! reported in-band as an AG-UI `RUN_ERROR` by `agui::run`.

use std::sync::Arc;

pub(crate) async fn handle_agui(
    axum::extract::State(app): axum::extract::State<Arc<crate::App>>,
    env: crate::Envelope,
    axum::extract::Json(input): axum::extract::Json<agui::RunAgentInput>,
) -> Result<impl axum::response::IntoResponse, crate::ApiError> {
    let claims = env.claims()?;
    authorize_agentic(env.snapshot(), claims)?;

    // A missing provider means agentic features aren't configured for this
    // deployment; surface it as a pre-stream error.
    let Some(provider) = app.agui_provider.clone() else {
        return Err(tonic::Status::unavailable("agentic features are not configured").into());
    };

    // Shape the audit trail: who ran which run/thread. Production will attach
    // token-usage accounting to this span from RUN_FINISHED.result.
    tracing::info!(
        user_id = %claims.sub,
        run_id = %input.run_id,
        thread_id = %input.thread_id,
        "starting AG-UI run"
    );

    Ok(agui::sse_response(agui::run(input, provider)))
}

/// Authorization gate for agentic (AG-UI) requests.
///
/// The spike requires authentication only and always authorizes. Production
/// will enforce, in order: a tenant-level agentic disable (a `directive` or
/// `tenants`-table flag consulted through the authorization `Snapshot`, e.g.
/// for HIPAA/GDPR tenants); a prefix capability check that the user may operate
/// agentically within the requested tenant; a reservation-style quota hold for
/// token spend; and a security classifier over the (fully parsed) input
/// messages before dispatch.
fn authorize_agentic(
    _snapshot: &crate::Snapshot,
    _claims: &crate::ControlClaims,
) -> Result<(), crate::ApiError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::test_server;

    /// A minimal but complete `RunAgentInput`. The `forwardedProps._mock` script
    /// drives the `MockProvider` deterministically (text -> tool call -> finish),
    /// and message ids derive from `runId`, so the resulting SSE body is stable.
    fn mock_run_input() -> serde_json::Value {
        serde_json::json!({
            "threadId": "t-agui",
            "runId": "r-agui",
            "state": {},
            "messages": [{"id": "u1", "role": "user", "content": "hi"}],
            "tools": [],
            "context": [],
            "forwardedProps": {
                "_mock": [
                    {"text": "Hello world"},
                    {"toolCall": {"name": "get_weather", "args": "{\"location\":\"Boston\"}"}},
                    {"finish": {"stopReason": "tool_use"}}
                ]
            }
        })
    }

    /// Without a bearer token the `Envelope` yields unauthenticated claims, which
    /// this authenticated endpoint rejects before any stream begins. The
    /// `unauthenticated` gRPC status maps to HTTP 401.
    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn agui_unauthenticated_is_rejected(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;

        let response = reqwest::Client::new()
            .post(format!("http://{}/api/v1/agui", server.addr))
            .header("accept", "text/event-stream")
            .json(&mock_run_input())
            .send()
            .await
            .unwrap();

        assert_eq!(response.status().as_u16(), 401);
    }

    /// A valid Supabase-style JWT passes `Envelope` auth and the agentic gate,
    /// selects the mock provider, and streams a well-formed AG-UI SSE response
    /// end-to-end through the real router.
    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn agui_authenticated_streams_sse(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;

        // `make_access_token` mints a self-signed ControlClaims JWT; no user row
        // is required because the endpoint only verifies the token signature and
        // `aud`, not catalog grants.
        let token = server.make_access_token(
            uuid::Uuid::from_bytes([0x11; 16]),
            Some("alice@example.com"),
        );

        let response = reqwest::Client::new()
            .post(format!("http://{}/api/v1/agui", server.addr))
            .header("accept", "text/event-stream")
            .bearer_auth(&token)
            .json(&mock_run_input())
            .send()
            .await
            .unwrap();

        assert_eq!(response.status().as_u16(), 200);
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("text/event-stream"),
        );

        // The mock stream is finite, so `text()` reads the whole SSE body.
        let raw = response.text().await.unwrap();
        insta::assert_snapshot!(raw);
    }
}
