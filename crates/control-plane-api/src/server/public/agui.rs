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
