//! Sandbox HTTP routes. Command execution and its output live in GraphQL (the
//! `sandboxExec` mutation and the `sandboxFileRead` query); this module holds
//! the reset route.
//!
//! It is an ordinary request, so it authenticates through the same
//! [`crate::Envelope`] extractor as every other route. It names its sandbox by
//! id in the path, and [`crate::sandboxes::fetch`] resolves that id against the
//! caller, so a sandbox that is not theirs is simply not found.

use axum::response::IntoResponse;
use std::sync::Arc;

/// Resets the caller's sandbox `id` to its provisioning baseline, discarding
/// every change made since. Responds with `{"ok": true}` on success.
pub(crate) async fn handle_post_sandbox_reset(
    axum::extract::State(app): axum::extract::State<Arc<crate::App>>,
    axum::extract::Path(id): axum::extract::Path<models::Id>,
    env: crate::Envelope,
) -> axum::response::Response {
    let Some(sprites) = app.sprites.clone() else {
        return error_response(
            axum::http::StatusCode::NOT_IMPLEMENTED,
            "Sandboxes are not configured",
        );
    };

    let sandbox = match resolve_sandbox(&app, &env, id).await {
        Ok(sandbox) => sandbox,
        Err(response) => return response,
    };

    match crate::sandboxes::reset(sprites, &sandbox).await {
        Ok(()) => (
            axum::http::StatusCode::OK,
            axum::Json(serde_json::json!({ "ok": true })),
        )
            .into_response(),
        Err(err) => {
            tracing::error!(?err, %sandbox.id, "failed to reset sandbox");
            error_response(
                axum::http::StatusCode::BAD_GATEWAY,
                &format!("failed to reset sandbox: {err:#}"),
            )
        }
    }
}

/// Resolves sandbox `id` for the caller, or the error response that ends the
/// request: an unauthenticated caller, a sandbox that is not theirs (not
/// found), or a database failure.
async fn resolve_sandbox(
    app: &crate::App,
    env: &crate::Envelope,
    id: models::Id,
) -> Result<crate::sandboxes::Sandbox, axum::response::Response> {
    let claims = env
        .claims()
        .map_err(|status| crate::ApiError::from(status).into_response())?;

    match crate::sandboxes::fetch(&app.pg_pool, id, claims.sub).await {
        Ok(Some(sandbox)) => Ok(sandbox),
        Ok(None) => Err(error_response(
            axum::http::StatusCode::NOT_FOUND,
            "sandbox not found",
        )),
        Err(err) => {
            tracing::error!(?err, %id, %claims.sub, "failed to look up sandbox");
            Err(error_response(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "failed to look up sandbox",
            ))
        }
    }
}

fn error_response(status: axum::http::StatusCode, message: &str) -> axum::response::Response {
    (status, axum::Json(serde_json::json!({"error": message}))).into_response()
}
