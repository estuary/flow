//! Standalone, no-auth AG-UI server for local testing and the TS interop
//! harness.
//!
//! Provider selection is by environment:
//! - `ANTHROPIC_API_KEY` set -> real Anthropic backend (with optional
//!   `ANTHROPIC_BASE_URL` override),
//! - otherwise -> the deterministic mock provider.
//!
//! Listens on `127.0.0.1:$PORT` (default 8137) and serves `POST /agui`.

use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let provider: Arc<dyn agui::Provider> = match std::env::var("ANTHROPIC_API_KEY") {
        Ok(key) if !key.is_empty() => {
            let mut provider = agui::AnthropicProvider::new(key);
            if let Ok(base_url) = std::env::var("ANTHROPIC_BASE_URL") {
                provider = provider.with_base_url(base_url);
            }
            Arc::new(provider)
        }
        _ => Arc::new(agui::MockProvider),
    };

    let router = axum::Router::new()
        .route("/agui", axum::routing::post(handle_agui))
        .with_state(provider);

    let port = std::env::var("PORT")
        .ok()
        .and_then(|port| port.parse::<u16>().ok())
        .unwrap_or(8137);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
    println!(
        "agui example server listening on http://{}",
        listener.local_addr()?
    );

    axum::serve(listener, router).await?;
    Ok(())
}

async fn handle_agui(
    axum::extract::State(provider): axum::extract::State<Arc<dyn agui::Provider>>,
    axum::extract::Json(input): axum::extract::Json<agui::RunAgentInput>,
) -> impl axum::response::IntoResponse {
    agui::sse_response(agui::run(input, provider))
}
