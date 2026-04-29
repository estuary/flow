use std::sync::Arc;

#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct SessionResponse {
    pub session_token: String,
    pub expires_at: String,
}

#[axum::debug_handler(state = Arc<crate::App>)]
pub(crate) async fn handle_create_kapa_session(
    axum::extract::State(app): axum::extract::State<Arc<crate::App>>,
    env: crate::Envelope,
) -> Result<axum::Json<SessionResponse>, crate::ApiError> {
    let _claims = env.claims()?;

    let kapa = app.kapa_config.as_ref().ok_or_else(|| {
        tonic::Status::not_found("Kapa integration is not configured")
    })?;

    let url = format!(
        "https://api.kapa.ai/agent/v1/projects/{}/agent/sessions/",
        kapa.project_id,
    );

    let response = reqwest::Client::new()
        .post(&url)
        .header("X-API-Key", &kapa.api_key)
        .send()
        .await
        .map_err(|err| {
            tracing::error!(%err, "failed to create Kapa session");
            tonic::Status::internal("failed to create Kapa session")
        })?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        tracing::error!(%status, %body, "Kapa session API returned an error");
        return Err(tonic::Status::internal("Kapa session creation failed").into());
    }

    let session: SessionResponse = response.json().await.map_err(|err| {
        tracing::error!(%err, "failed to parse Kapa session response");
        tonic::Status::internal("failed to parse Kapa session response")
    })?;

    Ok(axum::Json(session))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_response_deserialize() {
        let json = r#"{
            "session_token": "NRd60UqDpLSeeIFzCfmj5dxiRDOJL8G7aSXVzpQ0pPusbe9kHIjEymznutrJu6uf",
            "expires_at": "2026-03-17T09:54:51.165812Z"
        }"#;
        let session: SessionResponse = serde_json::from_str(json).unwrap();
        assert_eq!(session.session_token, "NRd60UqDpLSeeIFzCfmj5dxiRDOJL8G7aSXVzpQ0pPusbe9kHIjEymznutrJu6uf");
        assert_eq!(session.expires_at, "2026-03-17T09:54:51.165812Z");
    }
}
