use anyhow::Context;
use zeroize::Zeroize;

use crate::config::AzureConfig;
use crate::providers::gcp::google_sign_jwt;
use crate::tokens::AzureTokens;

/// Generate Azure access token using 2-step workload identity federation with Google as external provider:
/// 1. Sign JWT using Google's signJWT API with task_name as subject
/// 2. Exchange signed JWT for target App Registration access token
pub async fn generate_tokens(
    config: &AzureConfig,
    task_name: &str,
) -> anyhow::Result<AzureTokens> {
    // Step 1: Sign JWT using Google's signJWT API with task_name as subject
    let mut signed_jwt =
        google_sign_jwt(task_name, task_name, "api://AzureADTokenExchange").await?;

    // Step 2: Exchange signed JWT for target App Registration access token
    let access_token = exchange_azure_jwt_for_app_registration_token(
        &signed_jwt,
        &config.azure_tenant_id,
        &config.azure_client_id,
    )
    .await?;

    signed_jwt.zeroize();

    Ok(AzureTokens { access_token })
}

/// Exchange signed JWT for target App Registration access token
async fn exchange_azure_jwt_for_app_registration_token(
    jwt_token: &str,
    tenant_id: &str,
    target_client_id: &str,
) -> anyhow::Result<String> {
    use reqwest::header::CONTENT_TYPE;
    use std::collections::HashMap;

    let client = reqwest::Client::new();

    let mut params = HashMap::new();
    params.insert("grant_type", "client_credentials");
    params.insert(
        "client_assertion_type",
        "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
    );
    params.insert("client_assertion", jwt_token);
    params.insert("client_id", target_client_id);
    params.insert("scope", "https://graph.microsoft.com/.default");

    let response = client
        .post(&format!(
            "https://login.microsoftonline.com/{}/oauth2/v2.0/token",
            tenant_id
        ))
        .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
        .form(&params)
        .send()
        .await
        .context("failed to call Azure App Registration token exchange")?;

    if !response.status().is_success() {
        let error_text = response.text().await.unwrap_or_default();
        anyhow::bail!(
            "Azure App Registration token exchange failed: {}",
            error_text
        );
    }

    let response_json: serde_json::Value = response
        .json()
        .await
        .context("failed to parse Azure App Registration token response")?;

    response_json
        .get("access_token")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .context("missing access_token in Azure App Registration token response")
}