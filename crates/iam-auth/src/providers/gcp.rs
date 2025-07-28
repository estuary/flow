use anyhow::Context;
use zeroize::Zeroize;

use crate::config::GCPConfig;
use crate::tokens::GCPTokens;

/// Generate GCP access token using 3-step service account impersonation:
/// 1. Sign JWT for runtime service account with task_name
/// 2. Exchange JWT for access token using OAuth 2.0 token exchange
/// 3. Use the exchanged token to impersonate the target service account
pub async fn generate_tokens(config: &GCPConfig, task_name: &str) -> anyhow::Result<GCPTokens> {
    // Google presents the audience with https:, so we strip that if it exists
    let aud = config
        .gcp_workload_identity_pool_audience
        .strip_prefix("https:")
        .unwrap_or(&config.gcp_workload_identity_pool_audience);

    // Get the data plane name to use as subject
    let data_plane_fqdn = std::env::var("FLOW_DATA_PLANE_FQDN")
        .context("FLOW_DATA_PLANE_FQDN environment variable not set")?;

    if data_plane_fqdn.is_empty() {
        anyhow::bail!("FLOW_DATA_PLANE_FQDN environment variable is empty");
    }

    // Step 1: Sign a JWT using the default runtime service account with data plane name as subject
    let mut signed_jwt = google_sign_jwt(task_name, &data_plane_fqdn, aud).await?;

    // Step 2: Exchange the signed JWT for an access token via OAuth 2.0 token exchange
    let mut exchanged_token = exchange_jwt_for_service_account_token(&signed_jwt, aud).await?;

    signed_jwt.zeroize();

    // Step 3: Use the exchanged access token to impersonate the target service account
    let impersonated_token =
        impersonate_service_account(&exchanged_token, &config.gcp_service_account_to_impersonate)
            .await?;

    exchanged_token.zeroize();

    Ok(GCPTokens {
        access_token: impersonated_token,
    })
}

/// Get GCP access token from service account credentials JSON
async fn get_gcp_token_from_credentials(credentials_json: &str) -> anyhow::Result<String> {
    use serde_json::Value;

    let key_data: Value = serde_json::from_str(credentials_json)
        .context("failed to parse service account key JSON")?;

    let client_email = key_data
        .get("client_email")
        .and_then(|v| v.as_str())
        .context("missing client_email in service account key")?;

    let private_key = key_data
        .get("private_key")
        .and_then(|v| v.as_str())
        .context("missing private_key in service account key")?;

    create_service_account_jwt_token(client_email, private_key).await
}

/// Sign a JWT using Google's signJWT API with configurable subject and audience
pub async fn google_sign_jwt(task_name: &str, subject: &str, audience: &str) -> anyhow::Result<String> {
    let credentials_path = std::env::var("GOOGLE_APPLICATION_CREDENTIALS")
        .context("GOOGLE_APPLICATION_CREDENTIALS environment variable not set")?;

    if credentials_path.is_empty() {
        anyhow::bail!("GOOGLE_APPLICATION_CREDENTIALS environment variable is empty");
    }

    let mut credentials_json = tokio::fs::read_to_string(&credentials_path)
        .await
        .with_context(|| {
            format!(
                "Failed to read Google Cloud credentials from {}",
                credentials_path
            )
        })?;

    if credentials_json.trim().is_empty() {
        anyhow::bail!("Google Cloud credentials file is empty");
    }

    // Parse the credentials to get the runtime service account email
    let key_data: serde_json::Value = serde_json::from_str(&credentials_json)
        .context("failed to parse service account key JSON")?;
    let runtime_service_account_email = key_data
        .get("client_email")
        .and_then(|v| v.as_str())
        .context("missing client_email in service account key")?;

    // Get a token from the root credentials to authenticate with IAM API
    let mut runtime_token = get_gcp_token_from_credentials(&credentials_json).await?;

    credentials_json.zeroize();

    use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
    use serde_json::json;

    let data_plane_fqdn = std::env::var("FLOW_DATA_PLANE_FQDN")
        .context("FLOW_DATA_PLANE_FQDN environment variable not set")?;

    if data_plane_fqdn.is_empty() {
        anyhow::bail!("FLOW_DATA_PLANE_FQDN environment variable is empty");
    }

    let issuer = format!("https://openid.estuary.dev/{}/", data_plane_fqdn);

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as u64;

    let jwt_payload = json!({
        "iss": issuer,
        "sub": subject,
        "aud": audience,
        "iat": now,
        "exp": now + 3600,
        "task_name": task_name
    });

    let client = reqwest::Client::new();
    let url = format!(
        "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{}:signJwt",
        runtime_service_account_email
    );

    let body = json!({
        "payload": serde_json::to_string(&jwt_payload)?,
        "delegates": []
    });

    let response = client
        .post(&url)
        .header(AUTHORIZATION, format!("Bearer {}", runtime_token))
        .header(CONTENT_TYPE, "application/json")
        .json(&body)
        .send()
        .await
        .context("failed to call GCP signJwt API")?;

    runtime_token.zeroize();

    if !response.status().is_success() {
        let error_text = response.text().await.unwrap_or_default();
        anyhow::bail!("GCP signJwt failed: {}", error_text);
    }

    let response_json: serde_json::Value = response
        .json()
        .await
        .context("failed to parse GCP signJwt response")?;

    response_json
        .get("signedJwt")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .context("missing signedJwt in GCP signJwt response")
}

/// Exchange a JWT for a service account token using OAuth 2.0 token exchange
async fn exchange_jwt_for_service_account_token(
    jwt: &str,
    workload_identity_pool_audience: &str,
) -> anyhow::Result<String> {
    use reqwest::header::CONTENT_TYPE;
    use std::collections::HashMap;

    let client = reqwest::Client::new();

    let mut params = HashMap::new();
    params.insert("audience", workload_identity_pool_audience);
    params.insert(
        "grant_type",
        "urn:ietf:params:oauth:grant-type:token-exchange",
    );
    params.insert("subject_token_type", "urn:ietf:params:oauth:token-type:jwt");
    params.insert("subject_token", jwt);
    params.insert(
        "requested_token_type",
        "urn:ietf:params:oauth:token-type:access_token",
    );
    params.insert("scope", "https://www.googleapis.com/auth/cloud-platform");

    let response = client
        .post("https://sts.googleapis.com/v1/token")
        .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
        .form(&params)
        .send()
        .await
        .context("failed to call OAuth token exchange")?;

    if !response.status().is_success() {
        let error_text = response.text().await.unwrap_or_default();
        anyhow::bail!("OAuth token exchange failed: {}", error_text);
    }

    let response_json: serde_json::Value = response
        .json()
        .await
        .context("failed to parse OAuth token exchange response")?;

    response_json
        .get("access_token")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .context("missing access_token in OAuth token exchange response")
}

/// Impersonate a service account using the generateAccessToken API
async fn impersonate_service_account(
    access_token: &str,
    target_service_account: &str,
) -> anyhow::Result<String> {
    use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
    use serde_json::json;

    let client = reqwest::Client::new();
    let url = format!(
        "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{}:generateAccessToken",
        target_service_account
    );

    let body = json!({
        "scope": ["https://www.googleapis.com/auth/cloud-platform"],
        "delegates": [],
        "lifetime": "3600s" // 12 hours
    });

    let response = client
        .post(&url)
        .header(AUTHORIZATION, format!("Bearer {}", access_token))
        .header(CONTENT_TYPE, "application/json")
        .json(&body)
        .send()
        .await
        .context("failed to call GCP generateAccessToken API")?;

    if !response.status().is_success() {
        let error_text = response.text().await.unwrap_or_default();
        anyhow::bail!("GCP service account impersonation failed: {}", error_text);
    }

    let response_json: serde_json::Value = response
        .json()
        .await
        .context("failed to parse GCP generateAccessToken response")?;

    response_json
        .get("accessToken")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .context("missing accessToken in GCP generateAccessToken response")
}

/// Create JWT token and exchange it for an access token using OAuth 2.0 service account flow
async fn create_service_account_jwt_token(
    client_email: &str,
    private_key: &str,
) -> anyhow::Result<String> {
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    struct Claims {
        iss: String,
        scope: String,
        aud: String,
        exp: usize,
        iat: usize,
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as usize;

    let claims = Claims {
        iss: client_email.to_string(),
        scope: "https://www.googleapis.com/auth/cloud-platform".to_string(),
        aud: "https://oauth2.googleapis.com/token".to_string(),
        exp: now + 3600, // Expire in 1 hour
        iat: now,
    };

    let header = Header::new(Algorithm::RS256);
    let encoding_key = EncodingKey::from_rsa_pem(private_key.as_bytes())
        .context("failed to parse RSA private key")?;

    let mut jwt = encode(&header, &claims, &encoding_key).context("failed to create JWT")?;

    // Exchange JWT for access token
    let client = reqwest::Client::new();
    let params = [
        ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
        ("assertion", &jwt),
    ];

    let response = client
        .post("https://oauth2.googleapis.com/token")
        .form(&params)
        .send()
        .await
        .context("failed to exchange JWT for access token")?;

    jwt.zeroize();

    if !response.status().is_success() {
        let error_text = response.text().await.unwrap_or_default();
        anyhow::bail!("OAuth token exchange failed: {}", error_text);
    }

    let response_json: serde_json::Value = response
        .json()
        .await
        .context("failed to parse OAuth response")?;

    response_json
        .get("access_token")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .context("missing access_token in OAuth response")
}