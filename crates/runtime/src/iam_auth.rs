use anyhow::Context;
use std::time::Duration;

/// IAM authentication configuration extracted from connector config
#[derive(Debug, Clone)]
pub enum IAMAuthConfig {
    AWS(AWSConfig),
    GCP(GCPConfig),
}

/// AWS-specific configuration
#[derive(Debug, Clone)]
pub struct AWSConfig {
    pub role_arn: String,
    pub external_id: Option<String>,
    pub region: Option<String>,
}

/// GCP-specific configuration
#[derive(Debug, Clone)]
pub struct GCPConfig {
    pub service_account_email: String,
    pub project_id: Option<String>,
}

/// Generated short-lived tokens
#[derive(Debug, Clone)]
pub enum IAMTokens {
    AWS(AWSTokens),
    GCP(GCPTokens),
}

#[derive(Debug, Clone)]
pub struct AWSTokens {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
    pub region: String,
}

#[derive(Debug, Clone)]
pub struct GCPTokens {
    pub access_token: String,
    pub project_id: String,
}

impl IAMAuthConfig {
    /// Generate short-lived tokens for the configured IAM provider
    pub async fn generate_tokens(&self) -> anyhow::Result<IAMTokens> {
        match self {
            IAMAuthConfig::AWS(aws_config) => {
                let aws_tokens = generate_aws_tokens(aws_config).await?;
                Ok(IAMTokens::AWS(aws_tokens))
            }
            IAMAuthConfig::GCP(gcp_config) => {
                let gcp_tokens = generate_gcp_tokens(gcp_config).await?;
                Ok(IAMTokens::GCP(gcp_tokens))
            }
        }
    }

    /// Convert tokens to environment variables for the container
    pub fn tokens_to_env_vars(&self, tokens: &IAMTokens) -> Vec<String> {
        match tokens {
            IAMTokens::AWS(aws_tokens) => {
                vec![
                    format!("--env=AWS_ACCESS_KEY_ID={}", aws_tokens.access_key_id),
                    format!(
                        "--env=AWS_SECRET_ACCESS_KEY={}",
                        aws_tokens.secret_access_key
                    ),
                    format!("--env=AWS_SESSION_TOKEN={}", aws_tokens.session_token),
                    format!("--env=AWS_DEFAULT_REGION={}", aws_tokens.region),
                ]
            }
            IAMTokens::GCP(gcp_tokens) => {
                vec![
                    format!("--env=GOOGLE_CLOUD_PROJECT={}", gcp_tokens.project_id),
                    format!("--env=GOOGLE_ACCESS_TOKEN={}", gcp_tokens.access_token),
                ]
            }
        }
    }
}

/// Generate AWS temporary credentials using STS AssumeRole
async fn generate_aws_tokens(config: &AWSConfig) -> anyhow::Result<AWSTokens> {
    use aws_config::Region;

    let mut aws_config_builder = aws_config::defaults(aws_config::BehaviorVersion::latest());

    if let Some(ref region_str) = config.region {
        let region = Region::new(region_str.clone());
        aws_config_builder = aws_config_builder.region(region);
    }

    let aws_config = aws_config_builder.load().await;

    let sts_client = aws_sdk_sts::Client::new(&aws_config);

    let mut assume_role_request = sts_client
        .assume_role()
        .role_arn(&config.role_arn)
        .role_session_name(&format!(
            "flow-connector-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs()
        ))
        .duration_seconds(3600); // 1 hour maximum duration for connectors

    if let Some(external_id) = &config.external_id {
        assume_role_request = assume_role_request.external_id(external_id);
    }

    let response = assume_role_request
        .send()
        .await
        .context("Failed to assume AWS role")?;

    let credentials = response
        .credentials()
        .context("No credentials returned from STS AssumeRole")?;

    Ok(AWSTokens {
        access_key_id: credentials.access_key_id().to_string(),
        secret_access_key: credentials.secret_access_key().to_string(),
        session_token: credentials.session_token().to_string(),
        region: config
            .region
            .clone()
            .unwrap_or_else(|| "us-east-1".to_string()),
    })
}

/// Generate GCP access token using service account impersonation
async fn generate_gcp_tokens(config: &GCPConfig) -> anyhow::Result<GCPTokens> {
    // For now, use a simplified approach that relies on gcloud CLI or ADC
    // This could be enhanced later with direct API calls

    // Use the Google Cloud Auth library to get credentials
    let scopes = &["https://www.googleapis.com/auth/cloud-platform"];

    // If we have a specific service account, we should impersonate it
    let access_token = if config
        .service_account_email
        .ends_with(".iam.gserviceaccount.com")
    {
        // First get a token from the default credentials
        let default_token = get_default_gcp_token().await?;
        // Use IAM Service Account Credentials API for impersonation
        impersonate_service_account(&config.service_account_email, &default_token, scopes).await?
    } else {
        get_default_gcp_token().await?
    };

    Ok(GCPTokens {
        access_token,
        project_id: config
            .project_id
            .clone()
            .unwrap_or_else(|| std::env::var("GOOGLE_CLOUD_PROJECT").unwrap_or_default()),
    })
}

/// Get default GCP access token from Application Default Credentials
async fn get_default_gcp_token() -> anyhow::Result<String> {
    // Try to get token from metadata service (when running on GCP)
    if let Ok(token) = get_gcp_metadata_token().await {
        return Ok(token);
    }

    // Try to get token from service account key file
    if let Ok(token) = get_gcp_service_account_token().await {
        return Ok(token);
    }

    anyhow::bail!(
        "No valid GCP credentials found. Expected metadata service, or service account key"
    )
}

/// Impersonate a GCP service account to generate access token
async fn impersonate_service_account(
    service_account_email: &str,
    access_token: &str,
    scopes: &[&str],
) -> anyhow::Result<String> {
    use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
    use serde_json::json;

    let client = reqwest::Client::new();
    let url = format!(
        "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{}:generateAccessToken",
        service_account_email
    );

    let body = json!({
        "scope": scopes,
        "lifetime": "3600s" // 1 hour
    });

    let response = client
        .post(&url)
        .header(AUTHORIZATION, format!("Bearer {}", access_token))
        .header(CONTENT_TYPE, "application/json")
        .json(&body)
        .send()
        .await
        .context("Failed to call GCP impersonation API")?;

    if !response.status().is_success() {
        let error_text = response.text().await.unwrap_or_default();
        anyhow::bail!("GCP impersonation failed: {}", error_text);
    }

    let response_json: serde_json::Value = response
        .json()
        .await
        .context("Failed to parse GCP impersonation response")?;

    response_json
        .get("accessToken")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .context("Missing accessToken in GCP impersonation response")
}

/// Get GCP access token from metadata service (when running on GCP instances)
async fn get_gcp_metadata_token() -> anyhow::Result<String> {
    use reqwest::header::{HeaderName, HeaderValue};

    let client = reqwest::Client::new();
    let url = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";

    let response = client
        .get(url)
        .header(
            HeaderName::from_static("metadata-flavor"),
            HeaderValue::from_static("Google"),
        )
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .context("Failed to call GCP metadata service")?;

    if !response.status().is_success() {
        anyhow::bail!(
            "GCP metadata service returned status: {}",
            response.status()
        );
    }

    let response_json: serde_json::Value = response
        .json()
        .await
        .context("Failed to parse GCP metadata response")?;

    response_json
        .get("access_token")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .context("Missing access_token in GCP metadata response")
}

/// Get GCP access token from service account key file
async fn get_gcp_service_account_token() -> anyhow::Result<String> {
    use serde_json::Value;

    // Try to load service account key from environment variable or file
    let key_path = std::env::var("GOOGLE_APPLICATION_CREDENTIALS")
        .context("no GOOGLE_APPLICATION_CREDENTIALS found")?;
    let key_json = std::fs::read_to_string(&key_path)
        .with_context(|| format!("Failed to read service account key file: {}", key_path))?;

    let key_data: Value =
        serde_json::from_str(&key_json).context("Failed to parse service account key JSON")?;

    let client_email = key_data
        .get("client_email")
        .and_then(|v| v.as_str())
        .context("Missing client_email in service account key")?;

    let private_key = key_data
        .get("private_key")
        .and_then(|v| v.as_str())
        .context("Missing private_key in service account key")?;

    // Create JWT for OAuth 2.0 service account flow
    let token = create_service_account_jwt_token(client_email, private_key).await?;

    Ok(token)
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
        .context("Failed to parse RSA private key")?;

    let jwt = encode(&header, &claims, &encoding_key).context("Failed to create JWT")?;

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
        .context("Failed to exchange JWT for access token")?;

    if !response.status().is_success() {
        let error_text = response.text().await.unwrap_or_default();
        anyhow::bail!("OAuth token exchange failed: {}", error_text);
    }

    let response_json: serde_json::Value = response
        .json()
        .await
        .context("Failed to parse OAuth response")?;

    response_json
        .get("access_token")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .context("Missing access_token in OAuth response")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iam_auth_config_creation() {
        let aws_config = IAMAuthConfig::AWS(AWSConfig {
            role_arn: "arn:aws:iam::123456789012:role/FlowConnectorRole".to_string(),
            external_id: Some("unique-external-id".to_string()),
            region: Some("us-west-2".to_string()),
        });

        match aws_config {
            IAMAuthConfig::AWS(config) => {
                assert_eq!(
                    config.role_arn,
                    "arn:aws:iam::123456789012:role/FlowConnectorRole"
                );
            }
            _ => panic!("Expected AWS config"),
        }
    }

    #[test]
    fn test_tokens_to_env_vars() {
        let config = IAMAuthConfig::AWS(AWSConfig {
            role_arn: "arn:aws:iam::123456789012:role/FlowConnectorRole".to_string(),
            external_id: None,
            region: Some("us-east-1".to_string()),
        });

        let tokens = IAMTokens::AWS(AWSTokens {
            access_key_id: "AKIA...".to_string(),
            secret_access_key: "secret...".to_string(),
            session_token: "session...".to_string(),
            region: "us-east-1".to_string(),
        });

        let env_vars = config.tokens_to_env_vars(&tokens);
        assert_eq!(env_vars.len(), 4);
        assert!(env_vars
            .iter()
            .any(|v| v.starts_with("--env=AWS_ACCESS_KEY_ID=")));
        assert!(env_vars
            .iter()
            .any(|v| v.starts_with("--env=AWS_SECRET_ACCESS_KEY=")));
        assert!(env_vars
            .iter()
            .any(|v| v.starts_with("--env=AWS_SESSION_TOKEN=")));
        assert!(env_vars
            .iter()
            .any(|v| v.starts_with("--env=AWS_DEFAULT_REGION=")));
    }
}
