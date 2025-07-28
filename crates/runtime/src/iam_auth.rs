use anyhow::Context;
use aws_sdk_sts::error::ProvideErrorMetadata;
use serde::{Deserialize, Serialize};
use zeroize::Zeroize;

/// IAM authentication configuration extracted from connector config
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IAMAuthConfig {
    AWS(AWSConfig),
    GCP(GCPConfig),
    Azure(AzureConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AWSConfig {
    pub aws_role_arn: String,
    pub aws_region: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GCPConfig {
    pub gcp_service_account_to_impersonate: String,
    pub gcp_workload_identity_pool_audience: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureConfig {
    pub azure_client_id: String,
    pub azure_tenant_id: String,
}

/// Generated short-lived tokens which will be injected into connector config
#[derive(Debug, Clone, Zeroize)]
pub enum IAMTokens {
    AWS(AWSTokens),
    GCP(GCPTokens),
    Azure(AzureTokens),
}

#[derive(Debug, Clone, Zeroize)]
pub struct AWSTokens {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
}

#[derive(Debug, Clone, Zeroize)]
pub struct GCPTokens {
    pub access_token: String,
}

#[derive(Debug, Clone, Zeroize)]
pub struct AzureTokens {
    pub access_token: String,
}

impl IAMTokens {
    pub fn inject_into(&self, config: &mut String) -> anyhow::Result<()> {
        let mut parsed = serde_json::from_str::<serde_json::Value>(&config)?;

        let credentials = parsed
            .as_object_mut()
            .unwrap()
            .get_mut("credentials")
            .unwrap()
            .as_object_mut()
            .unwrap();

        match self {
            IAMTokens::AWS(AWSTokens {
                access_key_id,
                secret_access_key,
                session_token,
            }) => {
                credentials.insert(
                    "aws_access_key_id".to_string(),
                    serde_json::Value::String(access_key_id.clone()),
                );
                credentials.insert(
                    "aws_secret_access_key".to_string(),
                    serde_json::Value::String(secret_access_key.clone()),
                );
                credentials.insert(
                    "aws_session_token".to_string(),
                    serde_json::Value::String(session_token.clone()),
                );
            }
            IAMTokens::GCP(GCPTokens { access_token }) => {
                credentials.insert(
                    "gcp_access_token".to_string(),
                    serde_json::Value::String(access_token.clone()),
                );
            }
            IAMTokens::Azure(AzureTokens { access_token }) => {
                credentials.insert(
                    "azure_access_token".to_string(),
                    serde_json::Value::String(access_token.clone()),
                );
            }
        }

        *config = serde_json::to_string(&parsed)?;

        Ok(())
    }
}

impl IAMAuthConfig {
    /// Generate short-lived tokens for the configured IAM provider
    pub async fn generate_tokens(&self, task_name: &str) -> anyhow::Result<IAMTokens> {
        match self {
            IAMAuthConfig::AWS(aws_config) => {
                let aws_tokens = generate_aws_tokens(aws_config, task_name).await?;
                Ok(IAMTokens::AWS(aws_tokens))
            }
            IAMAuthConfig::GCP(gcp_config) => {
                let gcp_tokens = generate_gcp_tokens(gcp_config, task_name).await?;
                Ok(IAMTokens::GCP(gcp_tokens))
            }
            IAMAuthConfig::Azure(azure_config) => {
                let azure_tokens = generate_azure_tokens(azure_config, task_name).await?;
                Ok(IAMTokens::Azure(azure_tokens))
            }
        }
    }
}

/// Generate AWS temporary credentials using STS AssumeRoleWithWebIdentity
async fn generate_aws_tokens(config: &AWSConfig, task_name: &str) -> anyhow::Result<AWSTokens> {
    use aws_config::Region;

    // Step 1: Sign JWT using Google's signJWT API with task_name as subject
    let mut signed_jwt = google_sign_jwt(task_name, task_name, &config.aws_role_arn).await?;

    // Step 2: Use the signed JWT with AssumeRoleWithWebIdentity
    let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(config.aws_region.clone()))
        .load()
        .await;

    let sts_client = aws_sdk_sts::Client::new(&aws_config);

    // Sanitize and truncate task_name to fit within AWS role session name limit of 64 chars
    // AWS role session names must match pattern [\w+=,.@-]*
    // Format: "flow.{task_name}@{timestamp}" - keeping task_name <= 48 chars
    let sanitized_task_name = task_name.replace('/', ".");
    let truncated_task_name = if sanitized_task_name.len() > 48 {
        &sanitized_task_name[sanitized_task_name.len() - 48..]
    } else {
        &sanitized_task_name
    };

    let assume_role_request = sts_client
        .assume_role_with_web_identity()
        .role_arn(&config.aws_role_arn)
        .role_session_name(&format!(
            "flow.{}@{}",
            truncated_task_name,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs()
        ))
        .duration_seconds(12 * 3600) // 12 hour maximum duration for connectors
        .web_identity_token(&signed_jwt);

    let response = match assume_role_request.send().await {
        Ok(response) => response,
        Err(e) => anyhow::bail!(
            "failed to assume AWS role with web identity {} ({}): {}",
            config.aws_role_arn,
            e.code().unwrap_or_default(),
            e.message().unwrap_or_default()
        ),
    };

    signed_jwt.zeroize();

    let credentials = response
        .credentials()
        .context("no credentials returned from STS AssumeRoleWithWebIdentity")?;

    Ok(AWSTokens {
        access_key_id: credentials.access_key_id().to_string(),
        secret_access_key: credentials.secret_access_key().to_string(),
        session_token: credentials.session_token().to_string(),
    })
}

/// Generate Azure access token using 2-step workload identity federation with Google as external provider:
/// 1. Sign JWT using Google's signJWT API with task_name as subject
/// 2. Exchange signed JWT for target App Registration access token
async fn generate_azure_tokens(
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

/// Generate GCP access token using 3-step service account impersonation:
/// 1. Sign JWT for runtime service account with task_name
/// 2. Exchange JWT for access token using OAuth 2.0 token exchange
/// 3. Use the exchanged token to impersonate the target service account
async fn generate_gcp_tokens(config: &GCPConfig, task_name: &str) -> anyhow::Result<GCPTokens> {
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
async fn google_sign_jwt(
    task_name: &str,
    subject: &str,
    audience: &str,
) -> anyhow::Result<String> {
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
    let runtime_service_account = key_data
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
        runtime_service_account
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

/// Wrapper struct for parsing connector config with credentials
#[derive(Debug, Deserialize)]
struct ConnectorConfigWithCredentials {
    credentials: IAMAuthConfig,
}

/// Extract IAM authentication configuration from connector config JSON if x-iam-auth is set under credentials
pub fn extract_iam_auth_from_connector_config(
    config_json: &str,
    config_schema_json: &str,
) -> anyhow::Result<Option<IAMAuthConfig>> {
    if !has_credentials_iam_auth_annotation(config_schema_json)? {
        return Ok(None);
    }

    Ok(
        serde_json::from_str::<ConnectorConfigWithCredentials>(config_json)
            .ok()
            .map(|c| c.credentials),
    )
}

/// Check if schema has x-iam-auth: true under the credentials object
fn has_credentials_iam_auth_annotation(schema_json: &str) -> anyhow::Result<bool> {
    let built_schema =
        doc::validation::build_bundle(schema_json).context("failed to build schema bundle")?;
    let mut index = doc::SchemaIndexBuilder::new();
    index.add(&built_schema)?;
    let index = index.into_index();

    let shape = doc::Shape::infer(&built_schema, &index);

    let credentials_ptr = doc::Pointer::from("/credentials");
    let (credentials_shape, exists) = shape.locate(&credentials_ptr);

    if exists.cannot() {
        return Ok(false);
    }

    if let Some(iam_auth_value) = credentials_shape.annotations.get("x-iam-auth") {
        if let Some(iam_auth_bool) = iam_auth_value.as_bool() {
            return Ok(iam_auth_bool);
        }
    }

    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Example JSONSchema with oneOf for different credential providers
    const SCHEMA_WITH_ONEOF_CREDENTIALS: &str = r#"{
        "type": "object",
        "properties": {
            "bucket": {
                "type": "string",
                "title": "S3 Bucket"
            },
            "credentials": {
                "type": "object",
                "x-iam-auth": true,
                "oneOf": [
                    {
                        "title": "Manual Credentials",
                        "properties": {
                            "access_key": {
                                "type": "string"
                            },
                            "secret_key": {
                                "type": "string"
                            }
                        },
                        "required": ["access_key", "secret_key"]
                    },
                    {
                        "title": "AWS IAM",
                        "properties": {
                            "aws_role_arn": {
                                "type": "string",
                                "title": "IAM Role ARN"
                            },
                            "aws_region": {
                                "type": "string",
                                "title": "AWS Region"
                            }
                        },
                        "required": ["aws_role_arn", "aws_region"]
                    },
                    {
                        "title": "GCP IAM",
                        "properties": {
                            "gcp_service_account_to_impersonate": {
                                "type": "string",
                                "title": "Service Account Email"
                            },
                            "gcp_workload_identity_pool_audience": {
                                "type": "string",
                                "title": "Workload Identity Pool Audience"
                            }
                        },
                        "required": ["gcp_service_account_to_impersonate", "gcp_workload_identity_pool_audience"]
                    }
                ]
            }
        },
        "required": ["bucket", "credentials"]
    }"#;

    // Simple schema with direct x-iam-auth under credentials (for backward compatibility)
    const SIMPLE_SCHEMA_WITH_IAM_AUTH: &str = r#"{
        "type": "object",
        "properties": {
            "credentials": {
                "type": "object",
                "x-iam-auth": true,
                "properties": {
                    "aws_role_arn": {
                        "type": "string"
                    },
                    "aws_region": {
                        "type": "string"
                    }
                }
            }
        }
    }"#;

    // Schema with credentials defined in an allOf pattern
    const SCHEMA_WITH_ALLOF_CREDENTIALS: &str = r#"{
        "type": "object",
        "allOf": [
            {
                "properties": {
                    "bucket": {
                        "type": "string",
                        "title": "S3 Bucket"
                    }
                }
            },
            {
                "properties": {
                    "credentials": {
                        "type": "object",
                        "x-iam-auth": true,
                        "properties": {
                            "aws_role_arn": {
                                "type": "string",
                                "title": "IAM Role ARN"
                            },
                            "aws_region": {
                                "type": "string",
                                "title": "AWS Region"
                            }
                        },
                        "required": ["aws_role_arn", "aws_region"]
                    }
                }
            }
        ],
        "required": ["bucket", "credentials"]
    }"#;

    // Schema with nested allOf and credentials
    const SCHEMA_WITH_NESTED_ALLOF_CREDENTIALS: &str = r#"{
        "type": "object",
        "allOf": [
            {
                "properties": {
                    "bucket": {
                        "type": "string",
                        "title": "S3 Bucket"
                    }
                }
            },
            {
                "allOf": [
                    {
                        "properties": {
                            "region": {
                                "type": "string"
                            }
                        }
                    },
                    {
                        "properties": {
                            "credentials": {
                                "type": "object",
                                "x-iam-auth": true,
                                "properties": {
                                    "gcp_service_account_to_impersonate": {
                                        "type": "string",
                                        "title": "Service Account Email"
                                    },
                                    "gcp_workload_identity_pool_audience": {
                                        "type": "string",
                                        "title": "Workload Identity Pool Audience"
                                    }
                                },
                                "required": ["gcp_service_account_to_impersonate", "gcp_workload_identity_pool_audience"]
                            }
                        }
                    }
                ]
            }
        ],
        "required": ["bucket", "credentials"]
    }"#;

    #[test]
    fn test_iam_auth_config_creation() {
        let aws_config = IAMAuthConfig::AWS(AWSConfig {
            aws_role_arn: "arn:aws:iam::123456789012:role/FlowConnectorRole".to_string(),
            aws_region: "us-west-2".to_string(),
        });

        match aws_config {
            IAMAuthConfig::AWS(config) => {
                assert_eq!(
                    config.aws_role_arn,
                    "arn:aws:iam::123456789012:role/FlowConnectorRole"
                );
            }
            _ => panic!("Expected AWS config"),
        }
    }

    #[test]
    fn test_aws_iam_auth_with_oneof_second_item() {
        // Test AWS IAM auth using the second oneOf item
        let config = json!({
            "bucket": "my-test-bucket",
            "credentials": {
                "aws_role_arn": "arn:aws:iam::123456789012:role/FlowConnectorRole",
                "aws_region": "us-west-2"
            }
        });

        let result = extract_iam_auth_from_connector_config(
            &config.to_string(),
            SCHEMA_WITH_ONEOF_CREDENTIALS,
        )
        .unwrap();
        assert!(result.is_some());
        let iam_config = result.unwrap();

        match iam_config {
            IAMAuthConfig::AWS(aws_config) => {
                assert_eq!(
                    aws_config.aws_role_arn,
                    "arn:aws:iam::123456789012:role/FlowConnectorRole"
                );
                assert_eq!(aws_config.aws_region, "us-west-2");
            }
            _ => panic!("Expected AWS config"),
        }
    }

    #[test]
    fn test_gcp_iam_auth_with_oneof_third_item() {
        // Test GCP IAM auth using the third oneOf item
        let config = json!({
            "bucket": "my-test-bucket",
            "credentials": {
                "gcp_service_account_to_impersonate": "flow-connector@my-project.iam.gserviceaccount.com",
                "gcp_workload_identity_pool_audience": "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/test-pool/providers/test-provider"
            }
        });

        let result = extract_iam_auth_from_connector_config(
            &config.to_string(),
            SCHEMA_WITH_ONEOF_CREDENTIALS,
        )
        .unwrap();
        assert!(result.is_some());
        let iam_config = result.unwrap();

        match iam_config {
            IAMAuthConfig::GCP(gcp_config) => {
                assert_eq!(
                    gcp_config.gcp_service_account_to_impersonate,
                    "flow-connector@my-project.iam.gserviceaccount.com"
                );
            }
            _ => panic!("Expected GCP config"),
        }
    }

    #[test]
    fn test_no_iam_auth_without_root_annotation() {
        let schema_without_iam_auth = r#"{
            "type": "object",
            "properties": {
                "bucket": {
                    "type": "string",
                    "title": "S3 Bucket"
                },
                "credentials": {
                    "type": "object",
                    "properties": {
                        "access_key": {
                            "type": "string"
                        },
                        "secret_key": {
                            "type": "string"
                        }
                    }
                }
            }
        }"#;

        let config = json!({
            "bucket": "my-test-bucket",
            "credentials": {
                "access_key": "AKIAIOSFODNN7EXAMPLE",
                "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
            }
        });

        let result =
            extract_iam_auth_from_connector_config(&config.to_string(), schema_without_iam_auth)
                .unwrap();
        // Should return None because schema doesn't have x-iam-auth: true under credentials
        assert!(result.is_none());
    }

    #[test]
    fn test_no_iam_auth_without_standardized_properties() {
        let config = json!({
            "credentials": {
                "credentials_json": "..."
            }
        });

        let result = extract_iam_auth_from_connector_config(
            &config.to_string(),
            SIMPLE_SCHEMA_WITH_IAM_AUTH,
        )
        .unwrap();
        // Should return None because credentials don't match our IAM patterns
        assert!(result.is_none());
    }

    #[test]
    fn test_invalid_schema() {
        let config = json!({"bucket": "test"});
        let invalid_schema = "{ invalid json";

        let result = extract_iam_auth_from_connector_config(&config.to_string(), invalid_schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_with_credentials_annotation() {
        // Test schema with x-iam-auth: true under credentials
        let schema = r#"{
            "type": "object",
            "properties": {
                "bucket": {
                    "type": "string",
                    "title": "S3 Bucket"
                },
                "credentials": {
                    "type": "object",
                    "x-iam-auth": true,
                    "properties": {
                        "aws_role_arn": {
                            "type": "string",
                            "title": "IAM Role ARN"
                        },
                        "aws_region": {
                            "type": "string",
                            "title": "AWS Region"
                        }
                    }
                }
            },
            "required": ["bucket", "credentials"]
        }"#;

        let config = json!({
            "bucket": "my-test-bucket",
            "credentials": {
                "aws_role_arn": "arn:aws:iam::123456789012:role/FlowConnectorRole",
                "aws_region": "us-west-2"
            }
        });

        let result = extract_iam_auth_from_connector_config(&config.to_string(), schema).unwrap();

        // Should successfully extract IAM config
        assert!(result.is_some());
        let iam_config = result.unwrap();

        match iam_config {
            IAMAuthConfig::AWS(aws_config) => {
                assert_eq!(
                    aws_config.aws_role_arn,
                    "arn:aws:iam::123456789012:role/FlowConnectorRole"
                );
                assert_eq!(aws_config.aws_region, "us-west-2");
            }
            _ => panic!("Expected AWS config"),
        }
    }

    #[test]
    fn test_end_to_end_iam_config_extraction() {
        // This test demonstrates the complete flow from connector config + schema to IAM config
        let config = json!({
            "bucket": "my-s3-bucket",
            "credentials": {
                "aws_role_arn": "arn:aws:iam::123456789012:role/FlowS3Role",
                "aws_region": "us-east-1"
            }
        });

        let schema = SCHEMA_WITH_ONEOF_CREDENTIALS;

        let result = extract_iam_auth_from_connector_config(&config.to_string(), schema).unwrap();
        assert!(result.is_some());

        let iam_config = result.unwrap();

        match iam_config {
            IAMAuthConfig::AWS(aws_config) => {
                assert_eq!(
                    aws_config.aws_role_arn,
                    "arn:aws:iam::123456789012:role/FlowS3Role"
                );
                assert_eq!(aws_config.aws_region, "us-east-1");
            }
            _ => panic!("Expected AWS config"),
        }
    }

    #[test]
    fn test_has_credentials_iam_auth_annotation() {
        let schema_with_annotation = r#"{
            "type": "object",
            "properties": {
                "credentials": {
                    "type": "object",
                    "x-iam-auth": true
                }
            }
        }"#;

        let result = has_credentials_iam_auth_annotation(schema_with_annotation).unwrap();
        assert!(result);

        let schema_without_annotation = r#"{
            "type": "object",
            "properties": {
                "credentials": {
                    "type": "object"
                }
            }
        }"#;

        let result = has_credentials_iam_auth_annotation(schema_without_annotation).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_serde_deserialize_aws_config() {
        let credentials = json!({
            "aws_role_arn": "arn:aws:iam::123456789012:role/FlowConnectorRole",
            "aws_region": "us-west-2"
        });

        let iam_config: IAMAuthConfig = serde_json::from_value(credentials).unwrap();
        match iam_config {
            IAMAuthConfig::AWS(aws_config) => {
                assert_eq!(
                    aws_config.aws_role_arn,
                    "arn:aws:iam::123456789012:role/FlowConnectorRole"
                );
                assert_eq!(aws_config.aws_region, "us-west-2");
            }
            _ => panic!("Expected AWS config"),
        }
    }

    #[test]
    fn test_serde_deserialize_gcp_config() {
        let credentials = json!({
            "gcp_service_account_to_impersonate": "test@project.iam.gserviceaccount.com",
            "gcp_workload_identity_pool_audience": "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/test-pool/providers/test-provider"
        });

        let iam_config: IAMAuthConfig = serde_json::from_value(credentials).unwrap();
        match iam_config {
            IAMAuthConfig::GCP(gcp_config) => {
                assert_eq!(
                    gcp_config.gcp_service_account_to_impersonate,
                    "test@project.iam.gserviceaccount.com"
                );
            }
            _ => panic!("Expected GCP config"),
        }
    }

    #[test]
    fn test_serde_deserialize_fails_with_no_standardized_properties() {
        let credentials = json!({
            "manual_access_key": "AKIATEST",
            "manual_secret_key": "secret"
        });

        let result = serde_json::from_value::<IAMAuthConfig>(credentials);
        assert!(result.is_err());
    }

    #[test]
    fn test_tokens_inject_into_aws() {
        let mut config = serde_json::to_string(&json!({
            "address": "1.1.1.1",
            "credentials": {
                "auth_type": "aws"
            }
        }))
        .unwrap();

        let tokens = IAMTokens::AWS(AWSTokens {
            access_key_id: "test_access_key_id".to_string(),
            secret_access_key: "test_secret_access_key".to_string(),
            session_token: "test_session_token".to_string(),
        });

        let result = tokens.inject_into(&mut config);
        assert!(result.is_ok());
        assert_eq!(
            config,
            serde_json::to_string(&json!({
                "address": "1.1.1.1",
                "credentials": {
                    "auth_type": "aws",
                    "aws_access_key_id": "test_access_key_id",
                    "aws_secret_access_key": "test_secret_access_key",
                    "aws_session_token": "test_session_token"
                }
            }))
            .unwrap()
        )
    }

    #[test]
    fn test_tokens_inject_into_gcp() {
        let mut config = serde_json::to_string(&json!({
            "address": "1.1.1.1",
            "credentials": {
                "auth_type": "gcp"
            }
        }))
        .unwrap();

        let tokens = IAMTokens::GCP(GCPTokens {
            access_token: "test_access_token".to_string(),
        });

        let result = tokens.inject_into(&mut config);
        assert!(result.is_ok());
        assert_eq!(
            config,
            serde_json::to_string(&json!({
                "address": "1.1.1.1",
                "credentials": {
                    "auth_type": "gcp",
                    "gcp_access_token": "test_access_token",
                }
            }))
            .unwrap()
        )
    }

    #[test]
    fn test_tokens_inject_into_azure() {
        let mut config = serde_json::to_string(&json!({
            "address": "1.1.1.1",
            "credentials": {
                "auth_type": "azure"
            }
        }))
        .unwrap();

        let tokens = IAMTokens::Azure(AzureTokens {
            access_token: "test_azure_access_token".to_string(),
        });

        let result = tokens.inject_into(&mut config);
        assert!(result.is_ok());
        assert_eq!(
            config,
            serde_json::to_string(&json!({
                "address": "1.1.1.1",
                "credentials": {
                    "auth_type": "azure",
                    "azure_access_token": "test_azure_access_token",
                }
            }))
            .unwrap()
        )
    }

    #[test]
    fn test_serde_missing_required_aws_properties() {
        let credentials = json!({
            "aws_role_arn": "arn:aws:iam::123456789012:role/TestRole"
            // Missing aws_region
        });

        let result = serde_json::from_value::<IAMAuthConfig>(credentials);
        assert!(result.is_err());
    }

    #[test]
    fn test_serde_missing_required_gcp_properties() {
        let credentials = json!({
            // Missing gcp_service_account_to_impersonate
        });

        let result = serde_json::from_value::<IAMAuthConfig>(credentials);
        assert!(result.is_err());
    }

    #[test]
    fn test_serde_deserialize_azure_config() {
        let credentials = json!({
            "azure_client_id": "12345678-1234-1234-1234-123456789012",
            "azure_tenant_id": "87654321-4321-4321-4321-210987654321"
        });

        let iam_config: IAMAuthConfig = serde_json::from_value(credentials).unwrap();
        match iam_config {
            IAMAuthConfig::Azure(azure_config) => {
                assert_eq!(
                    azure_config.azure_client_id,
                    "12345678-1234-1234-1234-123456789012"
                );
                assert_eq!(
                    azure_config.azure_tenant_id,
                    "87654321-4321-4321-4321-210987654321"
                );
            }
            _ => panic!("Expected Azure config"),
        }
    }

    #[test]
    fn test_serde_missing_required_azure_properties() {
        let credentials = json!({
            "azure_client_id": "12345678-1234-1234-1234-123456789012"
            // Missing azure_tenant_id
        });

        let result = serde_json::from_value::<IAMAuthConfig>(credentials);
        assert!(result.is_err());
    }

    #[test]
    fn test_manual_credentials_with_oneof_first_item() {
        // Test that manual credentials (first oneOf item) errors because schema has IAM auth enabled
        let config = json!({
            "bucket": "my-test-bucket",
            "credentials": {
                "access_key": "AKIAIOSFODNN7EXAMPLE",
                "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
            }
        });

        let result = extract_iam_auth_from_connector_config(
            &config.to_string(),
            SCHEMA_WITH_ONEOF_CREDENTIALS,
        )
        .unwrap();

        // Should return None because credentials don't match our IAM patterns
        assert!(result.is_none());
    }

    #[test]
    fn test_simple_schema_with_direct_annotation() {
        // Test schema with x-iam-auth: true directly under credentials (no oneOf)
        let config = json!({
            "credentials": {
                "aws_role_arn": "arn:aws:iam::123456789012:role/FlowConnectorRole",
                "aws_region": "us-west-2"
            }
        });

        let result = extract_iam_auth_from_connector_config(
            &config.to_string(),
            SIMPLE_SCHEMA_WITH_IAM_AUTH,
        )
        .unwrap();

        // Should successfully extract IAM config
        assert!(result.is_some());
        let iam_config = result.unwrap();

        match iam_config {
            IAMAuthConfig::AWS(aws_config) => {
                assert_eq!(
                    aws_config.aws_role_arn,
                    "arn:aws:iam::123456789012:role/FlowConnectorRole"
                );
                assert_eq!(aws_config.aws_region, "us-west-2");
            }
            _ => panic!("Expected AWS config"),
        }
    }

    #[test]
    fn test_oneof_annotation_detection() {
        // Test that annotation detection works in oneOf items
        let result = has_credentials_iam_auth_annotation(SCHEMA_WITH_ONEOF_CREDENTIALS).unwrap();
        assert!(result, "Should detect x-iam-auth: true in oneOf items");

        let result = has_credentials_iam_auth_annotation(SIMPLE_SCHEMA_WITH_IAM_AUTH).unwrap();
        assert!(
            result,
            "Should detect x-iam-auth: true directly under credentials"
        );
    }

    #[test]
    fn test_allof_annotation_detection() {
        // Test that annotation detection works when credentials is defined in allOf
        let result = has_credentials_iam_auth_annotation(SCHEMA_WITH_ALLOF_CREDENTIALS).unwrap();
        assert!(result, "Should detect x-iam-auth: true in allOf pattern");

        // Test nested allOf structure
        let result =
            has_credentials_iam_auth_annotation(SCHEMA_WITH_NESTED_ALLOF_CREDENTIALS).unwrap();
        assert!(
            result,
            "Should detect x-iam-auth: true in nested allOf pattern"
        );
    }

    #[test]
    fn test_allof_iam_auth_extraction() {
        // Test that IAM auth extraction works with allOf schema
        let config = json!({
            "bucket": "my-test-bucket",
            "credentials": {
                "aws_role_arn": "arn:aws:iam::123456789012:role/FlowConnectorRole",
                "aws_region": "us-west-2"
            }
        });

        let result = extract_iam_auth_from_connector_config(
            &config.to_string(),
            SCHEMA_WITH_ALLOF_CREDENTIALS,
        )
        .unwrap();

        assert!(result.is_some());
        let iam_config = result.unwrap();

        match iam_config {
            IAMAuthConfig::AWS(aws_config) => {
                assert_eq!(
                    aws_config.aws_role_arn,
                    "arn:aws:iam::123456789012:role/FlowConnectorRole"
                );
                assert_eq!(aws_config.aws_region, "us-west-2");
            }
            _ => panic!("Expected AWS config"),
        }
    }

    #[test]
    fn test_nested_allof_iam_auth_extraction() {
        // Test that IAM auth extraction works with nested allOf schema
        let config = json!({
            "bucket": "my-test-bucket",
            "region": "us-east-1",
            "credentials": {
                "gcp_service_account_to_impersonate": "flow-connector@my-project.iam.gserviceaccount.com",
                "gcp_workload_identity_pool_audience": "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/test-pool/providers/test-provider"
            }
        });

        let result = extract_iam_auth_from_connector_config(
            &config.to_string(),
            SCHEMA_WITH_NESTED_ALLOF_CREDENTIALS,
        )
        .unwrap();

        assert!(result.is_some());
        let iam_config = result.unwrap();

        match iam_config {
            IAMAuthConfig::GCP(gcp_config) => {
                assert_eq!(
                    gcp_config.gcp_service_account_to_impersonate,
                    "flow-connector@my-project.iam.gserviceaccount.com"
                );
            }
            _ => panic!("Expected GCP config"),
        }
    }
}
