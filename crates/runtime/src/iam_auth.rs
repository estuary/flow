use anyhow::Context;

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
}

#[derive(Debug, Clone)]
pub struct GCPTokens {
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
        }

        *config = serde_json::to_string(&parsed)?;

        Ok(())
    }
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
}

/// Generate AWS temporary credentials using STS AssumeRole
async fn generate_aws_tokens(config: &AWSConfig) -> anyhow::Result<AWSTokens> {
    use aws_config::Region;
    use aws_credential_types::Credentials;

    // Get AWS credentials from environment variables
    let access_key_id = std::env::var("AWS_ACCESS_KEY_ID")
        .context("AWS_ACCESS_KEY_ID environment variable not set")?;
    let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY")
        .context("AWS_SECRET_ACCESS_KEY environment variable not set")?;

    if access_key_id.is_empty() || secret_access_key.is_empty() {
        anyhow::bail!("AWS credentials from environment variables are empty");
    }

    // Create credentials provider from the environment credentials
    let credentials = Credentials::new(
        &access_key_id,
        &secret_access_key,
        None, // session token
        None, // expiration
        "flow-root-credentials",
    );

    let mut aws_config_builder = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .credentials_provider(credentials);

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
    })
}

/// Generate GCP access token using service account impersonation
async fn generate_gcp_tokens(config: &GCPConfig) -> anyhow::Result<GCPTokens> {
    // Get GCP credentials from environment variable
    let credentials_path = std::env::var("GOOGLE_APPLICATION_CREDENTIALS")
        .context("GOOGLE_APPLICATION_CREDENTIALS environment variable not set")?;

    if credentials_path.is_empty() {
        anyhow::bail!("GOOGLE_APPLICATION_CREDENTIALS environment variable is empty");
    }

    // Read the credentials JSON file
    let credentials_json = tokio::fs::read_to_string(&credentials_path)
        .await
        .with_context(|| format!("Failed to read Google Cloud credentials from {}", credentials_path))?;

    if credentials_json.trim().is_empty() {
        anyhow::bail!("Google Cloud credentials file is empty");
    }

    // Use the Google Cloud Auth library to get credentials
    let scopes = &["https://www.googleapis.com/auth/cloud-platform"];

    // If we have a specific service account, we should impersonate it
    // First get a token from the root credentials
    let default_token = get_gcp_token_from_credentials(&credentials_json).await?;
    // Use IAM Service Account Credentials API for impersonation
    let access_token =
        impersonate_service_account(&config.service_account_email, &default_token, scopes).await?;

    Ok(GCPTokens { access_token })
}

/// Get GCP access token from service account credentials JSON
async fn get_gcp_token_from_credentials(credentials_json: &str) -> anyhow::Result<String> {
    use serde_json::Value;

    let key_data: Value = serde_json::from_str(credentials_json)
        .context("Failed to parse service account key JSON")?;

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

/// Extract IAM authentication configuration from connector config JSON using schema annotations
/// This function parses both the connector config and schema to find x-iam-auth: true under "credentials"
/// and infers the provider from standardized properties under "credentials" key
pub fn extract_iam_auth_from_connector_config(
    config_json: &str,
    config_schema_json: &str,
) -> anyhow::Result<Option<IAMAuthConfig>> {
    // Parse the connector config JSON
    let config_value = serde_json::from_str::<serde_json::Value>(config_json)?;

    // Check if schema has x-iam-auth: true under credentials
    if !has_credentials_iam_auth_annotation(config_schema_json)? {
        return Ok(None);
    }

    // Look for credentials object in config
    let credentials = config_value
        .get("credentials")
        .and_then(|v| v.as_object())
        .ok_or_else(|| anyhow::anyhow!("credentials object not found in config"))?;

    // Infer provider type from properties present in credentials
    let provider_type = infer_provider_from_credentials(credentials)?;

    // Extract IAM configuration using standardized property names
    extract_iam_config_from_credentials(&config_value, &provider_type)
}

/// Check if schema has x-iam-auth: true under the credentials object or in any of its oneOf items
fn has_credentials_iam_auth_annotation(schema_json: &str) -> anyhow::Result<bool> {
    let schema_value: serde_json::Value = serde_json::from_str(schema_json)?;

    let credentials_schema = schema_value
        .get("properties")
        .and_then(|props| props.get("credentials"));

    if let Some(creds) = credentials_schema {
        // Check direct x-iam-auth annotation
        if creds
            .get("x-iam-auth")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            return Ok(true);
        }

        // Check oneOf items for x-iam-auth annotation
        if let Some(one_of) = creds.get("oneOf").and_then(|v| v.as_array()) {
            for item in one_of {
                if item
                    .get("x-iam-auth")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    return Ok(true);
                }
            }
        }
    }

    Ok(false)
}

/// Infer provider type from standardized properties present in credentials object
fn infer_provider_from_credentials(
    credentials: &serde_json::Map<String, serde_json::Value>,
) -> anyhow::Result<String> {
    // Check for AWS standardized properties
    if credentials.contains_key("aws_role_arn")
        || credentials.contains_key("aws_external_id")
        || credentials.contains_key("aws_region")
    {
        return Ok("aws".to_string());
    }

    // Check for GCP standardized properties
    if credentials.contains_key("gcp_service_account_to_impersonate") {
        return Ok("gcp".to_string());
    }

    Err(anyhow::anyhow!(
        "Unable to infer IAM provider from credentials properties. Expected AWS properties (aws_role_arn, aws_external_id, aws_region) or GCP properties (gcp_service_account_to_impersonate)"
    ))
}

/// Extract IAM configuration from credentials object using standardized property names
fn extract_iam_config_from_credentials(
    config_json: &serde_json::Value,
    provider_type: &str,
) -> anyhow::Result<Option<IAMAuthConfig>> {
    let credentials = config_json
        .get("credentials")
        .and_then(|v| v.as_object())
        .ok_or_else(|| anyhow::anyhow!("credentials object not found"))?;

    match provider_type {
        "aws" => {
            // Require standardized AWS properties
            let role_arn = credentials
                .get("aws_role_arn")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("aws_role_arn is required for AWS IAM auth"))?;

            let external_id = credentials
                .get("aws_external_id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("aws_external_id is required for AWS IAM auth"))?;

            let region = credentials
                .get("aws_region")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("aws_region is required for AWS IAM auth"))?;

            Ok(Some(IAMAuthConfig::AWS(AWSConfig {
                role_arn: role_arn.to_string(),
                external_id: Some(external_id.to_string()),
                region: Some(region.to_string()),
            })))
        }
        "gcp" => {
            // Require standardized GCP properties
            let service_account_email = credentials
                .get("gcp_service_account_to_impersonate")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "gcp_service_account_to_impersonate is required for GCP IAM auth"
                    )
                })?;

            Ok(Some(IAMAuthConfig::GCP(GCPConfig {
                service_account_email: service_account_email.to_string(),
            })))
        }
        _ => Err(anyhow::anyhow!(
            "Unsupported IAM auth provider: {}",
            provider_type
        )),
    }
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
                        "x-iam-auth": true,
                        "properties": {
                            "aws_role_arn": {
                                "type": "string",
                                "title": "IAM Role ARN"
                            },
                            "aws_external_id": {
                                "type": "string",
                                "title": "External ID"
                            },
                            "aws_region": {
                                "type": "string",
                                "title": "AWS Region"
                            }
                        },
                        "required": ["aws_role_arn", "aws_external_id", "aws_region"]
                    },
                    {
                        "title": "GCP IAM",
                        "x-iam-auth": true,
                        "properties": {
                            "gcp_service_account_to_impersonate": {
                                "type": "string",
                                "title": "Service Account Email"
                            }
                        },
                        "required": ["gcp_service_account_to_impersonate"]
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
                    "aws_external_id": {
                        "type": "string"
                    },
                    "aws_region": {
                        "type": "string"
                    }
                }
            }
        }
    }"#;

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
    fn test_aws_iam_auth_with_oneof_second_item() {
        // Test AWS IAM auth using the second oneOf item
        let config = json!({
            "bucket": "my-test-bucket",
            "credentials": {
                "aws_role_arn": "arn:aws:iam::123456789012:role/FlowConnectorRole",
                "aws_external_id": "unique-external-id",
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
                    aws_config.role_arn,
                    "arn:aws:iam::123456789012:role/FlowConnectorRole"
                );
                assert_eq!(
                    aws_config.external_id,
                    Some("unique-external-id".to_string())
                );
                assert_eq!(aws_config.region, Some("us-west-2".to_string()));
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
                    gcp_config.service_account_email,
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
        );
        // Should return error because no standardized IAM properties found
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unable to infer IAM provider"));
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
                        "aws_external_id": {
                            "type": "string",
                            "title": "External ID"
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
                "aws_external_id": "unique-external-id",
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
                    aws_config.role_arn,
                    "arn:aws:iam::123456789012:role/FlowConnectorRole"
                );
                assert_eq!(
                    aws_config.external_id,
                    Some("unique-external-id".to_string())
                );
                assert_eq!(aws_config.region, Some("us-west-2".to_string()));
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
                "aws_external_id": "flow-external-id-123",
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
                    aws_config.role_arn,
                    "arn:aws:iam::123456789012:role/FlowS3Role"
                );
                assert_eq!(
                    aws_config.external_id,
                    Some("flow-external-id-123".to_string())
                );
                assert_eq!(aws_config.region, Some("us-east-1".to_string()));
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
    fn test_infer_provider_from_aws_properties() {
        let credentials = json!({
            "aws_role_arn": "arn:aws:iam::123456789012:role/FlowConnectorRole",
            "aws_external_id": "unique-external-id",
            "aws_region": "us-west-2"
        })
        .as_object()
        .unwrap()
        .clone();

        let provider = infer_provider_from_credentials(&credentials).unwrap();
        assert_eq!(provider, "aws");
    }

    #[test]
    fn test_infer_provider_from_gcp_properties() {
        let credentials = json!({
            "gcp_service_account_to_impersonate": "test@project.iam.gserviceaccount.com",
        })
        .as_object()
        .unwrap()
        .clone();

        let provider = infer_provider_from_credentials(&credentials).unwrap();
        assert_eq!(provider, "gcp");
    }

    #[test]
    fn test_infer_provider_from_mixed_properties_aws_wins() {
        // When both AWS and GCP properties are present, AWS should take precedence
        let credentials = json!({
            "aws_role_arn": "arn:aws:iam::123456789012:role/FlowConnectorRole",
            "gcp_service_account_to_impersonate": "test@project.iam.gserviceaccount.com"
        })
        .as_object()
        .unwrap()
        .clone();

        let provider = infer_provider_from_credentials(&credentials).unwrap();
        assert_eq!(provider, "aws");
    }

    #[test]
    fn test_infer_provider_fails_with_no_standardized_properties() {
        let credentials = json!({
            "manual_access_key": "AKIATEST",
            "manual_secret_key": "secret"
        })
        .as_object()
        .unwrap()
        .clone();

        let result = infer_provider_from_credentials(&credentials);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unable to infer IAM provider"));
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
    fn test_aws_config_extraction_with_standardized_properties() {
        let config = json!({
            "credentials": {
                "aws_role_arn": "arn:aws:iam::123456789012:role/TestRole",
                "aws_external_id": "test-external-id",
                "aws_region": "us-west-2"
            }
        });

        let result = extract_iam_config_from_credentials(&config, "aws").unwrap();

        assert!(result.is_some());
        match result.unwrap() {
            IAMAuthConfig::AWS(aws_config) => {
                assert_eq!(
                    aws_config.role_arn,
                    "arn:aws:iam::123456789012:role/TestRole"
                );
                assert_eq!(aws_config.external_id, Some("test-external-id".to_string()));
                assert_eq!(aws_config.region, Some("us-west-2".to_string()));
            }
            _ => panic!("Expected AWS config"),
        }
    }

    #[test]
    fn test_gcp_config_extraction_with_standardized_properties() {
        let config = json!({
            "credentials": {
                "gcp_service_account_to_impersonate": "test@project.iam.gserviceaccount.com",
            }
        });

        let result = extract_iam_config_from_credentials(&config, "gcp").unwrap();

        assert!(result.is_some());
        match result.unwrap() {
            IAMAuthConfig::GCP(gcp_config) => {
                assert_eq!(
                    gcp_config.service_account_email,
                    "test@project.iam.gserviceaccount.com"
                );
            }
            _ => panic!("Expected GCP config"),
        }
    }

    #[test]
    fn test_missing_required_aws_properties() {
        let config = json!({
            "credentials": {
                // Missing aws_role_arn, aws_external_id, aws_region
            }
        });

        let result = extract_iam_config_from_credentials(&config, "aws");

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("aws_role_arn is required"));
    }

    #[test]
    fn test_missing_required_gcp_properties() {
        let config = json!({
            "credentials": {
                // Missing gcp_service_account_to_impersonate
            }
        });

        let result = extract_iam_config_from_credentials(&config, "gcp");

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("gcp_service_account_to_impersonate is required"));
    }

    #[test]
    fn test_unsupported_provider_type() {
        let config = json!({
            "credentials": {
                "azure_client_id": "test"
            }
        });

        let result = extract_iam_config_from_credentials(&config, "azure");

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unsupported IAM auth provider: azure"));
    }

    #[test]
    fn test_extract_iam_config_from_credentials_missing_object() {
        let config = json!({
            "bucket": "test"
            // Missing credentials object
        });

        let result = extract_iam_config_from_credentials(&config, "aws");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("credentials object not found"));
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
        );

        // Should return error because schema has IAM auth enabled but config doesn't have IAM properties
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unable to infer IAM provider"));
    }

    #[test]
    fn test_simple_schema_with_direct_annotation() {
        // Test schema with x-iam-auth: true directly under credentials (no oneOf)
        let config = json!({
            "credentials": {
                "aws_role_arn": "arn:aws:iam::123456789012:role/FlowConnectorRole",
                "aws_external_id": "unique-external-id",
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
                    aws_config.role_arn,
                    "arn:aws:iam::123456789012:role/FlowConnectorRole"
                );
                assert_eq!(
                    aws_config.external_id,
                    Some("unique-external-id".to_string())
                );
                assert_eq!(aws_config.region, Some("us-west-2".to_string()));
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

}
