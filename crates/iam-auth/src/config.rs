use serde::{Deserialize, Serialize};

use crate::providers::{aws, azure, gcp};
use crate::tokens::IAMTokens;

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

impl IAMAuthConfig {
    /// Generate short-lived tokens for the configured IAM provider
    pub async fn generate_tokens(&self, task_name: &str) -> anyhow::Result<IAMTokens> {
        match self {
            IAMAuthConfig::AWS(aws_config) => {
                let aws_tokens = aws::generate_tokens(aws_config, task_name).await?;
                Ok(IAMTokens::AWS(aws_tokens))
            }
            IAMAuthConfig::GCP(gcp_config) => {
                let gcp_tokens = gcp::generate_tokens(gcp_config, task_name).await?;
                Ok(IAMTokens::GCP(gcp_tokens))
            }
            IAMAuthConfig::Azure(azure_config) => {
                let azure_tokens = azure::generate_tokens(azure_config, task_name).await?;
                Ok(IAMTokens::Azure(azure_tokens))
            }
        }
    }
}

/// Wrapper struct for parsing connector config with credentials
#[derive(Debug, Deserialize)]
pub(crate) struct ConnectorConfigWithCredentials {
    pub credentials: IAMAuthConfig,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
    fn test_serde_deserialize_fails_with_no_standardized_properties() {
        let credentials = json!({
            "manual_access_key": "AKIATEST",
            "manual_secret_key": "secret"
        });

        let result = serde_json::from_value::<IAMAuthConfig>(credentials);
        assert!(result.is_err());
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
    fn test_serde_missing_required_azure_properties() {
        let credentials = json!({
            "azure_client_id": "12345678-1234-1234-1234-123456789012"
            // Missing azure_tenant_id
        });

        let result = serde_json::from_value::<IAMAuthConfig>(credentials);
        assert!(result.is_err());
    }
}