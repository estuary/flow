pub mod aws;
pub mod azure;
pub mod gcp;

#[cfg(test)]
mod tests {
    // Note: Provider tests would require actual credentials and external services to test properly.
    // These tests are commented out as they cannot run in the test environment without proper setup.
    // In a real environment, you would need to set up appropriate test credentials and mock services.

    /*
    #[tokio::test]
    async fn test_aws_token_generation() {
        use crate::config::AWSConfig;
        use crate::providers::aws;

        let config = AWSConfig {
            aws_role_arn: "arn:aws:iam::123456789012:role/test-role".to_string(),
            aws_region: "us-east-1".to_string(),
        };

        // This would require actual credentials and external API calls
        // let result = aws::generate_tokens(&config, "test-task").await;
        // assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_gcp_token_generation() {
        use crate::config::GCPConfig;
        use crate::providers::gcp;

        let config = GCPConfig {
            gcp_service_account_to_impersonate: "test@project.iam.gserviceaccount.com".to_string(),
            gcp_workload_identity_pool_audience: "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/test-pool/providers/test-provider".to_string(),
        };

        // This would require actual credentials and external API calls
        // let result = gcp::generate_tokens(&config, "test-task").await;
        // assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_azure_token_generation() {
        use crate::config::AzureConfig;
        use crate::providers::azure;

        let config = AzureConfig {
            azure_client_id: "12345678-1234-1234-1234-123456789012".to_string(),
            azure_tenant_id: "87654321-4321-4321-4321-210987654321".to_string(),
        };

        // This would require actual credentials and external API calls
        // let result = azure::generate_tokens(&config, "test-task").await;
        // assert!(result.is_ok());
    }
    */

    // Placeholder test to ensure the module compiles
    #[test]
    fn test_provider_module_compiles() {
        // This is just a placeholder to ensure the provider modules are properly structured
        assert!(true);
    }
}