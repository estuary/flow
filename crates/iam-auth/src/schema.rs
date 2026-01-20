use crate::config::{ConnectorConfigWithCredentials, IAMAuthConfig};

/// IAM Annotations in the connection spec.
#[derive(Debug)]
struct IAMAnnotations {
    iam_auth: bool,
    azure_scope: Option<String>,
}

/// Extract IAM authentication configuration from connector config JSON if x-iam-auth is set under credentials
pub fn extract_iam_auth_from_connector_config(
    config_json: &[u8],
    config_schema_json: &[u8],
) -> anyhow::Result<Option<IAMAuthConfig>> {
    let annotations = extract_iam_auth_annotations(config_schema_json)?;
    if !annotations.iam_auth {
        return Ok(None);
    }

    let Ok(config) = serde_json::from_slice::<ConnectorConfigWithCredentials>(config_json) else {
        return Ok(None);
    };

    let config = match config.credentials {
        IAMAuthConfig::Azure(mut azure_config) => {
            azure_config.azure_scope = annotations.azure_scope;
            IAMAuthConfig::Azure(azure_config)
        }
        creds => creds,
    };
    Ok(Some(config))
}

/// Extract the `x-iam-*` annotations under the credentials object.
fn extract_iam_auth_annotations(schema: &[u8]) -> anyhow::Result<IAMAnnotations> {
    let schema = doc::validation::build_bundle(schema)?;
    let validator = doc::Validator::new(schema)?;
    let shape = doc::Shape::infer(validator.schema(), validator.schema_index());

    let credentials_ptr = json::Pointer::from("/credentials");
    let (credentials_shape, exists) = shape.locate(&credentials_ptr);

    let mut annotations = IAMAnnotations {
        iam_auth: false,
        azure_scope: None,
    };
    if exists.cannot() {
        return Ok(annotations);
    }

    if let Some(iam_auth_value) = credentials_shape.annotations.get("x-iam-auth") {
        if let Some(iam_auth_bool) = iam_auth_value.as_bool() {
            annotations.iam_auth = iam_auth_bool;
        }
    }
    if let Some(iam_azure_scope_value) = credentials_shape.annotations.get("x-iam-azure-scope") {
        if let Some(iam_azure_scope_str) = iam_azure_scope_value.as_str() {
            annotations.azure_scope = Some(iam_azure_scope_str.to_string());
        }
    }

    Ok(annotations)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Example JSONSchema with oneOf for different credential providers
    const SCHEMA_WITH_ONEOF_CREDENTIALS: &[u8] = br#"{
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
    const SIMPLE_SCHEMA_WITH_IAM_AUTH: &[u8] = br#"{
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
    const SCHEMA_WITH_ALLOF_CREDENTIALS: &[u8] = br#"{
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
    const SCHEMA_WITH_NESTED_ALLOF_CREDENTIALS: &[u8] = br#"{
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
            &config.to_string().as_bytes(),
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
            config.to_string().as_bytes(),
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
        let schema_without_iam_auth = br#"{
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

        let result = extract_iam_auth_from_connector_config(
            config.to_string().as_bytes(),
            schema_without_iam_auth,
        )
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
            config.to_string().as_bytes(),
            SIMPLE_SCHEMA_WITH_IAM_AUTH,
        )
        .unwrap();
        // Should return None because credentials don't match our IAM patterns
        assert!(result.is_none());
    }

    #[test]
    fn test_invalid_schema() {
        let config = json!({"bucket": "test"});
        let invalid_schema = b"{ invalid json";

        let result =
            extract_iam_auth_from_connector_config(config.to_string().as_bytes(), invalid_schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_with_credentials_annotation() {
        // Test schema with x-iam-auth: true under credentials
        let schema = br#"{
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

        let result =
            extract_iam_auth_from_connector_config(config.to_string().as_bytes(), schema).unwrap();

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

        let result =
            extract_iam_auth_from_connector_config(config.to_string().as_bytes(), schema).unwrap();
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
        let schema_with_annotation = br#"{
            "type": "object",
            "properties": {
                "credentials": {
                    "type": "object",
                    "x-iam-auth": true
                }
            }
        }"#;

        let result = extract_iam_auth_annotations(schema_with_annotation).unwrap();
        assert!(result.iam_auth);

        let schema_without_annotation = br#"{
            "type": "object",
            "properties": {
                "credentials": {
                    "type": "object"
                }
            }
        }"#;

        let result = extract_iam_auth_annotations(schema_without_annotation).unwrap();
        assert!(!result.iam_auth);
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
            config.to_string().as_bytes(),
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
            config.to_string().as_bytes(),
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
        let result = extract_iam_auth_annotations(SCHEMA_WITH_ONEOF_CREDENTIALS).unwrap();
        assert!(
            result.iam_auth,
            "Should detect x-iam-auth: true in oneOf items"
        );

        let result = extract_iam_auth_annotations(SIMPLE_SCHEMA_WITH_IAM_AUTH).unwrap();
        assert!(
            result.iam_auth,
            "Should detect x-iam-auth: true directly under credentials"
        );
    }

    #[test]
    fn test_allof_annotation_detection() {
        // Test that annotation detection works when credentials is defined in allOf
        let result = extract_iam_auth_annotations(SCHEMA_WITH_ALLOF_CREDENTIALS).unwrap();
        assert!(
            result.iam_auth,
            "Should detect x-iam-auth: true in allOf pattern"
        );

        // Test nested allOf structure
        let result = extract_iam_auth_annotations(SCHEMA_WITH_NESTED_ALLOF_CREDENTIALS).unwrap();
        assert!(
            result.iam_auth,
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
            config.to_string().as_bytes(),
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
            config.to_string().as_bytes(),
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

    #[test]
    fn test_iam_auth_azure_scope() {
        let schema = json!({
            "type": "object",
            "properties": {
                "credentials": {
                    "x-iam-auth": true,
                    "x-iam-azure-scope": "https://database.windows.net/.default",
                    "type": "object",
                    "properties": {
                        "azure_client_id": {
                            "type": "string"
                        },
                        "azure_tenant_id": {
                            "type": "string"
                        }
                    }
                }
            }
        });

        let config = json!({
            "credentials": {
                "azure_client_id": "abc",
                "azure_tenant_id": "def"
            }
        });

        let result = extract_iam_auth_from_connector_config(
            config.to_string().as_bytes(),
            schema.to_string().as_bytes(),
        )
        .unwrap();

        match result {
            Some(IAMAuthConfig::Azure(azure_config)) => {
                assert_eq!(azure_config.azure_client_id, "abc");
                assert_eq!(azure_config.azure_tenant_id, "def");
                assert_eq!(
                    azure_config.azure_scope,
                    Some("https://database.windows.net/.default".to_string())
                );
            }
            _ => panic!("Expected Azure config"),
        }
    }

    #[test]
    fn test_iam_auth_azure_no_scope() {
        let schema = json!({
            "type": "object",
            "properties": {
                "credentials": {
                    "x-iam-auth": true,
                    "type": "object",
                    "properties": {
                        "azure_client_id": {
                            "type": "string"
                        },
                        "azure_tenant_id": {
                            "type": "string"
                        }
                    }
                }
            }
        });

        let config = json!({
            "credentials": {
                "azure_client_id": "abc",
                "azure_tenant_id": "def"
            }
        });

        let result = extract_iam_auth_from_connector_config(
            config.to_string().as_bytes(),
            schema.to_string().as_bytes(),
        )
        .unwrap();

        match result {
            Some(IAMAuthConfig::Azure(azure_config)) => {
                assert_eq!(azure_config.azure_client_id, "abc");
                assert_eq!(azure_config.azure_tenant_id, "def");
                assert!(azure_config.azure_scope.is_none());
            }
            _ => panic!("Expected Azure config"),
        }
    }
}
