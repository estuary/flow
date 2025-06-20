use crate::iam_auth::{AWSConfig, GCPConfig, IAMAuthConfig};

/// Extract IAM authentication configuration from connector config JSON using schema annotations
/// This function parses both the connector config and schema to find x-iam-auth annotated fields
pub fn extract_iam_auth_from_connector_config(
    config_json: &str,
    config_schema_json: &str,
) -> anyhow::Result<Option<IAMAuthConfig>> {
    // Parse the connector config JSON
    let config_value = serde_json::from_str::<serde_json::Value>(config_json)?;

    // Parse the configuration schema to find IAM auth annotations
    let iam_pointers = parse_iam_auth_schema_annotations(config_schema_json)?;

    if iam_pointers.is_empty() {
        return Ok(None);
    }

    // Extract IAM configuration using the discovered pointers
    extract_iam_config_with_pointers(&config_value, &iam_pointers)
}

/// Parse JSONSchema to find IAM authentication annotations
fn parse_iam_auth_schema_annotations(schema_json: &str) -> anyhow::Result<IamAuthPointers> {
    // First try the normal shape-based approach
    let schema = doc::validation::build_bundle(schema_json)?;
    let mut builder = doc::SchemaIndexBuilder::new();
    builder.add(&schema)?;

    let mut pointers = IamAuthPointers::new();

    parse_raw_schema_for_annotations(schema_json, &mut pointers)?;

    Ok(pointers)
}

/// Fallback parser that traverses raw JSON schema to find annotations in oneOf/anyOf scenarios
fn parse_raw_schema_for_annotations(
    schema_json: &str,
    pointers: &mut IamAuthPointers,
) -> anyhow::Result<()> {
    let schema_value: serde_json::Value = serde_json::from_str(schema_json)?;
    traverse_schema_for_annotations(&schema_value, &doc::ptr::Pointer::empty(), pointers);
    Ok(())
}

/// Recursively traverse a JSON schema value looking for IAM annotations
fn traverse_schema_for_annotations(
    schema: &serde_json::Value,
    current_path: &doc::ptr::Pointer,
    pointers: &mut IamAuthPointers,
) {
    match schema {
        serde_json::Value::Object(obj) => {
            // Check if this object has IAM annotations
            check_object_for_annotations(obj, current_path, pointers);

            // Traverse properties
            if let Some(properties) = obj.get("properties").and_then(|p| p.as_object()) {
                for (prop_name, prop_schema) in properties {
                    let mut new_path = current_path.clone();
                    new_path.push(doc::ptr::Token::Property(prop_name.clone()));
                    traverse_schema_for_annotations(prop_schema, &new_path, pointers);
                }
            }

            // Traverse oneOf/anyOf/allOf
            for key in ["oneOf", "anyOf", "allOf"] {
                if let Some(schemas) = obj.get(key).and_then(|s| s.as_array()) {
                    for sub_schema in schemas {
                        traverse_schema_for_annotations(sub_schema, current_path, pointers);
                    }
                }
            }

            // Traverse items (for arrays)
            if let Some(items) = obj.get("items") {
                traverse_schema_for_annotations(items, current_path, pointers);
            }

            // Traverse additionalProperties
            if let Some(additional) = obj.get("additionalProperties") {
                if !additional.is_boolean() {
                    traverse_schema_for_annotations(additional, current_path, pointers);
                }
            }
        }
        serde_json::Value::Array(arr) => {
            // Handle schema arrays (like in oneOf)
            for item in arr {
                traverse_schema_for_annotations(item, current_path, pointers);
            }
        }
        _ => {}
    }
}

/// Check if a schema object contains IAM annotations and record them
fn check_object_for_annotations(
    obj: &serde_json::Map<String, serde_json::Value>,
    current_path: &doc::ptr::Pointer,
    pointers: &mut IamAuthPointers,
) {
    for (key, _value) in obj {
        if key.starts_with("x-") {
            match key.as_str() {
                "x-iam-auth-provider" => pointers.auth_provider = Some(current_path.clone()),
                "x-aws-role-arn" => pointers.aws_role_arn = Some(current_path.clone()),
                "x-aws-external-id" => pointers.aws_external_id = Some(current_path.clone()),
                "x-aws-region" => pointers.aws_region = Some(current_path.clone()),
                "x-gcp-service-account" => {
                    pointers.gcp_service_account = Some(current_path.clone())
                }
                "x-gcp-project-id" => pointers.gcp_project_id = Some(current_path.clone()),
                _ => {}
            }
        }
    }
}

/// Container for IAM authentication JSON pointers discovered from schema annotations
#[derive(Debug, Default)]
struct IamAuthPointers {
    auth_provider: Option<doc::Pointer>,
    aws_role_arn: Option<doc::Pointer>,
    aws_external_id: Option<doc::Pointer>,
    aws_region: Option<doc::Pointer>,
    gcp_service_account: Option<doc::Pointer>,
    gcp_project_id: Option<doc::Pointer>,
}

impl IamAuthPointers {
    fn new() -> Self {
        Self::default()
    }

    fn is_empty(&self) -> bool {
        self.auth_provider.is_none()
            && self.aws_role_arn.is_none()
            && self.gcp_service_account.is_none()
    }
}

/// Extract IAM configuration using discovered schema pointers
fn extract_iam_config_with_pointers(
    config_json: &serde_json::Value,
    pointers: &IamAuthPointers,
) -> anyhow::Result<Option<IAMAuthConfig>> {
    // Extract AWS config if AWS pointers are present
    if let Some(role_arn) = pointers
        .aws_role_arn
        .as_ref()
        .and_then(|ptr| ptr.query(config_json))
        .and_then(|v| v.as_str())
    {
        let external_id = pointers
            .aws_external_id
            .as_ref()
            .and_then(|ptr| ptr.query(config_json))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let region = pointers
            .aws_region
            .as_ref()
            .and_then(|ptr| ptr.query(config_json))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        return Ok(Some(IAMAuthConfig::AWS(AWSConfig {
            role_arn: role_arn.to_string(),
            external_id,
            region,
        })));
    }

    // Extract GCP config if GCP pointers are present
    if let Some(service_account_email) = pointers
        .gcp_service_account
        .as_ref()
        .and_then(|ptr| ptr.query(config_json))
        .and_then(|v| v.as_str())
    {
        let project_id = pointers
            .gcp_project_id
            .as_ref()
            .and_then(|ptr| ptr.query(config_json))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        return Ok(Some(IAMAuthConfig::GCP(GCPConfig {
            service_account_email: service_account_email.to_string(),
            project_id,
        })));
    }

    // No IAM configuration found
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Example JSONSchema with discriminator and oneOf pattern for AWS IAM authentication
    const AWS_SCHEMA_WITH_DISCRIMINATOR: &str = r#"{
        "type": "object",
        "properties": {
            "bucket": {
                "type": "string",
                "title": "S3 Bucket"
            },
            "credentials": {
                "type": "object",
                "properties": {
                    "auth_type": {
                        "type": "string",
                        "enum": ["aws_iam", "manual"],
                        "x-iam-auth-provider": "aws"
                    },
                    "role_arn": {
                        "type": "string",
                        "title": "IAM Role ARN",
                        "x-aws-role-arn": true
                    },
                    "external_id": {
                        "type": "string",
                        "title": "External ID",
                        "x-aws-external-id": true
                    },
                    "region": {
                        "type": "string",
                        "title": "AWS Region",
                        "x-aws-region": true
                    },
                    "access_key": {
                        "type": "string",
                        "secret": true
                    },
                    "secret_key": {
                        "type": "string",
                        "secret": true
                    }
                }
            }
        },
        "required": ["bucket", "credentials"]
    }"#;

    // Example JSONSchema for GCP with service account impersonation
    const GCP_SCHEMA_WITH_DISCRIMINATOR: &str = r#"{
        "type": "object",
        "properties": {
            "project_id": {
                "type": "string",
                "title": "GCP Project ID"
            },
            "authentication": {
                "type": "object",
                "properties": {
                    "method": {
                        "type": "string",
                        "enum": ["service_account_impersonation", "service_account_key"],
                        "x-iam-auth-provider": "gcp"
                    },
                    "service_account_email": {
                        "type": "string",
                        "title": "Service Account Email",
                        "x-gcp-service-account": true
                    },
                    "target_project_id": {
                        "type": "string",
                        "title": "Target Project ID",
                        "x-gcp-project-id": true
                    },
                    "credentials_json": {
                        "type": "string",
                        "secret": true
                    }
                }
            }
        },
        "required": ["project_id", "authentication"]
    }"#;

    #[test]
    fn test_aws_iam_auth_with_discriminator_schema() {
        let config = json!({
            "bucket": "my-test-bucket",
            "credentials": {
                "auth_type": "aws_iam",
                "role_arn": "arn:aws:iam::123456789012:role/FlowConnectorRole",
                "external_id": "unique-external-id",
                "region": "us-west-2"
            }
        });

        let result = extract_iam_auth_from_connector_config(
            &config.to_string(),
            AWS_SCHEMA_WITH_DISCRIMINATOR,
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
    fn test_gcp_service_account_impersonation_with_discriminator() {
        let config = json!({
            "project_id": "my-project",
            "authentication": {
                "method": "service_account_impersonation",
                "service_account_email": "flow-connector@my-project.iam.gserviceaccount.com",
                "target_project_id": "target-project"
            }
        });

        let result = extract_iam_auth_from_connector_config(
            &config.to_string(),
            GCP_SCHEMA_WITH_DISCRIMINATOR,
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
                assert_eq!(gcp_config.project_id, Some("target-project".to_string()));
            }
            _ => panic!("Expected GCP config"),
        }
    }

    #[test]
    fn test_no_iam_auth_with_manual_credentials() {
        let config = json!({
            "bucket": "my-test-bucket",
            "credentials": {
                "auth_type": "manual",
                "access_key": "AKIAIOSFODNN7EXAMPLE",
                "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
            }
        });

        let result = extract_iam_auth_from_connector_config(
            &config.to_string(),
            AWS_SCHEMA_WITH_DISCRIMINATOR,
        )
        .unwrap();
        // Should return None because this is manual credentials, not IAM auth
        assert!(result.is_none());
    }

    #[test]
    fn test_no_iam_provider_annotation() {
        let config = json!({
            "project_id": "my-project",
            "authentication": {
                "method": "service_account_key",
                "credentials_json": "..."
            }
        });

        let result = extract_iam_auth_from_connector_config(
            &config.to_string(),
            GCP_SCHEMA_WITH_DISCRIMINATOR,
        )
        .unwrap();
        // Should return None because service_account_key method doesn't have x-iam-auth-provider
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
    fn test_one_of_schemas_with_overlapping_properties() {
        // Test case where credentials has oneOf with two schemas:
        // 1. First schema: same property names but NO IAM annotations
        // 2. Second schema: same property names but WITH IAM annotations
        // This tests whether doc::Shape properly processes both schemas or only the first
        let schema_with_oneof_overlap = r#"{
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
                                "auth_type": {
                                    "type": "string",
                                    "const": "manual"
                                },
                                "role_arn": {
                                    "type": "string",
                                    "title": "Role ARN (no annotations)"
                                },
                                "external_id": {
                                    "type": "string", 
                                    "title": "External ID (no annotations)"
                                },
                                "region": {
                                    "type": "string",
                                    "title": "Region (no annotations)"
                                }
                            }
                        },
                        {
                            "title": "AWS IAM",
                            "properties": {
                                "auth_type": {
                                    "type": "string",
                                    "const": "aws_iam",
                                    "x-iam-auth-provider": "aws"
                                },
                                "role_arn": {
                                    "type": "string",
                                    "title": "IAM Role ARN",
                                    "x-aws-role-arn": true
                                },
                                "external_id": {
                                    "type": "string",
                                    "title": "External ID",
                                    "x-aws-external-id": true
                                },
                                "region": {
                                    "type": "string",
                                    "title": "AWS Region", 
                                    "x-aws-region": true
                                }
                            }
                        }
                    ]
                }
            },
            "required": ["bucket", "credentials"]
        }"#;

        let config = json!({
            "bucket": "my-test-bucket",
            "credentials": {
                "auth_type": "aws_iam",
                "role_arn": "arn:aws:iam::123456789012:role/FlowConnectorRole",
                "external_id": "unique-external-id",
                "region": "us-west-2"
            }
        });

        let result =
            extract_iam_auth_from_connector_config(&config.to_string(), schema_with_oneof_overlap)
                .unwrap();

        // This should find the IAM config from the second oneOf schema
        assert!(
            result.is_some(),
            "Should find IAM config from second oneOf schema"
        );
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
                "auth_type": "aws_iam",
                "role_arn": "arn:aws:iam::123456789012:role/FlowS3Role",
                "external_id": "flow-external-id-123",
                "region": "us-east-1"
            }
        });

        let schema = AWS_SCHEMA_WITH_DISCRIMINATOR;

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
}
