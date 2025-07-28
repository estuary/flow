use zeroize::Zeroize;

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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
}