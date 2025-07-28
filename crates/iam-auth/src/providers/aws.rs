use anyhow::Context;
use aws_sdk_sts::error::ProvideErrorMetadata;
use zeroize::Zeroize;

use crate::config::AWSConfig;
use crate::providers::gcp::google_sign_jwt;
use crate::tokens::AWSTokens;

/// Generate AWS temporary credentials using STS AssumeRoleWithWebIdentity
pub async fn generate_tokens(config: &AWSConfig, task_name: &str) -> anyhow::Result<AWSTokens> {
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
    // AWS role session names must match pattern [\\w+=,.@-]*
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