use anyhow::Context;
use proto_gazette::broker;
use serde_json::value::RawValue;

pub async fn fetch_ops_journal_template(
    pool: &sqlx::PgPool,
    collection: &models::Collection,
) -> anyhow::Result<Option<proto_gazette::broker::JournalSpec>> {
    let r = sqlx::query!(
        r#"
        select
            built_spec as "built_spec: crate::TextJson<Box<RawValue>>"
        from live_specs
        where catalog_name = $1
          and spec_type = 'collection'
        "#,
        collection
    )
    .fetch_optional(pool)
    .await?;

    let Some(built) = r.and_then(|r| r.built_spec) else {
        return Ok(None);
    };
    let journal_spec = serde_json::from_str::<proto_flow::flow::CollectionSpec>(built.get())?
        .partition_template
        .context("partition_template must exist")?;
    Ok(Some(journal_spec))
}

/// Build an authenticated journal client for a data plane.
///
/// This creates a client that can make RPCs to the data plane's broker,
/// authenticated with HMAC-signed JWT claims.
pub fn build_journal_client(
    data_plane: &tables::DataPlane,
) -> anyhow::Result<gazette::journal::Client> {
    // Parse first data-plane HMAC key (used for signing tokens).
    let (encode_key, _decode) =
        tokens::jwt::parse_base64_hmac_keys(data_plane.hmac_keys.iter().take(1))
            .context("invalid data-plane HMAC key")?;

    let iat = tokens::now();
    let claims = proto_gazette::Claims {
        cap: proto_gazette::capability::LIST | proto_gazette::capability::READ,
        exp: (iat + tokens::TimeDelta::seconds(60)).timestamp() as u64,
        iat: iat.timestamp() as u64,
        iss: data_plane.data_plane_fqdn.clone(),
        sel: broker::LabelSelector::default(),
        sub: "control-plane-api".to_string(),
    };
    let token =
        tokens::jwt::sign(&claims, &encode_key).context("failed to sign claims for data-plane")?;

    let metadata = proto_grpc::Metadata::new()
        .with_bearer_token(&token)
        .expect("token is valid");

    let router = gazette::Router::new("local");
    let journal_client = gazette::journal::Client::new(
        data_plane.broker_address.clone(),
        gazette::journal::Client::new_fragment_client(),
        metadata,
        router,
    );

    Ok(journal_client)
}

/// Cloud provider where the data plane is hosted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, async_graphql::Enum)]
pub enum DataPlaneCloudProvider {
    Aws,
    Azure,
    Gcp,
    Local,
}

/// Parses a data plane name into its component parts.
/// Returns None if the name format is invalid.
///
/// Expected formats:
/// - Cloud: "ops/dp/public/aws-us-east-1-c1" or "ops/dp/private/gcp-us-central1-c2"
/// - Local: "ops/dp/local/local-foo" (any suffix after "local-")
pub(crate) fn parse_data_plane_name(
    name: &str,
) -> Option<(DataPlaneCloudProvider, String, String, bool)> {
    let last_segment = name.rsplit('/').next()?;
    let (provider_str, after_provider) = last_segment.split_once('-')?;

    match provider_str {
        "local" => Some((
            DataPlaneCloudProvider::Local,
            "local".to_string(),
            "c1".to_string(),
            true,
        )),
        "aws" | "az" | "azure" | "gcp" => {
            // Must have privacy indicator in path.
            if !name.contains("ops/dp/private/") && !name.starts_with("ops/dp/public/") {
                return None;
            }

            // Parse tag (cluster) suffix (e.g., "-c1", "-c5").
            let idx = after_provider.rfind("-c")?;
            let tag = &after_provider[idx + 1..];
            if tag.len() < 2 || !tag[1..].chars().all(|c| c.is_ascii_digit()) {
                return None;
            }

            let region = &after_provider[..idx];
            if region.is_empty() {
                return None;
            }

            let cloud_provider = match provider_str {
                "aws" => DataPlaneCloudProvider::Aws,
                "az" | "azure" => DataPlaneCloudProvider::Azure,
                "gcp" => DataPlaneCloudProvider::Gcp,
                _ => unreachable!(),
            };

            let is_public = name.starts_with("ops/dp/public/");
            Some((
                cloud_provider,
                region.to_string(),
                tag.to_string(),
                is_public,
            ))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_aws_public() {
        let (provider, region, tag, is_public) =
            parse_data_plane_name("ops/dp/public/aws-us-east-1-c1").unwrap();
        assert_eq!(provider, DataPlaneCloudProvider::Aws);
        assert_eq!(region, "us-east-1");
        assert_eq!(tag, "c1");
        assert!(is_public);
    }

    #[test]
    fn parses_gcp_private() {
        let (provider, region, tag, is_public) =
            parse_data_plane_name("ops/dp/private/estuary/gcp-us-central1-c5").unwrap();
        assert_eq!(provider, DataPlaneCloudProvider::Gcp);
        assert_eq!(region, "us-central1");
        assert_eq!(tag, "c5");
        assert!(!is_public);
    }

    #[test]
    fn parses_azure_variants() {
        // "az" prefix
        let (provider, region, tag, _) =
            parse_data_plane_name("ops/dp/private/EastPack/az-australiaeast-c1").unwrap();
        assert_eq!(provider, DataPlaneCloudProvider::Azure);
        assert_eq!(region, "australiaeast");
        assert_eq!(tag, "c1");

        // "azure" prefix
        let (provider, region, tag, _) =
            parse_data_plane_name("ops/dp/private/AccumTech/azure-eastus-c1").unwrap();
        assert_eq!(provider, DataPlaneCloudProvider::Azure);
        assert_eq!(region, "eastus");
        assert_eq!(tag, "c1");
    }

    #[test]
    fn parses_local() {
        let (provider, region, tag, is_public) =
            parse_data_plane_name("ops/dp/local/local-foo").unwrap();
        assert_eq!(provider, DataPlaneCloudProvider::Local);
        assert_eq!(region, "local");
        assert_eq!(tag, "c1");
        assert!(is_public);
    }

    #[test]
    fn rejects_invalid_names() {
        // Missing privacy indicator
        assert!(parse_data_plane_name("ops/dp/aws-us-east-1-c1").is_none());
        // Unknown provider
        assert!(parse_data_plane_name("ops/dp/public/unknown-us-east-1-c1").is_none());
        // Missing cluster suffix
        assert!(parse_data_plane_name("ops/dp/public/aws-us-east-1").is_none());
        // Non-numeric cluster
        assert!(parse_data_plane_name("ops/dp/public/aws-us-east-1-ca").is_none());
    }
}
