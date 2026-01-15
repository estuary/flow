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
