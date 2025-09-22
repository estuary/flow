use crate::{Id, TextJson};
use anyhow::Context;
use serde_json::value::RawValue;

pub async fn fetch_ops_journal_template(
    pool: &sqlx::PgPool,
    collection: &models::Collection,
) -> anyhow::Result<Option<proto_gazette::broker::JournalSpec>> {
    let r = sqlx::query!(
        r#"
        select
            built_spec as "built_spec: TextJson<Box<RawValue>>"
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

pub async fn fetch_data_plane<'a>(
    pool: impl sqlx::PgExecutor<'a>,
    data_plane_id: models::Id,
) -> anyhow::Result<tables::DataPlane> {
    sqlx::query_as!(
        tables::DataPlane,
        r#"
        SELECT
            d.id AS "control_id: Id",
            d.data_plane_name,
            d.hmac_keys,
            d.encrypted_hmac_keys AS "encrypted_hmac_keys: models::RawValue",
            d.data_plane_fqdn,
            d.broker_address,
            d.reactor_address,
            d.ops_logs_name AS "ops_logs_name: models::Collection",
            d.ops_stats_name AS "ops_stats_name: models::Collection"
        FROM data_planes d
        WHERE id = $1
        "#,
        data_plane_id as models::Id,
    )
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to fetch data-plane {data_plane_id}"))
}

pub async fn fetch_all_data_planes<'a, 'b>(
    pool: impl sqlx::PgExecutor<'a>,
) -> sqlx::Result<tables::DataPlanes> {
    let r = sqlx::query_as!(
        tables::DataPlane,
        r#"
        SELECT
            d.id AS "control_id: Id",
            d.data_plane_name,
            d.hmac_keys,
            d.encrypted_hmac_keys AS "encrypted_hmac_keys: models::RawValue",
            d.data_plane_fqdn,
            d.broker_address,
            d.reactor_address,
            d.ops_logs_name AS "ops_logs_name: models::Collection",
            d.ops_stats_name AS "ops_stats_name: models::Collection"
        FROM data_planes d
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(r.into_iter().collect())
}
