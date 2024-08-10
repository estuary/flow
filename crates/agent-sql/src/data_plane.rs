use crate::{Id, TextJson};
use anyhow::Context;
use serde_json::value::RawValue;
use sqlx::types::Uuid;

#[tracing::instrument(level = "info", err, ret, skip(pool))]
pub async fn fetch_ops_journal_template(
    pool: &sqlx::PgPool,
    collection: &models::Collection,
) -> anyhow::Result<proto_gazette::broker::JournalSpec> {
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
        anyhow::bail!("collection {collection} is required to exist and does not");
    };
    Ok(
        serde_json::from_str::<proto_flow::flow::CollectionSpec>(built.get())?
            .partition_template
            .context("partition_template must exist")?,
    )
}

#[tracing::instrument(level = "info", err, ret, skip(pool))]
pub async fn fetch_data_planes(
    pool: &sqlx::PgPool,
    mut data_plane_ids: Vec<models::Id>,
    default_data_plane_name: &str,
    user_id: Uuid,
) -> sqlx::Result<tables::DataPlanes> {
    data_plane_ids.sort();
    data_plane_ids.dedup();

    let r = sqlx::query_as!(
        tables::DataPlane,
        r#"
        with auth_check as (
            select 1 from internal.user_roles($3, 'read') r
            where starts_with($2, r.role_prefix)
        )
        select
            id as "id: Id",
            data_plane_name = $2 and exists(select 1 from auth_check) as "is_default!: bool",
            hmac_key,
            broker_address,
            reactor_address,
            ops_logs_name as "ops_logs_name: models::Collection",
            ops_stats_name as "ops_stats_name: models::Collection"
        from data_planes
        where id in (select id from unnest($1::flowid[]))
           or data_plane_name = $2
        "#,
        &data_plane_ids as &[Id],
        default_data_plane_name,
        user_id as Uuid,
    )
    .fetch_all(pool)
    .await?;

    Ok(r.into_iter().collect())
}
