use crate::{Id, TextJson};
use anyhow::Context;
use serde_json::value::RawValue;
use sqlx::types::Uuid;

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
        select
            id as "id: Id",
            data_plane_name,
            data_plane_name = $2 and exists(
                select 1 from internal.user_roles($3, 'read') r
                where starts_with($2, r.role_prefix)
            ) as "is_default!: bool",
            hmac_keys,
            fqdn,
            broker_address,
            reactor_address,
            ops_logs_name as "ops_logs_name: models::Collection",
            ops_stats_name as "ops_stats_name: models::Collection"
        from data_planes
        where id in (select id from unnest($1::flowid[]) id)
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

pub async fn fetch_data_plane_by_task_and_fqdn(
    pool: &sqlx::PgPool,
    task_shard: &str,
    task_data_plane_fqdn: &str,
) -> sqlx::Result<Option<tables::DataPlane>> {
    sqlx::query_as!(
        tables::DataPlane,
        r#"
        select
            d.id as "id: Id",
            d.data_plane_name,
            false as "is_default!: bool",
            d.hmac_keys,
            d.fqdn,
            d.broker_address,
            d.reactor_address,
            d.ops_logs_name as "ops_logs_name: models::Collection",
            d.ops_stats_name as "ops_stats_name: models::Collection"
        from data_planes d
        join live_specs t on t.data_plane_id = d.id
        where d.fqdn = $2 and starts_with($1::text, t.catalog_name)
        "#,
        task_shard,
        task_data_plane_fqdn,
    )
    .fetch_optional(pool)
    .await
}

pub async fn verify_task_authorization(
    pool: &sqlx::PgPool,
    task_shard: &str,
    journal_name_or_prefix: &str,
    required_role: &str,
) -> sqlx::Result<Option<(String, models::Collection, models::Id, bool)>> {
    let r = sqlx::query!(
        r#"
        select
            t.catalog_name as "task_name: String",
            c.catalog_name as "collection_name: models::Collection",
            c.data_plane_id as "collection_data_plane_id: models::Id",
            exists(
                select 1
                from internal.task_roles($1, $3::text::grant_capability) r
                where starts_with($2, r.role_prefix)
            ) as "authorized!: bool"
        from live_specs t, live_specs c
        where starts_with($1, t.catalog_name)
          and starts_with($2, c.catalog_name)
        "#,
        task_shard,
        journal_name_or_prefix,
        required_role,
    )
    .fetch_optional(pool)
    .await?;

    Ok(r.map(|r| {
        (
            r.task_name,
            r.collection_name,
            r.collection_data_plane_id,
            r.authorized,
        )
    }))
}
