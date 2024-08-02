use crate::{Id, TextJson};
use serde_json::value::RawValue;
use sqlx::types::Uuid;

#[derive(Debug)]
pub struct DataPlane {
    pub data_plane_id: Id,
    pub ops_logs_spec: Option<TextJson<Box<RawValue>>>,
    pub ops_stats_spec: Option<TextJson<Box<RawValue>>>,
    pub hmac_key: String,
    pub broker_address: String,
    pub reactor_address: String,
}

#[tracing::instrument(level = "info", err, ret, skip(pool))]
pub async fn fetch_metadata(data_plane_id: Id, pool: &sqlx::PgPool) -> sqlx::Result<DataPlane> {
    sqlx::query_as!(
        DataPlane,
        r#"
        select
            dp.id as "data_plane_id: Id",
            dp.hmac_key,
            dp.broker_address,
            dp.reactor_address,
            ol.built_spec as "ops_logs_spec: TextJson<Box<RawValue>>",
            os.built_spec as "ops_stats_spec: TextJson<Box<RawValue>>"
        from data_planes dp
        left join live_specs ol on dp.ops_logs_name = ol.catalog_name
        left join live_specs os on dp.ops_stats_name = os.catalog_name
        where dp.id = $1
        "#,
        data_plane_id as Id
    )
    .fetch_one(pool)
    .await
}

#[tracing::instrument(level = "info", err, ret, skip(pool))]
pub async fn resolve_authorized_data_plane(
    data_plane_name: &str,
    user_id: Uuid,
    pool: &sqlx::PgPool,
) -> sqlx::Result<Option<Id>> {
    // Special-case support for legacy managed data plane.
    if data_plane_name.is_empty() {
        return Ok(Some(Id::new([0; 8])));
    }

    let r = sqlx::query!(
        r#"
        select id as "id: Id" from data_planes
        where data_plane_name = $1 and exists(
            select 1 from internal.user_roles($2, 'admin') r
            where starts_with($1, r.role_prefix)
        )
        "#,
        data_plane_name,
        user_id as Uuid,
    )
    .fetch_optional(pool)
    .await?;

    Ok(r.map(|r| r.id))
}
