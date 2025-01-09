use crate::TextJson;
use chrono::prelude::*;
use models::{CatalogType, Id};
use serde_json::value::RawValue;
use sqlx::types::Uuid;
use std::fmt::Debug;

#[derive(Debug)]
pub struct ControllerJob {
    pub live_spec_id: Id,
    pub catalog_name: String,
    pub last_pub_id: Id,
    pub last_build_id: Id,
    pub live_spec: Option<TextJson<Box<RawValue>>>,
    pub built_spec: Option<TextJson<Box<RawValue>>>,
    pub spec_type: Option<CatalogType>,
    pub controller_version: i32,
    pub controller_updated_at: DateTime<Utc>,
    pub controller_next_run: Option<DateTime<Utc>>,
    pub live_spec_updated_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub logs_token: Uuid,
    pub status: TextJson<Box<RawValue>>,
    pub failures: i32,
    pub error: Option<String>,
    pub data_plane_id: Id,
    pub data_plane_name: Option<String>,
    pub live_dependency_hash: Option<String>,
}

pub async fn fetch_controller_job(
    controller_task_id: Id,
    db: impl sqlx::PgExecutor<'static>,
) -> sqlx::Result<ControllerJob> {
    sqlx::query_as!(
        ControllerJob,
        r#"select
            ls.id as "live_spec_id: Id",
            ls.catalog_name as "catalog_name!: String",
            ls.last_pub_id as "last_pub_id: Id",
            ls.last_build_id as "last_build_id: Id",
            ls.spec as "live_spec: TextJson<Box<RawValue>>",
            ls.built_spec as "built_spec: TextJson<Box<RawValue>>",
            ls.spec_type as "spec_type: CatalogType",
            ls.dependency_hash as "live_dependency_hash",
            ls.created_at,
            ls.updated_at as "live_spec_updated_at",
            -- TODO(phil): remove controller_next_run after legacy agents no longer need it.
            ls.controller_next_run,
            cj.controller_version as "controller_version: i32",
            cj.updated_at as "controller_updated_at",
            cj.logs_token,
            cj.status as "status: TextJson<Box<RawValue>>",
            cj.failures,
            cj.error,
            ls.data_plane_id as "data_plane_id: Id",
            dp.data_plane_name as "data_plane_name?: String"
        from internal.tasks t
        join live_specs ls on t.task_id = ls.controller_task_id
        join controller_jobs cj on ls.id = cj.live_spec_id
        left outer join data_planes dp on ls.data_plane_id = dp.id
        where t.task_id = $1::flowid;"#,
        controller_task_id as Id,
    )
    .fetch_one(db)
    .await
}

#[tracing::instrument(level = "debug", skip(txn, status, controller_version))]
pub async fn update_status(
    txn: &mut sqlx::PgConnection,
    live_spec_id: Id,
    controller_version: i32,
    status: &models::status::ControllerStatus,
    failures: i32,
    error: Option<&str>,
) -> sqlx::Result<()> {
    sqlx::query!(
        r#"
        insert into controller_jobs(live_spec_id, controller_version, status, failures, error)
        values ($1, $2, $3, $4, $5)
        on conflict (live_spec_id) do update set
            controller_version = $2,
            status = $3,
            failures = $4,
            error = $5,
            updated_at = now()
        where controller_jobs.live_spec_id = $1;
        "#,
        live_spec_id as Id,
        controller_version as i32,
        status as &models::status::ControllerStatus,
        failures,
        error,
    )
    .execute(txn)
    .await?;
    Ok(())
}

/// Trigger a controller sync of all dependents of the given `catalog_name` that have not already
/// been published at the given `publication_id`. This will not update any dependents that already
/// have a `controller_next_run` set to an earlier time than the given `next_run`.
#[tracing::instrument(err, ret, skip(pool))]
pub async fn notify_dependents(live_spec_id: Id, pool: &sqlx::PgPool) -> sqlx::Result<u64> {
    // If the spec is a source, then notify all all targets, but only if the flow_type is
    // not 'capture'. Capture flows treat the capture as the source. But in terms of publication
    // dependencies, the capture depends on the collection, not the other way around. (Because the
    // capture spec embeds the collection spec.)
    // We send a zero-valued id as the sender in `send_to_task` because we don't
    // currently use the sender for anything, so it doesn't seem worthwhile to
    // thread it through.
    let result = sqlx::query!(
        r#"
        with dependents as (
            select lsf.target_id as id
            from live_spec_flows lsf
            where lsf.source_id = $1 and lsf.flow_type != 'capture'
            union
            select lsf.source_id as id
            from live_spec_flows lsf
            where lsf.target_id = $1 and lsf.flow_type = 'capture'
        ),
        dependent_tasks as (
            select ls.controller_task_id
            from dependents
            join live_specs ls on dependents.id = ls.id
        )
        select internal.send_to_task(
            dependent_tasks.controller_task_id,
            '0000000000000000'::flowid,
            '{"type":"dependency_updated"}'
        )
        from dependent_tasks
        "#,
        live_spec_id as Id,
    )
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}
