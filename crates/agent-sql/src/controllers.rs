use crate::{CatalogType, Id, TextJson};
use chrono::prelude::*;
use serde::Serialize;
use serde_json::value::RawValue;
use sqlx::types::Uuid;
use std::fmt::Debug;

#[derive(Debug)]
pub struct ControllerJob {
    pub live_spec_id: Id,
    pub catalog_name: String,
    pub last_pub_id: Id,
    pub live_spec: Option<TextJson<Box<RawValue>>>,
    pub built_spec: Option<TextJson<Box<RawValue>>>,
    pub spec_type: Option<CatalogType>,
    pub controller_next_run: Option<DateTime<Utc>>,
    pub controller_version: i32,
    pub updated_at: DateTime<Utc>,
    pub logs_token: Uuid,
    pub status: TextJson<Box<RawValue>>,
    pub failures: i32,
    pub error: Option<String>,
}

#[tracing::instrument(level = "debug", skip(txn))]
pub async fn dequeue(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    controller_version: i32,
) -> sqlx::Result<Option<ControllerJob>> {
    sqlx::query_as!(
        ControllerJob,
        r#"with needs_periodic(live_spec_id) as (
            select id as live_spec_id
            from live_specs
            where
                -- This condition is required in order to for this query to use the sparse index
                controller_next_run is not null
                and controller_next_run <= now()
            order by controller_next_run asc
        ),
        needs_upgrade(live_spec_id) as (
            select live_spec_id
            from controller_jobs cj
            join live_specs ls on cj.live_spec_id = ls.id
            where cj.controller_version < $1
            -- This condition is needed in order to respect the backoff when the upgrade run of a
            -- controller fails. If a spec has a controller_next_run, then that should always
            -- determine the time of the next run.
            and ls.controller_next_run is null
        ),
        next(live_spec_id) as (
            -- Scheduled runs take precedence over upgrades
            select live_spec_id
            from needs_periodic
            union
            select live_spec_id
            from needs_upgrade
        )
        select
            next.live_spec_id as "live_spec_id!: Id",
            ls.catalog_name as "catalog_name!: String",
            ls.controller_next_run,
            ls.last_pub_id as "last_pub_id: Id",
            ls.spec as "live_spec: TextJson<Box<RawValue>>",
            ls.built_spec as "built_spec: TextJson<Box<RawValue>>",
            ls.spec_type as "spec_type: CatalogType",
            cj.controller_version as "controller_version: i32",
            cj.updated_at,
            cj.logs_token,
            cj.status as "status: TextJson<Box<RawValue>>",
            cj.failures,
            cj.error
        from next
        join controller_jobs cj on next.live_spec_id = cj.live_spec_id
        join live_specs ls on next.live_spec_id = ls.id
        limit 1
        for update of cj skip locked;
        "#,
        controller_version as i32,
    )
    .fetch_optional(txn)
    .await
}

#[tracing::instrument(level = "debug", skip(txn, status, controller_version))]
pub async fn update<S: Serialize + Send + Sync + Debug>(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    live_spec_id: Id,
    controller_version: i32,
    status: S,
    failures: i32,
    error: Option<&str>,
    expect_next_run: Option<DateTime<Utc>>,
    set_next_run: Option<DateTime<Utc>>,
) -> sqlx::Result<()> {
    sqlx::query!(
        r#"
        with update_next_run as (
            update live_specs
            set controller_next_run = case
                when controller_next_run is not distinct from $6 then $7
                else controller_next_run end
            where id = $1
        )
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
        TextJson(status) as TextJson<S>,
        failures,
        error,
        expect_next_run,
        set_next_run,
    )
    .execute(txn)
    .await?;
    Ok(())
}

// TODO(phil): We may want to change to debug level once we gain more confidence.
/// Trigger a controller sync of all dependents of the given `catalog_name` that have not already
/// been published at the given `publication_id`. This will not update any dependents that already
/// have a `controller_next_run` set to an earlier time than the given `next_run`.
#[tracing::instrument(level = "info", err, ret, skip(pool))]
pub async fn notify_dependents(
    catalog_name: &str,
    next_run: DateTime<Utc>,
    pool: &sqlx::PgPool,
) -> sqlx::Result<u64> {
    // If the catalog_name is a source, then notify all all targets, but only if the flow_type is
    // not 'capture'. Capture flows treat the capture as the source. But in terms of publication
    // dependencies, the capture depends on the collection, not the other way around. (Because the
    // capture spec embeds the collection spec.)
    let result = sqlx::query!(
        r#"
        with dependents as (
            select lsf.target_id as id
            from live_specs ls
            join live_spec_flows lsf on ls.id = lsf.source_id
            where ls.catalog_name = $1 and lsf.flow_type != 'capture'
            union
            select lsf.source_id as id
            from live_specs ls
            join live_spec_flows lsf on ls.id = lsf.target_id
            where ls.catalog_name = $1 and lsf.flow_type = 'capture'
        ),
        filtered as (
            select dependents.id
            from dependents
            join live_specs ls on dependents.id = ls.id
            where (ls.controller_next_run is null or ls.controller_next_run > $2)
        )
        update live_specs set controller_next_run = $2
        from filtered
        where live_specs.id = filtered.id;
        "#,
        catalog_name,
        next_run,
    )
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}
