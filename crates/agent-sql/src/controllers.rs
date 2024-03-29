use crate::TextJson;
use chrono::prelude::*;
use serde::Serialize;
use serde_json::value::RawValue;
use sqlx::types::Uuid;
use std::fmt::Debug;

#[derive(Debug)]
pub struct ControllerJob {
    pub catalog_name: String,
    pub controller: String,
    pub next_run: Option<DateTime<Utc>>,
    pub active: bool,
    pub updated_at: DateTime<Utc>,
    pub logs_token: Uuid,
    pub status: TextJson<Box<RawValue>>,
    pub background: bool,
    pub failures: i32,
    pub error: Option<String>,
}

#[tracing::instrument(level = "debug", skip(txn))]
pub async fn dequeue(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    allow_background: bool,
) -> sqlx::Result<Option<ControllerJob>> {
    sqlx::query_as!(
        ControllerJob,
        r#"select
            catalog_name,
            controller,
            next_run,
            updated_at,
            logs_token,
            active,
            status as "status: TextJson<Box<RawValue>>",
            background,
            failures,
            error
        from controller_jobs
        where active = true and next_run < now() and (background = $1 or background = false)
        order by background asc, next_run asc
        limit 1
        for update of controller_jobs skip locked;
        "#,
        allow_background
    )
    .fetch_optional(txn)
    .await
}

#[tracing::instrument(level = "debug", skip(txn))]
pub async fn upsert<S: Serialize + Send + Sync + Debug>(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    catalog_name: &str,
    controller: &str,
    next_run: Option<DateTime<Utc>>,
    active: bool,
    status: Option<S>,
    failures: i32,
    error: Option<&str>,
) -> sqlx::Result<()> {
    sqlx::query!(
        r#"insert into controller_jobs (
        catalog_name,
        controller,
        next_run,
        active,
        status,
        failures,
        error
    ) values ($1, $2, $3, $4, $5, $6, $7)
    on conflict (catalog_name, controller) do update set
    next_run = $3,
    active = $4,
    status = case when excluded.status is not null then excluded.status else controller_jobs.status end,
    failures = $6,
    error = $7;"#,
        catalog_name as &str,
        controller,
        next_run,
        active,
        status.map(|s| TextJson(s)) as Option<TextJson<S>>,
        failures,
        error
    )
    .execute(txn)
    .await?;
    Ok(())
}
