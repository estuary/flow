use super::{Id, TextJson as Json};

use chrono::prelude::*;
use serde::Serialize;
use serde_json::value::RawValue;
use sqlx::types::Uuid;

pub mod accept_demo_tenant;
pub mod beta_onboard;
pub mod grant;
pub mod storage_mappings;

// Row is the dequeued task shape of an applied directive operation. We don't currently have a use
// for background directive applications, so the `background` column is omitted.
#[derive(Debug)]
pub struct Row {
    pub apply_id: Id,
    pub apply_updated_at: DateTime<Utc>,
    pub catalog_prefix: String,
    pub directive_id: Id,
    pub directive_spec: Json<Box<RawValue>>,
    pub logs_token: Uuid,
    pub user_claims: Json<Box<RawValue>>,
    pub user_id: Uuid,
}

#[tracing::instrument(level = "debug", skip(txn))]
pub async fn dequeue(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    allow_background: bool,
) -> sqlx::Result<Option<Row>> {
    // We don't currently have a use for `background` directive applications, but this function
    // still handles them as if we may.
    sqlx::query_as!(
        Row,
        r#"select
            a.id as "apply_id: Id",
            a.updated_at as "apply_updated_at",
            d.catalog_prefix as "catalog_prefix",
            d.id as "directive_id: Id",
            d.spec as "directive_spec: Json<Box<RawValue>>",
            a.logs_token,
            a.user_claims as "user_claims!: Json<Box<RawValue>>",
            a.user_id as "user_id"
        from directives as d
        join applied_directives as a on d.id = a.directive_id
        -- The user must supply claims before we can dequeue the application.
        where a.job_status->>'type' = 'queued' and a.user_claims is not null and (a.background = $1 or a.background = false)
        order by a.background, a.id asc
        limit 1
        for update of a skip locked;
        "#,
        allow_background
    )
    .fetch_optional(txn)
    .await
}

pub async fn resolve<S>(
    id: Id,
    status: S,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<()>
where
    S: Serialize + Send + Sync,
{
    sqlx::query!(
        r#"update applied_directives set
            job_status = $2,
            updated_at = clock_timestamp()
        where id = $1
        returning 1 as "must_exist";
        "#,
        id as Id,
        Json(status) as Json<S>,
    )
    .fetch_one(txn)
    .await?;

    Ok(())
}
