pub mod accept_demo_tenant;
pub mod beta_onboard;
pub mod grant;
pub mod storage_mappings;

use crate::TextJson;
use chrono::{DateTime, Utc};
use models::Id;
use serde::Serialize;
use serde_json::value::RawValue;
use sqlx::types::Uuid;

// Row is the dequeued task shape of an applied directive operation. We don't currently have a use
// for background directive applications, so the `background` column is omitted.
#[derive(Debug)]
pub struct Row {
    pub apply_id: Id,
    pub apply_updated_at: DateTime<Utc>,
    pub catalog_prefix: String,
    pub directive_id: Id,
    pub directive_spec: TextJson<Box<RawValue>>,
    pub logs_token: Uuid,
    pub user_claims: TextJson<Box<RawValue>>,
    pub user_id: Uuid,
    pub status_type: Option<String>,
}

pub async fn fetch_directive(
    task_id: Id,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<Row> {
    sqlx::query_as!(
        Row,
        r#"select
            a.id as "apply_id: Id",
            a.updated_at as "apply_updated_at",
            d.catalog_prefix as "catalog_prefix",
            d.id as "directive_id: Id",
            d.spec as "directive_spec: TextJson<Box<RawValue>>",
            a.logs_token,
            a.user_claims as "user_claims!: TextJson<Box<RawValue>>",
            a.user_id as "user_id",
            a.job_status->>'type' as "status_type"
        from directives as d
        join applied_directives as a on d.id = a.directive_id
        where a.id = $1::flowid
        for update of a;
        "#,
        task_id as Id,
    )
    .fetch_one(&mut **txn)
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
        TextJson(status) as TextJson<S>,
    )
    .fetch_one(&mut **txn)
    .await?;

    Ok(())
}
