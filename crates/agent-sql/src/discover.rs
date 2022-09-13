use super::{Id, TextJson as Json};

use chrono::prelude::*;
use serde::Serialize;
use serde_json::value::RawValue;
use sqlx::types::Uuid;

// Row is the dequeued task shape of a discover operation.
#[derive(Debug)]
pub struct Row {
    pub capture_name: String,
    pub connector_tag_id: Id,
    pub connector_tag_job_success: bool,
    pub created_at: DateTime<Utc>,
    pub draft_id: Id,
    pub endpoint_config: Json<Box<RawValue>>,
    pub id: Id,
    pub image_name: String,
    pub image_tag: String,
    pub logs_token: Uuid,
    pub protocol: String,
    pub updated_at: DateTime<Utc>,
    pub user_id: Uuid,
}

pub async fn dequeue(txn: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> sqlx::Result<Option<Row>> {
    sqlx::query_as!(
      Row,
      // TODO(johnny): If we stored `docker inspect` output within connector_tags,
      // we could pull a resolved digest directly from it?
      // Better: have `flowctl api spec` run it internally and surface the digest?
      r#"select
          discovers.capture_name,
          discovers.connector_tag_id as "connector_tag_id: Id",
          connector_tags.job_status->>'type' = 'success' as "connector_tag_job_success!",
          discovers.created_at,
          discovers.draft_id as "draft_id: Id",
          discovers.endpoint_config as "endpoint_config: Json<Box<RawValue>>",
          discovers.id as "id: Id",
          connectors.image_name,
          connector_tags.image_tag,
          discovers.logs_token,
          connector_tags.protocol as "protocol!",
          discovers.updated_at,
          drafts.user_id
      from discovers
      join drafts on discovers.draft_id = drafts.id
      join connector_tags on discovers.connector_tag_id = connector_tags.id
      join connectors on connectors.id = connector_tags.connector_id
      where discovers.job_status->>'type' = 'queued' and connector_tags.job_status->>'type' != 'queued'
      order by discovers.id asc
      limit 1
      for update of discovers skip locked;
      "#
  )
  .fetch_optional(txn).await
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
        r#"update discovers set
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
