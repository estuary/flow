use std::collections::HashMap;

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
    pub data_plane_name: String,
    pub draft_id: Id,
    pub endpoint_config: Json<Box<RawValue>>,
    pub id: Id,
    pub image_name: String,
    pub image_tag: String,
    pub logs_token: Uuid,
    pub protocol: String,
    pub update_only: bool,
    pub updated_at: DateTime<Utc>,
    pub user_id: Uuid,
    pub auto_publish: bool,
    pub auto_evolve: bool,
    pub background: bool,
}

#[tracing::instrument(level = "debug", skip(txn))]
pub async fn dequeue(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    allow_background: bool,
) -> sqlx::Result<Option<Row>> {
    sqlx::query_as!(
      Row,
      r#"select
          discovers.capture_name,
          discovers.connector_tag_id as "connector_tag_id: Id",
          connector_tags.job_status->>'type' = 'success' as "connector_tag_job_success!",
          discovers.created_at,
          discovers.data_plane_name,
          discovers.draft_id as "draft_id: Id",
          discovers.endpoint_config as "endpoint_config: Json<Box<RawValue>>",
          discovers.id as "id: Id",
          connectors.image_name,
          connector_tags.image_tag,
          discovers.logs_token,
          connector_tags.protocol as "protocol!",
          discovers.update_only,
          discovers.updated_at,
          discovers.auto_publish,
          discovers.auto_evolve,
          drafts.user_id,
          discovers.background
      from discovers
      join drafts on discovers.draft_id = drafts.id
      join connector_tags on discovers.connector_tag_id = connector_tags.id
      join connectors on connectors.id = connector_tags.connector_id
      where discovers.job_status->>'type' = 'queued' and connector_tags.job_status->>'type' != 'queued'
          and (discovers.background = $1 or discovers.background = false)
      order by discovers.background asc, discovers.id asc
      limit 1
      for update of discovers skip locked;
      "#,
      allow_background
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

/// Returns a map of catalog_name to md5 hash of the live spec. The map will only
/// include entities that exist and have a non-null md5 hash.
pub async fn fetch_spec_md5_hashes(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    spec_names: Vec<&str>,
) -> sqlx::Result<HashMap<String, String>> {
    let rows = sqlx::query!(
        r#"
            select
                ls.catalog_name,
                ls.md5
            from live_specs ls
            where ls.catalog_name = any ($1::text[]);
        "#,
        spec_names as Vec<&str>
    )
    .fetch_all(txn)
    .await?;

    let out = rows
        .into_iter()
        .filter_map(|r| r.md5.map(|md5| (r.catalog_name, md5)))
        .collect();
    Ok(out)
}
