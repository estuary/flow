use super::{CatalogType, Id, TextJson as Json};
use chrono::prelude::*;
use serde::Serialize;
use serde_json::value::RawValue;
use sqlx::types::Uuid;

// Row is the dequeued task shape of a discover operation.
#[derive(Debug)]
pub struct Row {
    pub created_at: DateTime<Utc>,
    pub draft_id: Id,
    pub id: Id,
    pub logs_token: Uuid,
    pub updated_at: DateTime<Utc>,
    pub num_documents: i32,
    pub collection_name: String,
}

pub async fn dequeue(txn: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> sqlx::Result<Option<Row>> {
    sqlx::query_as!(
      Row,
      r#"select
          derivation_previews.created_at,
          derivation_previews.draft_id as "draft_id: Id",
          derivation_previews.id as "id: Id",
          derivation_previews.logs_token,
          derivation_previews.updated_at,
          derivation_previews.num_documents,
          derivation_previews.collection_name
      from derivation_previews
      where derivation_previews.job_status->>'type' = 'queued'
      order by derivation_previews.id asc
      limit 1
      for update of derivation_previews skip locked;
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
        r#"update derivation_previews set
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
