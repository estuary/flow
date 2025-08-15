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
}

pub async fn fetch_discover(id: Id, db: &sqlx::PgPool) -> sqlx::Result<Row> {
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
            drafts.user_id
        from discovers
        join drafts on discovers.draft_id = drafts.id
        join connector_tags on discovers.connector_tag_id = connector_tags.id
        join connectors on connectors.id = connector_tags.connector_id
        where discovers.id = $1::flowid;
        "#,
        id as Id
    )
    .fetch_one(db)
    .await
}

pub async fn resolve<S>(id: Id, status: S, txn: &mut sqlx::PgConnection) -> sqlx::Result<()>
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
    .fetch_all(&mut **txn)
    .await?;

    let out = rows
        .into_iter()
        .filter_map(|r| r.md5.map(|md5| (r.catalog_name, md5)))
        .collect();
    Ok(out)
}
