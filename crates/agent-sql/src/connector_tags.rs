use super::{Id, TextJson as Json};

use chrono::prelude::*;
use serde::Serialize;
use serde_json::value::RawValue;
use sqlx::{types::Uuid, FromRow};

// Row is the dequeued task shape of a tag connector operation.
#[derive(Debug)]
pub struct Row {
    pub connector_id: Id,
    pub created_at: DateTime<Utc>,
    pub external_url: String,
    pub image_name: String,
    pub image_tag: String,
    pub logs_token: Uuid,
    pub tag_id: Id,
    pub updated_at: DateTime<Utc>,
}

pub async fn dequeue(txn: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> sqlx::Result<Option<Row>> {
    sqlx::query_as!(
        Row,
        r#"select
            c.id as "connector_id: Id",
            c.external_url,
            c.image_name,
            t.created_at,
            t.id as "tag_id: Id",
            t.image_tag,
            t.logs_token,
            t.updated_at
        from connector_tags as t
        join connectors as c on c.id = t.connector_id
        where t.job_status->>'type' = 'queued'
        order by t.id asc
        limit 1
        for update of t skip locked;
        "#
    )
    .fetch_optional(txn)
    .await
}

#[derive(Debug, FromRow, Serialize)]
pub struct UnknownConnector {
    pub catalog_name: String,
    pub image_name: String,
}

pub async fn resolve_unknown_connectors(
    live_spec_ids: Vec<Id>,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<Vec<UnknownConnector>> {
    let res = sqlx::query_as::<_, UnknownConnector>(
        r#"select
            live_spec.connector_image_name as "image_name",
            live_spec.catalog_name
        from live_specs as live_spec
        left join connectors as connector on connector.image_name = live_spec.connector_image_name
        where live_spec.id = ANY($1) and live_spec.connector_image_name is not null and connector.image_name is null
        order by live_spec.id asc;"#,
    )
    .bind(&live_spec_ids[..])
    .fetch_all(txn)
    .await;

    res
}

pub async fn does_connector_exist(
    connector_image: &str,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<bool> {
    sqlx::query!(
        r#"select 1 as "exists: bool" from connectors
        where connectors.image_name = $1;"#,
        connector_image
    )
    .fetch_optional(txn)
    .await
    .map(|exists| exists.is_some())
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
        r#"update connector_tags set
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

pub async fn update_oauth2_spec(
    connector_id: Id,
    oauth2_spec: Box<RawValue>,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<()> {
    sqlx::query!(
        r#"update connectors set
            oauth2_spec = $2,
            updated_at = clock_timestamp()
        where id = $1
        returning 1 as "must_exist";
        "#,
        connector_id as Id,
        Json(oauth2_spec) as Json<Box<RawValue>>,
    )
    .fetch_one(txn)
    .await?;

    Ok(())
}

pub async fn update_tag_fields(
    tag_id: Id,
    documentation_url: String,
    endpoint_spec_schema: Box<RawValue>,
    protocol: String,
    resource_spec_schema: Box<RawValue>,
    resource_path_pointers: Vec<String>,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<()> {
    sqlx::query!(
        r#"update connector_tags set
            documentation_url = $2,
            endpoint_spec_schema = $3,
            protocol = $4,
            resource_spec_schema = $5,
            resource_path_pointers = $6
        where id = $1
        returning 1 as "must_exist";
        "#,
        tag_id as Id,
        documentation_url,
        Json(endpoint_spec_schema) as Json<Box<RawValue>>,
        protocol,
        Json(resource_spec_schema) as Json<Box<RawValue>>,
        resource_path_pointers as Vec<String>,
    )
    .fetch_one(&mut *txn)
    .await?;

    Ok(())
}

/// Returns the `resource_path_pointers` for the given image and tag. Returns
/// `None` if there are no matching rows, or if the `resource_path_pointers`
/// column value is null.
pub async fn fetch_resource_path_pointers(
    image_name: &str,
    image_tag: &str,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<Vec<String>> {
    let row = sqlx::query!(
        r#"
        select ct.resource_path_pointers as "pointers: Vec<String>"
        from connectors c
        join connector_tags ct on c.id = ct.connector_id
        where c.image_name = $1
            and ct.image_tag = $2
        "#,
        image_name,
        image_tag
    )
    .fetch_optional(txn)
    .await?;

    Ok(row.and_then(|r| r.pointers).unwrap_or_default())
}
