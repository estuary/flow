use crate::{CatalogType, Id, TextJson as Json};
use chrono::prelude::*;
use serde::Serialize;
use serde_json::value::RawValue;
use sqlx::types::Uuid;

// Row is the dequeued task of an evolution operation.
#[derive(Debug)]
pub struct Row {
    pub id: Id,
    pub created_at: DateTime<Utc>,
    pub detail: Option<String>,
    pub draft_id: Id,
    pub logs_token: Uuid,
    pub updated_at: DateTime<Utc>,
    pub user_id: Uuid,
    pub collections: Json<Box<RawValue>>,
    pub auto_publish: bool,
}

pub async fn dequeue(txn: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> sqlx::Result<Option<Row>> {
    sqlx::query_as!(
        Row,
        r#"select
            id as "id: Id",
            created_at,
            detail,
            draft_id as "draft_id: Id",
            logs_token,
            updated_at,
            user_id,
            auto_publish,
            collections as "collections: Json<Box<RawValue>>"
        from evolutions where job_status->>'type' = 'queued'
        order by id asc
        limit 1
        for update of evolutions skip locked;
        "#
    )
    .fetch_optional(txn)
    .await
}

pub async fn resolve<S>(
    id: Id,
    status: &S,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<()>
where
    S: Serialize + Send + Sync,
{
    sqlx::query!(
        r#"update evolutions set
            job_status = $2,
            updated_at = clock_timestamp()
        where id = $1
        returning 1 as "must_exist";
        "#,
        id as Id,
        Json(status) as Json<&S>,
    )
    .fetch_one(txn)
    .await?;

    Ok(())
}

pub struct DraftSpecRow {
    pub draft_spec_id: Id,
    pub live_spec_id: Option<Id>,
    pub catalog_name: String,
    pub expect_pub_id: Option<Id>,
    pub last_pub_id: Option<Id>,
    pub draft_spec: Option<Json<Box<RawValue>>>,
    pub draft_type: Option<CatalogType>,
    pub live_type: Option<CatalogType>,
}

pub async fn fetch_draft_specs(
    draft_id: Id,
    user_id: Uuid,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<Vec<DraftSpecRow>> {
    sqlx::query_as!(
        DraftSpecRow,
        r#"
        select
            draft_specs.id as "draft_spec_id: Id",
            draft_specs.catalog_name,
            draft_specs.expect_pub_id as "expect_pub_id: Id",
            draft_specs.spec as "draft_spec: Json<Box<RawValue>>",
            draft_specs.spec_type as "draft_type: CatalogType",
            live_specs.spec_type as "live_type: CatalogType",
            live_specs.last_pub_id as "last_pub_id: Option<Id>",
            live_specs.id as "live_spec_id: Option<Id>"
        from drafts
        left join draft_specs on drafts.id = draft_specs.draft_id
        left join live_specs
            on draft_specs.catalog_name = live_specs.catalog_name
            -- Ensure that `live_spec_id` is `None` if the user does not have admin access to the spec.
            and exists (select 1 from internal.user_roles($2, 'admin') r where live_specs.catalog_name ^@ r.role_prefix)
        where drafts.id = $1 and drafts.user_id = $2
        "#,
        draft_id as Id,
        user_id as Uuid,
    )
    .fetch_all(txn)
    .await
}

pub async fn fetch_resource_spec_schema(
    image_name: String,
    image_tag: String,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<Option<Json<Box<RawValue>>>> {
    struct ResourceSpecSchema {
        resource_spec_schema: Option<Json<Box<RawValue>>>,
    }
    let res = sqlx::query_as!(
        ResourceSpecSchema,
        r#"
        select connector_tags.resource_spec_schema as "resource_spec_schema: Json<Box<RawValue>>"
        from connectors
            join connector_tags on connectors.id = connector_tags.connector_id
        where connectors.image_name = $1
            and connector_tags.image_tag = $2
        "#,
        image_name,
        image_tag
    )
    .fetch_optional(txn)
    .await?;

    Ok(res.and_then(|r| r.resource_spec_schema))
}
