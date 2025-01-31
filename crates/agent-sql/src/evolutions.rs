use crate::{CatalogType, Id, TextJson as Json};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::value::RawValue;
use sqlx::types::Uuid;

// Row is the dequeued task of an evolution operation.
#[derive(Debug)]
pub struct Row {
    pub id: Id,
    pub draft_id: Id,
    pub updated_at: DateTime<Utc>,
    pub user_id: Uuid,
    pub collections: Json<Box<RawValue>>,
}

pub async fn fetch_evolution(task_id: Id, db: &sqlx::PgPool) -> sqlx::Result<Row> {
    sqlx::query_as!(
        Row,
        r#"select
            id as "id: Id",
            draft_id as "draft_id: Id",
            updated_at,
            user_id,
            collections as "collections: Json<Box<RawValue>>"
        from evolutions
        where id = $1::flowid
        "#,
        task_id as Id
    )
    .fetch_one(db)
    .await
}

pub async fn create(
    pool: &sqlx::PgPool,
    user_id: Uuid,
    draft_id: Id,
    collections: Vec<serde_json::Value>,
    auto_publish: bool,
    detail: String,
) -> sqlx::Result<Id> {
    let rec = sqlx::query!(
        r#"
        insert into evolutions
            ( user_id, draft_id, collections, auto_publish, detail, background)
        values ( $1, $2, $3, $4, $5, true ) returning id as "id: Id"
        "#,
        user_id as Uuid,
        draft_id as Id,
        serde_json::Value::Array(collections),
        auto_publish,
        detail,
    )
    .fetch_one(pool)
    .await?;
    Ok(rec.id)
}

pub async fn resolve<S>(id: Id, status: &S, txn: &mut sqlx::PgConnection) -> sqlx::Result<()>
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

#[derive(Debug)]
pub struct SpecRow {
    pub catalog_name: String,
    /// The id of the draft spec, or None if it is not already in the draft
    pub draft_spec_id: Option<Id>,
    /// The id of the live spec, or None if the spec was never published (which
    /// will be surfaced as an error)
    pub live_spec_id: Option<Id>,
    /// The current value of `expect_pub_id` from the draft spec, if drafted
    pub expect_pub_id: Option<Id>,
    /// The last publication id that updated the live spec
    pub last_pub_id: Option<Id>,
    pub spec: Option<Json<Box<RawValue>>>,
    pub spec_type: Option<CatalogType>,
}

/// Fetches the initial set of specs that are needed for an evolutions job. The `collection_names` must be only the _current_ names of affected collections. It should not include the new names of collections requested to be re-created.
/// The set of returned specs will include:
/// - All draft_specs for the given `draft_id`
/// - live_specs rows for any of the `collection_names` that _aren't_ already drafted
///
/// The reason for including all draft specs is that the evolution may end
/// up affecting them, but we cannot know for certain until we check all of their
/// bindings. Technically, we could implement that filtering as part of the sql
/// query, but the extra complexity doesn't seem warranted at this time.
pub async fn resolve_specs(
    user_id: Uuid,
    draft_id: Id,
    collection_names: Vec<String>,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<Vec<SpecRow>> {
    sqlx::query_as!(
        SpecRow,
        r#"
        with drafted as (
            select
                ds.catalog_name,
                ds.id as draft_spec_id,
                ls.id as live_spec_id,
                ds.expect_pub_id,
                ls.last_pub_id as last_pub_id,
                ds.spec as spec,
                ds.spec_type as spec_type
            from draft_specs ds
            left join live_specs ls
                on ds.catalog_name = ls.catalog_name
                -- filter out live_specs rows that the user does not have admin access to
                and exists (select 1 from internal.user_roles($2, 'admin') r where ls.catalog_name ^@ r.role_prefix)
            where ds.draft_id = $1
        ),
        not_drafted as (
            select catalog_name from unnest($3::text[]) as names(catalog_name)
            except
            select catalog_name from drafted
        ),
        live as (
            select
                ls.catalog_name,
                ls.spec,
                ls.spec_type,
                ls.last_pub_id,
                ls.id
            from not_drafted
            join live_specs ls on not_drafted.catalog_name = ls.catalog_name
            where
                -- filter out live_specs rows that the user does not have admin access to
                exists (select 1 from internal.user_roles($2, 'admin') r where ls.catalog_name ^@ r.role_prefix)
        )
        select
            catalog_name as "catalog_name!: String",
            draft_spec_id as "draft_spec_id: Id",
            live_spec_id as "live_spec_id: Id",
            expect_pub_id as "expect_pub_id: Id",
            last_pub_id as "last_pub_id: Id",
            spec as "spec: Json<Box<RawValue>>",
            spec_type as "spec_type: CatalogType"
        from drafted
        union all
        select
            catalog_name as "catalog_name!: String",
            null as "draft_spec_id: Id",
            id as "live_spec_id: Id",
            null as "expect_pub_id: Id",
            last_pub_id as "last_pub_id: Id",
            spec as "spec: Json<Box<RawValue>>",
            spec_type as "spec_type: CatalogType"
        from live
        "#,
        draft_id as Id,
        user_id as Uuid,
        collection_names as Vec<String>,
    ).fetch_all(txn)
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
