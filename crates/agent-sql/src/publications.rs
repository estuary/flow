use super::{Capability, CatalogType, Id};

use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use sqlx::types::{Json, Uuid};

// Row is the dequeued task shape of a draft build & test operation.
#[derive(Debug)]
pub struct Row {
    pub created_at: DateTime<Utc>,
    pub detail: Option<String>,
    pub draft_id: Id,
    pub dry_run: bool,
    pub logs_token: Uuid,
    pub pub_id: Id,
    pub updated_at: DateTime<Utc>,
    pub user_id: Uuid,
}

pub async fn dequeue(txn: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> sqlx::Result<Option<Row>> {
    sqlx::query_as!(
        Row,
        r#"select
            created_at,
            detail,
            draft_id as "draft_id: Id",
            dry_run,
            logs_token,
            id as "pub_id: Id",
            updated_at,
            user_id
        from publications where job_status->>'type' = 'queued'
        order by id asc
        limit 1
        for update of publications skip locked;
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
        r#"update publications set
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

pub async fn delete_draft(delete_draft_id: Id, pg_pool: &sqlx::PgPool) -> sqlx::Result<()> {
    sqlx::query!(
        r#"
        delete from drafts where id = $1 and not exists
            (select 1 from draft_specs where draft_id = $1)
        "#,
        delete_draft_id as Id,
    )
    .execute(pg_pool)
    .await?;

    Ok(())
}

pub async fn delete_draft_errors(
    draft_id: Id,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<()> {
    sqlx::query!(
        "delete from draft_errors where draft_id = $1",
        draft_id as Id
    )
    .execute(txn)
    .await?;

    Ok(())
}

pub async fn savepoint_noop(txn: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> sqlx::Result<()> {
    sqlx::query!("savepoint noop;").execute(txn).await?;
    Ok(())
}

pub async fn rollback_noop(txn: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> sqlx::Result<()> {
    sqlx::query!("rollback transaction to noop;")
        .execute(txn)
        .await?;
    Ok(())
}

pub async fn insert_new_live_specs(
    draft_id: Id,
    pub_id: Id,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<u64> {
    let rows = sqlx::query!(
        r#"
        insert into live_specs(catalog_name, last_pub_id) (
            select catalog_name, $2
            from draft_specs
            where draft_specs.draft_id = $1
            for update of draft_specs
        ) on conflict (catalog_name) do nothing
        "#,
        draft_id as Id,
        pub_id as Id,
    )
    .execute(&mut *txn)
    .await?;

    Ok(rows.rows_affected())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RoleGrant {
    pub subject_role: String,
    pub object_role: String,
    pub capability: Capability,
}

#[derive(Debug)]
pub struct SpecRow {
    // Name of the specification.
    pub catalog_name: String,
    // Specification which will be applied by this draft.
    pub draft_spec: Option<Json<Box<RawValue>>>,
    // ID of the draft specification.
    pub draft_spec_id: Id,
    // Spec type of this draft.
    // We validate and require that this equals `live_type`.
    pub draft_type: Option<CatalogType>,
    // Optional expected value for `last_pub_id` of the live spec.
    // A special all-zero value means "this should be a creation".
    pub expect_pub_id: Option<Id>,
    // Last publication ID of the live spec.
    // If the spec is being created, this is the current publication ID.
    pub last_pub_id: Id,
    // Current live specification which will be replaced by this draft.
    pub live_spec: Option<Json<Box<RawValue>>>,
    // ID of the live specification.
    pub live_spec_id: Id,
    // Spec type of the live specification.
    pub live_type: Option<CatalogType>,
    // Capabilities of the specification with respect to other roles.
    pub spec_capabilities: Json<Vec<RoleGrant>>,
    // User's capability to the specification `catalog_name`.
    pub user_capability: Option<Capability>,
}

pub async fn resolve_spec_rows(
    draft_id: Id,
    user_id: Uuid,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<Vec<SpecRow>> {
    sqlx::query_as!(
        SpecRow,
        r#"
        select
            draft_specs.catalog_name,
            draft_specs.expect_pub_id as "expect_pub_id: Id",
            draft_specs.spec as "draft_spec: Json<Box<RawValue>>",
            draft_specs.id as "draft_spec_id: Id",
            draft_specs.spec_type as "draft_type: CatalogType",
            live_specs.last_pub_id as "last_pub_id: Id",
            live_specs.spec as "live_spec: Json<Box<RawValue>>",
            live_specs.id as "live_spec_id: Id",
            live_specs.spec_type as "live_type: CatalogType",
            coalesce(
                (select json_agg(row_to_json(role_grants))
                from role_grants
                where starts_with(draft_specs.catalog_name, subject_role)),
                '[]'
            ) as "spec_capabilities!: Json<Vec<RoleGrant>>",
            (
                select max(capability) from internal.user_roles($2) r
                where starts_with(draft_specs.catalog_name, r.role_prefix)
            ) as "user_capability: Capability"
        from draft_specs
        join live_specs
            on draft_specs.catalog_name = live_specs.catalog_name
        where draft_specs.draft_id = $1
        for update of draft_specs, live_specs;
        "#,
        draft_id as Id,
        user_id,
    )
    .fetch_all(txn)
    .await
}

#[derive(Debug)]
pub struct ExpandedRow {
    // Name of the specification.
    pub catalog_name: String,
    // Current live specification of this expansion.
    // It won't be changed by this publication.
    pub live_spec: Json<Box<RawValue>>,
    // ID of the expanded live specification.
    pub live_spec_id: Id,
    // Spec type of the live specification.
    pub live_type: CatalogType,
}

pub async fn resolve_expanded_rows(
    seed_ids: Vec<Id>,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<Vec<ExpandedRow>> {
    sqlx::query_as!(
        ExpandedRow,
        r#"
        -- Perform a graph traversal which expands the seed set of
        -- specifications. Directly-adjacent captures and materializations
        -- are resolved, as is the full connected component of tests and
        -- derivations.
        with recursive expanded(id, seed) as (
            select id, true from unnest($1::flowid[]) as id
          union
            select
                case when expanded.id = e.source_id then e.target_id else e.source_id end,
                false
            from expanded join live_spec_flows as e
            on expanded.id = e.source_id or expanded.id = e.target_id
            where expanded.seed or e.flow_type in ('collection', 'test')
        )
        -- Join the expanded IDs with live_specs.
        select
            id as "live_spec_id: Id",
            catalog_name,
            spec as "live_spec!: Json<Box<RawValue>>",
            spec_type as "live_type!: CatalogType"
        from live_specs natural join expanded
        -- Strip deleted specs which are still reach-able through a dataflow edge.
        where spec is not null
        -- Strip specs which are already part of the seed set.
        group by id having not bool_or(seed);
        "#,
        seed_ids as Vec<Id>,
    )
    .fetch_all(&mut *txn)
    .await
}

pub async fn insert_error(
    draft_id: Id,
    scope: String,
    detail: String,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<()> {
    sqlx::query!(
        r#"insert into draft_errors (
            draft_id,
            scope,
            detail
        ) values ($1, $2, $3)
        "#,
        draft_id as Id,
        scope,
        detail,
    )
    .execute(&mut *txn)
    .await?;

    Ok(())
}

pub async fn delete_draft_spec(
    draft_spec_id: Id,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<()> {
    sqlx::query!(
        r#"delete from draft_specs where id = $1 returning 1 as "must_exist";"#,
        draft_spec_id as Id,
    )
    .fetch_one(txn)
    .await?;

    Ok(())
}

pub async fn delete_stale_flow(
    live_spec_id: Id,
    catalog_type: CatalogType,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<()> {
    match catalog_type {
        CatalogType::Capture => {
            sqlx::query!(
                "delete from live_spec_flows where source_id = $1 and flow_type = 'capture'",
                live_spec_id as Id,
            )
            .execute(&mut *txn)
            .await?;
        }
        CatalogType::Collection => {
            sqlx::query!(
                "delete from live_spec_flows where target_id = $1 and flow_type = 'collection'",
                live_spec_id as Id,
            )
            .execute(&mut *txn)
            .await?;
        }
        CatalogType::Materialization => {
            sqlx::query!(
                "delete from live_spec_flows where target_id = $1 and flow_type = 'materialization'",
                live_spec_id as Id,
            )
            .execute(&mut *txn)
            .await?;
        }
        CatalogType::Test => {
            sqlx::query!(
                "delete from live_spec_flows where (source_id = $1 or target_id = $1) and flow_type = 'test'",
                live_spec_id as Id,
            )
            .execute(&mut *txn)
            .await?;
        }
    }
    Ok(())
}

pub async fn insert_publication_spec(
    live_spec_id: Id,
    pub_id: Id,
    detail: Option<&String>,
    draft_spec: &Option<Json<Box<RawValue>>>,
    draft_type: &Option<CatalogType>,
    user_id: Uuid,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<()> {
    sqlx::query!(
        r#"insert into publication_specs (
            live_spec_id,
            pub_id,
            detail,
            published_at,
            spec,
            spec_type,
            user_id
        ) values ($1, $2, $3, DEFAULT, $4, $5, $6);
        "#,
        live_spec_id as Id,
        pub_id as Id,
        detail as Option<&String>,
        draft_spec as &Option<Json<Box<RawValue>>>,
        draft_type as &Option<CatalogType>,
        user_id as Uuid,
    )
    .execute(&mut *txn)
    .await?;

    Ok(())
}

pub async fn update_live_spec(
    catalog_name: &str,
    connector_image_name: Option<&String>,
    connector_tag_name: Option<&String>,
    pub_id: Id,
    reads_from: &Option<Vec<&str>>,
    draft_spec: &Option<Json<Box<RawValue>>>,
    draft_type: &Option<CatalogType>,
    writes_to: &Option<Vec<&str>>,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<()> {
    sqlx::query!(
        r#"
        update live_specs set
            connector_image_name = $2,
            connector_image_tag = $3,
            last_pub_id = $4,
            reads_from = $5,
            spec = $6,
            spec_type = $7,
            updated_at = clock_timestamp(),
            writes_to = $8
        where catalog_name = $1
        returning 1 as "must_exist";
        "#,
        catalog_name,
        connector_image_name,
        connector_tag_name,
        pub_id as Id,
        reads_from as &Option<Vec<&str>>,
        draft_spec as &Option<Json<Box<RawValue>>>,
        draft_type as &Option<CatalogType>,
        writes_to as &Option<Vec<&str>>,
    )
    .fetch_one(&mut *txn)
    .await?;

    Ok(())
}

pub async fn insert_live_spec_flows(
    live_spec_id: Id,
    draft_type: &Option<CatalogType>,
    reads_from: Option<Vec<&str>>,
    writes_to: Option<Vec<&str>>,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<()> {
    sqlx::query!(
        r#"
        insert into live_spec_flows (source_id, target_id, flow_type)
            select live_specs.id, $1, $2::catalog_spec_type
            from unnest($3::text[]) as n join live_specs on catalog_name = n
        union
            select $1, live_specs.id, $2
            from unnest($4::text[]) as n join live_specs on catalog_name = n;
        "#,
        live_spec_id as Id,
        draft_type as &Option<CatalogType>,
        reads_from as Option<Vec<&str>>,
        writes_to as Option<Vec<&str>>,
    )
    .execute(&mut *txn)
    .await?;

    Ok(())
}

#[derive(Debug)]
pub struct StorageRow {
    pub catalog_prefix: String,
    pub spec: serde_json::Value,
}

pub async fn resolve_storage_mappings(
    names: Vec<&str>,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<Vec<StorageRow>> {
    sqlx::query_as!(
        StorageRow,
        r#"
        select
            m.catalog_prefix,
            m.spec
        from storage_mappings m,
        lateral unnest($1::text[]) as n
        where starts_with(n, m.catalog_prefix)
           or starts_with('recovery/' || n, m.catalog_prefix)
           -- TODO(johnny): hack until we better-integrate ops collections.
           or m.catalog_prefix = 'ops/'
        group by m.id;
        "#,
        names as Vec<&str>,
    )
    .fetch_all(&mut *txn)
    .await
}
