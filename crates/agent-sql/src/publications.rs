use crate::FlowType;

use super::{Capability, CatalogType, Id, TextJson as Json};

use chrono::prelude::*;
use serde::Serialize;
use serde_json::value::RawValue;
use sqlx::types::Uuid;

#[derive(Debug)]
pub struct LiveRevision {
    pub catalog_name: String,
    pub last_pub_id: Id,
}

/// Locks the given live specs rows and returns their current `last_pub_id`s.
/// This is used for verifying the `last_pub_id`s for specs that were used
/// during the build, but are not being updated. We verify the revisions
/// in-memory in order to handle the case where a row has subsequently been
/// deleted, since you can't use `for update` on the nullable side of an outer
/// join.
pub async fn lock_live_specs(
    catalog_names: &[&str],
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<Vec<LiveRevision>> {
    let fails = sqlx::query_as!(
        LiveRevision,
        r#"
        select
            ls.catalog_name,
            ls.last_pub_id as "last_pub_id: Id"
        from  live_specs ls
        where ls.catalog_name = any($1::text[])
        for update of ls
        "#,
        catalog_names as &[&str],
    )
    .fetch_all(txn)
    .await?;
    Ok(fails)
}

pub struct LiveSpecUpdate {
    pub catalog_name: String,
    pub live_spec_id: Id,
    pub expect_build_id: Id,
    pub last_build_id: Id,
}

/// Updates all live_specs rows for a publication. Accepts all inputs as slices, which _must_ all
/// have the same length. This is done in order to minimize the number of round trips. Returns a
/// `LiveSpecUpdate` for each affected row, which can be inspected to determine whether there was
/// an optimistic locking failure. It's the caller's responsibility to check for such failures and
/// roll back the transaction if any are found.
pub async fn update_live_specs(
    pub_id: Id,
    build_id: Id,
    catalog_names: &[String],
    spec_types: &[CatalogType],
    models: &[Option<Json<Box<RawValue>>>],
    built_specs: &[Option<Json<Box<RawValue>>>],
    expect_build_ids: &[Id],
    reads_from: &[Option<Json<Vec<String>>>],
    writes_to: &[Option<Json<Vec<String>>>],
    images: &[Option<String>],
    image_tags: &[Option<String>],
    data_plane_ids: &[Id],
    is_touches: &[bool],
    dependency_hashes: &[Option<&str>],
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<Vec<LiveSpecUpdate>> {
    let fails = sqlx::query_as!(
        LiveSpecUpdate,
        r#"
        with inputs(catalog_name, spec_type, spec, built_spec, expect_pub_id, reads_from, writes_to, image, image_tag, data_plane_id, is_touch, dependency_hash) as (
            select * from unnest(
                $3::text[],
                $4::catalog_spec_type[],
                $5::json[],
                $6::json[],
                $7::flowid[],
                $8::json[],
                $9::json[],
                $10::text[],
                $11::text[],
                $12::flowid[],
                $13::boolean[],
                $14::text[]
            )
        ),
        joined(catalog_name, spec_type, spec, built_spec, expect_build_id, reads_from, writes_to, image, image_tag, data_plane_id, is_touch, dependency_hash, last_build_id, next_pub_id) as (
            select
                inputs.*,
                case when ls.spec is null then '00:00:00:00:00:00:00:00'::flowid else ls.last_build_id end as last_build_id,
                case when inputs.is_touch then ls.last_pub_id else $1::flowid end as next_pub_id
            from inputs
            left outer join live_specs ls on ls.catalog_name = inputs.catalog_name
        ),
        insert_live_specs(catalog_name,live_spec_id) as (
            insert into live_specs (
                catalog_name,
                spec_type,
                spec,
                built_spec,
                last_build_id,
                last_pub_id,
                controller_next_run,
                reads_from,
                writes_to,
                connector_image_name,
                connector_image_tag,
                data_plane_id,
                dependency_hash
            ) select
                catalog_name,
                spec_type,
                spec,
                built_spec,
                $2::flowid,
                joined.next_pub_id,
                now(),
                case when json_typeof(reads_from) is null then
                    null
                else
                    array(select json_array_elements_text(reads_from))
                end,
                case when json_typeof(writes_to) is null then
                    null
                else
                    array(select json_array_elements_text(writes_to))
                end,
                image,
                image_tag,
                data_plane_id,
                dependency_hash
            from joined
            on conflict (catalog_name) do update set
                updated_at = now(),
                spec_type = excluded.spec_type,
                spec = excluded.spec,
                built_spec = excluded.built_spec,
                last_build_id = excluded.last_build_id,
                last_pub_id = excluded.last_pub_id,
                controller_next_run = excluded.controller_next_run,
                reads_from = excluded.reads_from,
                writes_to = excluded.writes_to,
                connector_image_name = excluded.connector_image_name,
                connector_image_tag = excluded.connector_image_tag,
                dependency_hash = excluded.dependency_hash
            returning
                catalog_name,
                id as live_spec_id,
                last_build_id
        ),
        insert_controller_jobs as (
            insert into controller_jobs(live_spec_id)
            select live_spec_id from insert_live_specs
            on conflict (live_spec_id) do nothing
        ),
        delete_alerts as (
            delete from alert_data_processing where catalog_name in (
                select catalog_name from inputs where inputs.spec is null
            )
        )
        select
            joined.catalog_name as "catalog_name!: String",
            insert_live_specs.live_spec_id as "live_spec_id!: Id",
            joined.expect_build_id as "expect_build_id!: Id",
            joined.last_build_id as "last_build_id!: Id"
        from insert_live_specs
        join joined using (catalog_name)
    "#,
    pub_id as Id, // 1
    build_id as Id, // 2
    catalog_names, // 3
    spec_types as &[CatalogType], // 4
    models as &[Option<Json<Box<RawValue>>>], // 5
    built_specs as &[Option<Json<Box<RawValue>>>], // 6
    expect_build_ids as &[Id], // 7
    reads_from as &[Option<Json<Vec<String>>>], // 8
    writes_to as &[Option<Json<Vec<String>>>], // 9
    images as &[Option<String>], // 10
    image_tags as &[Option<String>], // 11
    data_plane_ids as &[Id], // 12
    is_touches as &[bool], // 13
    dependency_hashes as &[Option<&str>], // 14
    )
    .fetch_all(txn)
    .await?;
    Ok(fails)
}

/// Enqueues a new publication of the given `draft_id`.
pub async fn create(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    user_id: Uuid,
    draft_id: Id,
    auto_evolve: bool,
    detail: String,
    background: bool,
    data_plane_name: String,
) -> sqlx::Result<Id> {
    let rec = sqlx::query!(
        r#"insert into publications (user_id, draft_id, auto_evolve, detail, background, data_plane_name)
            values ($1, $2, $3, $4, $5, $6) returning id as "id: Id";"#,
        user_id as Uuid,
        draft_id as Id,
        auto_evolve,
        detail,
        background,
        data_plane_name,
    )
    .fetch_one(txn)
    .await?;

    Ok(rec.id)
}

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
    pub auto_evolve: bool,
    pub background: bool,
    pub data_plane_name: String,
}

#[tracing::instrument(level = "debug", skip(txn))]
pub async fn dequeue(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    allow_background: bool,
) -> sqlx::Result<Option<Row>> {
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
            user_id,
            auto_evolve,
            background,
            data_plane_name
        from publications
        where job_status->>'type' = 'queued' and (background = $1 or background = false)
        order by background asc, id asc
        limit 1
        for update of publications skip locked;
        "#,
        allow_background
    )
    .fetch_optional(txn)
    .await
}

pub async fn resolve<S>(
    id: Id,
    status: &S,
    final_pub_id: Option<Id>,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<()>
where
    S: Serialize + Send + Sync,
{
    sqlx::query!(
        r#"update publications set
            job_status = $2,
            updated_at = clock_timestamp(),
            pub_id = $3
        where id = $1
        returning 1 as "must_exist";
        "#,
        id as Id,
        Json(status) as Json<&S>,
        final_pub_id as Option<Id>,
    )
    .fetch_one(txn)
    .await?;

    Ok(())
}

pub async fn delete_draft(delete_draft_id: Id, pg_pool: &sqlx::PgPool) -> sqlx::Result<()> {
    sqlx::query!(r#"delete from drafts where id = $1"#, delete_draft_id as Id,)
        .execute(pg_pool)
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

pub async fn add_inferred_schema_md5(
    live_spec_id: Id,
    inferred_schema_md5: Option<String>,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<()> {
    sqlx::query!(
        r#"
        update live_specs set inferred_schema_md5 = $1
        where id = $2
        returning 1 as "must_exist"
        "#,
        inferred_schema_md5 as Option<String>,
        live_spec_id as Id,
    )
    .fetch_one(txn)
    .await?;
    Ok(())
}

pub async fn add_built_specs<S>(
    live_spec_id: Id,
    built_spec: S,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<()>
where
    S: serde::Serialize + Send + Sync,
{
    sqlx::query!(
        r#"
        update live_specs set built_spec = $1
        where id = $2
        returning 1 as "must_exist";
        "#,
        Json(built_spec) as Json<S>,
        live_spec_id as Id,
    )
    .fetch_one(&mut *txn)
    .await?;

    Ok(())
}

#[derive(Debug, Serialize)]
pub struct Tenant {
    pub name: String,
    pub tasks_quota: i32,
    pub collections_quota: i32,
    pub tasks_used: i32,
    pub collections_used: i32,
}

pub async fn find_tenant_quotas(
    tenant_names: &[&str],
    txn: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
) -> sqlx::Result<Vec<Tenant>> {
    sqlx::query_as!(
        Tenant,
        r#"
        with tenant_names(tenant_name) as (
            select unnest($1::text[]) as tenant_name
        ),
        tenant_usages as (
            select
                tenant_names.tenant_name,
                (count(live_specs.catalog_name) filter (
                    where
                        live_specs.spec_type = 'capture' or
                        live_specs.spec_type = 'materialization' or
                        live_specs.spec_type = 'collection' and live_specs.spec->'derive' is not null
                ))::integer as tasks_used,
                (count(live_specs.catalog_name) filter (
                    where live_specs.spec_type = 'collection'
                ))::integer as collections_used
            from tenant_names
            left outer join live_specs on
                starts_with(live_specs.catalog_name, tenant_names.tenant_name) and
                (live_specs.spec->'shards'->>'disable')::boolean is not true
            group by tenant_names.tenant_name
        )
        select
            tenants.tenant as name,
            tenants.tasks_quota::integer as "tasks_quota!: i32",
            tenants.collections_quota::integer as "collections_quota!: i32",
            tenant_usages.tasks_used as "tasks_used!: i32",
            tenant_usages.collections_used as "collections_used!: i32"
        from tenant_usages
        join tenants on tenants.tenant = tenant_usages.tenant_name
        order by tenants.tenant;"#,
        tenant_names as &[&str]
    )
    .fetch_all(txn)
    .await
}

#[derive(Debug)]
pub struct ExpandedRow {
    // Name of the specification.
    pub catalog_name: String,
    // Last build ID of the live spec.
    pub last_build_id: Id,
    // Last publication ID of the live spec.
    pub last_pub_id: Id,
    // Current live specification of this expansion.
    // It won't be changed by this publication.
    pub live_spec: Json<Box<RawValue>>,
    // ID of the expanded live specification.
    pub live_spec_id: Id,
    // Spec type of the live specification.
    pub live_type: CatalogType,
    // User's capability to the specification `catalog_name`.
    pub user_capability: Option<Capability>,
}

pub async fn resolve_expanded_rows(
    user_id: Uuid,
    seed_ids: Vec<Id>,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<Vec<ExpandedRow>> {
    sqlx::query_as!(
        ExpandedRow,
        r#"
        with recursive
        -- Expand seed collections, captures, and materializations through
        -- edges that connect captures and materializations to their bound
        -- collections.
        seeds(id) as (
            select id from unnest($1::flowid[]) as id
        ),
        -- A seed collection expands to captures or materializations which bind it.
        bound_captures(id) as (
            select e.source_id
            from seeds as s join live_spec_flows as e
            on s.id = e.target_id and e.flow_type = 'capture'
        ),
        bound_materializations(id) as (
            select e.target_id
            from seeds as s join live_spec_flows as e
            on s.id = e.source_id and e.flow_type = 'materialization'
        ),
        -- A capture or materialization expands to all bound collections.
        -- This includes seed captures or materializations, as well as captures
        -- or materializations bound to seed collections.
        bound_collections(id) as (
              select e.target_id
              from live_spec_flows as e
              where e.source_id in (select id from bound_captures union select id from seeds) and e.flow_type = 'capture'
            union
              select e.source_id
              from live_spec_flows as e
              where e.target_id in (select id from bound_materializations union select id from seeds) and e.flow_type = 'materialization'
            union
              select e.target_id
              from seeds as s join live_spec_flows as e
              on s.id = e.source_id and e.flow_type = 'collection'
        ),
        -- The expanded set now includes the original seed item, all bound captures
        -- and materializations, and all bound collections.
        all_bound_items(id) as (
              select id from bound_collections
            union
              select id from bound_captures
            union
              select id from bound_materializations
            union
              select id from seeds
        ),
        -- A further expansion recursively walks backwards along data-flow edges to
        -- expand derivations and tests:
        --   * A derivation is expanded to its sources.
        --   * A collection or derivation is expanded to tests which write (ingest) into it.
        --   * A test is expanded to collections or derivations it reads (verifies).
        backprop_derivations_and_tests(id) as (
            (select id from all_bound_items)
          union
            select e.source_id
            from backprop_derivations_and_tests as p join live_spec_flows as e
            on p.id = e.target_id and e.flow_type in ('collection', 'test')
        )
        -- Join the expanded IDs with live_specs.
        select
            l.id as "live_spec_id!: Id",
            l.catalog_name as "catalog_name!",
            l.last_build_id as "last_build_id!: Id",
            l.last_pub_id as "last_pub_id!: Id",
            l.spec as "live_spec!: Json<Box<RawValue>>",
            l.spec_type as "live_type!: CatalogType",
            (
                select max(capability) from internal.user_roles($2) r
                where starts_with(l.catalog_name, r.role_prefix)
            ) as "user_capability: Capability"
        from live_specs l join backprop_derivations_and_tests p on l.id = p.id
        -- Strip deleted specs which are still reach-able through a dataflow edge,
        -- and strip rows already part of the seed set.
        where l.spec is not null and l.id not in (select id from seeds)
        for update of l nowait
        "#,
        seed_ids as Vec<Id>,
        user_id,
    )
    .fetch_all(&mut *txn)
    .await
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
                "delete from live_spec_flows where target_id = $1 and (flow_type = 'materialization' or flow_type = 'source_capture')",
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

pub async fn insert_live_spec_flows(
    live_spec_id: Id,
    draft_type: CatalogType,
    reads_from: Option<Vec<&str>>,
    writes_to: Option<Vec<&str>>,
    source_capture: Option<&str>,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<()> {
    let flow_type = FlowType::from(draft_type);
    // Precondition: `reads_from` and `writes_to` may or may not have a live_specs row,
    // and we silently ignore entries which don't match a live_specs row. If this happens,
    // it would be due to concurrent deletions of live specs, which will get surfaced elsewhere
    // as optimistic locking failures.
    sqlx::query!(
        r#"
        insert into live_spec_flows (source_id, target_id, flow_type)
        select live_specs.id, $1, $2::flow_type
            from unnest($3::text[]) as n inner join live_specs on catalog_name = n
        union
            select $1, live_specs.id, $2
            from unnest($4::text[]) as n inner join live_specs on catalog_name = n
        union
            select live_specs.id, $1, 'source_capture'
            from live_specs
            where catalog_name = $5
        "#,
        live_spec_id as Id,
        flow_type as FlowType,
        reads_from as Option<Vec<&str>>,
        writes_to as Option<Vec<&str>>,
        source_capture,
    )
    .execute(&mut *txn)
    .await?;

    Ok(())
}

#[derive(Debug)]
pub struct StorageRow {
    pub id: Id,
    pub catalog_prefix: String,
    pub spec: serde_json::Value,
}

/// Returns the storage mappings for the given set of tenants.
/// Mappings for `recovery/{tenant}` will also be returned.
pub async fn resolve_storage_mappings(
    tenant_names: Vec<&str>,
    db: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
) -> sqlx::Result<Vec<StorageRow>> {
    sqlx::query_as!(
        StorageRow,
        r#"
        with tenants(name) as (
          select unnest($1::text[])
        ),
        prefixes as (
          select name as prefix from tenants
          union all select 'recovery/' || name from tenants
        )
        select
            m.id as "id: Id",
            m.catalog_prefix,
            m.spec
        from prefixes p
        join storage_mappings m on m.catalog_prefix = p.prefix;
        "#,
        tenant_names as Vec<&str>,
    )
    .fetch_all(db)
    .await
}

pub struct ResolvedCollectionRow {
    pub built_spec: Option<Json<proto_flow::flow::CollectionSpec>>,
}

pub async fn resolve_collections(
    collections: Vec<String>,
    pool: sqlx::PgPool,
) -> sqlx::Result<Vec<ResolvedCollectionRow>> {
    sqlx::query_as!(
        ResolvedCollectionRow,
        r#"select
            built_spec as "built_spec: Json<proto_flow::flow::CollectionSpec>"
            from live_specs
            where catalog_name = ANY($1::text[])
            and spec_type = 'collection'
            "#,
        collections as Vec<String>,
    )
    .fetch_all(&pool)
    .await
}
