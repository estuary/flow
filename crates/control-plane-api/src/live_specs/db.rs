use crate::TextJson;
use models::{CatalogType, Id};
use serde_json::value::RawValue;
use sqlx::types::Json;
use tables::RoleGrant;

/// Deletes the given live spec row, along with the corresponding `controller_jobs` row.
pub async fn hard_delete_live_spec(id: Id, txn: &mut sqlx::PgConnection) -> sqlx::Result<()> {
    sqlx::query!(
        r#"
        with delete_inferred_schema as (
            delete from inferred_schemas
            where collection_name = (select catalog_name from live_specs where id = $1)
        )
        delete from live_specs where id = $1
        returning 1 as "must_exist!: i32"
        "#,
        id as Id,
    )
    .fetch_one(txn)
    .await?;
    Ok(())
}

/// Represents a live specification, which may or may not exist in the database.
pub struct LiveSpec {
    pub id: Id,
    pub last_pub_id: Id,
    pub last_build_id: Id,
    pub data_plane_id: Id,
    pub catalog_name: String,
    pub spec_type: Option<CatalogType>,
    pub spec: Option<TextJson<Box<RawValue>>>,
    pub built_spec: Option<TextJson<Box<RawValue>>>,
    pub inferred_schema_md5: Option<String>,
    // Capabilities of the specification with respect to other roles.
    pub spec_capabilities: Json<Vec<RoleGrant>>,
    pub dependency_hash: Option<String>,
}

/// Returns a `LiveSpec` row for each of the given `names`. This will always return a row for each
/// name, even if no live spec exists in the database.
pub async fn fetch_live_specs(
    names: &[&str],
    fetch_spec_capabilities: bool,
    db: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
) -> sqlx::Result<Vec<LiveSpec>> {
    sqlx::query_as!(
        LiveSpec,
        r#"
        select
            coalesce(ls.id, '00:00:00:00:00:00:00:00'::flowid) as "id!: Id",
            coalesce(ls.last_pub_id, '00:00:00:00:00:00:00:00'::flowid) as "last_pub_id!: Id",
            coalesce(ls.last_build_id, '00:00:00:00:00:00:00:00'::flowid) as "last_build_id!: Id",
            coalesce(ls.data_plane_id, '00:00:00:00:00:00:00:00'::flowid) as "data_plane_id!: Id",
            names as "catalog_name!: String",
            ls.spec_type as "spec_type?: CatalogType",
            ls.spec as "spec: TextJson<Box<RawValue>>",
            ls.built_spec as "built_spec: TextJson<Box<RawValue>>",
            ls.inferred_schema_md5,
            case when $2 then coalesce(
                (select json_agg(row_to_json(role_grants))
                from role_grants
                where starts_with(names, subject_role)),
                '[]'
            ) else
               '[]'
            end as "spec_capabilities!: Json<Vec<RoleGrant>>",
            ls.dependency_hash
        from unnest($1::text[]) names
        left outer join live_specs ls on ls.catalog_name = names
        "#,
        names as &[&str],
        fetch_spec_capabilities,
    )
    .fetch_all(db)
    .await
}

pub struct InferredSchemaRow {
    pub collection_name: String,
    pub schema: Json<Box<RawValue>>,
    pub md5: String,
}

pub async fn fetch_inferred_schemas(
    collections: &[&str],
    pool: &sqlx::PgPool,
) -> sqlx::Result<Vec<InferredSchemaRow>> {
    sqlx::query_as!(
        InferredSchemaRow,
        r#"select
            collection_name,
            schema as "schema!: Json<Box<RawValue>>",
            md5 as "md5!: String"
            from inferred_schemas
            where collection_name = ANY($1::text[])
            "#,
        collections as &[&str],
    )
    .fetch_all(pool)
    .await
}

/// Queries for all non-deleted `live_specs` that are connected to the given `collection_names` via
/// `live_spec_flows`. This is a pure data fetch: user authorization over the expanded rows is
/// evaluated by the caller, in-process against the authorization Snapshot.
pub async fn fetch_expanded_live_specs(
    collection_names: &[&str],
    exclude_names: &[&str],
    db: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
) -> sqlx::Result<Vec<LiveSpec>> {
    sqlx::query_as!(
        LiveSpec,
        r#"
        with collections(id) as (
            select ls.id
            from unnest($1::text[]) as names(catalog_name)
            join live_specs ls on ls.catalog_name = names.catalog_name
        ),
        exp(id) as (
            select lsf.source_id as id
            from collections c
            join live_spec_flows lsf on c.id = lsf.target_id
            union
            select lsf.target_id as id
            from collections c
            join live_spec_flows lsf on c.id = lsf.source_id
        )
        select
            ls.id as "id: Id",
            ls.last_pub_id as "last_pub_id: Id",
            ls.last_build_id as "last_build_id: Id",
            ls.data_plane_id as "data_plane_id: Id",
            ls.catalog_name,
            ls.spec_type as "spec_type?: CatalogType",
            ls.spec as "spec: TextJson<Box<RawValue>>",
            ls.built_spec as "built_spec: TextJson<Box<RawValue>>",
            ls.inferred_schema_md5,
            -- Placeholder with no reader: the caller discards this column,
            -- and specs which expansion adds to the draft have their
            -- capabilities fetched afresh by the resolve-path fetch_live_specs.
            '[]' as "spec_capabilities!: Json<Vec<RoleGrant>>",
            ls.dependency_hash
        from exp
        join live_specs ls on ls.id = exp.id
        where ls.spec is not null and not ls.catalog_name = any($2);
        "#,
        collection_names as &[&str],
        exclude_names as &[&str],
    )
    .fetch_all(db)
    .await
}

/// Returns all live spec names under the given prefix.
pub async fn fetch_live_spec_names_by_prefix(
    prefix: &str,
    db: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
) -> sqlx::Result<Vec<String>> {
    sqlx::query_scalar!(
        r#"
        select catalog_name
        from live_specs
        where starts_with(catalog_name, $1)
        and spec is not null
        order by catalog_name
        "#,
        prefix,
    )
    .fetch_all(db)
    .await
}
