use crate::TextJson;
use models::{Capability, CatalogType, Id};
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
    // User's capability to the specification `catalog_name`.
    pub user_capability: Option<Capability>,
    // Capabilities of the specification with respect to other roles.
    pub spec_capabilities: Json<Vec<RoleGrant>>,
    pub dependency_hash: Option<String>,
}

/// Returns a `LiveSpec` row for each of the given `names`. This will always return a row for each
/// name, even if no live spec exists in the database.
pub async fn fetch_live_specs(
    user_id: uuid::Uuid,
    names: &[String],
    fetch_user_capabilities: bool,
    fetch_spec_capabilities: bool,
    db: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
    snapshot: &crate::Snapshot,
) -> sqlx::Result<Vec<LiveSpec>> {
    let mut live_spec = sqlx::query_as!(
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
            null as "user_capability: Capability",
            -- `spec_capabilities` are synthesized from the authorization Snapshot
            -- below rather than queried here; see `fetch_spec_capabilities`.
            '[]' as "spec_capabilities!: Json<Vec<RoleGrant>>",
            ls.dependency_hash
        from unnest($1::text[]) names
        left outer join live_specs ls on ls.catalog_name = names
        "#,
        names,
    )
    .fetch_all(db)
    .await?;

    if fetch_user_capabilities {
        // Compute each spec's capability independently. The user's authorization
        // to one name must not leak to the others in the batch: a user with admin
        // on a drafted `dogs/` spec that references `cats/noms` must still show as
        // unauthorized to `cats/noms`. This mirrors the previous per-row SQL
        // `max(capability) ... where starts_with(name, role_prefix)` — the user's
        // greatest capability among the prefixes that `catalog_name` falls under.
        let reachable = snapshot.prefix_and_capabilities_per_user(user_id);
        for spec in live_spec.iter_mut() {
            let mut max_capability: Option<Capability> = None;
            for (prefix, (_, capability)) in reachable.iter() {
                if spec.catalog_name.starts_with(*prefix) {
                    max_capability = max_capability.max(Some(*capability));
                }
            }
            spec.user_capability = max_capability;
        }
    }
    if fetch_spec_capabilities {
        // A spec's capabilities are the role grants whose `subject_role` is a
        // prefix of its `catalog_name` — the grants it holds by virtue of its
        // own name/role. Sourced from the Snapshot's `role_grants` rather than
        // the database, mirroring `role_grants where starts_with(name, subject_role)`.
        for spec in live_spec.iter_mut() {
            spec.spec_capabilities = Json(snapshot.spec_capabilities(&spec.catalog_name));
        }
    }
    Ok(live_spec)
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
/// `live_spec_flows`.
pub async fn fetch_expanded_live_specs(
    collection_names: &[&str],
    exclude_names: &[&str],
    db: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
    permissions_set: &PrefixesAndCapabilities<'_>,
) -> sqlx::Result<Vec<LiveSpec>> {
    let (prefixes, capabilities): (Vec<String>, Vec<Capability>) = permissions_set
        .iter()
        .map(|(prefix, capabilities)| (prefix.to_string(), capabilities.1))
        .unzip();
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
        ),
        user_roles as materialized (
            select role_prefix, capability from UNNEST($3::text[], $4::grant_capability[]) as t(role_prefix, capability)
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
            (
                select max(capability) from user_roles r
                where starts_with(ls.catalog_name, r.role_prefix)
            ) as "user_capability: Capability",
            coalesce(
                (select json_agg(row_to_json(role_grants))
                from role_grants
                where starts_with(ls.catalog_name, subject_role)),
                '[]'
            ) as "spec_capabilities!: Json<Vec<RoleGrant>>",
            ls.dependency_hash
        from exp
        join live_specs ls on ls.id = exp.id
        where ls.spec is not null and not ls.catalog_name = any($2);
        "#,
        collection_names as &[&str],
        exclude_names as &[&str],
        &prefixes,
        &capabilities as &[Capability],
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
