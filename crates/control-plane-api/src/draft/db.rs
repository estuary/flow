use crate::TextJson;
use models::{CatalogType, Id};
use serde_json::value::RawValue;
use sqlx::PgPool;

pub struct DraftSpec {
    #[allow(unused)]
    pub draft_id: Id,
    pub catalog_name: String,
    pub spec_type: Option<CatalogType>,
    pub spec: Option<TextJson<Box<RawValue>>>,
    pub expect_pub_id: Option<Id>,
}

pub async fn fetch_draft_specs(
    draft_id: Id,
    db: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
) -> sqlx::Result<Vec<DraftSpec>> {
    sqlx::query_as!(
        DraftSpec,
        r#"
        select
            ds.draft_id as "draft_id!: Id",
            ds.catalog_name as "catalog_name!: String",
            coalesce(ds.spec_type, ls.spec_type) as "spec_type?: CatalogType",
            ds.spec as "spec?: TextJson<Box<RawValue>>",
            ds.expect_pub_id as "expect_pub_id?: Id"
        from draft_specs ds
        left join live_specs ls on ds.catalog_name = ls.catalog_name
        where ds.draft_id = $1;
        "#,
        draft_id as Id,
    )
    .fetch_all(db)
    .await
}

pub async fn delete_specs(
    draft_id: Id,
    catalog_names: &[&str],
    txn: &mut sqlx::PgConnection,
) -> sqlx::Result<Vec<models::Name>> {
    let mut deleted = sqlx::query!(
        r#"
        delete from draft_specs
        where draft_id = $1 and catalog_name::text = any($2)
        returning catalog_name as "catalog_name!: models::Name"
        "#,
        draft_id as Id,
        catalog_names as &[&str],
    )
    .fetch_all(txn)
    .await?
    .into_iter()
    .map(|row| row.catalog_name)
    .collect::<Vec<_>>();
    deleted.sort();
    Ok(deleted)
}

/// Replaces all client-editable columns of one draft specification.
///
/// This is intentionally separate from [`upsert_spec`], whose conflict path
/// preserves `expect_pub_id` for existing discovery and publication callers.
#[tracing::instrument(err, level = "debug", skip(spec, txn))]
pub async fn replace_spec<S>(
    draft_id: Id,
    catalog_name: &str,
    spec: Option<S>,
    spec_type: Option<CatalogType>,
    expect_pub_id: Option<Id>,
    detail: Option<&str>,
    txn: &mut sqlx::PgConnection,
) -> sqlx::Result<()>
where
    S: serde::Serialize + Send + Sync,
{
    sqlx::query!(
        r#"
        insert into draft_specs(
            draft_id,
            catalog_name,
            spec,
            spec_type,
            expect_pub_id,
            detail
        ) values ($1, $2, $3, $4, $5, $6)
        on conflict (draft_id, catalog_name) do update set
            spec = excluded.spec,
            spec_type = excluded.spec_type,
            expect_pub_id = excluded.expect_pub_id,
            detail = excluded.detail,
            updated_at = clock_timestamp()
        returning 1 as "must_exist";
        "#,
        draft_id as Id,
        catalog_name as &str,
        spec.map(TextJson) as Option<TextJson<S>>,
        spec_type as Option<CatalogType>,
        expect_pub_id as Option<Id>,
        detail as Option<&str>,
    )
    .fetch_one(txn)
    .await?;
    Ok(())
}

#[tracing::instrument(err, level = "debug", skip(spec, txn))]
pub async fn upsert_spec<S>(
    draft_id: Id,
    catalog_name: &str,
    spec: S,
    spec_type: CatalogType,
    expect_pub_id: Option<Id>,
    txn: &mut sqlx::PgConnection,
) -> sqlx::Result<()>
where
    S: serde::Serialize + Send + Sync,
{
    sqlx::query!(
        r#"
        insert into draft_specs(
            draft_id,
            catalog_name,
            spec,
            spec_type,
            expect_pub_id
        ) values ($1, $2, $3, $4, $5)
        on conflict (draft_id, catalog_name) do update set
            spec = $3,
            spec_type = $4
        returning 1 as "must_exist";
        "#,
        draft_id as Id,
        catalog_name as &str,
        TextJson(spec) as TextJson<S>,
        spec_type as CatalogType,
        expect_pub_id as Option<Id>,
    )
    .fetch_one(txn)
    .await?;
    Ok(())
}

pub async fn add_built_spec<S, V>(
    draft_id: Id,
    catalog_name: &str,
    built_spec: S,
    validated: Option<V>,
    db: &PgPool,
) -> sqlx::Result<()>
where
    S: serde::Serialize + Send + Sync,
    V: serde::Serialize + Send + Sync,
{
    sqlx::query!(
        r#"
        update draft_specs set built_spec = $1, validated = $2
        where draft_id = $3 and catalog_name = $4;
        "#,
        TextJson(built_spec) as TextJson<S>,
        validated.map(|v| TextJson(v)) as Option<TextJson<V>>,
        draft_id as Id,
        catalog_name as &str,
    )
    .execute(db)
    .await?;

    Ok(())
}

// touch_draft updates the modification time of the draft to now.
pub async fn touch(draft_id: Id, txn: &mut sqlx::PgConnection) -> sqlx::Result<()> {
    sqlx::query!(
        r#"update drafts set updated_at = clock_timestamp() where id = $1
            returning 1 as "must_exist";"#,
        draft_id as Id,
    )
    .fetch_one(txn)
    .await?;

    Ok(())
}

pub async fn delete_errors(draft_id: Id, txn: &mut sqlx::PgConnection) -> sqlx::Result<()> {
    sqlx::query!(
        "delete from draft_errors where draft_id = $1",
        draft_id as Id
    )
    .execute(txn)
    .await?;

    Ok(())
}

pub async fn insert_error(
    draft_id: Id,
    scope: String,
    detail: String,
    txn: &mut sqlx::PgConnection,
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
    .execute(txn)
    .await?;

    Ok(())
}
