use sqlx::PgPool;

use super::{CatalogType, Id, TextJson};
use serde_json::value::RawValue;

/// Creates a draft for the given user and returns the draft id. A user with
/// the given email must exist, and the email must have been confirmed, or else
/// the insert will fail due to a not-null constraint when inserting into the
/// drafts table.
pub async fn create(
    user_email: &str,
    detail: String,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<Id> {
    let row = sqlx::query!(
        r#"insert into drafts (user_id, detail)
            values ( (select id from auth.users where email = $1 and email_confirmed_at is not null), $2)
            returning id as "id: Id";"#,
        user_email,
        detail
    ).fetch_one(&mut **txn)
    .await?;
    Ok(row.id)
}

pub struct DraftSpec {
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
) -> sqlx::Result<()> {
    sqlx::query!(
        r#"
        delete from draft_specs
        where draft_id = $1 and catalog_name = any($2)
        "#,
        draft_id as Id,
        catalog_names as &[&str],
    )
    .execute(txn)
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

pub async fn delete_spec(
    draft_spec_id: Id,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<()> {
    sqlx::query!(
        r#"delete from draft_specs where id = $1 returning 1 as "must_exist";"#,
        draft_spec_id as Id,
    )
    .fetch_one(&mut **txn)
    .await?;

    Ok(())
}

#[derive(Debug)]
pub struct PrunedDraftSpec {
    pub catalog_name: String,
    pub spec_type: Option<CatalogType>,
    pub live_spec_md5: String,
    pub draft_spec_md5: String,
    pub inferred_schema_md5: Option<String>,
    pub live_inferred_schema_md5: Option<String>,
}

pub async fn prune_unchanged_draft_specs(
    draft_id: Id,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<Vec<PrunedDraftSpec>> {
    sqlx::query_as!(
        PrunedDraftSpec,
        r#"select
        catalog_name as "catalog_name!: String",
        spec_type as "spec_type: CatalogType",
        live_spec_md5 as "live_spec_md5!: String",
        draft_spec_md5 as "draft_spec_md5!: String",
        inferred_schema_md5,
        live_inferred_schema_md5
        from prune_unchanged_draft_specs($1)"#,
        draft_id as Id,
    )
    .fetch_all(&mut **txn)
    .await
}
