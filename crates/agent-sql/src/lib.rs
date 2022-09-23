pub mod connector_tags;
pub mod directives;
pub mod discover;
pub mod publications;
use serde::{Deserialize, Serialize};

mod id;
pub use id::Id;

mod text_json;
pub use text_json::TextJson;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "catalog_spec_type")]
#[sqlx(rename_all = "lowercase")]
pub enum CatalogType {
    Capture,
    Collection,
    Materialization,
    Test,
}

#[derive(
    Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type, schemars::JsonSchema,
)]
#[sqlx(type_name = "grant_capability")]
#[sqlx(rename_all = "lowercase")]
#[serde(rename_all = "camelCase")]
pub enum Capability {
    Read,
    Write,
    Admin,
}

// upsert_draft_spec inserts or updates the given specification into the draft.
#[tracing::instrument(err, skip(spec))]
pub async fn upsert_draft_spec<S>(
    draft_id: Id,
    catalog_name: &str,
    spec: S,
    spec_type: CatalogType,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<()>
where
    S: Serialize + Send + Sync,
{
    sqlx::query!(
        r#"
        insert into draft_specs(
            draft_id,
            catalog_name,
            spec,
            spec_type
        ) values ($1, $2, $3, $4)
        on conflict (draft_id, catalog_name) do update set
            spec = $3,
            spec_type = $4
        returning 1 as "must_exist";
        "#,
        draft_id as Id,
        catalog_name as &str,
        TextJson(spec) as TextJson<S>,
        spec_type as CatalogType,
    )
    .fetch_one(&mut *txn)
    .await?;

    Ok(())
}

// touch_draft updates the modification time of the draft to now.
pub async fn touch_draft(
    draft_id: Id,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<()> {
    sqlx::query!(
        r#"update drafts set updated_at = clock_timestamp() where id = $1
            returning 1 as "must_exist";"#,
        draft_id as Id,
    )
    .fetch_one(&mut *txn)
    .await?;

    Ok(())
}
