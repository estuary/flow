use crate::TextJson;
use chrono::Utc;
use serde_json::value::RawValue;
use sqlx::types::Uuid;

pub async fn user_has_admin_capability(
    user_id: Uuid,
    catalog_prefix: &str,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<bool> {
    let row = sqlx::query!(
        r#"select true as whatever_column from internal.user_roles($1, 'admin') where starts_with(role_prefix, $2)"#,
        user_id,
        catalog_prefix,
    )
    .fetch_optional(txn)
    .await?;
    Ok(row.is_some())
}

pub async fn upsert_storage_mapping<T: serde::Serialize + Send + Sync>(
    detail: &str,
    catalog_prefix: &str,
    spec: T,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<()> {
    sqlx::query!(
        r#"
        insert into storage_mappings (detail, catalog_prefix, spec, updated_at)
        values ($1, $2, $3, $4)
        on conflict (catalog_prefix) do update set detail = $1, spec = $3"#,
        detail as &str,
        catalog_prefix as &str,
        TextJson(spec) as TextJson<T>,
        Utc::now()
    )
    .execute(txn)
    .await?;
    Ok(())
}

#[derive(Debug)]
pub struct StorageMapping {
    pub catalog_prefix: String,
    pub spec: TextJson<Box<RawValue>>,
}

pub async fn fetch_storage_mappings(
    catalog_prefix: &str,
    recovery_prefix: &str,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<Vec<StorageMapping>> {
    sqlx::query_as!(
        StorageMapping,
        r#"select
            catalog_prefix,
            spec as "spec: TextJson<Box<RawValue>>"
         from storage_mappings
         where catalog_prefix = $1 or catalog_prefix = $2
         for update of storage_mappings"#,
        catalog_prefix,
        recovery_prefix
    )
    .fetch_all(txn)
    .await
}
