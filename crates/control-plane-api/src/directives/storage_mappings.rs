use crate::TextJson;
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
    .fetch_optional(&mut **txn)
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
        insert into storage_mappings (detail, catalog_prefix, spec)
        values ($1, $2, $3)
        on conflict (catalog_prefix) do update set
            detail = $1,
            spec = $3,
            updated_at = now()"#,
        detail as &str,
        catalog_prefix as &str,
        TextJson(spec) as TextJson<T>,
    )
    .execute(&mut **txn)
    .await?;
    Ok(())
}

pub async fn insert_storage_mapping<'e, T, E>(
    detail: &str,
    catalog_prefix: &str,
    spec: T,
    executor: E,
) -> sqlx::Result<bool>
where
    T: serde::Serialize + Send + Sync,
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let result = sqlx::query!(
        r#"
        insert into storage_mappings (detail, catalog_prefix, spec)
        values ($1, $2, $3)
        on conflict (catalog_prefix) do nothing"#,
        detail as &str,
        catalog_prefix as &str,
        TextJson(spec) as TextJson<T>,
    )
    .execute(executor)
    .await?;
    Ok(result.rows_affected() > 0)
}

pub async fn update_storage_mapping<'e, T, E>(
    detail: Option<&str>,
    catalog_prefix: &str,
    spec: T,
    executor: E,
) -> sqlx::Result<bool>
where
    T: serde::Serialize + Send + Sync,
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let result = sqlx::query!(
        r#"
        update storage_mappings set
            detail = $1,
            spec = $2,
            updated_at = now()
        where catalog_prefix = $3"#,
        detail,
        TextJson(spec) as TextJson<T>,
        catalog_prefix as &str,
    )
    .execute(executor)
    .await?;
    Ok(result.rows_affected() > 0)
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
    .fetch_all(&mut **txn)
    .await
}

const COLLECTION_DATA_SUFFIX: &str = "collection-data/";

/// Split a user-provided `StorageDef` into separate collection and recovery storage definitions.
///
/// The collection storage gets `collection-data/` appended to each store's prefix (if not already
/// present) and retains the data plane assignments. The recovery storage uses the base prefixes
/// (with `collection-data/` stripped if present) and has no data plane assignments.
pub fn split_collection_and_recovery_storage(
    storage: models::StorageDef,
) -> (models::StorageDef, models::StorageDef) {
    let models::StorageDef {
        data_planes,
        stores,
    } = storage;

    let collection_storage = models::StorageDef {
        data_planes,
        stores: stores
            .iter()
            .cloned()
            .map(|mut store| {
                let prefix = store.prefix_mut();
                if !prefix.as_str().ends_with(COLLECTION_DATA_SUFFIX) {
                    *prefix = models::Prefix::new(format!("{prefix}{COLLECTION_DATA_SUFFIX}"));
                }
                store
            })
            .collect(),
    };

    let recovery_storage = models::StorageDef {
        data_planes: Vec::new(),
        stores: stores
            .into_iter()
            .map(|mut store| {
                let prefix = store.prefix_mut();
                if let Some(base) = prefix.as_str().strip_suffix(COLLECTION_DATA_SUFFIX) {
                    *prefix = models::Prefix::new(base);
                }
                store
            })
            .collect(),
    };

    (collection_storage, recovery_storage)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gcs_store(bucket: &str, prefix: &str) -> models::Store {
        models::Store::Gcs(models::GcsBucketAndPrefix {
            bucket: bucket.to_string(),
            prefix: Some(models::Prefix::new(prefix)),
        })
    }

    fn get_prefix(store: &models::Store) -> &str {
        match store {
            models::Store::Gcs(cfg) => cfg.prefix.as_ref().map(|p| p.as_str()).unwrap_or(""),
            _ => panic!("unexpected store type"),
        }
    }

    #[test]
    fn test_split_appends_collection_data_suffix() {
        let storage = models::StorageDef {
            data_planes: vec!["ops/dp/public/gcp-us-central1".to_string()],
            stores: vec![gcs_store("my-bucket", "tenant/")],
        };

        let (collection, recovery) = split_collection_and_recovery_storage(storage);

        assert_eq!(get_prefix(&collection.stores[0]), "tenant/collection-data/");
        assert_eq!(get_prefix(&recovery.stores[0]), "tenant/");
    }

    #[test]
    fn test_split_does_not_double_append_suffix() {
        let storage = models::StorageDef {
            data_planes: vec!["ops/dp/public/gcp-us-central1".to_string()],
            stores: vec![gcs_store("my-bucket", "tenant/collection-data/")],
        };

        let (collection, recovery) = split_collection_and_recovery_storage(storage);

        assert_eq!(get_prefix(&collection.stores[0]), "tenant/collection-data/");
        assert_eq!(get_prefix(&recovery.stores[0]), "tenant/");
    }

    #[test]
    fn test_split_preserves_data_planes_only_for_collection() {
        let storage = models::StorageDef {
            data_planes: vec![
                "ops/dp/public/gcp-us-central1".to_string(),
                "ops/dp/public/aws-us-east1".to_string(),
            ],
            stores: vec![gcs_store("my-bucket", "tenant/")],
        };

        let (collection, recovery) = split_collection_and_recovery_storage(storage);

        assert_eq!(collection.data_planes.len(), 2);
        assert_eq!(collection.data_planes[0], "ops/dp/public/gcp-us-central1");
        assert_eq!(collection.data_planes[1], "ops/dp/public/aws-us-east1");
        assert!(recovery.data_planes.is_empty());
    }

    #[test]
    fn test_split_handles_multiple_stores() {
        let storage = models::StorageDef {
            data_planes: vec!["ops/dp/public/gcp-us-central1".to_string()],
            stores: vec![
                gcs_store("bucket-a", "prefix-a/"),
                gcs_store("bucket-b", "prefix-b/collection-data/"),
            ],
        };

        let (collection, recovery) = split_collection_and_recovery_storage(storage);

        assert_eq!(collection.stores.len(), 2);
        assert_eq!(
            get_prefix(&collection.stores[0]),
            "prefix-a/collection-data/"
        );
        assert_eq!(
            get_prefix(&collection.stores[1]),
            "prefix-b/collection-data/"
        );

        assert_eq!(recovery.stores.len(), 2);
        assert_eq!(get_prefix(&recovery.stores[0]), "prefix-a/");
        assert_eq!(get_prefix(&recovery.stores[1]), "prefix-b/");
    }

    #[test]
    fn test_split_handles_empty_prefix() {
        let storage = models::StorageDef {
            data_planes: vec![],
            stores: vec![gcs_store("my-bucket", "")],
        };

        let (collection, recovery) = split_collection_and_recovery_storage(storage);

        assert_eq!(get_prefix(&collection.stores[0]), "collection-data/");
        assert_eq!(get_prefix(&recovery.stores[0]), "");
    }
}
