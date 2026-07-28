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
    detail: Option<&str>,
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
        detail,
        catalog_prefix as &str,
        TextJson(spec) as TextJson<T>,
    )
    .execute(&mut **txn)
    .await?;
    Ok(())
}

pub async fn insert_storage_mapping<'e, T, E>(
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
        insert into storage_mappings (detail, catalog_prefix, spec)
        values ($1, $2, $3)
        on conflict (catalog_prefix) do nothing"#,
        detail,
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

/// Returns true if `prefix` ends with `collection-data/` as a distinct trailing
/// path segment: either the prefix is exactly `collection-data/`, or the suffix
/// is preceded by a `/` boundary.
///
/// A raw suffix match is not enough. A user-chosen prefix like
/// `estuary-collection-data/` ends with the same characters but *not* on a
/// segment boundary, and must be treated as an opaque prefix — appending must
/// still add the segment, and stripping must leave it untouched.
fn ends_with_collection_data_segment(prefix: &str) -> bool {
    prefix == COLLECTION_DATA_SUFFIX
        || prefix
            .strip_suffix(COLLECTION_DATA_SUFFIX)
            .is_some_and(|base| base.ends_with('/'))
}

/// Append "collection-data/" to each store's prefix in a StorageDef, if not already present.
pub fn append_collection_data_suffix(storage: models::StorageDef) -> models::StorageDef {
    let models::StorageDef {
        data_planes,
        stores,
    } = storage;

    models::StorageDef {
        data_planes,
        stores: stores
            .into_iter()
            .map(|mut store| {
                let prefix = store.prefix_mut();
                if !ends_with_collection_data_segment(prefix.as_str()) {
                    *prefix = models::Prefix::new(format!("{prefix}{COLLECTION_DATA_SUFFIX}"));
                }
                store
            })
            .collect(),
    }
}

/// Strip the "collection-data/" suffix from each store's prefix in a StorageDef.
///
/// This is the inverse of `append_collection_data_suffix`.
/// Used when returning storage mappings to users via the API.
pub fn strip_collection_data_suffix(storage: models::StorageDef) -> models::StorageDef {
    let models::StorageDef {
        data_planes,
        stores,
    } = storage;

    models::StorageDef {
        data_planes,
        stores: stores
            .into_iter()
            .map(|mut store| {
                let prefix = store.prefix_mut();
                if ends_with_collection_data_segment(prefix.as_str()) {
                    let base = prefix
                        .as_str()
                        .strip_suffix(COLLECTION_DATA_SUFFIX)
                        .expect("segment boundary implies the suffix is present");
                    *prefix = models::Prefix::new(base);
                }
                if prefix.as_str().is_empty() {
                    store.clear_prefix();
                }
                store
            })
            .collect(),
    }
}

/// Split a user-provided `StorageDef` into separate collection and recovery spec definitions.
///
/// The collection spec gets `collection-data/` appended to each store's prefix (if not already
/// present) and retains the data plane assignments. The recovery spec uses the base prefixes
/// (with `collection-data/` stripped if present) and has no data plane assignments.
pub fn collection_and_recovery_spec_from(
    spec: models::StorageDef,
) -> (models::StorageDef, models::StorageDef) {
    let models::StorageDef {
        data_planes,
        stores,
    } = spec;

    let collection_spec = append_collection_data_suffix(models::StorageDef {
        data_planes,
        stores: stores.clone(),
    });

    let recovery_spec = strip_collection_data_suffix(models::StorageDef {
        data_planes: Vec::new(),
        stores,
    });

    (collection_spec, recovery_spec)
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
        let spec = models::StorageDef {
            data_planes: vec!["ops/dp/public/gcp-us-central1".to_string()],
            stores: vec![gcs_store("my-bucket", "tenant/")],
        };

        let (collection, recovery) = collection_and_recovery_spec_from(spec);

        assert_eq!(get_prefix(&collection.stores[0]), "tenant/collection-data/");
        assert_eq!(get_prefix(&recovery.stores[0]), "tenant/");
    }

    #[test]
    fn test_split_does_not_double_append_suffix() {
        let spec = models::StorageDef {
            data_planes: vec!["ops/dp/public/gcp-us-central1".to_string()],
            stores: vec![gcs_store("my-bucket", "tenant/collection-data/")],
        };

        let (collection, recovery) = collection_and_recovery_spec_from(spec);

        assert_eq!(get_prefix(&collection.stores[0]), "tenant/collection-data/");
        assert_eq!(get_prefix(&recovery.stores[0]), "tenant/");
    }

    #[test]
    fn test_split_preserves_data_planes_only_for_collection() {
        let spec = models::StorageDef {
            data_planes: vec![
                "ops/dp/public/gcp-us-central1".to_string(),
                "ops/dp/public/aws-us-east1".to_string(),
            ],
            stores: vec![gcs_store("my-bucket", "tenant/")],
        };

        let (collection, recovery) = collection_and_recovery_spec_from(spec);

        assert_eq!(collection.data_planes.len(), 2);
        assert_eq!(collection.data_planes[0], "ops/dp/public/gcp-us-central1");
        assert_eq!(collection.data_planes[1], "ops/dp/public/aws-us-east1");
        assert!(recovery.data_planes.is_empty());
    }

    #[test]
    fn test_split_handles_multiple_stores() {
        let spec = models::StorageDef {
            data_planes: vec!["ops/dp/public/gcp-us-central1".to_string()],
            stores: vec![
                gcs_store("bucket-a", "prefix-a/"),
                gcs_store("bucket-b", "prefix-b/collection-data/"),
            ],
        };

        let (collection, recovery) = collection_and_recovery_spec_from(spec);

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
        let spec = models::StorageDef {
            data_planes: vec![],
            stores: vec![gcs_store("my-bucket", "")],
        };

        let (collection, recovery) = collection_and_recovery_spec_from(spec);

        assert_eq!(get_prefix(&collection.stores[0]), "collection-data/");
        assert_eq!(get_prefix(&recovery.stores[0]), "");
    }

    // A user-chosen prefix that ends with the characters "collection-data/"
    // without a preceding "/" boundary is an opaque prefix, not the managed
    // segment. The suffix must be appended to the collection spec and left
    // intact on the recovery spec — never chopped mid-word (which previously
    // mangled `estuary-collection-data/` into the invalid prefix `estuary-`).
    #[test]
    fn test_split_ignores_non_boundary_suffix_match() {
        let spec = models::StorageDef {
            data_planes: vec!["ops/dp/public/gcp-us-central1".to_string()],
            stores: vec![gcs_store("my-bucket", "estuary-collection-data/")],
        };

        let (collection, recovery) = collection_and_recovery_spec_from(spec);

        assert_eq!(
            get_prefix(&collection.stores[0]),
            "estuary-collection-data/collection-data/"
        );
        assert_eq!(get_prefix(&recovery.stores[0]), "estuary-collection-data/");
    }

    // The managed segment is only recognized on a "/" boundary, including when
    // it is nested under a further prefix.
    #[test]
    fn test_split_strips_nested_boundary_suffix() {
        let spec = models::StorageDef {
            data_planes: vec![],
            stores: vec![gcs_store("my-bucket", "tenant/nested/collection-data/")],
        };

        let (collection, recovery) = collection_and_recovery_spec_from(spec);

        assert_eq!(
            get_prefix(&collection.stores[0]),
            "tenant/nested/collection-data/"
        );
        assert_eq!(get_prefix(&recovery.stores[0]), "tenant/nested/");
    }

    #[test]
    fn test_ends_with_collection_data_segment() {
        // Exactly the segment, and the segment on a "/" boundary.
        assert!(ends_with_collection_data_segment("collection-data/"));
        assert!(ends_with_collection_data_segment("tenant/collection-data/"));
        assert!(ends_with_collection_data_segment(
            "tenant/nested/collection-data/"
        ));

        // Same trailing characters, but not a distinct segment.
        assert!(!ends_with_collection_data_segment(
            "estuary-collection-data/"
        ));
        // No segment at all.
        assert!(!ends_with_collection_data_segment("tenant/"));
        assert!(!ends_with_collection_data_segment(""));
    }

    #[test]
    fn test_strip_clears_root_prefix() {
        let stripped = strip_collection_data_suffix(models::StorageDef {
            data_planes: vec!["ops/dp/public/gcp-us-central1".to_string()],
            stores: vec![
                gcs_store("bucket-a", "tenant/collection-data/"),
                gcs_store("bucket-b", "collection-data/"),
                gcs_store("bucket-c", "tenant/"),
            ],
        });

        // A nested prefix keeps its base once the suffix is removed.
        assert_eq!(get_prefix(&stripped.stores[0]), "tenant/");
        // A prefix that is *only* the suffix strips to empty, and the store's
        // prefix is cleared to None rather than left as an empty string. This
        // is the shape returned to the user when a mapping is created at a bare
        // tenant root, where the collection spec's prefix is just the suffix.
        assert!(
            matches!(&stripped.stores[1], models::Store::Gcs(cfg) if cfg.prefix.is_none()),
            "expected root prefix to be cleared to None, got: {:?}",
            stripped.stores[1],
        );
        // A prefix without the suffix is left untouched.
        assert_eq!(get_prefix(&stripped.stores[2]), "tenant/");
        // Data planes pass through unchanged.
        assert_eq!(
            stripped.data_planes,
            vec!["ops/dp/public/gcp-us-central1".to_string()]
        );
    }
}
