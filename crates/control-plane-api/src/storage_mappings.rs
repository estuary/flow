use crate::TextJson;
use anyhow::Context;
use models::Id;
use serde_json::value::RawValue;
use std::collections::BTreeMap;

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

#[derive(Debug)]
pub(crate) struct StorageRow {
    pub id: Id,
    pub catalog_prefix: String,
    pub spec: serde_json::Value,
    pub recovery_spec: Option<serde_json::Value>,
}

/// Returns the storage mappings for the given set of tenants,
/// each joined with its `recovery/` twin.
///
/// `recovery/` mappings are omitted as rows in their own right: drafted names
/// are unvalidated, and a drafted `recovery/...` name would otherwise select
/// the recovery mappings of every tenant on the platform.
pub(crate) async fn resolve_storage_mappings(
    tenant_names: Vec<&str>,
    db: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
) -> sqlx::Result<Vec<StorageRow>> {
    sqlx::query_as!(
        StorageRow,
        r#"
        select
            m.id as "id: Id",
            m.catalog_prefix,
            m.spec,
            r.spec as "recovery_spec?"
        from unnest($1::text[]) t(name)
        join storage_mappings m on starts_with(m.catalog_prefix, t.name)
        left join storage_mappings r on r.catalog_prefix = 'recovery/' || m.catalog_prefix
        where not starts_with(m.catalog_prefix, 'recovery/');
        "#,
        tenant_names as Vec<&str>,
    )
    .fetch_all(db)
    .await
}

/// Build `tables::StorageMappings` from fetched `storage_mappings` rows, where
/// each row pairs the partition and recovery stores of a prefix. Also returns
/// the ordered data-plane names of each mapping, which are used for placement.
///
/// A spec which doesn't deserialize, or a mapping without its `recovery/` twin,
/// is an internal error: both are control-plane invariants which users can't
/// provoke.
pub(crate) fn join_storage_mappings(
    rows: Vec<StorageRow>,
) -> anyhow::Result<(
    tables::StorageMappings,
    BTreeMap<models::Prefix, Vec<String>>,
)> {
    let mut mappings = tables::StorageMappings::default();
    let mut mapping_planes = BTreeMap::new();

    for StorageRow {
        id,
        catalog_prefix,
        spec,
        recovery_spec,
    } in rows
    {
        let spec: models::StorageDef = serde_json::from_value(spec)
            .with_context(|| format!("deserializing storage mapping {catalog_prefix}"))?;
        let recovery_spec = recovery_spec.ok_or_else(|| {
            anyhow::anyhow!(
                "storage mapping {catalog_prefix} has no paired recovery/{catalog_prefix} mapping"
            )
        })?;
        let recovery: models::StorageDef = serde_json::from_value(recovery_spec)
            .with_context(|| format!("deserializing storage mapping recovery/{catalog_prefix}"))?;
        let prefix = models::Prefix::new(catalog_prefix);

        mappings.insert_row(&prefix, id, spec.stores, recovery.stores);
        mapping_planes.insert(prefix, spec.data_planes);
    }

    Ok((mappings, mapping_planes))
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

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn test_resolve_and_join_storage_mappings(pool: sqlx::PgPool) {
        sqlx::query(
            r#"
            insert into storage_mappings (id, catalog_prefix, spec) values
              ('00:00:00:00:00:00:00:01', 'aliceCo/', '{"stores":[{"provider":"S3","bucket":"alice"}]}'),
              ('00:00:00:00:00:00:00:02', 'recovery/aliceCo/', '{"stores":[{"provider":"S3","bucket":"alice"}]}'),
              ('00:00:00:00:00:00:00:03', 'bobCo/', '{"stores":[{"provider":"S3","bucket":"bob","prefix":"collection-data/"}],"data_planes":["ops/dp/public/one"]}'),
              ('00:00:00:00:00:00:00:04', 'recovery/bobCo/', '{"stores":[{"provider":"S3","bucket":"bob"}]}'),
              ('00:00:00:00:00:00:00:05', 'daveCo/', '{"stores":[{"provider":"S3","bucket":"dave"}]}'),
              ('00:00:00:00:00:00:00:06', 'recovery/orphanCo/', '{"stores":[{"provider":"S3","bucket":"orphan"}]}'),
              ('00:00:00:00:00:00:00:07', 'carolCo/', '{"stores":[{"provider":"S3","bucket":"carol"}]}'),
              ('00:00:00:00:00:00:00:08', 'recovery/carolCo/', '{"stores":42}');
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();

        // A drafted `recovery/...` name yields a `recovery/` tenant, which must
        // not select the recovery mappings of other tenants.
        let rows = resolve_storage_mappings(vec!["bobCo/", "recovery/"], &pool)
            .await
            .unwrap();
        insta::assert_debug_snapshot!(join_storage_mappings(rows).unwrap(), @r#"
        (
            [
                StorageMapping {
                    catalog_prefix: bobCo/,
                    control_id: "0000000000000003",
                    stores: [
                      {
                        "provider": "S3",
                        "bucket": "bob",
                        "prefix": "collection-data/",
                        "region": null
                      }
                    ],
                    recovery_stores: [
                      {
                        "provider": "S3",
                        "bucket": "bob",
                        "prefix": null,
                        "region": null
                      }
                    ],
                },
            ],
            {
                Prefix(
                    "bobCo/",
                ): [
                    "ops/dp/public/one",
                ],
            },
        )
        "#);

        let rows = resolve_storage_mappings(vec!["carolCo/"], &pool)
            .await
            .unwrap();
        insta::assert_snapshot!(format!("{:#}", join_storage_mappings(rows).unwrap_err()), @"deserializing storage mapping recovery/carolCo/: invalid type: integer `42`, expected a sequence");

        // A mapping without its `recovery/` twin is an error.
        let rows = resolve_storage_mappings(vec!["daveCo/"], &pool)
            .await
            .unwrap();
        insta::assert_snapshot!(format!("{:#}", join_storage_mappings(rows).unwrap_err()), @"storage mapping daveCo/ has no paired recovery/daveCo/ mapping");
    }
}
