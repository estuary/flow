use super::Error;

use anyhow::Context;

#[derive(Debug)]
pub struct StorageRow {
    pub catalog_prefix: String,
    pub spec: serde_json::Value,
}

// inject_mappings identifies all storage mappings which may relate to the
// provided set of catalog names, and adds each to the models::Catalog.
pub async fn inject_mappings(
    names: impl Iterator<Item = &str>,
    catalog: &mut models::Catalog,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<Vec<Error>> {
    let names: Vec<&str> = names.collect();

    let mappings: Vec<StorageRow> = sqlx::query_as!(
        StorageRow,
        r#"
        select
            m.catalog_prefix,
            m.spec
        from storage_mappings m,
        lateral unnest($1::text[]) as n
        where starts_with(n, m.catalog_prefix)
           or starts_with('recovery/' || n, m.catalog_prefix)
           -- TODO(johnny): hack until we better-integrate ops collections.
           or m.catalog_prefix = 'ops/'
        group by m.id;
        "#,
        names as Vec<&str>,
    )
    .fetch_all(&mut *txn)
    .await
    .context("selecting storage mappings")?;

    let mut errors = Vec::new();

    for StorageRow {
        catalog_prefix,
        spec,
    } in mappings
    {
        match serde_json::from_value::<models::StorageDef>(spec) {
            Ok(spec) => {
                catalog
                    .storage_mappings
                    .insert(models::Prefix::new(catalog_prefix), spec);
            }
            Err(err) => {
                errors.push(Error {
                    catalog_name: catalog_prefix.clone(),
                    detail: format!("invalid storage mapping {catalog_prefix}: {err:?}"),
                    ..Error::default()
                });
            }
        }
    }

    Ok(errors)
}
