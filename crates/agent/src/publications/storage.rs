use super::draft::Error;
use agent_sql::publications::StorageRow;
use anyhow::Context;

// inject_mappings identifies all storage mappings which may relate to the
// provided set of catalog names, and adds each to the models::Catalog.
pub async fn inject_mappings(
    names: impl Iterator<Item = &str>,
    catalog: &mut models::Catalog,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<Vec<Error>> {
    let names: Vec<&str> = names.collect();

    let mappings = agent_sql::publications::resolve_storage_mappings(names, txn)
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
