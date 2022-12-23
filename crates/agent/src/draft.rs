use super::Id;
use agent_sql::{drafts as drafts_sql, CatalogType};
use anyhow::Context;

#[derive(Debug, Default)]
pub struct Error {
    pub catalog_name: String,
    pub scope: Option<String>,
    pub detail: String,
}

// upsert_specs updates the given draft with specifications of the catalog.
pub async fn upsert_specs(
    draft_id: Id,
    models::Catalog {
        collections,
        captures,
        materializations,
        tests,
        ..
    }: models::Catalog,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), sqlx::Error> {
    for (collection, spec) in collections {
        drafts_sql::upsert_spec(
            draft_id,
            collection.as_str(),
            spec,
            CatalogType::Collection,
            txn,
        )
        .await?;
    }
    for (capture, spec) in captures {
        drafts_sql::upsert_spec(draft_id, capture.as_str(), spec, CatalogType::Capture, txn)
            .await?;
    }
    for (materialization, spec) in materializations {
        drafts_sql::upsert_spec(
            draft_id,
            materialization.as_str(),
            spec,
            CatalogType::Materialization,
            txn,
        )
        .await?;
    }
    for (test, steps) in tests {
        drafts_sql::upsert_spec(draft_id, test.as_str(), steps, CatalogType::Test, txn).await?;
    }

    agent_sql::drafts::touch(draft_id, txn).await?;
    Ok(())
}

pub fn extend_catalog<'a>(
    catalog: &mut models::Catalog,
    it: impl Iterator<Item = (CatalogType, &'a str, &'a serde_json::value::RawValue)>,
) -> Vec<Error> {
    let mut errors = Vec::new();

    for (catalog_type, catalog_name, spec) in it {
        let mut on_err = |detail| {
            errors.push(Error {
                catalog_name: catalog_name.to_string(),
                detail,
                ..Error::default()
            });
        };

        match catalog_type {
            CatalogType::Collection => match serde_json::from_str(spec.get()) {
                Ok(spec) => {
                    catalog
                        .collections
                        .insert(models::Collection::new(catalog_name), spec);
                }
                Err(err) => on_err(format!("parsing collection {catalog_name}: {err}")),
            },
            CatalogType::Capture => match serde_json::from_str(spec.get()) {
                Ok(spec) => {
                    catalog
                        .captures
                        .insert(models::Capture::new(catalog_name), spec);
                }
                Err(err) => on_err(format!("parsing capture {catalog_name}: {err}")),
            },
            CatalogType::Materialization => match serde_json::from_str(spec.get()) {
                Ok(spec) => {
                    catalog
                        .materializations
                        .insert(models::Materialization::new(catalog_name), spec);
                }
                Err(err) => on_err(format!("parsing materialization {catalog_name}: {err}")),
            },
            CatalogType::Test => match serde_json::from_str(spec.get()) {
                Ok(spec) => {
                    catalog.tests.insert(models::Test::new(catalog_name), spec);
                }
                Err(err) => on_err(format!("parsing test {catalog_name}: {err}")),
            },
        }
    }

    errors
}

pub async fn insert_errors(
    draft_id: Id,
    errors: Vec<Error>,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<()> {
    for err in errors {
        drafts_sql::insert_error(
            draft_id,
            err.scope.unwrap_or(err.catalog_name),
            err.detail,
            txn,
        )
        .await
        .context("inserting error")?;
    }
    Ok(())
}
