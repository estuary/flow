use std::collections::BTreeMap;

use crate::publications::LockFailure;

use super::Id;
use agent_sql::{drafts as drafts_sql, CatalogType};
use anyhow::Context;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Clone, JsonSchema)]
pub struct Error {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub catalog_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    pub detail: String,
}

impl Error {
    pub fn from_tables_error(err: &tables::Error) -> Self {
        let catalog_name = tables::parse_synthetic_scope(&err.scope)
            .map(|(_, name)| name)
            .unwrap_or_default();
        Error {
            catalog_name,
            scope: Some(err.scope.to_string()),
            // use alternate to print chained contexts
            detail: format!("{:#}", err.error),
        }
    }
}

impl From<LockFailure> for Error {
    fn from(err: LockFailure) -> Self {
        let detail = format!(
            "the expectPubId of spec {:?} {:?} did not match that of the live spec {:?}",
            err.catalog_name, err.expected, err.actual
        );
        Error {
            catalog_name: err.catalog_name,
            detail,
            scope: None,
        }
    }
}

/// upsert_specs updates the given draft with specifications of the catalog.
/// The `expect_pub_ids` parameter is used to lookup the `last_pub_id` by catalog name.
/// For each item in the catalog, if an entry exists in `expect_pub_ids`, then it will
/// be used as the `expect_pub_id` column.
pub async fn upsert_specs(
    draft_id: Id,
    models::Catalog {
        collections,
        captures,
        materializations,
        tests,
        ..
    }: models::Catalog,
    expect_pub_ids: &BTreeMap<&str, Id>,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), sqlx::Error> {
    for (collection, spec) in collections {
        drafts_sql::upsert_spec(
            draft_id,
            collection.as_str(),
            spec,
            CatalogType::Collection,
            expect_pub_ids.get(collection.as_str()).copied(),
            txn,
        )
        .await?;
    }
    for (capture, spec) in captures {
        drafts_sql::upsert_spec(
            draft_id,
            capture.as_str(),
            spec,
            CatalogType::Capture,
            expect_pub_ids.get(capture.as_str()).copied(),
            txn,
        )
        .await?;
    }
    for (materialization, spec) in materializations {
        drafts_sql::upsert_spec(
            draft_id,
            materialization.as_str(),
            spec,
            CatalogType::Materialization,
            expect_pub_ids.get(materialization.as_str()).copied(),
            txn,
        )
        .await?;
    }
    for (test, steps) in tests {
        drafts_sql::upsert_spec(
            draft_id,
            test.as_str(),
            steps,
            CatalogType::Test,
            expect_pub_ids.get(test.as_str()).copied(),
            txn,
        )
        .await?;
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
