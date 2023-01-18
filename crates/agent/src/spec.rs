use super::{CatalogType, Id};
use anyhow::Context;
use serde_json::value::RawValue;

/// Spec enumerates over the specification types known to the control-plane.
#[derive(Debug, serde::Serialize)]
pub enum Spec {
    Capture(models::CaptureDef),
    Collection(models::CollectionDef),
    Materialization(models::MaterializationDef),
    Test(Vec<models::TestStep>),
}

#[derive(thiserror::Error, Debug)]
#[error("invalid {catalog_type:?} {catalog_name:?}: {inner}")]
pub struct ParseError {
    pub catalog_name: String,
    pub catalog_type: CatalogType,
    pub inner: serde_json::Error,
}

impl Spec {
    /// Parse a named catalog specification. Errors only if the specification
    /// cannot parse under the model type, and performs no other validation.
    pub fn parse(
        catalog_name: &str,
        catalog_type: CatalogType,
        spec: &RawValue,
    ) -> Result<(String, Spec), ParseError> {
        let result = match catalog_type {
            CatalogType::Capture => serde_json::from_str(spec.get()).map(Self::Capture),
            CatalogType::Collection => serde_json::from_str(spec.get()).map(Self::Collection),
            CatalogType::Materialization => {
                serde_json::from_str(spec.get()).map(Self::Materialization)
            }
            CatalogType::Test => serde_json::from_str(spec.get()).map(Self::Test),
        };

        let catalog_name = catalog_name.to_string();
        match result {
            Ok(spec) => Ok((catalog_name, spec)),
            Err(inner) => Err(ParseError {
                catalog_name,
                catalog_type,
                inner,
            }),
        }
    }

    // Map an iterator of named Specs into upsert's over a control-plane draft.
    pub async fn upsert_draft(
        draft_id: Id,
        it: impl Iterator<Item = (String, Spec)>,
        txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> Result<(), anyhow::Error> {
        for (catalog_name, spec) in it {
            match spec {
                Spec::Capture(spec) => {
                    agent_sql::upsert_draft_spec(
                        draft_id,
                        &catalog_name,
                        spec,
                        CatalogType::Capture,
                        txn,
                    )
                    .await
                }
                Spec::Collection(spec) => {
                    agent_sql::upsert_draft_spec(
                        draft_id,
                        &catalog_name,
                        spec,
                        CatalogType::Collection,
                        txn,
                    )
                    .await
                }
                Spec::Materialization(spec) => {
                    agent_sql::upsert_draft_spec(
                        draft_id,
                        &catalog_name,
                        spec,
                        CatalogType::Materialization,
                        txn,
                    )
                    .await
                }
                Spec::Test(spec) => {
                    agent_sql::upsert_draft_spec(
                        draft_id,
                        &catalog_name,
                        spec,
                        CatalogType::Test,
                        txn,
                    )
                    .await
                }
            }
            .context("failed to upsert draft spec {catalog_name}")?;
        }
        agent_sql::touch_draft(draft_id, txn).await?;
        Ok(())
    }

    pub fn extend_catalog(catalog: &mut models::Catalog, it: impl Iterator<Item = (String, Spec)>) {
        for (name, spec) in it {
            match spec {
                Self::Capture(spec) => {
                    catalog.captures.insert(models::Capture::new(name), spec);
                }
                Self::Collection(spec) => {
                    catalog
                        .collections
                        .insert(models::Collection::new(name), spec);
                }
                Self::Materialization(spec) => {
                    catalog
                        .materializations
                        .insert(models::Materialization::new(name), spec);
                }
                Self::Test(spec) => {
                    catalog.tests.insert(models::Test::new(name), spec);
                }
            }
        }
    }

    pub fn from_catalog(catalog: models::Catalog) -> impl Iterator<Item = (String, Spec)> {
        let models::Catalog {
            _schema: _,
            resources: _,
            import: _,
            captures,
            collections,
            materializations,
            tests,
            storage_mappings: _,
        } = catalog;

        let captures = captures
            .into_iter()
            .map(|(name, spec)| (name.into(), Self::Capture(spec)));
        let collections = collections
            .into_iter()
            .map(|(name, spec)| (name.into(), Self::Collection(spec)));
        let materializations = materializations
            .into_iter()
            .map(|(name, spec)| (name.into(), Self::Materialization(spec)));
        let tests = tests
            .into_iter()
            .map(|(name, spec)| (name.into(), Self::Test(spec)));

        captures
            .chain(collections)
            .chain(materializations)
            .chain(tests)
    }
}
