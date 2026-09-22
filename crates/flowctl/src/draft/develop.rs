use crate::local_specs;
use models::{CatalogType, RawValue};

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Develop {
    /// Root flow specification to create or update.
    #[clap(long, default_value = "flow.yaml")]
    target: String,
    /// Should existing specs be over-written by specs from the Flow control plane?
    #[clap(long)]
    overwrite: bool,
    /// Should specs be written to the single specification file, or written in the canonical layout?
    #[clap(long)]
    flat: bool,
}

pub async fn do_develop(
    ctx: &mut crate::CliContext,
    Develop {
        target,
        overwrite,
        flat,
    }: &Develop,
) -> anyhow::Result<()> {
    let draft_id = ctx.config.selected_draft()?;
    develop(ctx, draft_id, target, *overwrite, *flat).await
}

pub async fn develop(
    ctx: &mut crate::CliContext,
    draft_id: models::Id,
    target: &str,
    overwrite: bool,
    flat: bool,
) -> anyhow::Result<()> {
    // Drafted deletions have no model to develop, and are skipped.
    let rows = super::fetch_draft_specs(ctx, draft_id, true)
        .await?
        .into_iter()
        .filter_map(|spec| {
            Some(DraftSpecRow {
                catalog_name: spec.catalog_name.to_string(),
                spec: spec.model?,
                spec_type: spec.catalog_type?,
                expect_pub_id: spec.expect_pub_id,
            })
        });

    let target = build::arg_source_to_url(&target, true)?;
    let mut sources = local_specs::surface_errors(local_specs::load(&target).await.into_result())?;

    let count = local_specs::extend_from_catalog(
        &mut sources,
        collect_specs(rows)?,
        local_specs::pick_policy(overwrite, flat),
    );
    let sources = local_specs::indirect_and_write_resources(sources)?;

    println!("Wrote {count} specifications under {target}.");
    let () = local_specs::generate_files(ctx, sources).await?;

    Ok(())
}

pub struct DraftSpecRow {
    pub catalog_name: String,
    pub spec: RawValue,
    pub spec_type: CatalogType,
    pub expect_pub_id: Option<models::Id>,
}

impl DraftSpecRow {
    fn catalog_name(&self) -> &str {
        &self.catalog_name
    }
    fn spec_type(&self) -> CatalogType {
        self.spec_type
    }
    fn spec(&self) -> Option<&RawValue> {
        Some(&self.spec)
    }
    fn expect_pub_id(&self) -> Option<models::Id> {
        self.expect_pub_id
    }
}

/// Collects an iterator of `SpecRow`s into a `tables::DraftCatalog`.
fn collect_specs(
    rows: impl IntoIterator<Item = DraftSpecRow>,
) -> anyhow::Result<tables::DraftCatalog> {
    let mut catalog = tables::DraftCatalog::default();

    fn parse<T: serde::de::DeserializeOwned>(
        model: Option<&RawValue>,
    ) -> anyhow::Result<Option<T>> {
        if let Some(model) = model {
            Ok(Some(serde_json::from_str::<T>(model.get())?))
        } else {
            Ok(None)
        }
    }

    for row in rows {
        let scope = url::Url::parse(&format!("flow://control/{}", row.catalog_name())).unwrap();

        match row.spec_type() {
            CatalogType::Capture => {
                catalog.captures.insert_row(
                    models::Capture::new(row.catalog_name()),
                    &scope,
                    row.expect_pub_id(),
                    parse::<models::CaptureDef>(row.spec())?,
                    false, // !is_touch
                );
            }
            CatalogType::Collection => {
                catalog.collections.insert_row(
                    models::Collection::new(row.catalog_name()),
                    &scope,
                    row.expect_pub_id(),
                    parse::<models::CollectionDef>(row.spec())?,
                    false, // !is_touch
                );
            }
            CatalogType::Materialization => {
                catalog.materializations.insert_row(
                    models::Materialization::new(row.catalog_name()),
                    &scope,
                    row.expect_pub_id(),
                    parse::<models::MaterializationDef>(row.spec())?,
                    false, // !is_touch
                );
            }
            CatalogType::Test => {
                catalog.tests.insert_row(
                    models::Test::new(row.catalog_name()),
                    &scope,
                    row.expect_pub_id(),
                    parse::<models::TestDef>(row.spec())?,
                    false, // !is_touch
                );
            }
        }
    }
    Ok(catalog)
}
