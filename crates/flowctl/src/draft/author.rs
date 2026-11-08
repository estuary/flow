use crate::{catalog::SpecSummaryItem, draft::encrypt, local_specs};
use anyhow::Context;
use models::CatalogType;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Author {
    /// Path or URL to a Flow specification file to author.
    #[clap(long)]
    source: String,
}

pub async fn clear_draft(ctx: &crate::CliContext, draft_id: models::Id) -> anyhow::Result<()> {
    tracing::info!(%draft_id, "clearing existing specs from draft");
    let catalog_names = super::fetch_draft_specs(ctx, draft_id, false)
        .await?
        .into_iter()
        .map(|spec| spec.catalog_name)
        .collect();

    super::unstage_draft_specs(ctx, draft_id, catalog_names)
        .await
        .context("failed to clear existing draft specs")?;
    Ok(())
}

/// Encrypts any unencrypted endpoint configurations in the draft catalog,
/// and then upserts the draft specs to the given draft ID.
pub async fn author(
    ctx: &crate::CliContext,
    draft_id: models::Id,
    draft: &mut tables::DraftCatalog,
) -> anyhow::Result<Vec<SpecSummaryItem>> {
    encrypt::encrypt_configs(draft, ctx).await?;
    upsert_draft_specs(ctx, draft_id, &*draft).await
}

pub async fn upsert_draft_specs(
    ctx: &crate::CliContext,
    draft_id: models::Id,
    draft: &tables::DraftCatalog,
) -> anyhow::Result<Vec<SpecSummaryItem>> {
    let tables::DraftCatalog {
        collections,
        captures,
        materializations,
        tests,
        ..
    } = draft;

    let mut specs = Vec::new();
    let mut summary = Vec::new();

    let mut push = |catalog_name: &str,
                    spec_type: CatalogType,
                    model: Option<models::RawValue>,
                    expect_pub_id: Option<models::Id>| {
        specs.push(super::DraftSpecInput {
            catalog_name: models::Name::new(catalog_name),
            // A drafted deletion has neither a model nor a type.
            catalog_type: model.is_some().then_some(spec_type),
            model,
            expect_pub_id,
            detail: None,
        });
        summary.push(SpecSummaryItem {
            catalog_name: catalog_name.to_string(),
            spec_type,
        });
    };

    for row in collections.iter() {
        push(
            &row.collection,
            CatalogType::Collection,
            to_raw_model(&row.model),
            row.expect_pub_id,
        );
    }
    for row in captures.iter() {
        push(
            &row.capture,
            CatalogType::Capture,
            to_raw_model(&row.model),
            row.expect_pub_id,
        );
    }
    for row in materializations.iter() {
        push(
            &row.materialization,
            CatalogType::Materialization,
            to_raw_model(&row.model),
            row.expect_pub_id,
        );
    }
    for row in tests.iter() {
        push(
            &row.test,
            CatalogType::Test,
            to_raw_model(&row.model),
            row.expect_pub_id,
        );
    }

    super::stage_draft_specs(ctx, draft_id, specs).await?;

    Ok(summary)
}

/// Serialize models directly to JSON without going through serde_json::Value
/// in order to avoid re-ordering fields which breaks sops hmac hashes.
fn to_raw_model<M: serde::Serialize>(model: &Option<M>) -> Option<models::RawValue> {
    model
        .as_ref()
        .map(|model| serde_json::value::to_raw_value(model).unwrap().into())
}

pub async fn do_author(
    ctx: &mut crate::CliContext,
    Author { source }: &Author,
) -> anyhow::Result<()> {
    let draft_id = ctx.config.selected_draft()?;
    let mut draft = local_specs::load_and_validate(ctx, &source).await?.draft;

    clear_draft(ctx, draft_id).await?;
    let rows = author(ctx, draft_id, &mut draft).await?;

    ctx.write_all(rows, ())
}
