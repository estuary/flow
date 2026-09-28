use crate::local_specs;

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
    let mut catalog = tables::DraftCatalog::default();
    for spec in super::fetch_draft_specs(ctx, draft_id, true).await? {
        // Drafted deletions have no model to develop, and are skipped.
        let (Some(model), Some(catalog_type)) = (spec.model, spec.catalog_type) else {
            continue;
        };
        let scope = tables::synthetic_scope("control", &spec.catalog_name);
        catalog
            .add_spec(
                catalog_type,
                &spec.catalog_name,
                scope,
                spec.expect_pub_id,
                Some(&model),
                false, // !is_touch
            )
            .map_err(|err| err.error)?;
    }

    let target = build::arg_source_to_url(&target, true)?;
    let mut sources = local_specs::surface_errors(local_specs::load(&target).await.into_result())?;

    let count = local_specs::extend_from_catalog(
        &mut sources,
        catalog,
        local_specs::pick_policy(overwrite, flat),
    );
    let sources = local_specs::indirect_and_write_resources(sources)?;

    println!("Wrote {count} specifications under {target}.");
    let () = local_specs::generate_files(ctx, sources).await?;

    Ok(())
}
