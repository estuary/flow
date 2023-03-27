use crate::catalog::{collect_specs, fetch_live_specs, List, NameSelector, SpecTypeSelector};
use crate::local_specs;
use crate::CliContext;

/// Arguments for the pull-specs subcommand
#[derive(Debug, clap::Args)]
pub struct PullSpecs {
    #[clap(flatten)]
    name_selector: NameSelector,
    #[clap(flatten)]
    type_selector: SpecTypeSelector,
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

pub async fn do_pull_specs(ctx: &mut CliContext, args: &PullSpecs) -> anyhow::Result<()> {
    let client = ctx.controlplane_client().await?;
    // Retrieve identified live specifications.
    let live_specs = fetch_live_specs(
        client.clone(),
        &List {
            flows: true,
            name_selector: args.name_selector.clone(),
            type_selector: args.type_selector.clone(),
            deleted: false, // deleted specs have nothing to pull
        },
        vec![
            "catalog_name",
            "id",
            "updated_at",
            "last_pub_user_email",
            "last_pub_user_full_name",
            "last_pub_user_id",
            "spec_type",
            "spec",
        ],
    )
    .await?;
    tracing::debug!(count = live_specs.len(), "successfully fetched live specs");

    let target = local_specs::arg_source_to_url(&args.target, true)?;
    let mut sources = local_specs::surface_errors(local_specs::load(&target).await)?;

    let count = local_specs::extend_from_catalog(
        &mut sources,
        collect_specs(live_specs)?,
        local_specs::pick_policy(args.overwrite, args.flat),
    );
    let sources = local_specs::indirect_and_write_resources(sources)?;

    println!("Wrote {count} specifications under {target}.");

    // Validate to generate associated files.
    let ((sources, validations), errors) =
        local_specs::inline_and_validate(client.clone(), sources).await;
    local_specs::write_generated_files(&sources, &validations)?;

    if !errors.is_empty() {
        tracing::warn!(
            "The written Flow specifications have {} errors. Run `test` to review",
            errors.len()
        );
    }
    Ok(())
}
