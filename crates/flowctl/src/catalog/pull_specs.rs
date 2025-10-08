use crate::CliContext;
use crate::catalog::{
    DataPlaneSelector, List, LiveSpecRow, NameSelector, SpecTypeSelector, collect_specs,
    fetch_live_specs,
};
use crate::local_specs;

/// Arguments for the pull-specs subcommand
#[derive(Debug, clap::Args)]
pub struct PullSpecs {
    #[clap(flatten)]
    name_selector: NameSelector,
    #[clap(flatten)]
    type_selector: SpecTypeSelector,
    #[clap(flatten)]
    data_plane_selector: DataPlaneSelector,
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
    // Retrieve identified live specifications.
    let live_specs = fetch_live_specs::<LiveSpecRow>(
        &ctx.client,
        &List {
            flows: false,
            name_selector: args.name_selector.clone(),
            type_selector: args.type_selector.clone(),
            data_plane_selector: args.data_plane_selector.clone(),
        },
        vec![
            "catalog_name",
            "id",
            "updated_at",
            "last_pub_id",
            "last_pub_user_email",
            "last_pub_user_full_name",
            "last_pub_user_id",
            "spec_type",
            "spec",
            "data_plane_id",
        ],
    )
    .await?;
    tracing::debug!(count = live_specs.len(), "successfully fetched live specs");

    let target = build::arg_source_to_url(&args.target, true)?;
    let mut sources = local_specs::surface_errors(local_specs::load(&target).await.into_result())?;

    let count = local_specs::extend_from_catalog(
        &mut sources,
        collect_specs(live_specs)?,
        local_specs::pick_policy(args.overwrite, args.flat),
    );
    let sources = local_specs::indirect_and_write_resources(sources)?;

    println!("Wrote {count} specifications under {target}.");
    let () = local_specs::generate_files(&ctx.client, sources).await?;

    Ok(())
}
