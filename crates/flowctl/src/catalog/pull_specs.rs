use crate::catalog::{collect_specs, fetch_live_specs, List, NameSelector, SpecTypeSelector};
use crate::source;
use crate::CliContext;

/// Arguments for the pull-specs subcommand
#[derive(Debug, clap::Args)]
pub struct PullSpecs {
    #[clap(flatten)]
    pub prefix_selector: NameSelector,
    #[clap(flatten)]
    pub type_selector: SpecTypeSelector,

    #[clap(flatten)]
    pub output_args: source::LocalSpecsArgs,
}

pub async fn do_pull_specs(ctx: &mut CliContext, args: &PullSpecs) -> anyhow::Result<()> {
    let client = ctx.controlplane_client()?;
    let columns = vec![
        "catalog_name",
        "id",
        "updated_at",
        "last_pub_user_email",
        "last_pub_user_full_name",
        "last_pub_user_id",
        "spec_type",
        "spec",
    ];

    let list_args = List {
        flows: true,
        prefix_selector: args.prefix_selector.clone(),
        type_selector: args.type_selector.clone(),
    };

    let live_specs = fetch_live_specs(client, &list_args, columns).await?;
    tracing::debug!(count = live_specs.len(), "successfully fetched live specs");
    let catalog = collect_specs(live_specs)?;

    source::write_local_specs(catalog, &args.output_args).await
}
