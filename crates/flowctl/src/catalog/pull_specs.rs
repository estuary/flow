use crate::catalog::{collect_specs, fetch_live_specs, List, NameSelector, SpecTypeSelector};
use crate::CliContext;
use crate::{source, typescript};
use anyhow::Context;

/// Arguments for the pull-specs subcommand
#[derive(Debug, clap::Args)]
pub struct PullSpecs {
    #[clap(flatten)]
    pub name_selector: NameSelector,
    #[clap(flatten)]
    pub type_selector: SpecTypeSelector,

    #[clap(flatten)]
    pub output_args: source::LocalSpecsArgs,

    /// Skip generating typescript classes for derivations.
    ///
    /// This is useful if you're authorized to access to a derivation, but
    /// lack authorization for all of its source collections. In that case,
    /// generating typescript would return an error due to being unable to
    /// fetch the source collection specs.
    #[clap(long)]
    pub no_generate_typescript: bool,
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
        name_selector: args.name_selector.clone(),
        type_selector: args.type_selector.clone(),
        deleted: false, // deleted specs have nothing to pull
    };

    let live_specs = fetch_live_specs(client, &list_args, columns).await?;
    tracing::debug!(count = live_specs.len(), "successfully fetched live specs");
    let catalog = collect_specs(live_specs)?;
    let has_any_derivations = catalog.collections.values().any(|c| c.derivation.is_some());

    source::write_local_specs(catalog, &args.output_args).await?;
    tracing::info!(%has_any_derivations, "finished writing specs");

    if has_any_derivations && !args.no_generate_typescript {
        // We intentionally don't re-use the bundled catalog during typescript generation
        // because there may be pre-existing specs in the directory, and we want typescript
        // generation to account for all of them.
        let generate = typescript::Generate {
            root_dir: args.output_args.output_dir.clone(),
            source: source::SourceArgs {
                source_dir: vec![args.output_args.output_dir.display().to_string()],
                ..Default::default()
            },
        };
        typescript::do_generate(ctx, &generate)
            .await
            .context("generating typescript")?;
    }
    Ok(())
}
