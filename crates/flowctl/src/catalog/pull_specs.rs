use crate::catalog::{
    collect_specs, fetch_live_specs, List, LiveSpecRow, NameSelector, SpecTypeSelector,
};
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
    // Retrieve identified live specifications.
    let live_specs = fetch_live_specs::<LiveSpecRow>(
        &ctx.client,
        &List {
            flows: false,
            name_selector: args.name_selector.clone(),
            type_selector: args.type_selector.clone(),
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

    let lower_bound = humantime::parse_rfc3339("2025-03-25T11:59:00Z").unwrap();

    let update_source = |source: &mut models::Source| match source {
        models::Source::Source(full_source) => {
            *source = models::Source::Source(models::FullSource {
                not_before: Some(lower_bound.into()),
                ..full_source.clone()
            });
        }
        models::Source::Collection(collection) => {
            *source = models::Source::Source(models::FullSource {
                name: collection.clone(),
                not_before: Some(lower_bound.into()),
                not_after: None,
                partitions: None,
            });
        }
    };

    for collection in sources.collections.iter_mut() {
        let Some(model) = &mut collection.model else {
            continue;
        };
        let Some(derive) = &mut model.derive else {
            continue;
        };
        for transform in &mut derive.transforms {
            update_source(&mut transform.source);
        }
    }
    for materialization in sources.materializations.iter_mut() {
        let Some(model) = &mut materialization.model else {
            continue;
        };
        for binding in &mut model.bindings {
            update_source(&mut binding.source);
        }
    }

    let sources = local_specs::indirect_and_write_resources(sources)?;

    println!("Wrote {count} specifications under {target}.");
    let () = local_specs::generate_files(&ctx.client, sources).await?;

    Ok(())
}
