use crate::{draft, local_specs, CliContext};
use anyhow::Context;

#[derive(Debug, clap::Args)]
pub struct TestArgs {
    /// Path or URL to a Flow specification file to author.
    #[clap(long)]
    source: String,
    /// Data-plane into which newly initialized specifications will be placed.
    /// This data-plane must be included in the set of data-planes associated
    /// with the specification's covering prefix.
    /// If omitted, the default data-plane of the covering prefix is used.
    #[clap(long)]
    init_data_plane: Option<String>,
}

/// Test is really just a publish with the `dry-run` flag set to true, but we have a separate subcommand
/// for it because the desired UX is different and because a `test` subcommand is much more obvious
/// and discoverable to users. There's also no need for any confirmation steps, since we're not
/// actually modifying the published specs.
pub async fn do_test(ctx: &mut CliContext, args: &TestArgs) -> anyhow::Result<()> {
    let (mut draft_catalog, _validations) =
        local_specs::load_and_validate(&ctx.client, &args.source).await?;

    let draft = draft::create_draft(&ctx.client).await?;
    println!("Created draft: {}", &draft.id);
    tracing::info!(draft_id = %draft.id, "created draft");
    let spec_rows = draft::author(&ctx.client, draft.id, &mut draft_catalog).await?;
    println!("Running tests for catalog items:");
    ctx.write_all(spec_rows, ())?;
    println!("Starting tests...");

    // Technically, test is just a publish with the dry-run flag set to true.
    let publish_result =
        draft::publish(&ctx.client, args.init_data_plane.as_deref(), draft.id, true).await;

    if let Err(del_err) = draft::delete_draft(&ctx.client, draft.id).await {
        tracing::error!(draft_id = %draft.id, error = %del_err, "failed to delete draft");
    }
    publish_result.context("Tests failed")?;
    println!("Tests successful");
    Ok(())
}
