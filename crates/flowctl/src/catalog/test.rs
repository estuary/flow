use crate::{draft, source, CliContext};
use anyhow::Context;

/// Test is really just a publish with the `dry-run` flag set to true, but we have a separate subcommand
/// for it because the desired UX is different and because a `test` subcommand is much more obvious
/// and discoverable to users. There's also no need for any confirmation steps, since we're not
/// actually modifying the published specs.
pub async fn do_test(ctx: &mut CliContext, args: &source::SourceArgs) -> anyhow::Result<()> {
    let client = ctx.controlplane_client().await?;

    let catalog = crate::source::bundle(args).await?;

    let draft = draft::create_draft(client.clone()).await?;
    println!("Created draft: {}", &draft.id);
    tracing::info!(draft_id = %draft.id, "created draft");
    let spec_rows = draft::upsert_draft_specs(client.clone(), &draft.id, &catalog).await?;
    println!("Running tests for catalog items:");
    ctx.write_all(spec_rows, ())?;
    println!("Starting tests...");

    // Technically, test is just a publish with the dry-run flag set to true.
    let publish_result = draft::publish(client.clone(), true, &draft.id).await;

    if let Err(del_err) = draft::delete_draft(client.clone(), &draft.id).await {
        tracing::error!(draft_id = %draft.id, error = %del_err, "failed to delete draft");
    }
    publish_result.context("Tests failed")?;
    println!("Tests successful");
    Ok(())
}
