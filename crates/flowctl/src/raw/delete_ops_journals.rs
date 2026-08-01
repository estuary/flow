use anyhow::Context;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct DeleteOpsJournals {
    #[clap(flatten)]
    task: crate::ops::TaskSelector,
    /// Print the journals that would be deleted, without deleting them.
    #[clap(long)]
    dry_run: bool,
}

/// Delete a task's ops log and stats journal partitions.
///
/// Deleting a task's specification does not remove these. They are partitions of the
/// data plane's shared `ops/tasks/.../logs` and `.../stats` collections, one pair per
/// task, and they outlive the task that produced them — so a workload that publishes
/// and deletes many tasks accumulates them without bound. That accumulation is not
/// harmless: at around fourteen hundred partitions a four-broker data plane ran out of
/// assignment slots, and newly published journals got no primary broker at all, which
/// presents as tasks that publish successfully and then never reach primary.
///
/// The task must still exist, because its ops journal names are resolved from its
/// authorization. Delete these first, then the specification — and disable the task
/// beforehand, or it will simply write its logs partition back.
pub async fn do_delete_ops_journals(
    ctx: &mut crate::CliContext,
    args: &DeleteOpsJournals,
) -> anyhow::Result<()> {
    let DeleteOpsJournals { task, dry_run } = args;
    let task_name = &task.task;

    #[derive(serde::Deserialize)]
    struct Row {
        spec_type: String,
        data_plane_name: String,
    }
    let rows: Vec<Row> = flow_client_next::postgrest::exec(
        ctx.pg
            .from("live_specs_ext")
            .select("spec_type,data_plane_name")
            .eq("catalog_name", task_name),
        ctx.access_token().as_deref(),
    )
    .await?;

    let Some(Row {
        spec_type,
        data_plane_name,
    }) = rows.into_iter().next()
    else {
        anyhow::bail!("task {task_name} was not found");
    };

    let task_type = match spec_type.as_str() {
        "capture" => ops::TaskType::Capture,
        "collection" => ops::TaskType::Derivation,
        "materialization" => ops::TaskType::Materialization,
        _ => anyhow::bail!("{task_name} is a {spec_type}, which has no ops journals"),
    };

    let (_ops_logs, _ops_stats, shard_client, journal_client) = crate::dataplane::user_task_admin(
        &ctx.rest,
        &ctx.user_tokens,
        &ctx.router,
        task_name,
        models::Name::new(data_plane_name),
    )
    .await?;

    let (_shards, _recovery, ops_logs, ops_stats) = activate::fetch_task_splits(
        &journal_client,
        &shard_client,
        task_type,
        task_name,
        None,
        None,
    )
    .await
    .context("listing the task's journals")?;

    // Only this task's own partitions: `fetch_task_splits` selects them by the task's
    // name label, so a sibling task's partitions of the same ops collection are not
    // included.
    let (_, _, logs_splits) = ops_logs;
    let (_, _, stats_splits) = ops_stats;

    let changes: Vec<activate::Change> = logs_splits
        .into_iter()
        .chain(stats_splits)
        .map(|split| {
            println!("Deleting ops journal {}.", split.name);

            activate::Change::Journal(proto_gazette::broker::apply_request::Change {
                expect_mod_revision: split.mod_revision,
                upsert: None,
                delete: split.name,
            })
        })
        .collect();

    if changes.is_empty() {
        println!("Task {task_name} has no ops journals.");
        return Ok(());
    }
    if *dry_run {
        println!("Dry run: {} journal(s) left in place.", changes.len());
        return Ok(());
    }

    let deleted = changes.len();
    activate::apply_changes(&journal_client, &shard_client, changes).await?;
    println!("Deleted {deleted} ops journal(s) of {task_name}.");

    Ok(())
}
