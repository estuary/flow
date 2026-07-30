use anyhow::Context;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Unassign {
    #[clap(flatten)]
    task: crate::ops::TaskSelector,
    /// Unassign every shard of the task, not only its failed ones.
    #[clap(long)]
    all: bool,
}

/// Unassign a task's failed shards so the allocator can schedule them again.
///
/// A shard whose processing loop fails is marked FAILED and *stays* failed: the
/// allocator will not reschedule it on its own. Recovery therefore means
/// unassigning it, which is what `activate` already does for every shard it
/// upserts — so a publication happens to clear failures as a side effect. This
/// command does only the unassigning, for when re-activating the task is not what
/// you want: recovering a crashed connector without touching its specification.
pub async fn do_unassign(ctx: &mut crate::CliContext, args: &Unassign) -> anyhow::Result<()> {
    let Unassign { task, all } = args;
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
        _ => anyhow::bail!("{task_name} is a {spec_type}, which has no shards"),
    };

    let (_ops_logs, _ops_stats, shard_client, _journal_client) = crate::dataplane::user_task_admin(
        &ctx.rest,
        &ctx.user_tokens,
        &ctx.router,
        task_name,
        models::Name::new(data_plane_name),
    )
    .await?;

    let listing = shard_client
        .list(activate::list_shards_request(task_type, task_name))
        .await
        .context("listing shards")?;

    let shards: Vec<String> = listing
        .shards
        .iter()
        .filter_map(|s| s.spec.as_ref().map(|spec| spec.id.clone()))
        .collect();

    anyhow::ensure!(!shards.is_empty(), "task {task_name} has no shards");

    let response = shard_client
        .unassign(proto_gazette::consumer::UnassignRequest {
            shards,
            only_failed: !*all,
            dry_run: false,
        })
        .await
        .context("unassigning shards")?;

    println!("Unassigned {} shard(s).", response.shards.len());
    Ok(())
}
