use anyhow::Context;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Join {
    #[clap(flatten)]
    task: crate::ops::TaskSelector,
    /// Print the changes that would be applied, without applying them.
    #[clap(long)]
    dry_run: bool,
}

/// Join a task's shards pairwise, halving their number.
///
/// The inverse of `split-shards`. Each pair merges into the shard with the lower
/// range, which absorbs its partner's range and then covers both; the partner is
/// deleted along with its recovery log. The survivor's range *begin* does not move,
/// and a shard's ID derives from that, so it keeps its ID, its recovery log, and
/// its accumulated state.
pub async fn do_join(ctx: &mut crate::CliContext, args: &Join) -> anyhow::Result<()> {
    let Join { task, dry_run } = args;
    let task_name = &task.task;

    #[derive(serde::Deserialize)]
    struct Row {
        spec_type: String,
        built_spec: Option<models::RawValue>,
        data_plane_name: String,
    }
    let rows: Vec<Row> = flow_client_next::postgrest::exec(
        ctx.pg
            .from("live_specs_ext")
            .select("spec_type,built_spec,data_plane_name")
            .eq("catalog_name", task_name),
        ctx.access_token().as_deref(),
    )
    .await?;

    anyhow::ensure!(
        rows.len() <= 1,
        "found {} live specs matching {task_name}, but expected exactly one",
        rows.len(),
    );
    let Some(Row {
        spec_type,
        built_spec: Some(built_spec),
        data_plane_name,
    }) = rows.into_iter().next()
    else {
        anyhow::bail!("task {task_name} was not found, or has no built specification");
    };

    let collection_spec: proto_flow::flow::CollectionSpec;
    let materialization_spec: proto_flow::flow::MaterializationSpec;

    let (task_type, template) = match spec_type.as_str() {
        "capture" => anyhow::bail!("capture shards cannot be joined yet"),
        "collection" => {
            collection_spec = serde_json::from_str(built_spec.get())
                .context("parsing built collection specification")?;
            (
                ops::TaskType::Derivation,
                activate::collection_template(Some(&collection_spec))?.1,
            )
        }
        "materialization" => {
            materialization_spec = serde_json::from_str(built_spec.get())
                .context("parsing built materialization specification")?;
            (
                ops::TaskType::Materialization,
                activate::materialization_template(Some(&materialization_spec))?,
            )
        }
        _ => (ops::TaskType::InvalidType, None),
    };
    let template = template
        .with_context(|| format!("{task_name} is not an active derivation or materialization"))?;

    {
        // Mirrors the same check in `split-shards`: joining rewrites shard specs
        // the V1 runtime does not expect to change under it.
        // TODO(whb): This check can be removed once the runtime-v2 migration is
        // complete.
        let is_runtime_v2 = template.shard.labels.as_ref().is_some_and(|set| {
            set.labels
                .iter()
                .any(|label| label.name == labels::RUNTIME_V2_FLAG && label.value == "true")
        });
        anyhow::ensure!(
            is_runtime_v2,
            "task {task_name} is not running the V2 runtime (its shards lack the \
             `{}: true` flag) and cannot be joined",
            labels::RUNTIME_V2_FLAG,
        );
    }

    let (ops_logs_journal, ops_stats_journal, shard_client, journal_client) =
        crate::dataplane::user_task_admin(
            &ctx.rest,
            &ctx.user_tokens,
            &ctx.router,
            task_name,
            models::Name::new(data_plane_name),
        )
        .await?;

    let (shards, recovery, _ops_logs, _ops_stats) = activate::fetch_task_splits(
        &journal_client,
        &shard_client,
        task_type,
        task_name,
        None,
        None,
    )
    .await?;

    anyhow::ensure!(!shards.is_empty(), "task {task_name} has no current shards");
    anyhow::ensure!(
        shards.len() > 1,
        "task {task_name} has a single shard, so there is nothing to join",
    );

    let (desired, removals) = activate::map_shards_to_join(&shards)?;

    for split in &desired {
        let range = labels::shard::decode_range_spec(&split.labels)?;
        println!(
            "Updating shard {} to keys [{:08x}, {:08x}] and r-clocks [{:08x}, {:08x}].",
            split.id, range.key_begin, range.key_end, range.r_clock_begin, range.r_clock_end,
        );
    }
    for split in &removals {
        println!("Deleting shard {}.", split.id);
    }

    // `task_changes` emits the upserts for surviving shards, and deletes every
    // recovery log no longer paired with one — but it derives deletions from the
    // list it is given, so the removed shards themselves are appended here.
    let mut changes = activate::task_changes(
        Some(template),
        desired,
        recovery,
        (ops_logs_journal, None, Vec::new()),
        (ops_stats_journal, None, Vec::new()),
    )?;

    changes.extend(removals.into_iter().map(|split| {
        activate::Change::Shard(proto_gazette::consumer::apply_request::Change {
            expect_mod_revision: split.mod_revision,
            upsert: None,
            delete: split.id,
            primary_hints: None,
        })
    }));

    if *dry_run {
        println!("{}", serde_json::to_string_pretty(&changes)?);
        println!("Dry run: no changes were applied.");
        return Ok(());
    }

    // `apply_changes` orders these correctly on its own: surviving shards are
    // widened first, then the removed shards are deleted, and only then their
    // recovery logs — so no key range is ever unowned.
    activate::apply_changes(&journal_client, &shard_client, changes).await?;
    println!("Join applied.");

    Ok(())
}
