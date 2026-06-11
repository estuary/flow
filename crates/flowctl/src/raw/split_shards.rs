use anyhow::Context;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Split {
    #[clap(flatten)]
    task: crate::ops::TaskSelector,
    /// Split on rotated clock (derivations only).
    #[clap(long)]
    split_rclock: bool,
    /// Print the changes that would be applied, without applying them.
    #[clap(long)]
    dry_run: bool,
}

pub async fn do_split(ctx: &mut crate::CliContext, args: &Split) -> anyhow::Result<()> {
    let Split {
        task,
        split_rclock,
        dry_run,
    } = args;
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
        "capture" => anyhow::bail!("capture shards cannot be split yet"),
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

    if *split_rclock && task_type != ops::TaskType::Derivation {
        anyhow::bail!("only derivations can split on r-clock");
    }

    {
        // TODO(whb): This check can be removed once the runtime-v2 migration is
        // complete.
        let is_runtime_v2 = template.shard.labels.as_ref().is_some_and(|set| {
            set.labels.iter().any(|label| {
                label.name == "estuary.dev/flag/enable-runtime-v2" && label.value == "true"
            })
        });
        anyhow::ensure!(
            is_runtime_v2,
            "task {task_name} is not running the V2 runtime (its shards lack the \
         `estuary.dev/flag/enable-runtime-v2: true` flag) and cannot be split",
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

    // Fetch current shards and recovery logs. The task's ops journals already
    // exist, so ops journal templates aren't needed.
    let (shards, recovery, _ops_logs, _ops_stats) = activate::fetch_task_splits(
        &journal_client,
        &shard_client,
        task_type,
        task_name,
        None,
        None,
    )
    .await?;

    anyhow::ensure!(!shards.is_empty(), "task {task_name} has no current shards",);

    // Split every current shard into two. Derivation and materialization
    // children are stateless: they start empty and acquire state through the
    // task's leader protocol, so no recovery-log seeding is required.
    let mut desired = Vec::with_capacity(shards.len() * 2);

    for shard in &shards {
        let (lhs, rhs) = activate::map_shard_to_split(shard, !split_rclock)
            .with_context(|| format!("cannot split shard {}", shard.id))?;

        for (verb, split) in [("Updating", &lhs), ("Creating", &rhs)] {
            let range = labels::shard::decode_range_spec(&split.labels)?;
            println!(
                "{verb} shard {} with keys [{:08x}, {:08x}] and r-clocks [{:08x}, {:08x}].",
                split.id, range.key_begin, range.key_end, range.r_clock_begin, range.r_clock_end,
            );
        }
        desired.push(lhs);
        desired.push(rhs);
    }

    let changes = activate::task_changes(
        Some(template),
        desired,
        recovery,
        (ops_logs_journal, None, Vec::new()),
        (ops_stats_journal, None, Vec::new()),
    )?;

    if *dry_run {
        println!("{}", serde_json::to_string_pretty(&changes)?);
        println!("Dry run: no changes were applied.");
        return Ok(());
    }

    activate::apply_changes(&journal_client, &shard_client, changes).await?;
    println!("Split applied.");

    Ok(())
}
