use anyhow::Context;
use doc::combine;
use proto_flow::flow;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Shuffle {
    /// Name of the catalog task (materialization, derivation, or collection).
    #[clap(long)]
    name: String,
    /// Port to run the shuffle server on.
    #[clap(long, default_value = "9876")]
    port: u16,
    /// Number of shards in the shuffle topology.
    #[clap(long, default_value = "1")]
    shards: u32,
    /// Directory for log segment files. If omitted, a temporary directory is used.
    #[clap(long)]
    directory: Option<std::path::PathBuf>,
    /// Minimum interval between transaction checkpoints, in milliseconds.
    #[clap(long, default_value = "500")]
    interval: u64,
    /// Number of checkpoints to process before exiting.
    #[clap(long, default_value = "20")]
    checkpoints: usize,
    /// Disk backlog threshold in Mebibytes before engaging back-pressure.
    #[clap(long, default_value = "1024")]
    disk_backlog_mib: u64,
}

impl Shuffle {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
        let Self {
            name,
            port,
            shards: shard_count,
            directory,
            interval,
            checkpoints,
            disk_backlog_mib,
        } = self;

        // Fetch the task spec from the control plane.
        let task = fetch_task_spec(ctx, name, *disk_backlog_mib * (1024 * 1024)).await?;

        // Start two shuffle servers on adjacent ports.
        // Even-indexed shards use the first server, odd-indexed use the second.
        let bind_addr_even = format!("127.0.0.1:{port}");
        let bind_addr_odd = format!("127.0.0.1:{}", port + 1);
        let peer_addr_even = format!("http://{bind_addr_even}");
        let peer_addr_odd = format!("http://{bind_addr_odd}");

        // TODO(johnny): handle refresh rotation.
        let user_tokens = tokens::fixed(Ok(flow_client_next::user_auth::UserToken {
            access_token: ctx.config.user_access_token.clone(),
            refresh_token: None,
        }));

        let factory = flow_client_next::workflows::user_collection_auth::new_journal_client_factory(
            flow_client_next::rest::Client::new(ctx.config.get_agent_url(), "flowctl"),
            models::Capability::Read,
            gazette::Router::new("local"),
            user_tokens,
        );
        let service_even = shuffle::Service::new(peer_addr_even.clone(), factory.clone());
        let service_odd = shuffle::Service::new(peer_addr_odd.clone(), factory.clone());

        let server_even = service_even.clone().build_tonic_server();
        let server_odd = service_odd.build_tonic_server();

        let listener_even = tokio::net::TcpListener::bind(&bind_addr_even)
            .await
            .with_context(|| format!("binding to {bind_addr_even}"))?;
        let listener_odd = tokio::net::TcpListener::bind(&bind_addr_odd)
            .await
            .with_context(|| format!("binding to {bind_addr_odd}"))?;

        tracing::info!(
            bind_addr_even,
            bind_addr_odd,
            shard_count,
            "starting shuffle servers"
        );

        let server_handle_even = tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener_even);
            server_even
                .serve_with_incoming(incoming)
                .await
                .context("shuffle server (even) error")
        });

        let server_handle_odd = tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener_odd);
            server_odd
                .serve_with_incoming(incoming)
                .await
                .context("shuffle server (odd) error")
        });

        // Use the provided directory or create a temporary one for log segment files.
        let _tmp_dir; // Hold the TempDir so it isn't dropped until the session ends.
        let log_dir_str = if let Some(dir) = directory {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("creating log directory {dir:?}"))?;
            _tmp_dir = None;
            dir.to_string_lossy().into_owned()
        } else {
            let td = tempfile::tempdir().context("creating temp directory for log segments")?;
            let s = td.path().to_string_lossy().into_owned();
            _tmp_dir = Some(td);
            s
        };

        tracing::info!(log_dir = %log_dir_str, "using log directory");
        let combine_spec = build_combine_spec(&task).context("building combine spec")?;

        // Create shard topology: even-indexed shards use even server, odd use odd.
        let shards =
            build_shard_topology(*shard_count, &peer_addr_even, &peer_addr_odd, &log_dir_str);

        tracing::info!(
            addr_even = %peer_addr_even,
            addr_odd = %peer_addr_odd,
            shard_count,
            "opening session"
        );

        let mut client =
            shuffle::SessionClient::open(&service_even, task, shards, shuffle::Frontier::default())
                .await
                .context("opening session")?;

        tracing::info!("session opened, requesting checkpoints");

        // Per-shard reader state, taken during each scan and returned after.
        let log_dir = std::path::Path::new(&log_dir_str);
        let mut shard_state: Vec<Option<ShardState>> = (0..*shard_count)
            .map(|i| {
                Some((
                    shuffle::log::reader::Reader::new(log_dir, i),
                    std::collections::VecDeque::new(),
                ))
            })
            .collect();

        let mut accumulator = combine::Accumulator::new(
            combine_spec,
            tempfile::tempfile().context("opening combiner spill file")?,
        )?;

        let mut total_read_docs: usize = 0;
        let mut total_read_bytes: u64 = 0;
        let mut bytes_behind: i64 = 0;

        let interval = std::time::Duration::from_millis(*interval);
        let start = std::time::Instant::now();
        let mut next_txn_time = start + interval;

        for i in 0..*checkpoints {
            // Run no more frequently than once every interval.
            tokio::time::sleep(next_txn_time.saturating_duration_since(std::time::Instant::now()))
                .await;
            next_txn_time = std::time::Instant::now() + interval;

            tracing::debug!(i, "requesting NextCheckpoint");

            let mut frontier = client
                .next_checkpoint()
                .await
                .context("requesting next checkpoint")?;

            while frontier.unresolved_hints != 0 {
                // Reducing intermediate peeks is unnecessary here because the
                // eventual ready frontier is a full restatement.
                frontier = client
                    .next_checkpoint()
                    .await
                    .context("requesting next checkpoint follow-up")?;
            }

            // Scan committed entries from each shard's log,
            // pushing documents into the combiner.
            let scan_frontier = frontier.clone();
            let (next_shard_state, next_accumulator, read_docs, read_bytes) =
                tokio::task::spawn_blocking(move || {
                    scan_frontier_shards(scan_frontier, shard_state, accumulator)
                })
                .await
                .context("joining scan task")??;

            shard_state = next_shard_state;
            accumulator = next_accumulator;

            bytes_behind += frontier
                .journals
                .iter()
                .map(|jf| jf.bytes_behind_delta)
                .sum::<i64>();

            total_read_docs += read_docs;
            total_read_bytes += read_bytes;

            let (next_accumulator, combined_docs, combined_bytes) =
                tokio::task::spawn_blocking(move || drain_accumulator_to_stdout(accumulator))
                    .await
                    .context("joining drain task")??;

            accumulator = next_accumulator;

            tracing::info!(
                i,
                journals = frontier.journals.len(),
                total_producers = frontier
                    .journals
                    .iter()
                    .map(|j| j.producers.len())
                    .sum::<usize>(),
                flushed_lsn = ?frontier.flushed_lsn,
                read_docs,
                read_mib = read_bytes / (1024 * 1024),
                combined_docs,
                combined_mib = combined_bytes / (1024 * 1024),
                mib_behind = bytes_behind / (1024 * 1024),
                "scanned and combined checkpoint"
            );
        }
        let elapsed = start.elapsed();

        client.close().await.context("closing session")?;

        tracing::info!(
            total_read_docs,
            total_read_mib = total_read_bytes / (1024 * 1024),
            avg_doc_rate = total_read_docs as f64 / elapsed.as_secs_f64(),
            avg_mib_rate = total_read_bytes as f64 / (elapsed.as_secs_f64() * 1024f64 * 1024f64),
            mib_behind = bytes_behind / (1024 * 1024),
            "shuffle test completed successfully"
        );

        server_handle_even.abort();
        server_handle_odd.abort();

        Ok(())
    }
}

type ShardState = (
    shuffle::log::reader::Reader,
    std::collections::VecDeque<shuffle::log::reader::Remainder>,
);

fn scan_frontier_shards(
    frontier: shuffle::Frontier,
    mut shard_state: Vec<Option<ShardState>>,
    mut accumulator: combine::Accumulator,
) -> anyhow::Result<(Vec<Option<ShardState>>, combine::Accumulator, usize, u64)> {
    let mut read_docs: usize = 0;
    let mut read_bytes: u64 = 0;

    for (shard_index, state_slot) in shard_state.iter_mut().enumerate() {
        let (reader, remainders) = state_slot.take().expect("shard state must be present");

        let mut scan =
            shuffle::log::reader::FrontierScan::new(frontier.clone(), reader, remainders)
                .with_context(|| format!("creating FrontierScan for shard {shard_index}"))?;

        while scan
            .advance_block()
            .with_context(|| format!("advancing block for shard {shard_index}"))?
        {
            let memtable = accumulator.memtable()?;

            for entry in scan.block_iter() {
                memtable.add_embedded(
                    entry.meta.binding.to_native(),
                    &entry.doc.packed_key_prefix,
                    entry.doc.doc.to_heap(memtable.alloc()),
                    false,
                    entry.meta.flags & shuffle::FLAGS_SCHEMA_VALID != 0,
                )?;
                read_docs += 1;
                read_bytes += entry.doc.source_byte_length.to_native() as u64;
            }
        }

        let (_, reader, remainders) = scan.into_parts();
        *state_slot = Some((reader, remainders));
    }

    Ok((shard_state, accumulator, read_docs, read_bytes))
}

fn drain_accumulator_to_stdout(
    accumulator: combine::Accumulator,
) -> anyhow::Result<(combine::Accumulator, usize, usize)> {
    let ser_policy = doc::SerPolicy::noop();
    let mut stdout = std::io::stdout();
    let mut combined_docs: usize = 0;
    let mut combined_bytes: usize = 0;
    let mut buf = Vec::<u8>::new();

    let mut drainer = accumulator.into_drainer()?;
    while let Some(drained) = drainer.next() {
        let drained = drained.context("draining combined document")?;
        serde_json::to_writer(&mut buf, &ser_policy.on_owned(&drained.root))
            .context("writing NDJSON")?;
        buf.push(b'\n');
        combined_docs += 1;
        combined_bytes += buf.len();

        std::io::Write::write_all(&mut stdout, &buf).context("flushing NDJSON to stdout")?;
        buf.clear();
    }

    let accumulator = drainer
        .into_new_accumulator()
        .context("recycling accumulator")?;

    Ok((accumulator, combined_docs, combined_bytes))
}

async fn fetch_task_spec(
    ctx: &mut crate::CliContext,
    name: &str,
    disk_backlog_threshold: u64,
) -> anyhow::Result<shuffle::proto::Task> {
    let builder = ctx
        .client
        .from("live_specs")
        .select("spec_type,built_spec")
        .eq("catalog_name", name);

    #[derive(serde::Deserialize)]
    struct Row {
        spec_type: String,
        built_spec: serde_json::Value,
    }

    let rows: Vec<Row> = crate::api_exec(builder).await?;

    let row = rows
        .into_iter()
        .next()
        .with_context(|| format!("task '{name}' not found"))?;

    let task = match row.spec_type.as_str() {
        "materialization" => {
            let spec: flow::MaterializationSpec = serde_json::from_value(row.built_spec)?;
            tracing::info!(name = spec.name, "fetched materialization");
            shuffle::proto::Task {
                task: Some(shuffle::proto::task::Task::Materialization(spec)),
            }
        }
        "collection" => {
            let spec: flow::CollectionSpec = serde_json::from_value(row.built_spec)?;

            if spec.derivation.is_some() {
                tracing::info!(name = spec.name, "fetched derivation");
                shuffle::proto::Task {
                    task: Some(shuffle::proto::task::Task::Derivation(spec)),
                }
            } else {
                tracing::info!(name = spec.name, "fetched collection");
                let partition_selector = Some(assemble::journal_selector(&spec, None));

                shuffle::proto::Task {
                    task: Some(shuffle::proto::task::Task::CollectionPartitions(
                        shuffle::proto::CollectionPartitions {
                            collection: Some(spec),
                            partition_selector,
                            disk_backlog_threshold,
                        },
                    )),
                }
            }
        }
        other => anyhow::bail!("unsupported spec_type: {other}"),
    };

    Ok(task)
}

/// Build a `combine::Spec` from the task's collection specs.
///
/// Each binding corresponds to a source collection, and uses the collection's
/// own key extractors for reduction (not the shuffle key, which may differ
/// for derivation transforms with explicit shuffle keys).
fn build_combine_spec(task: &shuffle::proto::Task) -> anyhow::Result<combine::Spec> {
    let collection_specs: Vec<&flow::CollectionSpec> = match &task.task {
        Some(shuffle::proto::task::Task::CollectionPartitions(cp)) => {
            vec![cp.collection.as_ref().context("missing collection spec")?]
        }
        Some(shuffle::proto::task::Task::Materialization(mat)) => mat
            .bindings
            .iter()
            .map(|b| b.collection.as_ref().context("missing collection spec"))
            .collect::<anyhow::Result<_>>()?,
        Some(shuffle::proto::task::Task::Derivation(col)) => {
            let derivation = col.derivation.as_ref().context("missing derivation")?;
            derivation
                .transforms
                .iter()
                .map(|t| t.collection.as_ref().context("missing collection spec"))
                .collect::<anyhow::Result<_>>()?
        }
        None => anyhow::bail!("missing task variant"),
    };

    let bindings = collection_specs.into_iter().map(|spec| {
        let (validator, shape) =
            shuffle::binding::build_schema(&spec.read_schema_json, &spec.write_schema_json)?;
        let key_extractors = shuffle::binding::build_key_extractors(&spec.key, &shape);

        Ok::<_, anyhow::Error>((true, key_extractors, spec.name.clone(), validator))
    });

    // Collect to surface errors before passing to with_bindings.
    let bindings: Vec<_> = bindings.collect::<anyhow::Result<_>>()?;

    Ok(combine::Spec::with_bindings(bindings, Vec::new()))
}

/// Build a shard topology with `count` shards, splitting the key space evenly.
/// Even-indexed shards use `addr_even`, odd-indexed shards use `addr_odd`.
fn build_shard_topology(
    count: u32,
    addr_even: &str,
    addr_odd: &str,
    directory: &str,
) -> Vec<shuffle::proto::Shard> {
    let mut shards = Vec::with_capacity(count as usize);

    for i in 0..count {
        // Split key space evenly across shards.
        let key_begin = if i == 0 {
            0
        } else {
            ((i as u64 * (u32::MAX as u64 + 1)) / count as u64) as u32
        };
        let key_end = if i == count - 1 {
            u32::MAX
        } else {
            (((i + 1) as u64 * (u32::MAX as u64 + 1)) / count as u64 - 1) as u32
        };

        let endpoint = if i % 2 == 0 { addr_even } else { addr_odd };

        shards.push(shuffle::proto::Shard {
            range: Some(flow::RangeSpec {
                key_begin,
                key_end,
                r_clock_begin: 0,
                r_clock_end: u32::MAX,
            }),
            endpoint: endpoint.to_string(),
            directory: directory.to_string(),
        });
    }

    tracing::info!(
        count,
        "built shard topology: {:?}",
        shards
            .iter()
            .enumerate()
            .map(|(i, m)| {
                let r = m.range.as_ref().unwrap();
                format!(
                    "{}:[{:#x}, {:#x}]@{}",
                    i,
                    r.key_begin,
                    r.key_end,
                    if i % 2 == 0 { "even" } else { "odd" }
                )
            })
            .collect::<Vec<_>>()
    );

    shards
}
