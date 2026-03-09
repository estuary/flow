use anyhow::Context;
use proto_flow::{flow, shuffle as proto};

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Shuffle {
    /// Name of the catalog task (materialization, derivation, or collection).
    #[clap(long)]
    name: String,
    /// Port to run the shuffle server on.
    #[clap(long, default_value = "9876")]
    port: u16,
    /// Number of members in the shuffle topology.
    #[clap(long, default_value = "1")]
    members: u32,
    /// Directory for log segment files. If omitted, a temporary directory is used.
    #[clap(long)]
    directory: Option<std::path::PathBuf>,
}

impl Shuffle {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
        let Self {
            name,
            port,
            members: member_count,
            directory,
        } = self;

        // Fetch the task spec from the control plane.
        let task = fetch_task_spec(ctx, name).await?;

        // Start two shuffle servers on adjacent ports.
        // Even-indexed members use the first server, odd-indexed use the second.
        let bind_addr_even = format!("127.0.0.1:{port}");
        let bind_addr_odd = format!("127.0.0.1:{}", port + 1);
        let peer_addr_even = format!("http://{bind_addr_even}");
        let peer_addr_odd = format!("http://{bind_addr_odd}");

        let user_tokens = tokens::fixed(Ok(flow_client_next::user_auth::UserToken {
            access_token: ctx.config.user_access_token.clone(),
            refresh_token: None,
        }));

        let api_client = flow_client_next::rest::Client::new(ctx.config.get_agent_url(), "flowctl");
        let fragment_client = gazette::journal::Client::new_fragment_client();
        let router = gazette::Router::new("local");

        let auth_fn = move |collection, _task| -> gazette::journal::Client {
            let source = flow_client_next::workflows::UserCollectionAuth {
                capability: models::Capability::Read,
                collection,
                client: api_client.clone(),
                user_tokens: user_tokens.clone(),
            };
            let watch = tokens::watch(source);

            flow_client_next::workflows::user_collection_auth::new_journal_client(
                fragment_client.clone(),
                router.clone(),
                watch,
            )
        };

        let service_even =
            ::shuffle::Service::new(peer_addr_even.clone(), Box::new(auth_fn.clone()));
        let service_odd = ::shuffle::Service::new(peer_addr_odd.clone(), Box::new(auth_fn.clone()));

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
            member_count,
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

        // Create member topology: even-indexed members use even server, odd use odd.
        let members =
            build_member_topology(*member_count, &peer_addr_even, &peer_addr_odd, &log_dir_str);

        tracing::info!(
            addr_even = %peer_addr_even,
            addr_odd = %peer_addr_odd,
            member_count,
            "opening session"
        );

        let mut client = ::shuffle::SessionClient::open(
            &service_even,
            task,
            members,
            ::shuffle::Frontier::default(),
        )
        .await
        .context("opening session")?;

        tracing::info!("session opened, requesting checkpoints");

        // Create a Reader per member to consume log entries.
        let log_dir = std::path::Path::new(&log_dir_str);
        let mut readers: Vec<::shuffle::log::reader::Reader> = (0..*member_count)
            .map(|i| ::shuffle::log::reader::Reader::new(log_dir, i))
            .collect();

        let mut total_entries: usize = 0;

        for i in 0..15 {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            tracing::info!(i, "requesting NextCheckpoint");

            let frontier = client
                .next_checkpoint()
                .await
                .context("requesting next checkpoint")?;

            let total_producers: usize = frontier.journals.iter().map(|j| j.producers.len()).sum();

            tracing::info!(
                i,
                journals = frontier.journals.len(),
                total_producers,
                flushed_lsn = ?frontier.flushed_lsn,
                "received NextCheckpoint"
            );

            for ::shuffle::JournalFrontier {
                journal,
                binding,
                producers,
            } in &frontier.journals
            {
                for ::shuffle::ProducerFrontier {
                    producer,
                    last_commit,
                    hinted_commit,
                    offset,
                } in producers
                {
                    tracing::debug!(
                        journal,
                        binding,
                        ?producer,
                        ?last_commit,
                        ?hinted_commit,
                        offset,
                        "producer frontier"
                    );
                }
            }

            // Read committed entries from each member's log files.
            let mut checkpoint_entries: usize = 0;
            for (member_index, reader) in readers.iter_mut().enumerate() {
                reader
                    .read_checkpoint(&frontier, |entry| {
                        checkpoint_entries += 1;
                        total_entries += 1;

                        tracing::info!(
                            member = member_index,
                            binding = entry.binding,
                            journal = entry.journal_name,
                            ?entry.producer,
                            ?entry.clock,
                            flags = entry.flags,
                            offset = entry.offset,
                            "log entry"
                        );
                    })
                    .with_context(|| format!("reading checkpoint for member {member_index}"))?;
            }

            tracing::info!(
                i,
                checkpoint_entries,
                total_entries,
                "read log entries for checkpoint"
            );
        }

        client.close().await.context("closing session")?;

        tracing::info!(total_entries, "shuffle test completed successfully");

        server_handle_even.abort();
        server_handle_odd.abort();

        Ok(())
    }
}

async fn fetch_task_spec(ctx: &mut crate::CliContext, name: &str) -> anyhow::Result<proto::Task> {
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
            proto::Task {
                task: Some(proto::task::Task::Materialization(spec)),
            }
        }
        "collection" => {
            let spec: flow::CollectionSpec = serde_json::from_value(row.built_spec)?;

            if spec.derivation.is_some() {
                tracing::info!(name = spec.name, "fetched derivation");
                proto::Task {
                    task: Some(proto::task::Task::Derivation(spec)),
                }
            } else {
                tracing::info!(name = spec.name, "fetched collection");
                let partition_selector = Some(assemble::journal_selector(&spec, None));

                proto::Task {
                    task: Some(proto::task::Task::CollectionPartitions(
                        proto::CollectionPartitions {
                            collection: Some(spec),
                            partition_selector,
                        },
                    )),
                }
            }
        }
        other => anyhow::bail!("unsupported spec_type: {other}"),
    };

    Ok(task)
}

/// Build a member topology with `count` members, splitting the key space evenly.
/// Even-indexed members use `addr_even`, odd-indexed members use `addr_odd`.
fn build_member_topology(
    count: u32,
    addr_even: &str,
    addr_odd: &str,
    directory: &str,
) -> Vec<proto::Member> {
    let mut members = Vec::with_capacity(count as usize);

    for i in 0..count {
        // Split key space evenly across members.
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

        members.push(proto::Member {
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
        "built member topology: {:?}",
        members
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

    members
}
