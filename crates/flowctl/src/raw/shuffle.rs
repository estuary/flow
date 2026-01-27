use anyhow::Context;
use futures::{SinkExt, StreamExt, TryStreamExt};
use proto_flow::shuffle::{
    JournalProducerChunk, Member, SessionRequest, Task, session_request, task,
};
use proto_grpc::shuffle::shuffle_client::ShuffleClient;

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
}

impl Shuffle {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
        let Self {
            name,
            port,
            members: member_count,
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

        let service_even = shuffle::Service::new(peer_addr_even.clone(), Box::new(auth_fn.clone()));
        let service_odd = shuffle::Service::new(peer_addr_odd.clone(), Box::new(auth_fn.clone()));

        let server_even = service_even.build_tonic_server();
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

        // Give the servers a moment to start.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Connect as a client to the even server and open a Session.
        tracing::info!(
            addr_even = %peer_addr_even,
            addr_odd = %peer_addr_odd,
            member_count,
            "connecting to shuffle servers"
        );

        let mut client = ShuffleClient::connect(peer_addr_even.clone())
            .await
            .context("connecting to shuffle server")?;

        // Create member topology: even-indexed members use even server, odd use odd.
        let members = build_member_topology(*member_count, &peer_addr_even, &peer_addr_odd);

        // Generate a unique session ID.
        let session_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        // Create the request stream.
        let (mut request_tx, request_rx) = futures::channel::mpsc::channel::<SessionRequest>(16);

        // Send the Open request.
        tracing::info!(session_id, "sending Session Open request");

        request_tx
            .send(SessionRequest {
                open: Some(session_request::Open {
                    session_id,
                    task: Some(task),
                    members,
                }),
                ..Default::default()
            })
            .await
            .context("sending Session Open")?;

        // Start the Session RPC.
        let mut response_stream = client
            .session(request_rx)
            .await
            .context("starting Session RPC")?
            .into_inner();

        // Wait for Opened response.
        let opened = response_stream
            .try_next()
            .await
            .context("waiting for Session Opened")?
            .context("Session closed without Opened")?;

        anyhow::ensure!(
            opened.opened.is_some(),
            "expected Opened response from Session"
        );

        tracing::info!(session_id, "received Opened from Session");

        // Send last-commit checkpoint (empty).
        request_tx
            .send(SessionRequest {
                last_commit_chunk: Some(JournalProducerChunk { chunk: Vec::new() }),
                ..Default::default()
            })
            .await
            .context("sending last-commit checkpoint")?;

        // Send read-through checkpoint (empty).
        request_tx
            .send(SessionRequest {
                read_through_chunk: Some(JournalProducerChunk { chunk: Vec::new() }),
                ..Default::default()
            })
            .await
            .context("sending read-through checkpoint")?;

        // Send a NextCheckpoint request.
        /*
        tracing::info!(session_id, "sending NextCheckpoint request");

        request_tx
            .send(SessionRequest {
                open: None,
                next_checkpoint: Some(session_request::NextCheckpoint {}),
            })
            .await
            .context("sending NextCheckpoint")?;

        // Wait for NextCheckpoint response.
        let checkpoint = response_stream
            .try_next()
            .await
            .context("waiting for NextCheckpoint")?
            .context("Session closed without NextCheckpoint")?;

        if let Some(next_cp) = checkpoint.next_checkpoint {
            tracing::info!(
                delta_count = next_cp.delta_checkpoint.len(),
                "received NextCheckpoint"
            );
        }
        */

        match tokio::time::timeout(std::time::Duration::from_secs(60), response_stream.next()).await
        {
            Err(_elapsed) => (),
            Ok(None) => {
                anyhow::bail!("Session response stream closed unexpectedly while request_tx held")
            }
            Ok(Some(Ok(unexpected))) => {
                anyhow::bail!("unexpected Session response received: {unexpected:?}")
            }
            Ok(Some(Err(status))) => {
                return Err(runtime::status_to_anyhow(status));
            }
        }

        // Close the request stream.
        drop(request_tx);
        let _ignored = response_stream.next().await;

        tracing::info!(session_id, "shuffle test completed successfully");

        // Abort both servers (we're done testing).
        server_handle_even.abort();
        server_handle_odd.abort();

        Ok(())
    }
}

async fn fetch_task_spec(ctx: &mut crate::CliContext, name: &str) -> anyhow::Result<Task> {
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
            let spec: proto_flow::flow::MaterializationSpec =
                serde_json::from_value(row.built_spec)?;
            tracing::info!(name = spec.name, "fetched materialization");
            Task {
                task: Some(task::Task::Materialization(spec)),
            }
        }
        "collection" => {
            let spec: proto_flow::flow::CollectionSpec = serde_json::from_value(row.built_spec)?;

            if spec.derivation.is_some() {
                tracing::info!(name = spec.name, "fetched derivation");
                Task {
                    task: Some(task::Task::Derivation(spec)),
                }
            } else {
                tracing::info!(name = spec.name, "fetched collection");
                let partition_selector = Some(assemble::journal_selector(&spec, None));

                Task {
                    task: Some(task::Task::CollectionPartitions(
                        proto_flow::shuffle::CollectionPartitions {
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
fn build_member_topology(count: u32, addr_even: &str, addr_odd: &str) -> Vec<Member> {
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

        members.push(Member {
            range: Some(proto_flow::flow::RangeSpec {
                key_begin,
                key_end,
                r_clock_begin: 0,
                r_clock_end: u32::MAX,
            }),
            endpoint: endpoint.to_string(),
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
