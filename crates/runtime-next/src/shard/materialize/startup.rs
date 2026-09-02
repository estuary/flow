use super::Task;
use crate::Logger as _;
use crate::proto;
use anyhow::Context;
use futures::StreamExt;
use prost::Message;
use proto_flow::{flow, materialize};
use tokio::sync::mpsc;

pub async fn dial_and_join(
    join: proto::Join,
    signer: Option<&proto_grpc::Signer>,
) -> anyhow::Result<(
    proto::Joined,
    Option<(
        mpsc::UnboundedSender<proto::Materialize>,
        tonic::Streaming<proto::Materialize>,
    )>,
)> {
    let leader_endpoint = join.leader_endpoint.clone();

    let channel = proto_grpc::dial_channel(&leader_endpoint).context("failed to dial leader")?;
    let shard_id = &join.shards[join.shard_index as usize].id;
    let metadata = crate::shard::leader_bearer(signer, shard_id)?;
    let mut leader_client =
        proto_grpc::runtime::leader_client::LeaderClient::with_interceptor(channel, metadata)
            .max_decoding_message_size(proto_grpc::MAX_MESSAGE_SIZE)
            .max_encoding_message_size(usize::MAX);

    // Start the materialize RPC. We use an unbounded sender because we never
    // pump messages to the leader (strictly request / response).
    let (leader_tx, leader_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut leader_rx = leader_client
        .materialize(tokio_stream::wrappers::UnboundedReceiverStream::new(
            leader_rx,
        ))
        .await
        .context("opening leader Materialize stream")?
        .into_inner();

    // Send L:Join, and read L:Joined.
    let verify = crate::verify("Materialize", "Joined", "leader");

    _ = leader_tx.send(proto::Materialize {
        join: Some(join),
        ..Default::default()
    });
    let joined = match verify.not_eof(leader_rx.next().await)? {
        proto::Materialize {
            joined: Some(joined),
            ..
        } => joined,
        response => return Err(verify.fail_msg(response)),
    };

    // Did leader signal that we need to retry?
    if joined.max_etcd_revision != 0 {
        // If leader signaled retry, expect it next sends EOF.
        () = verify.eof(leader_rx.next().await)?;

        Ok((joined, None))
    } else {
        Ok((joined, Some((leader_tx, leader_rx))))
    }
}

pub(super) struct Startup {
    pub accumulator: crate::Accumulator,
    pub connector_rx: futures::stream::BoxStream<'static, tonic::Result<materialize::Response>>,
    pub connector_tx: mpsc::Sender<materialize::Request>,
    pub db: crate::shard::RocksDB,
    pub disable_load_optimization: bool,
    pub codec: connector_init::Codec,
    pub leader_rx: tonic::Streaming<proto::Materialize>,
    pub leader_tx: mpsc::UnboundedSender<proto::Materialize>,
    pub max_keys: Vec<(bytes::Bytes, bytes::Bytes)>,
    pub shuffle_reader: shuffle::log::Reader,
    pub task: Task,
    pub token_restart_at: Option<std::time::SystemTime>,
}

pub(super) async fn run<R, P: crate::PublisherFactory, L: crate::LoggerFactory>(
    controller_rx: &mut R,
    controller_tx: &mpsc::UnboundedSender<tonic::Result<proto::Materialize>>,
    db: crate::shard::RocksDB,
    labeling: ops::proto::ShardLabeling,
    leader_producer: proto_gazette::uuid::Producer,
    mut leader_rx: tonic::Streaming<proto::Materialize>,
    leader_tx: mpsc::UnboundedSender<proto::Materialize>,
    log_level: ops::LogLevel,
    logger: &L::Logger,
    service: &crate::shard::Service<P, L>,
    shard_index: u32,
    shuffle_directory: String,
) -> anyhow::Result<Startup>
where
    R: futures::Stream<Item = tonic::Result<proto::Materialize>> + Send + Unpin + 'static,
{
    // Receive L:Task from controller.
    let verify = crate::verify("Materialize", "Open", "controller");
    let l_task = match verify.not_eof(controller_rx.next().await)? {
        proto::Materialize {
            task: Some(task), ..
        } => task,
        other => return Err(verify.fail_msg(other)),
    };
    // Shard zero (only) forwards L:Task to the leader, stamping in the
    // SessionLoop-stable producer the leader's Publisher should use.
    if shard_index == 0 {
        _ = leader_tx.send(proto::Materialize {
            task: Some(proto::Task {
                publisher_id: bytes::Bytes::copy_from_slice(leader_producer.as_bytes()),
                ..l_task.clone()
            }),
            ..Default::default()
        });
    }

    let proto::Task {
        max_transactions: _,
        spec: spec_bytes,
        sqlite_vfs_uri: _,
        publisher_id: _,
    } = l_task;

    let spec = flow::MaterializationSpec::decode(spec_bytes.as_ref())
        .context("invalid Task materialization")?;
    let task = Task::new(&spec, &labeling).context("building task definition")?;

    // Scan and send L:Recover state from RocksDB. Persisted state is
    // per-binding, keyed by each binding's `state_key`.
    let (mut db, mut recover) = db
        .scan(task.binding_state_keys.iter())
        .await
        .context("scanning RocksDB")?;
    if shard_index == 0 {
        db = db.seed_connector_state(&mut recover).await?;
    }

    _ = leader_tx.send(proto::Materialize {
        recover: Some(recover),
        ..Default::default()
    });

    // Read and execute L:Apply and L:Persist from the leader until L:Open.
    // The leader re-sends L:Apply once per Apply iteration, so latch the
    // spec-update event to report it at most once per session.
    let mut reported_spec_update = false;
    let open = loop {
        let verify = crate::verify("Materialize", "Apply, Persist, or Open", "leader");
        match verify.not_eof(leader_rx.next().await)? {
            proto::Materialize {
                apply:
                    Some(proto::Apply {
                        connector_state_json,
                        last_spec,
                        last_version,
                        spec,
                        version,
                    }),
                ..
            } => {
                if !reported_spec_update
                    && let Some(event) = crate::LogEvent::spec_update(&last_version, &version)
                {
                    reported_spec_update = true;
                    logger.event(event);
                }

                let last_spec = if last_spec.is_empty() {
                    None
                } else {
                    Some(
                        flow::MaterializationSpec::decode(last_spec.as_ref())
                            .context("invalid last Apply spec")?,
                    )
                };
                let spec = flow::MaterializationSpec::decode(spec.as_ref())
                    .context("invalid current Apply spec")?;

                let apply = materialize::request::Apply {
                    materialization: Some(spec),
                    last_materialization: last_spec,
                    last_version,
                    state_json: connector_state_json,
                    version,
                };
                _ = leader_tx.send(super::handler::serve_apply(service, apply, log_level).await?);
            }
            proto::Materialize {
                persist: Some(persist),
                ..
            } => {
                db = db
                    .persist(&persist, &task.binding_state_keys)
                    .await
                    .context("Persist failed")?;

                _ = leader_tx.send(proto::Materialize {
                    persisted: Some(proto::Persisted {
                        seq_no: persist.seq_no,
                    }),
                    ..Default::default()
                });
            }
            proto::Materialize {
                open: Some(open), ..
            } => break open,

            other => return Err(verify.fail_msg(other)),
        }
    };

    // Start the connector and send C:Open.
    let proto::Open {
        connector_state_json,
        max_keys,
        range,
        spec,
        version,
    } = open;

    let spec =
        flow::MaterializationSpec::decode(spec.as_ref()).context("invalid current Apply spec")?;

    let initial = materialize::Request {
        open: Some(materialize::request::Open {
            materialization: Some(spec),
            version,
            state_json: connector_state_json,
            range,
            // Populated by `connector::start` with the matched endpoint's inner
            // sealed configuration, which is not yet extracted from `spec` here.
            sealed_config_json: Default::default(),
        }),
        ..Default::default()
    };
    let (connector_tx, mut connector_rx, container, codec, token_restart_at) =
        super::connector::start(service, logger, log_level, initial).await?;

    // Read C:Opened from the connector.
    let verify = crate::verify("Materialize", "Opened", "connector");
    let opened = match verify.not_eof(connector_rx.next().await)? {
        materialize::Response {
            opened: Some(opened),
            ..
        } => opened,
        other => return Err(verify.fail_msg(other)),
    };
    let materialize::response::Opened {
        disable_load_optimization,
        runtime_checkpoint,
    } = opened;

    // Tell Leader and Controller of C:Opened.
    _ = leader_tx.send(proto::Materialize {
        opened: Some(proto::materialize::Opened {
            container: None,
            connector_checkpoint: runtime_checkpoint,
        }),
        ..Default::default()
    });
    _ = controller_tx.send(Ok(proto::Materialize {
        opened: Some(proto::materialize::Opened {
            container,
            connector_checkpoint: None,
        }),
        ..Default::default()
    }));

    let shuffle_reader =
        shuffle::log::Reader::new(std::path::Path::new(&shuffle_directory), shard_index);

    let accumulator =
        crate::Accumulator::new(task.combine_spec()?).context("building materialize combiner")?;

    // Densify the leader's sparse `max_keys` map into a per-binding Vec.
    let max_keys: Vec<(bytes::Bytes, bytes::Bytes)> = (0..task.bindings.len() as u32)
        .map(|i| {
            (
                max_keys.get(&i).cloned().unwrap_or_default(), // Previous max.
                bytes::Bytes::new(),                           // Next max.
            )
        })
        .collect();

    Ok(Startup {
        accumulator,
        connector_rx,
        connector_tx,
        db,
        disable_load_optimization,
        codec,
        leader_rx,
        leader_tx,
        max_keys,
        shuffle_reader,
        task,
        token_restart_at,
    })
}
