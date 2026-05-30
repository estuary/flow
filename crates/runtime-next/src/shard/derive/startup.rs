use super::Task;
use crate::proto;
use anyhow::Context;
use futures::{StreamExt, stream::BoxStream};
use prost::Message;
use proto_flow::{
    derive, flow,
    runtime::{DeriveRequestExt, derive_request_ext},
};
use tokio::sync::mpsc;

pub async fn dial_and_join(
    join: proto::Join,
    signer: Option<&proto_grpc::Signer>,
) -> anyhow::Result<(
    proto::Joined,
    Option<(
        mpsc::UnboundedSender<proto::Derive>,
        tonic::Streaming<proto::Derive>,
    )>,
)> {
    let leader_endpoint = join.leader_endpoint.clone();

    let channel = gazette::dial_channel(&leader_endpoint).context("failed to dial leader")?;
    let shard_id = &join.shards[join.shard_index as usize].id;
    let metadata = crate::shard::leader_bearer(signer, shard_id)?;
    let mut leader_client =
        proto_grpc::runtime::leader_client::LeaderClient::with_interceptor(channel, metadata)
            .max_decoding_message_size(crate::MAX_MESSAGE_SIZE)
            .max_encoding_message_size(usize::MAX);

    // Unbounded: we never pump messages to the leader (strictly request / response).
    let (leader_tx, leader_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut leader_rx = leader_client
        .derive(tokio_stream::wrappers::UnboundedReceiverStream::new(
            leader_rx,
        ))
        .await
        .context("opening leader Derive stream")?
        .into_inner();

    let verify = crate::verify("Derive", "Joined", "leader");

    _ = leader_tx.send(proto::Derive {
        join: Some(join),
        ..Default::default()
    });
    let joined = match verify.not_eof(leader_rx.next().await)? {
        proto::Derive {
            joined: Some(joined),
            ..
        } => joined,
        response => return Err(verify.fail_msg(response)),
    };

    if joined.max_etcd_revision != 0 {
        () = verify.eof(leader_rx.next().await)?;
        Ok((joined, None))
    } else {
        Ok((joined, Some((leader_tx, leader_rx))))
    }
}

pub(super) struct Startup {
    pub accumulator: crate::Accumulator,
    pub codec: connector_init::Codec,
    pub connector_rx: BoxStream<'static, tonic::Result<derive::Response>>,
    pub connector_tx: mpsc::Sender<derive::Request>,
    pub db: crate::shard::RocksDB,
    pub leader_rx: tonic::Streaming<proto::Derive>,
    pub leader_tx: mpsc::UnboundedSender<proto::Derive>,
    pub publisher: crate::Publisher,
    pub shuffle_reader: shuffle::log::Reader,
    pub task: Task,
    pub write_shape: doc::Shape,
}

pub(super) async fn run<R, L: crate::LogHandler>(
    controller_rx: &mut R,
    controller_tx: &mpsc::UnboundedSender<tonic::Result<proto::Derive>>,
    db: crate::shard::RocksDB,
    labeling: ops::proto::ShardLabeling,
    leader_producer: proto_gazette::uuid::Producer,
    mut leader_rx: tonic::Streaming<proto::Derive>,
    leader_tx: mpsc::UnboundedSender<proto::Derive>,
    log_level: ops::LogLevel,
    service: &crate::shard::Service<L>,
    shard_id: String,
    shard_index: u32,
    shard_producer: proto_gazette::uuid::Producer,
    shuffle_directory: String,
) -> anyhow::Result<Startup>
where
    R: futures::Stream<Item = tonic::Result<proto::Derive>> + Send + Unpin + 'static,
{
    // Receive L:Task from controller.
    let verify = crate::verify("Derive", "Task", "controller");
    let l_task = match verify.not_eof(controller_rx.next().await)? {
        proto::Derive {
            task: Some(task), ..
        } => task,
        other => return Err(verify.fail_msg(other)),
    };
    // Shard zero (only) forwards L:Task to the leader.
    if shard_index == 0 {
        _ = leader_tx.send(proto::Derive {
            task: Some(proto::Task {
                publisher_id: bytes::Bytes::copy_from_slice(leader_producer.as_bytes()),
                ..l_task.clone()
            }),
            ..Default::default()
        });
    }

    let proto::Task {
        max_transactions: _,
        preview,
        spec: spec_bytes,
        sqlite_vfs_uri,
        publisher_id: _, // Consumed above; the leader, not this shard, uses it.
    } = l_task;

    let spec =
        flow::CollectionSpec::decode(spec_bytes.as_ref()).context("invalid Task derivation")?;
    let task = Task::new(&spec).context("building task definition")?;
    let write_shape = task.write_shape.clone();

    // The derived collection is the single additional publisher binding;
    // publisher binding zero is the fixed ops-stats journal.
    let publisher = if preview {
        crate::Publisher::new_preview([&spec])
    } else {
        crate::Publisher::new_real(
            shard_id, // Shard ID is AuthZ subject.
            shard_producer,
            &service.publisher_factory,
            &labeling.stats_journal,
            [&spec],
        )
        .context("creating publisher")?
    };

    // Scan and send L:Recover state from RocksDB. Derivations have no max-keys
    // (connector state is a singleton), but the committed/hinted frontier is
    // per-transform, keyed by each transform's `state_key`.
    let mut sorted_state_keys: Vec<(String, u32)> = task
        .binding_state_keys
        .iter()
        .enumerate()
        .map(|(i, sk)| (sk.clone(), i as u32))
        .collect();
    sorted_state_keys.sort();

    let (mut db, recover) = db
        .scan(sorted_state_keys)
        .await
        .context("scanning RocksDB")?;

    _ = leader_tx.send(proto::Derive {
        recover: Some(recover),
        ..Default::default()
    });

    // Read and apply L:Persist from the leader until L:Open. Derive has no
    // Apply phase: the leader opens connectors directly after Recover.
    let open = loop {
        let verify = crate::verify("Derive", "Persist or Open", "leader");
        match verify.not_eof(leader_rx.next().await)? {
            proto::Derive {
                persist: Some(persist),
                ..
            } => {
                db = db
                    .persist(&persist, &task.binding_state_keys)
                    .await
                    .context("Persist failed")?;

                _ = leader_tx.send(proto::Derive {
                    persisted: Some(proto::Persisted {
                        seq_no: persist.seq_no,
                    }),
                    ..Default::default()
                });
            }
            proto::Derive {
                open: Some(open), ..
            } => break open,

            other => return Err(verify.fail_msg(other)),
        }
    };

    // Start the connector and send C:Open.
    let proto::Open {
        connector_state_json,
        max_keys: _,
        range,
        spec: open_spec,
        version,
    } = open;

    let open_spec = flow::CollectionSpec::decode(open_spec.as_ref())
        .context("invalid CollectionSpec in L:Open")?;

    let mut initial = derive::Request {
        open: Some(derive::request::Open {
            collection: Some(open_spec),
            version,
            range,
            state_json: non_empty_state(&connector_state_json),
        }),
        ..Default::default()
    };
    // Thread the recorded SQLite VFS to derive-sqlite (which requires it to be
    // set; absent it uses an in-memory database). Harmless for other connectors.
    if !sqlite_vfs_uri.is_empty() {
        initial.set_internal(|ext: &mut DeriveRequestExt| {
            ext.open = Some(derive_request_ext::Open { sqlite_vfs_uri });
        });
    }

    let (connector_tx, mut connector_rx, container, codec) =
        super::connector::start(service, log_level, initial).await?;

    let verify = crate::verify("Derive", "Opened", "connector");
    let opened = match verify.not_eof(connector_rx.next().await)? {
        derive::Response {
            opened: Some(opened),
            ..
        } => opened,
        other => return Err(verify.fail_msg(other)),
    };
    let derive::response::Opened { runtime_checkpoint } = opened;

    // Tell the leader (checkpoint, no container) and controller (container, no checkpoint).
    _ = leader_tx.send(proto::Derive {
        opened: Some(proto::derive::Opened {
            container: None,
            connector_checkpoint: runtime_checkpoint,
        }),
        ..Default::default()
    });
    _ = controller_tx.send(Ok(proto::Derive {
        opened: Some(proto::derive::Opened {
            container,
            connector_checkpoint: None,
        }),
        ..Default::default()
    }));

    let shuffle_reader =
        shuffle::log::Reader::new(std::path::Path::new(&shuffle_directory), shard_index);

    let accumulator =
        crate::Accumulator::new(task.combine_spec()?).context("building derive output combiner")?;

    Ok(Startup {
        accumulator,
        codec,
        connector_rx,
        connector_tx,
        db,
        leader_rx,
        leader_tx,
        publisher,
        shuffle_reader,
        task,
        write_shape,
    })
}

/// Connector `state_json` defaults to an empty JSON object when no state has
/// been persisted, since connectors expect a JSON document.
fn non_empty_state(connector_state_json: &bytes::Bytes) -> bytes::Bytes {
    if connector_state_json.is_empty() {
        bytes::Bytes::from_static(b"{}")
    } else {
        connector_state_json.clone()
    }
}
