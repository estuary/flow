//! Per-session shard-side startup orchestration.
//!
//! Drives the protocol handshake against the leader sidecar at
//! `Join.leader_endpoint` and the local connector, yielding a `Startup`
//! bundle that the actor consumes to run transactions.
//!
//! POD codec helpers (`state_to_frontier`, `reduce_state_patches`, etc.)
//! live in `recovery.rs`.

use super::recovery::{
    append_patch, labels_build_for, recover_stream_from_state, reduce_state_patches,
};
use crate::proto;
use crate::rocksdb::RocksDB;
use crate::verify_send;
use anyhow::Context;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{SinkExt, Stream, StreamExt};
use proto_flow::materialize;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::mpsc;

/// The per-session bundle produced by `run`.
pub struct Startup {
    pub leader_tx: mpsc::Sender<proto::Materialize>,
    pub leader_rx: BoxStream<'static, tonic::Result<proto::Materialize>>,
    pub connector_tx: futures::channel::mpsc::Sender<materialize::Request>,
    pub connector_rx: BoxStream<'static, tonic::Result<materialize::Response>>,
    pub task: super::Task,
    pub binding_state_keys: Arc<Vec<String>>,
    pub max_keys: BTreeMap<u32, Bytes>,
    pub publisher: crate::Publisher,
    pub shuffle_reader: shuffle::log::Reader,
    pub is_shard_zero: bool,
}

/// Output of `run`. The `rocksdb` slot is always returned (whether `Some` or
/// `None`) so the caller can store it back into its session-spanning
/// `Option<RocksDB>`, even when the session ends mid-startup.
pub struct RunOutput {
    pub startup: Startup,
    pub rocksdb: Option<RocksDB>,
}

impl Startup {
    /// Consume this Startup and the per-shard handles needed for actor IO,
    /// producing an `Actor` ready to call `.serve()`.
    pub fn into_actor(
        self,
        controller_tx: mpsc::Sender<tonic::Result<proto::Materialize>>,
        rocksdb: Option<RocksDB>,
    ) -> super::actor::Actor {
        let combine_spec = self.task.combine_spec().ok();
        let accumulator = combine_spec.and_then(|s| crate::Accumulator::new(s).ok());

        super::actor::Actor {
            controller_tx,
            leader_tx: self.leader_tx,
            leader_rx: self.leader_rx,
            connector_tx: self.connector_tx,
            connector_rx: self.connector_rx,
            rocksdb,
            binding_state_keys: self.binding_state_keys,
            accumulator,
            task: Some(self.task),
            committed_max_keys: self.max_keys,
            next_max_keys: BTreeMap::new(),
            load_keys: super::LoadKeySet::default(),
            shuffle_reader: Some(self.shuffle_reader),
            shuffle_remainders: std::collections::VecDeque::new(),
            persist_fut: None,
            drain_state: None,
            // Per-session reactor state — see `actor::Actor`.
            deltas: super::state::LoadDeltas::default(),
            frontier_journals: Vec::new(),
            c_flushed_received: false,
            received_l_start_commit: false,
            peer_patches_for_start_commit: Bytes::new(),
            persist_batch: rocksdb::WriteBatch::default(),
        }
    }
}

/// Inputs to `run`. The caller hands `rocksdb` over by value (shard zero
/// only; non-zero shards pass `None`); `run` returns it in `RunOutput`.
pub struct RunInputs<'a, L: crate::LogHandler> {
    pub runtime: &'a crate::Runtime<L>,
    pub rocksdb: Option<RocksDB>,
    pub join: proto::Join,
    pub shard_id: String,
}

/// Run the per-session startup protocol. Yields:
///   - `Ok(Some(RunOutput))` on `C:Opened`,
///   - `Ok(None)` if the session ended cleanly during startup (topology
///     disagreement signalled to the controller, or controller EOF before
///     Open) — the caller loses the RocksDB only on transport failure
///     paths,
///   - `Err(_)` on protocol or transport failure.
pub async fn run<L: crate::LogHandler, S>(
    inputs: RunInputs<'_, L>,
    controller_rx: &mut S,
    controller_tx: &mpsc::Sender<tonic::Result<proto::Materialize>>,
) -> anyhow::Result<Option<RunOutput>>
where
    S: Stream<Item = tonic::Result<proto::Materialize>> + Send + Unpin + 'static,
{
    let RunInputs {
        runtime,
        rocksdb,
        join,
        shard_id,
    } = inputs;

    let shard_index = join.shard_index;
    let is_shard_zero = shard_index == 0;
    let shuffle_directory = join.shuffle_directory.clone();
    let leader_endpoint = join.leader_endpoint.clone();
    let shard_range = join
        .shards
        .get(shard_index as usize)
        .and_then(|s| s.labeling.as_ref())
        .and_then(|l| l.range.clone());

    // 1. Dial the leader sidecar and forward Join → Joined.
    let (leader_tx, mut leader_rx) = dial_leader(&leader_endpoint).await?;
    if !forward_join_and_check_topology(&leader_tx, &mut leader_rx, controller_tx, join).await? {
        return Ok(None); // Topology disagreement; client should retry.
    }

    // 2. Receive Open from the controller. We hold it locally until step 4
    //    (Open must follow Recover on the wire to the leader); receiving it
    //    early lets us build the binding mapping for the RocksDB scan.
    let Some(open) = recv_open(controller_rx).await? else {
        return Ok(None);
    };
    let spec = open
        .materialization
        .as_ref()
        .context("Open missing materialization spec")?
        .clone();
    let version = labels_build_for(&spec);
    let controller_log_level = ::ops::LogLevel::UndefinedLevel as i32;

    // Sorted (state_key, binding_index) mapping for the recovery codec.
    let mut indexed: Vec<(String, u32)> = spec
        .bindings
        .iter()
        .enumerate()
        .map(|(i, b)| (b.state_key.clone(), i as u32))
        .collect();
    indexed.sort_by(|a, b| a.0.cmp(&b.0));

    // 3. Scan RocksDB on shard zero and stream Recover messages.
    let rocksdb = stream_recover(&leader_tx, rocksdb, is_shard_zero, indexed).await?;

    // 4. Apply loop on shard zero (forwards Open, runs L:Apply ↔ L:Persist).
    let (rocksdb, accumulated_patches, deferred) = if is_shard_zero {
        leader_tx
            .send(proto::Materialize {
                open: Some(open.clone()),
                ..Default::default()
            })
            .await
            .context("forwarding Open to leader")?;
        run_apply_loop(
            runtime,
            &leader_tx,
            &mut leader_rx,
            rocksdb,
            &spec,
            &version,
            controller_log_level,
        )
        .await?
    } else {
        (rocksdb, Bytes::new(), None)
    };

    // 5. Receive streamed Recovered (terminator-empty).
    let (recovered_patches, recovered_max_keys) =
        recv_recovered(&mut leader_rx, accumulated_patches, deferred).await?;

    // 6. Build and send C:Open; receive C:Opened; forward to leader and controller.
    let (connector_tx, mut connector_rx, task, connector_image) = open_connector(
        runtime,
        &spec,
        &version,
        shard_range,
        &recovered_patches,
        controller_log_level,
    )
    .await?;

    forward_opened(
        &leader_tx,
        controller_tx,
        &mut connector_rx,
        is_shard_zero,
        connector_image,
    )
    .await?;

    // 7. Build the per-session Publisher and shuffle log Reader.
    // ops_logs / ops_stats specs are optional: when both absent, the
    // per-shard publisher runs in `Publisher::Preview` (no journal IO). The
    // leader's session-startup applies the same logic to its own
    // publisher, so both sides stay in lockstep.
    let publisher = match (open.ops_logs_spec.as_ref(), open.ops_stats_spec.as_ref()) {
        (Some(logs_spec), Some(stats_spec)) => crate::Publisher::new_real(
            shard_id.clone(), // AuthZ subject.
            &runtime.publisher_factory,
            &open.ops_logs_journal,
            logs_spec,
            &open.ops_stats_journal,
            stats_spec,
            [], // No additional bindings.
        )?,
        (None, None) => crate::Publisher::new_preview(),
        _ => anyhow::bail!("Open ops_logs_spec / ops_stats_spec must both be set or both absent",),
    };

    let shuffle_reader =
        shuffle::log::Reader::new(std::path::Path::new(&shuffle_directory), shard_index);

    let binding_state_keys: Arc<Vec<String>> =
        Arc::new(task.bindings.iter().map(|b| b.state_key.clone()).collect());

    Ok(Some(RunOutput {
        startup: Startup {
            leader_tx,
            leader_rx,
            connector_tx,
            connector_rx,
            task,
            binding_state_keys,
            max_keys: recovered_max_keys,
            publisher,
            shuffle_reader,
            is_shard_zero,
        },
        rocksdb,
    }))
}

async fn dial_leader(
    endpoint: &str,
) -> anyhow::Result<(
    mpsc::Sender<proto::Materialize>,
    BoxStream<'static, tonic::Result<proto::Materialize>>,
)> {
    let channel = tonic::transport::Channel::from_shared(endpoint.to_string())
        .map_err(|e| anyhow::anyhow!("invalid leader endpoint: {e}"))?
        .connect()
        .await
        .with_context(|| format!("connecting to leader at {endpoint}"))?;

    let mut leader_client = proto_grpc::runtime::leader_client::LeaderClient::new(channel)
        .max_decoding_message_size(crate::MAX_MESSAGE_SIZE)
        .max_encoding_message_size(usize::MAX);

    let (leader_tx, leader_rx_chan) = mpsc::channel::<proto::Materialize>(crate::CHANNEL_BUFFER);
    let leader_outbound = tokio_stream::wrappers::ReceiverStream::new(leader_rx_chan);
    let leader_inbound = leader_client
        .materialize(leader_outbound)
        .await
        .context("opening leader Materialize stream")?
        .into_inner();

    Ok((leader_tx, leader_inbound.boxed()))
}

/// Forward Join, await Joined. Returns `false` on topology disagreement
/// (with Joined relayed to the controller); `true` on consensus.
async fn forward_join_and_check_topology(
    leader_tx: &mpsc::Sender<proto::Materialize>,
    leader_rx: &mut BoxStream<'static, tonic::Result<proto::Materialize>>,
    controller_tx: &mpsc::Sender<tonic::Result<proto::Materialize>>,
    join: proto::Join,
) -> anyhow::Result<bool> {
    leader_tx
        .send(proto::Materialize {
            join: Some(join),
            ..Default::default()
        })
        .await
        .context("forwarding Join to leader")?;

    let msg = leader_rx
        .next()
        .await
        .context("leader EOF before Joined")?
        .map_err(crate::status_to_anyhow)?;
    let joined = msg
        .joined
        .context("leader sent unexpected message (expected Joined)")?;
    let consensus = joined.max_etcd_revision == 0;

    verify_send(
        controller_tx,
        Ok(proto::Materialize {
            joined: Some(joined),
            ..Default::default()
        }),
    )
    .context("forwarding Joined to controller")?;

    Ok(consensus)
}

async fn stream_recover(
    leader_tx: &mpsc::Sender<proto::Materialize>,
    rocksdb: Option<RocksDB>,
    is_shard_zero: bool,
    binding_state_keys: Vec<(String, u32)>,
) -> anyhow::Result<Option<RocksDB>> {
    let (rocksdb, state) = match (rocksdb, is_shard_zero) {
        (Some(db), true) => {
            let (db, state) = db
                .scan(binding_state_keys)
                .await
                .context("scanning RocksDB")?;
            (Some(db), state)
        }
        (db, _) => (db, crate::recovery::State::default()),
    };

    for recover in recover_stream_from_state(state).context("building Recover stream")? {
        leader_tx
            .send(proto::Materialize {
                recover: Some(recover),
                ..Default::default()
            })
            .await
            .context("sending Recover")?;
    }
    Ok(rocksdb)
}

/// Receive Open from the controller. Returns `None` on clean EOF before Open.
async fn recv_open<S>(controller_rx: &mut S) -> anyhow::Result<Option<proto::materialize::Open>>
where
    S: Stream<Item = tonic::Result<proto::Materialize>> + Send + Unpin + 'static,
{
    let Some(msg) = controller_rx.next().await else {
        return Ok(None);
    };
    let msg = msg.map_err(crate::status_to_anyhow)?;
    let open = msg
        .open
        .clone()
        .with_context(|| format!("expected Open from controller after Joined, got {msg:?}"))?;
    Ok(Some(open))
}

/// Apply loop on shard zero. Receives L:Apply, runs a transient C:Apply
/// connector, and replies with L:Applied; on a non-empty applied patch the
/// leader follows with L:Persist, which we apply via the owned RocksDB and
/// reply with L:Persisted. The loop terminates on the first non-Apply,
/// non-Persist message.
///
/// Returns `(rocksdb, accumulated_patches, deferred_first_post_apply_message)`.
/// The deferred message is the first non-Apply/Persist seen; the caller's
/// `recv_recovered` consumes it before reading further from leader_rx.
async fn run_apply_loop<L: crate::LogHandler>(
    runtime: &crate::Runtime<L>,
    leader_tx: &mpsc::Sender<proto::Materialize>,
    leader_rx: &mut BoxStream<'static, tonic::Result<proto::Materialize>>,
    mut rocksdb: Option<RocksDB>,
    spec: &proto_flow::flow::MaterializationSpec,
    version: &str,
    log_level: i32,
) -> anyhow::Result<(Option<RocksDB>, Bytes, Option<proto::Materialize>)> {
    let mut connector_patches: Bytes = Bytes::new();

    loop {
        let msg = leader_rx
            .next()
            .await
            .context("leader EOF during Apply loop")?
            .map_err(crate::status_to_anyhow)?;

        match msg {
            proto::Materialize {
                apply: Some(apply), ..
            } => {
                let last_materialization: Option<proto_flow::flow::MaterializationSpec> =
                    if apply.last_applied.is_empty() {
                        None
                    } else {
                        Some(
                            prost::Message::decode(apply.last_applied.as_ref())
                                .context("decoding last_applied")?,
                        )
                    };
                let last_version = last_materialization
                    .as_ref()
                    .map(labels_build_for)
                    .unwrap_or_default();

                let applied_state = run_apply_connector(
                    runtime,
                    spec.clone(),
                    version.to_string(),
                    last_materialization,
                    last_version,
                    reduce_state_patches(&apply.connector_patches_json)
                        .context("reducing apply patches")?,
                    log_level,
                )
                .await?;

                let applied_patches = applied_state
                    .map(|s| Bytes::from(s.updated_json.to_vec()))
                    .unwrap_or_default();

                if !applied_patches.is_empty() {
                    connector_patches = append_patch(&connector_patches, &applied_patches);
                }

                leader_tx
                    .send(proto::Materialize {
                        applied: Some(proto::materialize::Applied {
                            connector_patches_json: applied_patches,
                        }),
                        ..Default::default()
                    })
                    .await
                    .context("sending L:Applied")?;
            }
            proto::Materialize {
                persist: Some(persist),
                ..
            } => {
                let nonce = persist.nonce;
                let db = rocksdb
                    .take()
                    .context("received L:Persist but shard has no RocksDB")?;

                // Apply-loop Persists arrive as singletons (non-zero nonce).
                let mut wb = rocksdb::WriteBatch::default();
                crate::rocksdb::extend_write_batch(&mut wb, &persist, &[] as &[&str])
                    .context("encoding Apply-loop Persist")?;

                let mut wo = rocksdb::WriteOptions::new();
                wo.set_sync(true);

                let db = db.write_opt(wb, wo).await.context("Apply-loop Persist")?;
                rocksdb = Some(db);

                leader_tx
                    .send(proto::Materialize {
                        persisted: Some(proto::Persisted { nonce }),
                        ..Default::default()
                    })
                    .await
                    .context("sending L:Persisted")?;
            }
            other => return Ok((rocksdb, connector_patches, Some(other))),
        }
    }
}

/// Run a single transient C:Apply connector. Returns the Applied state if
/// the connector returned one.
async fn run_apply_connector<L: crate::LogHandler>(
    runtime: &crate::Runtime<L>,
    spec: proto_flow::flow::MaterializationSpec,
    version: String,
    last_materialization: Option<proto_flow::flow::MaterializationSpec>,
    last_version: String,
    state_json: Bytes,
    log_level: i32,
) -> anyhow::Result<Option<proto_flow::flow::ConnectorState>> {
    let mut request = materialize::Request {
        apply: Some(materialize::request::Apply {
            materialization: Some(spec),
            version,
            last_materialization,
            last_version,
            state_json,
        }),
        ..Default::default()
    };
    request.set_internal(|internal| internal.log_level = log_level);

    let (mut connector_tx, mut connector_rx, _open_extras) =
        super::connector::start(runtime, request)
            .await
            .context("starting transient C:Apply connector")?;

    // Send EOF so the connector terminates after replying.
    let _ = connector_tx.close().await;

    let response = connector_rx
        .next()
        .await
        .context("connector EOF before Applied")??;

    let applied = response
        .applied
        .context("connector response is not Applied")?;

    if let Some(trailing) = connector_rx.next().await {
        let trailing = trailing?;
        anyhow::bail!("connector emitted unexpected message after Applied: {trailing:?}");
    }

    Ok(applied.state)
}

/// Read the streamed Recovered messages, terminating on the first empty one.
/// Aggregates `connector_patches_json` (concatenated using the State Update
/// Wire Format) and `max_keys` (last-write-wins). `deferred` is consumed
/// first (used by `run_apply_loop` to hand back its first post-Apply message).
async fn recv_recovered(
    leader_rx: &mut BoxStream<'static, tonic::Result<proto::Materialize>>,
    initial_patches: Bytes,
    deferred: Option<proto::Materialize>,
) -> anyhow::Result<(Bytes, BTreeMap<u32, Bytes>)> {
    let mut patches: Bytes = initial_patches;
    let mut max_keys: BTreeMap<u32, Bytes> = BTreeMap::new();
    let mut deferred = deferred;

    loop {
        let msg = match deferred.take() {
            Some(m) => m,
            None => leader_rx
                .next()
                .await
                .context("leader EOF during Recovered")?
                .map_err(crate::status_to_anyhow)?,
        };

        let recovered = msg
            .recovered
            .clone()
            .with_context(|| format!("expected Recovered, got {msg:?}"))?;

        if recovered.connector_patches_json.is_empty() && recovered.max_keys.is_empty() {
            return Ok((patches, max_keys));
        }
        if !recovered.connector_patches_json.is_empty() {
            patches = append_patch(&patches, &recovered.connector_patches_json);
        }
        for (k, v) in recovered.max_keys {
            max_keys.insert(k, v);
        }
    }
}

/// Build the C:Open request, start the connector, and parse the Task.
async fn open_connector<L: crate::LogHandler>(
    runtime: &crate::Runtime<L>,
    spec: &proto_flow::flow::MaterializationSpec,
    version: &str,
    shard_range: Option<proto_flow::flow::RangeSpec>,
    recovered_patches: &Bytes,
    log_level: i32,
) -> anyhow::Result<(
    futures::channel::mpsc::Sender<materialize::Request>,
    BoxStream<'static, tonic::Result<materialize::Response>>,
    super::Task,
    String,
)> {
    let state_json =
        reduce_state_patches(recovered_patches).context("reducing recovered patches")?;

    let mut open_req = materialize::Request {
        open: Some(materialize::request::Open {
            materialization: Some(spec.clone()),
            version: version.to_string(),
            range: shard_range,
            state_json,
        }),
        ..Default::default()
    };
    open_req.set_internal(|internal| internal.log_level = log_level);

    let task = super::Task::new(&open_req)?;

    let (connector_tx, connector_rx, open_extras) = super::connector::start(runtime, open_req)
        .await
        .context("starting materialize connector")?;

    Ok((
        connector_tx,
        connector_rx,
        task,
        open_extras.connector_image,
    ))
}

/// Receive C:Opened, then send L:Opened to the leader and Opened to the
/// controller.
async fn forward_opened(
    leader_tx: &mpsc::Sender<proto::Materialize>,
    controller_tx: &mpsc::Sender<tonic::Result<proto::Materialize>>,
    connector_rx: &mut BoxStream<'static, tonic::Result<materialize::Response>>,
    is_shard_zero: bool,
    connector_image: String,
) -> anyhow::Result<()> {
    let opened = connector_rx
        .next()
        .await
        .context("connector EOF before Opened")??;

    let opened_inner = opened
        .opened
        .as_ref()
        .with_context(|| format!("connector first response is not Opened: {opened:?}"))?
        .clone();
    let runtime_checkpoint = opened_inner.runtime_checkpoint.clone();
    let connector_container = opened.get_internal().ok().and_then(|i| i.container.clone());

    // Only shard zero reports task-level fields (legacy_checkpoint, connector_image).
    let (legacy_checkpoint, leader_connector_image) = if is_shard_zero {
        (runtime_checkpoint.clone(), connector_image)
    } else {
        (None, String::new())
    };
    leader_tx
        .send(proto::Materialize {
            opened: Some(proto::materialize::Opened {
                skip_replay_determinism: false,
                legacy_checkpoint,
                container: None,
                connector_image: leader_connector_image,
            }),
            ..Default::default()
        })
        .await
        .context("sending L:Opened")?;

    verify_send(
        controller_tx,
        Ok(proto::Materialize {
            opened: Some(proto::materialize::Opened {
                skip_replay_determinism: false,
                legacy_checkpoint: runtime_checkpoint,
                container: connector_container,
                connector_image: String::new(),
            }),
            ..Default::default()
        }),
    )
    .context("forwarding Opened to controller")?;

    Ok(())
}
