use super::{actor, fsm, startup};
use crate::{leader, proto};
use futures::StreamExt;
use tokio::sync::mpsc;
use tracing::Instrument;

pub(crate) async fn serve<R>(
    service: crate::Service,
    request_rx: R,
    response_tx: mpsc::UnboundedSender<tonic::Result<proto::Materialize>>,
) -> anyhow::Result<()>
where
    R: futures::Stream<Item = tonic::Result<proto::Materialize>> + Send + Unpin + 'static,
{
    // Run the whole handler inside its span so operator trace overrides (see
    // `service_kit::trace`) reach every log line — the actor loop's periodic
    // instrumentation included.
    let handler = service.registry.register("leader.materialize");
    let span = handler.span();
    serve_inner(service, request_rx, response_tx, handler)
        .instrument(span)
        .await
}

async fn serve_inner<R>(
    service: crate::Service,
    mut request_rx: R,
    response_tx: mpsc::UnboundedSender<tonic::Result<proto::Materialize>>,
    mut handler: service_kit::HandlerGuard,
) -> anyhow::Result<()>
where
    R: futures::Stream<Item = tonic::Result<proto::Materialize>> + Send + Unpin + 'static,
{
    let verify = crate::verify("Materialize", "Join", "shard");

    // Read the Join request.
    let join = match verify.not_eof(request_rx.next().await)? {
        proto::Materialize {
            join: Some(join), ..
        } => join,
        request => return Err(verify.fail_msg(request)),
    };
    let task_name = leader::validate_join(&join)?.to_string();

    handler.set_label(&join.shards[0].id);
    handler.set_field("shards", join.shards.len());
    handler.set_field("etcd_mod_revision", join.etcd_mod_revision);
    handler.set_phase("joining");

    service_kit::event!(
        tracing::Level::INFO,
        "shard",
        shard_index = join.shard_index,
        etcd_mod_revision = join.etcd_mod_revision,
        "received Join from shard",
    );

    // Scope `guard` to prove it's not held across await points.
    let outcome = {
        let mut guard = service.materialize_joins.lock().unwrap();

        let outcome = guard.entry(task_name.to_string()).or_default().register(
            join,
            request_rx.boxed(),
            response_tx,
        );
        if !matches!(&outcome, leader::JoinOutcome::Pending { .. }) {
            guard.remove(&task_name);
        }
        outcome
    };

    let slots = match outcome {
        leader::JoinOutcome::Pending { filled, target } => {
            service_kit::event!(
                tracing::Level::DEBUG,
                "leader",
                filled,
                target,
                "registered pending Join (awaiting consensus)",
            );
            handler.set_phase("awaiting-consensus");
            handler.finish_ok();
            return Ok(());
        }
        leader::JoinOutcome::Disagreement(slots) => {
            let max_etcd_revision = slots
                .iter()
                .map(|s| s.join.etcd_mod_revision)
                .max()
                .unwrap();

            service_kit::event!(
                tracing::Level::INFO,
                "leader",
                max_etcd_revision,
                retrying = slots.len(),
                "broadcasting retry due to topology disagreement",
            );
            let retry = proto::Materialize {
                joined: Some(proto::Joined { max_etcd_revision }),
                ..Default::default()
            };
            for slot in slots {
                let _ = slot.response_tx.send(Ok(retry.clone()));
            }
            handler.set_phase("topology-disagreement");
            handler.finish_ok();
            return Ok(());
        }

        leader::JoinOutcome::Consensus(slots) => slots,
    };

    handler.set_phase("starting");
    let metrics = super::Metrics::new(&slots[0].join.shards[0].id);

    service_kit::event!(
        tracing::Level::INFO,
        "leader",
        "consensus reached; starting session",
    );

    let mut build = String::new();
    let mut drop_v1_rollback = false;
    let mut ops_stats_journal = String::new();
    let mut reactors: Vec<String> = Vec::new();
    let mut shard_rx = Vec::with_capacity(slots.len());
    let mut shard_tx = Vec::with_capacity(slots.len());
    let mut shard_ids = Vec::with_capacity(slots.len());
    let mut shard_shuffles: Vec<shuffle::proto::Shard> = Vec::with_capacity(slots.len());

    for slot in slots {
        let leader::JoinSlot {
            join:
                proto::Join {
                    etcd_mod_revision: _,
                    shards: mut slot_shards,
                    shard_index,
                    shuffle_directory: directory,
                    shuffle_endpoint: endpoint,
                    leader_endpoint: _,
                },
            request_rx: slot_rx,
            response_tx: slot_tx,
        } = slot;

        let proto::join::Shard {
            id,
            labeling,
            reactor,
            etcd_create_revision: _,
        } = std::mem::take(&mut slot_shards[shard_index as usize]);

        let labeling = labeling.unwrap_or_default();

        reactors.push(reactor.unwrap_or_default().suffix);
        shard_rx.push(slot_rx);
        shard_tx.push(slot_tx);
        shard_ids.push(id.clone());
        shard_shuffles.push(shuffle::proto::Shard {
            id,
            range: labeling.range,
            directory,
            endpoint,
        });

        // Labels are identical across shards (enforced by Join equality check).
        build = labeling.build;
        drop_v1_rollback = leader::flag_enabled(&labeling.flags, leader::DROP_V1_ROLLBACK_FLAG);
        ops_stats_journal = labeling.stats_journal;
    }

    let error_tx = shard_tx.clone();

    // Run startup, and then the Actor transaction loop. The inner block is a
    // try-scope: an error from either gets a best-effort broadcast to all shards
    // below before propagating.
    let result = async {
        let startup::Startup {
            committed_close,
            committed_frontier,
            idempotent_replay,
            pending_ack_intents,
            pending_trigger_params,
            publisher,
            session,
            task,
        } = startup::run(
            build,
            drop_v1_rollback,
            ops_stats_journal,
            reactors,
            &mut shard_rx,
            &mut shard_tx,
            &service,
            shard_ids,
            shard_shuffles,
        )
        .await?;

        let head = fsm::Head::Idle(fsm::HeadIdle {
            last_close: committed_close,
            idempotent_replay,
        });
        let pending = fsm::PendingDeltas {
            ack_intents: pending_ack_intents,
            trigger_params: pending_trigger_params,
            ..Default::default()
        };
        let tail = fsm::Tail::Begin(fsm::TailBegin { pending });

        // Maintain the legacy V1 `consumer.Checkpoint` from the recovered
        // committed Frontier, unless the task has opted out of V1 rollback via
        // `drop-runtime-v1-rollback`.
        let legacy_checkpoint = if drop_v1_rollback {
            None
        } else {
            Some(committed_frontier)
        };

        let mut actor = actor::Actor::new(
            service.http_client.clone(),
            legacy_checkpoint,
            metrics,
            publisher,
            shard_tx,
            task,
        );
        handler.set_phase("running");
        actor.serve(head, tail, session, shard_rx).await
    }
    .await;

    let err = match result {
        Ok(()) => {
            handler.finish_ok();
            return Ok(());
        }
        Err(err) => err,
    };
    handler.finish_err(&format!("{err:#}"));

    // Best-effort broadcast of terminal error to all shards.
    let status = match err.downcast_ref::<tonic::Status>() {
        Some(status) => status.clone(),
        None => tonic::Status::unknown(format!("{err:?}")),
    };
    for tx in error_tx {
        let _ = tx.send(Err(status.clone()));
    }

    Err(err)
}
