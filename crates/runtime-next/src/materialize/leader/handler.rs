use super::{actor, fsm, startup};
use crate::{leader, proto};
use futures::StreamExt;
use tokio::sync::mpsc;

pub(crate) async fn serve<R>(
    service: crate::Service,
    mut request_rx: R,
    response_tx: mpsc::UnboundedSender<tonic::Result<proto::Materialize>>,
) -> anyhow::Result<()>
where
    R: futures::Stream<Item = tonic::Result<proto::Materialize>> + Send + Unpin + 'static,
{
    let verify = crate::verify("Materialize", "Join", "shard", 0);

    // Read the Join request.
    let join = match verify.not_eof(request_rx.next().await)? {
        proto::Materialize {
            join: Some(join), ..
        } => join,
        request => return Err(verify.fail_msg(request)),
    };
    let task_name = leader::validate_join(&join)?.to_string();

    tracing::info!(
        %task_name,
        shards = join.shards.len(),
        shard_index = join.shard_index,
        etcd_mod_revision = join.etcd_mod_revision,
        shuffle_directory = %join.shuffle_directory,
        "received Join",
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
            tracing::debug!(
                %task_name,
                filled,
                target,
                "registered pending Join",
            );
            return Ok(());
        }
        leader::JoinOutcome::Disagreement(slots) => {
            let max_etcd_revision = slots
                .iter()
                .map(|s| s.join.etcd_mod_revision)
                .max()
                .unwrap();

            tracing::info!(
                %task_name,
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
            return Ok(());
        }

        leader::JoinOutcome::Consensus(slots) => slots,
    };

    tracing::info!(
        task_name,
        shards = slots.len(),
        "consensus reached; starting session",
    );

    let mut build = String::new();
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
        shard_ids.push(id);
        shard_shuffles.push(shuffle::proto::Shard {
            range: labeling.range,
            directory,
            endpoint,
        });
        build = labeling.build;
    }

    let error_tx = shard_tx.clone();

    // Run startup, and then the Actor transaction loop.
    let result = async move {
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

        // TODO: Make this toggle-able for dropping rollback support.
        let legacy_checkpoint = Some(committed_frontier);

        let mut actor = actor::Actor::new(
            service.http_client.clone(),
            legacy_checkpoint,
            publisher,
            shard_tx,
            task,
        );
        actor.serve(head, tail, session, shard_rx).await
    }
    .await;

    let Err(err) = result else {
        return Ok(());
    };

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
