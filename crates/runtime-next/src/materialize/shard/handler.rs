use super::{connector, connector_state_to_patches_json};
use crate::{materialize::shard::startup, proto};
use anyhow::Context;
use futures::StreamExt;
use proto_flow::materialize;
use tokio::sync::mpsc;

#[allow(dead_code)]
pub(crate) async fn serve<R, L: crate::LogHandler>(
    service: crate::shard::Service<L>,
    mut controller_rx: R,
    controller_tx: mpsc::UnboundedSender<tonic::Result<proto::Materialize>>,
) -> anyhow::Result<()>
where
    R: futures::Stream<Item = tonic::Result<proto::Materialize>> + Send + Unpin + 'static,
{
    let verify = crate::verify("Materialize", "Start, Spec, or Validate", "coordinator", 0);
    while let Some(result) = controller_rx.next().await {
        match verify.ok(result)? {
            proto::Materialize {
                session_loop: Some(session_loop),
                ..
            } => {
                return serve_session_loop(
                    &service,
                    &mut controller_rx,
                    &controller_tx,
                    session_loop,
                )
                .await;
            }

            proto::Materialize {
                spec: Some(spec), ..
            } => {
                let request = materialize::Request {
                    spec: Some(spec),
                    ..Default::default()
                };
                let response = serve_unary(&service, request).await?;
                _ = controller_tx.send(Ok(response));
            }

            proto::Materialize {
                validate: Some(validate),
                ..
            } => {
                let request = materialize::Request {
                    validate: Some(validate),
                    ..Default::default()
                };
                let response = serve_unary(&service, request).await?;
                _ = controller_tx.send(Ok(response));
            }

            request => return Err(verify.fail_msg(request)),
        }
    }
    Ok(())
}

pub async fn serve_unary<L: crate::LogHandler>(
    service: &crate::shard::Service<L>,
    request: materialize::Request,
) -> anyhow::Result<proto::Materialize> {
    let is_spec = request.spec.is_some();
    let is_validate = request.validate.is_some();
    let is_apply = request.apply.is_some();

    let (connector_tx, mut connector_rx, _container) = connector::start(service, request).await?;
    std::mem::drop(connector_tx); // Send EOF.

    let verify = crate::verify("Materialize", "unary response", "connector", 0);
    let response = match verify.not_eof(connector_rx.next().await)? {
        materialize::Response {
            spec: Some(spec), ..
        } if is_spec => proto::Materialize {
            spec_response: Some(spec),
            ..Default::default()
        },
        materialize::Response {
            validated: Some(validated),
            ..
        } if is_validate => proto::Materialize {
            validated: Some(validated),
            ..Default::default()
        },
        materialize::Response {
            applied:
                Some(materialize::response::Applied {
                    action_description,
                    state,
                }),
            ..
        } if is_apply => proto::Materialize {
            applied: Some(proto::Applied {
                action_description,
                connector_patches_json: connector_state_to_patches_json(state),
            }),
            ..Default::default()
        },
        response => return Err(verify.fail_msg(response)),
    };

    () = verify.eof(connector_rx.next().await)?;

    Ok(response)
}

async fn serve_session_loop<R, L: crate::LogHandler>(
    service: &crate::shard::Service<L>,
    controller_rx: &mut R,
    controller_tx: &mpsc::UnboundedSender<tonic::Result<proto::Materialize>>,
    session_loop: proto::SessionLoop,
) -> anyhow::Result<()>
where
    R: futures::Stream<Item = tonic::Result<proto::Materialize>> + Send + Unpin + 'static,
{
    let proto::SessionLoop { rocksdb_descriptor } = session_loop;

    let mut db = crate::RocksDB::open(rocksdb_descriptor).await?;

    let verify = crate::verify("Materialize", "Join", "coordinator", 0);
    while let Some(result) = controller_rx.next().await {
        match verify.ok(result)? {
            proto::Materialize {
                join: Some(join), ..
            } => {
                db = serve_single_session(service, controller_rx, controller_tx, db, join).await?;
            }
            request => return Err(verify.fail_msg(request)),
        };
    }

    todo!()
}

async fn serve_single_session<R, L: crate::LogHandler>(
    service: &crate::shard::Service<L>,
    controller_rx: &mut R,
    controller_tx: &mpsc::UnboundedSender<tonic::Result<proto::Materialize>>,
    db: crate::RocksDB,
    join: proto::Join,
) -> anyhow::Result<crate::RocksDB>
where
    R: futures::Stream<Item = tonic::Result<proto::Materialize>> + Send + Unpin + 'static,
{
    let proto::join::Shard {
        etcd_create_revision: _,
        id: shard_id,
        labeling,
        reactor: _,
    } = join
        .shards
        .get(join.shard_index as usize)
        .context("missing shard for shard index")?;

    let labeling = labeling.as_ref().context("missing shard labeling")?.clone();
    let shard_id = shard_id.clone();
    let shard_zero = join.shard_index == 0;

    let (joined, leader_stream) = startup::dial_and_join(join).await?;

    // Forward Joined to leader.
    _ = controller_tx.send(Ok(proto::Materialize {
        joined: Some(joined),
        ..Default::default()
    }));
    let Some((leader_tx, leader_rx)) = leader_stream else {
        return Ok(db); // We must retry Join/Joined.
    };

    let _startup = startup::run(
        controller_rx,
        controller_tx,
        db,
        labeling,
        leader_rx,
        leader_tx,
        service,
        shard_id,
        shard_zero,
    )
    .await?;

    todo!()
}
