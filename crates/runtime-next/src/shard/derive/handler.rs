use super::{connector, startup};
use crate::proto;
use anyhow::Context;
use futures::StreamExt;
use proto_flow::derive;
use tokio::sync::mpsc;
use tracing::Instrument;

pub(crate) async fn serve<R, L: crate::LogHandler>(
    service: crate::shard::Service<L>,
    mut controller_rx: R,
    controller_tx: mpsc::UnboundedSender<tonic::Result<proto::Derive>>,
) -> anyhow::Result<()>
where
    R: futures::Stream<Item = tonic::Result<proto::Derive>> + Send + Unpin + 'static,
{
    let verify = crate::verify("Derive", "SessionLoop, Spec, or Validate", "controller");
    while let Some(result) = controller_rx.next().await {
        match verify.ok(result)? {
            proto::Derive {
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

            proto::Derive {
                spec: Some(spec),
                log_level,
                ..
            } => {
                let log_level =
                    ops::LogLevel::try_from(log_level).unwrap_or(ops::LogLevel::UndefinedLevel);
                service.set_log_level(log_level);
                let response = serve_unary(
                    &service,
                    derive::Request {
                        spec: Some(spec),
                        ..Default::default()
                    },
                    log_level,
                )
                .await?;
                _ = controller_tx.send(Ok(response));
            }

            proto::Derive {
                validate: Some(validate),
                log_level,
                ..
            } => {
                let log_level =
                    ops::LogLevel::try_from(log_level).unwrap_or(ops::LogLevel::UndefinedLevel);
                service.set_log_level(log_level);
                let response = serve_unary(
                    &service,
                    derive::Request {
                        validate: Some(validate),
                        ..Default::default()
                    },
                    log_level,
                )
                .await?;
                _ = controller_tx.send(Ok(response));
            }

            request => return Err(verify.fail_msg(request)),
        }
    }
    Ok(())
}

pub async fn serve_unary<L: crate::LogHandler>(
    service: &crate::shard::Service<L>,
    request: derive::Request,
    log_level: ops::LogLevel,
) -> anyhow::Result<proto::Derive> {
    let is_spec = request.spec.is_some();
    let is_validate = request.validate.is_some();

    let (connector_tx, mut connector_rx, _container, _codec) =
        connector::start(service, log_level, request).await?;
    std::mem::drop(connector_tx); // Send EOF.

    let verify = crate::verify("Derive", "unary response", "connector");
    let response = match verify.not_eof(connector_rx.next().await)? {
        derive::Response {
            spec: Some(spec), ..
        } if is_spec => proto::Derive {
            spec_response: Some(spec),
            ..Default::default()
        },
        derive::Response {
            validated: Some(validated),
            ..
        } if is_validate => proto::Derive {
            validated: Some(validated),
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
    controller_tx: &mpsc::UnboundedSender<tonic::Result<proto::Derive>>,
    session_loop: proto::SessionLoop,
) -> anyhow::Result<()>
where
    R: futures::Stream<Item = tonic::Result<proto::Derive>> + Send + Unpin + 'static,
{
    let proto::SessionLoop { rocksdb_descriptor } = session_loop;
    let mut db = crate::shard::RocksDB::open(rocksdb_descriptor).await?;

    // Producer identities selected once and held constant across every session
    // of the loop. Two distinct producers: `shard_producer` sequences this
    // shard's derived documents; `leader_producer` is forwarded by shard zero
    // in Task and sequences the leader's stats / ACK-intent Publisher.
    let shard_producer = crate::new_producer();
    let leader_producer = crate::new_producer();

    let verify = crate::verify("Derive", "Join", "controller");
    while let Some(result) = controller_rx.next().await {
        match verify.ok(result)? {
            proto::Derive {
                join: Some(join), ..
            } => {
                db = serve_session(
                    service,
                    controller_rx,
                    controller_tx,
                    db,
                    join,
                    shard_producer,
                    leader_producer,
                )
                .await?;
            }
            request => return Err(verify.fail_msg(request)),
        };
    }

    Ok(())
}

async fn serve_session<R, L: crate::LogHandler>(
    service: &crate::shard::Service<L>,
    controller_rx: &mut R,
    controller_tx: &mpsc::UnboundedSender<tonic::Result<proto::Derive>>,
    db: crate::shard::RocksDB,
    join: proto::Join,
    shard_producer: proto_gazette::uuid::Producer,
    leader_producer: proto_gazette::uuid::Producer,
) -> anyhow::Result<crate::shard::RocksDB>
where
    R: futures::Stream<Item = tonic::Result<proto::Derive>> + Send + Unpin + 'static,
{
    let handler = service.registry.register("shard.derive");
    let span = handler.span();
    serve_session_inner(
        service,
        controller_rx,
        controller_tx,
        db,
        join,
        shard_producer,
        leader_producer,
        handler,
    )
    .instrument(span)
    .await
}

async fn serve_session_inner<R, L: crate::LogHandler>(
    service: &crate::shard::Service<L>,
    controller_rx: &mut R,
    controller_tx: &mpsc::UnboundedSender<tonic::Result<proto::Derive>>,
    db: crate::shard::RocksDB,
    join: proto::Join,
    shard_producer: proto_gazette::uuid::Producer,
    leader_producer: proto_gazette::uuid::Producer,
    mut handler: service_kit::HandlerGuard,
) -> anyhow::Result<crate::shard::RocksDB>
where
    R: futures::Stream<Item = tonic::Result<proto::Derive>> + Send + Unpin + 'static,
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
    let log_level = labeling.log_level();
    let shard_id = shard_id.clone();
    let shard_index = join.shard_index;
    let shuffle_directory = join.shuffle_directory.clone();

    service.set_log_level(log_level);

    handler.set_label(&shard_id);
    handler.set_field("shard_index", shard_index);
    handler.set_field("etcd_mod_revision", join.etcd_mod_revision);
    handler.set_phase("joining");

    let metrics = super::Metrics::new(&shard_id);

    service_kit::event!(
        tracing::Level::INFO,
        "leader",
        shard_index,
        leader_endpoint = join.leader_endpoint.clone(),
        "dialing leader and sending Join",
    );

    let (joined, leader_stream) =
        startup::dial_and_join(join, service.data_plane_signer.as_ref()).await?;

    _ = controller_tx.send(Ok(proto::Derive {
        joined: Some(joined),
        ..Default::default()
    }));
    let Some((leader_tx, leader_rx)) = leader_stream else {
        service_kit::event!(
            tracing::Level::DEBUG,
            "leader",
            "leader returned non-zero max_etcd_revision; controller must retry Join",
        );
        handler.set_phase("awaiting-retry");
        handler.finish_ok();
        return Ok(db);
    };

    handler.set_phase("starting");

    let startup::Startup {
        accumulator,
        codec,
        mut connector_rx,
        connector_tx,
        db,
        mut leader_rx,
        leader_tx,
        publisher,
        shuffle_reader,
        task,
        write_shape,
    } = startup::run(
        controller_rx,
        controller_tx,
        db,
        labeling,
        leader_producer,
        leader_rx,
        leader_tx,
        log_level,
        service,
        shard_id,
        shard_index,
        shard_producer,
        shuffle_directory,
    )
    .await?;

    handler.set_phase("running");

    let result = super::actor::Actor::new(
        codec,
        connector_tx,
        db,
        leader_tx,
        metrics,
        publisher,
        std::sync::Arc::new(task),
        write_shape,
    )
    .serve(
        accumulator,
        &mut connector_rx,
        controller_rx,
        &mut leader_rx,
        shuffle_reader,
    )
    .await;

    let db = match result {
        Ok(db) => {
            handler.finish_ok();
            db
        }
        Err(err) => {
            handler.finish_err(&format!("{err:#}"));
            return Err(err);
        }
    };

    _ = controller_tx.send(Ok(proto::Derive {
        stopped: Some(proto::Stopped {}),
        ..Default::default()
    }));

    Ok(db)
}
