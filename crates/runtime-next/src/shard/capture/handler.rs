use crate::Logger as _;
use crate::leader::capture::fsm;
use crate::proto;
use crate::shard::connector;
use anyhow::Context;
use futures::StreamExt;
use prost::Message;
use proto_flow::{capture, flow};
use std::collections::BTreeMap;
use tokio::sync::mpsc;
use tracing::Instrument;

pub(crate) async fn serve<R, P: crate::PublisherFactory, L: crate::LoggerFactory>(
    service: crate::shard::Service<P, L>,
    mut controller_rx: R,
    controller_tx: mpsc::UnboundedSender<tonic::Result<proto::Capture>>,
) -> anyhow::Result<()>
where
    R: futures::Stream<Item = tonic::Result<proto::Capture>> + Send + Unpin + 'static,
{
    let verify = crate::verify("Capture", "SessionLoop", "controller");
    while let Some(result) = controller_rx.next().await {
        match verify.ok(result)? {
            proto::Capture {
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

            request => return Err(verify.fail_msg(request)),
        }
    }
    Ok(())
}

async fn serve_session_loop<R, P: crate::PublisherFactory, L: crate::LoggerFactory>(
    service: &crate::shard::Service<P, L>,
    controller_rx: &mut R,
    controller_tx: &mpsc::UnboundedSender<tonic::Result<proto::Capture>>,
    session_loop: proto::SessionLoop,
) -> anyhow::Result<()>
where
    R: futures::Stream<Item = tonic::Result<proto::Capture>> + Send + Unpin + 'static,
{
    let proto::SessionLoop {
        rocksdb_descriptor,
        initial_connector_state_json,
        report_final_state,
    } = session_loop;
    let mut db = crate::shard::RocksDB::open(rocksdb_descriptor).await?;

    if !initial_connector_state_json.is_empty() {
        db = db
            .put_connector_state_base(&initial_connector_state_json)
            .await
            .context("applying SessionLoop initial connector state")?;
    }
    let verify = crate::verify("Capture", "Join", "controller");

    // Inferred document shapes are held only in memory and accumulate across
    // every session of this Shard stream. They're keyed by stable collection
    // identity (`partition_template_name`) so a spec update that reorders or
    // re-fans bindings still resumes inference of each collection.
    let mut shapes_by_key: BTreeMap<String, doc::Shape> = BTreeMap::new();

    // Producer identity for this shard's Publisher, selected once and held
    // constant across every session of the loop.
    let producer = crate::new_producer();

    while let Some(result) = controller_rx.next().await {
        let join = match verify.ok(result)? {
            proto::Capture {
                join: Some(join), ..
            } => join,

            // A Stop addressed to a session which has already returned here;
            // see the materialize session loop for how the controller comes to
            // send one, and why failing the stream over it strands the shard.
            proto::Capture { stop: Some(_), .. } => continue,

            request => return Err(verify.fail_msg(request)),
        };

        db = serve_session(
            service,
            controller_rx,
            controller_tx,
            db,
            join,
            producer,
            report_final_state,
            &mut shapes_by_key,
        )
        .await?;
    }

    Ok(())
}

async fn serve_session<R, P: crate::PublisherFactory, L: crate::LoggerFactory>(
    service: &crate::shard::Service<P, L>,
    controller_rx: &mut R,
    controller_tx: &mpsc::UnboundedSender<tonic::Result<proto::Capture>>,
    db: crate::shard::RocksDB,
    join: proto::Join,
    producer: proto_gazette::uuid::Producer,
    report_final_state: bool,
    shapes_by_key: &mut BTreeMap<String, doc::Shape>,
) -> anyhow::Result<crate::shard::RocksDB>
where
    R: futures::Stream<Item = tonic::Result<proto::Capture>> + Send + Unpin + 'static,
{
    // Fresh handler (and span) per session, matching the materialize shard:
    // each `interval` poll is its own admin-dashboard entry, and span fields
    // like `label` are recorded exactly once per guard.
    let handler = service.registry.register("shard.capture");
    let span = handler.span();
    serve_session_inner(
        service,
        controller_rx,
        controller_tx,
        db,
        join,
        producer,
        report_final_state,
        shapes_by_key,
        handler,
    )
    .instrument(span)
    .await
}

async fn serve_session_inner<R, P: crate::PublisherFactory, L: crate::LoggerFactory>(
    service: &crate::shard::Service<P, L>,
    controller_rx: &mut R,
    controller_tx: &mpsc::UnboundedSender<tonic::Result<proto::Capture>>,
    db: crate::shard::RocksDB,
    join: proto::Join,
    producer: proto_gazette::uuid::Producer,
    report_final_state: bool,
    shapes_by_key: &mut BTreeMap<String, doc::Shape>,
    handler: service_kit::HandlerGuard,
) -> anyhow::Result<crate::shard::RocksDB>
where
    R: futures::Stream<Item = tonic::Result<proto::Capture>> + Send + Unpin + 'static,
{
    let proto::join::Shard {
        etcd_create_revision: _,
        id: shard_id,
        labeling,
        reactor: _,
    } = join
        .shards
        .first()
        .context("missing capture shard in Join")?;
    if join.shards.len() != 1 || join.shard_index != 0 {
        anyhow::bail!(
            "capture Join requires exactly one shard at index zero, got {} shards and index {}",
            join.shards.len(),
            join.shard_index
        );
    }
    let labeling = labeling.as_ref().context("missing shard labeling")?.clone();
    let log_level = labeling.log_level();
    let shard_id = shard_id.clone();

    service.set_log_level(log_level);
    handler.set_label(&shard_id);
    handler.set_field("etcd_mod_revision", join.etcd_mod_revision);
    handler.set_phase("joined");

    let logger = service.logger_factory.open(&service.task_name);
    let metrics = super::Metrics::new(&shard_id);

    _ = controller_tx.send(Ok(proto::Capture {
        joined: Some(proto::Joined {
            max_etcd_revision: 0,
        }),
        ..Default::default()
    }));

    // Receive Task. Captures have no leader, so the shard consumes Task itself
    // (materialize instead forwards Task to its leader, which replies Open).
    let verify = crate::verify("Capture", "Task", "controller");
    let proto::Task {
        spec,
        max_transactions,
        sqlite_vfs_uri: _,
        publisher_id: _, // Captures are leaderless; the shard's own producer is used.
    } = match verify.not_eof(controller_rx.next().await)? {
        proto::Capture {
            task: Some(task), ..
        } => task,
        request => return Err(verify.fail_msg(request)),
    };
    let spec = flow::CaptureSpec::decode(spec.as_ref()).context("invalid Task capture")?;

    // Build version and key range come from the live shard labeling carried by
    // Join — not from Task, which carries only the spec and harness controls.
    let version = labeling.build.clone();
    let range = labeling
        .range
        .clone()
        .context("missing range in shard labeling")?;
    handler.set_phase("starting");

    let (mut db, mut recover) = db
        .scan(spec.bindings.iter().map(|b| b.state_key.as_str()))
        .await
        .context("scanning RocksDB")?;
    db = db.seed_connector_state(&mut recover).await?;
    let proto::Recover {
        ack_intents,
        active_backfills,
        mut connector_state_json,
        last_applied,
        ..
    } = recover;
    let binding_state_keys: Vec<String> =
        spec.bindings.iter().map(|b| b.state_key.clone()).collect();

    // Re-encode the spec to canonical prost bytes. `last_applied` was persisted
    // by a prior session as these same canonical bytes, so apply_loop's
    // unchanged-spec short-circuit compares like for like — independent of how
    // the controller (Go gogoproto) happened to frame `Task.spec`.
    let next_applied = bytes::Bytes::from(spec.encode_to_vec());
    db = apply_loop(
        service,
        &logger,
        db,
        &labeling.task_name,
        &binding_state_keys,
        &last_applied,
        &next_applied,
        &version,
        &mut connector_state_json,
        log_level,
    )
    .await?;

    let open = capture::Request {
        kind: Some(capture::request::Kind::Open(Box::new(
            capture::request::Open {
                capture: Some(spec.clone()),
                version: version.clone(),
                range: Some(range.clone()),
                state_json: connector_state_json,
                // Populated by `connector::start` with the matched endpoint's inner
                // sealed configuration, which is not yet extracted from `spec` here.
                sealed_config_json: Default::default(),
            },
        ))),
        ..Default::default()
    };
    let (
        connector_tx,
        mut connector_rx,
        connector::Started {
            container,
            token_restart_at,
            ..
        },
    ) = connector::start(
        &*service.connector_router,
        &logger,
        &labeling.task_name,
        connector::proto::request::Start {
            log_level: log_level as i32,
            sqlite_vfs_uri: String::new(),
        },
        connector::proto::request::Kind::Capture(open.clone()),
    )
    .await?;
    let verify = crate::verify("Capture", "Opened", "connector");
    let next = connector::next(&mut connector_rx, &logger, connector::unwrap_capture);
    let opened = match verify.not_eof(next.await)? {
        capture::Response {
            kind: Some(capture::response::Kind::Opened(opened)),
            ..
        } => capture::Response {
            kind: Some(capture::response::Kind::Opened(opened)),
            ..Default::default()
        },
        response => return Err(verify.fail_msg(response)),
    };
    let task = std::sync::Arc::new(crate::leader::capture::Task::new(
        &open,
        &opened,
        max_transactions,
    )?);

    // Publisher targets follow the Task's targets, so a fan-in capture opens one
    // journal client and one partitions watch per collection rather than per
    // binding, and the combiner validator and publisher target of a binding are
    // grouped identically.
    let collection_specs: Vec<&flow::CollectionSpec> = task
        .targets
        .iter()
        .map(|target| {
            let index = target.first_binding as usize;
            Ok(spec
                .binding_collection(&spec.bindings[index])
                .context("missing collection")
                .context(index)?
                .0)
        })
        .collect::<anyhow::Result<_>>()?;
    let binding_targets: Vec<u32> = task.bindings.iter().map(|binding| binding.target).collect();

    let publisher = service
        .publisher_factory
        .open(
            shard_id,
            producer,
            &labeling.stats_journal,
            &collection_specs,
            &binding_targets,
        )
        .context("opening publisher")?;

    _ = controller_tx.send(Ok(proto::Capture {
        opened: Some(proto::capture::Opened { container }),
        ..Default::default()
    }));

    handler.set_phase("running");

    let head = fsm::Head::Idle(fsm::HeadIdle {
        extents: Default::default(),
        // We don't bother with cross-session persistence of last commit.
        last_close: proto_gazette::uuid::Clock::zero(),
    });
    let tail = fsm::Tail::Recover(fsm::TailRecover {
        checkpoints: 0,
        ack_intents,
    });

    // Restore inferred shapes accumulated by prior sessions into this session's
    // inference-slot layout, and stow the session's final shapes back when it ends.
    let shapes = task.shapes_by_target(std::mem::take(shapes_by_key));

    // Only shard zero drives backfill truncation: it owns the origin of the key
    // and r-clock ranges, so it sees each backfill's full lifecycle even when split.
    let is_shard_zero = range.key_begin == 0 && range.r_clock_begin == 0;

    let (db, shapes) = super::actor::Actor::new(
        active_backfills,
        binding_state_keys,
        connector_tx,
        db,
        is_shard_zero,
        metrics,
        logger,
        publisher,
        shapes,
        task.clone(),
        token_restart_at,
    )
    .serve(connector_rx, controller_rx, head, tail)
    .await?;

    *shapes_by_key = task.shapes_by_key(shapes);

    let (db, stopped) = crate::shard::stopped_message(db, report_final_state).await?;
    _ = controller_tx.send(Ok(proto::Capture {
        stopped: Some(stopped),
        ..Default::default()
    }));
    Ok(db)
}

/// Run the connector's Apply action until it converges, then promote the
/// applied spec to `last-applied` in RocksDB.
///
/// The persistent state machine is `(last_applied, connector_state_json)`. Each
/// iteration sends Apply carrying the current reduced connector state, so a
/// connector that returns state patches observes its own prior patches on the
/// next Apply and can converge. Iteration patches are persisted to RocksDB
/// before re-applying; `last_applied` is bumped only on the final converged
/// iteration. A crash mid-loop therefore resumes with the OLD `last_applied`
/// against partially-advanced state — the connector's Apply must be idempotent
/// across repeated invocations of the same target spec (see the `C:Apply` proto
/// comment).
async fn apply_loop<P: crate::PublisherFactory, L: crate::LoggerFactory>(
    service: &crate::shard::Service<P, L>,
    logger: &L::Logger,
    mut db: crate::shard::RocksDB,
    task_name: &str,
    binding_state_keys: &[String],
    last_applied: &bytes::Bytes,
    next_applied: &bytes::Bytes,
    next_version: &str,
    connector_state_json: &mut bytes::Bytes,
    log_level: ops::LogLevel,
) -> anyhow::Result<crate::shard::RocksDB> {
    // Spec is unchanged: a prior session already converged Apply and persisted
    // `last_applied`. Skip — captures re-Open every `interval`, and an Apply on
    // each restart would start a connector container for nothing.
    if last_applied == next_applied {
        return Ok(db);
    }

    let last_spec = if last_applied.is_empty() {
        None
    } else {
        Some(
            flow::CaptureSpec::decode(last_applied.as_ref())
                .context("invalid recovered last-applied CaptureSpec")?,
        )
    };
    let last_version = last_spec.as_ref().map(labels_build_for).unwrap_or_default();
    let next_spec = flow::CaptureSpec::decode(next_applied.as_ref())
        .context("invalid current CaptureSpec for Apply")?;

    if let Some(event) = crate::LogEvent::spec_update(&last_version, next_version) {
        logger.event(event);
    }

    const MAX_APPLY_ITERATIONS: u64 = 3;

    for iteration in 1..=MAX_APPLY_ITERATIONS {
        let apply = capture::request::Apply {
            capture: Some(next_spec.clone()),
            version: next_version.to_string(),
            last_capture: last_spec.clone(),
            last_version: last_version.clone(),
            state_json: connector_state_json.clone(),
        };

        let (connector_tx, mut connector_rx, _started) = connector::start(
            &*service.connector_router,
            logger,
            task_name,
            connector::proto::request::Start {
                log_level: log_level as i32,
                sqlite_vfs_uri: String::new(),
            },
            connector::proto::request::Kind::Capture(capture::Request {
                kind: Some(capture::request::Kind::Apply(Box::new(apply))),
                ..Default::default()
            }),
        )
        .await?;
        std::mem::drop(connector_tx);

        let verify = crate::verify("Capture", "Applied", "connector");
        let next = connector::next(&mut connector_rx, logger, connector::unwrap_capture);
        let (action_description, applied_patches_json) = match verify.not_eof(next.await)? {
            capture::Response {
                kind:
                    Some(capture::response::Kind::Applied(capture::response::Applied {
                        action_description,
                        state,
                    })),
                ..
            } => (
                action_description,
                crate::patches::encode_connector_state(state),
            ),
            response => return Err(verify.fail_msg(response)),
        };
        let next = connector::next(&mut connector_rx, logger, connector::unwrap_capture);
        verify.eof(next.await)?;

        logger.event(crate::LogEvent::Applied {
            action_description: &action_description,
        });

        service_kit::event!(
            tracing::Level::INFO,
            "shard",
            iteration,
            action_description = action_description.clone(),
            patches = service_kit::event::debug(applied_patches_json.clone()),
            "capture connector Apply completed",
        );

        if applied_patches_json.is_empty() {
            // Converged: promote `next_applied` to `last-applied`. We only reach
            // here with `last_applied != next_applied` (else we returned above).
            db = db
                .persist(
                    &proto::Persist {
                        last_applied: next_applied.clone(),
                        ..Default::default()
                    },
                    binding_state_keys,
                )
                .await
                .context("persisting capture last_applied")?;
            return Ok(db);
        }

        // Fold the iteration's patches into the running reduced state so the
        // next Apply — and the eventual connector Open — observe them.
        *connector_state_json =
            crate::patches::apply_state_patches(connector_state_json, &applied_patches_json)?;

        // Persist the iteration's patches, observing the delta as it's emitted.
        let persist = proto::Persist {
            connector_patches_json: applied_patches_json,
            ..Default::default()
        };
        logger.event(crate::LogEvent::Persist { persist: &persist });
        db = db
            .persist(&persist, binding_state_keys)
            .await
            .context("persisting capture Apply connector patches")?;
    }

    anyhow::bail!(
        "capture apply loop did not converge after {MAX_APPLY_ITERATIONS} iterations; \
         connector continues to return state patches"
    );
}

fn labels_build_for(spec: &flow::CaptureSpec) -> String {
    let Some(template) = spec.shard_template.as_ref() else {
        return String::new();
    };
    let Some(set) = template.labels.as_ref() else {
        return String::new();
    };

    labels::expect_one(set, labels::BUILD)
        .unwrap_or_default()
        .to_string()
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn stop_awaiting_join_leaves_the_session_loop_serving() {
        let registry = service_kit::Registry::new();
        let (_connector_svc, connector_router) =
            ::connector::Service::new_local(String::new(), registry.clone());
        let service = crate::shard::Service::new(
            std::sync::Arc::new(connector_router),
            None,
            "test/task".to_string(),
            crate::publish::RecordingPublisherFactory,
            crate::TracingLoggerFactory,
            registry,
            None,
        );

        let (controller_tx, controller_rx) = mpsc::unbounded_channel();
        let mut responses = service.spawn_capture(
            tokio_stream::wrappers::UnboundedReceiverStream::new(controller_rx),
        );

        controller_tx
            .send(Ok(proto::Capture {
                session_loop: Some(proto::SessionLoop {
                    rocksdb_descriptor: None,
                    ..Default::default()
                }),
                ..Default::default()
            }))
            .unwrap();
        controller_tx
            .send(Ok(proto::Capture {
                stop: Some(proto::Stop {}),
                ..Default::default()
            }))
            .unwrap();
        std::mem::drop(controller_tx);

        let mut collected = Vec::new();
        while let Some(response) = responses.recv().await {
            collected.push(response);
        }
        assert!(
            collected.is_empty(),
            "session loop must absorb the Stop and close cleanly, got {collected:?}",
        );
    }
}
