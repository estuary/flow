use super::connector;
use crate::Logger as _;
use crate::leader::capture::fsm;
use crate::proto;
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
    let verify = crate::verify(
        "Capture",
        "SessionLoop, Spec, Discover, or Validate",
        "controller",
    );
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

            proto::Capture {
                spec: Some(spec),
                log_level,
                ..
            } => {
                let log_level =
                    ops::LogLevel::try_from(log_level).unwrap_or(ops::LogLevel::UndefinedLevel);
                service.set_log_level(log_level);
                let response = serve_unary(
                    &service,
                    capture::Request {
                        spec: Some(spec),
                        ..Default::default()
                    },
                    log_level,
                )
                .await?;
                _ = controller_tx.send(Ok(response));
            }
            proto::Capture {
                discover: Some(discover),
                log_level,
                ..
            } => {
                let log_level =
                    ops::LogLevel::try_from(log_level).unwrap_or(ops::LogLevel::UndefinedLevel);
                service.set_log_level(log_level);
                let response = serve_unary(
                    &service,
                    capture::Request {
                        discover: Some(discover),
                        ..Default::default()
                    },
                    log_level,
                )
                .await?;
                _ = controller_tx.send(Ok(response));
            }
            proto::Capture {
                validate: Some(validate),
                log_level,
                ..
            } => {
                let log_level =
                    ops::LogLevel::try_from(log_level).unwrap_or(ops::LogLevel::UndefinedLevel);
                service.set_log_level(log_level);
                let response = serve_unary(
                    &service,
                    capture::Request {
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

async fn serve_unary<P: crate::PublisherFactory, L: crate::LoggerFactory>(
    service: &crate::shard::Service<P, L>,
    request: capture::Request,
    log_level: ops::LogLevel,
) -> anyhow::Result<proto::Capture> {
    let is_spec = request.spec.is_some();
    let is_discover = request.discover.is_some();
    let is_validate = request.validate.is_some();
    let logger = service.logger_factory.open(&service.task_name);
    let (connector_tx, mut connector_rx, _container, _token_restart_at) =
        connector::start(service, &logger, log_level, request).await?;
    std::mem::drop(connector_tx);

    let verify = crate::verify("Capture", "unary response", "connector");
    let response = match verify.not_eof(connector_rx.next().await)? {
        capture::Response {
            spec: Some(spec), ..
        } if is_spec => proto::Capture {
            spec_response: Some(spec),
            ..Default::default()
        },
        capture::Response {
            discovered: Some(discovered),
            ..
        } if is_discover => proto::Capture {
            discovered: Some(discovered),
            ..Default::default()
        },
        capture::Response {
            validated: Some(validated),
            ..
        } if is_validate => proto::Capture {
            validated: Some(validated),
            ..Default::default()
        },
        response => return Err(verify.fail_msg(response)),
    };
    verify.eof(connector_rx.next().await)?;
    Ok(response)
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
    let mut db = crate::shard::RocksDB::open(session_loop.rocksdb_descriptor).await?;
    let verify = crate::verify("Capture", "Join", "controller");

    // Inferred document shapes are held only in memory and accumulate across
    // every session of this Shard stream. They're keyed by stable binding
    // identity so a spec update that reorders bindings still resumes inference.
    let mut shapes_by_key: BTreeMap<String, doc::Shape> = BTreeMap::new();

    // Producer identity for this shard's Publisher, selected once and held
    // constant across every session of the loop.
    let producer = crate::new_producer();

    while let Some(result) = controller_rx.next().await {
        let join = match verify.ok(result)? {
            proto::Capture {
                join: Some(join), ..
            } => join,
            request => return Err(verify.fail_msg(request)),
        };

        db = serve_session(
            service,
            controller_rx,
            controller_tx,
            db,
            join,
            producer,
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

    let mut sorted_state_keys: Vec<(String, u32)> = spec
        .bindings
        .iter()
        .enumerate()
        .map(|(i, b)| (b.state_key.clone(), i as u32))
        .collect();
    sorted_state_keys.sort();
    let (mut db, mut recover) = db
        .scan(sorted_state_keys)
        .await
        .context("scanning RocksDB")?;
    db = db.seed_connector_state(&mut recover).await?;
    let proto::Recover {
        ack_intents,
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
        &binding_state_keys,
        &last_applied,
        &next_applied,
        &version,
        &mut connector_state_json,
        log_level,
    )
    .await?;

    let open = capture::Request {
        open: Some(capture::request::Open {
            capture: Some(spec.clone()),
            version: version.clone(),
            range: Some(range.clone()),
            state_json: connector_state_json,
        }),
        ..Default::default()
    };
    let (connector_tx, mut connector_rx, container, token_restart_at) =
        connector::start(service, &logger, log_level, open.clone()).await?;
    let verify = crate::verify("Capture", "Opened", "connector");
    let opened = match verify.not_eof(connector_rx.next().await)? {
        capture::Response {
            opened: Some(opened),
            ..
        } => capture::Response {
            opened: Some(opened),
            ..Default::default()
        },
        response => return Err(verify.fail_msg(response)),
    };
    let task = std::sync::Arc::new(crate::leader::capture::Task::new(
        &open,
        &opened,
        max_transactions,
    )?);

    let collection_specs: Vec<&flow::CollectionSpec> = spec
        .bindings
        .iter()
        .filter_map(|b| b.collection.as_ref())
        .collect();
    let publisher = service
        .publisher_factory
        .open(
            shard_id,
            producer,
            &labeling.stats_journal,
            &collection_specs,
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
    // binding layout, and stow the session's final shapes back when it ends.
    let shapes = task.binding_shapes_by_index(std::mem::take(shapes_by_key));

    let (db, shapes) = super::actor::Actor::new(
        binding_state_keys,
        connector_tx,
        db,
        metrics,
        logger,
        publisher,
        shapes,
        task.clone(),
        token_restart_at,
    )
    .serve(connector_rx, controller_rx, head, tail)
    .await?;

    *shapes_by_key = task.binding_shapes_by_key(shapes);

    _ = controller_tx.send(Ok(proto::Capture {
        stopped: Some(proto::Stopped {}),
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

    const MAX_APPLY_ITERATIONS: u64 = 3;

    for iteration in 1..=MAX_APPLY_ITERATIONS {
        let apply = capture::request::Apply {
            capture: Some(next_spec.clone()),
            version: next_version.to_string(),
            last_capture: last_spec.clone(),
            last_version: last_version.clone(),
            state_json: connector_state_json.clone(),
        };

        let (connector_tx, mut connector_rx, _container, _token_restart_at) = connector::start(
            service,
            logger,
            log_level,
            capture::Request {
                apply: Some(apply),
                ..Default::default()
            },
        )
        .await?;
        std::mem::drop(connector_tx);

        let verify = crate::verify("Capture", "Applied", "connector");
        let (action_description, applied_patches_json) =
            match verify.not_eof(connector_rx.next().await)? {
                capture::Response {
                    applied:
                        Some(capture::response::Applied {
                            action_description,
                            state,
                        }),
                    ..
                } => (
                    action_description,
                    crate::patches::encode_connector_state(state),
                ),
                response => return Err(verify.fail_msg(response)),
            };
        verify.eof(connector_rx.next().await)?;

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
