use super::super::frontier_mapping;
use super::Task;
use crate::proto;
use anyhow::Context;
use bytes::Bytes;
use futures::{StreamExt, stream::BoxStream};
use prost::Message;
use proto_flow::flow;
use proto_gazette::{consumer, uuid};
use std::collections::BTreeMap;
use tokio::sync::mpsc;

/// Outcomes of the leader protocol startup phase.
pub(super) struct Startup<P: crate::Publisher, S: crate::leader::ShuffleSession, L: crate::Logger> {
    // Clock at which the last-committed transaction closed.
    pub committed_close: uuid::Clock,
    // Fully committed Frontier.
    pub committed_frontier: shuffle::Frontier,
    // Is the first transaction an idempotent replay of a recovered hinted Frontier?
    pub idempotent_replay: bool,
    // Logger of task-centric state changes and events.
    pub logger: L,
    // Recovered ACK intents of the last transaction.
    pub pending_ack_intents: BTreeMap<String, Bytes>,
    // Recovered variables for the task.
    pub pending_trigger_params: Bytes,
    // Publisher for writing stats and ACK intents.
    pub publisher: P,
    // Initiated shuffle session for the task and topology.
    pub session: S,
    // Task definition.
    pub task: Task,
}

#[tracing::instrument(
    level = "debug",
    err(Debug, level = "warn"),
    skip_all,
    fields(shard_zero = %shard_ids[0], shards = shard_ids.len())
)]
pub(super) async fn run<
    S: crate::ShuffleSessionFactory,
    P: crate::PublisherFactory,
    L: crate::LoggerFactory,
>(
    build: String,
    drop_v1_rollback: bool,
    ops_stats_journal: String,
    reactors: Vec<String>,
    shard_rx: &mut Vec<BoxStream<'static, tonic::Result<proto::Materialize>>>,
    shard_tx: &Vec<mpsc::UnboundedSender<tonic::Result<proto::Materialize>>>,
    service: &crate::Service<S, P, L>,
    shard_ids: Vec<String>,
    shard_shuffles: Vec<shuffle::proto::Shard>,
) -> anyhow::Result<Startup<P::Publisher, S::Session, L::Logger>> {
    let n_shards = reactors.len();
    assert_eq!(n_shards, shard_rx.len());
    assert_eq!(n_shards, shard_tx.len());
    assert_eq!(n_shards, shard_ids.len());
    assert_eq!(n_shards, shard_shuffles.len());

    let peers: Vec<String> = shard_ids
        .iter()
        .zip(reactors.iter())
        .map(|(shard_id, reactor)| format!("{shard_id}@{reactor}"))
        .collect();

    // Send L:Joined response to all shards.
    for tx in shard_tx {
        let _ = tx.send(Ok(proto::Materialize {
            joined: Some(proto::Joined {
                max_etcd_revision: 0, // Success.
            }),
            ..Default::default()
        }));
    }

    // Receive L:Task from shard zero.
    let verify = crate::verify("Materialize", "Open", &peers[0]);
    let task = match verify.not_eof(shard_rx[0].next().await)? {
        proto::Materialize {
            task: Some(task), ..
        } => task,
        other => return Err(verify.fail_msg(other)),
    };

    // Build task definition.
    let proto::Task {
        max_transactions,
        spec: spec_bytes,
        sqlite_vfs_uri: _,
        publisher_id,
    } = task;

    let spec = flow::MaterializationSpec::decode(spec_bytes.as_ref())
        .context("invalid Task materialization")?;
    let task = Task::new(build, &spec, max_transactions, peers)
        .await
        .context("building task definition")?;

    // Open a Logger for runtime events, bound to the task.
    let logger = service.logger_factory.open(&task.shard_ref.name);

    // Open a publisher for stats and ACK intents (no collection bindings).
    let publisher = service
        .publisher_factory
        .open(
            shard_ids[0].clone(), // Shard zero is AuthZ subject.
            crate::publish::producer_from_bytes(&publisher_id)?,
            &ops_stats_journal,
            &[],
        )
        .context("opening publisher")?;

    // Receive Recover fan-in.
    let proto::Recover {
        ack_intents: pending_ack_intents,
        committed_close_clock: committed_close,
        committed_frontier,
        mut connector_state_json,
        hinted_close_clock: hinted_close,
        hinted_frontier,
        last_applied,
        legacy_checkpoint,
        max_keys,
        trigger_params_json: pending_trigger_params,
    } = recv_recovers(shard_rx, &task.peers)
        .await
        .context("receiving Recover fan-in")?;

    let committed_close = uuid::Clock::from_u64(committed_close);
    let hinted_close = uuid::Clock::from_u64(hinted_close);

    let hinted_frontier = shuffle::Frontier::decode(hinted_frontier.unwrap_or_default())
        .context("validating hinted Frontier")?;
    let committed_frontier = shuffle::Frontier::decode(committed_frontier.unwrap_or_default())
        .context("validating committed Frontier")?;

    tracing::debug!(
        ?committed_close,
        ?committed_frontier,
        connector_state_bytes = connector_state_json.len(),
        ?hinted_close,
        ?hinted_frontier,
        last_applied = !last_applied.is_empty(),
        ?legacy_checkpoint,
        ?max_keys,
        ?pending_trigger_params,
        "collected Recover from all shards",
    );

    // Run Apply on shard zero until convergence.
    apply_loop(
        &mut shard_rx[0],
        &shard_tx[0],
        &task.peers[0],
        &last_applied,
        &spec_bytes,
        &task.shard_ref.build,
        &mut connector_state_json,
        &logger,
    )
    .await?;

    // Open connectors across all shards.
    for (tx, shard) in shard_tx.iter().zip(shard_shuffles.iter()) {
        let _ = tx.send(Ok(proto::Materialize {
            open: Some(proto::Open {
                spec: spec_bytes.clone(),
                version: task.shard_ref.build.clone(),
                range: shard.range.clone(),
                connector_state_json: connector_state_json.clone(),
                max_keys: max_keys.clone(),
            }),
            ..Default::default()
        }));
    }

    // Receive Opened fan-in.
    let proto::materialize::Opened {
        container: _, // Not sent to leader.
        connector_checkpoint,
    } = recv_opened(shard_rx, &task.peers)
        .await
        .context("receiving Opened fan-in")?;
    let connector_checkpoint = connector_checkpoint.unwrap_or_default();

    // Build sorted index on journal_read_suffix => binding index, for frontier mapping.
    let mut journal_read_suffix_index: Vec<(&str, usize)> = task
        .binding_journal_read_suffixes
        .iter()
        .enumerate()
        .map(|(i, b)| (b.as_str(), i))
        .collect();
    journal_read_suffix_index.sort();

    // Converge recovered RocksDB state against any authoritative checkpoint.
    // Session startup state is then a projection of the actually-scanned
    // durable state.
    let scanned = Baseline {
        committed_close,
        committed_frontier,
        hinted_close,
        hinted_frontier,
        ack_intents: pending_ack_intents,
        legacy_checkpoint,
    };
    let mut connector_adopted = false;
    let (_, scanned) = super::super::reconcile_loop(
        (&mut shard_rx[0], &shard_tx[0], task.peers[0].as_str()),
        scanned,
        |scanned| {
            reconcile(
                scanned,
                &connector_checkpoint,
                &mut connector_adopted,
                drop_v1_rollback,
                &journal_read_suffix_index,
            )
        },
        |(rx, tx, peer), persist| async move {
            let scanned = send_rescan_persist(rx, tx, peer, persist).await?;
            Ok(((rx, tx, peer), scanned))
        },
    )
    .await
    .context("startup reconciliation")?;

    let (
        resume_frontier,
        idempotent_replay,
        committed_close,
        committed_frontier,
        pending_ack_intents,
    ) = scanned.into_projected_parts();

    // Open the shuffle Session with the recovered resume Frontier.
    let shuffle_task = shuffle::proto::Task {
        task: Some(shuffle::proto::task::Task::Materialization(spec)),
    };
    let session = service
        .shuffle_factory
        .open(shuffle_task, shard_shuffles, resume_frontier)
        .await
        .context("opening shuffle Session")?;

    Ok(Startup {
        committed_close,
        committed_frontier,
        idempotent_replay,
        logger,
        pending_ack_intents,
        pending_trigger_params,
        publisher,
        session,
        task,
    })
}

/// RocksDB state that startup reconciliation reads and converges toward an
/// authoritative checkpoint. Field are [`proto::Recover`] fields that the
/// reconciliation policy may change (the remaining `Recover` fields are inert
/// to reconciliation and threaded-through untouched).
#[derive(Clone, Debug)]
struct Baseline {
    /// Clock at which the last-committed transaction closed.
    committed_close: uuid::Clock,
    /// Committed Frontier (`FC:`).
    committed_frontier: shuffle::Frontier,
    /// Clock at which the last hinted transaction closed. Adoption steps
    /// overwrite it with the adopted close Clock (a `Persist` cannot clear it,
    /// and a hinted close equal to committed-close is inert); the fold step
    /// leaves it in place, already equal to the folded committed close.
    hinted_close: uuid::Clock,
    /// Hinted Frontier (`FH:`). A hint must survive a crash during idempotent
    /// replay, so it's preserved unless a checkpoint adoption discards it.
    hinted_frontier: shuffle::Frontier,
    /// Last-persisted ACK intents (`AI:`).
    ack_intents: BTreeMap<String, Bytes>,
    /// Legacy V1-rollback checkpoint, or None when absent / to be deleted.
    legacy_checkpoint: Option<consumer::Checkpoint>,
}

impl Baseline {
    /// Decode a recovered [`proto::Recover`] into its [`Baseline`].
    fn from_recover(recover: proto::Recover) -> anyhow::Result<Baseline> {
        let proto::Recover {
            committed_close_clock,
            committed_frontier,
            hinted_close_clock,
            hinted_frontier,
            ack_intents,
            legacy_checkpoint,
            last_applied: _,
            connector_state_json: _,
            max_keys: _,
            trigger_params_json: _,
        } = recover;

        let baseline = Baseline {
            committed_close: uuid::Clock::from_u64(committed_close_clock),
            committed_frontier: shuffle::Frontier::decode(committed_frontier.unwrap_or_default())
                .context("validating committed Frontier")?,
            hinted_close: uuid::Clock::from_u64(hinted_close_clock),
            hinted_frontier: shuffle::Frontier::decode(hinted_frontier.unwrap_or_default())
                .context("validating hinted Frontier")?,
            ack_intents,
            legacy_checkpoint,
        };
        Ok(baseline)
    }

    /// Consume this (converged) durable `Baseline` into exactly the parts that
    /// start a session: the shuffle resume Frontier and its idempotent-replay
    /// flag, plus the committed close/frontier and ACK intents threaded into
    /// `Startup`.
    fn into_projected_parts(
        self,
    ) -> (
        shuffle::Frontier,
        bool,
        uuid::Clock,
        shuffle::Frontier,
        BTreeMap<String, Bytes>,
    ) {
        let Baseline {
            committed_close,
            committed_frontier,
            hinted_frontier,
            ack_intents,
            ..
        } = self;
        let (resume_frontier, idempotent_replay) =
            Self::resume_frontier(hinted_frontier, committed_frontier.clone());
        (
            resume_frontier,
            idempotent_replay,
            committed_close,
            committed_frontier,
            ack_intents,
        )
    }

    /// Project a hinted + committed Frontier pair into the resume Frontier,
    /// and whether the first transaction is an idempotent replay.
    fn resume_frontier(
        hinted: shuffle::Frontier,
        committed: shuffle::Frontier,
    ) -> (shuffle::Frontier, bool) {
        let resume_frontier = frontier_mapping::project_hinted(hinted).reduce(committed);
        let idempotent_replay = resume_frontier.unresolved_hints != 0;
        (resume_frontier, idempotent_replay)
    }

    #[cfg(test)]
    fn session_state(&self) -> (shuffle::Frontier, bool) {
        Self::resume_frontier(
            self.hinted_frontier.clone(),
            self.committed_frontier.clone(),
        )
    }
}

/// Reconcile a recovered `scanned` [`Baseline`] toward its authoritative
/// checkpoints — a `legacy_checkpoint` carried within `scanned` (V1 rollback
/// migration) or a remote-authoritative `connector_checkpoint` from C:Opened —
/// as an ordered sequence of self-clearing steps. Each step tests an explicit
/// trigger over the scanned state and returns the incremental `Persist`
/// that clears it; `None` means no trigger fires and `scanned` is the
/// reconciled fixed point.
///
/// `connector_adopted` latches the one step whose trigger (a connector
/// checkpoint without an embedded close Clock) lives in the endpoint, where
/// no Persist can clear it: that step instead fires eagerly, at most once per
/// startup.
///
/// No IO, so the policy is unit-testable in isolation from the leader's
/// shard-protocol plumbing. `journal_read_suffix_index` maps
/// `journal_read_suffix` => binding index and must be sorted on the suffix.
/// The caller stamps `seq_no`/`rescan`.
///
/// See also: [`crate::leader::derive::startup::reconcile`].
fn reconcile(
    scanned: &Baseline,
    connector_checkpoint: &consumer::Checkpoint,
    connector_adopted: &mut bool,
    drop_v1_rollback: bool,
    journal_read_suffix_index: &[(&str, usize)],
) -> anyhow::Result<Option<proto::Persist>> {
    const FLOOR: uuid::Clock = frontier_mapping::COMMITTED_CLOSE_FLOOR;

    // Step: convert a V1-written legacy checkpoint into the V2 baseline.
    // Only the V1 runtime writes checkpoints without an embedded committed-close
    // Clock — every V2 commit re-embeds it — so a marker-less legacy checkpoint
    // means V1 wrote last (a fresh V1 → V2 migration, or a return from V1
    // rollback) and is authoritative. Adoption discards V2 state of any
    // abandoned timeline: stale `FC:`/`FH:` entries, and a `committed_close`
    // that regresses to the FLOOR seed. Self-clearing: the refreshed (or
    // deleted) legacy checkpoint carries the embedded FLOOR Clock.
    if let Some(legacy) = &scanned.legacy_checkpoint {
        if frontier_mapping::extract_committed_close(legacy).is_none() {
            service_kit::event!(
                tracing::Level::INFO,
                "leader",
                committed_close = scanned.committed_close,
                "legacy checkpoint has no committed-close Clock (V1 wrote it last); converting to the V2 baseline",
            );
            let persist = frontier_mapping::adopt_checkpoint(
                legacy,
                FLOOR,
                !drop_v1_rollback,
                true, // Discard hinted state.
                journal_read_suffix_index,
            )
            .context("converting legacy checkpoint into the V2 baseline")?;
            return Ok(Some(persist));
        }
    }

    // A legacy checkpoint reaching this point was V2-written (conversion above
    // would otherwise have fired), and a V2-written legacy checkpoint persists
    // atomically with committed-close: a mismatch is an implementation error.
    if let Some(legacy) = &scanned.legacy_checkpoint {
        let clock = frontier_mapping::extract_committed_close(legacy);
        if clock != Some(scanned.committed_close) {
            anyhow::bail!(
                "legacy_checkpoint has clock {clock:?} that doesn't match committed_close ({:?})",
                scanned.committed_close,
            );
        }
    }

    // Step: dropping V1 rollback deletes the (in-sync) legacy checkpoint.
    // Self-clearing: the next scan recovers no legacy checkpoint.
    if drop_v1_rollback && scanned.legacy_checkpoint.is_some() {
        service_kit::event!(
            tracing::Level::INFO,
            "leader",
            "dropping V1 rollback support; deleting the legacy checkpoint",
        );
        return Ok(Some(proto::Persist {
            delete_legacy_checkpoint: true,
            ..Default::default()
        }));
    }

    // Steps testing the connector checkpoint of a remote-authoritative endpoint.
    if connector_checkpoint.sources.is_empty() {
        return Ok(None);
    }

    match frontier_mapping::extract_committed_close(connector_checkpoint) {
        // In sync with the last commit: nothing to reconcile.
        Some(clock) if clock == scanned.committed_close => {}

        // Step: the connector declares the hinted txn did in fact commit. This
        // is the crashed session's Persist(commit), reconstructed from recovered
        // inputs: advance committed_close to hinted_close, write the hinted
        // delta as the committed Frontier — byte-identical to what the crashed
        // commit would have written (its extents Frontier is exactly this
        // `FH:`) — adopt the connector's ACK intents, and leave the `FH:` hint
        // in place (recovery treats a hint covered by committed as resolved,
        // matching a normal commit persist). When rollback is maintained the
        // connector checkpoint IS the full merged legacy-format checkpoint, so
        // it refreshes the legacy checkpoint verbatim. Self-clearing:
        // committed_close advances to the connector's Clock.
        Some(clock) if clock == scanned.hinted_close => {
            service_kit::event!(
                tracing::Level::INFO,
                "leader",
                committed_close = scanned.committed_close,
                hinted_close = scanned.hinted_close,
                "connector checkpoint matches hinted_close; folding the hinted delta as committed",
            );
            return Ok(Some(proto::Persist {
                committed_close_clock: clock.as_u64(),
                committed_frontier: Some(shuffle::JournalFrontier::encode(
                    &scanned.hinted_frontier.journals,
                )),
                delete_ack_intents: true,
                ack_intents: connector_checkpoint.ack_intents.clone(),
                legacy_checkpoint: (!drop_v1_rollback).then(|| connector_checkpoint.clone()),
                ..Default::default()
            }));
        }

        // Implementation error: a marker written at StartCommit implies its
        // hint persisted first (Persist(hint) strictly precedes StartCommit),
        // and only a later commit persist lets hinted_close advance past it —
        // so the marker always matches one of the two Clocks. Conversion can't
        // discard the hint out from under a live marker, either: it persists
        // at startup before the session's first StartCommit, and a V1 rollback
        // that re-arms it (by stripping the legacy marker) commits to the
        // endpoint first, stripping this marker too. A mismatch therefore
        // means state written outside these histories — e.g. a pre-FLOOR
        // migration crash — and the remediation is a brief V1 rollback (which
        // strips both markers) before re-migrating.
        Some(clock) => anyhow::bail!(
            "connector_checkpoint has clock {clock:?} which doesn't match committed_close \
             ({:?}) or hinted_close ({:?})",
            scanned.committed_close,
            scanned.hinted_close,
        ),

        // Step: no embedded close Clock, so the V1 runtime wrote the endpoint
        // checkpoint last (a fresh V1 → V2 migration, or a V1 rollback that ran
        // against the endpoint) and it's authoritative. No Persist can clear
        // this trigger — only StartCommit writes to the endpoint — so instead
        // adopt eagerly, at most once per startup: a redundant adoption is
        // idempotent, while a V1 rollback that advanced the endpoint during the
        // FLOOR epoch is adopted rather than silently skipped. The epoch ends
        // at the first V2 commit, which embeds its close Clock.
        None if !*connector_adopted => {
            *connector_adopted = true;
            service_kit::event!(
                tracing::Level::INFO,
                "leader",
                committed_close = scanned.committed_close,
                "connector checkpoint has no committed-close Clock (V1 wrote it last); adopting it",
            );
            let persist = frontier_mapping::adopt_checkpoint(
                connector_checkpoint,
                FLOOR,
                !drop_v1_rollback,
                true, // Discard hinted state.
                journal_read_suffix_index,
            )
            .context("adopting connector checkpoint")?;
            return Ok(Some(persist));
        }

        // Already adopted this startup.
        None => {}
    }

    Ok(None)
}

/// Send a rescan `Persist` to a shard and await the fresh `Recover` it scans in
/// reply, decoded into a [`Baseline`].
async fn send_rescan_persist(
    rx: &mut BoxStream<'static, tonic::Result<proto::Materialize>>,
    tx: &mpsc::UnboundedSender<tonic::Result<proto::Materialize>>,
    peer: &str,
    persist: proto::Persist,
) -> anyhow::Result<Baseline> {
    let verify = crate::verify("Materialize", "Recover", peer);

    // Sends are best-effort: a closed peer surfaces on the next `rx`.
    let _ = tx.send(Ok(proto::Materialize {
        persist: Some(persist),
        ..Default::default()
    }));

    match verify.not_eof(rx.next().await)? {
        proto::Materialize {
            recover: Some(recover),
            ..
        } => Baseline::from_recover(recover),
        other => Err(verify.fail_msg(other)),
    }
}

async fn recv_recovers(
    request_rxs: &mut [BoxStream<'static, tonic::Result<proto::Materialize>>],
    peers: &[String],
) -> anyhow::Result<proto::Recover> {
    let mut recovers = futures::future::try_join_all(request_rxs.into_iter().enumerate().map(
        |(shard_index, rx)| async move {
            let verify = crate::verify("Materialize", "Recover", &peers[shard_index]);
            match verify.not_eof(rx.next().await)? {
                proto::Materialize {
                    recover: Some(recover),
                    ..
                } if shard_index == 0 || recover == proto::Recover::default() => {
                    Ok::<_, anyhow::Error>(recover)
                }
                other => Err(verify.fail_msg(other)),
            }
        },
    ))
    .await?;

    Ok(recovers.swap_remove(0))
}

/// Send a `Persist` to a shard and await the matching `Persisted` echo.
async fn send_persist(
    rx: &mut BoxStream<'static, tonic::Result<proto::Materialize>>,
    tx: &mpsc::UnboundedSender<tonic::Result<proto::Materialize>>,
    peer: &str,
    persist: proto::Persist,
) -> anyhow::Result<()> {
    let verify = crate::verify("Materialize", "Persisted", peer);
    let seq_no = persist.seq_no;

    // Sends are best-effort: a closed peer surfaces on the next `rx`.
    let _ = tx.send(Ok(proto::Materialize {
        persist: Some(persist),
        ..Default::default()
    }));

    match verify.not_eof(rx.next().await)? {
        proto::Materialize {
            persisted: Some(proto::Persisted { seq_no: got }),
            ..
        } if got == seq_no => Ok(()),
        other => Err(verify.fail_msg(other)),
    }
}

// The apply loop's persistent state machine is `(last_applied,
// connector_state_json)`. Each iteration may persist new connector state
// patches; `last_applied` is bumped only on the FINAL iteration once the
// connector returns no further patches. A crash mid-loop therefore resumes
// with the OLD `last_applied` against the partially-advanced state,
// requiring the connector's Apply to be idempotent across repeated
// invocations of the same target spec.
async fn apply_loop<L: crate::Logger>(
    rx: &mut BoxStream<'static, tonic::Result<proto::Materialize>>,
    tx: &mpsc::UnboundedSender<tonic::Result<proto::Materialize>>,
    peer: &str,
    last_applied: &Bytes,
    next_applied: &Bytes,
    next_version: &str,
    connector_state_json: &mut Bytes,
    logger: &L,
) -> anyhow::Result<()> {
    let verify_applied = crate::verify("Materialize", "Applied", peer);
    let last_version = if last_applied.is_empty() {
        String::new()
    } else {
        let last_spec = flow::MaterializationSpec::decode(last_applied.as_ref())
            .context("invalid recovered last-applied spec")?;
        labels_build_for(&last_spec)
    };

    const MAX_APPLY_ITERATIONS: u64 = 3;

    for iteration in 1..=MAX_APPLY_ITERATIONS {
        // Send Apply carrying the current reduced connector state.
        // Sends are best-effort: a closed peer will surface on the next rx.
        let _ = tx.send(Ok(proto::Materialize {
            apply: Some(proto::Apply {
                spec: next_applied.clone(),
                version: next_version.to_string(),
                last_spec: last_applied.clone(),
                last_version: last_version.clone(),
                connector_state_json: connector_state_json.clone(),
            }),
            ..Default::default()
        }));

        // Receive Applied.
        let applied_patches_json = match verify_applied.not_eof(rx.next().await)? {
            proto::Materialize {
                applied:
                    Some(proto::Applied {
                        action_description,
                        connector_patches_json,
                    }),
                ..
            } => {
                logger.event(crate::LogEvent::Applied {
                    action_description: &action_description,
                });

                service_kit::event!(
                    tracing::Level::INFO,
                    "leader",
                    iteration,
                    action_description,
                    patches = service_kit::event::debug(connector_patches_json.clone()),
                    "connector Apply completed",
                );
                connector_patches_json
            }
            other => return Err(verify_applied.fail_msg(other)),
        };

        if applied_patches_json.is_empty() {
            service_kit::event!(
                tracing::Level::DEBUG,
                "leader",
                iteration,
                "apply loop complete",
            );

            if last_applied == next_applied {
                return Ok(());
            }

            send_persist(
                rx,
                tx,
                peer,
                proto::Persist {
                    seq_no: iteration,
                    last_applied: next_applied.clone(),
                    ..Default::default()
                },
            )
            .await?;

            return Ok(());
        }

        // Fold the iteration's patches into the running reduced state so
        // subsequent Apply iterations and Open observe the newly-applied state.
        *connector_state_json =
            crate::patches::apply_state_patches(connector_state_json, &applied_patches_json)?;

        // Persist the iteration's patches to shard zero, observing the delta.
        let persist = proto::Persist {
            seq_no: iteration, // End-of-sequence.
            connector_patches_json: applied_patches_json,
            ..Default::default()
        };
        logger.event(crate::LogEvent::Persist { persist: &persist });
        send_persist(rx, tx, peer, persist).await?;
    }

    anyhow::bail!(
        "apply loop did not converge after {MAX_APPLY_ITERATIONS} iterations; \
         connector continues to return state patches"
    );
}

fn labels_build_for(spec: &flow::MaterializationSpec) -> String {
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

async fn recv_opened(
    request_rxs: &mut [BoxStream<'static, tonic::Result<proto::Materialize>>],
    peers: &[String],
) -> anyhow::Result<proto::materialize::Opened> {
    let mut openeds = futures::future::try_join_all(request_rxs.iter_mut().enumerate().map(
        |(shard_index, rx)| async move {
            let verify = crate::verify("Materialize", "Opened", &peers[shard_index]);
            match verify.not_eof(rx.next().await)? {
                proto::Materialize {
                    opened: Some(opened),
                    ..
                } if shard_index == 0 || opened == proto::materialize::Opened::default() => {
                    Ok::<_, anyhow::Error>(opened)
                }
                other => Err(verify.fail_msg(other)),
            }
        },
    ))
    .await?;

    Ok(openeds.swap_remove(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_stream::wrappers::UnboundedReceiverStream;

    // Build a peer/leader pair of channels, returning the BoxStream
    // the apply_loop would read from and the receiver of leader-side
    // sends, plus a peer-side sender used to inject responses.
    fn channel_pair() -> (
        BoxStream<'static, tonic::Result<proto::Materialize>>,
        mpsc::UnboundedSender<tonic::Result<proto::Materialize>>,
        mpsc::UnboundedSender<tonic::Result<proto::Materialize>>,
        mpsc::UnboundedReceiver<tonic::Result<proto::Materialize>>,
    ) {
        let (peer_tx, peer_rx) = mpsc::unbounded_channel();
        let (leader_tx, leader_rx) = mpsc::unbounded_channel();
        let stream = UnboundedReceiverStream::new(peer_rx).boxed();
        (stream, peer_tx, leader_tx, leader_rx)
    }

    fn applied(patches: &'static [u8]) -> proto::Materialize {
        proto::Materialize {
            applied: Some(proto::Applied {
                action_description: String::new(),
                connector_patches_json: Bytes::from_static(patches),
            }),
            ..Default::default()
        }
    }

    fn persisted(seq_no: u64) -> proto::Materialize {
        proto::Materialize {
            persisted: Some(proto::Persisted { seq_no }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn apply_loop_no_op_when_last_eq_next_and_no_patches() {
        // When last_applied == next_applied and the connector returns no
        // patches, we issue a single Apply and return without Persist.
        let (mut rx, peer_tx, leader_tx, mut leader_rx) = channel_pair();
        peer_tx.send(Ok(applied(b""))).unwrap();

        let same = Bytes::new();
        let mut state = Bytes::from_static(b"{\"k\":1}");
        apply_loop(
            &mut rx,
            &leader_tx,
            "p",
            &same,
            &same,
            "v1",
            &mut state,
            &crate::TracingLogger,
        )
        .await
        .unwrap();

        let m = leader_rx.try_recv().unwrap().unwrap();
        let apply = m.apply.expect("Apply was sent");
        assert!(apply.last_spec.is_empty());
        assert!(apply.spec.is_empty());
        assert_eq!(apply.version, "v1");
        // No Persist since spec is unchanged.
        assert!(leader_rx.try_recv().is_err());
        // State is unchanged.
        assert_eq!(state.as_ref(), b"{\"k\":1}");
    }

    #[tokio::test]
    async fn apply_loop_persists_last_applied_when_no_patches_but_spec_changed() {
        // No patches but next != last: loop sends Apply, then Persist
        // marking next_applied as the new last_applied with matching seq_no.
        let (mut rx, peer_tx, leader_tx, mut leader_rx) = channel_pair();
        peer_tx.send(Ok(applied(b""))).unwrap();
        peer_tx.send(Ok(persisted(1))).unwrap();

        let last = Bytes::new();
        let next = Bytes::from_static(b"new-spec-bytes");
        let mut state = Bytes::from_static(b"{}");
        apply_loop(
            &mut rx,
            &leader_tx,
            "p",
            &last,
            &next,
            "v2",
            &mut state,
            &crate::TracingLogger,
        )
        .await
        .unwrap();

        let m1 = leader_rx.try_recv().unwrap().unwrap();
        let apply = m1.apply.unwrap();
        assert_eq!(apply.spec, next);
        assert!(apply.last_spec.is_empty());
        assert_eq!(apply.last_version, "");

        let m2 = leader_rx.try_recv().unwrap().unwrap();
        let p = m2.persist.unwrap();
        assert_eq!(p.seq_no, 1);
        assert_eq!(p.last_applied, next);
        assert!(p.connector_patches_json.is_empty());

        assert!(leader_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn apply_loop_accumulates_patches_across_iterations() {
        // Initial state has nested objects, a key the patches will overwrite,
        // and a key the patches will delete (RFC 7396 null sentinel). Across
        // two patching iterations we should observe: deep merge of `nested`,
        // replacement of `keep`, deletion of `drop`, and addition of `added`.
        let (mut rx, peer_tx, leader_tx, mut leader_rx) = channel_pair();

        let patch1 = b"[{\"nested\":{\"a\":1},\"keep\":\"v1\"}\t]";
        let patch2 = b"[{\"nested\":{\"b\":2},\"keep\":\"v2\",\"drop\":null,\"added\":true}\t]";
        peer_tx.send(Ok(applied(patch1))).unwrap();
        peer_tx.send(Ok(persisted(1))).unwrap();
        peer_tx.send(Ok(applied(patch2))).unwrap();
        peer_tx.send(Ok(persisted(2))).unwrap();
        peer_tx.send(Ok(applied(b""))).unwrap();
        peer_tx.send(Ok(persisted(3))).unwrap();

        let last = Bytes::new();
        let next = Bytes::from_static(b"spec");
        let mut state = Bytes::from_static(br#"{"nested":{"a":0},"keep":"v0","drop":"x"}"#);
        apply_loop(
            &mut rx,
            &leader_tx,
            "p",
            &last,
            &next,
            "v2",
            &mut state,
            &crate::TracingLogger,
        )
        .await
        .unwrap();

        // Apply (iter 1) — connector observes the original state.
        let apply1 = leader_rx.try_recv().unwrap().unwrap().apply.unwrap();
        let s1: serde_json::Value = serde_json::from_slice(&apply1.connector_state_json).unwrap();
        assert_eq!(
            s1,
            serde_json::json!({"nested":{"a":0},"keep":"v0","drop":"x"})
        );
        // Persist iter 1 carries the connector's patches but no last_applied.
        let p1 = leader_rx.try_recv().unwrap().unwrap().persist.unwrap();
        assert_eq!(p1.seq_no, 1);
        assert!(p1.last_applied.is_empty());
        assert_eq!(p1.connector_patches_json.as_ref(), patch1);

        // Apply (iter 2) — `keep` replaced, `nested.a` retained from iter 1.
        let apply2 = leader_rx.try_recv().unwrap().unwrap().apply.unwrap();
        let s2: serde_json::Value = serde_json::from_slice(&apply2.connector_state_json).unwrap();
        assert_eq!(
            s2,
            serde_json::json!({"nested":{"a":1},"keep":"v1","drop":"x"}),
        );
        let p2 = leader_rx.try_recv().unwrap().unwrap().persist.unwrap();
        assert_eq!(p2.seq_no, 2);
        assert!(p2.last_applied.is_empty());
        assert_eq!(p2.connector_patches_json.as_ref(), patch2);

        // Apply (iter 3) — `drop` removed via null, `nested` deep-merged, `added` added.
        let apply3 = leader_rx.try_recv().unwrap().unwrap().apply.unwrap();
        let s3: serde_json::Value = serde_json::from_slice(&apply3.connector_state_json).unwrap();
        assert_eq!(
            s3,
            serde_json::json!({"nested":{"a":1,"b":2},"keep":"v2","added":true}),
        );
        // Final Persist promotes spec and carries no patches.
        let p3 = leader_rx.try_recv().unwrap().unwrap().persist.unwrap();
        assert_eq!(p3.seq_no, 3);
        assert_eq!(p3.last_applied, next);
        assert!(p3.connector_patches_json.is_empty());

        // Final reduced state escapes via &mut, observable to caller.
        let final_state: serde_json::Value = serde_json::from_slice(&state).unwrap();
        assert_eq!(
            final_state,
            serde_json::json!({"nested":{"a":1,"b":2},"keep":"v2","added":true}),
        );
    }

    #[tokio::test]
    async fn apply_loop_error_paths() {
        struct Case {
            name: &'static str,
            // Build the peer-side responses; closure receives the peer tx.
            seed: fn(&mpsc::UnboundedSender<tonic::Result<proto::Materialize>>),
            expect: &'static str,
        }
        let cases = [
            Case {
                // Connector returns patches forever; we cap at MAX_APPLY_ITERATIONS.
                name: "no_convergence",
                seed: |tx| {
                    for seq_no in 1..=4 {
                        tx.send(Ok(applied(b"[{\"x\":1}\t]"))).unwrap();
                        tx.send(Ok(persisted(seq_no))).unwrap();
                    }
                },
                expect: "did not converge",
            },
            Case {
                // Peer returns Persisted with a wrong seq_no — protocol error.
                name: "persisted_seq_no_mismatch",
                seed: |tx| {
                    tx.send(Ok(applied(b"[{\"x\":1}\t]"))).unwrap();
                    tx.send(Ok(persisted(99))).unwrap();
                },
                expect: "expected Persisted",
            },
            Case {
                // Peer sends a non-Applied message in response to Apply.
                name: "unexpected_message_kind",
                seed: |tx| {
                    tx.send(Ok(proto::Materialize {
                        opened: Some(proto::materialize::Opened::default()),
                        ..Default::default()
                    }))
                    .unwrap();
                },
                expect: "expected Applied",
            },
            Case {
                // Peer closes the stream without sending Applied — surfaces as EOF.
                name: "eof",
                seed: |_tx| {},
                expect: "unexpected EOF",
            },
        ];

        for case in cases {
            let (mut rx, peer_tx, leader_tx, _leader_rx) = channel_pair();
            (case.seed)(&peer_tx);
            drop(peer_tx);

            let last = Bytes::new();
            let next = Bytes::from_static(b"spec");
            let mut state = Bytes::from_static(b"{}");
            let err = apply_loop(
                &mut rx,
                &leader_tx,
                "p",
                &last,
                &next,
                "v2",
                &mut state,
                &crate::TracingLogger,
            )
            .await
            .unwrap_err();
            let s = format!("{err:?}");
            assert!(
                s.contains(case.expect),
                "{}: missing {:?} in {s}",
                case.name,
                case.expect,
            );
        }
    }

    fn make_streams(
        per_shard: Vec<Vec<tonic::Result<proto::Materialize>>>,
    ) -> Vec<BoxStream<'static, tonic::Result<proto::Materialize>>> {
        per_shard
            .into_iter()
            .map(|msgs| {
                let (tx, rx) = mpsc::unbounded_channel();
                for m in msgs {
                    tx.send(m).unwrap();
                }
                drop(tx);
                UnboundedReceiverStream::new(rx).boxed()
            })
            .collect()
    }

    fn recover_msg(recover: proto::Recover) -> tonic::Result<proto::Materialize> {
        Ok(proto::Materialize {
            recover: Some(recover),
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn recv_recovers_returns_shard_zero_value() {
        let zero = proto::Recover {
            committed_close_clock: 42,
            ..Default::default()
        };
        let mut streams = make_streams(vec![
            vec![recover_msg(zero.clone())],
            vec![recover_msg(proto::Recover::default())],
            vec![recover_msg(proto::Recover::default())],
        ]);
        let peers = vec!["s0".into(), "s1".into(), "s2".into()];
        let got = recv_recovers(&mut streams, &peers).await.unwrap();
        assert_eq!(got, zero);
    }

    #[tokio::test]
    async fn recv_recovers_error_paths() {
        let cases = [
            (
                "non_default_from_non_zero_shard",
                vec![
                    vec![recover_msg(proto::Recover::default())],
                    vec![recover_msg(proto::Recover {
                        committed_close_clock: 1,
                        ..Default::default()
                    })],
                ],
                vec!["expected Recover", "from s1"],
            ),
            (
                "wrong_message_kind",
                vec![vec![Ok(proto::Materialize {
                    opened: Some(proto::materialize::Opened::default()),
                    ..Default::default()
                })]],
                vec!["expected Recover"],
            ),
            ("eof", vec![vec![]], vec!["unexpected EOF"]),
        ];

        for (name, per_shard, needles) in cases {
            let mut streams = make_streams(per_shard);
            let peers: Vec<String> = (0..streams.len()).map(|i| format!("s{i}")).collect();
            let err = recv_recovers(&mut streams, &peers).await.unwrap_err();
            let s = format!("{err:?}");
            for n in needles {
                assert!(s.contains(n), "{name}: missing {n:?} in {s}");
            }
        }
    }

    #[tokio::test]
    async fn recv_opened_returns_shard_zero_value_and_rejects_others() {
        let zero = proto::materialize::Opened {
            container: None,
            connector_checkpoint: Some(proto_gazette::consumer::Checkpoint::default()),
        };
        let mut streams = make_streams(vec![
            vec![Ok(proto::Materialize {
                opened: Some(zero.clone()),
                ..Default::default()
            })],
            vec![Ok(proto::Materialize {
                opened: Some(proto::materialize::Opened::default()),
                ..Default::default()
            })],
        ]);
        let peers = vec!["s0".into(), "s1".into()];
        let got = recv_opened(&mut streams, &peers).await.unwrap();
        assert_eq!(got, zero);

        // Now a non-zero shard sends a populated Opened — error.
        let mut streams = make_streams(vec![
            vec![Ok(proto::Materialize {
                opened: Some(proto::materialize::Opened::default()),
                ..Default::default()
            })],
            vec![Ok(proto::Materialize {
                opened: Some(proto::materialize::Opened {
                    container: None,
                    connector_checkpoint: Some(proto_gazette::consumer::Checkpoint::default()),
                }),
                ..Default::default()
            })],
        ]);
        let err = recv_opened(&mut streams, &peers).await.unwrap_err();
        let s = format!("{err:?}");
        assert!(s.contains("expected Opened"));
        assert!(s.contains("from s1"));
    }

    // ---- reconcile ----

    use crate::leader::fixtures::{
        authoritative_checkpoint, clk, close_only_checkpoint, frontier, pf, producer_tags,
    };

    const FLOOR: uuid::Clock = frontier_mapping::COMMITTED_CLOSE_FLOOR;

    /// Invoke `reconcile` as one fresh startup pass (an unset adoption latch).
    fn reconcile_once(
        scanned: &Baseline,
        connector_checkpoint: &consumer::Checkpoint,
        drop_v1_rollback: bool,
        index: &[(&str, usize)],
    ) -> anyhow::Result<Option<proto::Persist>> {
        let mut connector_adopted = false;
        reconcile(
            scanned,
            connector_checkpoint,
            &mut connector_adopted,
            drop_v1_rollback,
            index,
        )
    }

    /// With no adoption, a hinted producer that HAS a committed baseline
    /// is preserved and replayed from its committed offset — never offset 0.
    #[test]
    fn reconcile_no_adoption_preserves_hints() {
        let index = [("s-a", 0usize)];
        let committed = frontier(
            "j/one",
            0,
            vec![pf(0xbb, clk(50), uuid::Clock::zero(), -500)],
        );
        let hinted = frontier(
            "j/one",
            0,
            vec![pf(0xbb, clk(200), uuid::Clock::zero(), -500)],
        );

        let scanned = baseline(clk(50), committed, clk(60), hinted, [], None);
        let persist =
            reconcile_once(&scanned, &consumer::Checkpoint::default(), false, &index).unwrap();

        // Nothing to reconcile on disk: `scanned` is already the fixed point, so
        // session state derives directly from it.
        assert!(persist.is_none());
        let (resume_frontier, idempotent_replay) = scanned.session_state();
        // Hint preserved -> unresolved -> replayed, but from the committed offset.
        assert!(idempotent_replay);
        assert_eq!(resume_frontier.unresolved_hints, 1);
        let p0 = &resume_frontier.journals[0].producers[0];
        assert_eq!(p0.offset, -500);
        assert!(p0.hinted_commit > p0.last_commit);
    }

    /// Fold with a maintained legacy (V1-rollback) checkpoint: the fold persist
    /// refreshes the legacy checkpoint verbatim from the connector checkpoint, so
    /// the next recovery's legacy consistency check sees the advanced committed
    /// close rather than the stale one.
    #[test]
    fn reconcile_fold_refreshes_maintained_legacy_checkpoint() {
        let index = [("s-a", 0usize)];
        let committed = frontier(
            "j/one",
            0,
            vec![pf(0xbb, clk(50), uuid::Clock::zero(), -500)],
        );
        let hinted = frontier(
            "j/one",
            0,
            vec![pf(0xbb, clk(200), uuid::Clock::zero(), -800)],
        );
        // In-sync legacy checkpoint at committed_close=50.
        let legacy = close_only_checkpoint(clk(50), "ack/legacy");
        // Connector checkpoint confirming the hinted commit at T200. When legacy
        // is maintained this IS the full merged legacy-format checkpoint.
        let connector = close_only_checkpoint(clk(200), "ack/c");

        let scanned = baseline(clk(50), committed, clk(200), hinted, [], Some(legacy));
        let p = reconcile_once(
            &scanned, &connector, false, // drop_v1_rollback = false: legacy is maintained
            &index,
        )
        .unwrap()
        .expect("fold produces a persist");

        assert_eq!(p.committed_close_clock, clk(200).as_u64());
        // Legacy checkpoint refreshed to the connector checkpoint verbatim.
        assert_eq!(p.legacy_checkpoint.as_ref(), Some(&connector));
    }

    /// Crash window #1 — post-fold-persist, pre-WriteIntents. Recovery observes
    /// the persisted advance (committed_close = T200, folded committed frontier,
    /// AI: = the connector's intents) plus the connector clock still at T200. The
    /// second reconcile pass is a fixed point (no persist) and surfaces the folded
    /// transaction's ACK intents, so WriteIntents ACKs the folded appends rather
    /// than recovering the prior transaction's stale intents.
    #[test]
    fn reconcile_fold_post_persist_is_fixed_point() {
        let index = [("s-a", 0usize)];
        // Post-fold committed frontier (0xbb folded to T200) and the retained hint.
        let committed = frontier(
            "j/one",
            0,
            vec![pf(0xbb, clk(200), uuid::Clock::zero(), -800)],
        );
        let hinted = frontier(
            "j/one",
            0,
            vec![pf(0xbb, clk(200), uuid::Clock::zero(), -800)],
        );
        let connector = close_only_checkpoint(clk(200), "ack/c");

        // AI: already holds the folded txn's connector intents.
        let scanned = baseline(
            clk(200),
            committed,
            clk(200),
            hinted,
            [("ack/c", b"C".as_slice())],
            None,
        );
        let persist = reconcile_once(&scanned, &connector, false, &index).unwrap();

        // Connector clock now matches committed_close: no further change.
        assert!(persist.is_none());
        // `scanned` is the fixed point. The folded transaction's ACK intents are
        // what the session will write, and no idempotent replay is needed.
        assert!(scanned.ack_intents.contains_key("ack/c"));
        let (_, idempotent_replay) = scanned.session_state();
        assert!(!idempotent_replay);
    }

    /// Crash window #2 — post-fold, then a next transaction persisted a fresh hint
    /// at T300 before crashing pre-commit. Recovery has committed_close = T200 and
    /// hinted_close = T300, while the connector checkpoint is still at T200. The
    /// connector clock must match committed_close (the "ignoring" branch) — NOT be
    /// mistaken for a fold against the new hinted_close — so reconcile does not bail.
    #[test]
    fn reconcile_fold_then_next_hint_matches_committed_close() {
        let index = [("s-a", 0usize)];
        let committed = frontier(
            "j/one",
            0,
            vec![pf(0xbb, clk(200), uuid::Clock::zero(), -800)],
        );
        // A newly-persisted hint at T300 (next txn crashed before commit).
        let hinted = frontier(
            "j/one",
            0,
            vec![pf(0xbb, clk(300), uuid::Clock::zero(), -1200)],
        );
        let connector = close_only_checkpoint(clk(200), "ack/c");

        let scanned = baseline(
            clk(200),
            committed,
            clk(300),
            hinted,
            [("ack/c", b"C".as_slice())],
            None,
        );
        // Must not bail: connector clock T200 matches committed_close T200.
        let persist = reconcile_once(&scanned, &connector, false, &index).unwrap();
        assert!(persist.is_none());
        // The new hint at T300 remains unresolved, so the session idempotently
        // replays it as its first transaction.
        let (_, idempotent_replay) = scanned.session_state();
        assert!(idempotent_replay);
    }

    /// The conversion step: a marker-less (V1-written) legacy checkpoint is
    /// adopted with `committed_close` seeded at the FLOOR, hinted state
    /// discarded, and the legacy checkpoint refreshed with the FLOOR embedded
    /// (self-clearing its trigger). A stale `committed_close` from an abandoned
    /// V2 timeline (a V1 rollback that later returns) regresses to the FLOOR.
    #[test]
    fn reconcile_converts_v1_legacy_checkpoint() {
        let index = [("s-a", 0usize)];
        let committed = frontier(
            "j/one",
            0,
            vec![pf(0xaa, clk(50), uuid::Clock::zero(), -100)],
        );
        let hinted = frontier(
            "j/one",
            0,
            vec![pf(0xaa, clk(80), uuid::Clock::zero(), -99_999)],
        );
        let legacy = authoritative_checkpoint("j/one", "s-a", 0xbb, clk(1000), 5000);

        let scanned = baseline(clk(50), committed, clk(80), hinted, [], Some(legacy));
        let p = reconcile_once(&scanned, &consumer::Checkpoint::default(), false, &index)
            .unwrap()
            .expect("conversion produces a persist");

        assert_eq!(p.committed_close_clock, FLOOR.as_u64());
        assert_eq!(p.hinted_close_clock, FLOOR.as_u64());
        assert!(p.delete_committed_frontier);
        assert!(p.delete_hinted_frontier);
        assert!(p.delete_ack_intents);
        assert!(p.ack_intents.contains_key("ack/j"));
        let rebuilt =
            shuffle::Frontier::decode(p.committed_frontier.clone().expect("frontier")).unwrap();
        assert_eq!(producer_tags(&rebuilt), vec![0xbb]);
        // The refreshed legacy checkpoint embeds the FLOOR, clearing the trigger.
        assert!(!p.delete_legacy_checkpoint);
        let refreshed = p.legacy_checkpoint.expect("legacy is maintained");
        assert_eq!(
            frontier_mapping::extract_committed_close(&refreshed),
            Some(FLOOR),
        );

        // Under drop_v1_rollback the same conversion deletes the legacy instead.
        let p = reconcile_once(&scanned, &consumer::Checkpoint::default(), true, &index)
            .unwrap()
            .expect("conversion produces a persist");
        assert!(p.delete_legacy_checkpoint);
        assert!(p.legacy_checkpoint.is_none());
    }

    /// A V2-written legacy checkpoint whose embedded Clock doesn't match
    /// `committed_close` is an implementation error (they persist atomically).
    #[test]
    fn reconcile_legacy_clock_mismatch_bails() {
        let index = [("s-a", 0usize)];
        let committed = frontier(
            "j/one",
            0,
            vec![pf(0xbb, clk(50), uuid::Clock::zero(), -500)],
        );
        let legacy = close_only_checkpoint(clk(99), "ack/legacy");

        let scanned = baseline(
            clk(50),
            committed.clone(),
            clk(50),
            committed,
            [],
            Some(legacy),
        );
        let err =
            reconcile_once(&scanned, &consumer::Checkpoint::default(), false, &index).unwrap_err();
        assert!(
            format!("{err:?}").contains("doesn't match committed_close"),
            "{err:?}"
        );
    }

    /// A connector checkpoint Clock matching neither committed_close nor
    /// hinted_close — outside the FLOOR adoption epoch — is an implementation
    /// error.
    #[test]
    fn reconcile_connector_clock_mismatch_bails() {
        let index = [("s-a", 0usize)];
        let committed = frontier(
            "j/one",
            0,
            vec![pf(0xbb, clk(50), uuid::Clock::zero(), -500)],
        );
        let connector = close_only_checkpoint(clk(300), "ack/c");

        let scanned = baseline(clk(50), committed.clone(), clk(200), committed, [], None);
        let err = reconcile_once(&scanned, &connector, false, &index).unwrap_err();
        assert!(
            format!("{err:?}").contains("doesn't match committed_close"),
            "{err:?}"
        );
    }

    /// Build a `Baseline` from parts, with ACK intents given as `(journal, bytes)`
    /// tuples for brevity.
    fn baseline<const N: usize>(
        committed_close: uuid::Clock,
        committed_frontier: shuffle::Frontier,
        hinted_close: uuid::Clock,
        hinted_frontier: shuffle::Frontier,
        ack_intents: [(&str, &[u8]); N],
        legacy_checkpoint: Option<consumer::Checkpoint>,
    ) -> Baseline {
        Baseline {
            committed_close,
            committed_frontier,
            hinted_close,
            hinted_frontier,
            ack_intents: ack_intents
                .into_iter()
                .map(|(k, v)| (k.to_string(), Bytes::copy_from_slice(v)))
                .collect(),
            legacy_checkpoint,
        }
    }

    /// Drive `reconcile_loop` against a real RocksDB, as `run` does but
    /// without the shard-protocol plumbing — one fresh startup, with its own
    /// adoption latch. Returns the converged, actually-scanned `Baseline` and
    /// the Persists that were applied. `state_keys` maps binding index =>
    /// state_key for both the persist encoder and scan decoder.
    async fn run_reconcile_loop(
        seed: proto::Persist,
        connector_checkpoint: consumer::Checkpoint,
        drop_v1_rollback: bool,
        index: &[(&str, usize)],
        state_keys: &[&str],
    ) -> anyhow::Result<(Baseline, Vec<proto::Persist>)> {
        let db = crate::shard::rocksdb::RocksDB::open(None).await.unwrap();
        let db = db.persist(&seed, state_keys).await.unwrap();
        let (db, recover) = db.scan(state_keys.iter().copied()).await.unwrap();
        let scanned = Baseline::from_recover(recover)?;

        let mut connector_adopted = false;
        let ((_, persists), converged) = crate::leader::reconcile_loop(
            (db, Vec::new()),
            scanned,
            |scanned| {
                reconcile(
                    scanned,
                    &connector_checkpoint,
                    &mut connector_adopted,
                    drop_v1_rollback,
                    index,
                )
            },
            |(db, mut persists), persist| async move {
                let db = db.persist(&persist, state_keys).await?;
                let (db, recover) = db.scan(state_keys.iter().copied()).await?;
                persists.push(persist);
                Ok(((db, persists), Baseline::from_recover(recover)?))
            },
        )
        .await?;
        Ok((converged, persists))
    }

    /// A remote-authoritative connector whose checkpoint clock matches
    /// hinted_close declares the hinted txn committed. The fold persists the
    /// reconstructed Persist(commit) — making the committed-close advance
    /// durable — and converges on the second pass via the "matches
    /// committed_close" branch.
    #[tokio::test]
    async fn reconcile_loop_fold_converges() {
        let index = [("s-a", 0usize)];
        let committed = frontier(
            "j/one",
            0,
            vec![pf(0xbb, clk(50), uuid::Clock::zero(), -500)],
        );
        let hinted = frontier(
            "j/one",
            0,
            vec![pf(0xbb, clk(200), uuid::Clock::zero(), -800)],
        );
        let seed = proto::Persist {
            committed_frontier: Some(shuffle::JournalFrontier::encode(&committed.journals)),
            hinted_frontier: Some(shuffle::JournalFrontier::encode(&hinted.journals)),
            committed_close_clock: clk(50).as_u64(),
            hinted_close_clock: clk(200).as_u64(),
            ..Default::default()
        };
        let connector = close_only_checkpoint(clk(200), "ack/c");

        let (converged, persists) =
            run_reconcile_loop(seed, connector.clone(), false, &index, &["sk-0"])
                .await
                .unwrap();

        // The one Persist is the reconstructed Persist(commit): committed_close
        // advances to hinted_close, the hinted delta is written as committed,
        // and the connector's ACK intents are adopted, with the resolved `FH:`
        // hint left in place. Rollback is maintained (!drop_v1_rollback), so
        // the legacy checkpoint is refreshed verbatim — just as a normal commit
        // persist would have written it.
        let [p] = persists.as_slice() else {
            panic!("expected one persist, got {persists:?}");
        };
        assert_eq!(p.committed_close_clock, clk(200).as_u64());
        assert!(p.delete_ack_intents);
        assert!(p.ack_intents.contains_key("ack/c"));
        let delta =
            shuffle::Frontier::decode(p.committed_frontier.clone().expect("delta")).unwrap();
        assert_eq!(delta.journals[0].producers[0].offset, -800);
        assert_eq!(delta.journals[0].producers[0].last_commit, clk(200));
        assert!(!p.delete_committed_frontier);
        assert!(!p.delete_hinted_frontier);
        assert_eq!(p.legacy_checkpoint.as_ref(), Some(&connector));

        // Full converged durable Baseline:
        assert_eq!(converged.committed_close, clk(200));
        // committed folded to 0xbb @ T200 / offset -800 (the hinted delta won).
        assert_eq!(producer_tags(&converged.committed_frontier), vec![0xbb]);
        let p0 = &converged.committed_frontier.journals[0].producers[0];
        assert_eq!(p0.last_commit, clk(200));
        assert_eq!(p0.offset, -800);
        // The `FH:` hint is left in place (resolved by the covering committed).
        assert_eq!(producer_tags(&converged.hinted_frontier), vec![0xbb]);
        assert_eq!(converged.hinted_close, clk(200));
        // ACK intents are the folded commit's; the refreshed legacy checkpoint
        // carries the folded close Clock, in sync with committed_close.
        assert!(converged.ack_intents.contains_key("ack/c"));
        assert_eq!(
            frontier_mapping::extract_committed_close(
                converged.legacy_checkpoint.as_ref().expect("maintained")
            ),
            Some(clk(200)),
        );
        // Session state: hint resolved, no idempotent replay.
        let (_, idempotent_replay) = converged.session_state();
        assert!(!idempotent_replay);
    }

    /// Conversion of a pure-V1 legacy checkpoint (no embedded committed-close
    /// clock): the committed frontier and ACK intents are rewritten from the
    /// checkpoint, the stale hinted frontier is discarded — an orphaned hint
    /// (its producer absent from the converted frontier, e.g. V1-pruned after
    /// >24h idle) would otherwise replay from offset 0 — both close Clocks are
    /// seeded at the FLOOR (regressing the stale V2 clocks of the abandoned
    /// timeline), and the legacy checkpoint is deleted (drop_v1_rollback).
    #[tokio::test]
    async fn reconcile_loop_converts_v1_legacy_and_drops_rollback() {
        let index = [("s-a", 0usize)];
        let committed = frontier(
            "j/one",
            0,
            vec![pf(0xaa, clk(50), uuid::Clock::zero(), -100)],
        );
        // Stale hint of producer 0xaa, absent from the legacy checkpoint.
        let hinted = frontier(
            "j/one",
            0,
            vec![pf(0xaa, clk(80), uuid::Clock::zero(), -99_999)],
        );
        let legacy = authoritative_checkpoint("j/one", "s-a", 0xbb, clk(1000), 5000);
        let seed = proto::Persist {
            committed_frontier: Some(shuffle::JournalFrontier::encode(&committed.journals)),
            hinted_frontier: Some(shuffle::JournalFrontier::encode(&hinted.journals)),
            committed_close_clock: clk(50).as_u64(),
            hinted_close_clock: clk(80).as_u64(),
            legacy_checkpoint: Some(legacy),
            ..Default::default()
        };

        let (converged, persists) = run_reconcile_loop(
            seed,
            consumer::Checkpoint::default(),
            true, // drop_v1_rollback
            &index,
            &["sk-0"],
        )
        .await
        .unwrap();

        // The one Persist clears both frontiers, rewrites committed + ACK
        // intents, seeds both close Clocks at the FLOOR, and drops the legacy
        // checkpoint.
        let [p] = persists.as_slice() else {
            panic!("expected one persist, got {persists:?}");
        };
        assert_eq!(p.committed_close_clock, FLOOR.as_u64());
        assert_eq!(p.hinted_close_clock, FLOOR.as_u64());
        assert!(p.delete_committed_frontier);
        assert!(p.delete_hinted_frontier);
        assert!(p.delete_legacy_checkpoint);
        assert!(p.delete_ack_intents);
        assert!(p.ack_intents.contains_key("ack/j"));
        let rebuilt =
            shuffle::Frontier::decode(p.committed_frontier.clone().expect("frontier")).unwrap();
        assert_eq!(producer_tags(&rebuilt), vec![0xbb]);
        assert_eq!(rebuilt.journals[0].producers[0].offset, -5000);

        // Full converged durable Baseline and its session projection.
        assert_eq!(converged.committed_close, FLOOR);
        assert_eq!(converged.hinted_close, FLOOR);
        assert_eq!(producer_tags(&converged.committed_frontier), vec![0xbb]);
        assert!(converged.hinted_frontier.journals.is_empty());
        assert!(converged.ack_intents.contains_key("ack/j"));
        assert!(converged.legacy_checkpoint.is_none());
        let (resume_frontier, idempotent_replay) = converged.session_state();
        assert!(!idempotent_replay);
        assert_eq!(resume_frontier.journals[0].producers[0].offset, -5000);
    }

    /// Adoption of a marker-less connector checkpoint (V1 wrote the endpoint
    /// last): both frontiers are cleared, committed + ACK intents are rewritten
    /// from the connector checkpoint, the close Clocks seed at the FLOOR, and a
    /// maintained legacy checkpoint is written from the connector with the
    /// FLOOR embedded. Because no Persist can clear the endpoint-side trigger,
    /// a following startup within the FLOOR epoch re-adopts — one idempotent
    /// persist — rather than trusting equality that a V1 rollback could break.
    #[tokio::test]
    async fn reconcile_loop_adopts_marker_less_connector_checkpoint() {
        let index = [("s-a", 0usize)];
        let committed = frontier(
            "j/one",
            0,
            vec![pf(0xaa, clk(50), uuid::Clock::zero(), -100)],
        );
        let hinted = frontier(
            "j/one",
            0,
            vec![pf(0xaa, clk(80), uuid::Clock::zero(), -99_999)],
        );
        let seed = proto::Persist {
            committed_frontier: Some(shuffle::JournalFrontier::encode(&committed.journals)),
            hinted_frontier: Some(shuffle::JournalFrontier::encode(&hinted.journals)),
            committed_close_clock: clk(50).as_u64(),
            hinted_close_clock: clk(80).as_u64(),
            ..Default::default()
        };
        let connector = authoritative_checkpoint("j/one", "s-a", 0xbb, clk(1000), 5000);

        let (converged, persists) =
            run_reconcile_loop(seed, connector.clone(), false, &index, &["sk-0"])
                .await
                .unwrap();

        // The one Persist clears both frontiers, rewrites committed + ACK
        // intents from the connector checkpoint, and seeds the FLOOR.
        let [p] = persists.as_slice() else {
            panic!("expected one persist, got {persists:?}");
        };
        assert_eq!(p.committed_close_clock, FLOOR.as_u64());
        assert_eq!(p.hinted_close_clock, FLOOR.as_u64());
        assert!(p.delete_committed_frontier);
        assert!(p.delete_hinted_frontier);
        assert!(!p.delete_legacy_checkpoint);
        assert!(p.delete_ack_intents);
        assert!(p.ack_intents.contains_key("ack/j"));
        let rebuilt =
            shuffle::Frontier::decode(p.committed_frontier.clone().expect("frontier")).unwrap();
        assert_eq!(producer_tags(&rebuilt), vec![0xbb]);
        assert_eq!(rebuilt.journals[0].producers[0].offset, -5000);

        // Full converged durable Baseline: FLOOR epoch, committed rebuilt to the
        // sole current producer, hinted discarded, ACKs adopted, and a
        // maintained legacy checkpoint embedding the FLOOR.
        assert_eq!(converged.committed_close, FLOOR);
        assert_eq!(converged.hinted_close, FLOOR);
        assert_eq!(producer_tags(&converged.committed_frontier), vec![0xbb]);
        assert_eq!(
            converged.committed_frontier.journals[0].producers[0].offset,
            -5000
        );
        assert!(converged.hinted_frontier.journals.is_empty());
        assert!(converged.ack_intents.contains_key("ack/j"));
        assert_eq!(
            frontier_mapping::extract_committed_close(
                converged.legacy_checkpoint.as_ref().expect("maintained")
            ),
            Some(FLOOR),
        );
        // The orphan 0xaa hint is gone, so no offset-0 re-read and no replay.
        let (resume_frontier, idempotent_replay) = converged.session_state();
        assert!(!idempotent_replay);
        assert_eq!(producer_tags(&resume_frontier), vec![0xbb]);
        assert_eq!(resume_frontier.journals[0].producers[0].offset, -5000);

        // A "second startup" — the adoption persist is now the seed — re-adopts
        // the still-marker-less connector checkpoint exactly once, idempotently.
        let (readopted, persists) =
            run_reconcile_loop(persists[0].clone(), connector, false, &index, &["sk-0"])
                .await
                .unwrap();
        assert_eq!(persists.len(), 1, "one idempotent re-adoption persist");
        assert_eq!(readopted.committed_close, FLOOR);
        assert_eq!(producer_tags(&readopted.committed_frontier), vec![0xbb]);
    }

    /// Convergence: dropping V1 rollback deletes only the legacy checkpoint and
    /// leaves everything else intact, converging on the second pass.
    #[tokio::test]
    async fn reconcile_loop_drop_rollback_converges() {
        let index = [("s-a", 0usize)];
        let committed = frontier(
            "j/one",
            0,
            vec![pf(0xbb, clk(50), uuid::Clock::zero(), -500)],
        );
        let hinted = frontier(
            "j/one",
            0,
            vec![pf(0xbb, clk(200), uuid::Clock::zero(), -500)],
        );
        // In-sync legacy checkpoint (committed-close key matches committed_close).
        let legacy = close_only_checkpoint(clk(50), "ack/legacy");
        let seed = proto::Persist {
            committed_frontier: Some(shuffle::JournalFrontier::encode(&committed.journals)),
            hinted_frontier: Some(shuffle::JournalFrontier::encode(&hinted.journals)),
            committed_close_clock: clk(50).as_u64(),
            hinted_close_clock: clk(200).as_u64(),
            legacy_checkpoint: Some(legacy),
            ..Default::default()
        };

        let (converged, persists) = run_reconcile_loop(
            seed,
            consumer::Checkpoint::default(),
            true, // drop_v1_rollback
            &index,
            &["sk-0"],
        )
        .await
        .unwrap();

        // The one Persist deletes only the legacy checkpoint.
        let [p] = persists.as_slice() else {
            panic!("expected one persist, got {persists:?}");
        };
        assert!(p.delete_legacy_checkpoint);
        assert!(!p.delete_committed_frontier);
        assert!(!p.delete_hinted_frontier);
        assert!(p.committed_frontier.is_none());
        assert!(!p.delete_ack_intents);

        // Only the legacy checkpoint is gone; frontiers and close clocks intact.
        assert!(converged.legacy_checkpoint.is_none());
        assert_eq!(converged.committed_close, clk(50));
        assert_eq!(producer_tags(&converged.committed_frontier), vec![0xbb]);
        assert_eq!(producer_tags(&converged.hinted_frontier), vec![0xbb]);
        // The hint still resolves as an idempotent replay from its committed offset.
        let (resume_frontier, idempotent_replay) = converged.session_state();
        assert!(idempotent_replay);
        assert_eq!(resume_frontier.journals[0].producers[0].offset, -500);
    }

    /// The migration first-commit crash: startup A converted the V1 legacy
    /// checkpoint (FLOOR seeded, legacy marker embedded) and one-shot adopted
    /// the endpoint's V1 checkpoint; the session's first transaction then
    /// persisted its hint, stored the marked checkpoint in the endpoint at
    /// StartCommit, and crashed pre-Persist(commit). On the next startup the
    /// legacy marker matches the FLOOR, so conversion does NOT re-fire and the
    /// hint survives to drive the fold: the delta lands atop the adopted V1
    /// baseline. No adoption arm for a marked-ahead connector checkpoint is
    /// needed — Persist(hint) strictly precedes StartCommit, so the marker
    /// always finds its hint.
    #[tokio::test]
    async fn reconcile_loop_folds_migration_first_commit_crash() {
        let index = [("s-a", 0usize)];
        // The adopted V1 baseline (0xbb) at the FLOOR, plus the crashed first
        // transaction's hint (0xcc, the session's own producer) at T200, and
        // the conversion's legacy refresh embedding the FLOOR marker.
        let baseline = frontier(
            "j/one",
            0,
            vec![pf(0xbb, clk(1000), uuid::Clock::zero(), -5000)],
        );
        let hinted = frontier(
            "j/one",
            0,
            vec![pf(0xcc, clk(200), uuid::Clock::zero(), -8000)],
        );
        let mut legacy = authoritative_checkpoint("j/one", "s-a", 0xbb, clk(1000), 5000);
        let (k, v) = frontier_mapping::encode_committed_close(FLOOR);
        legacy.sources.insert(k, v);

        let seed = proto::Persist {
            committed_frontier: Some(shuffle::JournalFrontier::encode(&baseline.journals)),
            committed_close_clock: FLOOR.as_u64(),
            hinted_frontier: Some(shuffle::JournalFrontier::encode(&hinted.journals)),
            hinted_close_clock: clk(200).as_u64(),
            legacy_checkpoint: Some(legacy),
            ..Default::default()
        };
        // The connector checkpoint written at StartCommit: the full merged
        // legacy-format checkpoint plus the close key confirming the commit.
        let mut connector = authoritative_checkpoint("j/one", "s-a", 0xbb, clk(1000), 8000);
        let (k, v) = frontier_mapping::encode_committed_close(clk(200));
        connector.sources.insert(k, v);
        connector.ack_intents = [("ack/c".to_string(), Bytes::from_static(b"C"))].into();

        let (converged, persists) = run_reconcile_loop(
            seed,
            connector.clone(),
            false, // rollback maintained
            &index,
            &["sk-0"],
        )
        .await
        .unwrap();

        // The one Persist is the fold: committed_close advances to T200, the
        // hinted delta is written as committed, the connector's ACK intents
        // are adopted, and the legacy checkpoint is refreshed verbatim.
        let [p] = persists.as_slice() else {
            panic!("expected one persist, got {persists:?}");
        };
        assert_eq!(p.committed_close_clock, clk(200).as_u64());
        assert!(!p.delete_committed_frontier);
        assert!(!p.delete_hinted_frontier);
        assert!(p.delete_ack_intents);
        assert!(p.ack_intents.contains_key("ack/c"));
        assert_eq!(p.legacy_checkpoint.as_ref(), Some(&connector));

        // The folded delta merged atop the V1 baseline: both producers commit.
        assert_eq!(converged.committed_close, clk(200));
        assert_eq!(
            producer_tags(&converged.committed_frontier),
            vec![0xbb, 0xcc]
        );
        assert_eq!(
            converged.committed_frontier.journals[0].producers[1].offset,
            -8000
        );
        assert!(converged.ack_intents.contains_key("ack/c"));
        assert_eq!(
            frontier_mapping::extract_committed_close(
                converged
                    .legacy_checkpoint
                    .as_ref()
                    .expect("legacy maintained")
            ),
            Some(clk(200)),
        );
        // The hint is resolved by the covering committed entry: no replay.
        let (_, idempotent_replay) = converged.session_state();
        assert!(!idempotent_replay);
    }

    /// A marked connector checkpoint coexisting with a marker-less legacy
    /// checkpoint cannot be produced by this code (conversion persists before
    /// the session's first StartCommit, and a V1 rollback strips the endpoint
    /// marker before the legacy one) — it's residue of a pre-FLOOR migration
    /// crash or out-of-model divergence. The conversion fires, and the marked
    /// connector checkpoint — matching neither seeded Clock — then bails
    /// loudly rather than being silently adopted. Remediation is a brief V1
    /// rollback (stripping both markers) before re-migrating.
    #[tokio::test]
    async fn reconcile_loop_bails_on_marked_connector_with_v1_legacy() {
        let index = [("s-a", 0usize)];
        let legacy = authoritative_checkpoint("j/one", "s-a", 0xbb, clk(1000), 5000);
        let seed = proto::Persist {
            legacy_checkpoint: Some(legacy),
            ..Default::default()
        };
        let mut connector = authoritative_checkpoint("j/one", "s-a", 0xbb, clk(1000), 8000);
        let (k, v) = frontier_mapping::encode_committed_close(clk(200));
        connector.sources.insert(k, v);

        let err = run_reconcile_loop(seed, connector, false, &index, &["sk-0"])
            .await
            .unwrap_err();
        assert!(
            format!("{err:?}").contains("doesn't match committed_close"),
            "{err:?}"
        );
    }
}
