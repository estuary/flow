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
    // Logger of task-centric state changes and events.
    pub logger: L,
    // Recovered ACK intents of the last transaction.
    pub pending_ack_intents: BTreeMap<String, Bytes>,
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
    shard_rx: &mut Vec<BoxStream<'static, tonic::Result<proto::Derive>>>,
    shard_tx: &Vec<mpsc::UnboundedSender<tonic::Result<proto::Derive>>>,
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
        let _ = tx.send(Ok(proto::Derive {
            joined: Some(proto::Joined {
                max_etcd_revision: 0, // Success.
            }),
            ..Default::default()
        }));
    }

    // Receive L:Task from shard zero.
    let verify = crate::verify("Derive", "Open", &peers[0]);
    let task = match verify.not_eof(shard_rx[0].next().await)? {
        proto::Derive {
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

    let spec = flow::CollectionSpec::decode(spec_bytes.as_ref())
        .context("invalid Task collection (derivation)")?;
    let mut task = Task::new(build, &spec, max_transactions, peers)
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

    // Receive Recover fan-in. `connector_state_json` is inert to reconciliation
    // but needed for Open, so lift it out before folding the rest into `Baseline`.
    let recover = recv_recovers(shard_rx, &task.peers)
        .await
        .context("receiving Recover fan-in")?;
    let connector_state_json = recover.connector_state_json.clone();
    let scanned = Baseline::from_recover(recover)?;

    tracing::debug!(
        committed_close = ?scanned.committed_close,
        committed_frontier = ?scanned.committed_frontier,
        connector_state_bytes = connector_state_json.len(),
        legacy_checkpoint = ?scanned.legacy_checkpoint,
        "collected Recover from all shards",
    );

    // Derive has no Apply/Applied phase: open connectors across all shards directly.
    for (tx, shard) in shard_tx.iter().zip(shard_shuffles.iter()) {
        let _ = tx.send(Ok(proto::Derive {
            open: Some(proto::Open {
                spec: spec_bytes.clone(),
                version: task.shard_ref.build.clone(),
                range: shard.range.clone(),
                connector_state_json: connector_state_json.clone(),
                max_keys: BTreeMap::new(),
            }),
            ..Default::default()
        }));
    }

    // Receive Opened fan-in.
    let proto::derive::Opened {
        container: _, // Not sent to leader.
        connector_checkpoint,
    } = recv_opened(shard_rx, &task.peers)
        .await
        .context("receiving Opened fan-in")?;

    // A connector that returns a checkpoint at Opened is remote-authoritative
    // (only derive-sqlite, today).
    task.remote_authoritative = connector_checkpoint.is_some();

    // Remote-authoritative derivations (derive-sqlite) commit their checkpoint
    // to the connector at StartCommit and cannot drop V1 rollback support: the
    // leader must keep maintaining the legacy checkpoint to feed StartCommit.
    // Fail loudly at startup rather than panicking mid-transaction.
    anyhow::ensure!(
        !(task.remote_authoritative && drop_v1_rollback),
        "remote-authoritative derivation (e.g. derive-sqlite) cannot drop V1 rollback support; \
         the {} flag must not be set on this task",
        super::super::DROP_V1_ROLLBACK_FLAG,
    );

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

    let Baseline {
        committed_close,
        committed_frontier,
        ack_intents: pending_ack_intents,
        ..
    } = scanned;

    // No hints are possible (asserted in `Baseline::from_recover`), so the resume
    // Frontier is exactly the durable committed Frontier.
    let resume_frontier = committed_frontier.clone();

    let shuffle_task = shuffle::proto::Task {
        task: Some(shuffle::proto::task::Task::Derivation(spec)),
    };
    let session = service
        .shuffle_factory
        .open(shuffle_task, shard_shuffles, resume_frontier)
        .await
        .context("opening shuffle Session")?;

    Ok(Startup {
        committed_close,
        committed_frontier,
        logger,
        pending_ack_intents,
        publisher,
        session,
        task,
    })
}

/// RocksDB state that startup reconciliation reads and converges toward an
/// authoritative checkpoint. Fields are the [`proto::Recover`] fields that the
/// reconciliation policy may change (the remaining `Recover` fields are inert to
/// reconciliation and threaded-through untouched).
#[derive(Clone, Debug)]
struct Baseline {
    /// Clock at which the last-committed transaction closed.
    committed_close: uuid::Clock,
    /// Committed Frontier (`FC:`).
    committed_frontier: shuffle::Frontier,
    /// Last-persisted ACK intents (`AI:`).
    ack_intents: BTreeMap<String, Bytes>,
    /// Legacy V1-rollback checkpoint, or None when absent / to be deleted.
    legacy_checkpoint: Option<consumer::Checkpoint>,
}

impl Baseline {
    /// Decode a recovered [`proto::Recover`] into its [`Baseline`], asserting the
    /// derive-specific invariants that no hinted state or max-keys are present.
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
            max_keys,
            trigger_params_json: _,
        } = recover;

        // Derivations never track max-keys or a hinted frontier.
        anyhow::ensure!(
            max_keys.is_empty(),
            "derive Recover.max_keys must be empty, but recovered {} entries",
            max_keys.len(),
        );
        anyhow::ensure!(
            hinted_frontier.is_none() && hinted_close_clock == 0,
            "derive Recover carried hinted state (hinted_close_clock={hinted_close_clock}, \
             hinted_frontier {}), but derivations never write one",
            if hinted_frontier.is_some() {
                "present"
            } else {
                "absent"
            },
        );

        let baseline = Baseline {
            committed_close: uuid::Clock::from_u64(committed_close_clock),
            committed_frontier: shuffle::Frontier::decode(committed_frontier.unwrap_or_default())
                .context("validating committed Frontier")?,
            ack_intents,
            legacy_checkpoint,
        };
        Ok(baseline)
    }
}

/// Reconcile a recovered `scanned` [`Baseline`] toward its authoritative
/// checkpoints — a `legacy_checkpoint` carried within `scanned` (V1 rollback
/// migration) or the `connector_checkpoint` of a remote-authoritative
/// derivation (derive-sqlite) — as an ordered sequence of self-clearing steps.
/// See also: [`crate::leader::materialize::startup::reconcile`].
fn reconcile(
    scanned: &Baseline,
    connector_checkpoint: &Option<consumer::Checkpoint>,
    connector_adopted: &mut bool,
    drop_v1_rollback: bool,
    journal_read_suffix_index: &[(&str, usize)],
) -> anyhow::Result<Option<proto::Persist>> {
    const FLOOR: uuid::Clock = frontier_mapping::COMMITTED_CLOSE_FLOOR;

    // Step: convert a V1-written legacy checkpoint into the V2 baseline.
    // Only the V1 runtime writes checkpoints without an embedded committed-close
    // Clock, so a marker-less legacy checkpoint means V1 wrote last and is
    // authoritative. Self-clearing: the refreshed (or deleted) legacy
    // checkpoint carries the embedded FLOOR Clock.
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
                false, // Derivations have no hinted state.
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

    // Steps testing derive-sqlite's connector checkpoint. The connector is the
    // sole authority for its checkpoint, which commits at StartCommit ahead of
    // our own Persist.
    let Some(connector_checkpoint) = connector_checkpoint else {
        return Ok(None);
    };

    match frontier_mapping::extract_committed_close(connector_checkpoint) {
        // In sync with the last commit: nothing to reconcile.
        Some(clock) if clock == scanned.committed_close => {}

        // Step: the connector committed ahead (a crash between its StartCommit
        // and our Persist): adopt its checkpoint wholesale — the reconstructed
        // commit. Self-clearing: committed_close advances to the connector's
        // Clock.
        Some(clock) if clock > scanned.committed_close => {
            service_kit::event!(
                tracing::Level::INFO,
                "leader",
                committed_close = scanned.committed_close,
                connector_close = clock,
                "connector checkpoint committed ahead of committed_close; adopting it",
            );
            let persist = frontier_mapping::adopt_checkpoint(
                connector_checkpoint,
                clock,
                !drop_v1_rollback,
                false, // Derivations have no hinted state.
                journal_read_suffix_index,
            )
            .context("adopting connector checkpoint")?;
            return Ok(Some(persist));
        }

        // Implementation error: the connector's checkpoint can never be behind
        // our own committed-close, which persists only after its StartCommit.
        Some(clock) => anyhow::bail!(
            "connector_checkpoint has clock {clock:?} which is behind committed_close ({:?})",
            scanned.committed_close,
        ),

        // Step: no embedded close Clock, so the V1 runtime wrote the connector
        // checkpoint last (or the derivation is virgin) and it's authoritative.
        // No Persist can clear this trigger — only StartCommit writes it — so
        // instead adopt eagerly, at most once per startup: a redundant adoption
        // is idempotent, while a V1 rollback that advanced the checkpoint
        // during the FLOOR epoch is adopted rather than silently skipped. The
        // epoch ends at the first V2 commit, which embeds its close Clock.
        None if !*connector_adopted => {
            *connector_adopted = true;
            service_kit::event!(
                tracing::Level::INFO,
                "leader",
                committed_close = scanned.committed_close,
                "connector checkpoint has no committed-close Clock; adopting it",
            );
            let persist = frontier_mapping::adopt_checkpoint(
                connector_checkpoint,
                FLOOR,
                !drop_v1_rollback,
                false, // Derivations have no hinted state.
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

async fn recv_recovers(
    request_rxs: &mut [BoxStream<'static, tonic::Result<proto::Derive>>],
    peers: &[String],
) -> anyhow::Result<proto::Recover> {
    let mut recovers = futures::future::try_join_all(request_rxs.into_iter().enumerate().map(
        |(shard_index, rx)| async move {
            let verify = crate::verify("Derive", "Recover", &peers[shard_index]);
            match verify.not_eof(rx.next().await)? {
                proto::Derive {
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

/// Send a rescan `Persist` to a shard and await the fresh `Recover` it scans in
/// reply, decoded into a [`Baseline`].
async fn send_rescan_persist(
    rx: &mut BoxStream<'static, tonic::Result<proto::Derive>>,
    tx: &mpsc::UnboundedSender<tonic::Result<proto::Derive>>,
    peer: &str,
    persist: proto::Persist,
) -> anyhow::Result<Baseline> {
    let verify = crate::verify("Derive", "Recover", peer);

    // Sends are best-effort: a closed peer surfaces on the next `rx`.
    let _ = tx.send(Ok(proto::Derive {
        persist: Some(persist),
        ..Default::default()
    }));

    match verify.not_eof(rx.next().await)? {
        proto::Derive {
            recover: Some(recover),
            ..
        } => Baseline::from_recover(recover),
        other => Err(verify.fail_msg(other)),
    }
}

async fn recv_opened(
    request_rxs: &mut [BoxStream<'static, tonic::Result<proto::Derive>>],
    peers: &[String],
) -> anyhow::Result<proto::derive::Opened> {
    let mut openeds = futures::future::try_join_all(request_rxs.iter_mut().enumerate().map(
        |(shard_index, rx)| async move {
            let verify = crate::verify("Derive", "Opened", &peers[shard_index]);
            match verify.not_eof(rx.next().await)? {
                proto::Derive {
                    opened: Some(opened),
                    ..
                } if shard_index == 0 || opened == proto::derive::Opened::default() => {
                    Ok::<_, anyhow::Error>(opened)
                }
                proto::Derive {
                    opened: Some(_), ..
                } => Err(anyhow::anyhow!(
                    "non-zero shard {} reported connector checkpoint state at Opened; \
                     remote-authoritative derivations (e.g. derive-sqlite) must be single-shard",
                    peers[shard_index],
                )),
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

    fn make_streams(
        per_shard: Vec<Vec<tonic::Result<proto::Derive>>>,
    ) -> Vec<BoxStream<'static, tonic::Result<proto::Derive>>> {
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

    fn recover_msg(recover: proto::Recover) -> tonic::Result<proto::Derive> {
        Ok(proto::Derive {
            recover: Some(recover),
            ..Default::default()
        })
    }

    // ---- Baseline::from_recover ----

    #[test]
    fn from_recover_rejects_hinted_and_max_keys() {
        // A hinted-close clock is stale per-shard state derivations never write.
        let err = Baseline::from_recover(proto::Recover {
            hinted_close_clock: 1,
            ..Default::default()
        })
        .unwrap_err();
        assert!(format!("{err:?}").contains("hinted state"), "{err:?}");

        // A hinted Frontier likewise.
        let err = Baseline::from_recover(proto::Recover {
            hinted_frontier: Some(Default::default()),
            ..Default::default()
        })
        .unwrap_err();
        assert!(format!("{err:?}").contains("hinted state"), "{err:?}");

        // A non-empty max-keys set.
        let err = Baseline::from_recover(proto::Recover {
            max_keys: [(0u32, Bytes::new())].into(),
            ..Default::default()
        })
        .unwrap_err();
        assert!(format!("{err:?}").contains("max_keys"), "{err:?}");
    }

    #[test]
    fn from_recover_happy_path() {
        let baseline = Baseline::from_recover(proto::Recover {
            committed_close_clock: clk(42).as_u64(),
            ack_intents: [("ack/j".to_string(), Bytes::from_static(b"A"))].into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(baseline.committed_close, clk(42));
        assert!(baseline.committed_frontier.journals.is_empty());
        assert!(baseline.ack_intents.contains_key("ack/j"));
        assert!(baseline.legacy_checkpoint.is_none());
    }

    // ---- recv_recovers / recv_opened ----

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
                vec![vec![Ok(proto::Derive {
                    opened: Some(proto::derive::Opened::default()),
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
        let zero = proto::derive::Opened {
            container: None,
            connector_checkpoint: Some(consumer::Checkpoint::default()),
        };
        let mut streams = make_streams(vec![
            vec![Ok(proto::Derive {
                opened: Some(zero.clone()),
                ..Default::default()
            })],
            vec![Ok(proto::Derive {
                opened: Some(proto::derive::Opened::default()),
                ..Default::default()
            })],
        ]);
        let peers = vec!["s0".into(), "s1".into()];
        let got = recv_opened(&mut streams, &peers).await.unwrap();
        assert_eq!(got, zero);

        // A non-zero shard reporting connector checkpoint state is rejected: only
        // single-shard derivations may be remote-authoritative.
        let mut streams = make_streams(vec![
            vec![Ok(proto::Derive {
                opened: Some(proto::derive::Opened::default()),
                ..Default::default()
            })],
            vec![Ok(proto::Derive {
                opened: Some(proto::derive::Opened {
                    container: None,
                    connector_checkpoint: Some(consumer::Checkpoint::default()),
                }),
                ..Default::default()
            })],
        ]);
        let err = recv_opened(&mut streams, &peers).await.unwrap_err();
        let s = format!("{err:?}");
        assert!(s.contains("must be single-shard"), "{s}");
        assert!(s.contains("s1"), "{s}");
    }

    // ---- reconcile ----

    use crate::leader::fixtures::{
        authoritative_checkpoint, clk, close_only_checkpoint, frontier, producer_tags,
    };

    // Derivations carry no hints, so `hinted_commit` is fixed at zero.
    fn pf(tag: u8, last_commit: uuid::Clock, offset: i64) -> shuffle::ProducerFrontier {
        crate::leader::fixtures::pf(tag, last_commit, uuid::Clock::zero(), offset)
    }

    /// Build a `Baseline` from parts, with ACK intents given as `(journal, bytes)`
    /// tuples for brevity.
    fn baseline<const N: usize>(
        committed_close: uuid::Clock,
        committed_frontier: shuffle::Frontier,
        ack_intents: [(&str, &[u8]); N],
        legacy_checkpoint: Option<consumer::Checkpoint>,
    ) -> Baseline {
        Baseline {
            committed_close,
            committed_frontier,
            ack_intents: ack_intents
                .into_iter()
                .map(|(k, v)| (k.to_string(), Bytes::copy_from_slice(v)))
                .collect(),
            legacy_checkpoint,
        }
    }

    const FLOOR: uuid::Clock = frontier_mapping::COMMITTED_CLOSE_FLOOR;

    /// Invoke `reconcile` as one fresh startup pass (an unset adoption latch).
    fn reconcile_once(
        scanned: &Baseline,
        connector_checkpoint: &Option<consumer::Checkpoint>,
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

    /// No authoritative checkpoint at all: `scanned` is already the fixed point.
    #[test]
    fn reconcile_no_checkpoints_is_fixed_point() {
        let index = [("s-a", 0usize)];
        let committed = frontier("j/one", 0, vec![pf(0xbb, clk(50), -500)]);
        let scanned = baseline(clk(50), committed, [], None);
        assert!(
            reconcile_once(&scanned, &None, false, &index)
                .unwrap()
                .is_none()
        );
    }

    /// A legacy V1 checkpoint whose embedded committed-close clock matches
    /// `committed_close` is a V2-written refresh, already in sync: ignored.
    #[test]
    fn reconcile_legacy_in_sync_is_fixed_point() {
        let index = [("s-a", 0usize)];
        let committed = frontier("j/one", 0, vec![pf(0xbb, clk(50), -500)]);
        let legacy = close_only_checkpoint(clk(50), "ack/legacy");
        let scanned = baseline(clk(50), committed, [], Some(legacy));
        assert!(
            reconcile_once(&scanned, &None, false, &index)
                .unwrap()
                .is_none()
        );
    }

    /// A legacy checkpoint carrying a committed-close clock that DOESN'T match
    /// `committed_close` is an implementation error (they advance together).
    #[test]
    fn reconcile_legacy_clock_mismatch_bails() {
        let index = [("s-a", 0usize)];
        let committed = frontier("j/one", 0, vec![pf(0xbb, clk(50), -500)]);
        let legacy = close_only_checkpoint(clk(99), "ack/legacy");
        let scanned = baseline(clk(50), committed, [], Some(legacy));
        let err = reconcile_once(&scanned, &None, false, &index).unwrap_err();
        assert!(format!("{err:?}").contains("doesn't match committed_close"));
    }

    /// A connector checkpoint whose embedded Clock matches `committed_close`
    /// is in sync: the fixed point, with no deep comparison of its content.
    #[test]
    fn reconcile_connector_in_sync_is_fixed_point() {
        let index = [("s-a", 0usize)];
        let committed = frontier("j/one", 0, vec![pf(0xbb, clk(50), -500)]);
        let connector = close_only_checkpoint(clk(50), "ack/c");
        let scanned = baseline(clk(50), committed, [], None);
        assert!(
            reconcile_once(&scanned, &Some(connector), false, &index)
                .unwrap()
                .is_none()
        );
    }

    /// A connector checkpoint behind `committed_close` is an implementation
    /// error: our Persist only lands after the connector's StartCommit.
    #[test]
    fn reconcile_connector_behind_bails() {
        let index = [("s-a", 0usize)];
        let committed = frontier("j/one", 0, vec![pf(0xbb, clk(50), -500)]);
        let connector = close_only_checkpoint(clk(20), "ack/c");
        let scanned = baseline(clk(50), committed, [], None);
        let err = reconcile_once(&scanned, &Some(connector), false, &index).unwrap_err();
        assert!(
            format!("{err:?}").contains("behind committed_close"),
            "{err:?}"
        );
    }

    /// A marker-less connector checkpoint adopts at most once per startup: the
    /// trigger lives in the endpoint where no Persist can clear it, so the
    /// latch — not a deep comparison — bounds the step.
    #[test]
    fn reconcile_marker_less_connector_adopts_once() {
        let index = [("s-a", 0usize)];
        let connector = authoritative_checkpoint("j/one", "s-a", 0xbb, clk(1000), 5000);
        let scanned = baseline(uuid::Clock::zero(), shuffle::Frontier::default(), [], None);

        let mut connector_adopted = false;
        let p = reconcile(
            &scanned,
            &Some(connector.clone()),
            &mut connector_adopted,
            false,
            &index,
        )
        .unwrap()
        .expect("adoption persist");
        assert_eq!(p.committed_close_clock, FLOOR.as_u64());

        // The latch is now set: the same scanned state reconciles to None.
        assert!(
            reconcile(
                &scanned,
                &Some(connector),
                &mut connector_adopted,
                false,
                &index,
            )
            .unwrap()
            .is_none()
        );
    }

    // ---- reconcile loop (against a real RocksDB) ----

    /// The derive-sqlite operational contract: a newly-assigned task starts
    /// with an ephemeral, empty RocksDB (the DB survives session restarts of
    /// an assignment, but is discarded when the assignment migrates reactors)
    /// and must rebuild all state from the connector checkpoint alone. An
    /// empty DB scans as committed_close zero, which any real embedded Clock
    /// is ahead of, so the adoption arm rebuilds committed + ACK intents,
    /// advances committed_close, and rewrites the legacy checkpoint in one
    /// atomic persist.
    #[tokio::test]
    async fn reconcile_loop_rebuilds_empty_rocksdb_from_connector() {
        let index = [("s-a", 0usize)];
        let mut connector = authoritative_checkpoint("j/one", "s-a", 0xbb, clk(1000), 5000);
        let (k, v) = frontier_mapping::encode_committed_close(clk(200));
        connector.sources.insert(k, v);

        let (converged, persists) = run_reconcile_loop(
            proto::Persist::default(), // Empty RocksDB.
            Some(connector),
            false,
            &index,
            &["sk-0"],
        )
        .await
        .unwrap();

        assert_eq!(persists.len(), 1, "one adoption persist");
        assert_eq!(converged.committed_close, clk(200));
        assert_eq!(producer_tags(&converged.committed_frontier), vec![0xbb]);
        assert_eq!(
            converged.committed_frontier.journals[0].producers[0].offset,
            -5000
        );
        // The last transaction's ACK intents are re-published by the session:
        // the prior reactor's ACK writes may never have happened.
        assert!(converged.ack_intents.contains_key("ack/j"));
        assert_eq!(
            frontier_mapping::extract_committed_close(
                converged.legacy_checkpoint.as_ref().expect("maintained")
            ),
            Some(clk(200)),
        );
    }

    /// A virgin derive-sqlite task — empty RocksDB and an empty connector
    /// checkpoint — adopts at the FLOOR: one idempotent persist per startup
    /// until the first commit embeds a real Clock.
    #[tokio::test]
    async fn reconcile_loop_adopts_virgin_connector_at_floor() {
        let index = [("s-a", 0usize)];

        let (converged, persists) = run_reconcile_loop(
            proto::Persist::default(), // Empty RocksDB.
            Some(consumer::Checkpoint::default()),
            false,
            &index,
            &["sk-0"],
        )
        .await
        .unwrap();

        assert_eq!(persists.len(), 1, "one adoption persist");
        assert_eq!(converged.committed_close, FLOOR);
        assert!(converged.committed_frontier.journals.is_empty());
        assert!(converged.ack_intents.is_empty());
        assert_eq!(
            frontier_mapping::extract_committed_close(
                converged.legacy_checkpoint.as_ref().expect("maintained")
            ),
            Some(FLOOR),
        );
    }

    /// Drive `reconcile_loop` against a real RocksDB, as `run` does but
    /// without the shard-protocol plumbing — one fresh startup, with its own
    /// adoption latch. Returns the converged, actually-scanned `Baseline` and
    /// the Persists that were applied. `state_keys` maps binding index =>
    /// state_key for both the persist encoder and scan decoder.
    async fn run_reconcile_loop(
        seed: proto::Persist,
        connector_checkpoint: Option<consumer::Checkpoint>,
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

    /// A seeded state with no authoritative checkpoint converges immediately with
    /// zero persists — the scanned baseline is already the fixed point.
    #[tokio::test]
    async fn reconcile_loop_no_op_converges() {
        let index = [("s-a", 0usize)];
        let committed = frontier("j/one", 0, vec![pf(0xbb, clk(50), -500)]);
        let seed = proto::Persist {
            committed_frontier: Some(shuffle::JournalFrontier::encode(&committed.journals)),
            committed_close_clock: clk(50).as_u64(),
            ..Default::default()
        };
        let (converged, persists) = run_reconcile_loop(seed, None, false, &index, &["sk-0"])
            .await
            .unwrap();

        assert!(persists.is_empty(), "already the fixed point");
        assert_eq!(converged.committed_close, clk(50));
        assert_eq!(producer_tags(&converged.committed_frontier), vec![0xbb]);
    }

    /// Conversion of a pure-V1 legacy checkpoint (no embedded committed-close
    /// clock): committed + ACK intents are rewritten from the checkpoint,
    /// committed_close seeds at the FLOOR, and the legacy checkpoint is deleted
    /// (drop_v1_rollback).
    #[tokio::test]
    async fn reconcile_loop_converts_v1_legacy_and_drops_rollback() {
        let index = [("s-a", 0usize)];
        let committed = frontier("j/one", 0, vec![pf(0xaa, clk(50), -100)]);
        let legacy = authoritative_checkpoint("j/one", "s-a", 0xbb, clk(1000), 5000);
        let seed = proto::Persist {
            committed_frontier: Some(shuffle::JournalFrontier::encode(&committed.journals)),
            committed_close_clock: clk(50).as_u64(),
            legacy_checkpoint: Some(legacy),
            ..Default::default()
        };

        let (converged, persists) = run_reconcile_loop(seed, None, true, &index, &["sk-0"])
            .await
            .unwrap();

        // The one Persist rewrites committed + ACK intents, seeds the FLOOR,
        // and drops the legacy checkpoint.
        let [p] = persists.as_slice() else {
            panic!("expected one persist, got {persists:?}");
        };
        assert_eq!(p.committed_close_clock, FLOOR.as_u64());
        assert_eq!(p.hinted_close_clock, 0, "derivations have no hinted state");
        assert!(!p.delete_hinted_frontier);
        assert!(p.delete_committed_frontier);
        assert!(p.delete_legacy_checkpoint);
        assert!(p.delete_ack_intents);
        assert!(p.ack_intents.contains_key("ack/j"));
        let rebuilt =
            shuffle::Frontier::decode(p.committed_frontier.clone().expect("frontier")).unwrap();
        assert_eq!(producer_tags(&rebuilt), vec![0xbb]);
        assert_eq!(rebuilt.journals[0].producers[0].offset, -5000);

        // Full converged durable Baseline.
        assert_eq!(converged.committed_close, FLOOR);
        assert_eq!(producer_tags(&converged.committed_frontier), vec![0xbb]);
        assert_eq!(
            converged.committed_frontier.journals[0].producers[0].offset,
            -5000
        );
        assert!(converged.ack_intents.contains_key("ack/j"));
        assert!(converged.legacy_checkpoint.is_none());
    }

    /// A derive-sqlite connector checkpoint committed ahead of committed_close
    /// (a crash between its StartCommit and our Persist) is adopted wholesale,
    /// advancing committed_close to its embedded Clock. The connector
    /// round-trips our synthetic committed-close source key, which the mapping
    /// skips.
    #[tokio::test]
    async fn reconcile_loop_adopts_connector_committed_ahead() {
        let index = [("s-a", 0usize)];
        let committed = frontier("j/one", 0, vec![pf(0xaa, clk(50), -100)]);
        let seed = proto::Persist {
            committed_frontier: Some(shuffle::JournalFrontier::encode(&committed.journals)),
            committed_close_clock: clk(50).as_u64(),
            ..Default::default()
        };
        // derive-sqlite's checkpoint: a real source PLUS the synthetic close key.
        let mut connector = authoritative_checkpoint("j/one", "s-a", 0xbb, clk(1000), 5000);
        let (k, v) = frontier_mapping::encode_committed_close(clk(200));
        connector.sources.insert(k, v);

        let (converged, persists) =
            run_reconcile_loop(seed, Some(connector), false, &index, &["sk-0"])
                .await
                .unwrap();

        // The one Persist rewrites committed + ACK intents and advances
        // committed_close to the connector's Clock. Only the real source is
        // mapped; the skipped close key isn't a producer. Rollback is
        // maintained, so a legacy refresh (with the Clock embedded) is written.
        let [p] = persists.as_slice() else {
            panic!("expected one persist, got {persists:?}");
        };
        assert_eq!(p.committed_close_clock, clk(200).as_u64());
        assert!(p.delete_committed_frontier);
        assert!(!p.delete_legacy_checkpoint);
        assert!(p.delete_ack_intents);
        assert!(p.ack_intents.contains_key("ack/j"));
        let rebuilt =
            shuffle::Frontier::decode(p.committed_frontier.clone().expect("frontier")).unwrap();
        assert_eq!(producer_tags(&rebuilt), vec![0xbb]);
        assert_eq!(rebuilt.journals[0].producers[0].offset, -5000);

        assert_eq!(converged.committed_close, clk(200));
        assert_eq!(producer_tags(&converged.committed_frontier), vec![0xbb]);
        assert_eq!(
            converged.committed_frontier.journals[0].producers[0].offset,
            -5000
        );
        assert!(converged.ack_intents.contains_key("ack/j"));
        assert_eq!(
            frontier_mapping::extract_committed_close(
                converged.legacy_checkpoint.as_ref().expect("maintained")
            ),
            Some(clk(200)),
        );
    }

    /// Compound V1→V2 migration of a derive-sqlite task: a pure-V1 legacy
    /// checkpoint AND a marker-less connector checkpoint. The conversion adopts
    /// the legacy first; the connector — the sole authority — is then adopted
    /// atop it, superseding the legacy content (its frontier and ACK intents
    /// win).
    #[tokio::test]
    async fn reconcile_loop_connector_supersedes_legacy() {
        let index = [("s-a", 0usize)];
        // Pure-V1 legacy at offset 5000 with producer 0xbb.
        let legacy = authoritative_checkpoint("j/one", "s-a", 0xbb, clk(1000), 5000);
        // The connector checkpoint is ahead: producer 0xcc at offset 8000.
        let mut connector = authoritative_checkpoint("j/one", "s-a", 0xcc, clk(1000), 8000);
        connector.ack_intents = [("ack/c".to_string(), Bytes::from_static(b"C"))].into();
        let seed = proto::Persist {
            legacy_checkpoint: Some(legacy),
            ..Default::default()
        };

        let (converged, persists) =
            run_reconcile_loop(seed, Some(connector), false, &index, &["sk-0"])
                .await
                .unwrap();

        // Two persists: the legacy conversion, then the connector adoption.
        let [p1, p2] = persists.as_slice() else {
            panic!("expected two persists, got {persists:?}");
        };
        let converted =
            shuffle::Frontier::decode(p1.committed_frontier.clone().expect("frontier")).unwrap();
        assert_eq!(producer_tags(&converted), vec![0xbb]);
        let adopted =
            shuffle::Frontier::decode(p2.committed_frontier.clone().expect("frontier")).unwrap();
        assert_eq!(producer_tags(&adopted), vec![0xcc]);
        assert_eq!(adopted.journals[0].producers[0].offset, -8000);

        // The connector's frontier and ACK intents won over the legacy's.
        assert_eq!(converged.committed_close, FLOOR);
        assert_eq!(producer_tags(&converged.committed_frontier), vec![0xcc]);
        assert!(converged.ack_intents.contains_key("ack/c"));
    }

    /// Dropping V1 rollback deletes only the legacy checkpoint and leaves the
    /// committed frontier intact, converging on the second pass.
    #[tokio::test]
    async fn reconcile_loop_drop_rollback_converges() {
        let index = [("s-a", 0usize)];
        let committed = frontier("j/one", 0, vec![pf(0xbb, clk(50), -500)]);
        let legacy = close_only_checkpoint(clk(50), "ack/legacy");
        let seed = proto::Persist {
            committed_frontier: Some(shuffle::JournalFrontier::encode(&committed.journals)),
            committed_close_clock: clk(50).as_u64(),
            legacy_checkpoint: Some(legacy),
            ..Default::default()
        };

        let (converged, persists) = run_reconcile_loop(seed, None, true, &index, &["sk-0"])
            .await
            .unwrap();

        // The one Persist deletes only the legacy checkpoint.
        let [p] = persists.as_slice() else {
            panic!("expected one persist, got {persists:?}");
        };
        assert!(p.delete_legacy_checkpoint);
        assert!(!p.delete_committed_frontier);
        assert!(p.committed_frontier.is_none());
        assert!(!p.delete_ack_intents);

        assert!(converged.legacy_checkpoint.is_none());
        assert_eq!(converged.committed_close, clk(50));
        assert_eq!(producer_tags(&converged.committed_frontier), vec![0xbb]);
    }
}
