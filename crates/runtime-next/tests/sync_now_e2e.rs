//! End-to-end integration test of the leader's half of sync-now: the
//! `CloseNow` / `Synced` commit barrier of `runtime.proto`. A real tonic server
//! hosting the `Leader` service behind an armed AuthN interceptor, a real
//! materialize leader session, and real journal IO for stats and ACK-intent
//! writes — the IO that gates `Tail::Done`, the instant `Synced` reports a
//! transaction acknowledged.
//!
//! The test plays the shard side itself: it acts as shard zero of a
//! single-shard topology, speaking the `Materialize` session protocol over
//! the Leader service (Join → Task → Recover → Apply → Open, then the
//! Load / Flush / Store / StartCommit / Acknowledge transaction flow) with a
//! scripted "connector-side" that acknowledges commits only when the test
//! says so. Withholding `Acknowledged` is the test's control over the moment
//! a transaction becomes fully acknowledged.
//!
//! It also plays the *controller* side of the barrier, in `ShardDriver`: record
//! `acknowledged_count + pending_count` from the leader's last `Synced`, send
//! `CloseNow`, and await `acknowledged_count` reaching that sum. That mirrors
//! `materializeAppV2.syncNow` in `go/runtime/materialize_v2.go`, which is what
//! users actually reach through the reactor front door — none of which appears
//! here. AuthZ, routing, and the response stream are the front door's, and are
//! covered by `go/runtime/task_control_test.go`.
//!
//! Source checkpoints come from a fixture `ShuffleSessionFactory` fed by the
//! test — one synthetic `shuffle::Frontier` per transaction — so no journals
//! are read. Multi-shard session mechanics are deliberately NOT exercised: a
//! single-shard Join is immediate consensus, and the barrier's arithmetic
//! doesn't depend on shard count.
//!
//! Spawns real `etcd` (on PATH) and `~/go/bin/gazette` child processes via
//! `e2e_support::DataPlane`, exactly like the `shuffle` scenario tests that
//! run under `ci:nextest-run`.

use prost::Message;
use proto_flow::flow;
use proto_gazette::{broker, uuid};
use runtime_next::proto;
use std::time::Duration;
use tokio::sync::mpsc;

/// The schedule-paced fixture task: its open transactions (after the
/// session's first) are held for up to the 2h `baseInterval`.
const SCHEDULED_TASK: &str = "testing/sync-now";
/// The fixture task held by a 300s minimum-transaction-duration floor.
const FLOOR_TASK: &str = "testing/sync-now-floor";

/// Ops-stats journal that leader commits publish stats and ACK intents to.
/// Pre-created by the harness; carried in the Join's `stats_journal` labeling.
const OPS_STATS_JOURNAL: &str = "testing/ops/stats";

/// Source documents reported by each scripted `Loaded`.
const DOCS_PER_TXN: u64 = 3;

/// Bound on every await of the driver. Also the promptness assertion: a
/// schedule-collapsed or floor-bypassed commit must complete well within it
/// (the alternative was a 2h hold / 300s floor).
const EXPECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Window over which a held transaction must leave the leader silent: absent
/// a CloseNow, the shard is asked for nothing until the hold expires.
const HELD_QUIET_WINDOW: Duration = Duration::from_millis(500);

const ISSUER: &str = "sync-now.test";
const SECRET: &[u8] = b"sync-now-e2e-secret";

fn shard_zero_id(task_name: &str) -> String {
    format!("materialize/{task_name}/0011223344556677/00000000-00000000")
}

/// A fixture [`runtime_next::ShuffleSessionFactory`]: sessions relay the
/// Frontiers pushed by the test, reading no journals. The single receiver is
/// shared behind a mutex so a session owns it for its lifetime.
struct FixtureShuffleFactory {
    frontier_rx: std::sync::Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<shuffle::Frontier>>>,
}

impl runtime_next::ShuffleSessionFactory for FixtureShuffleFactory {
    type Session = FixtureFrontiers;

    async fn open(
        &self,
        _task: shuffle::proto::Task,
        _shards: Vec<shuffle::proto::Shard>,
        _resume: shuffle::Frontier,
    ) -> anyhow::Result<FixtureFrontiers> {
        Ok(FixtureFrontiers {
            frontier_rx: self.frontier_rx.clone().lock_owned().await,
        })
    }
}

/// A fixture [`runtime_next::ShuffleSession`]: yields one queued Frontier per
/// checkpoint request, and parks forever once the channel is idle or closed
/// (the leader keeps exactly one request in flight; an unanswered request is
/// simply an idle task).
struct FixtureFrontiers {
    frontier_rx: tokio::sync::OwnedMutexGuard<mpsc::UnboundedReceiver<shuffle::Frontier>>,
}

impl runtime_next::ShuffleSession for FixtureFrontiers {
    fn request_checkpoint(&self) {
        // No request protocol: `recv_checkpoint` pops the next queued frontier.
    }

    async fn recv_checkpoint(&mut self) -> anyhow::Result<shuffle::Frontier> {
        match self.frontier_rx.recv().await {
            Some(frontier) => Ok(frontier),
            None => std::future::pending().await,
        }
    }

    async fn close(self) -> anyhow::Result<()> {
        Ok(())
    }
}

/// Synthetic checkpoint Frontier for the `seq`-th transaction: binding zero,
/// one producer whose committed Clock strictly increases and whose negative
/// `offset` (a closed span) grows in magnitude. `flushed_lsn` carries one
/// entry for the single shard; the scripted shard never reads segments, so
/// its value is inert.
fn synthetic_frontier(seq: u64) -> shuffle::Frontier {
    shuffle::Frontier::new(
        vec![shuffle::JournalFrontier {
            journal: "testing/source/pivot=00".into(),
            binding: 0,
            producers: vec![shuffle::ProducerFrontier {
                producer: uuid::Producer::from_bytes([7, 19, 83, 3, 3, 17]),
                last_commit: uuid::Clock::from_unix(1_000_000 + seq, 0),
                hinted_commit: uuid::Clock::from_u64(0),
                offset: -((seq * 1_000) as i64),
            }],
            bytes_read_delta: (DOCS_PER_TXN * 100) as i64,
            bytes_behind_delta: 0,
        }],
        vec![0],
    )
    .expect("synthetic frontier is well-formed")
}

/// One test's hermetic world: a DataPlane, the built fixture catalog, and an
/// in-process tonic server hosting `Leader` behind an armed authentication
/// interceptor — assembled exactly as the runtime sidecar serves it.
struct Harness {
    data_plane: e2e_support::DataPlane,
    endpoint: String,
    server_task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    signer: proto_grpc::Signer,
    frontier_tx: mpsc::UnboundedSender<shuffle::Frontier>,
    /// Built `MaterializationSpec` bytes, keyed by task name.
    specs: std::collections::BTreeMap<String, bytes::Bytes>,
    /// Monotonic ordinal of pushed frontiers, for strictly-increasing clocks.
    txn_seq: std::sync::atomic::AtomicU64,
}

impl Harness {
    async fn start() -> Harness {
        // The tonic/reqwest TLS stack requires a process-level provider, as
        // installed by every production `main` (e.g. runtime-sidecar's).
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let data_plane =
            e2e_support::DataPlane::start(e2e_support::DataPlaneArgs { broker_count: 1 })
                .await
                .expect("DataPlane start");

        let source = build::arg_source_to_url("./tests/sync_now_e2e.flow.yaml", false).unwrap();
        let build::Output { built, .. } = build::for_local_test(&source, true)
            .await
            .into_result()
            .expect("fixture build");

        let specs: std::collections::BTreeMap<String, bytes::Bytes> = built
            .built_materializations
            .iter()
            .map(|row| {
                let spec = row.spec.as_ref().expect("built materialization has a spec");
                (spec.name.clone(), spec.encode_to_vec().into())
            })
            .collect();

        // Pre-create the ops-stats journal: commits publish stats and ACK
        // intents to it, and appends require the journal to exist. A clone of
        // the built collection's partition template carries a valid fragment
        // spec; replication drops to the single test broker.
        let mut ops_spec = built
            .built_collections
            .get_key(&models::Collection::new("testing/source"))
            .expect("built source collection")
            .spec
            .as_ref()
            .expect("collection spec")
            .partition_template
            .clone()
            .expect("partition template");
        ops_spec.name = OPS_STATS_JOURNAL.to_string();
        ops_spec.replication = 1;

        data_plane
            .journal_client
            .apply(broker::ApplyRequest {
                changes: vec![broker::apply_request::Change {
                    expect_mod_revision: 0, // Created by this Apply.
                    upsert: Some(ops_spec),
                    delete: String::new(),
                }],
            })
            .await
            .expect("creating ops stats journal");

        let (frontier_tx, frontier_rx) = mpsc::unbounded_channel();
        let shuffle_factory = FixtureShuffleFactory {
            frontier_rx: std::sync::Arc::new(tokio::sync::Mutex::new(frontier_rx)),
        };

        let journal_client = data_plane.journal_client.clone();
        let publisher_factory = runtime_next::JournalPublisherFactory::new(std::sync::Arc::new(
            move |_authz_sub, _authz_obj| journal_client.clone(),
        ));

        let svc = runtime_next::Service::new(
            shuffle_factory,
            publisher_factory,
            runtime_next::TracingLoggerFactory,
            service_kit::Registry::new(),
            false, // AuthN+AuthZ stays armed.
        );

        // Serve behind an interceptor, exactly as the sidecar does: the Leader
        // service requires LEAD.
        let authn = proto_grpc::Authenticator::new(
            ISSUER.to_string(),
            vec![tokens::jwt::DecodingKey::from_secret(SECRET)],
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("binding ephemeral listener");
        let endpoint = format!("http://{}", listener.local_addr().unwrap());

        let server_task = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(tonic::service::interceptor::InterceptedService::new(
                    svc.into_tonic_service(),
                    authn.interceptor(proto_flow::capability::LEAD),
                ))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );

        let signer = proto_grpc::Signer::new(
            ISSUER.to_string(),
            tokens::jwt::EncodingKey::from_secret(SECRET),
        );

        Harness {
            data_plane,
            endpoint,
            server_task,
            signer,
            frontier_tx,
            specs,
            txn_seq: std::sync::atomic::AtomicU64::new(0),
        }
    }

    async fn stop(self) {
        self.server_task.abort();
        self.data_plane
            .graceful_stop()
            .await
            .expect("graceful stop");
    }

    fn spec_bytes(&self, task_name: &str) -> bytes::Bytes {
        self.specs
            .get(task_name)
            .unwrap_or_else(|| panic!("fixture has no built task {task_name}"))
            .clone()
    }

    /// Push one synthetic checkpoint Frontier, feeding the leader's next
    /// transaction.
    fn push_frontier(&self) {
        let seq = self
            .txn_seq
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        self.frontier_tx
            .send(synthetic_frontier(seq))
            .expect("frontier channel is open");
    }

    /// Act as shard zero of `task_name`: dial the Leader service with a LEAD
    /// bearer and run session startup through the leader's first (recovered,
    /// empty) acknowledgement cycle, so the session begins fully drained.
    async fn join_session(&self, task_name: &str) -> ShardDriver {
        let shuffle_dir = tempfile::tempdir().expect("shuffle tempdir");
        let shard_id = shard_zero_id(task_name);

        let channel = gazette::dial_channel(&self.endpoint).expect("dialing test endpoint");
        let metadata = self
            .signer
            .shard_bearer(proto_flow::capability::LEAD, &shard_id)
            .expect("minting LEAD bearer");
        let mut client =
            proto_grpc::runtime::leader_client::LeaderClient::with_interceptor(channel, metadata);

        let (to_leader, request_rx) = mpsc::unbounded_channel();
        let from_leader = client
            .materialize(tokio_stream::wrappers::UnboundedReceiverStream::new(
                request_rx,
            ))
            .await
            .expect("opening leader Materialize stream")
            .into_inner();

        let join = proto::Join {
            etcd_mod_revision: 1,
            shards: vec![proto::join::Shard {
                id: shard_id,
                labeling: Some(ops::ShardLabeling {
                    build: "0011223344556677".to_string(),
                    range: Some(flow::RangeSpec {
                        key_begin: 0,
                        key_end: u32::MAX,
                        r_clock_begin: 0,
                        r_clock_end: u32::MAX,
                    }),
                    task_name: task_name.to_string(),
                    task_type: ops::TaskType::Materialization as i32,
                    stats_journal: OPS_STATS_JOURNAL.to_string(),
                    ..Default::default()
                }),
                reactor: Some(Default::default()),
                etcd_create_revision: 1,
            }],
            shard_index: 0,
            shuffle_directory: shuffle_dir.path().to_string_lossy().into_owned(),
            shuffle_endpoint: "http://shuffle.invalid".to_string(),
            leader_endpoint: String::new(),
        };

        let mut driver = ShardDriver {
            to_leader,
            from_leader,
            synced: None,
            close_seq: 0,
            deferred: std::collections::VecDeque::new(),
            _shuffle_dir: shuffle_dir,
        };

        driver.send(proto::Materialize {
            join: Some(join),
            ..Default::default()
        });
        let msg = driver.expect("Joined").await;
        let joined = msg
            .joined
            .clone()
            .unwrap_or_else(|| panic!("expected Joined, got {msg:?}"));
        assert_eq!(
            joined.max_etcd_revision, 0,
            "single-shard Join is immediate consensus",
        );

        driver.send(proto::Materialize {
            task: Some(proto::Task {
                spec: self.spec_bytes(task_name),
                max_transactions: 0,
                sqlite_vfs_uri: String::new(),
                publisher_id: bytes::Bytes::copy_from_slice(
                    runtime_next::new_producer().as_bytes(),
                ),
            }),
            ..Default::default()
        });
        // A fresh task: nothing recovered from RocksDB.
        driver.send(proto::Materialize {
            recover: Some(proto::Recover::default()),
            ..Default::default()
        });

        // Apply loop: no connector patches. The leader then persists
        // `last_applied` (echoed transparently by `expect`) and Opens.
        let msg = driver.expect("Apply").await;
        assert!(msg.apply.is_some(), "expected Apply, got {msg:?}");
        driver.send(proto::Materialize {
            applied: Some(proto::Applied {
                action_description: String::new(),
                connector_patches_json: bytes::Bytes::new(),
            }),
            ..Default::default()
        });

        // An empty connector checkpoint makes startup reconciliation a no-op
        // fixed point: no rescan Persists.
        let msg = driver.expect("Open").await;
        assert!(msg.open.is_some(), "expected Open, got {msg:?}");
        driver.send(proto::Materialize {
            opened: Some(proto::materialize::Opened {
                container: None,
                connector_checkpoint: None,
            }),
            ..Default::default()
        });

        // The actor's first act is acknowledging the recovered (empty) prior
        // transaction; answer it.
        driver.expect_acknowledge().await;
        driver.send_acknowledged();

        // Synced is demand-driven: however its counts change, a leader that
        // hasn't received a CloseNow broadcasts nothing.
        driver.assert_quiet(HELD_QUIET_WINDOW).await;

        // A first barrier primes broadcasts for the rest of the session;
        // awaiting the session drained gives every test a clean baseline.
        driver.sync_now().await;
        driver.await_pending(0).await;

        driver
    }
}

/// The scripted shard side of one Leader.Materialize session, which also
/// plays the controller half of the sync-now commit barrier.
struct ShardDriver {
    to_leader: mpsc::UnboundedSender<proto::Materialize>,
    from_leader: tonic::Streaming<proto::Materialize>,
    /// Counts of the last `Synced` observed from the leader, or None before
    /// the session's first.
    synced: Option<proto::Synced>,
    /// Sequence of the last close request we sent.
    close_seq: u64,
    /// Messages read while awaiting a `Synced`, for the caller's script to
    /// consume next.
    deferred: std::collections::VecDeque<proto::Materialize>,
    _shuffle_dir: tempfile::TempDir,
}

impl ShardDriver {
    fn send(&self, msg: proto::Materialize) {
        self.to_leader
            .send(msg)
            .expect("leader request stream is open");
    }

    /// Receive the next leader message, taking anything deferred by
    /// [`Self::await_synced`] before reading the stream.
    async fn recv(&mut self, awaiting: &'static str) -> proto::Materialize {
        match self.deferred.pop_front() {
            Some(msg) => msg,
            None => self.recv_stream(awaiting).await,
        }
    }

    /// Read the next leader message off the stream, transparently echoing
    /// `Persisted` for any `Persist`: the leader persists at multiple FSM
    /// points (hint before Store, commit after StartedCommit,
    /// legacy-checkpoint fields included), and answering them generically
    /// keeps this driver robust to FSM-internal ordering. A `Synced` is
    /// recorded and returned.
    async fn recv_stream(&mut self, awaiting: &'static str) -> proto::Materialize {
        loop {
            let msg = tokio::time::timeout(EXPECT_TIMEOUT, self.from_leader.message())
                .await
                .unwrap_or_else(|_| panic!("timed out awaiting {awaiting}"))
                .unwrap_or_else(|err| panic!("leader stream error awaiting {awaiting}: {err}"))
                .unwrap_or_else(|| panic!("unexpected leader EOF awaiting {awaiting}"));

            if let Some(persist) = &msg.persist {
                assert!(
                    !persist.rescan,
                    "unexpected rescan Persist awaiting {awaiting}"
                );
                self.send(proto::Materialize {
                    persisted: Some(proto::Persisted {
                        seq_no: persist.seq_no,
                    }),
                    ..Default::default()
                });
                continue;
            }
            if let Some(synced) = &msg.synced {
                self.synced = Some(synced.clone());
            }
            return msg;
        }
    }

    /// Receive the next leader message which isn't a `Synced`. Synced arrives
    /// interleaved with the transaction flow, so every transaction-shaped
    /// expectation absorbs it.
    async fn expect(&mut self, awaiting: &'static str) -> proto::Materialize {
        loop {
            let msg = self.recv(awaiting).await;
            if msg.synced.is_none() {
                return msg;
            }
        }
    }

    /// Counts of the last observed `Synced`.
    fn synced(&self) -> &proto::Synced {
        self.synced.as_ref().expect("leader has broadcast a Synced")
    }

    /// Drive the controller half of the sync-now barrier: send a close
    /// request, await the leader echoing its sequence, and take the target
    /// from the counts reported alongside that echo — which describe what the
    /// leader held when it read the request. Returns the `acknowledged_count`
    /// at which the barrier resolves.
    async fn sync_now(&mut self) -> u64 {
        self.close_seq += 1;
        let seq = self.close_seq;

        self.send(proto::Materialize {
            close_now: Some(proto::CloseNow { seq }),
            ..Default::default()
        });
        self.await_synced("Synced echoing our close request", move |s| {
            s.close_request_seq >= seq
        })
        .await;

        self.synced().acknowledged_count + self.synced().pending_count as u64
    }

    /// Await a `Synced` satisfying `pred`. The leader broadcasts Synced
    /// *after* the transaction messages of the same iteration, so a count is
    /// observable only once awaited. Only `Synced` may arrive: any other
    /// message means the leader still wants something the caller's script
    /// hasn't answered.
    async fn await_synced(
        &mut self,
        awaiting: &'static str,
        pred: impl Fn(&proto::Synced) -> bool,
    ) {
        while !self.synced.as_ref().is_some_and(|s| pred(s)) {
            let msg = self.recv_stream(awaiting).await;

            // The leader reports once both FSMs have settled, so a Synced
            // trails the transaction work of the same iteration. Defer that
            // work for the caller's script, which consumes it next.
            if msg.synced.is_none() {
                self.deferred.push_back(msg);
            }
        }
    }

    /// Await the barrier returned by [`Self::sync_now`] resolving.
    async fn await_barrier(&mut self, target: u64) {
        self.await_synced("Synced resolving our barrier", |s| {
            s.acknowledged_count >= target
        })
        .await;
    }

    /// Assert the barrier at `target` has not resolved.
    fn assert_awaiting(&self, target: u64) {
        assert!(
            self.synced().acknowledged_count < target,
            "barrier at {target} resolved early: {:?}",
            self.synced(),
        );
    }

    /// Await a `Synced` reporting `pending` transactions in flight.
    async fn await_pending(&mut self, pending: u32) {
        self.await_synced("Synced reporting pending transactions", |s| {
            s.pending_count == pending
        })
        .await;
    }

    async fn expect_load(&mut self) {
        let msg = self.expect("Load").await;
        assert!(msg.load.is_some(), "expected Load, got {msg:?}");
    }

    async fn expect_acknowledge(&mut self) {
        let msg = self.expect("Acknowledge").await;
        assert!(
            msg.acknowledge.is_some(),
            "expected Acknowledge, got {msg:?}"
        );
    }

    /// Assert the leader sends nothing further within `window`.
    async fn assert_quiet(&mut self, window: Duration) {
        assert!(
            self.deferred.is_empty(),
            "deferred messages are still unconsumed: {:?}",
            self.deferred,
        );
        match tokio::time::timeout(window, self.from_leader.message()).await {
            Err(_) => (),
            Ok(msg) => panic!("unexpected leader message: {msg:?}"),
        }
    }

    fn send_loaded(&self, sourced_docs: u64) {
        self.send(proto::Materialize {
            loaded: Some(proto::materialize::Loaded {
                bindings: vec![proto::materialize::loaded::Binding {
                    index: 0,
                    min_source_clock: 0,
                    max_source_clock: 0,
                    sourced_docs_total: sourced_docs,
                    sourced_bytes_total: sourced_docs * 100,
                    max_key_delta: bytes::Bytes::new(),
                }],
                combiner_usage_bytes: 1 << 10,
            }),
            ..Default::default()
        });
    }

    fn send_acknowledged(&self) {
        self.send(proto::Materialize {
            acknowledged: Some(proto::materialize::Acknowledged::default()),
            ..Default::default()
        });
    }

    /// Drive a closing transaction from Flush through StartedCommit, and park
    /// at the leader's `Acknowledge` — the caller controls when to
    /// `send_acknowledged`. Completing within the expect timeout is itself
    /// the promptness assertion for collapsed holds and bypassed floors.
    async fn run_close_script(&mut self) {
        let msg = self.expect("Flush").await;
        assert!(msg.flush.is_some(), "expected Flush, got {msg:?}");
        self.send(proto::Materialize {
            flushed: Some(proto::materialize::Flushed::default()),
            ..Default::default()
        });

        let msg = self.expect("Store").await;
        assert!(msg.store.is_some(), "expected Store, got {msg:?}");
        self.send(proto::Materialize {
            stored: Some(proto::materialize::Stored::default()),
            ..Default::default()
        });

        let msg = self.expect("StartCommit").await;
        assert!(
            msg.start_commit.is_some(),
            "expected StartCommit, got {msg:?}"
        );
        self.send(proto::Materialize {
            started_commit: Some(proto::materialize::StartedCommit::default()),
            ..Default::default()
        });

        self.expect_acknowledge().await;
    }
}

// ---- Scripted transaction helpers ----

/// Run one full transaction to completion, then await the leader drained.
/// Used as the warm-up before every held-transaction test: the first
/// transaction of a session is never schedule-held (`session_start`), so the
/// NEXT transaction is the one a schedule genuinely holds.
async fn warm_up_txn(harness: &Harness, driver: &mut ShardDriver) {
    harness.push_frontier();
    driver.expect_load().await;
    driver.send_loaded(DOCS_PER_TXN);
    driver.run_close_script().await;
    driver.send_acknowledged();
    driver.await_pending(0).await;
}

/// Open a transaction which the sync schedule holds.
///
/// The hold is proven by the leader's silence: a held transaction asks the
/// shard for nothing, because absent a CloseNow it would sit until the
/// schedule's next fire instant hours away. Wall-clock flake: the fire
/// instants sit on a fixed grid (`jitter(seed) + k * baseInterval`) which the
/// test cannot choose "now" against, so a transaction opening milliseconds
/// before an instant commits on its own and trips `assert_quiet` (~1-in-1e5
/// odds with the 2h interval).
async fn open_held_txn(harness: &Harness, driver: &mut ShardDriver) {
    harness.push_frontier();
    driver.expect_load().await;
    driver.send_loaded(DOCS_PER_TXN);

    driver.await_pending(1).await; // The transaction is open.
    driver.assert_quiet(HELD_QUIET_WINDOW).await;
}

// ---- Tests ----

/// A schedule-held transaction collapses on CloseNow, and its barrier resolves
/// only once the shard side acknowledges; a drained task's barrier is already
/// resolved when it's taken.
#[tokio::test(flavor = "multi_thread")]
async fn held_transaction_collapses_and_resolves_on_ack() {
    let harness = Harness::start().await;
    let mut driver = harness.join_session(SCHEDULED_TASK).await;
    warm_up_txn(&harness, &mut driver).await;

    // Transaction #2 is genuinely held (a 2h hold, absent the CloseNow).
    open_held_txn(&harness, &mut driver).await;
    let target = driver.sync_now().await;

    // The hold collapsed: the commit proceeds promptly rather than waiting
    // out the 2h grid, and parks at Acknowledge.
    driver.run_close_script().await;

    // The barrier must not resolve while the acknowledgement is withheld —
    // and until it releases, the leader has nothing further to report.
    driver.assert_quiet(Duration::from_secs(2)).await;
    driver.assert_awaiting(target);

    driver.send_acknowledged();
    driver.await_barrier(target).await;

    // With everything drained and no new frontier, a fresh barrier has
    // nothing to await: `pending_count` is zero, so its target is already met.
    let target = driver.sync_now().await;
    assert_eq!(target, driver.synced().acknowledged_count);
    driver.assert_quiet(HELD_QUIET_WINDOW).await;

    drop(driver);
    harness.stop().await;
}

/// Repeated CloseNow against one open transaction yields one commit, and every
/// barrier taken over it resolves at that single acknowledgement.
#[tokio::test(flavor = "multi_thread")]
async fn repeated_close_now_yields_one_commit() {
    let harness = Harness::start().await;
    let mut driver = harness.join_session(SCHEDULED_TASK).await;
    warm_up_txn(&harness, &mut driver).await;

    // The first CloseNow collapses the held transaction; three more land while
    // the (single) commit is in flight. All four share a target, because the
    // reported counts don't change until the commit is acknowledged.
    open_held_txn(&harness, &mut driver).await;
    let target = driver.sync_now().await;
    for _ in 0..3 {
        assert_eq!(driver.sync_now().await, target);
    }

    driver.run_close_script().await;
    driver.assert_awaiting(target);

    driver.send_acknowledged();
    driver.await_barrier(target).await;

    // One transaction total: the leader is idle and asks nothing more of the
    // shard once the shared commit drains.
    driver.await_pending(0).await;
    driver.assert_quiet(Duration::from_millis(500)).await;

    drop(driver);
    harness.stop().await;
}

/// A barrier taken after commit, while the connector acknowledgement still
/// drains, resolves at that same acknowledgement release rather than awaiting
/// a further transaction: the draining transaction is the one `pending_count`
/// reports.
#[tokio::test(flavor = "multi_thread")]
async fn barrier_in_drain_window_awaits_the_same_ack() {
    let harness = Harness::start().await;
    let mut driver = harness.join_session(SCHEDULED_TASK).await;
    warm_up_txn(&harness, &mut driver).await;

    open_held_txn(&harness, &mut driver).await;
    let collapse_target = driver.sync_now().await;

    // Drive through StartedCommit and the commit Persist: the transaction has
    // committed and its Tail drains the (withheld) acknowledgement.
    driver.run_close_script().await;

    let drain_target = driver.sync_now().await;
    assert_eq!(
        drain_target, collapse_target,
        "the draining transaction is what both barriers await",
    );
    driver.assert_awaiting(drain_target);

    driver.send_acknowledged();
    driver.await_barrier(drain_target).await;

    drop(driver);
    harness.stop().await;
}

/// A transaction held by the 300s min-duration floor (no schedule) commits
/// immediately on CloseNow — the close request bypasses the floor. The floor
/// holds even the session's first transaction (`session_start` waives only
/// schedule holds), so no warm-up is needed.
#[tokio::test(flavor = "multi_thread")]
async fn close_now_bypasses_the_min_duration_floor() {
    let harness = Harness::start().await;
    let mut driver = harness.join_session(FLOOR_TASK).await;

    harness.push_frontier();
    driver.expect_load().await;
    driver.send_loaded(DOCS_PER_TXN);
    driver.await_pending(1).await;
    driver.assert_quiet(HELD_QUIET_WINDOW).await;

    let target = driver.sync_now().await;

    // The commit proceeds well before the 300s floor, and the barrier resolves
    // on the released acknowledgement.
    driver.run_close_script().await;
    driver.send_acknowledged();
    driver.await_barrier(target).await;

    drop(driver);
    harness.stop().await;
}

/// With Head holding a transaction while Tail drains the prior one,
/// `pending_count` is two and a barrier taken there awaits BOTH
/// acknowledgements — the pipelined case the barrier's arithmetic exists for.
#[tokio::test(flavor = "multi_thread")]
async fn pipelined_head_and_tail_await_both_commits() {
    let harness = Harness::start().await;
    let mut driver = harness.join_session(FLOOR_TASK).await;

    // Commit transaction #1 and park at its Acknowledge, so Tail is draining.
    harness.push_frontier();
    driver.expect_load().await;
    driver.send_loaded(DOCS_PER_TXN);
    driver.await_pending(1).await;
    let first_target = driver.sync_now().await;
    driver.run_close_script().await;

    // Open transaction #2 behind it. The floor holds it, and the close policy
    // keeps the pipeline full: it can't commit until Tail drains.
    harness.push_frontier();
    driver.expect_load().await;
    driver.send_loaded(DOCS_PER_TXN);
    driver.await_pending(2).await; // Head holds one while Tail drains.

    let both_target = driver.sync_now().await;
    assert_eq!(both_target, first_target + 1, "the barrier awaits both");

    // Releasing the first acknowledgement resolves the first barrier and lets
    // transaction #2 close — but not the barrier taken over both.
    driver.send_acknowledged();
    driver.run_close_script().await;
    driver.assert_awaiting(both_target);
    assert!(
        driver.synced().acknowledged_count >= first_target,
        "the first barrier resolved: {:?}",
        driver.synced(),
    );

    driver.send_acknowledged();
    driver.await_barrier(both_target).await;
    driver.await_pending(0).await;

    drop(driver);
    harness.stop().await;
}
