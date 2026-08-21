//! End-to-end integration test of `TaskControl.SyncNow`: a real tonic server
//! hosting the `Leader` and `TaskControl` services behind armed AuthN
//! interceptors, a real materialize leader session, and real journal IO for
//! stats and ACK-intent writes — the IO that gates `Tail::Done`, the instant
//! sync-now waiters resolve at.
//!
//! The test plays the shard side itself: it acts as shard zero of a
//! single-shard topology, speaking the `Materialize` session protocol over
//! the Leader service (Join → Task → Recover → Apply → Open, then the
//! Load / Flush / Store / StartCommit / Acknowledge transaction flow) with a
//! scripted "connector-side" that acknowledges commits only when the test
//! says so. Withholding `Acknowledged` is the test's control over the moment
//! a transaction becomes fully acknowledged.
//!
//! Source checkpoints come from a fixture `ShuffleSessionFactory` fed by the
//! test — one synthetic `shuffle::Frontier` per transaction — so no journals
//! are read. Multi-shard session mechanics are deliberately NOT exercised: a
//! single-shard Join is immediate consensus, and sync-now waiter semantics
//! don't depend on shard count.
//!
//! Spawns real `etcd` (on PATH) and `~/go/bin/gazette` child processes via
//! `e2e_support::DataPlane`, exactly like the `shuffle` scenario tests that
//! run under `ci:nextest-run`.

use prost::Message;
use proto_flow::flow;
use proto_gazette::{broker, uuid};
use runtime_next::proto;
use runtime_next::proto::sync_now_response;
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

/// Bound on every await of the driver and of sync-now streams. Also the
/// promptness assertion: a schedule-collapsed or floor-bypassed commit must
/// complete well within it (the alternative was a 2h hold / 300s floor).
const EXPECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Window over which a held transaction must leave the leader silent: absent
/// a poke, the shard is asked for nothing until the hold expires.
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

type SyncNowStream = tonic::Streaming<proto::SyncNowResponse>;

/// One test's hermetic world: a DataPlane, the built fixture catalog, and an
/// in-process tonic server hosting `Leader` + `TaskControl` behind armed
/// authentication interceptors — assembled exactly as the runtime sidecar
/// serves them.
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

        // Serve both services behind interceptors, exactly as the sidecar
        // does: Leader requires LEAD; TaskControl requires gazette READ.
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
                    svc.clone().into_tonic_service(),
                    authn.clone().interceptor(proto_flow::capability::LEAD),
                ))
                .add_service(tonic::service::interceptor::InterceptedService::new(
                    svc.into_task_control_service(),
                    authn.interceptor(proto_gazette::capability::READ),
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

    /// Mint a gazette-READ bearer scoped to `id_prefix`, as any
    /// `/authorize/user/task` Read-level caller would hold.
    fn read_token(&self, id_prefix: &str) -> proto_grpc::Metadata {
        self.token(proto_gazette::capability::READ, id_prefix)
    }

    fn token(&self, capability: u32, id_prefix: &str) -> proto_grpc::Metadata {
        let token = self
            .signer
            .sign(
                capability,
                "test-caller".to_string(),
                broker::LabelSelector {
                    include: Some(labels::build_set([("id:prefix", id_prefix)])),
                    exclude: None,
                },
                tokens::TimeDelta::hours(1),
            )
            .expect("signing test token");
        proto_grpc::Metadata::new()
            .with_bearer_token(&token)
            .expect("building bearer metadata")
    }

    /// Open a SyncNow stream for `task_name` bearing `metadata`.
    async fn sync_now(
        &self,
        metadata: proto_grpc::Metadata,
        task_name: &str,
    ) -> tonic::Result<SyncNowStream> {
        let channel = gazette::dial_channel(&self.endpoint).expect("dialing test endpoint");
        let response =
            proto_grpc::runtime::task_control_client::TaskControlClient::with_interceptor(
                channel, metadata,
            )
            .sync_now(proto::SyncNowRequest {
                task_name: task_name.to_string(),
            })
            .await?;
        Ok(response.into_inner())
    }

    /// SyncNow with a correctly-scoped READ token, consuming the Ack and
    /// returning the still-open stream.
    async fn poke(&self, task_name: &str) -> SyncNowStream {
        let metadata = self.read_token(&format!("materialize/{task_name}/"));
        let mut stream = self
            .sync_now(metadata, task_name)
            .await
            .expect("SyncNow call");
        expect_ack(&mut stream).await;
        stream
    }

    /// Drain barrier: poke and await its Done, which proves the leader reached
    /// `Tail::Done` — so the next transaction starts from a fully-drained
    /// leader (deterministic `await_dones`). Idempotent re-poking is exercised
    /// for free. Only valid while no transaction is open: an open transaction
    /// would arm `close_requested` and park us until the shard side commits.
    async fn poke_until_drained(&self, task_name: &str) {
        let mut stream = self.poke(task_name).await;
        expect_done(&mut stream).await;
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
        // transaction; answer it so the session starts fully drained.
        driver.expect_acknowledge().await;
        driver.send_acknowledged();

        driver
    }
}

/// The scripted shard side of one Leader.Materialize session.
struct ShardDriver {
    to_leader: mpsc::UnboundedSender<proto::Materialize>,
    from_leader: tonic::Streaming<proto::Materialize>,
    _shuffle_dir: tempfile::TempDir,
}

impl ShardDriver {
    fn send(&self, msg: proto::Materialize) {
        self.to_leader
            .send(msg)
            .expect("leader request stream is open");
    }

    /// Receive the next leader message, transparently echoing `Persisted` for
    /// any `Persist`: the leader persists at multiple FSM points (hint before
    /// Store, commit after StartedCommit, legacy-checkpoint fields included),
    /// and answering them generically keeps this driver robust to
    /// FSM-internal ordering.
    async fn expect(&mut self, awaiting: &'static str) -> proto::Materialize {
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
            return msg;
        }
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

// ---- SyncNow stream helpers ----

async fn recv_sync_now(
    stream: &mut SyncNowStream,
    awaiting: &'static str,
    timeout: Duration,
) -> sync_now_response::Response {
    tokio::time::timeout(timeout, stream.message())
        .await
        .unwrap_or_else(|_| panic!("timed out awaiting SyncNow {awaiting}"))
        .unwrap_or_else(|err| panic!("SyncNow stream error awaiting {awaiting}: {err}"))
        .unwrap_or_else(|| panic!("unexpected SyncNow EOF awaiting {awaiting}"))
        .response
        .unwrap_or_else(|| panic!("SyncNow response with empty oneof awaiting {awaiting}"))
}

async fn expect_ack(stream: &mut SyncNowStream) {
    match recv_sync_now(stream, "Ack", EXPECT_TIMEOUT).await {
        sync_now_response::Response::Ack(_) => (),
        other => panic!("expected SyncNow Ack, got {other:?}"),
    }
}

/// Await Done, skipping any interleaved heartbeats.
async fn expect_done(stream: &mut SyncNowStream) {
    loop {
        match recv_sync_now(stream, "Done", EXPECT_TIMEOUT).await {
            sync_now_response::Response::Done(_) => return,
            sync_now_response::Response::Heartbeat(_) => continue,
            other => panic!("expected SyncNow Done, got {other:?}"),
        }
    }
}

async fn expect_heartbeat(stream: &mut SyncNowStream, timeout: Duration) {
    match recv_sync_now(stream, "Heartbeat", timeout).await {
        sync_now_response::Response::Heartbeat(_) => (),
        other => panic!("expected SyncNow Heartbeat, got {other:?}"),
    }
}

/// Assert no response arrives on `stream` within `window` (bounded peek).
async fn assert_no_response(stream: &mut SyncNowStream, window: Duration) {
    match tokio::time::timeout(window, stream.message()).await {
        Err(_) => (),
        Ok(result) => panic!("unexpected SyncNow response while ack withheld: {result:?}"),
    }
}

// ---- Scripted transaction helpers ----

/// Run one full transaction to completion, then drain via the IDLE barrier.
/// Used as the warm-up before every held-transaction test: the first
/// transaction of a session is never schedule-held (`session_start`), so the
/// NEXT transaction is the one a schedule genuinely holds.
async fn warm_up_txn(harness: &Harness, driver: &mut ShardDriver, task_name: &str) {
    harness.push_frontier();
    driver.expect_load().await;
    driver.send_loaded(DOCS_PER_TXN);
    driver.run_close_script().await;
    driver.send_acknowledged();
    harness.poke_until_drained(task_name).await;
}

/// Open a transaction that the sync schedule holds, and deliver the SyncNow
/// that collapses it, returning the caller's stream.
///
/// The hold is proven by the leader's silence: a held transaction asks the
/// shard for nothing, because absent our poke it would sit until the
/// schedule's next fire instant hours away. Wall-clock flake: the fire
/// instants sit on a fixed grid (`jitter(seed) + k * baseInterval`) which the
/// test cannot choose "now" against, so a transaction opening milliseconds
/// before an instant commits on its own and trips `assert_quiet` (~1-in-1e5
/// odds with the 2h interval).
async fn open_held_txn(harness: &Harness, driver: &mut ShardDriver) -> SyncNowStream {
    harness.push_frontier();
    driver.expect_load().await;
    driver.send_loaded(DOCS_PER_TXN);
    driver.assert_quiet(HELD_QUIET_WINDOW).await;

    harness.poke(SCHEDULED_TASK).await
}

// ---- Tests ----

/// A schedule-held transaction collapses on SyncNow and resolves only once
/// the shard side acknowledges; a fully-drained task then resolves at once.
#[tokio::test(flavor = "multi_thread")]
async fn held_transaction_collapses_and_resolves_on_ack() {
    let harness = Harness::start().await;
    let mut driver = harness.join_session(SCHEDULED_TASK).await;
    warm_up_txn(&harness, &mut driver, SCHEDULED_TASK).await;

    // Transaction #2 is genuinely held (a 2h hold, absent the poke).
    let mut stream = open_held_txn(&harness, &mut driver).await;

    // The hold collapsed: the commit proceeds promptly rather than waiting
    // out the 2h grid, and parks at Acknowledge.
    driver.run_close_script().await;

    // Done must not arrive while the acknowledgement is withheld.
    assert_no_response(&mut stream, Duration::from_secs(2)).await;

    driver.send_acknowledged();
    expect_done(&mut stream).await;

    // With everything drained and no new frontier, a fresh poke has nothing
    // to await and its Done follows immediately.
    let mut stream = harness.poke(SCHEDULED_TASK).await;
    expect_done(&mut stream).await;
    driver.assert_quiet(HELD_QUIET_WINDOW).await;

    drop(driver);
    harness.stop().await;
}

/// N concurrent SyncNow calls against the same open transaction all ack and
/// all resolve at the same single commit.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_pokes_share_one_commit() {
    let harness = Harness::start().await;
    let mut driver = harness.join_session(SCHEDULED_TASK).await;
    warm_up_txn(&harness, &mut driver, SCHEDULED_TASK).await;

    // The first poke collapses the held transaction; three more land while
    // the (single) commit is in flight, each acking wherever the actor is.
    let first = open_held_txn(&harness, &mut driver).await;
    let mut streams = vec![first];
    for _ in 0..3 {
        streams.push(harness.poke(SCHEDULED_TASK).await);
    }

    driver.run_close_script().await;

    // No stream resolves while the acknowledgement is withheld.
    for stream in &mut streams {
        assert_no_response(stream, Duration::from_millis(500)).await;
    }

    driver.send_acknowledged();
    for stream in &mut streams {
        expect_done(stream).await;
    }

    // One transaction total: the leader is idle and asks nothing more of the
    // shard once the shared commit drains.
    harness.poke_until_drained(SCHEDULED_TASK).await;
    driver.assert_quiet(Duration::from_millis(500)).await;

    drop(driver);
    harness.stop().await;
}

/// A poke arriving after commit, while the connector acknowledgement still
/// drains, resolves at that same acknowledgement release rather than awaiting
/// a further transaction.
#[tokio::test(flavor = "multi_thread")]
async fn poke_in_drain_window_awaits_the_same_ack() {
    let harness = Harness::start().await;
    let mut driver = harness.join_session(SCHEDULED_TASK).await;
    warm_up_txn(&harness, &mut driver, SCHEDULED_TASK).await;

    let mut collapse_stream = open_held_txn(&harness, &mut driver).await;

    // Drive through StartedCommit and the commit Persist: the transaction has
    // committed and its Tail drains the (withheld) acknowledgement.
    driver.run_close_script().await;

    let mut drain_stream = harness.poke(SCHEDULED_TASK).await;
    assert_no_response(&mut drain_stream, Duration::from_millis(500)).await;

    driver.send_acknowledged();
    expect_done(&mut collapse_stream).await;
    expect_done(&mut drain_stream).await;

    drop(driver);
    harness.stop().await;
}

/// A transaction held by the 300s min-duration floor (no schedule) commits
/// immediately on SyncNow — the close request bypasses the floor. The floor
/// holds even the session's first transaction (`session_start` waives only
/// schedule holds), so no warm-up is needed.
#[tokio::test(flavor = "multi_thread")]
async fn sync_now_bypasses_the_min_duration_floor() {
    let harness = Harness::start().await;
    let mut driver = harness.join_session(FLOOR_TASK).await;

    harness.push_frontier();
    driver.expect_load().await;
    driver.send_loaded(DOCS_PER_TXN);
    driver.assert_quiet(HELD_QUIET_WINDOW).await;

    let mut stream = harness.poke(FLOOR_TASK).await;

    // The commit proceeds well before the 300s floor, and Done follows the
    // released acknowledgement.
    driver.run_close_script().await;
    driver.send_acknowledged();
    expect_done(&mut stream).await;

    drop(driver);
    harness.stop().await;
}

/// A parked waiter receives heartbeats. The cadence is the production 15s
/// `SYNC_NOW_HEARTBEAT` constant, kept un-injectable on purpose, so this is
/// deliberately the one slow test: the first beat lands one full interval
/// after the waiter parks.
#[tokio::test(flavor = "multi_thread")]
async fn parked_waiter_receives_heartbeats() {
    let harness = Harness::start().await;
    let mut driver = harness.join_session(FLOOR_TASK).await;

    harness.push_frontier();
    driver.expect_load().await;
    driver.send_loaded(DOCS_PER_TXN);

    let mut stream = harness.poke(FLOOR_TASK).await;

    // Commit, then park with the acknowledgement withheld.
    driver.run_close_script().await;

    // One beat proves the machinery; don't wait for a second.
    expect_heartbeat(&mut stream, Duration::from_secs(30)).await;

    driver.send_acknowledged();
    expect_done(&mut stream).await;

    drop(driver);
    harness.stop().await;
}

/// Token scope and capability checks over the real wire, and NOT_FOUND
/// resolution from the caller's shard-scope when no live session matches.
#[tokio::test(flavor = "multi_thread")]
async fn authz_and_task_resolution() {
    let harness = Harness::start().await;
    let driver = harness.join_session(SCHEDULED_TASK).await;

    // A correctly-scoped READ token is accepted, and with no open transaction
    // resolves immediately.
    harness.poke_until_drained(SCHEDULED_TASK).await;

    // A READ token scoped to a DIFFERENT task is denied against the live
    // session's concrete shard-zero ID.
    let err = harness
        .sync_now(
            harness.read_token("materialize/testing/other/0011223344556677/"),
            SCHEDULED_TASK,
        )
        .await
        .expect_err("differently-scoped token is rejected");
    assert_eq!(err.code(), tonic::Code::PermissionDenied, "{err}");

    // A token minted WITHOUT the READ capability bit fails at the
    // interceptor, before any Ack.
    let err = harness
        .sync_now(
            harness.token(
                proto_flow::capability::LEAD,
                &format!("materialize/{SCHEDULED_TASK}/"),
            ),
            SCHEDULED_TASK,
        )
        .await
        .expect_err("token without READ is rejected");
    assert!(
        matches!(
            err.code(),
            tonic::Code::PermissionDenied | tonic::Code::Unauthenticated
        ),
        "{err}",
    );

    // A materialization the token names, but which isn't running here, is
    // NOT_FOUND.
    let err = harness
        .sync_now(
            harness.read_token("materialize/testing/not-running/"),
            "testing/not-running",
        )
        .await
        .expect_err("not-running task is NOT_FOUND");
    assert_eq!(err.code(), tonic::Code::NotFound, "{err}");

    // A capture or derivation is NOT_FOUND here too: this service hosts only
    // Materialize leader sessions. A task with nothing to sync is resolved by
    // the reactor front door, which has the shard keyspace to know its type.
    let err = harness
        .sync_now(
            harness.read_token("capture/testing/some-capture/"),
            "testing/some-capture",
        )
        .await
        .expect_err("a capture has no leader session here");
    assert_eq!(err.code(), tonic::Code::NotFound, "{err}");

    // An empty task_name is an invalid argument.
    let err = harness
        .sync_now(harness.read_token("materialize/testing/"), "")
        .await
        .expect_err("empty task_name is rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument, "{err}");

    drop(driver);
    harness.stop().await;
}

/// A session that exits with waiters parked errors them out (they can't
/// resolve: the Tail::Done they await happens, if ever, in a future session),
/// and the SyncNowGuard un-registers the handle so follow-up pokes are
/// NOT_FOUND.
#[tokio::test(flavor = "multi_thread")]
async fn session_exit_errors_parked_waiters() {
    let harness = Harness::start().await;
    let mut driver = harness.join_session(FLOOR_TASK).await;

    // Park a waiter: a floor-held open transaction whose close script the
    // driver never answers.
    harness.push_frontier();
    driver.expect_load().await;
    driver.send_loaded(DOCS_PER_TXN);

    let mut stream = harness.poke(FLOOR_TASK).await;

    // Kill the session: the leader errors on the shard stream's EOF.
    drop(driver);

    let err = tokio::time::timeout(EXPECT_TIMEOUT, stream.message())
        .await
        .expect("waiter resolves after session exit")
        .expect_err("waiter errors after session exit");
    assert_eq!(err.code(), tonic::Code::Unavailable, "{err}");
    assert!(err.message().contains("retry"), "{err}");

    // The guard un-registered the handle; a follow-up poke is NOT_FOUND.
    // Guard drop races the client's next call, so retry briefly.
    let deadline = tokio::time::Instant::now() + EXPECT_TIMEOUT;
    loop {
        match harness
            .sync_now(
                harness.read_token(&format!("materialize/{FLOOR_TASK}/")),
                FLOOR_TASK,
            )
            .await
        {
            Err(status) if status.code() == tonic::Code::NotFound => break,
            other => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "timed out awaiting NOT_FOUND after session exit (last: {other:?})",
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    harness.stop().await;
}
