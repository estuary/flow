/// Quickcheck fuzz test for the shuffle crate's Session→Slice→Log pipeline.
///
/// Generates randomized multi-producer workloads with OUTSIDE_TXN,
/// CONTINUE_TXN+ACK, and rollback actions. Verifies transactional
/// correctness (completeness, safety, recovery, cross-journal atomicity,
/// rollback isolation) against an oracle.
///
/// See `shuffle-fuzz-testing-reqs.md` for the full design specification.
use proto_flow::flow;
use proto_gazette::uuid;
use quickcheck::Arbitrary;
use shuffle::log::reader::{FrontierScan, Reader, Remainder};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const NUM_PARTITIONS: usize = 5;
const MAX_PRODUCERS: usize = 4;
const MAX_ROUNDS: usize = 4;
const MAX_CONTINUES: usize = 4;

const PARTITION_CATEGORIES: &[&str] = &["cat-0", "cat-1", "cat-2", "cat-3", "cat-4"];

// ---------------------------------------------------------------------------
// Fuzz input types
// ---------------------------------------------------------------------------

type ProducerId = u8;
type PartitionId = u8;

#[derive(Clone, Debug)]
struct TestCase {
    num_shards: usize,
    num_producers: usize,
    rounds: Vec<Round>,
}

#[derive(Clone, Debug)]
struct Round {
    actions: HashMap<ProducerId, Action>,
    crash: bool,
}

#[derive(Clone, Debug)]
enum Action {
    /// Write a single self-committing document to one partition.
    OutsideTxn { partition: PartitionId },
    /// Write one or more CONTINUE_TXN documents, then commit with ACK.
    ContinueAck { continues: Vec<PartitionId> },
    /// Write one or more CONTINUE_TXN documents, then rollback. Retires the producer.
    ContinueRollback { continues: Vec<PartitionId> },
}

// ---------------------------------------------------------------------------
// Arbitrary implementation
// ---------------------------------------------------------------------------

impl Arbitrary for TestCase {
    fn arbitrary(g: &mut quickcheck::Gen) -> Self {
        let num_shards = 1 + usize::arbitrary(g) % 3;
        let num_producers = 1 + usize::arbitrary(g) % MAX_PRODUCERS;
        let num_rounds = 1 + usize::arbitrary(g) % MAX_ROUNDS;
        let mut retired: HashSet<ProducerId> = HashSet::new();
        let mut rounds = Vec::with_capacity(num_rounds);

        for _ in 0..num_rounds {
            let mut actions = HashMap::new();

            for prod_id in 0..num_producers as u8 {
                if retired.contains(&prod_id) {
                    continue;
                }
                // ~50% chance of NoOp (absent from map).
                if bool::arbitrary(g) {
                    continue;
                }

                let action = match usize::arbitrary(g) % 3 {
                    0 => Action::OutsideTxn {
                        partition: (usize::arbitrary(g) % NUM_PARTITIONS) as u8,
                    },
                    1 => {
                        let num_continues = 1 + usize::arbitrary(g) % MAX_CONTINUES;
                        let continues = (0..num_continues)
                            .map(|_| (usize::arbitrary(g) % NUM_PARTITIONS) as u8)
                            .collect();
                        Action::ContinueAck { continues }
                    }
                    _ => {
                        // Rollback is less likely: only pick it ~25% of the time
                        // (1/3 chance to reach this arm, then 75% downgrade to ContinueAck).
                        let num_continues = 1 + usize::arbitrary(g) % MAX_CONTINUES;
                        let continues: Vec<PartitionId> = (0..num_continues)
                            .map(|_| (usize::arbitrary(g) % NUM_PARTITIONS) as u8)
                            .collect();

                        if bool::arbitrary(g) && bool::arbitrary(g) {
                            retired.insert(prod_id);
                            Action::ContinueRollback { continues }
                        } else {
                            Action::ContinueAck { continues }
                        }
                    }
                };
                actions.insert(prod_id, action);
            }

            let crash = bool::arbitrary(g) && bool::arbitrary(g); // ~25% chance
            rounds.push(Round { actions, crash });
        }

        TestCase {
            num_shards,
            num_producers,
            rounds,
        }
    }

    // Shrinking is disabled: each shrink candidate requires a full session
    // lifecycle (data plane reset, publish, poll, scan) which makes shrinking
    // prohibitively slow in practice.
    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        quickcheck::empty_shrinker()
    }
}

// ---------------------------------------------------------------------------
// Shared test harness (initialized once across all quickcheck invocations)
// ---------------------------------------------------------------------------

struct SharedHarness {
    /// Wrapped in Mutex<Option> so we can take ownership for graceful_stop
    /// after quickcheck completes. OnceLock statics are never dropped, so
    /// we must explicitly tear down.
    data_plane: std::sync::Mutex<Option<e2e_support::DataPlane>>,
    journal_client: gazette::journal::Client,
    /// Path to the gazette fragment store, used by reset_data_plane.
    fragment_root: std::path::PathBuf,
    service: shuffle::Service,
    materialization_spec: flow::MaterializationSpec,
    capture_spec: flow::CaptureSpec,
    log_dir: tempfile::TempDir,
    _server_handle: tokio::task::JoinHandle<()>,
    runtime: tokio::runtime::Runtime,
    case_counter: std::sync::atomic::AtomicU64,
}

static HARNESS: std::sync::OnceLock<SharedHarness> = std::sync::OnceLock::new();

fn get_harness() -> &'static SharedHarness {
    HARNESS.get_or_init(|| {
        // Initialize tracing globally (before DataPlane, which uses set_default
        // which is thread-local and won't cover tokio worker threads).
        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_env_filter(
                tracing_subscriber::EnvFilter::builder()
                    .with_default_directive(tracing::level_filters::LevelFilter::INFO.into())
                    .from_env()
                    .expect("parsing RUST_LOG filter"),
            )
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build tokio runtime");

        let (data_plane, service, materialization_spec, capture_spec, log_dir, server_handle) =
            runtime.block_on(async {
                let source =
                    build::arg_source_to_url("./tests/shuffle_fuzz.flow.yaml", false).unwrap();
                let build_output = Arc::new(
                    build::for_local_test(&source, true)
                        .await
                        .into_result()
                        .expect("build catalog fixture"),
                );

                let materialization_spec = build_output
                    .built
                    .built_materializations
                    .get_by_key(&models::Materialization::new(
                        "testing/fuzz-materialization",
                    ))
                    .expect("built materialization")
                    .spec
                    .as_ref()
                    .expect("materialization spec")
                    .clone();

                let capture_spec = build_output
                    .built
                    .built_captures
                    .get_by_key(&models::Capture::new("testing/fuzz-capture"))
                    .expect("built capture")
                    .spec
                    .as_ref()
                    .expect("capture spec")
                    .clone();

                let data_plane = e2e_support::DataPlane::start(Default::default())
                    .await
                    .expect("DataPlane start");

                let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .expect("bind shuffle server");
                let endpoint = format!("http://{}", listener.local_addr().unwrap());

                let factory: gazette::journal::ClientFactory = Arc::new({
                    let journal_client = data_plane.journal_client.clone();
                    move |_authz_sub, _authz_obj| journal_client.clone()
                });
                let service = shuffle::Service::new(endpoint, factory);

                let server = service.clone().build_tonic_server();
                let server_handle = tokio::spawn(async move {
                    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
                    server
                        .serve_with_incoming(incoming)
                        .await
                        .expect("shuffle server error")
                });

                let log_dir = tempfile::tempdir().expect("create temp dir");

                (
                    data_plane,
                    service,
                    materialization_spec,
                    capture_spec,
                    log_dir,
                    server_handle,
                )
            });

        let journal_client = data_plane.journal_client.clone();
        let fragment_root = data_plane.gazette.fragment_root.clone();

        SharedHarness {
            data_plane: std::sync::Mutex::new(Some(data_plane)),
            journal_client,
            fragment_root,
            service,
            materialization_spec,
            capture_spec,
            log_dir,
            _server_handle: server_handle,
            runtime,
            case_counter: std::sync::atomic::AtomicU64::new(0),
        }
    })
}

/// Reset the data plane to a clean state between test cases.
async fn reset_data_plane(harness: &SharedHarness) -> anyhow::Result<()> {
    e2e_support::reset_journals(&harness.journal_client, &harness.fragment_root).await
}

// ---------------------------------------------------------------------------
// Publisher helpers
// ---------------------------------------------------------------------------

fn build_task(spec: &flow::MaterializationSpec) -> shuffle::proto::Task {
    shuffle::proto::Task {
        task: Some(shuffle::proto::task::Task::Materialization(spec.clone())),
    }
}

fn build_shards(
    count: u32,
    endpoint: &str,
    directory: &std::path::Path,
) -> Vec<shuffle::proto::Shard> {
    (0..count)
        .map(|i| {
            let key_begin = if i == 0 {
                0
            } else {
                ((i as u64 * (u32::MAX as u64 + 1)) / count as u64) as u32
            };
            let key_end = if i == count - 1 {
                u32::MAX
            } else {
                (((i + 1) as u64 * (u32::MAX as u64 + 1)) / count as u64 - 1) as u32
            };

            shuffle::proto::Shard {
                range: Some(flow::RangeSpec {
                    key_begin,
                    key_end,
                    r_clock_begin: 0,
                    r_clock_end: u32::MAX,
                }),
                endpoint: endpoint.to_string(),
                directory: directory.to_str().unwrap().to_string(),
            }
        })
        .collect()
}

fn make_producer_id(idx: u8) -> uuid::Producer {
    // Multicast bit (bit 0 of byte 0) must be set per RFC 4122.
    uuid::Producer::from_bytes([0x01, idx, idx, idx, idx, idx])
}

fn make_publisher(
    capture_spec: &flow::CaptureSpec,
    journal_client: &gazette::journal::Client,
    producer: uuid::Producer,
) -> publisher::Publisher {
    let factory: gazette::journal::ClientFactory = Arc::new({
        let journal_client = journal_client.clone();
        move |_authz_sub, _authz_obj| journal_client.clone()
    });

    let bindings = publisher::Binding::from_capture_spec(capture_spec)
        .expect("build bindings from capture spec");

    publisher::Publisher::new(
        String::new(), // Empty AuthZ subject.
        bindings,
        factory,
        producer,
        uuid::Clock::default(),
    )
}

// ---------------------------------------------------------------------------
// Producer state
// ---------------------------------------------------------------------------

struct ProdState {
    publisher: publisher::Publisher,
    producer: uuid::Producer,
    counter: u64,
    last_committed_clock: uuid::Clock,
    /// Per-journal clock of the last commit ACK written by this producer.
    /// On rollback, each journal's ACK must use *its own* last-committed clock
    /// to correctly roll back. Otherwise, we'd generate AckPartialCommit errors.
    journal_committed_clocks: HashMap<String, uuid::Clock>,
}

// ---------------------------------------------------------------------------
// Oracle
// ---------------------------------------------------------------------------

struct Oracle {
    /// Per-producer: committed (counter, partition) pairs accumulated across all rounds.
    committed: HashMap<ProducerId, Vec<(u64, PartitionId)>>,
    /// Per-producer: pending (counter, partition) pairs for open ContinueAck transaction.
    pending: HashMap<ProducerId, Vec<(u64, PartitionId)>>,
    /// Producers permanently retired via rollback.
    retired: HashSet<ProducerId>,
    /// Expected (counter, partition) pairs committed in the current round, per producer.
    round_expected: HashMap<ProducerId, Vec<(u64, PartitionId)>>,
}

impl Oracle {
    fn new() -> Self {
        Oracle {
            committed: HashMap::new(),
            pending: HashMap::new(),
            retired: HashSet::new(),
            round_expected: HashMap::new(),
        }
    }

    fn record_outside_txn(&mut self, producer: ProducerId, counter: u64, partition: PartitionId) {
        self.committed
            .entry(producer)
            .or_default()
            .push((counter, partition));
        self.round_expected
            .entry(producer)
            .or_default()
            .push((counter, partition));
    }

    fn record_continue(&mut self, producer: ProducerId, counter: u64, partition: PartitionId) {
        self.pending
            .entry(producer)
            .or_default()
            .push((counter, partition));
    }

    fn record_ack_commit(&mut self, producer: ProducerId) {
        let pending = self.pending.remove(&producer).unwrap_or_default();
        self.committed.entry(producer).or_default().extend(&pending);
        self.round_expected
            .entry(producer)
            .or_default()
            .extend(&pending);
    }

    fn record_ack_rollback(&mut self, producer: ProducerId) {
        self.pending.remove(&producer);
        self.retired.insert(producer);
    }

    fn clear_round(&mut self) {
        self.round_expected.clear();
    }

    /// Verify scanned entries match expected for this round.
    /// Returns Ok(()) on success, Err(message) on failure.
    fn verify_round(&self, scanned: &[(ProducerId, u64, PartitionId)]) -> Result<(), String> {
        // Build expected set.
        let mut expected: HashSet<(ProducerId, u64, PartitionId)> = HashSet::new();
        for (&prod, entries) in &self.round_expected {
            for &(counter, partition) in entries {
                expected.insert((prod, counter, partition));
            }
        }

        // Build actual set.
        let actual: HashSet<(ProducerId, u64, PartitionId)> = scanned.iter().cloned().collect();

        if expected == actual {
            return Ok(());
        }

        let missing: Vec<_> = expected.difference(&actual).collect();
        let extra: Vec<_> = actual.difference(&expected).collect();

        Err(format!(
            "Oracle mismatch!\n  Missing (expected but not scanned): {:?}\n  Extra (scanned but not expected): {:?}",
            missing, extra
        ))
    }
}

// ---------------------------------------------------------------------------
// Action execution
// ---------------------------------------------------------------------------

/// Write a round's actions to journals. Returns commit clocks for producers
/// that committed in this round (for polling termination).
async fn write_actions(
    producers: &mut HashMap<ProducerId, ProdState>,
    actions: &HashMap<ProducerId, Action>,
) -> HashMap<ProducerId, uuid::Clock> {
    let mut commit_clocks: HashMap<ProducerId, uuid::Clock> = HashMap::new();

    for (&prod_id, action) in actions {
        let state = producers.get_mut(&prod_id).unwrap();

        match action {
            Action::OutsideTxn { partition } => {
                let counter = state.counter;
                let partition = *partition;
                let mut captured_clock = uuid::Clock::default();

                let appender = state
                    .publisher
                    .enqueue(
                        |u| {
                            let (_, clock, _) = uuid::parse(u).unwrap();
                            captured_clock = clock;
                            (
                                0,
                                serde_json::json!({
                                    "_meta": {"uuid": u.to_string()},
                                    "id": format!("p{prod_id}-c{counter}"),
                                    "category": PARTITION_CATEGORIES[partition as usize],
                                    "counter": counter,
                                }),
                            )
                        },
                        uuid::Flags::OUTSIDE_TXN,
                    )
                    .await
                    .unwrap();

                state.counter += 1;
                state.last_committed_clock = captured_clock;
                state
                    .journal_committed_clocks
                    .insert(appender.journal().to_string(), captured_clock);

                state.publisher.flush().await.unwrap();
                commit_clocks.insert(prod_id, captured_clock);
            }
            Action::ContinueAck { continues } => {
                for &partition in continues {
                    let counter = state.counter;
                    state
                        .publisher
                        .enqueue(
                            |u| {
                                (
                                    0,
                                    serde_json::json!({
                                        "_meta": {"uuid": u.to_string()},
                                        "id": format!("p{prod_id}-c{counter}"),
                                        "category": PARTITION_CATEGORIES[partition as usize],
                                        "counter": counter,
                                    }),
                                )
                            },
                            uuid::Flags::CONTINUE_TXN,
                        )
                        .await
                        .unwrap();
                    state.counter += 1;
                }
                state.publisher.flush().await.unwrap();
                let (producer_id, commit_clock, journals) = state.publisher.commit_intents();
                let intents = publisher::intents::build_transaction_intents(&[(
                    producer_id,
                    commit_clock,
                    journals,
                )]);
                for (journal, _) in &intents {
                    state
                        .journal_committed_clocks
                        .insert(journal.clone(), commit_clock);
                }
                state.publisher.write_intents(intents).await.unwrap();
                state.last_committed_clock = commit_clock;
                commit_clocks.insert(prod_id, commit_clock);
            }
            Action::ContinueRollback { continues } => {
                for &partition in continues {
                    let counter = state.counter;
                    state
                        .publisher
                        .enqueue(
                            |u| {
                                (
                                    0,
                                    serde_json::json!({
                                        "_meta": {"uuid": u.to_string()},
                                        "id": format!("p{prod_id}-c{counter}"),
                                        "category": PARTITION_CATEGORIES[partition as usize],
                                        "counter": counter,
                                    }),
                                )
                            },
                            uuid::Flags::CONTINUE_TXN,
                        )
                        .await
                        .unwrap();
                    state.counter += 1;
                }
                state.publisher.flush().await.unwrap();
                let (_producer_id, _tick_clock, journals) = state.publisher.commit_intents();

                // Send rollback ACK only to journals that have received a
                // prior commit from this producer. Each journal's ACK must use
                // that journal's own last-committed clock — not the global one —
                // because different journals may have been committed at different
                // times. Using the wrong clock causes AckPartialCommit errors
                // when the global clock falls between a journal's last_commit
                // and its pending max_continue.
                let rollback_acks: Vec<(String, bytes::Bytes)> = journals
                    .iter()
                    .filter_map(|journal| {
                        let clock = state.journal_committed_clocks.get(journal)?;
                        let ack_uuid = uuid::build(state.producer, *clock, uuid::Flags::ACK_TXN);
                        let doc = serde_json::json!({
                            "_meta": { "uuid": ack_uuid },
                            "is_ack": true,
                        });
                        let mut buf = serde_json::to_vec(&doc).unwrap();
                        buf.push(b'\n');
                        Some((journal.clone(), bytes::Bytes::from(buf)))
                    })
                    .collect();
                state.publisher.write_intents(rollback_acks).await.unwrap();
            }
        }
    }

    commit_clocks
}

/// Record a round's actions into the oracle, using counter values that
/// match what write_actions will/did produce.
fn record_oracle_with_counters(
    oracle: &mut Oracle,
    actions: &HashMap<ProducerId, Action>,
    counter_starts: &HashMap<ProducerId, u64>,
) {
    for (&prod_id, action) in actions {
        let mut counter = counter_starts.get(&prod_id).copied().unwrap_or(0);

        match action {
            Action::OutsideTxn { partition } => {
                oracle.record_outside_txn(prod_id, counter, *partition);
            }
            Action::ContinueAck { continues } => {
                for &partition in continues {
                    oracle.record_continue(prod_id, counter, partition);
                    counter += 1;
                }
                oracle.record_ack_commit(prod_id);
            }
            Action::ContinueRollback { continues } => {
                for &partition in continues {
                    oracle.record_continue(prod_id, counter, partition);
                    counter += 1;
                }
                oracle.record_ack_rollback(prod_id);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Polling and scanning helpers
// ---------------------------------------------------------------------------

/// Check whether every committing producer is visible in at least one journal.
fn polling_complete(
    frontier: &shuffle::Frontier,
    commit_clocks: &HashMap<ProducerId, uuid::Clock>,
) -> bool {
    for (&prod_id, &expected_clock) in commit_clocks {
        let producer = make_producer_id(prod_id);
        let visible = frontier.journals.iter().any(|jf| {
            jf.producers
                .iter()
                .any(|pf| pf.producer == producer && pf.last_commit >= expected_clock)
        });
        if !visible {
            return false;
        }
    }
    true
}

/// Client-side hints projection: project last_commit → hinted_commit.
fn project_hints(round_frontier: &shuffle::Frontier) -> shuffle::Frontier {
    let journals: Vec<shuffle::JournalFrontier> = round_frontier
        .journals
        .iter()
        .map(|jf| shuffle::JournalFrontier {
            binding: jf.binding,
            journal: jf.journal.clone(),
            producers: jf
                .producers
                .iter()
                .map(|pf| shuffle::ProducerFrontier {
                    producer: pf.producer,
                    last_commit: uuid::Clock::zero(),
                    hinted_commit: pf.last_commit,
                    offset: 0,
                })
                .collect(),
            bytes_read_delta: 0,
            bytes_behind_delta: 0,
        })
        .collect();

    // Each producer has `last_commit: zero` and a non-zero `hinted_commit`,
    // so the unresolved count is the total producer count across journals.
    let unresolved_hints = journals.iter().map(|jf| jf.producers.len()).sum();
    shuffle::Frontier {
        unresolved_hints,
        journals,
        flushed_lsn: vec![],
    }
}

/// Parse a scanned entry into (ProducerId, counter, PartitionId).
fn parse_entry(entry: &shuffle::log::reader::Entry) -> (ProducerId, u64, PartitionId) {
    let doc = serde_json::to_value(doc::SerPolicy::noop().on(entry.doc.doc.get()))
        .expect("serialize doc");

    let counter = doc["counter"]
        .as_u64()
        .expect("doc should have counter field");
    let category = doc["category"]
        .as_str()
        .expect("doc should have category field");
    let partition_id = category
        .strip_prefix("cat-")
        .expect("category should start with cat-")
        .parse::<u8>()
        .expect("category suffix should be numeric");

    // Extract producer index from the document ID field (e.g., "p2-c5" → 2).
    let id = doc["id"].as_str().expect("doc should have id field");
    let prod_str = id
        .strip_prefix("p")
        .and_then(|s| s.split("-c").next())
        .expect("id should be p{N}-c{M}");
    let producer_id: u8 = prod_str.parse().expect("producer id should be numeric");

    (producer_id, counter, partition_id)
}

/// Drive FrontierScan, collecting all committed entries.
fn collect_scanned_entries(
    frontier: &shuffle::Frontier,
    log_dir: &std::path::Path,
    shard_state: &mut Vec<Option<(Reader, VecDeque<Remainder>)>>,
) -> Vec<(ProducerId, u64, PartitionId)> {
    let mut entries = Vec::new();

    for (shard_index, state_slot) in shard_state.iter_mut().enumerate() {
        let (reader, remainders) = state_slot
            .take()
            .unwrap_or_else(|| (Reader::new(log_dir, shard_index as u32), VecDeque::new()));

        let mut scan = FrontierScan::new(frontier.clone(), reader, remainders)
            .unwrap_or_else(|e| panic!("FrontierScan::new for shard {shard_index}: {e}"));

        while scan
            .advance_block()
            .unwrap_or_else(|e| panic!("advance_block for shard {shard_index}: {e}"))
        {
            for entry in scan.block_iter() {
                entries.push(parse_entry(&entry));
            }
        }

        let (_, reader, remainders) = scan.into_parts();
        *state_slot = Some((reader, remainders));
    }

    entries
}

// ---------------------------------------------------------------------------
// Test case execution
// ---------------------------------------------------------------------------

async fn run_test_case(harness: &SharedHarness, test_case: TestCase) -> Result<(), String> {
    reset_data_plane(harness)
        .await
        .map_err(|e| format!("data_plane.reset: {e}"))?;

    let case_id = harness
        .case_counter
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let case_log_dir = harness.log_dir.path().join(format!("case-{case_id}"));
    std::fs::create_dir_all(&case_log_dir).unwrap();

    let result = run_test_case_inner(harness, &test_case, &case_log_dir).await;

    // Clean up log directory.
    let _ = std::fs::remove_dir_all(&case_log_dir);

    result
}

async fn run_test_case_inner(
    harness: &SharedHarness,
    test_case: &TestCase,
    log_dir: &std::path::Path,
) -> Result<(), String> {
    // Create producers.
    let mut producers: HashMap<ProducerId, ProdState> = HashMap::new();
    for idx in 0..test_case.num_producers {
        let producer = make_producer_id(idx as u8);
        let publisher = make_publisher(&harness.capture_spec, &harness.journal_client, producer);
        producers.insert(
            idx as u8,
            ProdState {
                publisher,
                producer,
                counter: 0,
                last_committed_clock: uuid::Clock::default(),
                journal_committed_clocks: HashMap::new(),
            },
        );
    }

    let mut oracle = Oracle::new();
    let task = build_task(&harness.materialization_spec);
    let shards = build_shards(
        test_case.num_shards as u32,
        harness.service.peer_endpoint(),
        log_dir,
    );

    let mut recovery = shuffle::Frontier::default();
    tracing::debug!("  opening initial session...");
    let mut session = shuffle::SessionClient::open(
        &harness.service,
        task.clone(),
        shards.clone(),
        recovery.clone(),
    )
    .await
    .map_err(|e| format!("SessionClient::open: {e}"))?;
    tracing::debug!("  session opened");

    let mut shard_state: Vec<Option<(Reader, VecDeque<Remainder>)>> =
        (0..test_case.num_shards).map(|_| None).collect();
    let mut next_round_pre_written = false;

    for (round_idx, round) in test_case.rounds.iter().enumerate() {
        let is_last = round_idx == test_case.rounds.len() - 1;

        tracing::debug!(
            round_idx,
            crash=round.crash,
            actions=?round.actions,
            "beginning round execution",
        );

        // STEP 1: WRITE (skip if pre-written in previous round's step 5).
        if !next_round_pre_written {
            tracing::debug!("    step 1: writing actions...");
            write_actions(&mut producers, &round.actions).await;
        } else {
            tracing::debug!("    step 1: skipped (pre-written)");
        }
        next_round_pre_written = false;

        // Record oracle for current round.
        // Counter was already incremented by write_actions for data-bearing
        // actions. We need the counters as they were BEFORE write_actions.
        let counter_starts: HashMap<ProducerId, u64> = producers
            .iter()
            .map(|(&id, state)| {
                let data_actions = match round.actions.get(&id) {
                    Some(Action::OutsideTxn { .. }) => 1u64,
                    Some(Action::ContinueAck { continues })
                    | Some(Action::ContinueRollback { continues }) => continues.len() as u64,
                    None => 0,
                };
                (id, state.counter - data_actions)
            })
            .collect();
        record_oracle_with_counters(&mut oracle, &round.actions, &counter_starts);

        // Compute commit clocks for polling termination.
        let commit_clocks: HashMap<ProducerId, uuid::Clock> = round
            .actions
            .iter()
            .filter(|(_, action)| {
                matches!(
                    action,
                    Action::OutsideTxn { .. } | Action::ContinueAck { .. }
                )
            })
            .map(|(&id, _)| (id, producers[&id].last_committed_clock))
            .collect();

        // STEP 2: INIT ROUND FRONTIER.
        let mut round_frontier = shuffle::Frontier {
            journals: vec![],
            flushed_lsn: recovery.flushed_lsn.clone(),
            unresolved_hints: 0,
        };

        // STEP 3: POLL CHECKPOINTS.
        tracing::debug!(?commit_clocks, "polling for committing producers");

        if !commit_clocks.is_empty() {
            loop {
                let delta = session
                    .next_checkpoint()
                    .await
                    .map_err(|e| format!("next_checkpoint: {e}"))?;

                tracing::debug!(?delta, "session.next_checkpoint returned delta");

                round_frontier = round_frontier.reduce(delta);
                tracing::debug!(
                    ?round_frontier,
                    "updated round_frontier after reducing delta"
                );

                if round_frontier.unresolved_hints == 0
                    && polling_complete(&round_frontier, &commit_clocks)
                {
                    tracing::debug!("polling is complete");
                    break;
                }
            }
        }

        // STEP 4: PROJECT HINTS INTO RECOVERY.
        let projection = project_hints(&round_frontier);
        recovery = recovery.reduce(projection);

        // STEP 5: WRITE NEXT ROUND (if not last).
        if !is_last {
            let next_round = &test_case.rounds[round_idx + 1];
            write_actions(&mut producers, &next_round.actions).await;
            next_round_pre_written = true;
        }

        // STEP 6: MAYBE CRASH AND RESTART.
        if round.crash {
            session
                .close()
                .await
                .map_err(|e| format!("session.close on crash: {e}"))?;

            // Reset aspects of the cumulative recovery Frontier that aren't
            // intended to be meaningful across sessions.
            recovery.flushed_lsn = vec![];
            recovery.journals.iter_mut().for_each(|jf| {
                jf.bytes_behind_delta = 0;
            });

            shard_state = (0..test_case.num_shards).map(|_| None).collect();

            session = shuffle::SessionClient::open(
                &harness.service,
                task.clone(),
                shards.clone(),
                recovery.clone(),
            )
            .await
            .map_err(|e| format!("SessionClient::open on recovery: {e}"))?;

            round_frontier = shuffle::Frontier {
                journals: vec![],
                flushed_lsn: recovery.flushed_lsn.clone(),
                unresolved_hints: 0,
            };

            if recovery.unresolved_hints != 0 {
                // Loop past peeks until the recovery checkpoint fully resolves.
                loop {
                    let recovery_delta = session
                        .next_checkpoint()
                        .await
                        .map_err(|e| format!("recovery next_checkpoint: {e}"))?;

                    round_frontier = round_frontier.reduce(recovery_delta);

                    if round_frontier.unresolved_hints == 0 {
                        break;
                    }
                }
            }
        }

        // STEP 7: SCAN.
        // Skip scanning when there's no log data yet (flushed_lsn is empty before
        // any checkpoint has been received). FrontierScan::new requires flushed_lsn
        // to contain an entry for our shard_index.
        let scanned = if round_frontier.flushed_lsn.is_empty() {
            vec![]
        } else {
            collect_scanned_entries(&round_frontier, log_dir, &mut shard_state)
        };
        oracle.verify_round(&scanned).map_err(|e| {
            format!("Round {round_idx} verification failed: {e}\n  Test case: {test_case:?}")
        })?;
        oracle.clear_round();

        // STEP 8: ACCUMULATE.
        recovery = recovery.reduce(round_frontier);
    }

    session
        .close()
        .await
        .map_err(|e| format!("final session.close: {e}"))?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Quickcheck entry point
// ---------------------------------------------------------------------------

#[test]
fn fuzz_shuffle_pipeline() {
    // Run quickcheck, catching panics so we can always tear down.
    let result = std::panic::catch_unwind(|| {
        quickcheck::QuickCheck::new().quickcheck(prop as fn(TestCase) -> quickcheck::TestResult)
    });

    // Tear down the data plane gracefully (OnceLock statics are never dropped).
    let harness = get_harness();
    if let Some(data_plane) = harness.data_plane.lock().unwrap().take() {
        harness.runtime.block_on(async {
            if let Err(e) = data_plane.graceful_stop().await {
                eprintln!("DataPlane graceful_stop error: {e}");
            }
        });
    }

    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

fn prop(input: TestCase) -> quickcheck::TestResult {
    // Discard empty test cases.
    if input.rounds.is_empty() || input.rounds.iter().all(|r| r.actions.is_empty()) {
        return quickcheck::TestResult::discard();
    }
    let harness = get_harness();

    match harness.runtime.block_on(run_test_case(harness, input)) {
        Ok(()) => quickcheck::TestResult::passed(),
        Err(_msg) => quickcheck::TestResult::failed(),
    }
}
