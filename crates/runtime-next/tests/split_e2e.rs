//! End-to-end integration test of automatic journal splitting: a real
//! etcd + gazette broker, a real `Publisher`, and real broker flow-control
//! driving `SplitPolicy` → `shard::start_due_split` → `shard::finish_split`
//! until the collection's partitions converge to the min-width floor.
//!
//! The test owns the per-transaction loop that the capture / derive actors
//! run in production: publish, drain throttle samples, observe, dispatch at
//! most one split, apply its outcome. The policy uses an aggressive test
//! `Config` so journals go due within a couple of throttled transactions —
//! production timing constants are not touched.
//!
//! Spawns real `etcd` (on PATH) and `~/go/bin/gazette` child processes via
//! `e2e_support::DataPlane`, exactly like the `shuffle` scenario tests that
//! run under `ci:nextest-run`.
//!
//! Beyond the cascade and concurrent CAS-race tests, this module also covers:
//! a `Lost` CAS outcome leaving the journal layout untouched and writable;
//! document completeness (no loss, no duplication) across a cascade of
//! splits; and isolation of splits to a single logical partition of a
//! partitioned collection.

use runtime_next::shard::split_policy::SplitPolicy;
use runtime_next::{Publisher, shard};

use proto_gazette::{broker, uuid};
use publisher::SplitOutcome;
use std::collections::{BTreeSet, VecDeque};
use std::time::{Duration, Instant};

/// Per-partition append-rate limit. Far below the test's offered load, and
/// above gazette's `MinAppendRate` (64 KiB/s): below that floor a throttled
/// append aborts with ErrFlowControlUnderflow instead of merely delaying.
const TEST_MAX_APPEND_RATE: i64 = 1 << 17; // 128 KiB/s.

/// Documents per over-rate transaction, and each document's filler payload
/// size: ~256 KiB per transaction of uniformly-keyed documents.
const DOCS_PER_TXN: usize = 64;
const PAYLOAD_LEN: usize = 4096;

/// Documents per targeted drain transaction: ~192 KiB aimed at a single
/// partition, exceeding both its one-second burst allowance (128 KiB) and
/// what its rate refills over the transaction.
const DRAIN_DOCS_PER_TXN: usize = 48;

/// All eight leaves at the `MIN_PARTITION_WIDTH` floor.
const FLOOR_PARTITIONS: usize = 8;

/// A generator of document keys, bucketed by which floor-width partition
/// the key's hash routes to. Keys flow through the same packed-key encoding
/// and hash as the publisher's own partition mapping.
struct KeyPool {
    seq: u64,
    buckets: [VecDeque<String>; FLOOR_PARTITIONS],
    key_extractors: Vec<doc::Extractor>,
    packed_buf: bytes::BytesMut,
}

impl KeyPool {
    fn new(spec: &proto_flow::flow::CollectionSpec) -> Self {
        let key_extractors =
            extractors::for_key(&spec.key, &spec.projections, &doc::SerPolicy::noop())
                .expect("key extractors");
        Self {
            seq: 0,
            buckets: Default::default(),
            key_extractors,
            packed_buf: bytes::BytesMut::new(),
        }
    }

    /// Next sequential id, and its packed-key hash.
    fn next(&mut self) -> (String, u32) {
        let id = format!("{:016x}", self.seq);
        self.seq += 1;

        self.packed_buf.clear();
        doc::Extractor::extract_all(
            &serde_json::json!({ "id": &id }),
            &self.key_extractors,
            doc::Encoding::Packed,
            &mut self.packed_buf,
            None,
        );
        let hash = doc::Extractor::packed_hash(&self.packed_buf);

        (id, hash)
    }

    /// Next id hashing into floor-width bucket `bucket` (rejection sampling).
    fn next_for_bucket(&mut self, bucket: usize) -> String {
        loop {
            if let Some(id) = self.buckets[bucket].pop_front() {
                return id;
            }
            let (id, hash) = self.next();
            // Top three hash bits index the 2^29 floor-width bucket.
            self.buckets[(hash >> 29) as usize].push_back(id);
        }
    }
}

/// Publish one transaction of `ids` as documents.
async fn publish_txn(publisher: &mut Publisher, ids: &[String], payload: &str) {
    let docs = ids
        .iter()
        .map(|id| serde_json::json!({"id": id, "payload": payload}))
        .collect();
    publish_txn_docs(publisher, docs).await;
}

/// Publish one transaction of arbitrary collection documents, stamping each
/// with its assigned UUID under `/_meta/uuid`.
async fn publish_txn_docs(publisher: &mut Publisher, docs: Vec<serde_json::Value>) {
    let Publisher::Real(inner) = publisher else {
        unreachable!("test publisher is Real");
    };
    for mut doc in docs {
        inner
            .enqueue(
                |uuid| {
                    doc["_meta"] = serde_json::json!({"uuid": uuid.to_string()});
                    // Binding zero of the inner publisher is the fixed
                    // ops-stats journal; our collection is binding one.
                    Ok((1, doc))
                },
                uuid::Flags::OUTSIDE_TXN,
            )
            .await
            .expect("enqueue");
    }
    inner.flush().await.expect("flush");
}

/// Drain this transaction's throttle samples into owned pairs, releasing the
/// publisher borrow so `start_due_split` can take it afterwards.
fn take_samples(publisher: &mut Publisher) -> Vec<(String, bool)> {
    publisher
        .take_throttle_samples()
        .into_iter()
        .map(|s| (s.journal_name.to_string(), s.throttled))
        .collect()
}

fn observe(policy: &mut SplitPolicy, samples: &[(String, bool)], now: Instant) {
    shard::observe_throttle_samples(
        policy,
        samples
            .iter()
            .map(|(journal_name, throttled)| publisher::ThrottleSample {
                journal_name,
                throttled: *throttled,
            }),
        now,
    );
}

/// One turn of the actor's dispatch glue: start at most one due split, await
/// its detached future, and apply the outcome. Records `Split` / `AtFloor`
/// outcomes. A `Lost` outcome is tolerated: the detached split CAS'es on the
/// partition watch's revision, which can briefly lag a just-applied split —
/// the loop simply re-evaluates against the refreshed watch.
async fn dispatch_one_split(
    policy: &mut SplitPolicy,
    publisher: &Publisher,
    now: Instant,
    split_count: &mut usize,
    at_floor: &mut BTreeSet<String>,
) {
    let Some(split) = shard::start_due_split(policy, publisher, now) else {
        return;
    };
    let (journal, outcome) = split.await;
    match &outcome {
        Ok(publisher::SplitOutcome::Split) => *split_count += 1,
        Ok(publisher::SplitOutcome::AtFloor) => {
            at_floor.insert(journal.clone());
        }
        Ok(publisher::SplitOutcome::Lost) => (),
        Ok(publisher::SplitOutcome::Transient) => {
            panic!("unexpected Transient outcome for {journal}")
        }
        Err(status) => panic!("split of {journal} failed: {status}"),
    }
    shard::finish_split(policy, &journal, outcome, Instant::now());
}

/// List the collection's partitions from the broker, as
/// `(name, key_begin, key_end)` ordered by key range.
async fn list_partitions(
    client: &gazette::journal::Client,
    prefix: &str,
) -> Vec<(String, u32, u32)> {
    let response = client
        .list(broker::ListRequest {
            selector: Some(broker::LabelSelector {
                include: Some(labels::build_set([("name:prefix", prefix)])),
                exclude: None,
            }),
            ..Default::default()
        })
        .await
        .expect("list partitions");

    let mut parts: Vec<_> = response
        .journals
        .into_iter()
        .map(|journal| {
            let spec = journal.spec.expect("listed journal has spec");
            let (begin, end) =
                labels::partition::decode_key_range_labels(spec.labels.as_ref().unwrap())
                    .expect("partition has key-range labels");
            (spec.name, begin, end)
        })
        .collect();
    parts.sort_by_key(|(_, begin, _)| *begin);
    parts
}

/// Start a hermetic DataPlane and build fixture collection `collection`,
/// returning a client factory which routes every binding to the DataPlane's
/// broker.
async fn start_fixture(
    collection: &str,
) -> (
    e2e_support::DataPlane,
    proto_flow::flow::CollectionSpec,
    gazette::journal::ClientFactory,
) {
    let data_plane = e2e_support::DataPlane::start(Default::default())
        .await
        .expect("DataPlane start");

    let source = build::arg_source_to_url("./tests/split_e2e.flow.yaml", false).unwrap();
    let build::Output { built, .. } = build::for_local_test(&source, true)
        .await
        .into_result()
        .expect("fixture build");

    let spec = built
        .built_collections
        .get_key(&models::Collection::new(collection))
        .expect("built collection")
        .spec
        .clone()
        .expect("collection spec");

    let factory: gazette::journal::ClientFactory = std::sync::Arc::new({
        let client = data_plane.journal_client.clone();
        move |_authz_sub, _authz_obj| client.clone()
    });

    (data_plane, spec, factory)
}

/// Aggressive policy configuration: a journal is due on its second throttled
/// sample (the first sample of a tracked journal has dt=0 and can't move the
/// EWMA), with no cold-start span and no cooldown. The min-width floor — not
/// timing — is what terminates splitting.
fn aggressive_config() -> runtime_next::shard::split_policy::Config {
    runtime_next::shard::split_policy::Config {
        tau: Duration::from_millis(100),
        threshold: 0.25,
        cooldown: Duration::ZERO,
        min_observation_span: Duration::ZERO,
        max_sample_dt: Duration::from_secs(30),
        max_staleness: Duration::MAX,
    }
}

/// Fetch a journal's spec and mod_revision by exact name.
async fn fetch_journal(
    client: &gazette::journal::Client,
    name: &str,
) -> (broker::JournalSpec, i64) {
    let response = client
        .list(broker::ListRequest {
            selector: Some(broker::LabelSelector {
                include: Some(labels::build_set([("name", name)])),
                exclude: None,
            }),
            ..Default::default()
        })
        .await
        .expect("list journal by name");

    let journal = response
        .journals
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("journal {name} is not listed"));
    (
        journal.spec.expect("listed journal has spec"),
        journal.mod_revision,
    )
}

/// List the collection's partition JournalSpecs from the broker, ordered by name.
async fn list_partition_specs(
    client: &gazette::journal::Client,
    prefix: &str,
) -> Vec<broker::JournalSpec> {
    let response = client
        .list(broker::ListRequest {
            selector: Some(broker::LabelSelector {
                include: Some(labels::build_set([("name:prefix", prefix)])),
                exclude: None,
            }),
            ..Default::default()
        })
        .await
        .expect("list partitions");

    let mut specs: Vec<_> = response
        .journals
        .into_iter()
        .map(|journal| journal.spec.expect("listed journal has spec"))
        .collect();
    specs.sort_by(|a, b| a.name.cmp(&b.name));
    specs
}

/// Read the complete current content of `journal`, parsing every document.
/// Non-blocking: returns once the read catches up to the journal's write head.
async fn read_all_docs(client: &gazette::journal::Client, journal: &str) -> Vec<serde_json::Value> {
    use futures::StreamExt;
    use gazette::journal::ReadJsonLine;

    let mut lines = client.clone().read_json_lines(
        broker::ReadRequest {
            journal: journal.to_string(),
            block: false,
            ..Default::default()
        },
        0,
    );
    let policy = doc::SerPolicy::noop();
    let mut docs = Vec::new();

    while let Some(line) = lines.next().await {
        match line {
            Ok(ReadJsonLine::Doc { root, .. }) => docs.push(
                serde_json::to_value(policy.on(root.get())).expect("document serializes to JSON"),
            ),
            Ok(ReadJsonLine::Meta(_)) => (),
            // The broker ends a non-blocking proxy read at the write head
            // with OffsetNotYetAvailable: the journal is fully consumed.
            Err(gazette::RetryError {
                inner: gazette::Error::BrokerStatus(broker::Status::OffsetNotYetAvailable),
                ..
            }) => break,
            Err(gazette::RetryError { attempt, inner }) if inner.is_transient() => {
                tracing::warn!(
                    ?inner,
                    attempt,
                    journal,
                    "transient read error (will retry)"
                );
            }
            Err(gazette::RetryError { inner, .. }) => panic!("reading {journal}: {inner}"),
        }
    }
    docs
}

#[tokio::test(flavor = "multi_thread")]
async fn journal_auto_split_converges_to_floor() {
    let (data_plane, mut spec, factory) = start_fixture("testing/autosplit").await;

    // Lower the partition append-rate limit from the production 4 MiB/s to a
    // rate the test trivially exceeds.
    spec.partition_template.as_mut().unwrap().max_append_rate = TEST_MAX_APPEND_RATE;
    let partitions_prefix = format!("{}/", spec.partition_template.as_ref().unwrap().name);

    let mut publisher = Publisher::new_real(
        "test".to_string(),
        runtime_next::new_producer(),
        &factory,
        "testing/ops/stats", // Fixed binding zero; never appended to.
        [&spec],
    )
    .expect("Publisher::new_real");

    let mut policy = SplitPolicy::with_config(aggressive_config());

    let payload: String = "x".repeat(PAYLOAD_LEN);
    let mut keys = KeyPool::new(&spec);
    let mut split_count = 0;
    let mut at_floor = BTreeSet::new();

    // ---- Control: load below the append rate never splits. ----
    // A few single-document transactions stay well inside the journal's
    // one-second burst allowance, so the broker never delays them.
    for _ in 0..5 {
        let ids = vec![keys.next().0];
        publish_txn(&mut publisher, &ids, &payload).await;
        let now = Instant::now();
        let samples = take_samples(&mut publisher);

        assert!(
            samples.iter().all(|(_, throttled)| !throttled),
            "below-rate appends must not be throttled: {samples:?}",
        );
        observe(&mut policy, &samples, now);
        assert!(
            shard::start_due_split(&mut policy, &publisher, now).is_none(),
            "below-rate load must never trigger a split",
        );
    }
    let parts = list_partitions(&data_plane.journal_client, &partitions_prefix).await;
    assert_eq!(parts.len(), 1, "control: a single partition: {parts:?}");

    // ---- Sustained over-rate load splits down to the floor. ----
    // Run the actor loop with uniformly-keyed load until seven midpoint
    // splits have taken the one full-range partition to eight.
    let deadline = Instant::now() + Duration::from_secs(45);
    while split_count < FLOOR_PARTITIONS - 1 {
        assert!(
            Instant::now() < deadline,
            "timed out splitting to the floor: split_count={split_count} at_floor={at_floor:?}",
        );

        let ids: Vec<String> = (0..DOCS_PER_TXN).map(|_| keys.next().0).collect();
        publish_txn(&mut publisher, &ids, &payload).await;
        let now = Instant::now();
        let samples = take_samples(&mut publisher);
        observe(&mut policy, &samples, now);

        dispatch_one_split(
            &mut policy,
            &publisher,
            now,
            &mut split_count,
            &mut at_floor,
        )
        .await;
    }

    // Widths converge to an even tiling at the floor — the uniform-hash
    // behavior that matters, not just "the count grew".
    let parts = list_partitions(&data_plane.journal_client, &partitions_prefix).await;
    assert_eq!(
        parts.len(),
        FLOOR_PARTITIONS,
        "seven midpoint splits yield eight partitions: {parts:?}",
    );
    for (index, (name, begin, end)) in parts.iter().enumerate() {
        let expect_begin = index as u64 * publisher::MIN_PARTITION_WIDTH;
        let expect_end = expect_begin + publisher::MIN_PARTITION_WIDTH - 1;
        assert_eq!(
            (*begin as u64, *end as u64),
            (expect_begin, expect_end),
            "{name} evenly tiles the key space at the min width",
        );
    }

    // ---- Every leaf refuses to split below the floor. ----
    // Under the closed publish loop, a leaf which still holds broker burst
    // credit refills as fast as uniform load drains it, so target each
    // not-yet-evaluated leaf with keys hashing into its own range: a few
    // over-rate transactions drain its credit, it throttles, goes due, and
    // takes the terminal AtFloor off-ramp.
    let deadline = Instant::now() + Duration::from_secs(45);
    while at_floor.len() < FLOOR_PARTITIONS {
        assert!(
            Instant::now() < deadline,
            "timed out evaluating leaves at the floor: at_floor={at_floor:?}",
        );

        let bucket = parts
            .iter()
            .position(|(name, _, _)| !at_floor.contains(name))
            .expect("some leaf is not yet at the floor");
        let ids: Vec<String> = (0..DRAIN_DOCS_PER_TXN)
            .map(|_| keys.next_for_bucket(bucket))
            .collect();

        publish_txn(&mut publisher, &ids, &payload).await;
        let now = Instant::now();
        let samples = take_samples(&mut publisher);
        observe(&mut policy, &samples, now);

        dispatch_one_split(
            &mut policy,
            &publisher,
            now,
            &mut split_count,
            &mut at_floor,
        )
        .await;
    }

    assert_eq!(
        split_count,
        FLOOR_PARTITIONS - 1,
        "no splits below the floor"
    );
    assert_eq!(
        at_floor,
        parts.iter().map(|(name, _, _)| name.clone()).collect(),
        "every leaf partition took the terminal AtFloor off-ramp",
    );

    // ---- After the floor: writes re-route to every leaf; no re-trigger. ----
    // Throttle samples name exactly the journals appended to, so they double
    // as proof that the partition watch re-routed documents into the new
    // (RHS) journals and not only the original full-range journal.
    let mut post_floor_appended = BTreeSet::new();
    for _ in 0..5 {
        let ids: Vec<String> = (0..DOCS_PER_TXN).map(|_| keys.next().0).collect();
        publish_txn(&mut publisher, &ids, &payload).await;
        let now = Instant::now();
        let samples = take_samples(&mut publisher);
        post_floor_appended.extend(samples.iter().map(|(journal, _)| journal.clone()));
        observe(&mut policy, &samples, now);

        assert!(
            shard::start_due_split(&mut policy, &publisher, now).is_none(),
            "journals at the min-width floor must never re-trigger",
        );
    }
    for (name, _, _) in &parts {
        assert!(
            post_floor_appended.contains(name),
            "leaf {name} must receive writes after the split (watch re-route)",
        );
    }
    let parts = list_partitions(&data_plane.journal_client, &partitions_prefix).await;
    assert_eq!(parts.len(), FLOOR_PARTITIONS, "count holds at the floor");

    data_plane.graceful_stop().await.expect("graceful stop");
}

/// Two contending shards race to split the same journal: etcd's CAS admits
/// exactly one Apply, the loser observes `SplitOutcome::Lost` (a completed
/// attempt: cooldown, no re-dispatch), and the loser then proceeds against
/// the now-split layout its watch re-observes.
///
/// Due-ness is fabricated (`threshold: -1.0` makes any observed journal
/// instantly due) — the organic throttle → EWMA → due pipeline is covered by
/// `journal_auto_split_converges_to_floor`. What's real here is the
/// concurrent two-change Apply CAS against the broker's etcd.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_journal_split_loses_cas_race() {
    let (data_plane, spec, factory) = start_fixture("testing/autosplit").await;
    let partitions_prefix = format!("{}/", spec.partition_template.as_ref().unwrap().name);

    // Two publishers stand in for two shards of one task. Each holds its own
    // partition watch and split policy.
    let mut pub_a = Publisher::new_real(
        "test".to_string(),
        runtime_next::new_producer(),
        &factory,
        "testing/ops/stats",
        [&spec],
    )
    .expect("Publisher::new_real");
    let mut pub_b = Publisher::new_real(
        "test".to_string(),
        runtime_next::new_producer(),
        &factory,
        "testing/ops/stats",
        [&spec],
    )
    .expect("Publisher::new_real");

    // Any observed journal is instantly due, and the cooldown is long enough
    // that a completed attempt is observable as suppression.
    let config = runtime_next::shard::split_policy::Config {
        threshold: -1.0,
        min_observation_span: Duration::ZERO,
        cooldown: Duration::from_secs(3600),
        ..Default::default()
    };
    let mut policy_a = SplitPolicy::with_config(config);
    let mut policy_b = SplitPolicy::with_config(config);

    let payload: String = "x".repeat(256);
    let mut keys = KeyPool::new(&spec);

    // Each contender publishes one document: the first maps (and creates)
    // the partition, and both initialize their own partition watch at the
    // same pre-split revision.
    let ids = vec![keys.next().0];
    publish_txn(&mut pub_a, &ids, &payload).await;
    let samples = take_samples(&mut pub_a);
    assert_eq!(samples.len(), 1);
    let parent = samples[0].0.clone();
    // The policy only tracks a journal once it's been throttled at least once,
    // and these tiny appends don't throttle. Fabricate the throttled sample;
    // with `threshold: -1.0` a single throttled observation makes it due.
    policy_a.observe(&parent, true, Instant::now());

    let ids = vec![keys.next().0];
    publish_txn(&mut pub_b, &ids, &payload).await;
    let samples = take_samples(&mut pub_b);
    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0].0, parent);
    policy_b.observe(&parent, true, Instant::now());

    // Dispatch both contenders concurrently. One Split + one Lost is
    // deterministic, not probabilistic: `join!` first-polls both futures in
    // the same task, and `split_partition` captures its CAS revision at the
    // top of the future (its watch is ready, so `ready().await` resolves
    // synchronously) — so both capture the same revision while either Apply
    // is still two RPC round-trips from existing, and later watch refreshes
    // can't alter an already-captured revision. Both-Split would require B
    // to snapshot after A's Apply committed AND after B's watch streamed the
    // new layout; if this assert ever fires, that capture-before-Apply
    // structure changed — investigate rather than tolerate.
    let now = Instant::now();
    let fut_a = shard::start_due_split(&mut policy_a, &pub_a, now).expect("parent is due for a");
    let fut_b = shard::start_due_split(&mut policy_b, &pub_b, now).expect("parent is due for b");
    let ((journal_a, outcome_a), (journal_b, outcome_b)) = futures::join!(fut_a, fut_b);

    assert_eq!(journal_a, parent);
    assert_eq!(journal_b, parent);
    let outcome_a = outcome_a.expect("split RPC of contender a");
    let outcome_b = outcome_b.expect("split RPC of contender b");

    shard::finish_split(&mut policy_a, &journal_a, Ok(outcome_a), Instant::now());
    shard::finish_split(&mut policy_b, &journal_b, Ok(outcome_b), Instant::now());

    let (loser_pub, loser_policy) = match (outcome_a, outcome_b) {
        (SplitOutcome::Split, SplitOutcome::Lost) => (&mut pub_b, &mut policy_b),
        (SplitOutcome::Lost, SplitOutcome::Split) => (&mut pub_a, &mut policy_a),
        other => panic!("expected exactly one winner and one loser: {other:?}"),
    };

    // The losing Apply mutated nothing: the parent split exactly once.
    let parts = list_partitions(&data_plane.journal_client, &partitions_prefix).await;
    assert_eq!(parts.len(), 2, "exactly one split applied: {parts:?}");
    assert_eq!(
        parts
            .iter()
            .map(|(_, begin, end)| (*begin as u64, *end as u64))
            .collect::<Vec<_>>(),
        vec![(0, (1u64 << 31) - 1), (1u64 << 31, (1u64 << 32) - 1),],
    );

    // Lost is a completed attempt: the loser's parent is in cooldown and is
    // not re-dispatched even under renewed pressure.
    let now = Instant::now();
    loser_policy.observe(&parent, true, now);
    assert!(
        !loser_policy.should_split(&parent, now),
        "Lost must start a cooldown",
    );

    // The loser proceeds against the now-split layout: once its watch
    // refreshes, documents re-route into the RHS journal, which becomes due
    // (the LHS — the parent's name — is in cooldown) and the loser wins its
    // own split of it.
    let rhs = parts[1].0.clone();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for the loser's watch to re-route to {rhs}",
        );
        let ids: Vec<String> = (0..16).map(|_| keys.next().0).collect();
        publish_txn(loser_pub, &ids, &payload).await;
        let samples = take_samples(loser_pub);

        if samples.iter().any(|(journal, _)| journal == &rhs) {
            // RHS now receives writes; fabricate its throttled observation so it
            // becomes due (same reason as the parent above).
            loser_policy.observe(&rhs, true, Instant::now());
            break;
        }
    }

    let now = Instant::now();
    let split = shard::start_due_split(loser_policy, loser_pub, now).expect("RHS is due");
    let (journal, outcome) = split.await;
    assert_eq!(journal, rhs);
    let outcome = outcome.expect("split RPC of loser's retry");
    assert_eq!(
        outcome,
        SplitOutcome::Split,
        "loser re-splits against the refreshed layout",
    );
    shard::finish_split(loser_policy, &journal, Ok(outcome), Instant::now());

    let parts = list_partitions(&data_plane.journal_client, &partitions_prefix).await;
    assert_eq!(parts.len(), 3, "the loser's own split applied: {parts:?}");

    data_plane.graceful_stop().await.expect("graceful stop");
}

/// A `Lost` outcome — the split's CAS rejected by the real broker's etcd —
/// is non-destructive: the two-change Apply (narrow the parent, create the
/// RHS) is a single atomic etcd transaction, so a rejected CAS lands nothing.
///
/// The CAS conflict is manufactured deterministically: `split_partition` is
/// fed the real e2e journal client plus a `tokens::fixed` watch snapshot
/// pinned to the journal's *old* mod_revision, after the journal was mutated
/// out-of-band so its live revision differs. (This mirrors the
/// `tokens::fixed` pattern of the `mapping` unit tests, against a real
/// broker.)
#[tokio::test(flavor = "multi_thread")]
async fn journal_split_lost_cas_is_non_destructive() {
    let (data_plane, spec, factory) = start_fixture("testing/autosplit").await;
    let partitions_prefix = format!("{}/", spec.partition_template.as_ref().unwrap().name);

    let mut publisher = Publisher::new_real(
        "test".to_string(),
        runtime_next::new_producer(),
        &factory,
        "testing/ops/stats",
        [&spec],
    )
    .expect("Publisher::new_real");

    // One document creates the full-range partition.
    let payload: String = "x".repeat(256);
    publish_txn(&mut publisher, &["doc-before".to_string()], &payload).await;
    let samples = take_samples(&mut publisher);
    assert_eq!(samples.len(), 1);
    let parent = samples[0].0.clone();

    // Capture the journal's revision R, then mutate it out-of-band (a benign
    // spec bump) so its live revision R' != R.
    let (mut oob_spec, stale_revision) = fetch_journal(&data_plane.journal_client, &parent).await;
    oob_spec.max_append_rate += 1;
    data_plane
        .journal_client
        .apply(broker::ApplyRequest {
            changes: vec![broker::apply_request::Change {
                expect_mod_revision: stale_revision,
                upsert: Some(oob_spec),
                delete: String::new(),
            }],
        })
        .await
        .expect("out-of-band journal update applies");
    let (_, live_revision) = fetch_journal(&data_plane.journal_client, &parent).await;
    assert_ne!(live_revision, stale_revision);

    // Build split_partition's inputs: the collection's partition template, the
    // real journal client, and a stale fixed watch pinned to revision R. The
    // watched journal's full key range (width 2^32, well above 2W) clears the
    // floor check, so the split proceeds to its Apply.
    let publisher::Binding::Mapped(binding) =
        publisher::Binding::from_collection_spec(&spec).expect("binding builds")
    else {
        unreachable!("from_collection_spec builds Mapped bindings");
    };
    let stale_watch = tokens::fixed(Ok(vec![publisher::watch::PartitionSplit {
        name: parent.clone().into(),
        key_begin: u32::MIN,
        key_end: u32::MAX,
        mod_revision: stale_revision,
    }]));
    let client = data_plane.journal_client.clone();

    let outcome = publisher::mapping::split_partition(
        &binding.partitions_template,
        &client,
        &stale_watch,
        &parent,
    )
    .await
    .expect("split RPC completes");
    assert_eq!(outcome, SplitOutcome::Lost, "stale CAS loses against R'");

    // (a) Layout unchanged: still exactly one full-range journal under the
    // prefix (no orphan RHS, parent not narrowed), at the same revision the
    // out-of-band update left it — the rejected Apply landed nothing.
    let parts = list_partitions(&data_plane.journal_client, &partitions_prefix).await;
    assert_eq!(
        parts,
        vec![(parent.clone(), u32::MIN, u32::MAX)],
        "layout is unchanged after Lost",
    );
    let (_, revision_after_lost) = fetch_journal(&data_plane.journal_client, &parent).await;
    assert_eq!(revision_after_lost, live_revision, "parent spec untouched");

    // (b) Still writable: append a document and read it back.
    publish_txn(&mut publisher, &["doc-after".to_string()], &payload).await;
    let docs = read_all_docs(&data_plane.journal_client, &parent).await;
    for id in ["doc-before", "doc-after"] {
        assert_eq!(
            docs.iter().filter(|doc| doc["id"] == id).count(),
            1,
            "{id} is present exactly once: {docs:?}",
        );
    }

    data_plane.graceful_stop().await.expect("graceful stop");
}

/// Data integrity across automatic splits: every document written before,
/// during, and after a cascade of splits is read back from the full journal
/// set exactly once — no loss, no duplication.
///
/// Completeness is asserted over the FULL journal set, not per-journal range
/// purity: a document written before a split stays physically in the
/// now-narrowed LHS (which keeps the parent name) even if its hash falls in
/// the RHS range. That's correct and inherited from the manual split path.
/// Documents written after the layout settles ARE additionally asserted to
/// land in the journal whose range covers their key hash.
#[tokio::test(flavor = "multi_thread")]
async fn journal_auto_split_preserves_document_completeness() {
    let (data_plane, mut spec, factory) = start_fixture("testing/autosplit").await;
    spec.partition_template.as_mut().unwrap().max_append_rate = TEST_MAX_APPEND_RATE;
    let partitions_prefix = format!("{}/", spec.partition_template.as_ref().unwrap().name);

    let mut publisher = Publisher::new_real(
        "test".to_string(),
        runtime_next::new_producer(),
        &factory,
        "testing/ops/stats",
        [&spec],
    )
    .expect("Publisher::new_real");
    let mut policy = SplitPolicy::with_config(aggressive_config());

    let payload: String = "x".repeat(PAYLOAD_LEN);
    let mut keys = KeyPool::new(&spec);
    let mut written = BTreeSet::new();
    let mut split_count = 0;
    let mut at_floor = BTreeSet::new();

    // ---- Phase 1: tracked over-rate load until three splits land. ----
    const TARGET_SPLITS: usize = 3;
    let deadline = Instant::now() + Duration::from_secs(45);
    while split_count < TARGET_SPLITS {
        assert!(
            Instant::now() < deadline,
            "timed out splitting: split_count={split_count}",
        );

        let ids: Vec<String> = (0..DOCS_PER_TXN).map(|_| keys.next().0).collect();
        publish_txn(&mut publisher, &ids, &payload).await;
        written.extend(ids);
        let now = Instant::now();
        let samples = take_samples(&mut publisher);
        observe(&mut policy, &samples, now);

        dispatch_one_split(
            &mut policy,
            &publisher,
            now,
            &mut split_count,
            &mut at_floor,
        )
        .await;
    }
    let parts = list_partitions(&data_plane.journal_client, &partitions_prefix).await;
    assert_eq!(parts.len(), TARGET_SPLITS + 1, "three splits: {parts:?}");

    // ---- Settle: the layout is now frozen (no further dispatch). ----
    // Publish tracked transactions until a single transaction appends to
    // every journal: the publisher's watch then reflects the final layout,
    // so all subsequent documents route against it.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for the watch to route to all journals",
        );

        let ids: Vec<String> = (0..DOCS_PER_TXN).map(|_| keys.next().0).collect();
        publish_txn(&mut publisher, &ids, &payload).await;
        written.extend(ids);
        let appended: BTreeSet<String> = take_samples(&mut publisher)
            .into_iter()
            .map(|(journal, _)| journal)
            .collect();

        if parts.iter().all(|(name, _, _)| appended.contains(name)) {
            break;
        }
    }

    // ---- Phase 2: tracked writes routed against the settled layout. ----
    let mut routed = std::collections::BTreeMap::new();
    for _ in 0..3 {
        let ids: Vec<(String, u32)> = (0..DOCS_PER_TXN).map(|_| keys.next()).collect();
        publish_txn(
            &mut publisher,
            &ids.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>(),
            &payload,
        )
        .await;
        for (id, hash) in ids {
            written.insert(id.clone());
            routed.insert(id, hash);
        }
        _ = take_samples(&mut publisher);
    }

    // ---- Read the ENTIRE collection back across ALL its journals. ----
    let reread = list_partitions(&data_plane.journal_client, &partitions_prefix).await;
    assert_eq!(reread, parts, "layout held while writing");

    let mut counts = std::collections::BTreeMap::<String, usize>::new();
    for (name, begin, end) in &parts {
        for doc in read_all_docs(&data_plane.journal_client, name).await {
            let id = doc["id"].as_str().expect("document has /id").to_string();

            // Post-settle documents routed to the range-covering journal.
            if let Some(hash) = routed.get(&id) {
                assert!(
                    begin <= hash && hash <= end,
                    "post-settle doc {id} (hash {hash:08x}) is in {name} [{begin:08x}, {end:08x}]",
                );
            }
            *counts.entry(id).or_default() += 1;
        }
    }

    // Completeness: every written document is present exactly once.
    let read_ids: BTreeSet<String> = counts.keys().cloned().collect();
    assert_eq!(read_ids, written, "all written documents are read back");
    let duplicated: Vec<_> = counts.iter().filter(|(_, count)| **count != 1).collect();
    assert!(duplicated.is_empty(), "no duplicates: {duplicated:?}");

    data_plane.graceful_stop().await.expect("graceful stop");
}

/// Automatic splits respect logical-partition boundaries: over-rate load on
/// one partition-field combination splits only that logical partition's
/// journal, the LHS / RHS carry the parent's `estuary.dev/field/*` labels
/// and the expected names, and the other combination's journal is untouched.
#[tokio::test(flavor = "multi_thread")]
async fn journal_auto_split_respects_logical_partitions() {
    let (data_plane, mut spec, factory) = start_fixture("testing/autosplit-part").await;
    spec.partition_template.as_mut().unwrap().max_append_rate = TEST_MAX_APPEND_RATE;
    let partitions_prefix = format!("{}/", spec.partition_template.as_ref().unwrap().name);

    let mut publisher = Publisher::new_real(
        "test".to_string(),
        runtime_next::new_producer(),
        &factory,
        "testing/ops/stats",
        [&spec],
    )
    .expect("Publisher::new_real");
    let mut policy = SplitPolicy::with_config(aggressive_config());

    let payload: String = "x".repeat(PAYLOAD_LEN);
    let mut seq = 0u64;
    let mut next_docs = |n: usize, region: &str| -> Vec<serde_json::Value> {
        (0..n)
            .map(|_| {
                let id = format!("{seq:016x}");
                seq += 1;
                serde_json::json!({"id": id, "region": region, "payload": &payload})
            })
            .collect()
    };

    // A single below-rate document creates the cold combo's partition.
    publish_txn_docs(&mut publisher, next_docs(1, "cold")).await;
    let now = Instant::now();
    let samples = take_samples(&mut publisher);
    observe(&mut policy, &samples, now);
    assert_eq!(samples.len(), 1);
    let cold_journal = samples[0].0.clone();
    assert_eq!(
        cold_journal,
        format!("{partitions_prefix}region=cold/pivot=00")
    );

    // ---- Over-rate load into the hot combo only, until one split lands. ----
    // Each transaction also carries one below-rate cold document, so the
    // policy continuously observes (and must not split) the cold journal
    // under concurrent load.
    let mut split_count = 0;
    let mut at_floor = BTreeSet::new();
    let deadline = Instant::now() + Duration::from_secs(45);
    while split_count < 1 {
        assert!(
            Instant::now() < deadline,
            "timed out splitting the hot combo"
        );

        let mut docs = next_docs(DOCS_PER_TXN, "hot");
        docs.extend(next_docs(1, "cold"));
        publish_txn_docs(&mut publisher, docs).await;
        let now = Instant::now();
        let samples = take_samples(&mut publisher);
        assert!(
            samples
                .iter()
                .all(|(journal, throttled)| journal != &cold_journal || !throttled),
            "below-rate cold appends must not be throttled: {samples:?}",
        );
        observe(&mut policy, &samples, now);

        dispatch_one_split(
            &mut policy,
            &publisher,
            now,
            &mut split_count,
            &mut at_floor,
        )
        .await;
    }

    // ---- Only the hot logical partition split; labels and names are right. ----
    let specs = list_partition_specs(&data_plane.journal_client, &partitions_prefix).await;
    let summary: Vec<(String, (u32, u32), Vec<(String, String)>)> = specs
        .iter()
        .map(|spec| {
            let labels = spec.labels.as_ref().expect("partition has labels");
            let range = labels::partition::decode_key_range_labels(labels)
                .expect("partition has key-range labels");
            let fields: Vec<(String, String)> = labels
                .labels
                .iter()
                .filter(|label| label.name.starts_with(labels::FIELD_PREFIX))
                .map(|label| (label.name.clone(), label.value.clone()))
                .collect();
            (spec.name.clone(), range, fields)
        })
        .collect();

    let expect: Vec<(String, (u32, u32), Vec<(String, String)>)> = vec![
        (cold_journal.clone(), (u32::MIN, u32::MAX), "cold"),
        (
            format!("{partitions_prefix}region=hot/pivot=00"),
            (u32::MIN, 0x7fffffff),
            "hot",
        ),
        (
            format!("{partitions_prefix}region=hot/pivot=80000000"),
            (0x80000000, u32::MAX),
            "hot",
        ),
    ]
    .into_iter()
    .map(|(name, range, region)| {
        let fields = vec![("estuary.dev/field/region".to_string(), region.to_string())];
        (name, range, fields)
    })
    .collect();

    assert_eq!(
        summary, expect,
        "hot combo split once with field labels intact; cold combo untouched",
    );

    data_plane.graceful_stop().await.expect("graceful stop");
}
