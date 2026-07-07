//! In-crate integration tests: drive real `derive-sqlite` derivations through the
//! resident [`DerivationRunner`], with no connector containers (derive-sqlite
//! runs in-process). Each test builds a small catalog, constructs the scheduler
//! [`Graph`], ingests source documents into the [`CollectionStore`], drives the
//! stat cascade by hand, and snapshots the resulting derived documents.

use runtime_harness::clock::Clock;
use runtime_harness::graph::Graph;
use runtime_harness::runner::DerivationRunner;
use runtime_harness::store::{self, CollectionStore};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

/// Build a catalog from inline YAML, validating derivations in-process (no
/// Docker). Returns the built collection specs, keyed by collection name.
async fn build_collections(yaml: &str) -> BTreeMap<String, proto_flow::flow::CollectionSpec> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("catalog.flow.yaml");
    std::fs::write(&path, yaml).unwrap();
    let url = build::arg_source_to_url(path.to_str().unwrap(), false).unwrap();

    let output = build::for_local_test(&url, false)
        .await
        .into_result()
        .expect("catalog build should succeed");

    output
        .built
        .built_collections
        .iter()
        .filter_map(|bc| bc.spec.as_ref().map(|s| (s.name.clone(), s.clone())))
        .collect()
}

/// Append raw source documents to a collection's store journal and return the
/// resulting write clock (a stand-in for a V1 ingest; combine-by-key is a
/// Phase-4 refinement and these fixtures use distinct or reduction-free keys).
fn ingest(
    store: &Arc<Mutex<CollectionStore>>,
    clock: &AtomicU64,
    collection: &str,
    docs: &[Value],
) -> Clock {
    let journal = store::default_partition_journal(collection);
    let mut store = store.lock().unwrap();
    for doc in docs {
        let c = clock.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        store.append(&journal, serde_json::to_vec(doc).unwrap(), c);
    }
    store.write_clock(collection)
}

/// All documents of a collection, parsed, in store order, with document UUIDs
/// masked (as Verify does) so snapshots are deterministic.
fn dump(store: &Arc<Mutex<CollectionStore>>, collection: &str) -> Vec<Value> {
    let store = store.lock().unwrap();
    let journals = store.journals_of(collection);
    let empty = Clock::new();
    store
        .read_collection_window(&journals, &empty, &empty_to_head(&journals))
        .into_iter()
        .map(|d| {
            let mut v: Value = serde_json::from_slice(&d.doc).unwrap();
            runtime_harness::diff::mask_uuid(&mut v, "/_meta/uuid");
            v
        })
        .collect()
}

/// A clock reading each journal through its head (Verify's `to` for a full dump).
fn empty_to_head(journals: &[String]) -> Clock {
    journals.iter().map(|j| (j.clone(), -1)).collect()
}

/// Drive the graph's stat cascade to quiescence against the runners, exactly as
/// `run_test_case` would between steps: pop ready stats, execute each, feed
/// results back, and advance synthetic time when nothing else can progress.
async fn drive_cascade(graph: &mut Graph, runners: &mut BTreeMap<String, DerivationRunner>) {
    loop {
        let (ready, next, _name) = graph.pop_ready_stats();
        if !ready.is_empty() {
            for stat in &ready {
                let runner = runners
                    .get_mut(&stat.task_name)
                    .unwrap_or_else(|| panic!("no runner for task {}", stat.task_name));
                let (read, write) = runner.stat(stat).await.expect("stat");
                graph.completed_stat(&stat.task_name, read, &write);
            }
            continue;
        }
        match next {
            Some(delta) if delta != runtime_harness::graph::TestTime::ZERO => {
                graph.completed_advance(delta)
            }
            _ => break,
        }
    }
}

/// Start a resident runner for every derivation in `collections`.
async fn start_runners(
    collections: &BTreeMap<String, proto_flow::flow::CollectionSpec>,
    n_shards: u32,
    store: &Arc<Mutex<CollectionStore>>,
    clock: &Arc<AtomicU64>,
) -> BTreeMap<String, DerivationRunner> {
    // The runtime-next stack dials loopback channels through rustls; install a
    // process crypto provider once (idempotent across tests).
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let registry = service_kit::Registry::new();
    let mut runners = BTreeMap::new();
    for (name, spec) in collections {
        if spec.derivation.is_none() {
            continue;
        }
        let runner = DerivationRunner::start(
            spec,
            n_shards,
            String::new(),
            registry.clone(),
            store.clone(),
            clock.clone(),
            std::sync::Arc::new(::ops::tracing_log_handler),
        )
        .await
        .unwrap_or_else(|e| panic!("starting runner for {name}: {e:#}"));
        runners.insert(name.clone(), runner);
    }
    runners
}

const SINGLE_HOP: &str = r#"
collections:
  harness/ints:
    schema:
      type: object
      properties:
        Key: { type: string }
        Int: { type: integer }
      required: [Key, Int]
    key: [/Key]

  harness/sums:
    schema:
      type: object
      properties:
        Key: { type: string }
        Sum: { type: integer }
      required: [Key, Sum]
    key: [/Key]
    derive:
      using:
        sqlite:
          migrations:
            - |
              CREATE TABLE sum_state (
                key TEXT NOT NULL PRIMARY KEY,
                sum INTEGER NOT NULL
              );
      transforms:
        - name: fromInts
          source: { name: harness/ints }
          shuffle: { key: [/Key] }
          lambda: |
            INSERT INTO sum_state (key, sum) VALUES ($Key, $Int)
              ON CONFLICT DO UPDATE SET sum = sum + $Int;
            SELECT JSON_OBJECT('Key', key, 'Sum', sum) FROM sum_state WHERE key = $Key;
"#;

/// One source collection feeding one SQLite derivation: ingest a few ints, drive
/// the resulting stat, and confirm the running sums the derivation emitted.
#[tokio::test]
async fn single_hop_running_sum() {
    let collections = build_collections(SINGLE_HOP).await;
    let mut graph =
        Graph::from_built_collections(&collections.values().cloned().collect::<Vec<_>>());

    let store = Arc::new(Mutex::new(CollectionStore::new()));
    let clock = Arc::new(AtomicU64::new(1));
    let mut runners = start_runners(&collections, 1, &store, &clock).await;

    let write = ingest(
        &store,
        &clock,
        "harness/ints",
        &[
            serde_json::json!({"Key": "a", "Int": 3}),
            serde_json::json!({"Key": "a", "Int": 5}),
            serde_json::json!({"Key": "b", "Int": 10}),
        ],
    );
    graph.completed_ingest("harness/ints", &write);

    drive_cascade(&mut graph, &mut runners).await;

    // The derivation emits a running-sum snapshot per input: a→3, a→8, b→10.
    let sums = dump(&store, "harness/sums");
    insta::assert_json_snapshot!(sums, @r###"
    [
      {
        "Key": "a",
        "Sum": 3,
        "_meta": {
          "uuid": "flow-uuid"
        }
      },
      {
        "Key": "a",
        "Sum": 8,
        "_meta": {
          "uuid": "flow-uuid"
        }
      },
      {
        "Key": "b",
        "Sum": 10,
        "_meta": {
          "uuid": "flow-uuid"
        }
      }
    ]
    "###);

    for (_, runner) in runners {
        runner.shutdown().await.unwrap();
    }
}

const MULTI_HOP: &str = r#"
collections:
  harness/ints:
    schema:
      type: object
      properties:
        Key: { type: string }
        Int: { type: integer }
      required: [Key, Int]
    key: [/Key]

  harness/sums:
    schema:
      type: object
      properties:
        Key: { type: string }
        Sum: { type: integer }
      required: [Key, Sum]
    key: [/Key]
    derive:
      using:
        sqlite:
          migrations:
            - |
              CREATE TABLE sum_state (key TEXT NOT NULL PRIMARY KEY, sum INTEGER NOT NULL);
      transforms:
        - name: fromInts
          source: { name: harness/ints }
          shuffle: { key: [/Key] }
          lambda: |
            INSERT INTO sum_state (key, sum) VALUES ($Key, $Int)
              ON CONFLICT DO UPDATE SET sum = sum + $Int;
            SELECT JSON_OBJECT('Key', key, 'Sum', sum) FROM sum_state WHERE key = $Key;

  harness/doubled:
    schema:
      type: object
      properties:
        Key: { type: string }
        Doubled: { type: integer }
      required: [Key, Doubled]
    key: [/Key]
    derive:
      using:
        sqlite: {}
      transforms:
        - name: fromSums
          source: { name: harness/sums }
          shuffle: { key: [/Key] }
          lambda: |
            SELECT JSON_OBJECT('Key', $Key, 'Doubled', $Sum * 2);
"#;

/// A two-hop chain — ints → sums → doubled — exercising a cascading stat: the
/// `sums` stat's output projects onto `doubled`, whose stat is then driven from
/// the store the first hop wrote. Each `sums` snapshot is doubled downstream.
#[tokio::test]
async fn multi_hop_chain() {
    let collections = build_collections(MULTI_HOP).await;
    let mut graph =
        Graph::from_built_collections(&collections.values().cloned().collect::<Vec<_>>());

    let store = Arc::new(Mutex::new(CollectionStore::new()));
    let clock = Arc::new(AtomicU64::new(1));
    let mut runners = start_runners(&collections, 1, &store, &clock).await;

    let write = ingest(
        &store,
        &clock,
        "harness/ints",
        &[
            serde_json::json!({"Key": "a", "Int": 3}),
            serde_json::json!({"Key": "a", "Int": 5}),
            serde_json::json!({"Key": "b", "Int": 10}),
        ],
    );
    graph.completed_ingest("harness/ints", &write);
    drive_cascade(&mut graph, &mut runners).await;

    // sums snapshots (a→3, a→8, b→10) each doubled downstream.
    let doubled = strip_meta(dump(&store, "harness/doubled"));
    insta::assert_json_snapshot!(doubled, @r###"
    [
      {
        "Doubled": 6,
        "Key": "a"
      },
      {
        "Doubled": 16,
        "Key": "a"
      },
      {
        "Doubled": 20,
        "Key": "b"
      }
    ]
    "###);

    for (_, runner) in runners {
        runner.shutdown().await.unwrap();
    }
}

const SELF_CYCLE: &str = r#"
collections:
  harness/ints:
    schema:
      type: object
      properties:
        Key: { type: string }
        Int: { type: integer }
      required: [Key, Int]
    key: [/Key]

  harness/cycle:
    schema:
      type: object
      properties:
        Key: { type: string }
        Int: { type: integer }
      required: [Key, Int]
    key: [/Key]
    derive:
      using:
        sqlite: {}
      transforms:
        - name: fromInts
          source: { name: harness/ints }
          shuffle: { key: [/Key] }
          lambda: |
            SELECT JSON_OBJECT('Key', $Key, 'Int', $Int);
        - name: fromSelf
          source: { name: harness/cycle }
          shuffle: { key: [/Key] }
          lambda: |
            SELECT 1 WHERE 0;
"#;

/// A self-referential derivation: `harness/cycle` reads `harness/ints` (echoing
/// each input) and reads itself (emitting nothing). The self-read must reach a
/// fixed point via `ContainsClock` — the cascade quiesces once the cycle has
/// read through its own writes — rather than looping forever.
#[tokio::test]
async fn self_cycle_terminates() {
    let collections = build_collections(SELF_CYCLE).await;
    let mut graph =
        Graph::from_built_collections(&collections.values().cloned().collect::<Vec<_>>());

    let store = Arc::new(Mutex::new(CollectionStore::new()));
    let clock = Arc::new(AtomicU64::new(1));
    let mut runners = start_runners(&collections, 1, &store, &clock).await;

    let write = ingest(
        &store,
        &clock,
        "harness/ints",
        &[
            serde_json::json!({"Key": "a", "Int": 1}),
            serde_json::json!({"Key": "b", "Int": 2}),
        ],
    );
    graph.completed_ingest("harness/ints", &write);

    // Terminates (no infinite self-cascade): the echoed inputs, and nothing more.
    drive_cascade(&mut graph, &mut runners).await;

    let cycle = strip_meta(dump(&store, "harness/cycle"));
    insta::assert_json_snapshot!(cycle, @r###"
    [
      {
        "Int": 1,
        "Key": "a"
      },
      {
        "Int": 2,
        "Key": "b"
      }
    ]
    "###);

    for (_, runner) in runners {
        runner.shutdown().await.unwrap();
    }
}

/// N-shard key-routing of the shared segment writer, exercised directly.
///
/// A full end-to-end *multi-shard derivation* can't use derive-sqlite: it is
/// remote-authoritative (its checkpoint lives in SQLite, a singleton), so the
/// runtime requires it to be single-shard — the `run_tests` runner picks one
/// shard for such connectors and three for image derivations (validated against
/// the TypeScript examples in later phases). What is container-free and testable
/// here is the routing itself: over a three-shard topology, each key must land
/// on exactly one shard (consistently), and distinct keys must spread.
#[tokio::test]
async fn multi_shard_segment_routing() {
    use runtime_harness::drive::segments;
    use std::collections::{BTreeSet, HashMap};

    let collections = build_collections(SINGLE_HOP).await;
    let task = shuffle::proto::Task {
        task: Some(shuffle::proto::task::Task::Derivation(
            collections["harness/sums"].clone(),
        )),
    };
    let (bindings, mut validators, collection_bindings) =
        segments::task_bindings(&task).expect("task bindings");

    let shards = segments::full_range_shards(3);
    let dir = tempfile::tempdir().unwrap();
    // Each shard writes into its own directory (which must exist first).
    let mut writers: Vec<segments::ShardWriter> = (0..3)
        .map(|i| {
            let d = dir.path().join(format!("shard-{i}"));
            std::fs::create_dir_all(&d).unwrap();
            segments::ShardWriter::new(&d, i as u32).unwrap()
        })
        .collect();

    let mut sealed = Vec::new();
    let mut clock = proto_gazette::uuid::Clock::from_unix(1, 0);
    let mut journal_offsets = HashMap::new();
    let mut round_robin = HashMap::new();
    let mut packed_key = bytes::BytesMut::new();

    // Feed each key as its own transaction and record which shard advanced.
    let mut key_to_shard: HashMap<String, usize> = HashMap::new();
    for i in 0..12 {
        let key = format!("key-{i}");
        let before: Vec<u64> = writers.iter().map(|w| w.last_lsn.as_u64()).collect();
        let txn = vec![(
            "harness/ints".to_string(),
            serde_json::json!({"Key": key, "Int": i}),
        )];
        segments::write_transaction(
            &txn,
            &bindings,
            &mut validators,
            &collection_bindings,
            &shards,
            &mut writers,
            &mut sealed,
            &mut clock,
            &mut journal_offsets,
            &mut round_robin,
            &mut packed_key,
        )
        .unwrap();
        let advanced: Vec<usize> = writers
            .iter()
            .enumerate()
            .filter(|(i, w)| w.last_lsn.as_u64() > before[*i])
            .map(|(i, _)| i)
            .collect();
        assert_eq!(advanced.len(), 1, "{key} must route to exactly one shard");
        key_to_shard.insert(key, advanced[0]);
    }

    // Distinct keys spread across more than one shard.
    let distinct: BTreeSet<usize> = key_to_shard.values().copied().collect();
    assert!(
        distinct.len() > 1,
        "keys should distribute across shards, got {distinct:?}",
    );

    // Re-feeding a key routes to the same shard (key-consistent routing).
    for (key, &shard) in &key_to_shard {
        let before: Vec<u64> = writers.iter().map(|w| w.last_lsn.as_u64()).collect();
        let txn = vec![(
            "harness/ints".to_string(),
            serde_json::json!({"Key": key, "Int": 0}),
        )];
        segments::write_transaction(
            &txn,
            &bindings,
            &mut validators,
            &collection_bindings,
            &shards,
            &mut writers,
            &mut sealed,
            &mut clock,
            &mut journal_offsets,
            &mut round_robin,
            &mut packed_key,
        )
        .unwrap();
        let again = writers
            .iter()
            .enumerate()
            .find(|(i, w)| w.last_lsn.as_u64() > before[*i])
            .map(|(i, _)| i)
            .unwrap();
        assert_eq!(
            again, shard,
            "{key} must route consistently to shard {shard}"
        );
    }
}

/// Drop `_meta` (the masked UUID) for compact snapshots of derived bodies.
fn strip_meta(docs: Vec<Value>) -> Vec<Value> {
    docs.into_iter()
        .map(|mut v| {
            if let Some(obj) = v.as_object_mut() {
                obj.remove("_meta");
            }
            v
        })
        .collect()
}
