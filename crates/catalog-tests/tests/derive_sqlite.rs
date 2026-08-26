//! Integration tests driving real `derive-sqlite` derivations through the
//! resident [`DerivationSession`]. derive-sqlite runs in-process, so these need no
//! connector containers and no Docker.
//!
//! Each test builds a small catalog, constructs the scheduler [`Graph`], ingests
//! source documents into the [`CollectionStore`], drives the read cascade by hand
//! (as `run_test_case` will), and snapshots the derived documents.

use catalog_tests::clock::Clock;
use catalog_tests::graph::{Graph, TestTime};
use catalog_tests::partitions;
use catalog_tests::session::DerivationSession;
use catalog_tests::store::CollectionStore;
use serde_json::Value;
use std::collections::BTreeMap;
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

/// Append source documents to a collection's store journal and return the
/// resulting write clock — a stand-in for an ingest step. These fixtures use
/// reduction-free keys, so no combine-by-key is needed. The fixtures are also
/// unpartitioned, so every document lands in the collection's single journal.
fn ingest(
    store: &Arc<Mutex<CollectionStore>>,
    collection: &proto_flow::flow::CollectionSpec,
    docs: &[Value],
) -> Clock {
    let template = &collection.partition_template.as_ref().unwrap().name;
    let routing = partitions::Partitioning::for_collection(collection).unwrap();

    let mut store = store.lock().unwrap();
    for doc in docs {
        let body = serde_json::to_vec(doc).unwrap();
        partitions::append_routed(&mut store, &routing, doc, body).unwrap();
    }
    store.write_clock(template)
}

/// Every document of a collection, in store order. Derived documents carry the
/// runtime's constant UUID placeholder rather than a random UUID, so snapshots
/// are deterministic as-is.
fn dump(
    store: &Arc<Mutex<CollectionStore>>,
    collection: &proto_flow::flow::CollectionSpec,
) -> Vec<Value> {
    let store = store.lock().unwrap();
    let journals = store.journals_of(&collection.partition_template.as_ref().unwrap().name);
    let from = Clock::new();
    // `-1` reads each journal through its head.
    let to: Clock = journals.iter().map(|j| (j.clone(), -1)).collect();

    store
        .read_collection_window(&journals, &from, &to)
        .into_iter()
        .map(|d| serde_json::from_slice(d).unwrap())
        .collect()
}

/// `dump`, with `_meta` removed — for snapshots where the placeholder UUID adds
/// only noise.
fn strip_meta(docs: Vec<Value>) -> Vec<Value> {
    docs.into_iter()
        .map(|mut d| {
            if let Some(obj) = d.as_object_mut() {
                obj.remove("_meta");
            }
            d
        })
        .collect()
}

/// Drive the graph's read cascade to quiescence against the sessions, exactly as
/// `run_test_case` does between steps: pop ready reads, execute each, feed the
/// results back, and advance synthetic time when nothing else can progress.
async fn drive_cascade(graph: &mut Graph, sessions: &mut BTreeMap<String, DerivationSession>) {
    loop {
        let (ready, next, _name) = graph.pop_ready_reads();

        if !ready.is_empty() {
            for pending in &ready {
                let session = sessions
                    .get_mut(&pending.derivation)
                    .unwrap_or_else(|| panic!("no session for derivation {}", pending.derivation));
                let (read, write) = session.read(pending).await.expect("read");
                graph.completed_read(&pending.derivation, read, &write);
            }
            continue;
        }
        match next {
            Some(delta) if delta != TestTime::ZERO => graph.completed_advance(delta),
            _ => break,
        }
    }
}

/// Start a resident session for every derivation in `collections`.
async fn start_sessions(
    collections: &BTreeMap<String, proto_flow::flow::CollectionSpec>,
    n_shards: u32,
    store: &Arc<Mutex<CollectionStore>>,
) -> BTreeMap<String, DerivationSession> {
    // The runtime-next stack dials loopback channels over rustls; install a
    // process crypto provider once (idempotent across tests).
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let registry = service_kit::Registry::new();
    let mut sessions = BTreeMap::new();

    for (name, spec) in collections {
        if spec.derivation.is_none() {
            continue;
        }
        let session = DerivationSession::start(
            spec,
            n_shards,
            String::new(),
            registry.clone(),
            store.clone(),
            runtime_next::TracingLoggerFactory,
        )
        .await
        .unwrap_or_else(|e| panic!("starting session for {name}: {e:#}"));
        sessions.insert(name.clone(), session);
    }
    sessions
}

async fn shutdown(sessions: BTreeMap<String, DerivationSession>) {
    for (name, session) in sessions {
        session
            .shutdown()
            .await
            .unwrap_or_else(|e| panic!("shutting down {name}: {e:#}"));
    }
}

const SINGLE_HOP: &str = r#"
collections:
  acmeCo/ints:
    schema:
      type: object
      properties:
        Key: { type: string }
        Int: { type: integer }
      required: [Key, Int]
    key: [/Key]

  acmeCo/sums:
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
          source: { name: acmeCo/ints }
          shuffle: { key: [/Key] }
          lambda: |
            INSERT INTO sum_state (key, sum) VALUES ($Key, $Int)
              ON CONFLICT DO UPDATE SET sum = sum + $Int;
            SELECT JSON_OBJECT('Key', key, 'Sum', sum) FROM sum_state WHERE key = $Key;
"#;

/// One source collection feeding one SQLite derivation: ingest a few ints, drive
/// the resulting read, and confirm the running sums the derivation emitted.
#[tokio::test]
async fn single_hop_running_sum() {
    let collections = build_collections(SINGLE_HOP).await;
    let mut graph =
        Graph::from_built_collections(&collections.values().cloned().collect::<Vec<_>>()).unwrap();

    let store = Arc::new(Mutex::new(CollectionStore::new()));
    let mut sessions = start_sessions(&collections, 1, &store).await;

    let write = ingest(
        &store,
        &collections["acmeCo/ints"],
        &[
            serde_json::json!({"Key": "a", "Int": 3}),
            serde_json::json!({"Key": "a", "Int": 5}),
            serde_json::json!({"Key": "b", "Int": 10}),
        ],
    );
    graph.completed_ingest(&write);

    drive_cascade(&mut graph, &mut sessions).await;

    // A running-sum snapshot per input: a→3, a→8, b→10.
    insta::assert_json_snapshot!(strip_meta(dump(&store, &collections["acmeCo/sums"])), @r###"
    [
      {
        "Key": "a",
        "Sum": 3
      },
      {
        "Key": "a",
        "Sum": 8
      },
      {
        "Key": "b",
        "Sum": 10
      }
    ]
    "###);

    shutdown(sessions).await;
}

/// A Reset between two rounds of input must clear the connector's SQLite state
/// (so the running sum restarts from zero) while leaving read progress and
/// already-derived collection data intact. This is the mechanism that isolates
/// one test case from the next.
#[tokio::test]
async fn reset_clears_connector_state_but_not_data() {
    let collections = build_collections(SINGLE_HOP).await;
    let mut graph =
        Graph::from_built_collections(&collections.values().cloned().collect::<Vec<_>>()).unwrap();

    let store = Arc::new(Mutex::new(CollectionStore::new()));
    let mut sessions = start_sessions(&collections, 1, &store).await;

    let write = ingest(
        &store,
        &collections["acmeCo/ints"],
        &[serde_json::json!({"Key": "a", "Int": 3})],
    );
    graph.completed_ingest(&write);
    drive_cascade(&mut graph, &mut sessions).await;

    sessions
        .get_mut("acmeCo/sums")
        .unwrap()
        .reset()
        .await
        .expect("reset");

    let write = ingest(
        &store,
        &collections["acmeCo/ints"],
        &[serde_json::json!({"Key": "a", "Int": 5})],
    );
    graph.completed_ingest(&write);
    drive_cascade(&mut graph, &mut sessions).await;

    // Had SQLite state survived, the second sum would be 8. It is 5, so the
    // register was cleared — and the first document is still present, so the
    // reset did not discard collection data.
    insta::assert_json_snapshot!(strip_meta(dump(&store, &collections["acmeCo/sums"])), @r###"
    [
      {
        "Key": "a",
        "Sum": 3
      },
      {
        "Key": "a",
        "Sum": 5
      }
    ]
    "###);

    shutdown(sessions).await;
}

const MULTI_HOP: &str = r#"
collections:
  acmeCo/ints:
    schema:
      type: object
      properties:
        Key: { type: string }
        Int: { type: integer }
      required: [Key, Int]
    key: [/Key]

  acmeCo/sums:
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
          source: { name: acmeCo/ints }
          shuffle: { key: [/Key] }
          lambda: |
            INSERT INTO sum_state (key, sum) VALUES ($Key, $Int)
              ON CONFLICT DO UPDATE SET sum = sum + $Int;
            SELECT JSON_OBJECT('Key', key, 'Sum', sum) FROM sum_state WHERE key = $Key;

  acmeCo/doubled:
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
          source: { name: acmeCo/sums }
          shuffle: { key: [/Key] }
          lambda: |
            SELECT JSON_OBJECT('Key', $Key, 'Doubled', $Sum * 2);
"#;

/// A two-hop chain — ints → sums → doubled — exercising a cascading read: the
/// `sums` read's output projects onto `doubled`, whose read is then driven from
/// the store the first hop wrote.
#[tokio::test]
async fn multi_hop_chain() {
    let collections = build_collections(MULTI_HOP).await;
    let mut graph =
        Graph::from_built_collections(&collections.values().cloned().collect::<Vec<_>>()).unwrap();

    let store = Arc::new(Mutex::new(CollectionStore::new()));
    let mut sessions = start_sessions(&collections, 1, &store).await;

    let write = ingest(
        &store,
        &collections["acmeCo/ints"],
        &[
            serde_json::json!({"Key": "a", "Int": 3}),
            serde_json::json!({"Key": "a", "Int": 5}),
            serde_json::json!({"Key": "b", "Int": 10}),
        ],
    );
    graph.completed_ingest(&write);
    drive_cascade(&mut graph, &mut sessions).await;

    // Each `sums` snapshot (a→3, a→8, b→10), doubled downstream.
    insta::assert_json_snapshot!(strip_meta(dump(&store, &collections["acmeCo/doubled"])), @r###"
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

    shutdown(sessions).await;
}

const SELF_CYCLE: &str = r#"
collections:
  acmeCo/ints:
    schema:
      type: object
      properties:
        Key: { type: string }
        Int: { type: integer }
      required: [Key, Int]
    key: [/Key]

  acmeCo/cycle:
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
          source: { name: acmeCo/ints }
          shuffle: { key: [/Key] }
          lambda: |
            SELECT JSON_OBJECT('Key', $Key, 'Int', $Int);
        - name: fromSelf
          source: { name: acmeCo/cycle }
          shuffle: { key: [/Key] }
          lambda: |
            SELECT 1 WHERE 0;
"#;

/// A self-referential derivation: `acmeCo/cycle` echoes `acmeCo/ints` and also
/// reads itself, emitting nothing from the self-read. The self-read must reach a
/// fixed point via `contains_clock` — the cascade quiesces once the cycle has read
/// through its own writes — rather than looping forever.
#[tokio::test]
async fn self_cycle_terminates() {
    let collections = build_collections(SELF_CYCLE).await;
    let mut graph =
        Graph::from_built_collections(&collections.values().cloned().collect::<Vec<_>>()).unwrap();

    let store = Arc::new(Mutex::new(CollectionStore::new()));
    let mut sessions = start_sessions(&collections, 1, &store).await;

    let write = ingest(
        &store,
        &collections["acmeCo/ints"],
        &[
            serde_json::json!({"Key": "a", "Int": 1}),
            serde_json::json!({"Key": "b", "Int": 2}),
        ],
    );
    graph.completed_ingest(&write);

    // Terminating at all is the assertion; the echoed inputs, and nothing more.
    drive_cascade(&mut graph, &mut sessions).await;

    insta::assert_json_snapshot!(strip_meta(dump(&store, &collections["acmeCo/cycle"])), @r###"
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

    shutdown(sessions).await;
}

/// N-shard key-routing of the shared segment writer, exercised directly: over a
/// three-shard topology every key must land on exactly one shard, consistently,
/// and distinct keys must spread.
///
/// A multi-shard *derivation* cannot use derive-sqlite, which is
/// remote-authoritative and so must be single-shard.
#[tokio::test]
async fn multi_shard_segment_routing() {
    use runtime_local::segments;
    use std::collections::{BTreeSet, HashMap};

    let collections = build_collections(SINGLE_HOP).await;
    let task = shuffle::proto::Task {
        task: Some(shuffle::proto::task::Task::Derivation(
            collections["acmeCo/sums"].clone(),
        )),
    };
    let (bindings, sources, mut validators, _) = segments::task_bindings(&task).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let shards = segments::full_range_shards(3);
    let mut writers = segments::open_shard_writers(dir.path(), 3).unwrap();
    let mut sealed = Vec::new();
    let mut journal_offsets = HashMap::new();
    let mut clock = proto_gazette::uuid::Clock::from_unix(1, 0);

    // Route each key twice, in separate transactions, recording the shards whose
    // read barrier advanced.
    let mut shard_of: BTreeMap<String, BTreeSet<usize>> = BTreeMap::new();
    for key in ["a", "b", "c", "d", "e", "f", "g", "h"] {
        for _ in 0..2 {
            let before: Vec<_> = writers.iter().map(|w| w.last_lsn).collect();
            let doc = serde_json::json!({"Key": key, "Int": 1});
            clock = clock.tick();

            segments::write_transaction_for_bindings(
                &[(0, clock, &doc)],
                &bindings,
                &sources,
                &mut validators,
                &shards,
                &mut writers,
                &mut sealed,
                &mut journal_offsets,
                &mut bytes::BytesMut::new(),
            )
            .unwrap();

            let advanced: BTreeSet<usize> = writers
                .iter()
                .enumerate()
                .filter(|(i, w)| w.last_lsn != before[*i])
                .map(|(i, _)| i)
                .collect();
            shard_of
                .entry(key.to_string())
                .or_default()
                .extend(advanced);
        }
    }

    // Each key routed to exactly one shard, both times.
    for (key, shards) in &shard_of {
        assert_eq!(shards.len(), 1, "key {key} routed to {shards:?}");
    }
    // And the eight keys did not all pile onto one shard.
    let used: BTreeSet<usize> = shard_of.values().flatten().copied().collect();
    assert!(
        used.len() > 1,
        "keys did not spread across shards: {used:?}"
    );
}

/// A session that fails *after* its shards Open must report that failure rather
/// than hang, and must not take the process down as it unwinds.
///
/// A multi-shard derive-sqlite session is one that gets past readiness and only
/// then dies: every shard reports connector state at Opened, and the leader
/// rejects the non-zero ones.
#[tokio::test]
async fn start_failure_after_open_reports_and_tears_down() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let collections = build_collections(SINGLE_HOP).await;
    let spec = collections.get("acmeCo/sums").unwrap();

    // Bounded: the defect guarded against is an unbounded wait, which would
    // hang the suite rather than fail it.
    let started = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        DerivationSession::start(
            spec,
            3, // Rejected: remote-authoritative derivations must be single-shard.
            String::new(),
            service_kit::Registry::new(),
            Arc::new(Mutex::new(CollectionStore::new())),
            runtime_next::TracingLoggerFactory,
        ),
    )
    .await
    .expect("start must report the failure, not block on a signal that cannot arrive");

    let err = started
        .err()
        .expect("a multi-shard derive-sqlite session cannot start");

    let err = format!("{err:#}");
    assert!(
        err.contains("must be single-shard"),
        "unexpected error: {err}"
    );

    // Outlive the teardown a crashing run would not survive.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
}
