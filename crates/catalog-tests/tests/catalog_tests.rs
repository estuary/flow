//! End-to-end `run_tests` tests over real derive-sqlite catalogs (no connector
//! containers). These drive the whole path: build → test cases sorted by scope →
//! ingest (combine-by-key) → stat cascade → verify (window + partition selector +
//! combine + diff) → Reset between cases.
//!
//! derive-sqlite is remote-authoritative and so runs single-shard; multi-shard
//! image execution is covered by the examples suite in CI.

use catalog_tests::run::{self, Options};

/// Build a catalog from inline YAML to built specs, validating derivations
/// in-process (no Docker).
async fn build_catalog(yaml: &str) -> build::Output {
    // The runtime-next loopback stack dials over rustls 0.23; install a process
    // crypto provider once (idempotent across tests), as `flowctl` main does.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("catalog.flow.yaml");
    std::fs::write(&path, yaml).unwrap();
    let url = build::arg_source_to_url(path.to_str().unwrap(), false).unwrap();

    build::for_catalog_test(&url, "", ops::tracing_log_handler)
        .await
        .into_result()
        .expect("catalog build should succeed")
}

fn options() -> Options {
    Options {
        splits: 1, // derive-sqlite is remote-authoritative; single-shard only.
        ..Default::default()
    }
}

/// Run `yaml`'s tests, printing any failure so a broken assertion is diagnosable.
async fn run(yaml: &str) -> run::TestResults {
    let output = build_catalog(yaml).await;
    let results = run::run_tests(&output.built, options())
        .await
        .expect("run_tests");

    for outcome in &results.outcomes {
        if let Some(err) = &outcome.error {
            eprintln!("test {} failed:\n{err}", outcome.name);
        }
    }
    results
}

/// A running-sum SQLite derivation with two independent test cases. The second
/// must observe cleared connector state, so its sum restarts from zero — and its
/// ingest is combined by key before it is written, so the first case's two
/// documents reach the derivation as one summed document.
const RESET_BETWEEN_CASES: &str = r#"
collections:
  acmeCo/ints:
    schema:
      type: object
      properties:
        Key: { type: string }
        Int: { type: integer, reduce: { strategy: sum } }
      required: [Key, Int]
      reduce: { strategy: merge }
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

tests:
  acmeCo/test/first:
    - ingest:
        collection: acmeCo/ints
        documents:
          - { Key: a, Int: 3 }
          - { Key: a, Int: 5 }
    - verify:
        collection: acmeCo/sums
        documents:
          - { Key: a, Sum: 8 }
  acmeCo/test/second:
    - ingest:
        collection: acmeCo/ints
        documents:
          - { Key: a, Int: 10 }
    - verify:
        collection: acmeCo/sums
        documents:
          - { Key: a, Sum: 10 }
"#;

#[tokio::test]
async fn reset_between_cases() {
    let results = run(RESET_BETWEEN_CASES).await;

    let report: Vec<(String, bool)> = results
        .outcomes
        .iter()
        .map(|o| (o.name.clone(), o.passed()))
        .collect();

    // Both pass, and in scope order. The first case's ingest sum-combines to
    // {a:8}; the Reset then clears the SQLite table so the second restarts at
    // {a:10} rather than reaching 18. Its verify also proves the window is
    // per-case: the first case's {a:8} is still in the store but not compared.
    assert_eq!(
        report,
        vec![
            ("acmeCo/test/first".to_string(), true),
            ("acmeCo/test/second".to_string(), true),
        ],
    );
    assert!(results.all_passed());
}

const PARTITIONED_VERIFY: &str = r#"
collections:
  acmeCo/src:
    schema:
      type: object
      properties:
        id: { type: string }
        region: { type: string }
      required: [id, region]
    key: [/id]

  acmeCo/routed:
    schema:
      type: object
      properties:
        id: { type: string }
        region: { type: string }
      required: [id, region]
    key: [/id]
    projections:
      region:
        location: /region
        partition: true
    derive:
      using:
        sqlite: {}
      transforms:
        - name: echo
          source: { name: acmeCo/src }
          shuffle: { key: [/id] }
          lambda: |
            SELECT JSON_OBJECT('id', $id, 'region', $region);

tests:
  acmeCo/test/partitions:
    - ingest:
        collection: acmeCo/src
        documents:
          - { id: a, region: east }
          - { id: b, region: west }
          - { id: c, region: east }
    - verify:
        collection:
          name: acmeCo/routed
          partitions: { include: { region: [east] } }
        documents:
          - { id: a, region: east }
          - { id: c, region: east }
    - verify:
        collection:
          name: acmeCo/routed
          partitions: { include: { region: [west] } }
        documents:
          - { id: b, region: west }
"#;

/// Derived documents route to their logical partition, and a verify step's
/// partition selector restricts the comparison to matching partitions.
#[tokio::test]
async fn partitioned_verify_filters_by_selector() {
    let results = run(PARTITIONED_VERIFY).await;
    assert!(results.all_passed(), "partitioned verify should pass");
}

const READ_DELAY: &str = r#"
collections:
  acmeCo/ints:
    schema:
      type: object
      properties:
        Key: { type: string }
        Int: { type: integer }
      required: [Key, Int]
    key: [/Key]

  acmeCo/delayed:
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
        - name: slow
          source: { name: acmeCo/ints }
          shuffle: { key: [/Key] }
          readDelay: 1h
          lambda: |
            SELECT JSON_OBJECT('Key', $Key, 'Int', $Int);

tests:
  acmeCo/test/delayed:
    - ingest:
        collection: acmeCo/ints
        documents:
          - { Key: a, Int: 1 }
    - verify:
        collection: acmeCo/delayed
        documents:
          - { Key: a, Int: 1 }
"#;

/// A one-hour read delay must be satisfied by advancing *synthetic* time, not by
/// sleeping: the scheduler withholds the delayed read until its ready-at time,
/// then drives it. If any wall-clock sleep crept in, this test would hang rather
/// than complete in milliseconds.
#[tokio::test]
async fn read_delay_gates_on_synthetic_time() {
    let results = run(READ_DELAY).await;
    assert!(results.all_passed(), "read-delayed verify should pass");
}

const FAILING_VERIFY: &str = r#"
collections:
  acmeCo/ints:
    schema:
      type: object
      properties:
        Key: { type: string }
        Int: { type: integer }
      required: [Key, Int]
    key: [/Key]

  acmeCo/echo:
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
        - name: echo
          source: { name: acmeCo/ints }
          shuffle: { key: [/Key] }
          lambda: |
            SELECT JSON_OBJECT('Key', $Key, 'Int', $Int);

tests:
  acmeCo/test/wrong:
    - ingest:
        collection: acmeCo/ints
        documents:
          - { Key: a, Int: 1 }
    - verify:
        collection: acmeCo/echo
        documents:
          - { Key: a, Int: 999 }
"#;

/// A deliberately wrong expectation must fail, with a readable diff and a scope
/// pointing at the failing step — not pass silently.
#[tokio::test]
async fn failing_verify_reports_diff() {
    let output = build_catalog(FAILING_VERIFY).await;
    let results = run::run_tests(&output.built, options())
        .await
        .expect("run_tests");

    assert_eq!(results.failed(), 1, "the wrong expectation must fail");
    assert!(!results.all_passed());

    let outcome = &results.outcomes[0];
    let err = outcome.error.as_ref().expect("a failure message");
    assert!(
        err.contains("did not match") && err.contains("999"),
        "failure should render a readable diff, got:\n{err}",
    );
    // The scope locates the failing step — step 1, the verify — rather than the
    // test as a whole, so a caller can render a precise source location.
    assert!(
        outcome
            .scope
            .ends_with("/tests/acmeCo~1test~1wrong/1/verify"),
        "scope should point at the failing step, got {}",
        outcome.scope,
    );
}

/// A failing verification writes its actual documents into the snapshot
/// directory, at V1's `{dir}/{test}/verify-{step}.json` path.
#[tokio::test]
async fn failing_verify_writes_snapshot() {
    let output = build_catalog(FAILING_VERIFY).await;
    let dir = tempfile::tempdir().unwrap();

    let results = run::run_tests(
        &output.built,
        Options {
            splits: 1,
            snapshot_dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        },
    )
    .await
    .expect("run_tests");
    assert_eq!(results.failed(), 1);

    let path = dir.path().join("acmeCo/test/wrong").join("verify-1.json");
    let actuals: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).expect("snapshot written")).unwrap();

    // The snapshot holds what was actually produced (Int 1), not the expectation.
    assert_eq!(actuals[0]["Int"], serde_json::json!(1));
}
