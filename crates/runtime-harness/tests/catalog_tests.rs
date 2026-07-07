//! End-to-end `run_tests` integration tests over real derive-sqlite catalogs (no
//! connector containers). These drive the full runner: build → sorted test cases
//! → ingest (combine-by-key) → stat cascade → verify (window + partition selector
//! + combine + diff) → Reset between cases. derive-sqlite is remote-authoritative,
//! so these run single-shard; multi-shard image execution is covered by the
//! examples-suite parity test.

use runtime_harness::run::{self, Options};

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

    build::for_local_test(&url, false)
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

/// A running-sum SQLite derivation whose ingest write schema sums `/Int` by key,
/// with two independent test cases. The second case must observe a cleared
/// connector state (Reset between cases), so its sum restarts from zero.
const RESET_BETWEEN_CASES: &str = r#"
collections:
  harness/ints:
    schema:
      type: object
      properties:
        Key: { type: string }
        Int: { type: integer, reduce: { strategy: sum } }
      required: [Key, Int]
      reduce: { strategy: merge }
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

tests:
  harness/test/first:
    - ingest:
        collection: harness/ints
        documents:
          - { Key: a, Int: 3 }
          - { Key: a, Int: 5 }
    - verify:
        collection: harness/sums
        documents:
          - { Key: a, Sum: 8 }
  harness/test/second:
    - ingest:
        collection: harness/ints
        documents:
          - { Key: a, Int: 10 }
    - verify:
        collection: harness/sums
        documents:
          - { Key: a, Sum: 10 }
"#;

#[tokio::test]
async fn reset_between_cases() {
    let output = build_catalog(RESET_BETWEEN_CASES).await;
    let results = run::run_tests(&output.built, options())
        .await
        .expect("run_tests");

    let report: Vec<(String, bool)> = results
        .outcomes
        .iter()
        .map(|o| (o.name.clone(), o.passed()))
        .collect();
    for outcome in &results.outcomes {
        if let Some(err) = &outcome.error {
            eprintln!("test {} failed:\n{err}", outcome.name);
        }
    }

    // Both cases pass: the ingest sum-combine yields {a:8} then the reset clears
    // the SQLite table so the second case restarts at {a:10}.
    assert_eq!(
        report,
        vec![
            ("harness/test/first".to_string(), true),
            ("harness/test/second".to_string(), true),
        ],
    );
    assert!(results.all_passed());
}

/// A partitioned derive-sqlite output verified through a partition selector: only
/// the matching logical partition's documents are compared.
const PARTITIONED_VERIFY: &str = r#"
collections:
  harness/src:
    schema:
      type: object
      properties:
        id: { type: string }
        region: { type: string }
      required: [id, region]
    key: [/id]

  harness/routed:
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
          source: { name: harness/src }
          shuffle: { key: [/id] }
          lambda: |
            SELECT JSON_OBJECT('id', $id, 'region', $region);

tests:
  harness/test/partitions:
    - ingest:
        collection: harness/src
        documents:
          - { id: a, region: east }
          - { id: b, region: west }
          - { id: c, region: east }
    - verify:
        collection:
          name: harness/routed
          partitions: { include: { region: [east] } }
        documents:
          - { id: a, region: east }
          - { id: c, region: east }
    - verify:
        collection:
          name: harness/routed
          partitions: { include: { region: [west] } }
        documents:
          - { id: b, region: west }
"#;

#[tokio::test]
async fn partitioned_verify_filters_by_selector() {
    let output = build_catalog(PARTITIONED_VERIFY).await;
    let results = run::run_tests(&output.built, options())
        .await
        .expect("run_tests");

    for outcome in &results.outcomes {
        if let Some(err) = &outcome.error {
            eprintln!("test {} failed:\n{err}", outcome.name);
        }
    }
    assert!(results.all_passed(), "partitioned verify should pass");
}

/// A deliberately wrong expectation must produce a readable failure rather than
/// a pass — exercising the diff/report path.
const FAILING_VERIFY: &str = r#"
collections:
  harness/ints:
    schema:
      type: object
      properties:
        Key: { type: string }
        Int: { type: integer }
      required: [Key, Int]
    key: [/Key]

  harness/echo:
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
          source: { name: harness/ints }
          shuffle: { key: [/Key] }
          lambda: |
            SELECT JSON_OBJECT('Key', $Key, 'Int', $Int);

tests:
  harness/test/wrong:
    - ingest:
        collection: harness/ints
        documents:
          - { Key: a, Int: 1 }
    - verify:
        collection: harness/echo
        documents:
          - { Key: a, Int: 999 }
"#;

#[tokio::test]
async fn failing_verify_reports_diff() {
    let output = build_catalog(FAILING_VERIFY).await;
    let results = run::run_tests(&output.built, options())
        .await
        .expect("run_tests");

    assert_eq!(results.failed(), 1, "the wrong expectation must fail");
    let outcome = &results.outcomes[0];
    let err = outcome.error.as_ref().expect("a failure message");
    assert!(
        err.contains("did not match") && err.contains("999"),
        "failure should render a readable diff, got:\n{err}",
    );
}
