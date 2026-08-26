//! End-to-end `run_tests` tests over real derive-sqlite catalogs (no connector
//! containers). These drive the whole path: build → test cases sorted by scope →
//! ingest (combine-by-key) → read cascade → verify (window + partition selector +
//! combine + diff) → Reset between cases.

use catalog_tests::run;

/// Build a catalog from inline YAML to built specs, validating derivations
/// in-process (no Docker).
async fn build_catalog(yaml: &str) -> build::Output {
    // The runtime-next loopback stack dials over rustls; install a process
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

/// Run `yaml`'s tests, printing any failure so a broken assertion is diagnosable.
async fn run(yaml: &str) -> run::TestResults {
    let output = build_catalog(yaml).await;
    let results = run::run_tests(&output.built, run::Options::default())
        .await
        .expect("run_tests");

    for outcome in &results.outcomes {
        if let run::TestStatus::Failed { error } = &outcome.status {
            eprintln!("test {} failed:\n{error}", outcome.name);
        }
    }
    results
}

/// A running-sum SQLite derivation with two independent test cases.
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
    let results = run::run_tests(&output.built, run::Options::default())
        .await
        .expect("run_tests");

    assert_eq!(results.failed(), 1, "the wrong expectation must fail");
    assert!(!results.all_passed());

    let outcome = &results.outcomes[0];
    let run::TestStatus::Failed { error: err } = &outcome.status else {
        panic!("expected a failure message, got {:?}", outcome.status);
    };
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

const SESSION_KILLING_FAILURE: &str = r#"
collections:
  acmeCo/ints:
    schema:
      type: object
      properties:
        Key: { type: string }
        Int: { type: integer }
      required: [Key, Int]
    key: [/Key]

  acmeCo/bad:
    schema:
      type: object
      properties:
        Key: { type: string }
        Sum: { type: integer }
      required: [Key, Sum]
    key: [/Key]
    derive:
      using:
        sqlite: {}
      transforms:
        - name: fromInts
          source: { name: acmeCo/ints }
          shuffle: { key: [/Key] }
          lambda: |
            SELECT JSON_OBJECT('Key', $Key);

tests:
  acmeCo/test/dies:
    - ingest:
        collection: acmeCo/ints
        documents:
          - { Key: a, Int: 1 }
    - verify:
        collection: acmeCo/bad
        documents: []
  acmeCo/test/not-reached:
    - ingest:
        collection: acmeCo/ints
        documents:
          - { Key: b, Int: 2 }
    - verify:
        collection: acmeCo/ints
        documents:
          - { Key: b, Int: 2 }
"#;

/// A derivation emitting documents that violate its own schema kills its session
/// mid-case, and the failed inter-case Reset then ends the run. The failing
/// case's outcome — carrying the session's actual error — must be recorded
/// rather than discarded for the Reset's uninformative teardown symptom, and the
/// cases that never ran must say so. Bounded because the guarded-against
/// regression is an await on a dead session's commit, which would hang the suite.
#[tokio::test]
async fn session_death_records_outcome_and_not_run() {
    let output = build_catalog(SESSION_KILLING_FAILURE).await;
    let results = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        run::run_tests(&output.built, run::Options::default()),
    )
    .await
    .expect("a dead session must fail the case, not hang the run")
    .expect("a case failure must not surface as a run-level error");

    assert_eq!(results.failed(), 1);
    assert_eq!(results.not_run(), 1);

    let run::TestStatus::Failed { error } = &results.outcomes[0].status else {
        panic!("first case must fail, got {:?}", results.outcomes[0].status);
    };
    assert_eq!(results.outcomes[0].name, "acmeCo/test/dies");
    assert!(
        error.contains("acmeCo/bad shard 0")
            && error.contains("failed validation against its collection JSON Schema"),
        "the case must carry the session's own failure, naming the shard that \
         reported it, got:\n{error}",
    );

    let run::TestStatus::NotRun { reason } = &results.outcomes[1].status else {
        panic!(
            "second case must be not-run, got {:?}",
            results.outcomes[1].status
        );
    };
    assert_eq!(results.outcomes[1].name, "acmeCo/test/not-reached");
    assert!(
        reason.contains("acmeCo/test/dies"),
        "the reason must name the case that ended the run, got: {reason}",
    );
}

const REDACTION: &str = r#"
collections:
  acmeCo/people:
    schema:
      type: object
      properties:
        id: { type: string }
        pii: { type: string, redact: { strategy: sha256 } }
      required: [id, pii]
    key: [/id]

tests:
  acmeCo/test/redaction:
    - ingest:
        collection: acmeCo/people
        documents:
          - { id: a, pii: hello }
    - verify:
        collection: acmeCo/people
        documents:
          - { id: a, pii: "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824" }
"#;

/// Redaction in a catalog test always uses an EMPTY salt, so an expected digest
/// is `sha256:` followed by the plain SHA-256 of the raw value — here
/// `printf hello | sha256sum`. The platform salt would instead rotate with the
/// task's generation, making the expectation unwritable.
#[tokio::test]
async fn redaction_digests_are_unsalted() {
    let results = run(REDACTION).await;
    assert!(
        results.all_passed(),
        "expected digest must be the unsalted SHA-256 of the raw value",
    );
}

const DERIVED_REDACTION: &str = r#"
collections:
  acmeCo/raw:
    schema:
      type: object
      properties:
        id: { type: string }
        pii: { type: string }
      required: [id, pii]
    key: [/id]

  acmeCo/derived:
    schema:
      type: object
      properties:
        id: { type: string }
        pii: { type: string, redact: { strategy: sha256 } }
      required: [id, pii]
    key: [/id]
    derive:
      using:
        sqlite: {}
      transforms:
        - name: echo
          source: { name: acmeCo/raw }
          shuffle: { key: [/id] }
          lambda: |
            SELECT JSON_OBJECT('id', $id, 'pii', $pii);

tests:
  acmeCo/test/derived-redaction:
    - ingest:
        collection: acmeCo/raw
        documents:
          - { id: a, pii: hello }
    - verify:
        collection: acmeCo/derived
        documents:
          - { id: a, pii: "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824" }
"#;

/// The empty-salt contract holds through a derivation too: the runner clears the
/// derivation's platform `redact_salt`, which is otherwise derived from the
/// task's shard-ID prefix and rotates with its generation.
#[tokio::test]
async fn derived_redaction_digests_are_unsalted() {
    let results = run(DERIVED_REDACTION).await;
    assert!(
        results.all_passed(),
        "a derived digest must be the unsalted SHA-256 of the raw value",
    );
}

const NESTED_COLLECTION_NAMES: &str = r#"
collections:
  acmeCo/ints:
    schema:
      type: object
      properties:
        Key: { type: string }
        Int: { type: integer }
      required: [Key, Int]
    key: [/Key]

  acmeCo/nest:
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
        - name: double
          source: { name: acmeCo/ints }
          shuffle: { key: [/Key] }
          lambda: |
            SELECT JSON_OBJECT('Key', $Key, 'Doubled', $Int * 2);

  acmeCo/nest/inner:
    schema:
      type: object
      properties:
        Key: { type: string }
        Quadrupled: { type: integer }
      required: [Key, Quadrupled]
    key: [/Key]
    derive:
      using:
        sqlite: {}
      transforms:
        - name: redouble
          source: { name: acmeCo/nest }
          shuffle: { key: [/Key] }
          lambda: |
            SELECT JSON_OBJECT('Key', $Key, 'Quadrupled', $Doubled * 2);

tests:
  acmeCo/test/nested:
    - ingest:
        collection: acmeCo/ints
        documents:
          - { Key: a, Int: 3 }
    - verify:
        collection: acmeCo/nest
        documents:
          - { Key: a, Doubled: 6 }
    - verify:
        collection: acmeCo/nest/inner
        documents:
          - { Key: a, Quadrupled: 12 }
"#;

/// A collection whose name is a prefix path of another's — `acmeCo/nest` and
/// `acmeCo/nest/inner`, both legal in the catalog namespace — must not have the
/// descendant's journals folded into its own. Selecting by the bare collection
/// name did exactly that, in two ways at once: the verify of `acmeCo/nest` read
/// `acmeCo/nest/inner`'s documents and failed on its schema, and `acmeCo/nest`'s
/// write clock carried the child's journals, so the `redouble` transform was fed
/// `acmeCo/nest/inner`'s own output as a source.
#[tokio::test]
async fn nested_collection_names_stay_separate() {
    let results = run(NESTED_COLLECTION_NAMES).await;
    assert!(
        results.all_passed(),
        "a collection nested under another's name must not share its journals",
    );
}

const SOURCE_PARTITION_SELECTOR: &str = r#"
collections:
  acmeCo/src:
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

  acmeCo/east-only:
    schema:
      type: object
      properties:
        id: { type: string }
        region: { type: string }
      required: [id, region]
    key: [/id]
    derive:
      using:
        sqlite: {}
      transforms:
        - name: echoEast
          source:
            name: acmeCo/src
            partitions: { include: { region: [east] } }
          shuffle: { key: [/id] }
          lambda: |
            SELECT JSON_OBJECT('id', $id, 'region', $region);

tests:
  acmeCo/test/source-selector:
    - ingest:
        collection: acmeCo/src
        documents:
          - { id: a, region: east }
          - { id: b, region: west }
          - { id: c, region: east }
    - verify:
        collection: acmeCo/east-only
        documents:
          - { id: a, region: east }
          - { id: c, region: east }
"#;

/// A transform's source partition selector restricts what the derivation
/// reads, as the production shuffle's journal listing does: `west` is never fed
/// to `echoEast`, so it must not appear in the derived collection.
#[tokio::test]
async fn source_partition_selector_filters_feed() {
    let results = run(SOURCE_PARTITION_SELECTOR).await;
    assert!(
        results.all_passed(),
        "source-selected derivation should pass"
    );
}

const CROSS_PARTITION_ORDER: &str = r#"
collections:
  acmeCo/events:
    schema:
      type: object
      properties:
        id: { type: string }
        region: { type: string }
        value: { type: integer }
      required: [id, region, value]
    key: [/id]
    projections:
      region:
        location: /region
        partition: true

  acmeCo/latest:
    schema:
      type: object
      properties:
        id: { type: string }
        value: { type: integer, reduce: { strategy: lastWriteWins } }
      required: [id, value]
      reduce: { strategy: merge }
    key: [/id]
    derive:
      using:
        sqlite: {}
      transforms:
        - name: fromEvents
          source: { name: acmeCo/events }
          shuffle: { key: [/id] }
          readDelay: 1h
          lambda: |
            SELECT JSON_OBJECT('id', $id, 'value', $value);

tests:
  acmeCo/test/cross-partition-order:
    # Journal names sort `east` before `west`, so a feeder ordering by journal
    # would read the *later* east write first and the west value would win.
    # Both ingests land in one delayed read; transaction order must hold.
    - ingest:
        collection: acmeCo/events
        documents:
          - { id: x, region: west, value: 1 }
    - ingest:
        collection: acmeCo/events
        documents:
          - { id: x, region: east, value: 2 }
    - verify:
        collection: acmeCo/latest
        documents:
          - { id: x, value: 2 }
"#;

/// Documents of an earlier ingest step are read ahead of a later step's, even
/// when a single delayed read spans partition journals whose names sort the
/// other way.
#[tokio::test]
async fn cross_partition_reads_follow_transaction_order() {
    let results = run(CROSS_PARTITION_ORDER).await;
    assert!(results.all_passed(), "later transaction's value should win");
}

const VERIFY_TRANSACTION_ORDER: &str = r#"
collections:
  acmeCo/events:
    schema:
      type: object
      properties:
        id: { type: string }
        region: { type: string }
        value: { type: integer, reduce: { strategy: lastWriteWins } }
      required: [id, region, value]
      reduce: { strategy: merge }
    key: [/id]
    projections:
      region:
        location: /region
        partition: true

tests:
  # `east` sorts before `west`, so a verify reading journal-by-journal would
  # reduce the later (east) write first and let the earlier (west) value win.
  acmeCo/test/later-txn-in-first-journal:
    - ingest:
        collection: acmeCo/events
        documents:
          - { id: x, region: west, value: 1 }
    - ingest:
        collection: acmeCo/events
        documents:
          - { id: x, region: east, value: 2 }
    - verify:
        collection: acmeCo/events
        documents:
          - { id: x, value: 2 }
  acmeCo/test/later-txn-in-last-journal:
    - ingest:
        collection: acmeCo/events
        documents:
          - { id: y, region: east, value: 1 }
    - ingest:
        collection: acmeCo/events
        documents:
          - { id: y, region: west, value: 2 }
    - verify:
        collection: acmeCo/events
        documents:
          - { id: y, value: 2 }
"#;

/// A verify reduces a collection's partitions in transaction order, not journal
/// order — the same contract the feeder gives a derivation. No derivation is
/// involved: this is purely the store's read-back path.
#[tokio::test]
async fn verify_reduces_in_transaction_order() {
    let results = run(VERIFY_TRANSACTION_ORDER).await;
    assert!(results.all_passed(), "later transaction's value should win");
}

/// Dropping a run's future mid-flight — with shard sessions live — must be
/// safe: no crash, and nothing left behind that spoils a later run in the same
/// process.
///
/// This is the crate's drop-safety contract: every shard's RocksDB is a tempdir
/// `runtime-next` makes and owns, so a dropped future's serve tasks unwind
/// detached, each removing its own directory. Callers may therefore bound a run
/// with `tokio::time::timeout` (or any select) without special teardown.
#[tokio::test]
async fn dropping_a_run_mid_flight_is_safe() {
    let output = build_catalog(RESET_BETWEEN_CASES).await;

    {
        let mut running = std::pin::pin!(run::run_tests(&output.built, run::Options::default()));

        // Poll the run forward, yielding so its spawned tasks make progress, and
        // require it to still be pending at every step.
        for i in 0..16 {
            assert!(
                futures::poll!(running.as_mut()).is_pending(),
                "run completed within {i} polls, so the drop below is not mid-flight",
            );
            tokio::task::yield_now().await;
        }
    } // The run's future drops here, mid-flight.

    // Nothing from the dropped run spoils a fresh one in the same process.
    let results = run::run_tests(&output.built, run::Options::default())
        .await
        .expect("a run following a dropped run must still succeed");

    assert_eq!(results.failed(), 0);
    assert_eq!(results.passed(), 2);
}
