//! Timeout and poisoning semantics of `run_tests`.

use catalog_tests::run;

/// Build a catalog from inline YAML to built specs, validating derivations
/// in-process (no Docker).
async fn build_catalog(yaml: &str) -> build::Output {
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

fn options(timeouts: run::Timeouts) -> run::Options {
    run::Options {
        timeouts,
        ..Default::default()
    }
}

/// The run-level error of a run which must fail. (`TestResults` isn't `Debug`,
/// so `expect_err` is unavailable.)
fn expect_err(result: anyhow::Result<run::TestResults>, whynot: &str) -> String {
    match result {
        Ok(results) => panic!("{whynot}, got:\n{}", render(&results)),
        Err(err) => format!("{err:#}"),
    }
}

/// One line per outcome, for snapshotting. The scope is deliberately omitted:
/// it carries the catalog's tempdir path.
fn render(results: &run::TestResults) -> String {
    results
        .outcomes
        .iter()
        .map(|outcome| match &outcome.status {
            run::TestStatus::Passed => format!("{}: passed", outcome.name),
            run::TestStatus::Failed { error } => format!("{}: failed: {error}", outcome.name),
            run::TestStatus::NotRun { reason } => format!("{}: not run: {reason}", outcome.name),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// A `local:` derive connector which stalls, as an inline catalog. `hang` is
/// `open` (never answers Open) or `transaction` (opens, then never commits).
fn hanging_catalog(hang: &str, tests: &str) -> String {
    let script = format!(
        "{}/tests/fixtures/hanging_connector.py",
        env!("CARGO_MANIFEST_DIR")
    );
    format!(
        r#"
collections:
  acmeCo/ints:
    schema:
      type: object
      properties:
        Key: {{ type: string }}
        Int: {{ type: integer }}
      required: [Key, Int]
    key: [/Key]

  acmeCo/hangs:
    schema:
      type: object
      properties:
        Key: {{ type: string }}
        Int: {{ type: integer }}
      required: [Key, Int]
    key: [/Key]
    derive:
      using:
        local:
          command: ["python3", "{script}"]
          config: {{ hang: {hang} }}
          protobuf: false
      transforms:
        - name: fromInts
          source: acmeCo/ints
          shuffle: {{ key: [/Key] }}
          # Never invoked: the connector never publishes.
          lambda: {{}}
{tests}"#
    )
}

/// A connector which never answers Open times out the session start, promptly
/// and as a run-level error — rather than holding a publication for as long as
/// the connector cares to stay silent.
#[tokio::test]
async fn a_session_which_never_opens_times_out() {
    let output = build_catalog(&hanging_catalog(
        "open",
        r#"
tests:
  acmeCo/test/never-opens:
    - ingest:
        collection: acmeCo/ints
        documents:
          - { Key: a, Int: 1 }
    - verify:
        collection: acmeCo/hangs
        documents:
          - { Key: a, Int: 1 }
"#,
    ))
    .await;

    let timeouts = run::Timeouts {
        start: std::time::Duration::from_millis(200),
        ..Default::default()
    };
    let started = std::time::Instant::now();
    let err = expect_err(
        run::run_tests(&output.built, options(timeouts)).await,
        "a session which never opens must fail the run",
    );
    let elapsed = started.elapsed();
    assert!(
        err.contains("starting derivation session for acmeCo/hangs")
            && err.contains("timed out after"),
        "the run error must name the derivation and the timeout, got:\n{err}",
    );
    // The connector is still sitting on its unanswered Open; the bound is what
    // ended the run, not the connector.
    assert!(
        elapsed < std::time::Duration::from_secs(30),
        "the run must end on its own bound, took {elapsed:?}",
    );
}

/// A transaction which never commits fails its case and poisons the run: the
/// session is no longer quiescent, so the inter-case Reset is skipped and every
/// later case is reported as not-run rather than silently understating
/// coverage. The run itself still returns `Ok` — the shutdown error which
/// follows is a symptom of the failure already recorded.
#[tokio::test]
async fn a_transaction_timeout_poisons_the_run() {
    let output = build_catalog(&hanging_catalog(
        "transaction",
        r#"
tests:
  acmeCo/test/1-first:
    - ingest:
        collection: acmeCo/ints
        documents:
          - { Key: a, Int: 1 }
    - verify:
        collection: acmeCo/hangs
        documents:
          - { Key: a, Int: 1 }
  acmeCo/test/2-second:
    - ingest:
        collection: acmeCo/ints
        documents:
          - { Key: b, Int: 2 }
    - verify:
        collection: acmeCo/hangs
        documents:
          - { Key: b, Int: 2 }
  acmeCo/test/3-third:
    - ingest:
        collection: acmeCo/ints
        documents:
          - { Key: c, Int: 3 }
    - verify:
        collection: acmeCo/hangs
        documents:
          - { Key: c, Int: 3 }
"#,
    ))
    .await;

    let timeouts = run::Timeouts {
        transaction: std::time::Duration::from_millis(200),
        ..Default::default()
    };
    let results = run::run_tests(&output.built, options(timeouts))
        .await
        .expect("a timed-out transaction is a recorded failure, not a run error");

    assert_eq!(results.failed(), 1);
    assert_eq!(results.not_run(), 2);
    insta::assert_snapshot!("transaction_timeout_poisons_the_run", render(&results));
}
