//! Verifies the control-plane's `test_catalog` linkage to `runtime-harness`:
//! publication tests run on the Rust-only runtime-next stack (no temp data-plane,
//! no `flowctl-go`), and a failing test surfaces a `tables::Error` to the
//! publication. Uses derive-sqlite so it needs no connector containers, and
//! `test_catalog` never touches Postgres, so this runs without a database.

use control_plane_api::logs;
use control_plane_api::publications::builds;

/// Build a derive-sqlite catalog (with tests) to a `build::Output`, validating
/// in-process with no control-plane round-trip.
async fn build_catalog(yaml: &str) -> build::Output {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("catalog.flow.yaml");
    std::fs::write(&path, yaml).unwrap();
    let url = build::arg_source_to_url(path.to_str().unwrap(), false).unwrap();

    let output = build::for_catalog_test(&url, "", ::ops::tracing_log_handler).await;
    assert!(
        output.errors().next().is_none(),
        "catalog should build cleanly: {:?}",
        output.errors().collect::<Vec<_>>()
    );
    output
}

/// A drained log sink standing in for a publication's `logs_tx`.
fn logs_sink() -> logs::Tx {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
    tokio::spawn(async move { while rx.recv().await.is_some() {} });
    tx
}

const PASSING: &str = r#"
collections:
  acmeCo/ints:
    schema:
      type: object
      properties:
        Key: { type: string }
        Int: { type: integer }
      required: [Key, Int]
    key: [/Key]
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
        - name: fromInts
          source: { name: acmeCo/ints }
          shuffle: { key: [/Key] }
          lambda: SELECT JSON_OBJECT('Key', $Key, 'Doubled', $Int * 2);
tests:
  acmeCo/test/doubles:
    - ingest:
        collection: acmeCo/ints
        documents:
          - { Key: a, Int: 3 }
    - verify:
        collection: acmeCo/doubled
        documents:
          - { Key: a, Doubled: 6 }
"#;

/// A catalog whose expectation is deliberately wrong.
const FAILING: &str = r#"
collections:
  acmeCo/ints:
    schema:
      type: object
      properties:
        Key: { type: string }
        Int: { type: integer }
      required: [Key, Int]
    key: [/Key]
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
        - name: fromInts
          source: { name: acmeCo/ints }
          shuffle: { key: [/Key] }
          lambda: SELECT JSON_OBJECT('Key', $Key, 'Doubled', $Int * 2);
tests:
  acmeCo/test/wrong:
    - ingest:
        collection: acmeCo/ints
        documents:
          - { Key: a, Int: 3 }
    - verify:
        collection: acmeCo/doubled
        documents:
          - { Key: a, Doubled: 999 }
"#;

#[tokio::test]
async fn passing_catalog_test_reports_no_errors() {
    let output = build_catalog(PASSING).await;
    let logs_token = uuid::Uuid::nil();

    let errors = builds::test_catalog(logs_token, &logs_sink(), &output)
        .await
        .expect("test_catalog runs");

    assert!(
        errors.is_empty(),
        "a passing catalog test should surface no errors, got: {:?}",
        errors
            .iter()
            .map(|e| e.error.to_string())
            .collect::<Vec<_>>(),
    );
}

#[tokio::test]
async fn failing_catalog_test_surfaces_error() {
    let output = build_catalog(FAILING).await;
    let logs_token = uuid::Uuid::nil();

    let errors = builds::test_catalog(logs_token, &logs_sink(), &output)
        .await
        .expect("test_catalog runs");

    assert_eq!(
        errors.len(),
        1,
        "the wrong expectation must surface one error"
    );
    let rendered = errors.iter().next().unwrap().error.to_string();
    assert!(
        rendered.contains("acmeCo/test/wrong") && rendered.contains("999"),
        "error should name the failing test and render the diff, got:\n{rendered}",
    );
}
