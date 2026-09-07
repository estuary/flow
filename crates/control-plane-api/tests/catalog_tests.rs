//! Verifies the control-plane's `test_catalog` linkage to `catalog-tests`:
//! publication tests run on the Rust-only runtime-next stack, every derivation
//! reaches its connector through the router the publisher supplies, and a
//! failing test surfaces a `tables::Error` to the publication.
//!
//! Which data plane a name resolves to is the router's own concern, and is
//! covered by the unit tests of `publications::catalog_test_router`. What's
//! proven here is that `test_catalog` routes at all.

use control_plane_api::logs;
use control_plane_api::publications::builds;

/// Records every session it is asked to open, then delegates to `inner`.
struct RecordingRouter {
    inner: std::sync::Arc<dyn proto_grpc::connector::Router>,
    opened: std::sync::Arc<std::sync::Mutex<Vec<(ops::TaskType, String)>>>,
}

impl proto_grpc::connector::Router for RecordingRouter {
    fn open(
        &self,
        task_type: ops::TaskType,
        task_name: &str,
        request_rx: tokio::sync::mpsc::Receiver<proto_flow::connector::Request>,
    ) -> tokio::sync::mpsc::Receiver<tonic::Result<proto_flow::connector::Response>> {
        self.opened
            .lock()
            .unwrap()
            .push((task_type, task_name.to_string()));

        self.inner.open(task_type, task_name, request_rx)
    }
}

impl RecordingRouter {
    fn new() -> (
        std::sync::Arc<dyn proto_grpc::connector::Router>,
        std::sync::Arc<std::sync::Mutex<Vec<(ops::TaskType, String)>>>,
    ) {
        let opened = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));

        (
            std::sync::Arc::new(Self {
                inner: runtime_local::local_test_router(),
                opened: opened.clone(),
            }),
            opened,
        )
    }
}

/// Build a derive-sqlite catalog (with tests) to a `build::Output`, validating
/// in-process with no control-plane round-trip.
async fn build_catalog(yaml: &str) -> build::Output {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("catalog.flow.yaml");
    std::fs::write(&path, yaml).unwrap();
    let url = build::arg_source_to_url(path.to_str().unwrap(), false).unwrap();

    let connector_router = runtime_local::local_test_router();
    let output = build::for_catalog_test(&url, connector_router, ::ops::tracing_log_handler).await;
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
    let (router, opened) = RecordingRouter::new();

    let errors = builds::test_catalog(logs_token, &logs_sink(), &output, router)
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

    // `test_catalog` splits the derivation across shards and may restart one,
    // so assert over distinct sessions rather than a count.
    let mut opened = opened.lock().unwrap().clone();
    opened.sort();
    opened.dedup();

    assert_eq!(
        opened,
        vec![(ops::TaskType::Derivation, "acmeCo/doubled".to_string())],
        "the derivation's connector is the only session, and it is routed",
    );
}

#[tokio::test]
async fn failing_catalog_test_surfaces_error() {
    let output = build_catalog(FAILING).await;
    let logs_token = uuid::Uuid::nil();

    let errors = builds::test_catalog(
        logs_token,
        &logs_sink(),
        &output,
        runtime_local::local_test_router(),
    )
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
