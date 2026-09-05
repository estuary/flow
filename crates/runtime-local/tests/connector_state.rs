//! End-to-end coverage of the connector-state fields this crate threads onto
//! the `Shard` protocol: [`Controls::initial_state_json`] becoming
//! `SessionLoop.initial_connector_state_json` going in, and
//! `Stopped.connector_state_json` coming back out as `run_sessions`' return.
//!
//! It drives a real capture through [`capture_driver`] — captures are
//! leaderless, so this needs no leader, no shuffle topology, and no Docker (the
//! connector is a `local:` python script). The capture's RocksDB is a tempdir
//! `runtime-next` makes and owns; nothing here names a path.
//!
//! Published documents are discarded by `runtime_next::RecordingPublisher`:
//! what the connector captured is beside the point, and only shard zero's
//! reported state is asserted.

use runtime_local::Controls;

/// A capture whose `local:` connector advances a `cursor` in its connector
/// state, one merge patch per transaction.
fn catalog(transactions: u32) -> String {
    let script = format!(
        "{}/tests/fixtures/state_connector.py",
        env!("CARGO_MANIFEST_DIR")
    );
    format!(
        r#"
captures:
  acmeCo/source:
    endpoint:
      local:
        command: ["python3", "{script}"]
        config: {{ transactions: {transactions} }}
        protobuf: false
    bindings:
      - resource: {{ name: docs }}
        target: acmeCo/docs

collections:
  acmeCo/docs:
    schema:
      type: object
      properties:
        id: {{ type: string }}
      required: [id]
    key: [/id]
"#
    )
}

/// Run one single-shard capture of `transactions` transactions under `controls`,
/// returning the final connector state that shard zero reported at `Stopped`.
///
/// A `u32::MAX` session target is preview's "unbounded": the session ends when
/// the connector exits, having emitted all of its checkpoints.
async fn run_capture(
    transactions: u32,
    session_targets: Vec<u32>,
    initial_state_json: bytes::Bytes,
    report_final_state: bool,
) -> Option<bytes::Bytes> {
    // The runtime-next loopback stack dials over rustls; install a process
    // crypto provider once (idempotent across tests), as `flowctl` main does.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let spec = build_capture(&catalog(transactions)).await;

    let registry = service_kit::Registry::new();
    let run = runtime_local::services::Run::start_capture(
        runtime_local::local_router(String::new(), registry.clone()),
        1,
        None,
        registry,
    )
    .await
    .unwrap();

    let controls = Controls {
        initial_state_json,
        report_final_state,
        publisher_factory: runtime_next::RecordingPublisherFactory,
        logger_factory: runtime_next::TracingLoggerFactory,
    };

    runtime_local::capture_driver::run_sessions(
        &run,
        &spec,
        session_targets,
        controls,
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .expect("capture sessions run")
}

/// Build `yaml` and return its one capture's spec. `for_local_test` (not
/// `for_catalog_test`) because this actually runs the capture's connector.
async fn build_capture(yaml: &str) -> proto_flow::flow::CaptureSpec {
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
        .built_captures
        .iter()
        .find_map(|row| row.spec.clone())
        .expect("the catalog builds one capture")
}

/// Seed a base document through `SessionLoop`, run three transactions, and
/// confirm the state reported at `Stopped` is the seed with the connector's
/// three merge patches folded in — i.e. the last committed transaction's.
#[tokio::test]
async fn initial_and_final_connector_state_round_trip() {
    let final_state = run_capture(
        3,
        vec![u32::MAX],
        bytes::Bytes::from_static(br#"{"cursor":10,"kept":"yes"}"#),
        true,
    )
    .await
    .expect("report_final_state asked for a state");
    let final_state: serde_json::Value = serde_json::from_slice(&final_state).unwrap();

    // `cursor` advanced 10 → 13, and the seed's unrelated field survived the
    // merge patches.
    assert_eq!(
        final_state,
        serde_json::json!({"cursor": 13, "kept": "yes"}),
    );
}

/// Without `report_final_state`, `Stopped` carries no state.
#[tokio::test]
async fn final_state_is_absent_unless_requested() {
    assert_eq!(
        run_capture(1, vec![u32::MAX], bytes::Bytes::new(), false).await,
        None
    );
}

/// A run which stops before any session reaches `Stopped` still reports the
/// state it began with, so `flowctl preview --output-state` always emits a
/// state line on success.
#[tokio::test]
async fn zero_session_run_reports_initial_state() {
    let seed = bytes::Bytes::from_static(br#"{"cursor":10}"#);
    assert_eq!(
        run_capture(1, Vec::new(), seed.clone(), true).await,
        Some(seed),
    );
}
