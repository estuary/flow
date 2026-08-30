//! Regression tests for issue #3245: "runtime-v2: multi-shard shuffle open
//! deadlocks when a peer sidecar is briefly unreachable".
//!
//! These exercise the Session→Slice→Log open fan-out across two in-process
//! `Service` instances with distinct `peer_endpoint`s, one of which runs a real
//! tonic server over `http://127.0.0.1:<port>` loopback. They assert that a
//! transient open failure of any peer promptly errors the whole `open` (rather
//! than wedging forever) and leaves no partial rendezvous state behind in the
//! process-global `Service::log_joins` map.
//!
//! Against the unfixed code these tests fail by exhausting their `tokio::time`
//! timeout: `join_all` traps the peer's error while sibling in-process opens
//! park on a Log rendezvous that can never complete.

use proto_flow::{flow, shuffle};

/// A journal-client factory that is never dialed by these tests: `open`
/// aborts (or is torn down) before any Slice actor begins reading journals.
/// It builds a client pointed at an unroutable endpoint so that, in the one
/// test where `open` succeeds, the post-open listing simply errors and tears
/// the session down instead of panicking.
fn dummy_journal_factory() -> gazette::journal::ClientFactory {
    std::sync::Arc::new(|_authz_sub: String, _authz_obj: String| {
        gazette::journal::Client::new(
            "http://127.0.0.1:1".to_string(),
            gazette::journal::Client::new_fragment_client(),
            proto_grpc::Metadata::new(),
            gazette::Router::new("local"),
        )
    })
}

/// Build a Service with the given `peer_endpoint`, unauthenticated (no signer),
/// matching how tests run the shuffle fan-out.
fn build_service(peer_endpoint: &str) -> crate::Service {
    crate::Service::new(
        peer_endpoint.to_string(),
        dummy_journal_factory(),
        crate::DEFAULT_SHUFFLE_DISK_LIMIT_BYTES,
        service_kit::Registry::new(),
        None,
    )
}

/// Bind a loopback listener and return it alongside its `http://` endpoint.
async fn bind_endpoint() -> (tokio::net::TcpListener, String) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    (listener, endpoint)
}

/// Reserve then release a loopback port, yielding an endpoint that will refuse
/// connections instantly (nothing is listening).
async fn unbound_endpoint() -> String {
    let (listener, endpoint) = bind_endpoint().await;
    drop(listener);
    endpoint
}

/// Serve `service` on `listener` until the returned handle is aborted.
fn serve(
    service: crate::Service,
    listener: tokio::net::TcpListener,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let _ = service
            .build_tonic_server()
            .serve_with_incoming(incoming)
            .await;
    })
}

/// Build an N-shard topology, one endpoint per shard, with each shard's log
/// directory rooted under `dir` (created so a completing Log can open a Writer).
fn build_shards(endpoints: &[&str], dir: &std::path::Path) -> Vec<shuffle::Shard> {
    let count = endpoints.len() as u32;
    labels::shard::even_splits("open-deadlock", count, 1)
        .into_iter()
        .zip(endpoints)
        .enumerate()
        .map(|(i, (split, endpoint))| {
            let directory = dir.join(format!("shard-{i:03}"));
            std::fs::create_dir_all(&directory).unwrap();

            shuffle::Shard {
                id: split.id,
                range: Some(split.range),
                endpoint: endpoint.to_string(),
                directory: directory.to_str().unwrap().to_string(),
                ..Default::default()
            }
        })
        .collect()
}

fn build_task(spec: &flow::MaterializationSpec) -> shuffle::Task {
    shuffle::Task {
        task: Some(shuffle::task::Task::Materialization(spec.clone())),
    }
}

/// Build the shuffle task from the shared catalog fixture (offline; no
/// data-plane). Only the open path is exercised, so the bindings' shuffle
/// configuration is all that matters.
async fn fixture_task() -> shuffle::Task {
    let source = build::arg_source_to_url("./tests/shuffle.flow.yaml", false).unwrap();
    let output = build::for_local_test(&source, true)
        .await
        .into_result()
        .expect("build of catalog fixture should succeed");

    let spec = output
        .built
        .built_materializations
        .get_by_key(&models::Materialization::new("testing/materialization"))
        .expect("should have built materialization")
        .spec
        .as_ref()
        .expect("built materialization should have a spec")
        .clone();

    build_task(&spec)
}

/// Poll `service.log_joins` until it drains to empty, or panic after the bound.
async fn await_log_joins_empty(service: &crate::Service, label: &str) {
    for _ in 0..200 {
        if service.log_joins.lock().unwrap().is_empty() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!(
        "{label}: log_joins did not drain: {} entries remain",
        service.log_joins.lock().unwrap().len(),
    );
}

/// Guards against a hang: `open` must resolve (Ok or Err) well within this
/// bound. Against the unfixed code it never resolves, so the timeout fires.
const OPEN_GUARD: std::time::Duration = std::time::Duration::from_secs(30);

/// (a) Trapped-error repro: a 2-shard session whose peer endpoint refuses
/// connections instantly must fail `open` promptly rather than deadlocking.
#[tokio::test]
async fn open_errors_when_peer_dial_refused() {
    let task = fixture_task().await;

    // Shard 0 is in-process; shard 1 points at an unbound port (instant refuse).
    let endpoint_a = "http://127.0.0.1:1".to_string();
    let service = build_service(&endpoint_a);
    let unbound = unbound_endpoint().await;

    let dir = tempfile::tempdir().unwrap();
    let shards = build_shards(&[&endpoint_a, &unbound], dir.path());

    let result = tokio::time::timeout(
        OPEN_GUARD,
        crate::SessionClient::open(&service, task, shards, Default::default()),
    )
    .await
    .expect("open must not hang when a peer is unreachable (issue #3245)");

    let Err(err) = result else {
        panic!("open must fail when a peer dial is refused");
    };
    tracing::info!(%err, "open failed as expected");

    await_log_joins_empty(&service, "dial-refused").await;
}

/// (b) Reachable-peer open failure: the peer sidecar is reachable, but its
/// Slice open is rejected (endpoint-identity mismatch). `open` must error and
/// both Services' `log_joins` maps must drain to empty.
#[tokio::test]
async fn open_errors_when_reachable_peer_rejects() {
    let task = fixture_task().await;

    // Coordinator (shard 0, in-process). It makes remote hops to the peer for
    // both the Slice RPC and its in-process Slice's Log RPC, but needs no
    // server of its own: the peer's Slice open fails before it dials back.
    let endpoint_a = "http://127.0.0.1:1".to_string();
    let service_a = build_service(&endpoint_a);

    // Reachable peer (shard 1). Its `peer_endpoint` deliberately mismatches the
    // address shard 1 is dialed at, so its Slice open rejects with an
    // endpoint-identity error — a reachable-peer open failure, not a dial error.
    let (listener_b, endpoint_b) = bind_endpoint().await;
    let service_b = build_service("http://127.0.0.1:9");
    let handle_b = serve(service_b.clone(), listener_b);

    let dir = tempfile::tempdir().unwrap();
    let shards = build_shards(&[&endpoint_a, &endpoint_b], dir.path());

    let result = tokio::time::timeout(
        OPEN_GUARD,
        crate::SessionClient::open(&service_a, task, shards, Default::default()),
    )
    .await
    .expect("open must not hang when a reachable peer rejects (issue #3245)");

    let Err(err) = result else {
        panic!("open must fail when a reachable peer rejects the Slice open");
    };
    tracing::info!(%err, "open failed as expected");

    // Both the coordinator's in-process Log and the peer's remote Log
    // registered partial rendezvous state that must be reaped on abort.
    await_log_joins_empty(&service_a, "reachable-reject coordinator").await;
    await_log_joins_empty(&service_b, "reachable-reject peer").await;

    handle_b.abort();
}

/// (c) Abort-then-retry: a partial rendezvous that aborts must not poison a
/// subsequent session reusing the same shard directories. The stale slot is
/// reaped, and the retry opens cleanly (no "duplicate Slice connection").
#[tokio::test]
async fn retry_after_abort_reuses_directory() {
    let task = fixture_task().await;

    // Two fully-functional peers, each serving loopback. The retry needs both
    // (the peer's Slice dials back to the coordinator's Log).
    let (listener_a, endpoint_a) = bind_endpoint().await;
    let service_a = build_service(&endpoint_a);
    let handle_a = serve(service_a.clone(), listener_a);

    let (listener_b, endpoint_b) = bind_endpoint().await;
    let service_b = build_service(&endpoint_b);
    let handle_b = serve(service_b.clone(), listener_b);

    let dir = tempfile::tempdir().unwrap();

    // Attempt 1: shard 1 points at an unbound port, aborting a partial
    // rendezvous on the coordinator's in-process Log.
    let unbound = unbound_endpoint().await;
    let shards1 = build_shards(&[&endpoint_a, &unbound], dir.path());

    let result = tokio::time::timeout(
        OPEN_GUARD,
        crate::SessionClient::open(&service_a, task.clone(), shards1, Default::default()),
    )
    .await
    .expect("first open must not hang (issue #3245)");
    assert!(
        result.is_err(),
        "first open must fail with shard 1 unreachable",
    );

    // The stale slot must be reaped before the retry reuses the directory.
    await_log_joins_empty(&service_a, "retry attempt-1 coordinator").await;

    // Attempt 2: shard 1 now points at the reachable peer, reusing the same
    // shard directories. This must open cleanly.
    let shards2 = build_shards(&[&endpoint_a, &endpoint_b], dir.path());

    let session = tokio::time::timeout(
        OPEN_GUARD,
        crate::SessionClient::open(&service_a, task, shards2, Default::default()),
    )
    .await
    .expect("retry open must not hang")
    .expect("retry open must succeed (no duplicate Slice connection)");

    // Drop the session to tear down; the dummy factory would error a real read.
    drop(session);

    handle_a.abort();
    handle_b.abort();
}
