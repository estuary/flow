/// Integration tests for the shuffle crate, exercising the full
/// Session→Slice→Log pipeline against real published documents.
///
/// The fixture defines three collections (apples, bananas, cherries) shared
/// between a capture and a materialization. Tests build the shuffle task from
/// the MaterializationSpec (exercising `Binding::from_materialization_binding`)
/// and construct the publisher from the CaptureSpec (exercising
/// `Binding::from_capture_spec`).
use proto_flow::{flow, shuffle as proto};
use proto_gazette::uuid;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a Materialization task from a built MaterializationSpec.
/// Exercises `shuffle::Binding::from_materialization_binding()`.
fn build_task(spec: &flow::MaterializationSpec) -> proto::Task {
    proto::Task {
        task: Some(proto::task::Task::Materialization(spec.clone())),
    }
}

/// Build an N-member topology with all members sharing a single endpoint.
fn build_members(count: u32, endpoint: &str, directory: &std::path::Path) -> Vec<proto::Member> {
    (0..count)
        .map(|i| {
            let key_begin = if i == 0 {
                0
            } else {
                ((i as u64 * (u32::MAX as u64 + 1)) / count as u64) as u32
            };
            let key_end = if i == count - 1 {
                u32::MAX
            } else {
                (((i + 1) as u64 * (u32::MAX as u64 + 1)) / count as u64 - 1) as u32
            };

            proto::Member {
                range: Some(flow::RangeSpec {
                    key_begin,
                    key_end,
                    r_clock_begin: 0,
                    r_clock_end: u32::MAX,
                }),
                endpoint: endpoint.to_string(),
                directory: directory.to_str().unwrap().to_string(),
            }
        })
        .collect()
}

/// Build a Publisher from a CaptureSpec.
/// Exercises `publisher::Binding::from_capture_spec()`.
fn make_publisher(
    spec: &flow::CaptureSpec,
    journal_client: &gazette::journal::Client,
    producer: uuid::Producer,
) -> publisher::Publisher {
    let client_factory: publisher::JournalClientFactory = Arc::new({
        let journal_client = journal_client.clone();
        move |_collection, _task_name| journal_client.clone()
    });

    let bindings = publisher::Binding::from_capture_spec(spec, &client_factory)
        .expect("should build bindings from capture spec");

    publisher::Publisher::new(bindings, producer, uuid::Clock::default())
}

/// Compact frontier representation for stable, readable snapshots.
///
/// Clocks are expressed as (seconds, nanos) relative to UNIX epoch.
/// Offsets retain their sign convention: negative = committed end,
/// non-negative = pending begin.
fn snapshot_frontier(
    frontier: &shuffle::Frontier,
) -> Vec<(&str, u16, Vec<(&str, (u64, u32), (u64, u32), i64)>)> {
    frontier
        .journals
        .iter()
        .map(|jf| {
            let producers: Vec<_> = jf
                .producers
                .iter()
                .map(|pf| {
                    // Strip the "Producer(...)" wrapper for brevity.
                    let p = format!("{:?}", pf.producer);
                    let p_str = p
                        .strip_prefix("Producer(")
                        .and_then(|s| s.strip_suffix(')'))
                        .unwrap_or(&p);
                    // Leak the string so we can return a &str — fine for test assertions.
                    let p_ref: &str = Box::leak(p_str.to_string().into_boxed_str());
                    (
                        p_ref,
                        pf.last_commit.to_unix(),
                        pf.hinted_commit.to_unix(),
                        pf.offset,
                    )
                })
                .collect();
            (jf.journal.as_ref(), jf.binding, producers)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Main test harness
// ---------------------------------------------------------------------------

/// Shared DataPlane and shuffle server, with all scenarios run sequentially.
#[tokio::test]
async fn shuffle_scenarios() {
    // Build the catalog fixture.
    let source = build::arg_source_to_url("./tests/shuffle.flow.yaml", false).unwrap();
    let build_output = Arc::new(
        build::for_local_test(&source, true)
            .await
            .into_result()
            .expect("build of catalog fixture should succeed"),
    );

    let materialization_spec = build_output
        .built
        .built_materializations
        .get_by_key(&models::Materialization::new("testing/materialization"))
        .expect("should have built materialization")
        .spec
        .as_ref()
        .expect("built materialization should have a spec")
        .clone();

    let capture_spec = build_output
        .built
        .built_captures
        .get_by_key(&models::Capture::new("testing/capture"))
        .expect("should have built capture")
        .spec
        .as_ref()
        .expect("built capture should have a spec")
        .clone();

    // Start the hermetic data-plane.
    let data_plane = e2e_support::DataPlane::start(Default::default())
        .await
        .expect("DataPlane start");

    // Start a shuffle gRPC server so that multi-member Slice/Log RPCs
    // can dial back to us.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind shuffle server");
    let endpoint = format!("http://{}", listener.local_addr().unwrap());

    let service = {
        let journal_client = data_plane.journal_client.clone();
        shuffle::Service::new(
            endpoint.clone(),
            Box::new(move |_collection, _task| journal_client.clone()),
        )
    };

    let server = service.clone().build_tonic_server();
    let server_handle = tokio::spawn(async move {
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        server
            .serve_with_incoming(incoming)
            .await
            .expect("shuffle server error")
    });

    let log_dir = tempfile::tempdir().expect("create temp dir for log segments");

    // Run test scenarios sequentially, resetting the data-plane between each
    // to ensure clean journal state.
    single_producer_outside_txn(
        &materialization_spec,
        &capture_spec,
        &data_plane.journal_client,
        &service,
        log_dir.path(),
    )
    .await;
    data_plane.reset().await.expect("reset");

    continue_then_ack(
        &materialization_spec,
        &capture_spec,
        &data_plane.journal_client,
        &service,
        log_dir.path(),
    )
    .await;
    data_plane.reset().await.expect("reset");

    multi_member_routing(
        &materialization_spec,
        &capture_spec,
        &data_plane.journal_client,
        &service,
        log_dir.path(),
    )
    .await;
    data_plane.reset().await.expect("reset");

    multiple_producers(
        &materialization_spec,
        &capture_spec,
        &data_plane.journal_client,
        &service,
        log_dir.path(),
    )
    .await;
    data_plane.reset().await.expect("reset");

    resume_from_checkpoint(
        &materialization_spec,
        &capture_spec,
        &data_plane.journal_client,
        &service,
        log_dir.path(),
    )
    .await;
    data_plane.reset().await.expect("reset");

    multi_partition_transaction(
        &materialization_spec,
        &capture_spec,
        &data_plane.journal_client,
        &service,
        log_dir.path(),
    )
    .await;
    data_plane.reset().await.expect("reset");

    partition_filtered_hints(
        &materialization_spec,
        &capture_spec,
        &data_plane.journal_client,
        &service,
        log_dir.path(),
    )
    .await;

    server_handle.abort();
    data_plane
        .graceful_stop()
        .await
        .expect("DataPlane graceful_stop");
}

// ---------------------------------------------------------------------------
// Test scenarios
// ---------------------------------------------------------------------------

/// Publish several OUTSIDE_TXN documents (self-committing) to a single
/// partition. Open a 1-member session, poll a checkpoint, and verify the
/// frontier shows the producer committed at the correct clock.
async fn single_producer_outside_txn(
    materialization_spec: &flow::MaterializationSpec,
    capture_spec: &flow::CaptureSpec,
    journal_client: &gazette::journal::Client,
    service: &shuffle::Service,
    log_dir: &std::path::Path,
) {
    let scenario_dir = log_dir.join("single_producer_outside_txn");
    std::fs::create_dir_all(&scenario_dir).unwrap();

    let producer = uuid::Producer::from_bytes([0x01, 0x00, 0x00, 0x00, 0x00, 0x01]);
    let mut pub_ = make_publisher(capture_spec, journal_client, producer);

    for id in ["a1", "a2", "a3"] {
        pub_.enqueue(
            |uuid| {
                (
                    0,
                    serde_json::json!({
                        "_meta": {"uuid": uuid.to_string()},
                        "id": id,
                        "category": "alpha",
                        "value": 1,
                    }),
                )
            },
            uuid::Flags::OUTSIDE_TXN,
        )
        .await
        .unwrap();
    }
    pub_.flush().await.unwrap();

    let mut session = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_members(1, service.peer_endpoint(), &scenario_dir),
        Default::default(),
    )
    .await
    .expect("SessionClient::open");

    let frontier = session.next_checkpoint().await.expect("next_checkpoint");
    insta::assert_debug_snapshot!("single_producer_outside_txn", snapshot_frontier(&frontier));

    session.close().await.expect("close");
}

/// Publish CONTINUE_TXN documents, then commit via ACK. Verify the frontier
/// reflects the committed clock matching the ACK.
async fn continue_then_ack(
    materialization_spec: &flow::MaterializationSpec,
    capture_spec: &flow::CaptureSpec,
    journal_client: &gazette::journal::Client,
    service: &shuffle::Service,
    log_dir: &std::path::Path,
) {
    let scenario_dir = log_dir.join("continue_then_ack");
    std::fs::create_dir_all(&scenario_dir).unwrap();

    let producer = uuid::Producer::from_bytes([0x01, 0x00, 0x00, 0x00, 0x00, 0x01]);
    let mut pub_ = make_publisher(capture_spec, journal_client, producer);

    for id in ["b1", "b2"] {
        pub_.enqueue(
            |uuid| {
                (
                    0,
                    serde_json::json!({
                        "_meta": {"uuid": uuid.to_string()},
                        "id": id,
                        "category": "alpha",
                        "value": 2,
                    }),
                )
            },
            uuid::Flags::CONTINUE_TXN,
        )
        .await
        .unwrap();
    }

    // Commit the transaction.
    let (producer, commit_clock, journals) = pub_.commit_intents();
    let journal_acks =
        publisher::intents::build_transaction_intents(&[(producer, commit_clock, journals)]);
    pub_.write_intents(&journal_acks).await.unwrap();

    let mut session = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_members(1, service.peer_endpoint(), &scenario_dir),
        Default::default(),
    )
    .await
    .expect("SessionClient::open");

    let frontier = session.next_checkpoint().await.expect("next_checkpoint");
    insta::assert_debug_snapshot!("continue_then_ack", snapshot_frontier(&frontier));

    session.close().await.expect("close");
}

/// Open a session with 3 members (split key space). Publish documents with
/// varied /id values so they hash to different key ranges. Verify the
/// frontier covers all journals.
async fn multi_member_routing(
    materialization_spec: &flow::MaterializationSpec,
    capture_spec: &flow::CaptureSpec,
    journal_client: &gazette::journal::Client,
    service: &shuffle::Service,
    log_dir: &std::path::Path,
) {
    let scenario_dir = log_dir.join("multi_member_routing");
    std::fs::create_dir_all(&scenario_dir).unwrap();

    let producer = uuid::Producer::from_bytes([0x01, 0x00, 0x00, 0x00, 0x00, 0x01]);
    let mut pub_ = make_publisher(capture_spec, journal_client, producer);

    // Use varied IDs that will produce different key hashes, exercising
    // routing across the 3-member topology.
    for id in ["m-aaa", "m-bbb", "m-ccc", "m-ddd", "m-eee"] {
        pub_.enqueue(
            |uuid| {
                (
                    0,
                    serde_json::json!({
                        "_meta": {"uuid": uuid.to_string()},
                        "id": id,
                        "category": "alpha",
                        "value": 3,
                    }),
                )
            },
            uuid::Flags::OUTSIDE_TXN,
        )
        .await
        .unwrap();
    }
    pub_.flush().await.unwrap();

    let mut session = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_members(3, service.peer_endpoint(), &scenario_dir),
        Default::default(),
    )
    .await
    .expect("SessionClient::open");

    let frontier = session.next_checkpoint().await.expect("next_checkpoint");
    insta::assert_debug_snapshot!("multi_member_routing", snapshot_frontier(&frontier));

    session.close().await.expect("close");
}

/// Two publishers with distinct Producer IDs write OUTSIDE_TXN documents
/// to the same collection. Verify the frontier tracks both producers.
async fn multiple_producers(
    materialization_spec: &flow::MaterializationSpec,
    capture_spec: &flow::CaptureSpec,
    journal_client: &gazette::journal::Client,
    service: &shuffle::Service,
    log_dir: &std::path::Path,
) {
    let scenario_dir = log_dir.join("multiple_producers");
    std::fs::create_dir_all(&scenario_dir).unwrap();

    let p1 = uuid::Producer::from_bytes([0x01, 0x00, 0x00, 0x00, 0x00, 0x01]);
    let p2 = uuid::Producer::from_bytes([0x03, 0x00, 0x00, 0x00, 0x00, 0x02]);

    let mut pub1 = make_publisher(capture_spec, journal_client, p1);
    let mut pub2 = make_publisher(capture_spec, journal_client, p2);

    // Serialize writes to ensure deterministic journal layout:
    // p1's data occupies the first bytes, p2's data follows.
    pub1.enqueue(
        |uuid| {
            (
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "p1-doc",
                    "category": "alpha",
                    "value": 10,
                }),
            )
        },
        uuid::Flags::OUTSIDE_TXN,
    )
    .await
    .unwrap();
    pub1.flush().await.unwrap();

    pub2.enqueue(
        |uuid| {
            (
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "p2-doc",
                    "category": "alpha",
                    "value": 20,
                }),
            )
        },
        uuid::Flags::OUTSIDE_TXN,
    )
    .await
    .unwrap();
    pub2.flush().await.unwrap();

    let mut session = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_members(1, service.peer_endpoint(), &scenario_dir),
        Default::default(),
    )
    .await
    .expect("SessionClient::open");

    // Each OUTSIDE_TXN commit triggers a separate flush cycle, so we need
    // two checkpoints to capture both producers' progress.
    let f1 = session.next_checkpoint().await.expect("next_checkpoint");
    let f2 = session.next_checkpoint().await.expect("next_checkpoint");
    let frontier = f1.reduce(f2);
    insta::assert_debug_snapshot!("multiple_producers", snapshot_frontier(&frontier));

    session.close().await.expect("close");
}

/// Phase 1: write documents, capture a checkpoint.
/// Phase 2: write more documents, resume from the captured checkpoint.
/// Assert the new checkpoint reflects only the progress from phase 2.
async fn resume_from_checkpoint(
    materialization_spec: &flow::MaterializationSpec,
    capture_spec: &flow::CaptureSpec,
    journal_client: &gazette::journal::Client,
    service: &shuffle::Service,
    log_dir: &std::path::Path,
) {
    let phase1_dir = log_dir.join("resume_checkpoint_p1");
    let phase2_dir = log_dir.join("resume_checkpoint_p2");
    std::fs::create_dir_all(&phase1_dir).unwrap();
    std::fs::create_dir_all(&phase2_dir).unwrap();

    let producer = uuid::Producer::from_bytes([0x01, 0x00, 0x00, 0x00, 0x00, 0x01]);

    // ---- Phase 1: write initial docs and capture a checkpoint. ----
    let mut pub_ = make_publisher(capture_spec, journal_client, producer);

    for id in ["r1", "r2"] {
        pub_.enqueue(
            |uuid| {
                (
                    0,
                    serde_json::json!({
                        "_meta": {"uuid": uuid.to_string()},
                        "id": id,
                        "category": "alpha",
                        "value": 100,
                    }),
                )
            },
            uuid::Flags::OUTSIDE_TXN,
        )
        .await
        .unwrap();
    }
    pub_.flush().await.unwrap();

    let mut session = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_members(1, service.peer_endpoint(), &phase1_dir),
        Default::default(),
    )
    .await
    .expect("SessionClient::open");

    let phase1_frontier = session.next_checkpoint().await.expect("next_checkpoint");
    insta::assert_debug_snapshot!(
        "resume_from_checkpoint_phase1",
        snapshot_frontier(&phase1_frontier)
    );
    session.close().await.expect("close");

    // ---- Phase 2: write more docs, resume from the phase 1 checkpoint. ----

    // Continue with the same publisher (same producer, clock advances).
    for id in ["r3", "r4"] {
        pub_.enqueue(
            |uuid| {
                (
                    0,
                    serde_json::json!({
                        "_meta": {"uuid": uuid.to_string()},
                        "id": id,
                        "category": "alpha",
                        "value": 200,
                    }),
                )
            },
            uuid::Flags::OUTSIDE_TXN,
        )
        .await
        .unwrap();
    }
    pub_.flush().await.unwrap();

    let mut session = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_members(1, service.peer_endpoint(), &phase2_dir),
        phase1_frontier,
    )
    .await
    .expect("SessionClient::open");

    let phase2_frontier = session.next_checkpoint().await.expect("next_checkpoint");
    insta::assert_debug_snapshot!(
        "resume_from_checkpoint_phase2",
        snapshot_frontier(&phase2_frontier)
    );

    session.close().await.expect("close");
}

/// One publisher writes CONTINUE_TXN documents across two different
/// collections (bindings), then commits with ACK intents referencing
/// both journals. Verifies the multi-binding read/route/flush path.
async fn multi_partition_transaction(
    materialization_spec: &flow::MaterializationSpec,
    capture_spec: &flow::CaptureSpec,
    journal_client: &gazette::journal::Client,
    service: &shuffle::Service,
    log_dir: &std::path::Path,
) {
    let scenario_dir = log_dir.join("multi_partition_transaction");
    std::fs::create_dir_all(&scenario_dir).unwrap();

    let producer = uuid::Producer::from_bytes([0x01, 0x00, 0x00, 0x00, 0x00, 0x01]);
    let mut pub_ = make_publisher(capture_spec, journal_client, producer);

    // Write to binding 0 (testing/apples).
    pub_.enqueue(
        |uuid| {
            (
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "mp-apple",
                    "category": "alpha",
                    "value": 1,
                }),
            )
        },
        uuid::Flags::CONTINUE_TXN,
    )
    .await
    .unwrap();

    // Write to binding 1 (testing/bananas).
    pub_.enqueue(
        |uuid| {
            (
                1,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "mp-banana",
                    "category": "alpha",
                    "value": 2,
                }),
            )
        },
        uuid::Flags::CONTINUE_TXN,
    )
    .await
    .unwrap();

    // Commit with ACK intents spanning both journals.
    let (producer, commit_clock, journals) = pub_.commit_intents();
    let journal_acks =
        publisher::intents::build_transaction_intents(&[(producer, commit_clock, journals)]);
    pub_.write_intents(&journal_acks).await.unwrap();

    let mut session = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_members(1, service.peer_endpoint(), &scenario_dir),
        Default::default(),
    )
    .await
    .expect("SessionClient::open");

    let frontier = session.next_checkpoint().await.expect("next_checkpoint");
    insta::assert_debug_snapshot!("multi_partition_transaction", snapshot_frontier(&frontier));

    session.close().await.expect("close");
}

/// A single transaction spans three partitions of the same collection
/// (alpha, beta, gamma). The materialization only reads alpha and gamma
/// (beta is excluded by the partition selector). The ACK in each journal
/// hints at all three journals. Since no Slice reads beta, beta hints
/// must be dropped — otherwise they would block `next_checkpoint()` forever.
async fn partition_filtered_hints(
    materialization_spec: &flow::MaterializationSpec,
    capture_spec: &flow::CaptureSpec,
    journal_client: &gazette::journal::Client,
    service: &shuffle::Service,
    log_dir: &std::path::Path,
) {
    let scenario_dir = log_dir.join("partition_filtered_hints");
    std::fs::create_dir_all(&scenario_dir).unwrap();

    let producer = uuid::Producer::from_bytes([0x01, 0x00, 0x00, 0x00, 0x00, 0x01]);
    let mut pub_ = make_publisher(capture_spec, journal_client, producer);

    // Write to binding 0 (testing/apples), partition category=alpha (included).
    pub_.enqueue(
        |uuid| {
            (
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "pf-alpha",
                    "category": "alpha",
                    "value": 1,
                }),
            )
        },
        uuid::Flags::CONTINUE_TXN,
    )
    .await
    .unwrap();

    // Write to binding 0 (testing/apples), partition category=beta (excluded).
    pub_.enqueue(
        |uuid| {
            (
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "pf-beta",
                    "category": "beta",
                    "value": 2,
                }),
            )
        },
        uuid::Flags::CONTINUE_TXN,
    )
    .await
    .unwrap();

    // Write to binding 0 (testing/apples), partition category=gamma (included).
    pub_.enqueue(
        |uuid| {
            (
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "pf-gamma",
                    "category": "gamma",
                    "value": 3,
                }),
            )
        },
        uuid::Flags::CONTINUE_TXN,
    )
    .await
    .unwrap();

    // Commit with ACK intents spanning all three partition journals.
    let (producer, commit_clock, journals) = pub_.commit_intents();
    let journal_acks =
        publisher::intents::build_transaction_intents(&[(producer, commit_clock, journals)]);
    pub_.write_intents(&journal_acks).await.unwrap();

    let mut session = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_members(1, service.peer_endpoint(), &scenario_dir),
        Default::default(),
    )
    .await
    .expect("SessionClient::open");

    let frontier = session.next_checkpoint().await.expect("next_checkpoint");
    insta::assert_debug_snapshot!("partition_filtered_hints", snapshot_frontier(&frontier));

    session.close().await.expect("close");
}
