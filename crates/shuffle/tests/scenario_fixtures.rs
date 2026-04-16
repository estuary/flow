/// Integration tests for the shuffle crate, exercising the full
/// Session→Slice→Log pipeline against real published documents.
///
/// The fixture defines three collections (apples, bananas, cherries) shared
/// between a capture and a materialization. Tests build the shuffle task from
/// the MaterializationSpec (exercising `Binding::from_materialization_binding`)
/// and construct the publisher from the CaptureSpec (exercising
/// `Binding::from_capture_spec`).
///
/// Each scenario verifies both the `Frontier` checkpoint metadata AND the
/// actual documents read back by a `FrontierScan` from the on-disk log.
use proto_flow::flow;
use proto_gazette::uuid;
use shuffle::log::reader::{FrontierScan, Reader, Remainder};
use std::collections::VecDeque;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a Materialization task from a built MaterializationSpec.
/// Exercises `shuffle::Binding::from_materialization_binding()`.
fn build_task(spec: &flow::MaterializationSpec) -> shuffle::proto::Task {
    shuffle::proto::Task {
        task: Some(shuffle::proto::task::Task::Materialization(spec.clone())),
    }
}

/// Build an N-member topology with all members sharing a single endpoint.
fn build_members(
    count: u32,
    endpoint: &str,
    directory: &std::path::Path,
) -> Vec<shuffle::proto::Member> {
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

            shuffle::proto::Member {
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

/// A document read back from the on-disk log by a `FrontierScan`.
#[derive(Debug)]
#[allow(dead_code)]
struct ReadEntry {
    binding: u16,
    journal: String,
    doc: serde_json::Value,
}

/// Combined checkpoint: frontier metadata plus the documents actually read.
#[derive(Debug)]
#[allow(dead_code)]
struct Checkpoint<'a> {
    frontier: &'a shuffle::Frontier,
    read: Vec<ReadEntry>,
}

type MemberState = Vec<Option<(Reader, VecDeque<Remainder>)>>;

/// Drive `FrontierScan` for each member, collecting all committed entries.
/// Carries `(Reader, VecDeque<Remainder>)` state across calls.
fn collect_read_entries(
    frontier: &shuffle::Frontier,
    log_dir: &std::path::Path,
    member_state: &mut MemberState,
) -> Vec<ReadEntry> {
    let ser = doc::SerPolicy::noop();
    let mut entries = Vec::new();

    for (member_index, state_slot) in member_state.iter_mut().enumerate() {
        let (reader, remainders) = state_slot
            .take()
            .unwrap_or_else(|| (Reader::new(log_dir, member_index as u32), VecDeque::new()));

        let mut scan = FrontierScan::new(frontier.clone(), reader, remainders)
            .unwrap_or_else(|e| panic!("FrontierScan::new for member {member_index}: {e}"));

        while scan
            .advance_block()
            .unwrap_or_else(|e| panic!("advance_block for member {member_index}: {e}"))
        {
            for entry in scan.block_iter() {
                entries.push(ReadEntry {
                    binding: entry.meta.binding.to_native(),
                    journal: entry.journal.name.as_str().to_owned(),
                    doc: serde_json::to_value(ser.on(entry.doc.doc.get())).expect("serialize doc"),
                });
            }
        }

        let (_, reader, remainders) = scan.into_parts();
        *state_slot = Some((reader, remainders));
    }

    entries
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

    // Run test scenarios sequentially, resetting the data-plane between each.

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

    control_docs_are_metadata_only(
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
    data_plane.reset().await.expect("reset");

    clock_window_filtering(
        &materialization_spec,
        &capture_spec,
        &data_plane.journal_client,
        &service,
        log_dir.path(),
    )
    .await;
    data_plane.reset().await.expect("reset");

    rollback(
        &materialization_spec,
        &capture_spec,
        &data_plane.journal_client,
        &service,
        log_dir.path(),
    )
    .await;
    data_plane.reset().await.expect("reset");

    resume_with_backfill_metadata(
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
/// frontier shows the producer committed at the correct clock and
/// the reader yields all three documents.
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
    let mut member_state: MemberState = (0..1).map(|_| None).collect();
    let read = collect_read_entries(&frontier, &scenario_dir, &mut member_state);
    insta::assert_debug_snapshot!(
        "single_producer_outside_txn",
        Checkpoint {
            frontier: &frontier,
            read,
        }
    );

    session.close().await.expect("close");
}

/// Publish CONTINUE_TXN documents, then commit via ACK. Verify the frontier
/// reflects the committed clock matching the ACK and the reader yields
/// both documents.
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
    let mut member_state: MemberState = (0..1).map(|_| None).collect();
    let read = collect_read_entries(&frontier, &scenario_dir, &mut member_state);
    insta::assert_debug_snapshot!(
        "continue_then_ack",
        Checkpoint {
            frontier: &frontier,
            read,
        }
    );

    session.close().await.expect("close");
}

/// Publish control docs and regular documents in one transaction. Verify
/// control docs update checkpoint metadata but do not appear in shuffle logs.
///
/// Control documents carry `Flag_CONTROL` alone (which implies OUTSIDE_TXN)
/// — they are self-committing metadata events, not participants in the
/// transactional data span. Ordinary data documents are still published as
/// `CONTINUE_TXN` and committed by an `ACK_TXN` issued via `commit_intents()`.
async fn control_docs_are_metadata_only(
    materialization_spec: &flow::MaterializationSpec,
    capture_spec: &flow::CaptureSpec,
    journal_client: &gazette::journal::Client,
    service: &shuffle::Service,
    log_dir: &std::path::Path,
) {
    let scenario_dir = log_dir.join("control_docs_are_metadata_only");
    std::fs::create_dir_all(&scenario_dir).unwrap();

    let producer = uuid::Producer::from_bytes([0x01, 0x00, 0x00, 0x00, 0x00, 0x11]);
    let mut pub_ = make_publisher(capture_spec, journal_client, producer);
    let mut expected_clock = uuid::Clock::default();

    // BackfillBegin (OUTSIDE_TXN) is published before any ordinary data of the
    // isolated checkpoint, while no CONTINUE_TXN span is open.
    let begin_clock = expected_clock.tick();
    pub_.enqueue(
        |uuid| {
            (
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string(), "backfillBegin": true},
                    "id": "control",
                    "category": "alpha",
                    "value": 0,
                }),
            )
        },
        uuid::Flags::CONTROL,
    )
    .await
    .unwrap();

    _ = expected_clock.tick();
    pub_.enqueue(
        |uuid| {
            (
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "data",
                    "category": "alpha",
                    "value": 7,
                }),
            )
        },
        uuid::Flags::CONTINUE_TXN,
    )
    .await
    .unwrap();

    // Commit the CONTINUE_TXN span first — BackfillComplete cannot be written
    // while a CONTINUE_TXN span is still open, because OUTSIDE_TXN sequencing
    // disallows a non-zero `max_continue`.
    let (producer, commit_clock, journals) = pub_.commit_intents();
    assert_eq!(commit_clock, expected_clock.tick());
    let journal_acks =
        publisher::intents::build_transaction_intents(&[(producer, commit_clock, journals)]);
    pub_.write_intents(&journal_acks).await.unwrap();

    // Now that the ACK has committed the data span, BackfillComplete can be
    // published as OUTSIDE_TXN and is self-committing.
    let complete_clock = expected_clock.tick();
    pub_.enqueue(
        |uuid| {
            (
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string(), "backfillComplete": true},
                    "id": "control",
                    "category": "alpha",
                    "value": 0,
                }),
            )
        },
        uuid::Flags::CONTROL,
    )
    .await
    .unwrap();
    pub_.flush().await.unwrap();

    let mut session = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_members(1, service.peer_endpoint(), &scenario_dir),
        Default::default(),
    )
    .await
    .expect("SessionClient::open");

    // Because BackfillBegin is `OUTSIDE_TXN` and self-committing while the
    // intervening CONTINUE_TXN span is only committed by its ACK, the begin
    // and complete clocks may land in separate checkpoint flushes. Aggregate
    // across checkpoints until both clocks are visible.
    let suffix_0: &str = &materialization_spec.bindings[0].journal_read_suffix;
    let mut frontier = session.next_checkpoint().await.expect("next_checkpoint");
    let mut member_state: MemberState = (0..1).map(|_| None).collect();
    let mut read = collect_read_entries(&frontier, &scenario_dir, &mut member_state);
    while frontier.latest_backfill_begin.get(suffix_0) != Some(&begin_clock)
        || frontier.latest_backfill_complete.get(suffix_0) != Some(&complete_clock)
    {
        let next = session.next_checkpoint().await.expect("next_checkpoint");
        read.extend(collect_read_entries(
            &next,
            &scenario_dir,
            &mut member_state,
        ));
        frontier = frontier.reduce(next);
    }
    assert_eq!(
        frontier.latest_backfill_begin.get(suffix_0),
        Some(&begin_clock)
    );
    assert_eq!(
        frontier.latest_backfill_complete.get(suffix_0),
        Some(&complete_clock)
    );

    insta::assert_debug_snapshot!(
        "control_docs_are_metadata_only",
        Checkpoint {
            frontier: &frontier,
            read,
        }
    );

    session.close().await.expect("close");
}

/// Open a session with 3 members (split key space). Publish documents with
/// varied /id values so they hash to different key ranges. Verify the
/// frontier covers all journals and all documents are readable across members.
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
    let mut member_state: MemberState = (0..3).map(|_| None).collect();
    let read = collect_read_entries(&frontier, &scenario_dir, &mut member_state);
    insta::assert_debug_snapshot!(
        "multi_member_routing",
        Checkpoint {
            frontier: &frontier,
            read,
        }
    );

    session.close().await.expect("close");
}

/// Two publishers: P2 writes CONTINUE_TXN (uncommitted), then P1 writes
/// OUTSIDE_TXN (self-committing, triggers a flush cycle). The first
/// checkpoint shows P1 committed and P2 pending (positive offset). The
/// reader yields only P1's doc. Then P2 commits and the second checkpoint
/// advances P2; the reader yields P2's doc.
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

    // Serialize writes: P2's uncommitted CONTINUE_TXN first, then P1's
    // self-committing OUTSIDE_TXN. P1's commit triggers a flush cycle that
    // includes P2 as pending (uncommitted).
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
        uuid::Flags::CONTINUE_TXN,
    )
    .await
    .unwrap();
    pub2.flush().await.unwrap();

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

    let mut session = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_members(1, service.peer_endpoint(), &scenario_dir),
        Default::default(),
    )
    .await
    .expect("SessionClient::open");

    // First checkpoint: P1 committed, P2 pending. Reader yields only P1's doc.
    let frontier1 = session.next_checkpoint().await.expect("next_checkpoint 1");
    let mut member_state: MemberState = (0..1).map(|_| None).collect();
    let read1 = collect_read_entries(&frontier1, &scenario_dir, &mut member_state);
    insta::assert_debug_snapshot!(
        "multiple_producers_checkpoint1",
        Checkpoint {
            frontier: &frontier1,
            read: read1,
        }
    );

    // Now commit P2's transaction.
    let (producer, commit_clock, journals) = pub2.commit_intents();
    let journal_acks =
        publisher::intents::build_transaction_intents(&[(producer, commit_clock, journals)]);
    pub2.write_intents(&journal_acks).await.unwrap();

    // Second checkpoint: P2 now committed. Reader yields P2's doc.
    let frontier2 = session.next_checkpoint().await.expect("next_checkpoint 2");
    let read2 = collect_read_entries(&frontier2, &scenario_dir, &mut member_state);
    insta::assert_debug_snapshot!(
        "multiple_producers_checkpoint2",
        Checkpoint {
            frontier: &frontier2,
            read: read2,
        }
    );

    session.close().await.expect("close");

    // ---- Phase 3: resume from checkpoint 1, verify dedup and offset selection. ----
    //
    // Resuming from checkpoint 1 means P1 is committed (negative offset) and
    // P2 is pending (positive offset). `resolve_checkpoint` must pick P2's
    // begin offset (the minimum uncommitted) as the journal read start.
    //
    // Re-reading from that offset, the sequencer sees:
    //   P2's CONTINUE_TXN → ContinueBeginSpan (max_continue was zeroed on resume)
    //   P1's OUTSIDE_TXN  → OutsideDuplicate  (clock ≤ P1's last_commit from checkpoint 1)
    //   P2's ACK           → AckCommit
    //
    // The reader must yield only P2's doc; P1's is silently dropped as a duplicate.
    let resume_dir = log_dir.join("multiple_producers_resume");
    std::fs::create_dir_all(&resume_dir).unwrap();

    let mut resumed_session = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_members(1, service.peer_endpoint(), &resume_dir),
        frontier1.clone(),
    )
    .await
    .expect("SessionClient::open resumed");

    let frontier3 = resumed_session
        .next_checkpoint()
        .await
        .expect("next_checkpoint resumed");
    let mut resumed_member_state: MemberState = (0..1).map(|_| None).collect();
    let read3 = collect_read_entries(&frontier3, &resume_dir, &mut resumed_member_state);
    insta::assert_debug_snapshot!(
        "multiple_producers_resumed",
        Checkpoint {
            frontier: &frontier3,
            read: read3,
        }
    );

    resumed_session.close().await.expect("close resumed");
}

/// Phase 1: multi-journal CONTINUE_TXN + ACK, producing hinted_commit values.
/// Construct a modified resume frontier with unresolved hints (simulating a
/// crash before hint resolution). Phase 2: write more data, resume from the
/// modified frontier. First checkpoint advances to exactly the hinted boundary;
/// second checkpoint picks up the remaining progress.
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

    // ---- Phase 1: multi-journal transaction, capture a checkpoint. ----
    let mut pub_ = make_publisher(capture_spec, journal_client, producer);

    // Write to binding 0 (testing/apples).
    pub_.enqueue(
        |uuid| {
            (
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "r-apple",
                    "category": "alpha",
                    "value": 100,
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
                    "id": "r-banana",
                    "category": "alpha",
                    "value": 200,
                }),
            )
        },
        uuid::Flags::CONTINUE_TXN,
    )
    .await
    .unwrap();

    // Commit with ACK spanning both journals.
    let (producer_id, commit_clock, journals) = pub_.commit_intents();
    let journal_acks =
        publisher::intents::build_transaction_intents(&[(producer_id, commit_clock, journals)]);
    pub_.write_intents(&journal_acks).await.unwrap();

    let mut session = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_members(1, service.peer_endpoint(), &phase1_dir),
        Default::default(),
    )
    .await
    .expect("SessionClient::open phase 1");

    let phase1_frontier = session.next_checkpoint().await.expect("phase 1 checkpoint");
    let mut phase1_member_state: MemberState = (0..1).map(|_| None).collect();
    let phase1_read = collect_read_entries(&phase1_frontier, &phase1_dir, &mut phase1_member_state);
    insta::assert_debug_snapshot!(
        "resume_from_checkpoint_phase1",
        Checkpoint {
            frontier: &phase1_frontier,
            read: phase1_read,
        }
    );
    session.close().await.expect("close phase 1");

    // ---- Construct modified resume frontier with unresolved hints. ----
    // Simulate a crash with a recovery that requires us to read from the beginning,
    // through a hinted frontier.
    let mut resume_frontier = phase1_frontier.clone();
    for jf in &mut resume_frontier.journals {
        for pf in &mut jf.producers {
            (pf.hinted_commit, pf.last_commit) = (pf.last_commit, uuid::Clock::zero());
            pf.offset = 0;
        }
    }

    // ---- Phase 2: write more data, resume from modified frontier. ----
    pub_.enqueue(
        |uuid| {
            (
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "r-apple2",
                    "category": "alpha",
                    "value": 300,
                }),
            )
        },
        uuid::Flags::OUTSIDE_TXN,
    )
    .await
    .unwrap();
    pub_.flush().await.unwrap();

    let mut session = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_members(1, service.peer_endpoint(), &phase2_dir),
        resume_frontier,
    )
    .await
    .expect("SessionClient::open phase 2");

    // First checkpoint: recovery resolves the banana hint. New progress
    // (apple2) is held back by recovery_pending.
    let recovery_frontier = session
        .next_checkpoint()
        .await
        .expect("phase 2 recovery checkpoint");
    let mut phase2_member_state: MemberState = (0..1).map(|_| None).collect();
    let recovery_read =
        collect_read_entries(&recovery_frontier, &phase2_dir, &mut phase2_member_state);
    insta::assert_debug_snapshot!(
        "resume_from_checkpoint_recovery",
        Checkpoint {
            frontier: &recovery_frontier,
            read: recovery_read,
        }
    );

    // Second checkpoint: picks up remaining progress (apples original + new).
    let progress_frontier = session
        .next_checkpoint()
        .await
        .expect("phase 2 progress checkpoint");
    let progress_read =
        collect_read_entries(&progress_frontier, &phase2_dir, &mut phase2_member_state);
    insta::assert_debug_snapshot!(
        "resume_from_checkpoint_progress",
        Checkpoint {
            frontier: &progress_frontier,
            read: progress_read,
        }
    );

    session.close().await.expect("close phase 2");
}

/// One publisher writes CONTINUE_TXN documents across two different
/// collections (bindings), then commits with ACK intents referencing
/// both journals. Verifies the multi-binding read/route/flush path
/// and that both documents are readable.
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
    let mut member_state: MemberState = (0..1).map(|_| None).collect();
    let read = collect_read_entries(&frontier, &scenario_dir, &mut member_state);
    insta::assert_debug_snapshot!(
        "multi_partition_transaction",
        Checkpoint {
            frontier: &frontier,
            read,
        }
    );

    session.close().await.expect("close");
}

/// A single transaction spans three partitions of the same collection
/// (alpha, beta, gamma). The materialization only reads alpha and gamma
/// (beta is excluded by the partition selector). The ACK in each journal
/// hints at all three journals. Since no Slice reads beta, beta hints
/// must be dropped — otherwise they would block `next_checkpoint()` forever.
/// The reader must also exclude beta documents.
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
    let mut member_state: MemberState = (0..1).map(|_| None).collect();
    let read = collect_read_entries(&frontier, &scenario_dir, &mut member_state);
    insta::assert_debug_snapshot!(
        "partition_filtered_hints",
        Checkpoint {
            frontier: &frontier,
            read,
        }
    );

    session.close().await.expect("close");
}

/// Clock-window filtering: set `not_before` on binding 0 (apples) to a
/// future clock so all documents are filtered (the Publisher starts from
/// Clock::default(), so document clocks are near epoch). Binding 1 (bananas)
/// is unfiltered.
///
/// Verify that filtered documents are NOT yielded by the reader, but the
/// frontier still shows the producer committed on both bindings — flush and
/// progress propagate regardless of clock filtering.
async fn clock_window_filtering(
    materialization_spec: &flow::MaterializationSpec,
    capture_spec: &flow::CaptureSpec,
    journal_client: &gazette::journal::Client,
    service: &shuffle::Service,
    log_dir: &std::path::Path,
) {
    let scenario_dir = log_dir.join("clock_window_filtering");
    std::fs::create_dir_all(&scenario_dir).unwrap();

    // Set not_before on binding 0 to 1 second after unix epoch. Document
    // clocks start from Clock::default() (unix epoch) and tick forward by
    // nanoseconds, so all clocks are well below 1 second — meaning
    // `clock >= not_before` is false, suppressing append while preserving
    // flush/progress.
    let mut filtered_spec = materialization_spec.clone();
    filtered_spec.bindings[0].not_before = Some(pbjson_types::Timestamp {
        seconds: 1,
        nanos: 0,
    });

    let producer = uuid::Producer::from_bytes([0x01, 0x00, 0x00, 0x00, 0x00, 0x01]);
    let mut pub_ = make_publisher(capture_spec, journal_client, producer);

    // Write to binding 0 (testing/apples) — will be filtered by not_before.
    pub_.enqueue(
        |uuid| {
            (
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "naf-apple",
                    "category": "alpha",
                    "value": 1,
                }),
            )
        },
        uuid::Flags::CONTINUE_TXN,
    )
    .await
    .unwrap();

    // Write to binding 1 (testing/bananas) — not filtered.
    pub_.enqueue(
        |uuid| {
            (
                1,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "naf-banana",
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
        build_task(&filtered_spec),
        build_members(1, service.peer_endpoint(), &scenario_dir),
        Default::default(),
    )
    .await
    .expect("SessionClient::open");

    let frontier = session.next_checkpoint().await.expect("next_checkpoint");
    let mut member_state: MemberState = (0..1).map(|_| None).collect();
    let read = collect_read_entries(&frontier, &scenario_dir, &mut member_state);
    insta::assert_debug_snapshot!(
        "clock_window_filtering",
        Checkpoint {
            frontier: &frontier,
            read,
        }
    );

    session.close().await.expect("close");
}

/// Build ACK intent documents with an explicit clock, bypassing the normal
/// `commit_intents` flow which always ticks forward. This lets tests trigger
/// `AckCleanRollback` (clock == last_commit) and `AckDeepRollback` (clock < last_commit).
fn build_rollback_ack(
    producer: uuid::Producer,
    clock: uuid::Clock,
    journals: &[String],
) -> Vec<(String, Vec<serde_json::Value>)> {
    journals
        .iter()
        .map(|journal| {
            let ack_uuid = uuid::build(producer, clock, uuid::Flags::ACK_TXN);
            (
                journal.clone(),
                vec![serde_json::json!({
                    "_meta": { "uuid": ack_uuid },
                    "is_ack": true,
                })],
            )
        })
        .collect()
}

/// Exercises both clean and deep rollback with producer retirement.
///
/// Two producers share a journal: P1 is healthy throughout, P2 gets rolled
/// back and retired (never writes at a later clock after rollback).
///
/// Phase 1: Both P1 and P2 commit transactions, establishing baselines.
///   P1 commits OUTSIDE_TXN docs. P2 commits CONTINUE_TXN + ACK, setting
///   P2's `last_commit = C_p2`.
///
/// Phase 2: P2 writes more CONTINUE_TXN docs, then clean-rolls-back
///   (ACK at C_p2, triggering `AckCleanRollback`). P2 is now retired.
///   Checkpoint verifies P2's pending docs are suppressed.
///
/// Phase 3: P1 commits new docs. Checkpoint verifies only P1's new docs
///   appear — P2's rolled-back docs stay permanently uncommitted because
///   P2 is retired.
///
/// Phase 4: Deep rollback for P1 — P1 writes fresh CONTINUE_TXN docs
///   and then receives an ACK at `Clock::default()` (< P1's last_commit).
///   Because there are pending CONTINUEs, this triggers `AckDeepRollback`.
///   P1's `last_commit` regresses to `Clock::default()`.
async fn rollback(
    materialization_spec: &flow::MaterializationSpec,
    capture_spec: &flow::CaptureSpec,
    journal_client: &gazette::journal::Client,
    service: &shuffle::Service,
    log_dir: &std::path::Path,
) {
    let scenario_dir = log_dir.join("rollback");
    std::fs::create_dir_all(&scenario_dir).unwrap();

    let p1 = uuid::Producer::from_bytes([0x01, 0x00, 0x00, 0x00, 0x00, 0x01]);
    let p2 = uuid::Producer::from_bytes([0x03, 0x00, 0x00, 0x00, 0x00, 0x02]);

    let mut pub1 = make_publisher(capture_spec, journal_client, p1);
    let mut pub2 = make_publisher(capture_spec, journal_client, p2);

    // ---- Phase 1: both producers commit, establishing baselines. ----

    // P2 commits a transaction via CONTINUE_TXN + ACK, establishing C_p2.
    pub2.enqueue(
        |uuid| {
            (
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "rb-p2-committed",
                    "category": "alpha",
                    "value": 10,
                }),
            )
        },
        uuid::Flags::CONTINUE_TXN,
    )
    .await
    .unwrap();

    let (p2_id, commit_clock_p2, p2_journals) = pub2.commit_intents();
    let p2_acks = publisher::intents::build_transaction_intents(&[(
        p2_id,
        commit_clock_p2,
        p2_journals.clone(),
    )]);
    pub2.write_intents(&p2_acks).await.unwrap();

    // P1 commits OUTSIDE_TXN docs.
    for id in ["rb-p1-a1", "rb-p1-a2"] {
        pub1.enqueue(
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
    pub1.flush().await.unwrap();

    let mut session = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_members(1, service.peer_endpoint(), &scenario_dir),
        Default::default(),
    )
    .await
    .expect("SessionClient::open");

    // Both producers committed. Await to read Reader yields all three docs.
    let mut frontier1 = shuffle::Frontier::default();
    loop {
        let delta = session.next_checkpoint().await.expect("phase 1 checkpoint");
        frontier1 = frontier1.reduce(delta);

        if frontier1.journals[0].bytes_behind_delta == 0 {
            break;
        }
    }

    let mut member_state: MemberState = (0..1).map(|_| None).collect();
    let read1 = collect_read_entries(&frontier1, &scenario_dir, &mut member_state);
    insta::assert_debug_snapshot!(
        "rollback_phase1_committed",
        Checkpoint {
            frontier: &frontier1,
            read: read1,
        }
    );

    // ---- Phase 2: P2 writes CONTINUE_TXN, then clean rollback. ----
    // ACK at commit_clock_p2 triggers AckCleanRollback: the sequencer sees
    // clock == last_commit AND max_continue > 0. P2 is retired after this.

    for id in ["rb-p2-rolled-back-1", "rb-p2-rolled-back-2"] {
        pub2.enqueue(
            |uuid| {
                (
                    0,
                    serde_json::json!({
                        "_meta": {"uuid": uuid.to_string()},
                        "id": id,
                        "category": "alpha",
                        "value": 999,
                    }),
                )
            },
            uuid::Flags::CONTINUE_TXN,
        )
        .await
        .unwrap();
    }
    pub2.flush().await.unwrap();

    // Roll back at P2's prior commit clock.
    let rollback_acks = build_rollback_ack(p2_id, commit_clock_p2, &p2_journals);
    pub2.write_intents(&rollback_acks).await.unwrap();

    // P2's pending docs must NOT appear. Frontier advances (flush propagates).
    let frontier2 = session.next_checkpoint().await.expect("phase 2 checkpoint");
    let read2 = collect_read_entries(&frontier2, &scenario_dir, &mut member_state);
    insta::assert_debug_snapshot!(
        "rollback_phase2_clean_rollback",
        Checkpoint {
            frontier: &frontier2,
            read: read2,
        }
    );

    // ---- Phase 3: P1 commits new work; P2 stays retired. ----

    pub1.enqueue(
        |uuid| {
            (
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "p1-new-msg",
                    "category": "alpha",
                    "value": 2,
                }),
            )
        },
        uuid::Flags::OUTSIDE_TXN,
    )
    .await
    .unwrap();
    pub1.flush().await.unwrap();

    // Only P1's new doc appears. P2's rolled-back docs remain permanently
    // uncommitted — P2 is retired and no future ACK will advance its
    // last_commit past their clocks.
    let frontier3 = session.next_checkpoint().await.expect("phase 3 checkpoint");
    let read3 = collect_read_entries(&frontier3, &scenario_dir, &mut member_state);
    insta::assert_debug_snapshot!(
        "rollback_phase3_p1_continues",
        Checkpoint {
            frontier: &frontier3,
            read: read3,
        }
    );

    // ---- Phase 4: P1 deep rollback (disaster recovery replay). ----
    // P1 writes fresh CONTINUE_TXN docs (setting max_continue > 0), then
    // receives an ACK at Clock::default() (< P1's last_commit). Because
    // there are pending CONTINUEs, this triggers AckDeepRollback rather
    // than AckDuplicate. P1's last_commit regresses.

    for id in ["rb-p1-deep-1", "rb-p1-deep-2"] {
        pub1.enqueue(
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
            uuid::Flags::CONTINUE_TXN,
        )
        .await
        .unwrap();
    }
    pub1.flush().await.unwrap();

    // Deep rollback ACK for P1 at Clock::default() (< P1's last_commit).
    let deep_rollback_acks = build_rollback_ack(p1, uuid::Clock::default(), &p2_journals);
    pub1.write_intents(&deep_rollback_acks).await.unwrap();

    // P1's pending CONTINUE docs are rolled back. P1's last_commit regresses
    // to Clock::default().
    let frontier4 = session.next_checkpoint().await.expect("phase 4 checkpoint");
    let read4 = collect_read_entries(&frontier4, &scenario_dir, &mut member_state);
    insta::assert_debug_snapshot!(
        "rollback_phase4_deep_rollback",
        Checkpoint {
            frontier: &frontier4,
            read: read4,
        }
    );

    session.close().await.expect("close");
}

/// Verify that `latest_backfill_begin` and `latest_backfill_complete` survive
/// the Drain→wire→Frontier::new() resume round-trip through the Session
/// handler.
///
/// Phase 1: Publish control docs + data in a committed transaction. Capture
///   a checkpoint whose frontier carries non-empty backfill maps.
/// Phase 2: Reopen a new session using that frontier as the resume
///   checkpoint. Write additional data and poll a checkpoint. The resumed
///   session must have accepted the backfill metadata from the resume
///   frontier — we verify indirectly because the session would error if
///   the terminal chunk were malformed, and because new progress comes back
///   correctly layered on top of the resumed state.
async fn resume_with_backfill_metadata(
    materialization_spec: &flow::MaterializationSpec,
    capture_spec: &flow::CaptureSpec,
    journal_client: &gazette::journal::Client,
    service: &shuffle::Service,
    log_dir: &std::path::Path,
) {
    let phase1_dir = log_dir.join("resume_backfill_p1");
    let phase2_dir = log_dir.join("resume_backfill_p2");
    std::fs::create_dir_all(&phase1_dir).unwrap();
    std::fs::create_dir_all(&phase2_dir).unwrap();

    let producer = uuid::Producer::from_bytes([0x01, 0x00, 0x00, 0x00, 0x00, 0x21]);
    let mut pub_ = make_publisher(capture_spec, journal_client, producer);
    let mut expected_clock = uuid::Clock::default();

    // ---- Phase 1: Commit control docs + data, capture a checkpoint. ----

    // BackfillBegin (OUTSIDE_TXN) is published before the ordinary data span.
    let begin_clock = expected_clock.tick();
    pub_.enqueue(
        |uuid| {
            (
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string(), "backfillBegin": true},
                    "id": "rb-ctrl",
                    "category": "alpha",
                    "value": 0,
                }),
            )
        },
        uuid::Flags::CONTROL,
    )
    .await
    .unwrap();

    _ = expected_clock.tick();
    pub_.enqueue(
        |uuid| {
            (
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "rb-data",
                    "category": "alpha",
                    "value": 42,
                }),
            )
        },
        uuid::Flags::CONTINUE_TXN,
    )
    .await
    .unwrap();

    // Commit the CONTINUE_TXN span before publishing BackfillComplete.
    let (producer_id, commit_clock, journals) = pub_.commit_intents();
    assert_eq!(commit_clock, expected_clock.tick());
    let journal_acks =
        publisher::intents::build_transaction_intents(&[(producer_id, commit_clock, journals)]);
    pub_.write_intents(&journal_acks).await.unwrap();

    // BackfillComplete as OUTSIDE_TXN is self-committing and published after
    // the data-span's ACK.
    let complete_clock = expected_clock.tick();
    pub_.enqueue(
        |uuid| {
            (
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string(), "backfillComplete": true},
                    "id": "rb-ctrl",
                    "category": "alpha",
                    "value": 0,
                }),
            )
        },
        uuid::Flags::CONTROL,
    )
    .await
    .unwrap();
    pub_.flush().await.unwrap();

    let mut session = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_members(1, service.peer_endpoint(), &phase1_dir),
        Default::default(),
    )
    .await
    .expect("SessionClient::open phase 1");

    // Aggregate checkpoint deltas until both the backfill begin clock (from
    // the OUTSIDE_TXN BackfillBegin commit) and the backfill complete clock
    // (from the OUTSIDE_TXN BackfillComplete commit after the span's ACK) are
    // visible. The two control docs commit in separate flush cycles.
    let suffix_0: &str = &materialization_spec.bindings[0].journal_read_suffix;
    let mut phase1_frontier = session.next_checkpoint().await.expect("phase 1 checkpoint");
    while phase1_frontier.latest_backfill_begin.get(suffix_0) != Some(&begin_clock)
        || phase1_frontier.latest_backfill_complete.get(suffix_0) != Some(&complete_clock)
    {
        let next = session
            .next_checkpoint()
            .await
            .expect("phase 1 next checkpoint");
        phase1_frontier = phase1_frontier.reduce(next);
    }
    assert_eq!(
        phase1_frontier.latest_backfill_begin.get(suffix_0),
        Some(&begin_clock),
        "phase 1 frontier should carry backfill begin"
    );
    assert_eq!(
        phase1_frontier.latest_backfill_complete.get(suffix_0),
        Some(&complete_clock),
        "phase 1 frontier should carry backfill complete"
    );
    session.close().await.expect("close phase 1");

    // ---- Phase 2: Resume from phase1_frontier, write more data. ----

    pub_.enqueue(
        |uuid| {
            (
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "rb-new",
                    "category": "alpha",
                    "value": 99,
                }),
            )
        },
        uuid::Flags::OUTSIDE_TXN,
    )
    .await
    .unwrap();
    pub_.flush().await.unwrap();

    let mut session = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_members(1, service.peer_endpoint(), &phase2_dir),
        phase1_frontier.clone(),
    )
    .await
    .expect("SessionClient::open phase 2 (resume with backfill metadata)");

    let phase2_frontier = session.next_checkpoint().await.expect("phase 2 checkpoint");

    let mut phase2_member_state: MemberState = (0..1).map(|_| None).collect();
    let phase2_read = collect_read_entries(&phase2_frontier, &phase2_dir, &mut phase2_member_state);
    insta::assert_debug_snapshot!(
        "resume_with_backfill_metadata",
        Checkpoint {
            frontier: &phase2_frontier,
            read: phase2_read,
        }
    );

    session.close().await.expect("close phase 2");
}

// NOTE: The former `rollback_control_docs` scenario tested that CONTINUE_TXN
// control docs were correctly discarded on rollback via staged-then-committed
// sequencing. Under the new design, control docs carry `Flag_CONTROL` alone
// (implying OUTSIDE_TXN) and are immediately committed, so transactional
// rollback does not apply to them; interleaving `OUTSIDE_TXN` with a still-
// open CONTINUE_TXN span is a protocol violation
// (`OutsideWithPrecedingContinue`). Duplicate handling of
// immediately-committed control docs is covered by the unit test
// `test_sequence_outside_txn_control_docs_commit_immediately` in
// `crates/shuffle/src/slice/state.rs`.
