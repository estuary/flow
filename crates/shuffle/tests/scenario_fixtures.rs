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

/// Loop `next_checkpoint` until a fully-resolved Frontier is obtained.
async fn next_resolved_checkpoint(
    session: &mut shuffle::SessionClient,
    label: &str,
) -> shuffle::Frontier {
    let mut frontier = session
        .next_checkpoint()
        .await
        .unwrap_or_else(|e| panic!("next_checkpoint ({label}): {e}"));

    while frontier.unresolved_hints != 0 {
        // We don't reduce because the resolved frontier is a full restatement
        // of any partial progress (unresolved_hints != 0).
        frontier = session
            .next_checkpoint()
            .await
            .unwrap_or_else(|e| panic!("next_checkpoint follow-up ({label}): {e}"));
    }
    frontier
}

/// Build a Materialization task from a built MaterializationSpec.
/// Exercises `shuffle::Binding::from_materialization_binding()`.
fn build_task(spec: &flow::MaterializationSpec) -> shuffle::proto::Task {
    shuffle::proto::Task {
        task: Some(shuffle::proto::task::Task::Materialization(spec.clone())),
    }
}

/// Build an N-shard topology with all shards sharing a single endpoint.
fn build_shards(
    count: u32,
    endpoint: &str,
    directory: &std::path::Path,
) -> Vec<shuffle::proto::Shard> {
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

            shuffle::proto::Shard {
                id: format!("scenario-fixtures/shard-{i:03}"),
                range: Some(flow::RangeSpec {
                    key_begin,
                    key_end,
                    r_clock_begin: 0,
                    r_clock_end: u32::MAX,
                }),
                endpoint: endpoint.to_string(),
                directory: directory.to_str().unwrap().to_string(),
                ..Default::default()
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
    let factory: gazette::journal::ClientFactory = Arc::new({
        let journal_client = journal_client.clone();
        move |_authz_sub, _authz_obj| journal_client.clone()
    });

    let bindings = publisher::Binding::from_capture_spec(spec)
        .expect("should build bindings from capture spec");

    publisher::Publisher::new(
        String::new(), // Empty AuthZ subject.
        bindings,
        factory,
        producer,
        // Deterministic base clock for reproducible snapshots. Must be >=
        // UNIX_EPOCH so document clocks clear a binding's default `not_before`
        // floor (also UNIX_EPOCH) and are appended to shard logs.
        uuid::Clock::UNIX_EPOCH,
    )
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

type ShardState = Vec<Option<(Reader, VecDeque<Remainder>)>>;

/// Drive `FrontierScan` for each shard, collecting all committed entries.
/// Carries `(Reader, VecDeque<Remainder>)` state across calls.
fn collect_read_entries(
    frontier: &shuffle::Frontier,
    log_dir: &std::path::Path,
    shard_state: &mut ShardState,
) -> Vec<ReadEntry> {
    let ser = doc::SerPolicy::noop();
    let mut entries = Vec::new();

    for (shard_index, state_slot) in shard_state.iter_mut().enumerate() {
        let (reader, remainders) = state_slot
            .take()
            .unwrap_or_else(|| (Reader::new(log_dir, shard_index as u32), VecDeque::new()));

        let mut scan = FrontierScan::new(frontier.clone(), reader, remainders)
            .unwrap_or_else(|e| panic!("FrontierScan::new for shard {shard_index}: {e}"));

        while scan
            .advance_block()
            .unwrap_or_else(|e| panic!("advance_block for shard {shard_index}: {e}"))
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

    // Start a shuffle gRPC server so that multi-shard Slice/Log RPCs
    // can dial back to us.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind shuffle server");
    let endpoint = format!("http://{}", listener.local_addr().unwrap());

    let factory: gazette::journal::ClientFactory = Arc::new({
        let journal_client = data_plane.journal_client.clone();
        move |_authz_sub, _authz_obj| journal_client.clone()
    });
    let service = shuffle::Service::new(
        endpoint.clone(),
        factory,
        10 * 1024 * 1024 * 1024,
        service_kit::Registry::new(),
        None, // Tests run the shuffle fan-out unauthenticated.
    );

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

    multi_shard_routing(
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

    gapped_replay(
        &materialization_spec,
        &capture_spec,
        &data_plane.journal_client,
        &service,
        log_dir.path(),
    )
    .await;
    data_plane.reset().await.expect("reset");

    gapped_continue_trigger(
        &materialization_spec,
        &capture_spec,
        &data_plane.journal_client,
        &service,
        log_dir.path(),
    )
    .await;
    data_plane.reset().await.expect("reset");

    gapped_replay_blocks_other_journal(
        &materialization_spec,
        &capture_spec,
        &data_plane.journal_client,
        &service,
        log_dir.path(),
    )
    .await;
    data_plane.reset().await.expect("reset");

    hint_elevated_offset_flip(
        &materialization_spec,
        &capture_spec,
        &data_plane.journal_client,
        &service,
        log_dir.path(),
    )
    .await;
    data_plane.reset().await.expect("reset");

    gapped_outside_violation(
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
/// partition. Open a 1-shard session, poll a checkpoint, and verify the
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
                Ok((
                    0,
                    serde_json::json!({
                        "_meta": {"uuid": uuid.to_string()},
                        "id": id,
                        "category": "alpha",
                        "value": 1,
                    }),
                ))
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
        build_shards(1, service.peer_endpoint(), &scenario_dir),
        Default::default(),
    )
    .await
    .expect("SessionClient::open");

    let frontier = next_resolved_checkpoint(&mut session, "next_checkpoint").await;
    let mut shard_state: ShardState = (0..1).map(|_| None).collect();
    let read = collect_read_entries(&frontier, &scenario_dir, &mut shard_state);
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
                Ok((
                    0,
                    serde_json::json!({
                        "_meta": {"uuid": uuid.to_string()},
                        "id": id,
                        "category": "alpha",
                        "value": 2,
                    }),
                ))
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
    pub_.write_intents(journal_acks).await.unwrap();

    let mut session = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_shards(1, service.peer_endpoint(), &scenario_dir),
        Default::default(),
    )
    .await
    .expect("SessionClient::open");

    let frontier = next_resolved_checkpoint(&mut session, "next_checkpoint").await;
    let mut shard_state: ShardState = (0..1).map(|_| None).collect();
    let read = collect_read_entries(&frontier, &scenario_dir, &mut shard_state);
    insta::assert_debug_snapshot!(
        "continue_then_ack",
        Checkpoint {
            frontier: &frontier,
            read,
        }
    );

    session.close().await.expect("close");
}

/// Open a session with 3 shards (split key space). Publish documents with
/// varied /id values so they hash to different key ranges. Verify the
/// frontier covers all journals and all documents are readable across shards.
async fn multi_shard_routing(
    materialization_spec: &flow::MaterializationSpec,
    capture_spec: &flow::CaptureSpec,
    journal_client: &gazette::journal::Client,
    service: &shuffle::Service,
    log_dir: &std::path::Path,
) {
    let scenario_dir = log_dir.join("multi_shard_routing");
    std::fs::create_dir_all(&scenario_dir).unwrap();

    let producer = uuid::Producer::from_bytes([0x01, 0x00, 0x00, 0x00, 0x00, 0x01]);
    let mut pub_ = make_publisher(capture_spec, journal_client, producer);

    // Use varied IDs that will produce different key hashes, exercising
    // routing across the 3-shard topology.
    for id in ["m-aaa", "m-bbb", "m-ccc", "m-ddd", "m-eee"] {
        pub_.enqueue(
            |uuid| {
                Ok((
                    0,
                    serde_json::json!({
                        "_meta": {"uuid": uuid.to_string()},
                        "id": id,
                        "category": "alpha",
                        "value": 3,
                    }),
                ))
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
        build_shards(3, service.peer_endpoint(), &scenario_dir),
        Default::default(),
    )
    .await
    .expect("SessionClient::open");

    let frontier = next_resolved_checkpoint(&mut session, "next_checkpoint").await;
    let mut shard_state: ShardState = (0..3).map(|_| None).collect();
    let read = collect_read_entries(&frontier, &scenario_dir, &mut shard_state);
    insta::assert_debug_snapshot!(
        "multi_shard_routing",
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
            Ok((
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "p2-doc",
                    "category": "alpha",
                    "value": 20,
                }),
            ))
        },
        uuid::Flags::CONTINUE_TXN,
    )
    .await
    .unwrap();
    pub2.flush().await.unwrap();

    pub1.enqueue(
        |uuid| {
            Ok((
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "p1-doc",
                    "category": "alpha",
                    "value": 10,
                }),
            ))
        },
        uuid::Flags::OUTSIDE_TXN,
    )
    .await
    .unwrap();
    pub1.flush().await.unwrap();

    let mut session = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_shards(1, service.peer_endpoint(), &scenario_dir),
        Default::default(),
    )
    .await
    .expect("SessionClient::open");

    // First checkpoint: P1 committed, P2 pending. Reader yields only P1's doc.
    let frontier1 = next_resolved_checkpoint(&mut session, "next_checkpoint 1").await;
    let mut shard_state: ShardState = (0..1).map(|_| None).collect();
    let read1 = collect_read_entries(&frontier1, &scenario_dir, &mut shard_state);
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
    pub2.write_intents(journal_acks).await.unwrap();

    // Second checkpoint: P2 now committed. Reader yields P2's doc.
    let frontier2 = next_resolved_checkpoint(&mut session, "next_checkpoint 2").await;
    let read2 = collect_read_entries(&frontier2, &scenario_dir, &mut shard_state);
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
    // P2 is pending (positive offset). `resolve_checkpoint` starts the journal
    // read at M (P1's committed end) and gaps P2, whose span begins before M.
    //
    // The main read then sees only P2's ACK (P2's single CONTINUE is below M,
    // so the ACK is its first newer document): the read parks at the ACK and
    // replays P2's span [F, ack.begin), skipping P1's already-committed
    // OUTSIDE and sequencing P2's CONTINUE. On completion the parked ACK is
    // re-presented to the normal path, which commits it (AckCommit).
    //
    // The reader must yield only P2's doc, exactly once; P1's is never re-read.
    let resume_dir = log_dir.join("multiple_producers_resume");
    std::fs::create_dir_all(&resume_dir).unwrap();

    let mut resumed_session = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_shards(1, service.peer_endpoint(), &resume_dir),
        frontier1.clone(),
    )
    .await
    .expect("SessionClient::open resumed");

    let frontier3 = next_resolved_checkpoint(&mut resumed_session, "next_checkpoint resumed").await;
    let mut resumed_shard_state: ShardState = (0..1).map(|_| None).collect();
    let read3 = collect_read_entries(&frontier3, &resume_dir, &mut resumed_shard_state);
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
            Ok((
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "r-apple",
                    "category": "alpha",
                    "value": 100,
                }),
            ))
        },
        uuid::Flags::CONTINUE_TXN,
    )
    .await
    .unwrap();

    // Write to binding 1 (testing/bananas).
    pub_.enqueue(
        |uuid| {
            Ok((
                1,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "r-banana",
                    "category": "alpha",
                    "value": 200,
                }),
            ))
        },
        uuid::Flags::CONTINUE_TXN,
    )
    .await
    .unwrap();

    // Commit with ACK spanning both journals.
    let (producer_id, commit_clock, journals) = pub_.commit_intents();
    let journal_acks =
        publisher::intents::build_transaction_intents(&[(producer_id, commit_clock, journals)]);
    pub_.write_intents(journal_acks).await.unwrap();

    let mut session = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_shards(1, service.peer_endpoint(), &phase1_dir),
        Default::default(),
    )
    .await
    .expect("SessionClient::open phase 1");

    let phase1_frontier = next_resolved_checkpoint(&mut session, "phase 1 checkpoint").await;
    let mut phase1_shard_state: ShardState = (0..1).map(|_| None).collect();
    let mut phase1_read =
        collect_read_entries(&phase1_frontier, &phase1_dir, &mut phase1_shard_state);

    // Journal creation / discovery / read races may interleave, so we assert content only.
    phase1_read.sort_by_key(|e| e.binding);
    assert_eq!(
        phase1_read
            .iter()
            .map(|e| (e.binding, e.doc["id"].as_str().unwrap()))
            .collect::<Vec<_>>(),
        vec![(0, "r-apple"), (1, "r-banana")],
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
            Ok((
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "r-apple2",
                    "category": "alpha",
                    "value": 300,
                }),
            ))
        },
        uuid::Flags::OUTSIDE_TXN,
    )
    .await
    .unwrap();
    pub_.flush().await.unwrap();

    let mut session = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_shards(1, service.peer_endpoint(), &phase2_dir),
        resume_frontier,
    )
    .await
    .expect("SessionClient::open phase 2");

    // First checkpoint: recovery resolves the banana hint. New progress
    // (apple2) is held back by recovery_pending.
    let recovery_frontier =
        next_resolved_checkpoint(&mut session, "phase 2 recovery checkpoint").await;
    let mut phase2_shard_state: ShardState = (0..1).map(|_| None).collect();
    let recovery_read =
        collect_read_entries(&recovery_frontier, &phase2_dir, &mut phase2_shard_state);
    insta::assert_debug_snapshot!(
        "resume_from_checkpoint_recovery",
        Checkpoint {
            frontier: &recovery_frontier,
            read: recovery_read,
        }
    );

    // Second checkpoint: picks up remaining progress (apples original + new).
    let progress_frontier =
        next_resolved_checkpoint(&mut session, "phase 2 progress checkpoint").await;
    let progress_read =
        collect_read_entries(&progress_frontier, &phase2_dir, &mut phase2_shard_state);
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
            Ok((
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "mp-apple",
                    "category": "alpha",
                    "value": 1,
                }),
            ))
        },
        uuid::Flags::CONTINUE_TXN,
    )
    .await
    .unwrap();

    // Write to binding 1 (testing/bananas).
    pub_.enqueue(
        |uuid| {
            Ok((
                1,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "mp-banana",
                    "category": "alpha",
                    "value": 2,
                }),
            ))
        },
        uuid::Flags::CONTINUE_TXN,
    )
    .await
    .unwrap();

    // Commit with ACK intents spanning both journals.
    let (producer, commit_clock, journals) = pub_.commit_intents();
    let journal_acks =
        publisher::intents::build_transaction_intents(&[(producer, commit_clock, journals)]);
    pub_.write_intents(journal_acks).await.unwrap();

    let mut session = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_shards(1, service.peer_endpoint(), &scenario_dir),
        Default::default(),
    )
    .await
    .expect("SessionClient::open");

    let frontier = next_resolved_checkpoint(&mut session, "next_checkpoint").await;
    let mut shard_state: ShardState = (0..1).map(|_| None).collect();
    let read = collect_read_entries(&frontier, &scenario_dir, &mut shard_state);
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
            Ok((
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "pf-alpha",
                    "category": "alpha",
                    "value": 1,
                }),
            ))
        },
        uuid::Flags::CONTINUE_TXN,
    )
    .await
    .unwrap();

    // Write to binding 0 (testing/apples), partition category=beta (excluded).
    pub_.enqueue(
        |uuid| {
            Ok((
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "pf-beta",
                    "category": "beta",
                    "value": 2,
                }),
            ))
        },
        uuid::Flags::CONTINUE_TXN,
    )
    .await
    .unwrap();

    // Write to binding 0 (testing/apples), partition category=gamma (included).
    pub_.enqueue(
        |uuid| {
            Ok((
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "pf-gamma",
                    "category": "gamma",
                    "value": 3,
                }),
            ))
        },
        uuid::Flags::CONTINUE_TXN,
    )
    .await
    .unwrap();

    // Commit with ACK intents spanning all three partition journals.
    let (producer, commit_clock, journals) = pub_.commit_intents();
    let journal_acks =
        publisher::intents::build_transaction_intents(&[(producer, commit_clock, journals)]);
    pub_.write_intents(journal_acks).await.unwrap();

    let mut session = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_shards(1, service.peer_endpoint(), &scenario_dir),
        Default::default(),
    )
    .await
    .expect("SessionClient::open");

    let frontier = next_resolved_checkpoint(&mut session, "next_checkpoint").await;
    let mut shard_state: ShardState = (0..1).map(|_| None).collect();
    let read = collect_read_entries(&frontier, &scenario_dir, &mut shard_state);
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
/// Clock::UNIX_EPOCH, so document clocks are near epoch). Binding 1 (bananas)
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
    // clocks start from Clock::UNIX_EPOCH and tick forward by
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
            Ok((
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "naf-apple",
                    "category": "alpha",
                    "value": 1,
                }),
            ))
        },
        uuid::Flags::CONTINUE_TXN,
    )
    .await
    .unwrap();

    // Write to binding 1 (testing/bananas) — not filtered.
    pub_.enqueue(
        |uuid| {
            Ok((
                1,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "naf-banana",
                    "category": "alpha",
                    "value": 2,
                }),
            ))
        },
        uuid::Flags::CONTINUE_TXN,
    )
    .await
    .unwrap();

    // Commit with ACK intents spanning both journals.
    let (producer, commit_clock, journals) = pub_.commit_intents();
    let journal_acks =
        publisher::intents::build_transaction_intents(&[(producer, commit_clock, journals)]);
    pub_.write_intents(journal_acks).await.unwrap();

    let mut session = shuffle::SessionClient::open(
        service,
        build_task(&filtered_spec),
        build_shards(1, service.peer_endpoint(), &scenario_dir),
        Default::default(),
    )
    .await
    .expect("SessionClient::open");

    let frontier = next_resolved_checkpoint(&mut session, "next_checkpoint").await;
    let mut shard_state: ShardState = (0..1).map(|_| None).collect();
    let read = collect_read_entries(&frontier, &scenario_dir, &mut shard_state);
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
) -> Vec<(String, bytes::Bytes)> {
    journals
        .iter()
        .map(|journal| {
            let ack_uuid = uuid::build(producer, clock, uuid::Flags::ACK_TXN);
            let doc = serde_json::json!({
                "_meta": { "uuid": ack_uuid },
                "is_ack": true,
            });
            let mut buf = serde_json::to_vec(&doc).unwrap();
            buf.push(b'\n');
            (journal.clone(), bytes::Bytes::from(buf))
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
            Ok((
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "rb-p2-committed",
                    "category": "alpha",
                    "value": 10,
                }),
            ))
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
    pub2.write_intents(p2_acks).await.unwrap();

    // P1 commits OUTSIDE_TXN docs.
    for id in ["rb-p1-a1", "rb-p1-a2"] {
        pub1.enqueue(
            |uuid| {
                Ok((
                    0,
                    serde_json::json!({
                        "_meta": {"uuid": uuid.to_string()},
                        "id": id,
                        "category": "alpha",
                        "value": 1,
                    }),
                ))
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
        build_shards(1, service.peer_endpoint(), &scenario_dir),
        Default::default(),
    )
    .await
    .expect("SessionClient::open");

    // Both producers committed. Await to read Reader yields all three docs.
    let mut frontier1 = shuffle::Frontier::default();
    loop {
        let delta = next_resolved_checkpoint(&mut session, "phase 1 checkpoint").await;
        frontier1 = frontier1.reduce(delta);

        if frontier1.journals[0].bytes_behind_delta == 0 {
            break;
        }
    }

    let mut shard_state: ShardState = (0..1).map(|_| None).collect();
    let read1 = collect_read_entries(&frontier1, &scenario_dir, &mut shard_state);
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
                Ok((
                    0,
                    serde_json::json!({
                        "_meta": {"uuid": uuid.to_string()},
                        "id": id,
                        "category": "alpha",
                        "value": 999,
                    }),
                ))
            },
            uuid::Flags::CONTINUE_TXN,
        )
        .await
        .unwrap();
    }
    pub2.flush().await.unwrap();

    // Roll back at P2's prior commit clock.
    let rollback_acks = build_rollback_ack(p2_id, commit_clock_p2, &p2_journals);
    pub2.write_intents(rollback_acks).await.unwrap();

    // P2's pending docs must NOT appear. Frontier advances (flush propagates).
    let frontier2 = next_resolved_checkpoint(&mut session, "phase 2 checkpoint").await;
    let read2 = collect_read_entries(&frontier2, &scenario_dir, &mut shard_state);
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
            Ok((
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "p1-new-msg",
                    "category": "alpha",
                    "value": 2,
                }),
            ))
        },
        uuid::Flags::OUTSIDE_TXN,
    )
    .await
    .unwrap();
    pub1.flush().await.unwrap();

    // Only P1's new doc appears. P2's rolled-back docs remain permanently
    // uncommitted — P2 is retired and no future ACK will advance its
    // last_commit past their clocks.
    let frontier3 = next_resolved_checkpoint(&mut session, "phase 3 checkpoint").await;
    let read3 = collect_read_entries(&frontier3, &scenario_dir, &mut shard_state);
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
                Ok((
                    0,
                    serde_json::json!({
                        "_meta": {"uuid": uuid.to_string()},
                        "id": id,
                        "category": "alpha",
                        "value": 3,
                    }),
                ))
            },
            uuid::Flags::CONTINUE_TXN,
        )
        .await
        .unwrap();
    }
    pub1.flush().await.unwrap();

    // Deep rollback ACK for P1 at Clock::default() (< P1's last_commit).
    let deep_rollback_acks = build_rollback_ack(p1, uuid::Clock::default(), &p2_journals);
    pub1.write_intents(deep_rollback_acks).await.unwrap();

    // P1's pending CONTINUE docs are rolled back. P1's last_commit regresses
    // to Clock::default().
    let frontier4 = next_resolved_checkpoint(&mut session, "phase 4 checkpoint").await;
    let read4 = collect_read_entries(&frontier4, &scenario_dir, &mut shard_state);
    insta::assert_debug_snapshot!(
        "rollback_phase4_deep_rollback",
        Checkpoint {
            frontier: &frontier4,
            read: read4,
        }
    );

    session.close().await.expect("close");
}

/// Gapped-producer recovery, ACK-triggered. P2 opens an uncommitted CONTINUE
/// span, then P1 commits an OUTSIDE document that advances the checkpoint
/// maximum M past P2's begin F.
///
/// Resuming from that checkpoint skips ahead to M, so P2's span `[F, M)` is
/// *gapped* and the main read never re-reads it. When P2 later commits, its ACK
/// is the first newer document the main read reaches (P2's CONTINUE is below M):
/// the Slice parks at the ACK, replays `[F, ack.begin)` — skipping P1's
/// already-committed document — then re-presents the ACK to the normal path,
/// which commits and delivers P2's span exactly once, atomically.
async fn gapped_replay(
    materialization_spec: &flow::MaterializationSpec,
    capture_spec: &flow::CaptureSpec,
    journal_client: &gazette::journal::Client,
    service: &shuffle::Service,
    log_dir: &std::path::Path,
) {
    let phase1_dir = log_dir.join("gapped_replay_p1");
    let resume_dir = log_dir.join("gapped_replay_resume");
    std::fs::create_dir_all(&phase1_dir).unwrap();
    std::fs::create_dir_all(&resume_dir).unwrap();

    let p1 = uuid::Producer::from_bytes([0x01, 0x00, 0x00, 0x00, 0x00, 0x01]);
    let p2 = uuid::Producer::from_bytes([0x03, 0x00, 0x00, 0x00, 0x00, 0x02]);
    let mut pub1 = make_publisher(capture_spec, journal_client, p1);
    let mut pub2 = make_publisher(capture_spec, journal_client, p2);

    // P2 opens an uncommitted CONTINUE span (its begin F = 0, journal start).
    // Writing it before P1's commit ensures the first flushed checkpoint
    // observes P2 as pending, alongside P1 committed.
    pub2.enqueue(
        |uuid| {
            Ok((
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "g-p2",
                    "category": "alpha",
                    "value": 20,
                }),
            ))
        },
        uuid::Flags::CONTINUE_TXN,
    )
    .await
    .unwrap();
    pub2.flush().await.unwrap();

    // P1 commits an OUTSIDE document, advancing M past F. The resume read
    // then starts at M rather than P2's span begin — the acceptance
    // criterion — and P2 (F = 0 < M) is gapped. F = 0 additionally
    // exercises the last-commit floor: the flushed checkpoint persists P2's
    // entry as `{last_commit: raw 1, offset: 0}` so recovery can distinguish
    // this real span from a hint-only placeholder, and it must still be
    // gapped on resume.
    pub1.enqueue(
        |uuid| {
            Ok((
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "g-p1",
                    "category": "alpha",
                    "value": 10,
                }),
            ))
        },
        uuid::Flags::OUTSIDE_TXN,
    )
    .await
    .unwrap();
    pub1.flush().await.unwrap();

    // Session 1: capture a checkpoint with P1 committed and P2 pending (F).
    let mut session = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_shards(1, service.peer_endpoint(), &phase1_dir),
        Default::default(),
    )
    .await
    .expect("SessionClient::open phase 1");

    let frontier1 = next_resolved_checkpoint(&mut session, "gapped phase 1").await;
    let mut shard_state: ShardState = (0..1).map(|_| None).collect();
    let read1 = collect_read_entries(&frontier1, &phase1_dir, &mut shard_state);
    insta::assert_debug_snapshot!(
        "gapped_replay_checkpoint1",
        Checkpoint {
            frontier: &frontier1,
            read: read1,
        }
    );
    session.close().await.expect("close phase 1");

    // Resume: P2 (F < M) is gapped; its span is skipped.
    let mut resumed = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_shards(1, service.peer_endpoint(), &resume_dir),
        frontier1.clone(),
    )
    .await
    .expect("SessionClient::open resumed");

    // P2 now commits its pending transaction. The main read reaches P2's ACK
    // (its first newer document), triggers a replay of [F, ack.begin), and on
    // completion re-presents the ACK to commit and deliver P2's span.
    let (producer, commit_clock, journals) = pub2.commit_intents();
    let acks = publisher::intents::build_transaction_intents(&[(producer, commit_clock, journals)]);
    pub2.write_intents(acks).await.unwrap();

    let frontier2 = next_resolved_checkpoint(&mut resumed, "gapped replay").await;
    let mut resumed_shard_state: ShardState = (0..1).map(|_| None).collect();
    let read2 = collect_read_entries(&frontier2, &resume_dir, &mut resumed_shard_state);
    // The reader must yield exactly P2's recovered document (g-p2) — once —
    // and never re-yield P1's already-committed document.
    insta::assert_debug_snapshot!(
        "gapped_replay_resumed",
        Checkpoint {
            frontier: &frontier2,
            read: read2,
        }
    );

    resumed.close().await.expect("close resumed");
}

/// Gapped-producer recovery where the *trigger* is a CONTINUE, not an ACK.
/// P2 opens a CONTINUE span at `F = 0`; P1 then commits an OUTSIDE document,
/// advancing the checkpoint maximum `M` past `F`; finally P2 writes a *second*
/// CONTINUE that lands at or after `M`.
///
/// On resume the main read starts at `M` and reaches P2's second CONTINUE — its
/// first newer document — before any ACK. That CONTINUE is the trigger: the read
/// parks and replays `[F, trigger.begin)`, recovering P2's first CONTINUE
/// (skipping P1's committed OUTSIDE). Completion installs the reconstructed open
/// span and re-presents the CONTINUE, which extends the span (`ContinueExtendSpan`)
/// and appends — committing nothing yet. Only when P2's ACK is then read by the
/// main read does the span commit, delivering both of P2's documents exactly
/// once and never re-delivering P1's.
async fn gapped_continue_trigger(
    materialization_spec: &flow::MaterializationSpec,
    capture_spec: &flow::CaptureSpec,
    journal_client: &gazette::journal::Client,
    service: &shuffle::Service,
    log_dir: &std::path::Path,
) {
    let phase1_dir = log_dir.join("gapped_continue_p1");
    let resume_dir = log_dir.join("gapped_continue_resume");
    std::fs::create_dir_all(&phase1_dir).unwrap();
    std::fs::create_dir_all(&resume_dir).unwrap();

    let p1 = uuid::Producer::from_bytes([0x01, 0x00, 0x00, 0x00, 0x00, 0x01]);
    let p2 = uuid::Producer::from_bytes([0x03, 0x00, 0x00, 0x00, 0x00, 0x02]);
    let mut pub1 = make_publisher(capture_spec, journal_client, p1);
    let mut pub2 = make_publisher(capture_spec, journal_client, p2);

    // P2's first CONTINUE opens its span at F = 0 (journal start).
    pub2.enqueue(
        |uuid| {
            Ok((
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "gc-p2a",
                    "category": "alpha",
                    "value": 21,
                }),
            ))
        },
        uuid::Flags::CONTINUE_TXN,
    )
    .await
    .unwrap();
    pub2.flush().await.unwrap();

    // P1 commits an OUTSIDE document, advancing M past F = 0.
    pub1.enqueue(
        |uuid| {
            Ok((
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "gc-p1",
                    "category": "alpha",
                    "value": 10,
                }),
            ))
        },
        uuid::Flags::OUTSIDE_TXN,
    )
    .await
    .unwrap();
    pub1.flush().await.unwrap();

    // P2's second CONTINUE lands at or after M (it is written after P1's commit).
    // P2's checkpoint entry still pins F = 0 (the span begin), so on resume this
    // document sits at-or-above the M-derived start and becomes the trigger.
    pub2.enqueue(
        |uuid| {
            Ok((
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "gc-p2b",
                    "category": "alpha",
                    "value": 22,
                }),
            ))
        },
        uuid::Flags::CONTINUE_TXN,
    )
    .await
    .unwrap();
    pub2.flush().await.unwrap();

    // Session 1: capture a checkpoint with P1 committed and P2 pending at F = 0.
    let mut session = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_shards(1, service.peer_endpoint(), &phase1_dir),
        Default::default(),
    )
    .await
    .expect("SessionClient::open phase 1");

    let frontier1 = next_resolved_checkpoint(&mut session, "gapped continue phase 1").await;
    session.close().await.expect("close phase 1");

    // Resume: P2 (F = 0 < M) is gapped; its span is skipped. The main read starts
    // at M and reaches P2's second CONTINUE first, triggering the replay.
    let mut resumed = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_shards(1, service.peer_endpoint(), &resume_dir),
        frontier1.clone(),
    )
    .await
    .expect("SessionClient::open resumed");

    // P2 commits. Its ACK is read by the (now un-gapped) main read after the
    // replay completes, committing both of P2's CONTINUE documents together.
    let (producer, commit_clock, journals) = pub2.commit_intents();
    let acks = publisher::intents::build_transaction_intents(&[(producer, commit_clock, journals)]);
    pub2.write_intents(acks).await.unwrap();

    let frontier2 = next_resolved_checkpoint(&mut resumed, "gapped continue replay").await;
    let mut resumed_shard_state: ShardState = (0..1).map(|_| None).collect();
    let read2 = collect_read_entries(&frontier2, &resume_dir, &mut resumed_shard_state);
    // The reader must yield exactly P2's two recovered documents (gc-p2a via the
    // replay, gc-p2b via the re-presented trigger) — each once — and never
    // re-yield P1's already-committed document.
    insta::assert_debug_snapshot!(
        "gapped_continue_trigger_resumed",
        Checkpoint {
            frontier: &frontier2,
            read: read2,
        }
    );

    resumed.close().await.expect("close resumed");
}

/// A replay blocks all main-read → Log draining for the whole Slice until it
/// completes — an accepted head-of-line-blocking cost. This asserts the blocking
/// gate does not drop or deadlock an unrelated journal's
/// ready documents: while P2's gapped span in `apples` is being replayed, P3
/// commits a fresh document to a *different* journal (`bananas`) read by the same
/// single-shard Slice. P3's document is not appended while the replay is in
/// flight; once the replay completes and the parked trigger is re-presented,
/// normal draining resumes and P3 flows. Both P2's recovered span and P3's
/// document must be delivered exactly once, and P1's already-committed document
/// must never be re-read.
async fn gapped_replay_blocks_other_journal(
    materialization_spec: &flow::MaterializationSpec,
    capture_spec: &flow::CaptureSpec,
    journal_client: &gazette::journal::Client,
    service: &shuffle::Service,
    log_dir: &std::path::Path,
) {
    let phase1_dir = log_dir.join("gapped_blocks_p1");
    let resume_dir = log_dir.join("gapped_blocks_resume");
    std::fs::create_dir_all(&phase1_dir).unwrap();
    std::fs::create_dir_all(&resume_dir).unwrap();

    let p1 = uuid::Producer::from_bytes([0x01, 0x00, 0x00, 0x00, 0x00, 0x01]);
    let p2 = uuid::Producer::from_bytes([0x03, 0x00, 0x00, 0x00, 0x00, 0x02]);
    let p3 = uuid::Producer::from_bytes([0x05, 0x00, 0x00, 0x00, 0x00, 0x03]);
    let mut pub1 = make_publisher(capture_spec, journal_client, p1);
    let mut pub2 = make_publisher(capture_spec, journal_client, p2);
    let mut pub3 = make_publisher(capture_spec, journal_client, p3);

    // P2 opens an uncommitted CONTINUE span in `apples` (begin F = 0).
    pub2.enqueue(
        |uuid| {
            Ok((
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "gb-p2",
                    "category": "alpha",
                    "value": 20,
                }),
            ))
        },
        uuid::Flags::CONTINUE_TXN,
    )
    .await
    .unwrap();
    pub2.flush().await.unwrap();

    // P1 commits an OUTSIDE document in `apples`, advancing M past F. P2 is gapped.
    pub1.enqueue(
        |uuid| {
            Ok((
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "gb-p1",
                    "category": "alpha",
                    "value": 10,
                }),
            ))
        },
        uuid::Flags::OUTSIDE_TXN,
    )
    .await
    .unwrap();
    pub1.flush().await.unwrap();

    // Session 1: capture a checkpoint with P1 committed and P2 pending in apples.
    let mut session = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_shards(1, service.peer_endpoint(), &phase1_dir),
        Default::default(),
    )
    .await
    .expect("SessionClient::open phase 1");

    let frontier1 = next_resolved_checkpoint(&mut session, "gapped blocks phase 1").await;
    session.close().await.expect("close phase 1");

    // Resume: P2 (F = 0 < M) is gapped in apples; its span is skipped.
    let mut resumed = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_shards(1, service.peer_endpoint(), &resume_dir),
        frontier1.clone(),
    )
    .await
    .expect("SessionClient::open resumed");

    // P2 commits its apples span — its ACK triggers the replay on resume. P3
    // commits a fresh OUTSIDE document to the *bananas* journal, read by the same
    // Slice: it is ready while the apples replay is in flight, and the blocking
    // gate must hold it until the replay completes, then let it flow.
    let (producer, commit_clock, journals) = pub2.commit_intents();
    let acks = publisher::intents::build_transaction_intents(&[(producer, commit_clock, journals)]);
    pub2.write_intents(acks).await.unwrap();

    pub3.enqueue(
        |uuid| {
            Ok((
                1,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "gb-p3",
                    "category": "alpha",
                    "value": 30,
                }),
            ))
        },
        uuid::Flags::OUTSIDE_TXN,
    )
    .await
    .unwrap();
    pub3.flush().await.unwrap();

    // Poll until both P2 (apples, recovered via replay) and P3 (bananas) are
    // committed. They commit in separate flush cycles — P2's ACK after the
    // replay completes, then P3 once draining resumes — so accumulate deltas.
    let mut frontier2 = shuffle::Frontier::default();
    let committed = |f: &shuffle::Frontier, p: uuid::Producer| {
        f.journals.iter().any(|jf| {
            jf.producers
                .iter()
                .any(|pf| pf.producer == p && pf.last_commit > uuid::Clock::zero())
        })
    };
    loop {
        let delta = next_resolved_checkpoint(&mut resumed, "gapped blocks replay").await;
        frontier2 = frontier2.reduce(delta);
        if committed(&frontier2, p2) && committed(&frontier2, p3) {
            break;
        }
    }

    let mut resumed_shard_state: ShardState = (0..1).map(|_| None).collect();
    let mut read2 = collect_read_entries(&frontier2, &resume_dir, &mut resumed_shard_state);

    // Exactly-once completeness/safety, order-independent: P2's recovered apples
    // document and P3's bananas document, each once; P1's is never re-read.
    let mut delivered: Vec<(u16, String)> = read2
        .iter()
        .map(|e| (e.binding, e.doc["id"].as_str().unwrap().to_string()))
        .collect();
    delivered.sort();
    assert_eq!(
        delivered,
        vec![(0, "gb-p2".to_string()), (1, "gb-p3".to_string())],
        "both the replayed apples span and the unrelated bananas commit must be delivered exactly once",
    );

    // Snapshot the read entries in log order. Journal creation/read races across
    // the two bindings make the inter-journal order unstable, so sort for a
    // deterministic snapshot; the exactly-once assertion above is the substantive
    // check that the blocking gate neither drops nor duplicates B's document.
    read2.sort_by(|a, b| (a.binding, &a.journal).cmp(&(b.binding, &b.journal)));
    insta::assert_debug_snapshot!("gapped_replay_blocks_other_journal_resumed", read2);

    resumed.close().await.expect("close resumed");
}

/// Find a producer's frontier entry within journals whose name contains
/// `journal_substr` (one binding per collection makes this unambiguous).
fn find_producer<'f>(
    frontier: &'f shuffle::Frontier,
    journal_substr: &str,
    producer: uuid::Producer,
) -> Option<&'f shuffle::ProducerFrontier> {
    frontier
        .journals
        .iter()
        .filter(|jf| jf.journal.contains(journal_substr))
        .flat_map(|jf| jf.producers.iter())
        .find(|pf| pf.producer == producer)
}

/// Zero measures that depend on flush-cycle granularity (byte deltas, flushed
/// LSNs) so a snapshot captures only the producer state a scenario asserts on.
/// Emission boundaries can race with flush cycles, splitting the same producer
/// state across one or more checkpoint responses; the producer state itself is
/// deterministic under the client's cumulative reduce, but which emission
/// carries which byte deltas is not.
fn scrub_measures(frontier: &shuffle::Frontier) -> shuffle::Frontier {
    let mut scrubbed = frontier.clone();
    scrubbed.flushed_lsn.clear();
    for jf in &mut scrubbed.journals {
        jf.bytes_read_delta = 0;
        jf.bytes_behind_delta = 0;
    }
    scrubbed
}

/// Causal-hint elevation flips a producer's stale span-begin offset to the
/// journal's cut floor `-M` (`JournalFrontier::resolve_hints`), and the flip
/// surfaces in a persisted checkpoint exactly when the resolving progress is
/// itself held back by a *fresh* unresolved hint — otherwise the read-derived
/// commit offset (a larger magnitude) folds into the same emission and
/// dominates the flip in reduction.
///
/// Phase 1 (held): P1 commits an OUTSIDE baseline in apples (so its later span
/// begins at O > 0 — the shape that would re-gap on recovery), then opens
/// transaction T1 spanning apples (span at O) and bananas. P2 commits later in
/// apples, so its ACK end M > O is apples' maximum offset. T1 commits at H1,
/// but only bananas' ACK is written — P1 "crashed" before writing apples' —
/// so the apples read can never observe T1's commit directly and the pipeline
/// holds P1's entry `{C0, H1, +O}` in `unresolved`, answering checkpoint
/// requests with peeks.
///
/// Phase 2 (flip): P1 recovers — transaction T2 adds a bananas document and
/// commits at H2, writing recovery ACKs for both session journals. Apples'
/// ACK is written: it proves P1's apples commits through H2, elevating the
/// held entry (capped at the H1 hint) and flipping its offset to the cut
/// floor -M; and it carries a fresh causal hint (bananas @ H2) whose own ACK
/// is withheld, holding the read-derived apples progress in `unresolved`. The
/// emitted checkpoint therefore shows P1 at `{H1, H1, -M}` — matching P2's
/// committed end — and the flip survives the client's cumulative
/// max-magnitude reduce over the earlier `+O` peek.
///
/// Phase 3 (resumed): resuming from the cumulative checkpoint recovers P1
/// committed (negative offset) — NOT gapped — so the apples read starts at M
/// with no replay and never re-reads the settled span `[O, M)`. The
/// withheld bananas ACK is written before the resume, so T2 resolves and
/// delivers its bananas document exactly once; nothing of P1's settled apples
/// span is re-delivered.
async fn hint_elevated_offset_flip(
    materialization_spec: &flow::MaterializationSpec,
    capture_spec: &flow::CaptureSpec,
    journal_client: &gazette::journal::Client,
    service: &shuffle::Service,
    log_dir: &std::path::Path,
) {
    let phase1_dir = log_dir.join("hint_flip_p1");
    let resume_dir = log_dir.join("hint_flip_resume");
    std::fs::create_dir_all(&phase1_dir).unwrap();
    std::fs::create_dir_all(&resume_dir).unwrap();

    let p1 = uuid::Producer::from_bytes([0x01, 0x00, 0x00, 0x00, 0x00, 0x01]);
    let p2 = uuid::Producer::from_bytes([0x03, 0x00, 0x00, 0x00, 0x00, 0x02]);
    let mut pub1 = make_publisher(capture_spec, journal_client, p1);
    let mut pub2 = make_publisher(capture_spec, journal_client, p2);

    // P1's OUTSIDE baseline: gives P1 a committed last_commit C0 and pushes its
    // later span begin O above zero. Being the lowest-clock commit, it also
    // absorbs the Slice's first flush-cycle boundary, so all remaining stage-1
    // state lands in one subsequent flush frontier and is promoted to
    // `unresolved` whole — with P2's -M sibling present as the cut floor.
    pub1.enqueue(
        |uuid| {
            Ok((
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "he-p1-base",
                    "category": "alpha",
                    "value": 10,
                }),
            ))
        },
        uuid::Flags::OUTSIDE_TXN,
    )
    .await
    .unwrap();
    pub1.flush().await.unwrap();

    // P2 opens its span in apples ahead of P1's, so P1's span begin O sits
    // strictly between the baseline and P2's eventual committed end M.
    pub2.enqueue(
        |uuid| {
            Ok((
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "he-p2",
                    "category": "alpha",
                    "value": 20,
                }),
            ))
        },
        uuid::Flags::CONTINUE_TXN,
    )
    .await
    .unwrap();
    pub2.flush().await.unwrap();

    // T1: P1's transaction spans apples (span begins at O) and bananas.
    pub1.enqueue(
        |uuid| {
            Ok((
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "he-p1-span",
                    "category": "alpha",
                    "value": 11,
                }),
            ))
        },
        uuid::Flags::CONTINUE_TXN,
    )
    .await
    .unwrap();
    pub1.enqueue(
        |uuid| {
            Ok((
                1,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "he-p1-b1",
                    "category": "alpha",
                    "value": 12,
                }),
            ))
        },
        uuid::Flags::CONTINUE_TXN,
    )
    .await
    .unwrap();
    pub1.flush().await.unwrap();

    // P2 commits: its ACK's end offset M becomes apples' maximum offset (M > O).
    let (p2_id, p2_commit, p2_journals) = pub2.commit_intents();
    let p2_acks = publisher::intents::build_transaction_intents(&[(p2_id, p2_commit, p2_journals)]);
    pub2.write_intents(p2_acks).await.unwrap();

    // T1 commits at H1 — but only bananas' ACK is written (P1 "crashed" before
    // writing apples'). The apples read can never observe T1's commit directly;
    // only the causal hint (apples, P1) @ H1 from bananas' ACK covers the span.
    let (p1_id, t1_commit, t1_journals) = pub1.commit_intents();
    let mut t1_acks =
        publisher::intents::build_transaction_intents(&[(p1_id, t1_commit, t1_journals.clone())]);
    t1_acks.retain(|journal, _| journal.contains("bananas"));
    assert_eq!(t1_acks.len(), 1, "T1 spans apples and bananas");
    pub1.write_intents(t1_acks).await.unwrap();

    let mut session = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_shards(1, service.peer_endpoint(), &phase1_dir),
        Default::default(),
    )
    .await
    .expect("SessionClient::open phase 1");

    // Held phase: the H1 hint cannot resolve (apples' T1 ACK is unwritten), so
    // the pipeline parks P1's entry in `unresolved` and answers with peeks.
    // Emissions may split across flush cycles, so accumulate the client-side
    // cumulative checkpoint until the held state is fully visible.
    let mut base = shuffle::Frontier::default();
    for attempt in 1..=8 {
        let f = session
            .next_checkpoint()
            .await
            .expect("next_checkpoint (held phase)");
        base = base.reduce(f);

        let held = matches!(
            find_producer(&base, "apples", p1),
            Some(pf) if pf.hinted_commit == t1_commit && pf.last_commit < t1_commit
        ) && matches!(
            find_producer(&base, "apples", p2),
            Some(pf) if pf.offset < 0
        ) && matches!(
            find_producer(&base, "bananas", p1),
            Some(pf) if pf.last_commit == t1_commit
        );
        if held {
            break;
        }
        assert!(attempt < 8, "held state did not converge: {base:?}");
    }
    assert_eq!(base.unresolved_hints, 1, "the H1 hint is held");
    let held = find_producer(&base, "apples", p1).unwrap();
    assert!(held.offset > 0, "pre-flip: P1 is an open span begun at +O");

    let mut shard_state: ShardState = (0..1).map(|_| None).collect();
    let mut read1 = collect_read_entries(&base, &phase1_dir, &mut shard_state);
    read1.sort_by_key(|e| (e.binding, e.doc["id"].as_str().map(str::to_owned)));
    insta::assert_debug_snapshot!(
        "hint_elevated_offset_checkpoint1",
        Checkpoint {
            frontier: &scrub_measures(&base),
            read: read1,
        }
    );

    // Phase 2: P1 recovers — T2 adds a bananas document and commits at H2,
    // with recovery ACKs for both session journals. Only apples' ACK is
    // written: it elevates the held entry (capped at H1, flipping the offset
    // to -M) and carries the fresh bananas @ H2 hint that holds the
    // read-derived apples progress in `unresolved`, letting the flip surface.
    pub1.enqueue(
        |uuid| {
            Ok((
                1,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "he-p1-b2",
                    "category": "alpha",
                    "value": 13,
                }),
            ))
        },
        uuid::Flags::CONTINUE_TXN,
    )
    .await
    .unwrap();
    pub1.flush().await.unwrap();

    let (_, t2_commit, _) = pub1.commit_intents();
    let mut t2_acks =
        publisher::intents::build_transaction_intents(&[(p1_id, t2_commit, t1_journals.clone())]);
    let ack_b2: Vec<(String, bytes::Bytes)> = t2_acks
        .iter()
        .filter(|(journal, _)| journal.contains("bananas"))
        .map(|(journal, ack)| (journal.clone(), ack.clone()))
        .collect();
    t2_acks.retain(|journal, _| journal.contains("apples"));
    assert_eq!(t2_acks.len(), 1, "T2 recovery ACKs span apples and bananas");
    pub1.write_intents(t2_acks).await.unwrap();

    let cp2 = next_resolved_checkpoint(&mut session, "hint flip checkpoint 2").await;

    // The core of this fixture: P1's held entry was elevated to (and capped
    // at) the H1 hint, and its offset flipped from the stale +O span begin to
    // -M — the journal's cut floor, which is exactly P2's committed end.
    let flipped = find_producer(&cp2, "apples", p1).expect("P1 in checkpoint 2");
    let sibling = find_producer(&cp2, "apples", p2).expect("P2 in checkpoint 2");
    assert_eq!(flipped.last_commit, t1_commit, "elevated to the H1 cap");
    assert_eq!(flipped.hinted_commit, t1_commit);
    assert!(flipped.offset < 0, "offset flipped to a committed encoding");
    assert_eq!(
        flipped.offset, sibling.offset,
        "the flip writes -M, the cut floor set by P2's committed end",
    );

    // Durability: |-M| beats the earlier +O span begin in the client's
    // cumulative max-magnitude reduce, so the flip survives into the base
    // checkpoint a coordinator would resume from.
    base = base.reduce(cp2.clone());
    assert_eq!(
        find_producer(&base, "apples", p1).unwrap().offset,
        flipped.offset,
        "-M survives the cumulative reduce over the earlier +O",
    );

    // The resolved emission releases exactly the held span's document.
    let read2 = collect_read_entries(&base, &phase1_dir, &mut shard_state);
    insta::assert_debug_snapshot!(
        "hint_elevated_offset_checkpoint2",
        Checkpoint {
            frontier: &scrub_measures(&cp2),
            read: read2,
        }
    );
    session.close().await.expect("close phase 1");

    // The withheld bananas ACK is written before resuming, so the resumed
    // session can resolve T2's fresh hint.
    pub1.write_intents(ack_b2).await.unwrap();

    // Resume from the cumulative checkpoint. P1's apples entry {H1, H1, -M}
    // recovers committed — NOT gapped — so the read starts at M, skips the
    // settled span [O, M) without a replay, and treats apples' recovery ACK
    // as an empty commit. Only T2's bananas document is newly delivered.
    let mut resumed = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_shards(1, service.peer_endpoint(), &resume_dir),
        base.clone(),
    )
    .await
    .expect("SessionClient::open resumed");

    let frontier3 = next_resolved_checkpoint(&mut resumed, "hint flip resumed").await;
    let mut resumed_shard_state: ShardState = (0..1).map(|_| None).collect();
    let read3 = collect_read_entries(&frontier3, &resume_dir, &mut resumed_shard_state);
    assert_eq!(
        read3
            .iter()
            .map(|e| e.doc["id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["he-p1-b2"],
        "only T2's bananas document is delivered; the settled apples span is never re-read",
    );
    insta::assert_debug_snapshot!(
        "hint_elevated_offset_resumed",
        Checkpoint {
            frontier: &frontier3,
            read: read3,
        }
    );

    resumed.close().await.expect("close resumed");
}

/// A gapped producer's OUTSIDE is a protocol violation: the gapped sentinel is a
/// real (if unread) open span, and an OUTSIDE_TXN with no intervening rollback
/// ACK cannot sequence against it. The session must fail-fast with
/// `OutsideWithPrecedingContinue` — identical to a non-gapped producer carrying a
/// genuine pending span.
///
/// P2 opens a CONTINUE span at F = 0; P1 commits an OUTSIDE advancing M past F;
/// session 1 captures the checkpoint. P2 then violates the producer protocol by
/// writing an OUTSIDE without first rolling back its open span. On resume P2 is
/// gapped, and the main read reaches P2's OUTSIDE: `uuid::sequence` rejects it
/// against the pending sentinel and the session tears down with the sequencing
/// error. No replay is attempted — an OUTSIDE never triggers one.
async fn gapped_outside_violation(
    materialization_spec: &flow::MaterializationSpec,
    capture_spec: &flow::CaptureSpec,
    journal_client: &gazette::journal::Client,
    service: &shuffle::Service,
    log_dir: &std::path::Path,
) {
    let phase1_dir = log_dir.join("gapped_outside_violation_p1");
    let resume_dir = log_dir.join("gapped_outside_violation_resume");
    std::fs::create_dir_all(&phase1_dir).unwrap();
    std::fs::create_dir_all(&resume_dir).unwrap();

    let p1 = uuid::Producer::from_bytes([0x01, 0x00, 0x00, 0x00, 0x00, 0x01]);
    let p2 = uuid::Producer::from_bytes([0x03, 0x00, 0x00, 0x00, 0x00, 0x02]);
    let mut pub1 = make_publisher(capture_spec, journal_client, p1);
    let mut pub2 = make_publisher(capture_spec, journal_client, p2);

    // P2 opens an uncommitted CONTINUE span (begin F = 0).
    pub2.enqueue(
        |uuid| {
            Ok((
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "gv-p2",
                    "category": "alpha",
                    "value": 20,
                }),
            ))
        },
        uuid::Flags::CONTINUE_TXN,
    )
    .await
    .unwrap();
    pub2.flush().await.unwrap();

    // P1 commits an OUTSIDE document, advancing M past F. P2 is gapped on resume.
    pub1.enqueue(
        |uuid| {
            Ok((
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "gv-p1",
                    "category": "alpha",
                    "value": 10,
                }),
            ))
        },
        uuid::Flags::OUTSIDE_TXN,
    )
    .await
    .unwrap();
    pub1.flush().await.unwrap();

    // Session 1: capture a checkpoint with P1 committed and P2 pending at F = 0.
    let mut session = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_shards(1, service.peer_endpoint(), &phase1_dir),
        Default::default(),
    )
    .await
    .expect("SessionClient::open phase 1");

    let frontier1 = next_resolved_checkpoint(&mut session, "gapped violation phase 1").await;
    session.close().await.expect("close phase 1");

    // P2 violates the producer protocol: an OUTSIDE with no rollback ACK for its
    // still-open span.
    pub2.enqueue(
        |uuid| {
            Ok((
                0,
                serde_json::json!({
                    "_meta": {"uuid": uuid.to_string()},
                    "id": "gv-p2-outside",
                    "category": "alpha",
                    "value": 21,
                }),
            ))
        },
        uuid::Flags::OUTSIDE_TXN,
    )
    .await
    .unwrap();
    pub2.flush().await.unwrap();

    // Resume: the main read reaches P2's OUTSIDE and fails to sequence it against
    // the gapped sentinel. No checkpoint can become ready first — nothing has
    // committed — so the first response is the teardown error.
    let mut resumed = shuffle::SessionClient::open(
        service,
        build_task(materialization_spec),
        build_shards(1, service.peer_endpoint(), &resume_dir),
        frontier1.clone(),
    )
    .await
    .expect("SessionClient::open resumed");

    let err = match resumed.next_checkpoint().await {
        Ok(frontier) => panic!("expected session teardown, got checkpoint: {frontier:?}"),
        Err(err) => format!("{err:#}"),
    };
    assert!(
        err.contains("OUTSIDE_TXN with a preceding unacknowledged CONTINUE_TXN"),
        "expected OutsideWithPrecedingContinue teardown, got: {err}",
    );
    // The session tore down on error; there is no clean close to perform.
}
