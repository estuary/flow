use futures::StreamExt;
use proto_gazette::uuid;
use std::sync::Arc;

/// DataPlanes are somewhat expensive to start, so as best-practice,
/// run multiple sub-tests using a single DataPlane instance.
#[tokio::test]
async fn example_data_plane_tests() {
    // Build the common catalog fixture.
    let source = build::arg_source_to_url("./tests/hello.flow.yaml", false).unwrap();
    let build_output = Arc::new(
        build::for_local_test(&source, true)
            .await
            .into_result()
            .expect("build of catalog fixture should succeed"),
    );
    // Start the common data-plane.
    let data_plane = e2e_support::DataPlane::start(e2e_support::DataPlaneArgs { broker_count: 3 })
        .await
        .expect("DataPlane start");

    // Spawn multiple sub-tests as tokio tasks, running concurrently and joined over.
    // Note that tests which conflict over specific collections would need to be run serially.
    let tests = futures::future::join_all([
        // Basic hello-world test.
        tokio::spawn(hello_world(
            build_output.clone(),
            data_plane.journal_client.clone(),
        )),
    ])
    .await;

    data_plane
        .graceful_stop()
        .await
        .expect("DataPlane graceful_stop");

    // Collect and report status for failed sub-tests.
    for test in tests {
        if let Err(join_err) = test {
            std::panic::resume_unwind(join_err.into_panic());
        }
    }
}

async fn hello_world(build: Arc<build::Output>, journal_client: gazette::journal::Client) {
    let collection_spec = build
        .built
        .built_collections
        .get_by_key(&models::Collection::new("testing/hello-world"))
        .expect("should have one built collection")
        .spec
        .as_ref()
        .expect("built collection should have a spec");

    let binding = publisher::Binding::from_collection_spec(collection_spec, None)
        .expect("should build binding from collection spec");

    let factory: gazette::journal::ClientFactory = Arc::new({
        let journal_client = journal_client.clone();
        move |_authz_sub, _authz_obj| journal_client.clone()
    });

    // Create a Publisher with deterministic identity for reproducible snapshots.
    let mut publisher = publisher::Publisher::new(
        String::new(), // No AuthZ subject.
        vec![binding],
        factory,
        uuid::Producer::from_bytes([0x01, 0x00, 0x00, 0x00, 0x00, 0x01]),
        uuid::Clock::default(),
    );

    // Enqueue data documents across logical partitions.
    publisher
        .enqueue(
            |uuid| {
                (
                    0,
                    serde_json::json!({
                        "_meta": {"uuid": uuid.to_string()},
                        "id": "hello-1",
                        "message": "Hello, World!",
                        "region": "us"
                    }),
                )
            },
            uuid::Flags::CONTINUE_TXN,
        )
        .await
        .expect("first enqueue should succeed");

    publisher
        .enqueue(
            |uuid| {
                (
                    0,
                    serde_json::json!({
                        "_meta": {"uuid": uuid.to_string()},
                        "id": "hello-2",
                        "message": "Greetings from the test!",
                        "region": "eu"
                    }),
                )
            },
            uuid::Flags::CONTINUE_TXN,
        )
        .await
        .expect("second enqueue should succeed");

    // Build and write ACK intent documents.
    let (producer, commit_clock, journals) = publisher.commit_intents();
    let journal_acks =
        publisher::intents::build_transaction_intents(&[(producer, commit_clock, journals)]);

    publisher
        .write_intents(&journal_acks)
        .await
        .expect("ACK write should succeed");

    // Snapshot the partition listing from the Publisher's own watch.
    let (_client, partitions) = publisher.binding_client(0);
    let partitions_watch = partitions.ready().await;
    let splits = partitions_watch.token();
    let splits = splits.result().expect("partitions should be available");

    insta::assert_json_snapshot!("journal_listing", splits, {
        "[].mod_revision" => insta::dynamic_redaction(|value, _path| {
            assert!(value.as_i64().unwrap() > 0);
            "[mod_revision]"
        }),
    });

    // Read all documents (data + ACKs) back from each partition.
    let mut read_content = Vec::new();
    for split in splits.iter() {
        let read_stream = journal_client
            .clone()
            .read(proto_gazette::broker::ReadRequest {
                journal: split.name.to_string(),
                offset: 0,
                block: false,
                ..Default::default()
            });

        let lines = gazette::journal::read::ReadLines::<8192, 0, _>::new(read_stream, 0, false);
        tokio::pin!(lines);

        while let Some(Ok(batch)) = lines.next().await {
            read_content.extend_from_slice(&batch.content);
        }
    }

    let read_str = String::from_utf8(read_content).expect("should be valid utf8");
    insta::assert_snapshot!("round_trip_docs", read_str);
}
