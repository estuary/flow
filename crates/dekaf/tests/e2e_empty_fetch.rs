//! Test that empty fetch responses have valid MessageSetSize (>= 0, not -1).
//!
//! Regression test for PR #1693: When Dekaf returned an empty record set (consumer
//! caught up, or offset between documents), it passed `None` to kafka-protocol which
//! encoded the message set length as -1. librdkafka rejects negative lengths with
//! "Protocol parse failure for Fetch v11 ... invalid MessageSetSize -1".

mod e2e;

use e2e::{kafka::snapshot_records, DekafTestEnv};
use serde_json::json;

const FIXTURE: &str = include_str!("e2e/fixtures/basic.flow.yaml");

/// Verify that empty fetch responses are valid and don't cause parse errors.
///
/// The test:
/// 1. Injects 2 documents and consumes them
/// 2. Issues another fetch (no more documents exist, triggers empty response)
/// 3. Verifies no parse errors occurred
/// 4. Injects one more document and confirms consumer can still fetch it
#[ignore] // Requires local stack: mise run local:stack
#[tokio::test]
async fn test_empty_fetch_valid_message_set_size() -> anyhow::Result<()> {
    e2e::init_tracing();

    let env = DekafTestEnv::setup("empty_fetch", FIXTURE).await?;

    env.inject_documents(
        "data",
        vec![
            json!({"id": "1", "value": "first"}),
            json!({"id": "2", "value": "second"}),
        ],
    )
    .await?;

    tracing::info!("Creating Kafka consumer");
    let consumer = env.kafka_consumer("test-token-12345");
    consumer.subscribe(&["test_topic"])?;

    tracing::info!("Fetching initial documents");
    let records = consumer.fetch().await?;
    tracing::info!(count = records.len(), "Received");

    assert_eq!(records.len(), 2, "should receive 2 initial documents");
    insta::assert_json_snapshot!("initial_fetch", snapshot_records(&records));

    // Fetch again when caught up. Should return empty since no new documents.
    // If MessageSetSize were -1, librdkafka would throw a parse error here.
    tracing::info!("Fetching when caught up (expecting empty)");
    let empty_records = consumer.fetch().await?;
    tracing::info!(count = empty_records.len(), "Received");
    assert_eq!(empty_records.len(), 0, "should receive empty response when caught up");

    env.inject_documents("data", vec![json!({"id": "3", "value": "third"})])
        .await?;

    tracing::info!("Fetching after injecting 1 more document");
    let more_records = consumer.fetch().await?;
    tracing::info!(count = more_records.len(), "Received");

    assert_eq!(more_records.len(), 1, "should receive 1 document after reinject");
    insta::assert_json_snapshot!("after_empty_fetch", snapshot_records(&more_records));

    Ok(())
}
