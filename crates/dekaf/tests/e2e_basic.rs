//! Basic E2E test to verify the test harness works end-to-end.

mod e2e;

use e2e::DekafTestEnv;
use serde_json::json;

const FIXTURE: &str = include_str!("e2e/fixtures/basic.flow.yaml");

/// Basic roundtrip test: publish specs, inject documents, consume via Dekaf.
#[tokio::test]
async fn test_basic_roundtrip() -> anyhow::Result<()> {
    e2e::init_tracing();

    let env = DekafTestEnv::setup("basic_roundtrip", FIXTURE).await?;

    env.inject_documents(
        "data",
        vec![
            json!({"id": "doc-1", "value": "hello"}),
            json!({"id": "doc-2", "value": "world"}),
        ],
    )
    .await?;

    tracing::info!("Creating Kafka consumer");
    let consumer = env.kafka_consumer("test-token-12345");
    consumer.subscribe(&["test_topic"])?;

    tracing::info!("Fetching all available documents");
    let records = consumer.fetch().await?;
    tracing::info!(count = records.len(), "Received");

    assert_eq!(records.len(), 2, "should receive both injected documents");
    assert_eq!(records[0].value["id"], "doc-1");
    assert_eq!(records[0].value["value"], "hello");
    assert_eq!(records[1].value["id"], "doc-2");
    assert_eq!(records[1].value["value"], "world");

    Ok(())
}
