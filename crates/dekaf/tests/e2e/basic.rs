//! Basic E2E test to verify the test harness works end-to-end.

use super::DekafTestEnv;
use serde_json::json;

const FIXTURE: &str = include_str!("fixtures/basic.flow.yaml");

/// Basic roundtrip test: publish specs, inject documents, consume via Dekaf.
#[ignore] // Requires local stack: mise run local:stack
#[tokio::test]
async fn test_basic_roundtrip() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("basic_roundtrip", FIXTURE).await?;

    env.inject_documents(
        "data",
        vec![
            json!({"id": "doc-1", "value": "hello"}),
            json!({"id": "doc-2", "value": "world"}),
        ],
    )
    .await?;

    let consumer = env.kafka_consumer()?;
    consumer.subscribe(&["test_topic"])?;

    let records = consumer.fetch().await?;

    assert_eq!(records.len(), 2, "should receive both injected documents");
    insta::assert_json_snapshot!(snapshot_records(&records));

    Ok(())
}
