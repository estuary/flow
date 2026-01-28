use super::DekafTestEnv;
use serde_json::json;

const FIXTURE: &str = include_str!("fixtures/basic.flow.yaml");

/// Basic roundtrip test: publish specs, inject documents, consume via Dekaf.
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

    let consumer = env.kafka_consumer().await?;
    consumer.subscribe(&["test_topic"])?;

    let records = consumer.fetch().await?;

    assert_eq!(records.len(), 2, "should receive both injected documents");
    assert_eq!(records[0].value["id"], "doc-1");
    assert_eq!(records[0].value["value"], "hello");
    assert_eq!(records[1].value["id"], "doc-2");
    assert_eq!(records[1].value["value"], "world");

    Ok(())
}
