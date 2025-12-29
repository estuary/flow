use super::DekafTestEnv;
use serde_json::json;

const FIXTURE: &str = include_str!("fixtures/basic.flow.yaml");

/// Verify that empty fetch responses are valid and don't cause parse errors.
///
/// The test:
/// 1. Injects 2 documents and consumes them
/// 2. Issues another fetch (no more documents exist, triggers empty response)
/// 3. Verifies no parse errors occurred
/// 4. Injects one more document and confirms consumer can still fetch it
#[tokio::test]
async fn test_empty_fetch_valid_message_set_size() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("empty_fetch", FIXTURE).await?;

    env.inject_documents(
        "data",
        vec![
            json!({"id": "1", "value": "first"}),
            json!({"id": "2", "value": "second"}),
        ],
    )
    .await?;

    let consumer = env.kafka_consumer()?;
    consumer.subscribe(&["test_topic"])?;

    let records = consumer.fetch().await?;

    assert_eq!(records.len(), 2, "should receive 2 initial documents");
    assert_eq!(records[0].value["id"], "1");
    assert_eq!(records[0].value["value"], "first");
    assert_eq!(records[1].value["id"], "2");
    assert_eq!(records[1].value["value"], "second");

    // Fetch again when caught up. Should return empty since no new documents.
    let empty_records = consumer.fetch().await?;
    assert_eq!(
        empty_records.len(),
        0,
        "should receive empty response when caught up"
    );

    env.inject_documents("data", vec![json!({"id": "3", "value": "third"})])
        .await?;

    let more_records = consumer.fetch().await?;

    assert_eq!(
        more_records.len(),
        1,
        "should receive 1 document after reinject"
    );
    assert_eq!(more_records[0].value["id"], "3");
    assert_eq!(more_records[0].value["value"], "third");

    Ok(())
}
