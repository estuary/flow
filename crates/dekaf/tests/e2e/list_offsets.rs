use super::DekafTestEnv;
use super::raw_kafka::{TestKafkaClient, list_offsets_partition_error};
use kafka_protocol::ResponseError;
use rdkafka::consumer::Consumer;
use serde_json::json;
use std::time::Duration;

const BASIC_FIXTURE: &str = include_str!("fixtures/basic.flow.yaml");

/// Verify ListOffsets returns valid earliest (-2) and latest (-1) offsets.
#[tokio::test]
async fn test_list_offsets_earliest_and_latest() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("list_offsets_basic", BASIC_FIXTURE).await?;

    env.inject_documents(
        "data",
        vec![
            json!({"id": "1", "value": "first"}),
            json!({"id": "2", "value": "second"}),
            json!({"id": "3", "value": "third"}),
        ],
    )
    .await?;

    let consumer = env.kafka_consumer()?;

    // fetch_watermarks internally uses ListOffsets with timestamp=-2 (earliest)
    // and timestamp=-1 (latest).
    let (low, high) =
        consumer
            .inner()
            .fetch_watermarks("test_topic", 0, Duration::from_secs(10))?;

    assert!(low >= 0, "earliest offset should be >= 0, got {low}");
    assert!(
        high > 0,
        "latest offset should be > 0 after injecting docs, got {high}"
    );
    assert!(high >= low, "latest ({high}) should be >= earliest ({low})");

    // Inject more documents and verify latest offset advances
    let high_before = high;

    env.inject_documents("data", vec![json!({"id": "4", "value": "fourth"})])
        .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let (_, high_after) =
        consumer
            .inner()
            .fetch_watermarks("test_topic", 0, Duration::from_secs(10))?;

    assert!(
        high_after > high_before,
        "latest offset should advance after injecting more docs: before={high_before}, after={high_after}"
    );

    Ok(())
}

/// When a client requests offsets for a partition that doesn't exist,
/// Dekaf should return UnknownTopicOrPartition error code.
#[tokio::test]
async fn test_list_offsets_unknown_partition() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("list_offsets_unknown", BASIC_FIXTURE).await?;

    // Inject a document so the collection is in Ready state (journals exist)
    env.inject_documents("data", vec![json!({"id": "1", "value": "test"})])
        .await?;

    let info = env.connection_info();
    let token = env.dekaf_token()?;
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;

    let resp = client
        .list_offsets_with_epoch("test_topic", 99, -1, 1)
        .await?;

    let error_code = list_offsets_partition_error(&resp, "test_topic", 99)
        .expect("partition should exist in ListOffsets response");

    assert_eq!(
        error_code,
        ResponseError::UnknownTopicOrPartition.code(),
        "expected UnknownTopicOrPartition error, got error code {error_code}"
    );

    Ok(())
}

/// Verify that multiple ListOffsets queries return consistent results.
///
/// When no data changes between queries, offsets should remain stable.
#[tokio::test]
async fn test_list_offsets_multiple_queries() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("list_offsets_multi", BASIC_FIXTURE).await?;

    env.inject_documents(
        "data",
        vec![
            json!({"id": "1", "value": "a"}),
            json!({"id": "2", "value": "b"}),
            json!({"id": "3", "value": "c"}),
        ],
    )
    .await?;

    let consumer = env.kafka_consumer()?;

    let (baseline_low, baseline_high) =
        consumer
            .inner()
            .fetch_watermarks("test_topic", 0, Duration::from_secs(10))?;

    assert!(
        baseline_low >= 0,
        "baseline: earliest should be >= 0, got {baseline_low}"
    );
    assert!(
        baseline_high > 0,
        "baseline: latest should be > 0 after injecting docs, got {baseline_high}"
    );
    assert!(
        baseline_high >= baseline_low,
        "baseline: latest ({baseline_high}) should be >= earliest ({baseline_low})"
    );

    // Make multiple watermark queries and verify offsets remain stable
    for i in 1..5 {
        let (low, high) =
            consumer
                .inner()
                .fetch_watermarks("test_topic", 0, Duration::from_secs(10))?;

        assert_eq!(
            low, baseline_low,
            "iteration {i}: earliest offset changed (expected {baseline_low}, got {low})"
        );
        assert_eq!(
            high, baseline_high,
            "iteration {i}: latest offset changed (expected {baseline_high}, got {high})"
        );
    }

    Ok(())
}
