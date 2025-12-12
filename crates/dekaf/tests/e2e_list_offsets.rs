//! Test ListOffsets behavior for earliest, latest, and timestamp-based queries.
//!
//! Covers:
//! - `list_offsets_earliest`: timestamp=-2 returns first fragment offset
//! - `list_offsets_latest`: timestamp=-1 returns write_head (PR #1758)
//! - `list_offsets_timestamp`: Specific timestamp returns covering fragment offset
//! - `list_offsets_flex_versioning`: v6+ flex versioning parsed correctly (PR #1693)
//!
//! Suspended journal handling (#2358) requires additional infrastructure to suspend
//! journals and is deferred to a follow-up test.

mod e2e;

use e2e::DekafTestEnv;
use rdkafka::consumer::Consumer;
use serde_json::json;
use std::time::Duration;

const BASIC_FIXTURE: &str = include_str!("e2e/fixtures/basic.flow.yaml");

/// Verify ListOffsets returns valid earliest (-2) and latest (-1) offsets.
///
/// librdkafka negotiates ListOffsets v6+ via ApiVersions, then issues requests
/// with flex versioning. This test verifies:
/// 1. Flex versioning is correctly detected and parsed (PR #1693)
/// 2. Earliest offset returns the first fragment's begin offset
/// 3. Latest offset returns the write_head (PR #1758)
#[ignore] // Requires local stack: mise run local:stack
#[tokio::test]
async fn test_list_offsets_earliest_and_latest() -> anyhow::Result<()> {
    e2e::init_tracing();

    let env = DekafTestEnv::setup("list_offsets_basic", BASIC_FIXTURE).await?;

    // Inject documents so offsets exist
    env.inject_documents(
        "data",
        vec![
            json!({"id": "1", "value": "first"}),
            json!({"id": "2", "value": "second"}),
            json!({"id": "3", "value": "third"}),
        ],
    )
    .await?;

    let consumer = env.kafka_consumer("test-token-12345");

    // fetch_watermarks internally uses ListOffsets with timestamp=-2 (earliest)
    // and timestamp=-1 (latest). librdkafka uses v6+ which requires flex versioning.
    let (low, high) =
        consumer
            .inner()
            .fetch_watermarks("test_topic", 0, Duration::from_secs(10))?;

    tracing::info!(earliest = low, latest = high, "Watermarks received");

    // Verify offsets are valid
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

    // Small delay to ensure data is written
    tokio::time::sleep(Duration::from_millis(500)).await;

    let (_, high_after) =
        consumer
            .inner()
            .fetch_watermarks("test_topic", 0, Duration::from_secs(10))?;

    tracing::info!(
        before = high_before,
        after = high_after,
        "Latest offset after injection"
    );

    assert!(
        high_after > high_before,
        "latest offset should advance after injecting more docs: before={high_before}, after={high_after}"
    );

    Ok(())
}

/// Verify ListOffsets for specific timestamps returns appropriate offsets.
///
/// This test verifies behavior with both:
/// 1. Unpersisted (open) fragments - have mod_time=0, always returned for any timestamp
/// 2. Persisted fragments - have mod_time>0, filtered by begin_mod_time in query
///
/// Uses a fixture with 1-minute flush interval.
// #[ignore] // Requires local stack: mise run local:stack
// #[tokio::test]
// async fn test_list_offsets_by_timestamp() -> anyhow::Result<()> {
//      Omitted because it requires a way to force a fragment refresh.
// }

/// Verify ListOffsets returns an error for unknown partitions.
///
/// When a client requests offsets for a partition that doesn't exist,
/// Dekaf should return UnknownTopicOrPartition error code, which librdkafka
/// surfaces as an error from fetch_watermarks.
#[ignore] // Requires local stack: mise run local:stack
#[tokio::test]
async fn test_list_offsets_unknown_partition() -> anyhow::Result<()> {
    e2e::init_tracing();

    let env = DekafTestEnv::setup("list_offsets_unknown", BASIC_FIXTURE).await?;

    let consumer = env.kafka_consumer("test-token-12345");

    // Try to fetch watermarks for a non-existent partition (partition 99)
    tracing::info!("Fetching watermarks for non-existent partition 99");
    let result = consumer
        .inner()
        .fetch_watermarks("test_topic", 99, Duration::from_secs(5));

    assert!(
        result.is_err(),
        "ListOffsets should return error for non-existent partition 99, got Ok({:?})",
        result.ok()
    );

    tracing::info!(error = %result.unwrap_err(), "Got expected error for unknown partition");

    Ok(())
}

/// Verify that multiple ListOffsets queries return consistent results.
///
/// When no data changes between queries, offsets should remain stable.
/// This exercises the ListOffsets path multiple times to catch any
/// state leakage or drift between calls.
#[ignore] // Requires local stack: mise run local:stack
#[tokio::test]
async fn test_list_offsets_multiple_queries() -> anyhow::Result<()> {
    e2e::init_tracing();

    let env = DekafTestEnv::setup("list_offsets_multi", BASIC_FIXTURE).await?;

    // Inject documents
    env.inject_documents(
        "data",
        vec![
            json!({"id": "1", "value": "a"}),
            json!({"id": "2", "value": "b"}),
            json!({"id": "3", "value": "c"}),
        ],
    )
    .await?;

    let consumer = env.kafka_consumer("test-token-12345");

    // Fetch baseline watermarks
    tracing::info!("Fetching baseline watermarks");
    let (baseline_low, baseline_high) =
        consumer
            .inner()
            .fetch_watermarks("test_topic", 0, Duration::from_secs(10))?;

    tracing::info!(
        earliest = baseline_low,
        latest = baseline_high,
        "Baseline watermarks"
    );

    // Verify baseline is valid
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
        tracing::info!(iteration = i, "Fetching watermarks");
        let (low, high) =
            consumer
                .inner()
                .fetch_watermarks("test_topic", 0, Duration::from_secs(10))?;

        tracing::info!(iteration = i, earliest = low, latest = high, "Watermarks");

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
