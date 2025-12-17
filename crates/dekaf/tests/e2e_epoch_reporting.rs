//! Category 1: Basic Epoch Reporting Tests
//!
//! These tests verify that Dekaf correctly reports leader epochs in various
//! Kafka protocol responses. The epoch is derived from the binding's backfill
//! counter (offset by +1 to start at epoch 1).

mod e2e;

use e2e::{
    DekafTestEnv,
    raw_kafka::{
        TestKafkaClient, fetch_current_leader_epoch, fetch_partition_error,
        list_offsets_partition_error, metadata_leader_epoch,
    },
};
use serde_json::json;

const FIXTURE: &str = include_str!("e2e/fixtures/basic.flow.yaml");

/// Test 1.1: Verify Metadata response includes leader_epoch >= 1.
///
/// Dekaf maps the binding's backfill counter to Kafka's leader epoch.
/// Since we add 1 to avoid epoch 0 (which consumers handle poorly),
/// the epoch should always be >= 1.
#[ignore] // Requires local stack
#[tokio::test]
async fn test_metadata_includes_leader_epoch() -> anyhow::Result<()> {
    e2e::init_tracing();

    let env = DekafTestEnv::setup("metadata_epoch", FIXTURE).await?;
    let info = env.connection_info();

    // Inject a document so the topic has data
    env.inject_documents("data", vec![json!({"id": "1", "value": "test"})])
        .await?;

    tracing::info!("Connecting raw Kafka client");
    let mut client =
        TestKafkaClient::connect(&info.broker, &info.username, "test-token-12345").await?;

    tracing::info!("Fetching metadata");
    let metadata = client.metadata(&["test_topic"]).await?;

    // Extract leader epoch from the partition
    let leader_epoch = metadata_leader_epoch(&metadata, "test_topic", 0);
    tracing::info!(?leader_epoch, "Got leader epoch from metadata");

    assert!(
        leader_epoch.is_some(),
        "metadata response should include leader_epoch"
    );

    let epoch = leader_epoch.unwrap();
    assert!(
        epoch >= 1,
        "leader_epoch should be >= 1 (got {epoch}), since Dekaf adds 1 to backfill counter"
    );

    // Snapshot the relevant metadata fields
    let snapshot = serde_json::json!({
        "topic": "test_topic",
        "partition": 0,
        "leader_epoch": epoch,
        "epoch_valid": epoch >= 1,
    });
    insta::assert_json_snapshot!("metadata_epoch", snapshot);

    Ok(())
}

/// Test 1.2: Verify ListOffsets response includes leader_epoch.
///
/// When requesting earliest (-2) or latest (-1) offsets, the response
/// should include the current leader epoch.
#[ignore] // Requires local stack
#[tokio::test]
async fn test_list_offsets_includes_leader_epoch() -> anyhow::Result<()> {
    e2e::init_tracing();

    let env = DekafTestEnv::setup("list_offsets_epoch", FIXTURE).await?;
    let info = env.connection_info();

    // Inject documents so offsets exist
    env.inject_documents(
        "data",
        vec![
            json!({"id": "1", "value": "first"}),
            json!({"id": "2", "value": "second"}),
        ],
    )
    .await?;

    tracing::info!("Connecting raw Kafka client");
    let mut client =
        TestKafkaClient::connect(&info.broker, &info.username, "test-token-12345").await?;

    // First get the current epoch from metadata
    let metadata = client.metadata(&["test_topic"]).await?;
    let current_epoch =
        metadata_leader_epoch(&metadata, "test_topic", 0).expect("metadata should have epoch");
    tracing::info!(current_epoch, "Got current epoch from metadata");

    // Request latest offset (-1) with the current epoch
    tracing::info!("Requesting latest offset with current epoch");
    let list_resp = client
        .list_offsets_with_epoch("test_topic", 0, -1, current_epoch)
        .await?;

    // Should succeed (no error)
    let error_code = list_offsets_partition_error(&list_resp, "test_topic", 0);
    tracing::info!(?error_code, "ListOffsets error code");

    assert!(
        error_code.map_or(false, |s| s == 0),
        "ListOffsets should succeed with current epoch, got error: {:?}",
        error_code
    );

    // Extract the leader_epoch from response
    let partition = list_resp
        .topics
        .iter()
        .find(|t| t.name.as_str() == "test_topic")
        .and_then(|t| t.partitions.iter().find(|p| p.partition_index == 0))
        .expect("partition should exist");

    tracing::info!(
        leader_epoch = partition.leader_epoch,
        offset = partition.offset,
        "ListOffsets response"
    );

    assert!(
        partition.leader_epoch >= 1,
        "leader_epoch in ListOffsets response should be >= 1, got {}",
        partition.leader_epoch
    );

    // Request earliest offset (-2) as well
    tracing::info!("Requesting earliest offset");
    let earliest_resp = client
        .list_offsets_with_epoch("test_topic", 0, -2, current_epoch)
        .await?;

    let earliest_partition = earliest_resp
        .topics
        .iter()
        .find(|t| t.name.as_str() == "test_topic")
        .and_then(|t| t.partitions.iter().find(|p| p.partition_index == 0))
        .expect("partition should exist");

    tracing::info!(
        leader_epoch = earliest_partition.leader_epoch,
        offset = earliest_partition.offset,
        "Earliest offset response"
    );

    assert!(
        earliest_partition.leader_epoch >= 1,
        "leader_epoch for earliest offset should be >= 1, got {}",
        earliest_partition.leader_epoch
    );

    // Snapshot the results
    let snapshot = serde_json::json!({
        "topic": "test_topic",
        "partition": 0,
        "latest": {
            "leader_epoch": partition.leader_epoch,
            "offset_valid": partition.offset >= 0,
        },
        "earliest": {
            "leader_epoch": earliest_partition.leader_epoch,
            "offset_valid": earliest_partition.offset >= 0,
        },
    });
    insta::assert_json_snapshot!("list_offsets_epoch", snapshot);

    Ok(())
}

/// Test 1.3: Verify Fetch response includes leader_epoch in current_leader.
///
/// When fetching data, the response should include the current_leader field
/// with the leader_epoch, allowing consumers to detect epoch changes.
#[ignore] // Requires local stack
#[tokio::test]
async fn test_fetch_response_includes_leader_epoch() -> anyhow::Result<()> {
    e2e::init_tracing();

    let env = DekafTestEnv::setup("fetch_epoch", FIXTURE).await?;
    let info = env.connection_info();

    // Inject documents to fetch
    env.inject_documents(
        "data",
        vec![
            json!({"id": "1", "value": "hello"}),
            json!({"id": "2", "value": "world"}),
        ],
    )
    .await?;

    tracing::info!("Connecting raw Kafka client");
    let mut client =
        TestKafkaClient::connect(&info.broker, &info.username, "test-token-12345").await?;

    // Get current epoch from metadata
    let metadata = client.metadata(&["test_topic"]).await?;
    let current_epoch =
        metadata_leader_epoch(&metadata, "test_topic", 0).expect("metadata should have epoch");
    tracing::info!(current_epoch, "Got current epoch from metadata");

    // Fetch with the current epoch
    tracing::info!("Fetching data with current epoch");
    let fetch_resp = client
        .fetch_with_epoch("test_topic", 0, 0, current_epoch)
        .await?;

    // Should succeed
    let error_code = fetch_partition_error(&fetch_resp, "test_topic", 0);
    tracing::info!(?error_code, "Fetch error code");

    assert!(
        error_code.map_or(false, |s| s == 0),
        "Fetch should succeed with current epoch, got error: {:?}",
        error_code
    );

    // Extract leader_epoch from current_leader
    let response_epoch = fetch_current_leader_epoch(&fetch_resp, "test_topic", 0);
    tracing::info!(?response_epoch, "Leader epoch from fetch response");

    assert!(
        response_epoch.is_some(),
        "Fetch response should include current_leader with leader_epoch"
    );

    let epoch = response_epoch.unwrap();
    assert!(
        epoch >= 1,
        "leader_epoch in Fetch response should be >= 1, got {epoch}"
    );

    assert_eq!(
        epoch, current_epoch,
        "Fetch response epoch ({epoch}) should match metadata epoch ({current_epoch})"
    );

    // Verify we got some records
    let partition = fetch_resp
        .responses
        .iter()
        .find(|t| t.topic.as_str() == "test_topic")
        .and_then(|t| t.partitions.iter().find(|p| p.partition_index == 0))
        .expect("partition should exist");

    let has_records = partition.records.as_ref().map_or(false, |r| !r.is_empty());

    tracing::info!(
        has_records,
        high_watermark = partition.high_watermark,
        "Fetch partition data"
    );

    // Snapshot the results
    let snapshot = serde_json::json!({
        "topic": "test_topic",
        "partition": 0,
        "leader_epoch": epoch,
        "epoch_matches_metadata": epoch == current_epoch,
        "has_records": has_records,
        "high_watermark_valid": partition.high_watermark >= 0,
    });
    insta::assert_json_snapshot!("fetch_epoch", snapshot);

    Ok(())
}
