//! Category 2 & 3: Collection Reset Detection Tests
//!
//! These tests verify that Dekaf correctly handles collection resets by:
//! - Returning `FENCED_LEADER_EPOCH` when consumers send stale epochs
//! - Returning `UNKNOWN_LEADER_EPOCH` for future epochs
//! - Returning `end_offset=0` for old epochs via `OffsetForLeaderEpoch`
//!
//! Collection reset increments the binding's `backfill` counter, which Dekaf
//! maps to Kafka's `leader_epoch` (offset by +1 to start at epoch 1).

use super::{
    DekafTestEnv,
    raw_kafka::{
        TestKafkaClient, fetch_current_leader_epoch, fetch_partition_error,
        list_offsets_partition_error, metadata_leader_epoch, offset_for_epoch_result,
    },
};
use anyhow::Context;
use kafka_protocol::ResponseError;
use serde_json::json;
use std::time::Duration;

const FIXTURE: &str = include_str!("fixtures/basic.flow.yaml");

/// Default timeout for waiting for epoch changes.
/// With local dev stack, SPEC_TTL is 10s. Give some buffer for test reliability.
const EPOCH_CHANGE_TIMEOUT: Duration = Duration::from_secs(30);

/// Verify `FENCED_LEADER_EPOCH` is returned when consumer sends stale epoch.
///
/// When a consumer sends a `current_leader_epoch` that is less than Dekaf's
/// current epoch (derived from binding backfill counter), Dekaf returns `FENCED_LEADER_EPOCH`.
///
/// The response also includes the current leader epoch in `current_leader`, allowing
/// the consumer to know what the new epoch is.
#[ignore] // Requires local stack
#[tokio::test]
async fn test_fenced_leader_epoch_on_stale_consumer() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("fenced_epoch", FIXTURE).await?;

    // Inject initial document so the collection has data
    env.inject_documents("data", vec![json!({"id": "1", "value": "pre-reset"})])
        .await?;

    let initial_epoch = get_leader_epoch(&env, "test_topic", 0).await?;

    perform_collection_reset(&env, "test_topic", 0, initial_epoch, EPOCH_CHANGE_TIMEOUT).await?;

    let new_epoch = get_leader_epoch(&env, "test_topic", 0).await?;

    // Now fetch with the OLD (stale) epoch - should get FENCED_LEADER_EPOCH
    let info = env.connection_info();
    let token = env.dekaf_token()?;
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, token).await?;

    let fetch_resp = client
        .fetch_with_epoch("test_topic", 0, 0, initial_epoch)
        .await?;
    let error = fetch_partition_error(&fetch_resp, "test_topic", 0).expect("should have error");

    assert!(
        error == ResponseError::FencedLeaderEpoch.code(),
        "expected FENCED_LEADER_EPOCH for stale consumer, got error code {error}"
    );

    tracing::info!(
        initial_epoch,
        new_epoch,
        "FENCED_LEADER_EPOCH received when fetching stale epoch"
    );

    // Extract the leader epoch from the fetch response and verify it's correct
    let response_epoch =
        fetch_current_leader_epoch(&fetch_resp, "test_topic", 0).expect("should have epoch");
    assert_eq!(
        response_epoch, new_epoch,
        "response should include current epoch"
    );

    // Verify fetch works with the new epoch
    let fetch_resp = client
        .fetch_with_epoch("test_topic", 0, 0, new_epoch)
        .await?;
    let error = fetch_partition_error(&fetch_resp, "test_topic", 0);
    assert!(
        error.map_or(false, |s| s == 0),
        "fetch should succeed with new epoch, got error: {:?}",
        error
    );

    // Snapshot the test results
    let snapshot = serde_json::json!({
        "initial_epoch": initial_epoch,
        "new_epoch": new_epoch,
    });
    insta::assert_json_snapshot!("fenced_epoch_on_stale_consumer", snapshot);

    Ok(())
}

/// Verify that ListOffsets returns `FENCED_LEADER_EPOCH` for stale epoch.
///
/// Both Fetch and ListOffsets should validate the `current_leader_epoch` parameter.
#[tokio::test]
async fn test_list_offsets_fenced_epoch() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("list_offsets_fenced", FIXTURE).await?;
    let info = env.connection_info();

    env.inject_documents("data", vec![json!({"id": "1", "value": "test"})])
        .await?;

    // Get initial epoch and verify ListOffsets works before reset
    let token = env.dekaf_token()?;
    let initial_epoch = {
        let mut client = TestKafkaClient::connect(&info.broker, &info.username, token).await?;

        let metadata = client.metadata(&["test_topic"]).await?;
        let epoch =
            metadata_leader_epoch(&metadata, "test_topic", 0).expect("metadata should have epoch");

        // Verify ListOffsets works with current epoch before reset
        let list_resp = client
            .list_offsets_with_epoch("test_topic", 0, -1, epoch) // -1 = latest
            .await?;
        let error = list_offsets_partition_error(&list_resp, "test_topic", 0);
        assert!(
            error.map_or(false, |s| s == 0),
            "ListOffsets should succeed before reset, got error: {:?}",
            error
        );

        epoch
    };

    perform_collection_reset(&env, "test_topic", 0, initial_epoch, EPOCH_CHANGE_TIMEOUT).await?;

    let token = env.dekaf_token()?;
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, token).await?;

    let list_resp = client
        .list_offsets_with_epoch("test_topic", 0, -1, initial_epoch)
        .await?;
    let error =
        list_offsets_partition_error(&list_resp, "test_topic", 0).expect("should have error");

    assert!(
        error == ResponseError::FencedLeaderEpoch.code(),
        "expected FENCED_LEADER_EPOCH for stale epoch in ListOffsets, got error code {error}"
    );

    // Snapshot
    let snapshot = serde_json::json!({
        "initial_epoch": initial_epoch,
        "new_epoch": new_epoch,
    });
    insta::assert_json_snapshot!("list_offsets_fenced_epoch", snapshot);

    Ok(())
}

/// Verify that Fetch returns `UNKNOWN_LEADER_EPOCH` for epoch > current.
///
/// When a consumer sends an epoch that is greater than the current epoch,
/// Dekaf should return `UNKNOWN_LEADER_EPOCH`. This shouldn't happen in normal
/// operation but is something that could theoretically happen.
#[tokio::test]
async fn test_unknown_leader_epoch_for_future_epoch() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("unknown_epoch", FIXTURE).await?;
    let info = env.connection_info();

    env.inject_documents("data", vec![json!({"id": "1", "value": "test"})])
        .await?;

    tracing::info!("Connecting raw Kafka client");
    let token = env.dekaf_token()?;
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, token).await?;

    let current_epoch = get_leader_epoch(&env, "test_topic", 0).await?;

    // Fetch with a future epoch (way higher than current)
    let future_epoch = current_epoch + 100;

    let fetch_resp = client
        .fetch_with_epoch("test_topic", 0, 0, future_epoch)
        .await?;
    let error = fetch_partition_error(&fetch_resp, "test_topic", 0).expect("should have error");

    assert!(
        error == ResponseError::UnknownLeaderEpoch.code(),
        "expected UNKNOWN_LEADER_EPOCH for future epoch, got error code {error}"
    );

    // Also test ListOffsets with future epoch
    let list_resp = client
        .list_offsets_with_epoch("test_topic", 0, -1, future_epoch)
        .await?;
    let list_error =
        list_offsets_partition_error(&list_resp, "test_topic", 0).expect("should have error");

    assert!(
        list_error == ResponseError::UnknownLeaderEpoch.code(),
        "expected UNKNOWN_LEADER_EPOCH for future epoch in ListOffsets, got error code {list_error}"
    );

    // Snapshot
    let snapshot = serde_json::json!({
        "current_epoch": current_epoch,
        "future_epoch": future_epoch,
        "fetch_error": "UNKNOWN_LEADER_EPOCH",
        "list_offsets_error": "UNKNOWN_LEADER_EPOCH",
    });
    insta::assert_json_snapshot!("unknown_leader_epoch_for_future", snapshot);

    Ok(())
}

/// Verify that OffsetForLeaderEpoch returns `end_offset=0` for old epochs.
///
/// After receiving `FENCED_LEADER_EPOCH`, consumers call `OffsetForLeaderEpoch`
/// to find the end offset for their old epoch. Dekaf returns `end_offset=0` for
/// old epochs, indicating the consumer should reset to the beginning.
#[ignore] // Requires local stack
#[tokio::test]
async fn test_offset_for_leader_epoch_returns_zero_for_old_epoch() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("offset_for_old_epoch", FIXTURE).await?;

    env.inject_documents("data", vec![json!({"id": "1", "value": "test"})])
        .await?;

    let initial_epoch = get_leader_epoch(&env, "test_topic", 0).await?;

    perform_collection_reset(&env, "test_topic", 0, initial_epoch, EPOCH_CHANGE_TIMEOUT).await?;

    let info = env.connection_info();
    let token = env.dekaf_token()?;
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, token).await?;

    let resp = client
        .offset_for_leader_epoch("test_topic", 0, initial_epoch)
        .await?;
    let result = offset_for_epoch_result(&resp, "test_topic", 0).expect("should have result");

    assert_eq!(
        result.error_code, 0,
        "OffsetForLeaderEpoch should succeed for old epoch"
    );

    assert_eq!(
        result.end_offset, 0,
        "old epoch should return end_offset=0 (reset to beginning), got {}",
        result.end_offset
    );

    let new_epoch = get_leader_epoch(&env, "test_topic", 0).await?;

    assert_eq!(
        result.leader_epoch, new_epoch,
        "should return current epoch in response"
    );

    // Snapshot
    let snapshot = serde_json::json!({
        "old_epoch": initial_epoch,
        "new_epoch": new_epoch,
        "end_offset_for_old_epoch": result.end_offset,
        "returned_leader_epoch": result.leader_epoch,
    });
    insta::assert_json_snapshot!("offset_for_old_epoch_returns_zero", snapshot);

    Ok(())
}

/// Verify current epoch returns actual high watermark (not 0).
///
/// When querying `OffsetForLeaderEpoch` for the current epoch, Dekaf should
/// return the actual high watermark, not 0. This ensures that only old epochs
/// trigger reset-to-beginning behavior.
#[ignore] // Requires local stack
#[tokio::test]
async fn test_offset_for_leader_epoch_returns_highwater_for_current() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("offset_for_current", FIXTURE).await?;
    let info = env.connection_info();

    // Inject multiple documents so we have a meaningful high watermark
    env.inject_documents(
        "data",
        vec![
            json!({"id": "1", "value": "first"}),
            json!({"id": "2", "value": "second"}),
            json!({"id": "3", "value": "third"}),
        ],
    )
    .await?;

    let token = env.dekaf_token()?;
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, token).await?;

    let metadata = client.metadata(&["test_topic"]).await?;
    let current_epoch =
        metadata_leader_epoch(&metadata, "test_topic", 0).expect("metadata should have epoch");

    // Get the actual high watermark via ListOffsets for comparison
    let list_resp = client
        .list_offsets_with_epoch("test_topic", 0, -1, current_epoch) // -1 = latest
        .await?;
    let list_partition = list_resp
        .topics
        .iter()
        .find(|t| t.name.as_str() == "test_topic")
        .and_then(|t| t.partitions.iter().find(|p| p.partition_index == 0))
        .expect("partition should exist");
    let high_watermark = list_partition.offset;

    let resp = client
        .offset_for_leader_epoch("test_topic", 0, current_epoch)
        .await?;
    let result = offset_for_epoch_result(&resp, "test_topic", 0).expect("should have result");

    assert_eq!(
        result.error_code, 0,
        "OffsetForLeaderEpoch should succeed for current epoch"
    );

    assert!(
        result.end_offset > 0,
        "current epoch should return actual offset (> 0), got {}",
        result.end_offset
    );

    assert!(
        result.end_offset <= high_watermark,
        "end_offset ({}) should be <= high_watermark ({})",
        result.end_offset,
        high_watermark
    );

    assert_eq!(
        result.leader_epoch, current_epoch,
        "should return current epoch in response"
    );

    // Snapshot
    let snapshot = serde_json::json!({
        "current_epoch": current_epoch,
        "end_offset": result.end_offset,
        "high_watermark": high_watermark,
    });
    insta::assert_json_snapshot!("offset_for_current_epoch_returns_highwater", snapshot);

    Ok(())
}

// Collection reset can cause a race condition between activation and deletion,
// so we need to do this song and dance where we first disable the task in order
// to prevent it from recreating journals with an old first-pub ID, then reset the collection,
// then re-enable the task. We then want to wait until Dekaf's cache has picked up the new spec.
pub async fn perform_collection_reset(
    env: &DekafTestEnv,
    topic: &str,
    partition: i32,
    initial_epoch: i32,
    epoch_timeout: Duration,
) -> anyhow::Result<()> {
    env.disable_capture().await?;
    env.reset_collection(None).await?;
    env.enable_capture().await?;

    // Wait for capture to be ready
    let capture = env.capture.as_ref().context("no capture in fixture")?;
    env.wait_for_primary(capture).await?;

    // Inject a document to trigger lazy journal creation for the new epoch
    tracing::info!("Injecting document to create new journal");
    env.inject_documents(
        "data",
        vec![json!({"id": "reset-trigger", "value": "post-reset"})],
    )
    .await?;

    let info = env.connection_info();
    let token = env.dekaf_token()?;
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, token).await?;

    // Wait for Dekaf to pick up the new epoch
    let new_epoch =
        wait_for_epoch_change(&mut client, topic, partition, initial_epoch, epoch_timeout).await?;
    tracing::info!(initial_epoch, new_epoch, "Collection reset completed");

    Ok(())
}

pub async fn get_leader_epoch(
    env: &DekafTestEnv,
    topic: &str,
    partition: i32,
) -> anyhow::Result<i32> {
    let info = env.connection_info();
    let token = env.dekaf_token()?;
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, token).await?;
    let metadata = client.metadata(&[topic]).await?;
    let epoch =
        metadata_leader_epoch(&metadata, topic, partition).context("metadata should have epoch")?;
    tracing::info!(initial_epoch = epoch, "Got initial epoch");
    Ok(epoch)
}

/// Wait for Dekaf to report a leader epoch greater than `previous_epoch` AND have partitions available.
///
/// This polls the metadata endpoint until both conditions are met:
/// 1. The epoch changes (spec refresh completed)
/// 2. The topic has at least one partition (journal listing completed)
///
/// The second condition is needed because after a collection reset, there's a delay between
/// the spec refresh (epoch change) and the new journal being listed by Gazette.
pub async fn wait_for_epoch_change(
    client: &mut TestKafkaClient,
    topic: &str,
    partition: i32,
    previous_epoch: i32,
    timeout: std::time::Duration,
) -> anyhow::Result<i32> {
    let deadline = std::time::Instant::now() + timeout;

    tracing::info!(
        %topic,
        partition,
        previous_epoch,
        timeout_secs = timeout.as_secs(),
        "Waiting for epoch change and partitions"
    );

    loop {
        match client.metadata(&[topic]).await {
            Ok(metadata) => {
                if let Some(epoch) = metadata_leader_epoch(&metadata, topic, partition) {
                    // Check if partitions exist (not just epoch change)
                    let has_partitions = metadata
                        .topics
                        .iter()
                        .find(|t| t.name.as_ref().map(|n| n.as_str()) == Some(topic))
                        .map(|t| !t.partitions.is_empty())
                        .unwrap_or(false);

                    if epoch > previous_epoch && has_partitions {
                        tracing::info!(
                            %topic,
                            partition,
                            previous_epoch,
                            new_epoch = epoch,
                            "Epoch changed and partitions available"
                        );
                        return Ok(epoch);
                    }
                    tracing::debug!(
                        %topic,
                        partition,
                        current_epoch = epoch,
                        previous_epoch,
                        has_partitions,
                        "Waiting for epoch change and/or partitions"
                    );
                }
            }
            Err(e) => {
                tracing::debug!(error = %e, "Metadata request failed, will retry");
            }
        }

        if std::time::Instant::now() > deadline {
            anyhow::bail!(
                "timeout waiting for epoch to change from {previous_epoch} for {topic}:{partition}"
            );
        }

        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}
