//! Category 2 & 3: Collection Reset Detection Tests
//!
//! These tests verify that Dekaf correctly handles collection resets by:
//! - Returning `FENCED_LEADER_EPOCH` when consumers send stale epochs
//! - Returning `UNKNOWN_LEADER_EPOCH` for future epochs
//! - Returning `end_offset=0` for old epochs via `OffsetForLeaderEpoch`
//!
//! Collection reset increments the binding's `backfill` counter, which Dekaf
//! maps to Kafka's `leader_epoch` (offset by +1 to start at epoch 1).

mod e2e;

use e2e::{
    DekafTestEnv,
    raw_kafka::{
        TestKafkaClient, fetch_current_leader_epoch, fetch_partition_error,
        list_offsets_partition_error, metadata_leader_epoch, offset_for_epoch_result,
        wait_for_epoch_change,
    },
};
use kafka_protocol::ResponseError;
use serde_json::json;
use std::time::Duration;

const FIXTURE: &str = include_str!("e2e/fixtures/basic.flow.yaml");

/// Default timeout for waiting for epoch changes.
/// With local dev stack, SPEC_TTL is 10s. Give some buffer for test reliability.
const EPOCH_CHANGE_TIMEOUT: Duration = Duration::from_secs(30);

/// Test 2.1: Core test - verify `FENCED_LEADER_EPOCH` is returned when consumer sends stale epoch.
///
/// This is the primary collection reset detection mechanism. When a consumer sends a
/// `current_leader_epoch` that is less than Dekaf's current epoch (derived from
/// binding backfill counter), Dekaf returns `FENCED_LEADER_EPOCH`.
///
/// The response also includes the current leader epoch in `current_leader`, allowing
/// the consumer to know what the new epoch is.
#[ignore] // Requires local stack
#[tokio::test]
async fn test_fenced_leader_epoch_on_stale_consumer() -> anyhow::Result<()> {
    e2e::init_tracing();

    let env = DekafTestEnv::setup("fenced_epoch", FIXTURE).await?;
    let info = env.connection_info();

    // Inject initial document so the collection has data
    env.inject_documents("data", vec![json!({"id": "1", "value": "pre-reset"})])
        .await?;

    tracing::info!("Connecting raw Kafka client");
    let mut client =
        TestKafkaClient::connect(&info.broker, &info.username, "test-token-12345").await?;

    // Get initial epoch from metadata
    let metadata = client.metadata(&["test_topic"]).await?;
    let initial_epoch =
        metadata_leader_epoch(&metadata, "test_topic", 0).expect("metadata should have epoch");
    tracing::info!(initial_epoch, "Got initial epoch");

    // Trigger collection reset: disable capture → reset → re-enable
    env.disable_capture().await?;
    env.reset_collection(None).await?;
    env.enable_capture().await?;

    // Wait for capture to be ready
    let capture = env.capture.as_ref().unwrap();
    env.wait_for_primary(capture).await?;

    // Inject a document to trigger lazy journal creation for the new epoch
    tracing::info!("Injecting document to create new journal");
    env.inject_documents("data", vec![json!({"id": "2", "value": "post-reset"})])
        .await?;

    // Wait for Dekaf to pick up the new epoch
    let new_epoch = wait_for_epoch_change(
        &mut client,
        "test_topic",
        0,
        initial_epoch,
        EPOCH_CHANGE_TIMEOUT,
    )
    .await?;
    tracing::info!(new_epoch, "Dekaf picked up new epoch");

    assert!(
        new_epoch > initial_epoch,
        "new epoch ({new_epoch}) should be greater than initial ({initial_epoch})"
    );

    // Now fetch with the OLD (stale) epoch - should get FENCED_LEADER_EPOCH
    tracing::info!(
        stale_epoch = initial_epoch,
        "Fetching with stale epoch (should get FENCED_LEADER_EPOCH)"
    );
    let fetch_resp = client
        .fetch_with_epoch("test_topic", 0, 0, initial_epoch)
        .await?;
    let error = fetch_partition_error(&fetch_resp, "test_topic", 0).expect("should have error");

    assert!(
        error == ResponseError::FencedLeaderEpoch.code(),
        "expected FENCED_LEADER_EPOCH for stale consumer, got error code {error}"
    );

    // Response should include the NEW epoch in current_leader
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

/// Test 2.2: Verify ListOffsets also returns `FENCED_LEADER_EPOCH` for stale epoch.
///
/// Both Fetch and ListOffsets validate the `current_leader_epoch` parameter.
/// This ensures consumers can't get offset information for a stale epoch.
#[ignore] // Requires local stack
#[tokio::test]
async fn test_list_offsets_fenced_epoch() -> anyhow::Result<()> {
    e2e::init_tracing();

    let env = DekafTestEnv::setup("list_offsets_fenced", FIXTURE).await?;
    let info = env.connection_info();

    // Inject document
    env.inject_documents("data", vec![json!({"id": "1", "value": "test"})])
        .await?;

    tracing::info!("Connecting raw Kafka client");
    let mut client =
        TestKafkaClient::connect(&info.broker, &info.username, "test-token-12345").await?;

    // Get initial epoch
    let metadata = client.metadata(&["test_topic"]).await?;
    let initial_epoch =
        metadata_leader_epoch(&metadata, "test_topic", 0).expect("metadata should have epoch");
    tracing::info!(initial_epoch, "Got initial epoch");

    // Verify ListOffsets works with current epoch
    let list_resp = client
        .list_offsets_with_epoch("test_topic", 0, -1, initial_epoch) // -1 = latest
        .await?;
    let error = list_offsets_partition_error(&list_resp, "test_topic", 0);
    assert!(
        error.map_or(false, |s| s == 0),
        "ListOffsets should succeed before reset, got error: {:?}",
        error
    );

    env.disable_capture().await?;
    env.reset_collection(None).await?;
    env.enable_capture().await?;

    let capture = env.capture.as_ref().unwrap();
    env.wait_for_primary(capture).await?;

    // Inject a document to trigger lazy journal creation for the new epoch
    tracing::info!("Injecting document to create new journal");
    env.inject_documents("data", vec![json!({"id": "2", "value": "post-reset"})])
        .await?;

    // Wait for Dekaf to pick up new epoch
    let new_epoch = wait_for_epoch_change(
        &mut client,
        "test_topic",
        0,
        initial_epoch,
        EPOCH_CHANGE_TIMEOUT,
    )
    .await?;
    tracing::info!(new_epoch, "Dekaf picked up new epoch");

    // ListOffsets with stale epoch should get FENCED_LEADER_EPOCH
    tracing::info!(
        stale_epoch = initial_epoch,
        "ListOffsets with stale epoch (should get FENCED_LEADER_EPOCH)"
    );
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

/// Test 2.3: Verify `UNKNOWN_LEADER_EPOCH` for epoch > current.
///
/// When a consumer sends an epoch that is greater than the current epoch,
/// Dekaf returns `UNKNOWN_LEADER_EPOCH`. This shouldn't happen in normal
/// operation but protects against corrupted consumer state.
#[ignore] // Requires local stack
#[tokio::test]
async fn test_unknown_leader_epoch_for_future_epoch() -> anyhow::Result<()> {
    e2e::init_tracing();

    let env = DekafTestEnv::setup("unknown_epoch", FIXTURE).await?;
    let info = env.connection_info();

    // Inject document
    env.inject_documents("data", vec![json!({"id": "1", "value": "test"})])
        .await?;

    tracing::info!("Connecting raw Kafka client");
    let mut client =
        TestKafkaClient::connect(&info.broker, &info.username, "test-token-12345").await?;

    // Get current epoch
    let metadata = client.metadata(&["test_topic"]).await?;
    let current_epoch =
        metadata_leader_epoch(&metadata, "test_topic", 0).expect("metadata should have epoch");
    tracing::info!(current_epoch, "Got current epoch");

    // Fetch with a future epoch (way higher than current)
    let future_epoch = current_epoch + 100;
    tracing::info!(
        future_epoch,
        "Fetching with future epoch (should get UNKNOWN_LEADER_EPOCH)"
    );

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

/// Test 3.1: Verify old epochs return `end_offset=0` via OffsetForLeaderEpoch.
///
/// After receiving `FENCED_LEADER_EPOCH`, consumers call `OffsetForLeaderEpoch`
/// to find the end offset for their old epoch. Dekaf returns `end_offset=0` for
/// old epochs, indicating the consumer should reset to the beginning.
#[ignore] // Requires local stack
#[tokio::test]
async fn test_offset_for_leader_epoch_returns_zero_for_old_epoch() -> anyhow::Result<()> {
    e2e::init_tracing();

    let env = DekafTestEnv::setup("offset_for_old_epoch", FIXTURE).await?;
    let info = env.connection_info();

    // Inject document
    env.inject_documents("data", vec![json!({"id": "1", "value": "test"})])
        .await?;

    tracing::info!("Connecting raw Kafka client");
    let mut client =
        TestKafkaClient::connect(&info.broker, &info.username, "test-token-12345").await?;

    // Get initial epoch
    let metadata = client.metadata(&["test_topic"]).await?;
    let initial_epoch =
        metadata_leader_epoch(&metadata, "test_topic", 0).expect("metadata should have epoch");
    tracing::info!(initial_epoch, "Got initial epoch");

    // Trigger collection reset
    tracing::info!("Starting collection reset sequence");
    env.disable_capture().await?;
    env.reset_collection(None).await?;
    env.enable_capture().await?;

    let capture = env.capture.as_ref().unwrap();
    env.wait_for_primary(capture).await?;

    // Inject a document to trigger lazy journal creation for the new epoch
    tracing::info!("Injecting document to create new journal");
    env.inject_documents("data", vec![json!({"id": "2", "value": "post-reset"})])
        .await?;

    // Wait for Dekaf to pick up new epoch
    let new_epoch = wait_for_epoch_change(
        &mut client,
        "test_topic",
        0,
        initial_epoch,
        EPOCH_CHANGE_TIMEOUT,
    )
    .await?;
    tracing::info!(new_epoch, "Dekaf picked up new epoch");

    // Query OffsetForLeaderEpoch with the OLD epoch
    tracing::info!(
        old_epoch = initial_epoch,
        "Querying OffsetForLeaderEpoch for old epoch"
    );
    let resp = client
        .offset_for_leader_epoch("test_topic", 0, initial_epoch)
        .await?;
    let result = offset_for_epoch_result(&resp, "test_topic", 0).expect("should have result");

    tracing::info!(
        error_code = result.error_code,
        leader_epoch = result.leader_epoch,
        end_offset = result.end_offset,
        "OffsetForLeaderEpoch response"
    );

    // Should succeed (no error)
    assert_eq!(
        result.error_code, 0,
        "OffsetForLeaderEpoch should succeed for old epoch"
    );

    // Old epoch should return end_offset=0 (reset to beginning)
    assert_eq!(
        result.end_offset, 0,
        "old epoch should return end_offset=0 (reset to beginning), got {}",
        result.end_offset
    );

    // Should return the current epoch
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

/// Test 3.2: Verify current epoch returns actual high watermark (not 0).
///
/// When querying `OffsetForLeaderEpoch` for the current epoch, Dekaf should
/// return the actual high watermark, not 0. This ensures that only old epochs
/// trigger reset-to-beginning behavior.
#[ignore] // Requires local stack
#[tokio::test]
async fn test_offset_for_leader_epoch_returns_highwater_for_current() -> anyhow::Result<()> {
    e2e::init_tracing();

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

    tracing::info!("Connecting raw Kafka client");
    let mut client =
        TestKafkaClient::connect(&info.broker, &info.username, "test-token-12345").await?;

    // Get current epoch
    let metadata = client.metadata(&["test_topic"]).await?;
    let current_epoch =
        metadata_leader_epoch(&metadata, "test_topic", 0).expect("metadata should have epoch");
    tracing::info!(current_epoch, "Got current epoch");

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
    tracing::info!(high_watermark, "Got high watermark from ListOffsets");

    // Query OffsetForLeaderEpoch with the CURRENT epoch
    tracing::info!(
        current_epoch,
        "Querying OffsetForLeaderEpoch for current epoch"
    );
    let resp = client
        .offset_for_leader_epoch("test_topic", 0, current_epoch)
        .await?;
    let result = offset_for_epoch_result(&resp, "test_topic", 0).expect("should have result");

    tracing::info!(
        error_code = result.error_code,
        leader_epoch = result.leader_epoch,
        end_offset = result.end_offset,
        "OffsetForLeaderEpoch response"
    );

    // Should succeed
    assert_eq!(
        result.error_code, 0,
        "OffsetForLeaderEpoch should succeed for current epoch"
    );

    // Current epoch should return actual offset, not 0
    assert!(
        result.end_offset > 0,
        "current epoch should return actual offset (> 0), got {}",
        result.end_offset
    );

    // The end_offset should match or be close to the high watermark
    // (they might differ slightly due to timing)
    assert!(
        result.end_offset <= high_watermark,
        "end_offset ({}) should be <= high_watermark ({})",
        result.end_offset,
        high_watermark
    );

    // Should return current epoch
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
