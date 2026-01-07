use super::{
    DekafTestEnv,
    raw_kafka::{
        TestKafkaClient, fetch_current_leader_epoch, fetch_partition_error,
        list_offsets_partition_error, metadata_leader_epoch, offset_for_epoch_result,
    },
};
use crate::raw_kafka::{offset_commit_partition_error, offset_fetch_partition_result};
use anyhow::Context;
use kafka_protocol::ResponseError;
use serde_json::json;
use std::time::Duration;

const FIXTURE: &str = include_str!("fixtures/basic.flow.yaml");
const EPOCH_CHANGE_TIMEOUT: Duration = Duration::from_secs(30);

/// When a consumer sends a `current_leader_epoch` that is less than Dekaf's
/// current epoch (derived from binding backfill counter), Dekaf should return `FENCED_LEADER_EPOCH`.
///
/// The response also includes the current leader epoch in `current_leader`, allowing
/// the consumer to know what the new epoch is.
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
    let info = env.connection_info().await?;
    let token = env.dekaf_token()?;
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;

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
    let error = fetch_partition_error(&fetch_resp, "test_topic", 0)
        .expect("partition should exist in fetch response");
    assert_eq!(
        error, 0,
        "fetch should succeed with new epoch, got error code {error}"
    );

    Ok(())
}

/// Verify that ListOffsets returns `FENCED_LEADER_EPOCH` for stale epoch.
///
/// Both Fetch and ListOffsets should validate the `current_leader_epoch` parameter.
#[tokio::test]
async fn test_list_offsets_fenced_epoch() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("list_offsets_fenced", FIXTURE).await?;
    let info = env.connection_info().await?;

    env.inject_documents("data", vec![json!({"id": "1", "value": "test"})])
        .await?;

    // Get initial epoch and verify ListOffsets works before reset
    let token = env.dekaf_token()?;
    let initial_epoch = {
        let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;

        let metadata = client.metadata(&["test_topic"]).await?;
        let epoch =
            metadata_leader_epoch(&metadata, "test_topic", 0).expect("metadata should have epoch");

        // Verify ListOffsets works with current epoch before reset
        let list_resp = client
            .list_offsets_with_epoch("test_topic", 0, -1, epoch) // -1 = latest
            .await?;
        let error = list_offsets_partition_error(&list_resp, "test_topic", 0)
            .expect("partition should exist in ListOffsets response");
        assert_eq!(
            error, 0,
            "ListOffsets should succeed before reset, got error code {error}"
        );

        epoch
    };

    perform_collection_reset(&env, "test_topic", 0, initial_epoch, EPOCH_CHANGE_TIMEOUT).await?;

    let token = env.dekaf_token()?;
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;

    let list_resp = client
        .list_offsets_with_epoch("test_topic", 0, -1, initial_epoch)
        .await?;
    let error =
        list_offsets_partition_error(&list_resp, "test_topic", 0).expect("should have error");

    assert!(
        error == ResponseError::FencedLeaderEpoch.code(),
        "expected FENCED_LEADER_EPOCH for stale epoch in ListOffsets, got error code {error}"
    );

    // Verify ListOffsets succeeds with the NEW epoch
    let new_epoch = get_leader_epoch(&env, "test_topic", 0).await?;
    let list_resp_new = client
        .list_offsets_with_epoch("test_topic", 0, -1, new_epoch)
        .await?;
    let error_new = list_offsets_partition_error(&list_resp_new, "test_topic", 0)
        .expect("partition should exist");
    assert_eq!(
        error_new, 0,
        "ListOffsets should succeed with new epoch, got error code {error_new}"
    );

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
    let info = env.connection_info().await?;

    env.inject_documents("data", vec![json!({"id": "1", "value": "test"})])
        .await?;

    tracing::info!("Connecting raw Kafka client");
    let token = env.dekaf_token()?;
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;

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

    // Verify fetch/list_offsets work with the CURRENT epoch
    let fetch_resp_ok = client
        .fetch_with_epoch("test_topic", 0, 0, current_epoch)
        .await?;
    let error_ok =
        fetch_partition_error(&fetch_resp_ok, "test_topic", 0).expect("partition should exist");
    assert_eq!(
        error_ok, 0,
        "Fetch should succeed with current epoch, got error code {error_ok}"
    );

    let list_resp_ok = client
        .list_offsets_with_epoch("test_topic", 0, -1, current_epoch)
        .await?;
    let list_error_ok = list_offsets_partition_error(&list_resp_ok, "test_topic", 0)
        .expect("partition should exist");
    assert_eq!(
        list_error_ok, 0,
        "ListOffsets should succeed with current epoch, got error code {list_error_ok}"
    );

    Ok(())
}

/// Verify that OffsetForLeaderEpoch returns `end_offset=0` for old epochs.
///
/// After receiving `FENCED_LEADER_EPOCH`, consumers call `OffsetForLeaderEpoch`
/// to find the end offset for their old epoch. Dekaf returns `end_offset=0` for
/// old epochs, indicating the consumer should reset to the beginning.
#[tokio::test]
async fn test_offset_for_leader_epoch_returns_zero_for_old_epoch() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("offset_for_old_epoch", FIXTURE).await?;

    env.inject_documents("data", vec![json!({"id": "1", "value": "test"})])
        .await?;

    let initial_epoch = get_leader_epoch(&env, "test_topic", 0).await?;

    perform_collection_reset(&env, "test_topic", 0, initial_epoch, EPOCH_CHANGE_TIMEOUT).await?;

    let info = env.connection_info().await?;
    let token = env.dekaf_token()?;
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;

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

    Ok(())
}

/// Verify current epoch returns actual high watermark (not 0).
///
/// When querying `OffsetForLeaderEpoch` for the current epoch, Dekaf should
/// return the actual high watermark, not 0. This ensures that only old epochs
/// trigger reset-to-beginning behavior.
#[tokio::test]
async fn test_offset_for_leader_epoch_returns_highwater_for_current() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("offset_for_current", FIXTURE).await?;
    let info = env.connection_info().await?;

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
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;

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

    assert_eq!(
        result.end_offset, high_watermark,
        "current epoch should return exact high_watermark"
    );

    assert_eq!(
        result.leader_epoch, current_epoch,
        "should return current epoch in response"
    );

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
    env.reset_collections().await?;
    env.enable_capture().await?;

    // Wait for capture to be ready
    let capture = env.capture_name().context("no capture in fixture")?;
    env.wait_for_primary(capture, epoch_timeout).await?;

    // Inject a document to trigger lazy journal creation for the new epoch
    tracing::info!("Injecting document to create new journal");
    env.inject_documents(
        "data",
        vec![json!({"id": "reset-trigger", "value": "post-reset"})],
    )
    .await?;

    let info = env.connection_info().await?;
    let token = env.dekaf_token()?;
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;

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
    let info = env.connection_info().await?;
    let token = env.dekaf_token()?;
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;
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

    let mut last_error: Option<String> = None;
    let mut last_epoch: Option<i32> = None;
    let mut last_has_partitions = false;

    loop {
        match client.metadata(&[topic]).await {
            Ok(metadata) => {
                if let Some(epoch) = metadata_leader_epoch(&metadata, topic, partition) {
                    last_epoch = Some(epoch);
                    // Check if partitions exist (not just epoch change)
                    let has_partitions = metadata
                        .topics
                        .iter()
                        .find(|t| t.name.as_ref().map(|n| n.as_str()) == Some(topic))
                        .map(|t| !t.partitions.is_empty())
                        .unwrap_or(false);
                    last_has_partitions = has_partitions;

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
                last_error = Some(e.to_string());
                tracing::debug!(error = %e, "Metadata request failed, will retry");
            }
        }

        if std::time::Instant::now() > deadline {
            let context = match (&last_error, last_epoch) {
                (Some(err), _) => format!("last error: {err}"),
                (None, Some(epoch)) => {
                    format!("last_epoch={epoch}, has_partitions={last_has_partitions}")
                }
                (None, None) => "no metadata received".to_string(),
            };
            anyhow::bail!(
                "timeout waiting for epoch to change from {previous_epoch} for {topic}:{partition} ({context})"
            );
        }

        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

/// Verify Metadata response includes leader_epoch >= 1.
///
/// Dekaf maps the binding's backfill counter to Kafka's leader epoch.
/// Since we add 1 to avoid epoch 0 (which consumers handle poorly),
/// the epoch should always be >= 1.
#[tokio::test]
async fn test_metadata_includes_leader_epoch() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("metadata_epoch", FIXTURE).await?;
    let info = env.connection_info().await?;

    // Inject a document so the topic has data
    env.inject_documents("data", vec![json!({"id": "1", "value": "test"})])
        .await?;

    let token = env.dekaf_token()?;
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;

    let metadata = client.metadata(&["test_topic"]).await?;

    // Extract leader epoch from the partition
    let leader_epoch = metadata_leader_epoch(&metadata, "test_topic", 0)
        .expect("metadata should include leader_epoch");

    // For a fresh collection with no resets, backfill counter is 0, so epoch = 0 + 1 = 1
    assert_eq!(
        leader_epoch, 1,
        "fresh collection should have leader_epoch = 1 (backfill_counter 0 + 1)"
    );

    Ok(())
}

/// Verify ListOffsets response includes leader_epoch.
#[tokio::test]
async fn test_list_offsets_includes_leader_epoch() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("list_offsets_epoch", FIXTURE).await?;
    let info = env.connection_info().await?;

    // Inject documents so offsets exist
    env.inject_documents(
        "data",
        vec![
            json!({"id": "1", "value": "first"}),
            json!({"id": "2", "value": "second"}),
        ],
    )
    .await?;

    let token = env.dekaf_token()?;
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;

    let metadata = client.metadata(&["test_topic"]).await?;
    let current_epoch =
        metadata_leader_epoch(&metadata, "test_topic", 0).expect("metadata should have epoch");

    // Fresh collection should have epoch = 1
    assert_eq!(current_epoch, 1, "fresh collection should have epoch = 1");

    let list_resp = client
        .list_offsets_with_epoch("test_topic", 0, -1, current_epoch)
        .await?;

    let error_code = list_offsets_partition_error(&list_resp, "test_topic", 0)
        .expect("partition should exist in ListOffsets response");
    assert_eq!(
        error_code, 0,
        "ListOffsets should succeed with current epoch, got error code {error_code}"
    );

    // Extract the leader_epoch from response
    let latest_partition = list_resp
        .topics
        .iter()
        .find(|t| t.name.as_str() == "test_topic")
        .and_then(|t| t.partitions.iter().find(|p| p.partition_index == 0))
        .expect("partition should exist");

    assert_eq!(
        latest_partition.leader_epoch, current_epoch,
        "leader_epoch in ListOffsets response should match metadata epoch"
    );

    let earliest_resp = client
        .list_offsets_with_epoch("test_topic", 0, -2, current_epoch)
        .await?;

    let earliest_partition = earliest_resp
        .topics
        .iter()
        .find(|t| t.name.as_str() == "test_topic")
        .and_then(|t| t.partitions.iter().find(|p| p.partition_index == 0))
        .expect("partition should exist");

    assert_eq!(
        earliest_partition.leader_epoch, current_epoch,
        "leader_epoch for earliest offset should match metadata epoch"
    );

    // Earliest offset should be 0
    assert_eq!(
        earliest_partition.offset, 0,
        "earliest offset should be 0, got {}",
        earliest_partition.offset
    );

    // Latest offset should be > 0 after injecting documents
    assert!(
        latest_partition.offset > 0,
        "latest offset should be > 0 after injecting documents, got {}",
        latest_partition.offset
    );

    // Latest should be greater than earliest
    assert!(
        latest_partition.offset > earliest_partition.offset,
        "latest offset ({}) should be > earliest offset ({})",
        latest_partition.offset,
        earliest_partition.offset
    );

    Ok(())
}

/// Verify Fetch response includes leader_epoch in current_leader.
///
/// When fetching data, the response should include the current_leader field
/// with the leader_epoch, allowing consumers to detect epoch changes.
#[tokio::test]
async fn test_fetch_response_includes_leader_epoch() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("fetch_epoch", FIXTURE).await?;
    let info = env.connection_info().await?;

    // Inject documents to fetch
    env.inject_documents(
        "data",
        vec![
            json!({"id": "1", "value": "hello"}),
            json!({"id": "2", "value": "world"}),
        ],
    )
    .await?;

    let token = env.dekaf_token()?;
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;

    let metadata = client.metadata(&["test_topic"]).await?;
    let current_epoch =
        metadata_leader_epoch(&metadata, "test_topic", 0).expect("metadata should have epoch");

    // Fresh collection should have epoch = 1
    assert_eq!(current_epoch, 1, "fresh collection should have epoch = 1");

    let fetch_resp = client
        .fetch_with_epoch("test_topic", 0, 0, current_epoch)
        .await?;

    let error_code = fetch_partition_error(&fetch_resp, "test_topic", 0)
        .expect("partition should exist in fetch response");
    assert_eq!(
        error_code, 0,
        "Fetch should succeed with current epoch, got error code {error_code}"
    );

    // Extract leader_epoch from current_leader
    let response_epoch = fetch_current_leader_epoch(&fetch_resp, "test_topic", 0)
        .expect("Fetch response should include current_leader with leader_epoch");

    assert_eq!(
        response_epoch, current_epoch,
        "Fetch response epoch ({response_epoch}) should match metadata epoch ({current_epoch})"
    );

    let partition = fetch_resp
        .responses
        .iter()
        .find(|t| t.topic.as_str() == "test_topic")
        .and_then(|t| t.partitions.iter().find(|p| p.partition_index == 0))
        .expect("partition should exist");

    assert!(
        partition.high_watermark > 0,
        "high_watermark should be > 0 after injecting documents, got {}",
        partition.high_watermark
    );

    Ok(())
}

/// Commit offset, reset collection. OffsetFetch should NOT return the old offset
#[tokio::test]
async fn test_offset_isolation_after_reset() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("or_epoch_fetch", FIXTURE).await?;

    env.inject_documents("data", vec![json!({"id": "doc1", "value": "test"})])
        .await?;

    let info = env.connection_info().await?;
    let token = env.dekaf_token()?;

    let group_id = format!("test-group-{}", uuid::Uuid::new_v4());
    let commit_offset = 1i64;

    let initial_epoch = get_leader_epoch(&env, "test_topic", 0).await?;

    // Commit an offset and verify it is visible before reset.
    {
        let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;

        let commit_resp = client
            .offset_commit(&group_id, "test_topic", &[(0, commit_offset)])
            .await?;

        let commit_error = offset_commit_partition_error(&commit_resp, "test_topic", 0);
        assert_eq!(commit_error, Some(0), "commit should succeed");

        let fetch_resp = client.offset_fetch(&group_id, "test_topic", &[0]).await?;
        let result = offset_fetch_partition_result(&fetch_resp, "test_topic", 0)
            .expect("response should include requested topic and partition");

        assert_eq!(result.error_code, 0, "fetch should succeed");
        assert_eq!(result.committed_offset, commit_offset);
        assert_eq!(
            result.committed_leader_epoch, initial_epoch,
            "committed_leader_epoch should match initial epoch"
        );
    }

    // Perform collection reset
    perform_collection_reset(&env, "test_topic", 0, initial_epoch, EPOCH_CHANGE_TIMEOUT).await?;

    // OffsetFetch after reset should NOT return the old offset
    {
        let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;
        let fetch_resp = client.offset_fetch(&group_id, "test_topic", &[0]).await?;
        let result = offset_fetch_partition_result(&fetch_resp, "test_topic", 0)
            .expect("response should include requested topic and partition");

        assert_eq!(result.error_code, 0, "fetch should succeed");
        assert_eq!(
            result.committed_offset, -1,
            "offset should not be found after reset (epoch isolation)"
        );
    }

    Ok(())
}
