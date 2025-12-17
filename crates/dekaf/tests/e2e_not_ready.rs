//! Tests for CollectionStatus::NotReady behavior.
//!
//! These tests verify that Dekaf correctly handles the case where a collection
//! binding exists but journals are not yet available. This happens when:
//! - A collection was recently reset and the writer hasn't created journals yet
//! - The collection exists in the control plane but no data has been written
//!
//! In these cases, Dekaf should return `LeaderNotAvailable` to signal clients
//! to retry, rather than returning confusing errors or fake partition data.

mod e2e;

use e2e::{
    DekafTestEnv,
    raw_kafka::{TestKafkaClient, list_offsets_partition_error, metadata_leader_epoch},
};
use kafka_protocol::ResponseError;
use serde_json::json;
use std::time::Duration;

const FIXTURE: &str = include_str!("e2e/fixtures/basic.flow.yaml");

/// Timeout for waiting for Dekaf to see the reset (spec refresh).
const SPEC_REFRESH_TIMEOUT: Duration = Duration::from_secs(30);

/// Test that all partition-aware operations return LeaderNotAvailable when no journals exist.
///
/// After a collection reset, there's a window where:
/// 1. The new partition template exists (spec updated)
/// 2. But the journal hasn't been created yet (created lazily on first write)
///
/// During this window, Dekaf should return LeaderNotAvailable for all operations
/// that require partition data, signaling clients to retry.
///
/// This test verifies: Metadata, ListOffsets, Fetch, and OffsetForLeaderEpoch.
#[tokio::test]
async fn test_all_operations_return_leader_not_available_when_no_journals() -> anyhow::Result<()> {
    e2e::init_tracing();

    let env = DekafTestEnv::setup("not_ready", FIXTURE).await?;
    let info = env.connection_info();

    // Inject initial document so the collection has data and journals exist
    env.inject_documents("data", vec![json!({"id": "1", "value": "initial"})])
        .await?;

    tracing::info!("Connecting raw Kafka client");
    let mut client =
        TestKafkaClient::connect(&info.broker, &info.username, "test-token-12345").await?;

    // Verify all operations work initially
    let metadata = client.metadata(&["test_topic"]).await?;
    let initial_epoch =
        metadata_leader_epoch(&metadata, "test_topic", 0).expect("should have epoch before reset");
    tracing::info!(initial_epoch, "Initial metadata OK");

    let list_resp = client
        .list_offsets_with_epoch("test_topic", 0, -1, initial_epoch)
        .await?;
    assert!(
        list_offsets_partition_error(&list_resp, "test_topic", 0) == Some(0),
        "ListOffsets should succeed before reset"
    );
    tracing::info!("Initial ListOffsets OK");

    let fetch_resp = client
        .fetch_with_epoch("test_topic", 0, 0, initial_epoch)
        .await?;
    assert!(
        e2e::raw_kafka::fetch_partition_error(&fetch_resp, "test_topic", 0) == Some(0),
        "Fetch should succeed before reset"
    );
    tracing::info!("Initial Fetch OK");

    let epoch_resp = client
        .offset_for_leader_epoch("test_topic", 0, initial_epoch)
        .await?;
    assert!(
        e2e::raw_kafka::offset_for_epoch_result(&epoch_resp, "test_topic", 0)
            .map_or(false, |r| r.error_code == 0),
        "OffsetForLeaderEpoch should succeed before reset"
    );
    tracing::info!("Initial OffsetForLeaderEpoch OK");

    // Reset collection without injecting documents afterward - leaves journals uncreated
    tracing::info!("Starting collection reset sequence (without post-reset document injection)");
    env.disable_capture().await?;
    env.reset_collection(None).await?;
    env.enable_capture().await?;

    let capture = env.capture.as_ref().unwrap();
    env.wait_for_primary(capture).await?;

    // Poll until we enter the NotReady state, then verify all operations return LeaderNotAvailable
    tracing::info!("Polling Dekaf for NotReady state (LeaderNotAvailable)");

    let deadline = std::time::Instant::now() + SPEC_REFRESH_TIMEOUT;

    while std::time::Instant::now() < deadline {
        let metadata = client.metadata(&["test_topic"]).await?;

        let topic = metadata
            .topics
            .iter()
            .find(|t| t.name.as_ref().map(|n| n.as_str()) == Some("test_topic"));

        let Some(topic) = topic else {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        };

        // Check if we're in NotReady state (metadata returns LeaderNotAvailable)
        if topic.error_code == ResponseError::LeaderNotAvailable.code() {
            tracing::info!("Metadata returned LeaderNotAvailable");

            // Verify ListOffsets also returns LeaderNotAvailable
            let list_resp = client
                .list_offsets_with_epoch("test_topic", 0, -1, -1)
                .await?;
            let list_error = list_offsets_partition_error(&list_resp, "test_topic", 0);
            assert_eq!(
                list_error,
                Some(ResponseError::LeaderNotAvailable.code()),
                "ListOffsets should return LeaderNotAvailable during NotReady state"
            );
            tracing::info!("ListOffsets returned LeaderNotAvailable");

            // Verify Fetch also returns LeaderNotAvailable
            let fetch_resp = client.fetch_with_epoch("test_topic", 0, 0, -1).await?;
            let fetch_error = e2e::raw_kafka::fetch_partition_error(&fetch_resp, "test_topic", 0);
            assert_eq!(
                fetch_error,
                Some(ResponseError::LeaderNotAvailable.code()),
                "Fetch should return LeaderNotAvailable during NotReady state"
            );
            tracing::info!("Fetch returned LeaderNotAvailable");

            // Verify OffsetForLeaderEpoch also returns LeaderNotAvailable
            let epoch_resp = client.offset_for_leader_epoch("test_topic", 0, 1).await?;
            let epoch_result =
                e2e::raw_kafka::offset_for_epoch_result(&epoch_resp, "test_topic", 0);
            assert!(
                epoch_result.map_or(false, |r| r.error_code
                    == ResponseError::LeaderNotAvailable.code()),
                "OffsetForLeaderEpoch should return LeaderNotAvailable during NotReady state"
            );
            tracing::info!("OffsetForLeaderEpoch returned LeaderNotAvailable");

            break;
        }

        // Check if journals were created before we could test (race condition)
        if let Some(new_epoch) = metadata_leader_epoch(&metadata, "test_topic", 0) {
            if new_epoch > initial_epoch && !topic.partitions.is_empty() {
                anyhow::bail!("Journals were created before we could test NotReady state");
            }
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Verify that after injecting a document (creating journals), operations work again
    tracing::info!("Injecting document to create journals");
    env.inject_documents("data", vec![json!({"id": "2", "value": "post-reset"})])
        .await?;

    // Poll until metadata returns successfully with partitions
    let deadline = std::time::Instant::now() + SPEC_REFRESH_TIMEOUT;
    loop {
        let metadata = client.metadata(&["test_topic"]).await?;
        let topic = metadata
            .topics
            .iter()
            .find(|t| t.name.as_ref().map(|n| n.as_str()) == Some("test_topic"));

        if let Some(topic) = topic {
            if topic.error_code == 0 && !topic.partitions.is_empty() {
                tracing::info!(
                    partitions = topic.partitions.len(),
                    "Metadata returned successfully after journal creation"
                );
                break;
            }
        }

        if std::time::Instant::now() > deadline {
            anyhow::bail!("Timeout waiting for metadata to succeed after journal creation");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    Ok(())
}
