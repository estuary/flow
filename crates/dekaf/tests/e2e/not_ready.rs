use super::{
    DekafTestEnv,
    raw_kafka::{TestKafkaClient, list_offsets_partition_error, metadata_leader_epoch},
};
use kafka_protocol::ResponseError;
use serde_json::json;
use std::time::Duration;

const FIXTURE: &str = include_str!("fixtures/basic.flow.yaml");
const SPEC_REFRESH_TIMEOUT: Duration = Duration::from_secs(30);

/// Test that all partition-aware operations return LeaderNotAvailable when no journals exist.
///
/// After a collection reset, there's a window where the new partition template exists
/// but the journal hasn't been created yet. Dekaf should return LeaderNotAvailable
/// for all operations that require partition data, signaling clients to retry.
#[tokio::test]
async fn test_all_operations_return_leader_not_available_when_no_journals() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("not_ready", FIXTURE).await?;
    let info = env.connection_info().await?;

    env.inject_documents("data", vec![json!({"id": "1", "value": "initial"})])
        .await?;

    let token = env.dekaf_token()?;
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;

    // Verify all operations work initially
    let metadata = client.metadata(&["test_topic"]).await?;
    let initial_epoch =
        metadata_leader_epoch(&metadata, "test_topic", 0).expect("should have epoch before reset");

    let list_resp = client
        .list_offsets_with_epoch("test_topic", 0, -1, initial_epoch)
        .await?;
    assert!(
        list_offsets_partition_error(&list_resp, "test_topic", 0) == Some(0),
        "ListOffsets should succeed before reset"
    );

    let fetch_resp = client
        .fetch_with_epoch("test_topic", 0, 0, initial_epoch)
        .await?;
    assert!(
        super::raw_kafka::fetch_partition_error(&fetch_resp, "test_topic", 0) == Some(0),
        "Fetch should succeed before reset"
    );

    let epoch_resp = client
        .offset_for_leader_epoch("test_topic", 0, initial_epoch)
        .await?;
    assert!(
        super::raw_kafka::offset_for_epoch_result(&epoch_resp, "test_topic", 0)
            .map_or(false, |r| r.error_code == 0),
        "OffsetForLeaderEpoch should succeed before reset"
    );

    // Reset collection without injecting documents afterward - leaves journals uncreated
    env.disable_capture().await?;
    env.reset_collections().await?;
    env.enable_capture().await?;

    let capture = env.capture_name().unwrap();
    env.wait_for_primary(capture).await?;

    // Poll until we enter the NotReady state
    let deadline = std::time::Instant::now() + SPEC_REFRESH_TIMEOUT;
    let mut observed_not_ready = false;

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
            observed_not_ready = true;

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

            // Verify Fetch also returns LeaderNotAvailable
            let fetch_resp = client.fetch_with_epoch("test_topic", 0, 0, -1).await?;
            let fetch_error = super::raw_kafka::fetch_partition_error(&fetch_resp, "test_topic", 0);
            assert_eq!(
                fetch_error,
                Some(ResponseError::LeaderNotAvailable.code()),
                "Fetch should return LeaderNotAvailable during NotReady state"
            );

            // Verify OffsetForLeaderEpoch also returns LeaderNotAvailable
            let epoch_resp = client.offset_for_leader_epoch("test_topic", 0, 1).await?;
            let epoch_result =
                super::raw_kafka::offset_for_epoch_result(&epoch_resp, "test_topic", 0);
            assert!(
                epoch_result.map_or(false, |r| r.error_code
                    == ResponseError::LeaderNotAvailable.code()),
                "OffsetForLeaderEpoch should return LeaderNotAvailable during NotReady state"
            );

            break;
        }

        // Check if journals were created before we could test
        if let Some(new_epoch) = metadata_leader_epoch(&metadata, "test_topic", 0) {
            if new_epoch > initial_epoch && !topic.partitions.is_empty() {
                anyhow::bail!("Journals were created before we could test NotReady state");
            }
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    assert!(
        observed_not_ready,
        "Test never observed NotReady state within timeout - unable to verify LeaderNotAvailable behavior"
    );

    // Verify that after injecting a document (creating journals), operations work again
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
