use super::{
    DekafTestEnv,
    raw_kafka::{
        TestKafkaClient, fetch_partition_error, list_offsets_partition_error,
        offset_for_epoch_result,
    },
};
use anyhow::Context;
use kafka_protocol::ResponseError;
use serde_json::json;
use std::time::Duration;

const FIXTURE: &str = include_str!("fixtures/basic.flow.yaml");
const SPEC_REFRESH_TIMEOUT: Duration = Duration::from_secs(30);

/// A freshly-published collection has no partition journals until the capture
/// commits its first document (journals are created lazily by the runtime
/// mapper). During that window dekaf's binding lookup sees an empty partition
/// list and must return LeaderNotAvailable so consumers retry rather than
/// erroring out.
#[tokio::test]
async fn test_all_operations_return_leader_not_available_when_no_journals() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("not_ready", FIXTURE).await?;
    let info = env.connection_info().await?;
    let token = env.dekaf_token()?;
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;

    let leader_not_available = ResponseError::LeaderNotAvailable.code();

    // No documents have been injected yet, so no journals exist. All
    // partition-aware operations should report LeaderNotAvailable.
    let metadata = client.metadata(&["test_topic"]).await?;
    let topic = metadata
        .topics
        .iter()
        .find(|t| t.name.as_ref().map(|n| n.as_str()) == Some("test_topic"))
        .context("test_topic missing from metadata response")?;
    assert_eq!(
        topic.error_code, leader_not_available,
        "Metadata should return LeaderNotAvailable when no journals exist"
    );

    let list_resp = client
        .list_offsets_with_epoch("test_topic", 0, -1, -1)
        .await?;
    assert_eq!(
        list_offsets_partition_error(&list_resp, "test_topic", 0),
        Some(leader_not_available),
        "ListOffsets should return LeaderNotAvailable when no journals exist"
    );

    let fetch_resp = client.fetch_with_epoch("test_topic", 0, 0, -1).await?;
    assert_eq!(
        fetch_partition_error(&fetch_resp, "test_topic", 0),
        Some(leader_not_available),
        "Fetch should return LeaderNotAvailable when no journals exist"
    );

    let epoch_resp = client.offset_for_leader_epoch("test_topic", 0, 1).await?;
    let epoch_result = offset_for_epoch_result(&epoch_resp, "test_topic", 0)
        .context("test_topic partition missing from OffsetForLeaderEpoch response")?;
    assert_eq!(
        epoch_result.error_code, leader_not_available,
        "OffsetForLeaderEpoch should return LeaderNotAvailable when no journals exist"
    );

    // Inject a document. The runtime mapper creates the partition journal on
    // first commit, after which all operations should succeed.
    env.inject_documents("data", vec![json!({"id": "1", "value": "first"})])
        .await?;

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

    let list_resp = client
        .list_offsets_with_epoch("test_topic", 0, -1, -1)
        .await?;
    assert_eq!(
        list_offsets_partition_error(&list_resp, "test_topic", 0),
        Some(0),
        "ListOffsets should succeed once journals exist"
    );

    let fetch_resp = client.fetch_with_epoch("test_topic", 0, 0, -1).await?;
    assert_eq!(
        fetch_partition_error(&fetch_resp, "test_topic", 0),
        Some(0),
        "Fetch should succeed once journals exist"
    );

    Ok(())
}
