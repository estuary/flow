use super::DekafTestEnv;
use crate::raw_kafka::{
    TestKafkaClient, offset_commit_partition_error, offset_fetch_partition_result,
};
use serde_json::json;

const FIXTURE: &str = include_str!("fixtures/basic.flow.yaml");

/// Consume messages and commit an offset. Verify the committed offset is readable,
/// and that a new consumer with the same group.id resumes from that position.
#[tokio::test]
async fn test_offset_commit_and_resume() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("cg_commit_resume", FIXTURE).await?;

    env.inject_documents(
        "data",
        vec![
            json!({"id": "first", "value": "1"}),
            json!({"id": "second", "value": "2"}),
        ],
    )
    .await?;

    let group_id = format!("test-group-{}", uuid::Uuid::new_v4());

    // First consumer: consume, commit, and read back committed offset
    {
        let consumer = env.kafka_consumer_with_group_id(&group_id)?;
        consumer.subscribe(&["test_topic"])?;

        let records = consumer.fetch().await?;
        assert_eq!(records.len(), 2, "first consumer should see all messages");

        // Commit offset after the last record (offset + 1 is next to read)
        let last_record = records.last().unwrap();
        let offset = last_record.offset + 1;
        consumer.commit_offset("test_topic", last_record.partition, offset)?;

        let readback = consumer.committed_offset("test_topic", last_record.partition)?;
        assert_eq!(readback, Some(offset), "committed offset should match");
        offset
    };

    // Inject more data
    env.inject_documents("data", vec![json!({"id": "third", "value": "3"})])
        .await?;

    // Second consumer with same group: should resume from committed position
    {
        let consumer = env.kafka_consumer_with_group_id(&group_id)?;
        consumer.subscribe(&["test_topic"])?;

        let records = consumer.fetch().await?;
        assert_eq!(
            records.len(),
            1,
            "second consumer should only see new message"
        );
        assert_eq!(records[0].value["id"], "third");
    }

    Ok(())
}

/// Query metadata for a topic that doesn't exist in the materialization.
/// Dekaf should return UnknownTopicOrPartition error code.
#[tokio::test]
async fn test_unknown_topic_returns_error() -> anyhow::Result<()> {
    use kafka_protocol::ResponseError;

    super::init_tracing();

    let env = DekafTestEnv::setup("cg_unknown_topic", FIXTURE).await?;
    let info = env.connection_info();
    let token = env.dekaf_token()?;

    let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;

    // Request metadata for a topic that doesn't exist in the materialization
    let metadata = client.metadata(&["nonexistent_topic"]).await?;

    let topic = metadata
        .topics
        .iter()
        .find(|t| t.name.as_ref().map(|n| n.as_str()) == Some("nonexistent_topic"))
        .expect("response should include requested topic");

    assert_eq!(
        topic.error_code,
        ResponseError::UnknownTopicOrPartition.code(),
        "unknown topic should return UnknownTopicOrPartition error"
    );
    assert!(
        topic.partitions.is_empty(),
        "unknown topic should have no partitions"
    );

    Ok(())
}
/// OffsetFetch for a group that never committed returns committed_offset = -1
/// and committed_leader_epoch = -1.
#[tokio::test]
async fn test_offset_fetch_no_committed_offset() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("om_no_commit", FIXTURE).await?;

    env.inject_documents("data", vec![json!({"id": "doc1", "value": "test"})])
        .await?;

    let info = env.connection_info();
    let token = env.dekaf_token()?;

    let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;

    // Use a group that has never committed
    let group_id = format!("never-committed-{}", uuid::Uuid::new_v4());

    let fetch_resp = client.offset_fetch(&group_id, "test_topic", &[0]).await?;

    let result =
        offset_fetch_partition_result(&fetch_resp, "test_topic", 0).expect("should have result");

    assert_eq!(result.error_code, 0, "fetch should succeed");
    assert_eq!(
        result.committed_offset, -1,
        "committed_offset should be -1 for never-committed group"
    );
    assert_eq!(
        result.committed_leader_epoch, -1,
        "committed_leader_epoch should be -1 for never-committed group"
    );

    Ok(())
}

/// OffsetCommit for a topic not in the materialization returns UnknownTopicOrPartition.
#[tokio::test]
async fn test_offset_commit_unknown_topic() -> anyhow::Result<()> {
    use kafka_protocol::ResponseError;

    super::init_tracing();

    let env = DekafTestEnv::setup("om_unknown_topic", FIXTURE).await?;

    let info = env.connection_info();
    let token = env.dekaf_token()?;

    let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;

    let group_id = format!("test-group-{}", uuid::Uuid::new_v4());

    // Try to commit to a topic that doesn't exist
    let commit_resp = client
        .offset_commit(&group_id, "nonexistent_topic", &[(0, 100)])
        .await?;

    let error = offset_commit_partition_error(&commit_resp, "nonexistent_topic", 0)
        .expect("response should include requested topic and partition");

    assert_eq!(
        error,
        ResponseError::UnknownTopicOrPartition.code(),
        "commit to unknown topic should return UnknownTopicOrPartition"
    );

    Ok(())
}
