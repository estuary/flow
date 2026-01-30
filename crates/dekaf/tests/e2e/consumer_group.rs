use super::DekafTestEnv;
use crate::raw_kafka::{
    TestKafkaClient, offset_commit_partition_error, offset_fetch_partition_result,
};
use anyhow::Context;
use kafka_protocol::{messages::GroupId, protocol::StrBytes};
use serde_json::json;

const FIXTURE: &str = include_str!("fixtures/basic.flow.yaml");
const TWO_TOPICS_FIXTURE: &str = include_str!("fixtures/two_topics.flow.yaml");

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

#[tokio::test]
async fn test_offset_fetch_legacy_fallback() -> anyhow::Result<()> {
    super::init_tracing();
    let env = DekafTestEnv::setup("legacy_fallback", TWO_TOPICS_FIXTURE).await?;
    use kafka_protocol::messages as m;

    let info = env.connection_info();
    let token = env.dekaf_token()?;

    // Ensure both topics exist.
    env.inject_documents("data_a", [json!({"id":"a"})]).await?;
    env.inject_documents("data_b", [json!({"id":"b"})]).await?;

    // Determine the current epoch (binding backfill counter) from Dekaf.
    let mut dekaf_probe = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;
    let list = dekaf_probe
        .list_offsets_with_epoch("topic_a", 0, -1, -1)
        .await?;
    let epoch = list
        .topics
        .iter()
        .find(|t| t.name.as_str() == "topic_a")
        .and_then(|t| t.partitions.iter().find(|p| p.partition_index == 0))
        .context("expected ListOffsets response for topic_a partition 0")?
        .leader_epoch;
    let epoch_u32 = u32::try_from(epoch).context("epoch must be non-negative")?;

    // Build upstream topic names for legacy and epoch-qualified formats.
    let secret = env.dekaf_encryption_secret()?;
    let task_name = env
        .materialization_name()
        .context("fixture must include a materialization")?
        .to_string();
    use dekaf::to_upstream_topic_name;
    use kafka_protocol::messages::TopicName;

    let epoch_a = to_upstream_topic_name(
        TopicName::from(StrBytes::from("topic_a")),
        secret.clone(),
        task_name.clone(),
        Some(epoch_u32),
    );
    let legacy_b = to_upstream_topic_name(
        TopicName::from(StrBytes::from("topic_b")),
        secret.clone(),
        token.clone(),
        None,
    );

    // Commit offsets to upstream in a partially-migrated state:
    // - topic_a has an epoch-qualified committed offset
    // - topic_b only has a legacy committed offset
    let mut upstream = env.upstream_kafka_client().await?;

    upstream
        .ensure_topics(vec![(epoch_a.clone(), 1), (legacy_b.clone(), 1)])
        .await
        .context("failed to ensure upstream topics exist")?;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let group_id = "legacy-cg";

    let commit_header = m::RequestHeader::default()
        .with_request_api_key(m::ApiKey::OffsetCommit as i16)
        .with_request_api_version(6);
    let commit_one = |topic: &m::TopicName, offset: i64| {
        m::OffsetCommitRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str(group_id)))
            .with_generation_id_or_member_epoch(-1)
            .with_member_id(StrBytes::from_static_str(""))
            .with_topics(vec![
                m::offset_commit_request::OffsetCommitRequestTopic::default()
                    .with_name(topic.clone())
                    .with_partitions(vec![
                        m::offset_commit_request::OffsetCommitRequestPartition::default()
                            .with_partition_index(0)
                            .with_committed_offset(offset)
                            .with_committed_leader_epoch(-1),
                    ]),
            ])
    };
    upstream
        .send_request(commit_one(&epoch_a, 5), Some(commit_header.clone()))
        .await?;
    upstream
        .send_request(commit_one(&legacy_b, 7), Some(commit_header.clone()))
        .await?;

    // Single OffsetFetch request for both topics. Current code only falls back
    // if *all* partitions in the response are -1, so topic_b is dropped.
    let mut dekaf = dekaf::KafkaApiClient::connect(
        &[format!("tcp://{}", info.broker)],
        dekaf::KafkaClientAuth::plain(&info.username, &token),
    )
    .await?;
    let fetch_req = m::OffsetFetchRequest::default()
        .with_group_id(m::GroupId::from(StrBytes::from_string(
            group_id.to_string(),
        )))
        .with_topics(Some(vec![
            m::offset_fetch_request::OffsetFetchRequestTopic::default()
                .with_name(m::TopicName::from(StrBytes::from_string(
                    "topic_a".to_string(),
                )))
                .with_partition_indexes(vec![0]),
            m::offset_fetch_request::OffsetFetchRequestTopic::default()
                .with_name(m::TopicName::from(StrBytes::from_string(
                    "topic_b".to_string(),
                )))
                .with_partition_indexes(vec![0]),
        ]));
    let fetch_header = m::RequestHeader::default()
        .with_request_api_key(m::ApiKey::OffsetFetch as i16)
        .with_request_api_version(7);

    let resp = dekaf.send_request(fetch_req, Some(fetch_header)).await?;
    let offset_a = resp
        .topics
        .iter()
        .find(|t| t.name.as_str() == "topic_a")
        .and_then(|t| t.partitions.iter().find(|p| p.partition_index == 0))
        .context("expected OffsetFetch response for topic_a")?
        .committed_offset;
    let offset_b = resp
        .topics
        .iter()
        .find(|t| t.name.as_str() == "topic_b")
        .and_then(|t| t.partitions.iter().find(|p| p.partition_index == 0))
        .context("expected OffsetFetch response for topic_b")?
        .committed_offset;

    assert_eq!(offset_a, 5, "expected topic_a committed offset 5");
    assert_eq!(
        offset_b, 7,
        "expected topic_b to fall back to legacy committed offset 7"
    );

    Ok(())
}
