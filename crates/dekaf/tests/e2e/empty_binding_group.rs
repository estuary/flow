use super::DekafTestEnv;
use crate::raw_kafka::{TestKafkaClient, fetch_partition_error, list_offsets_partition_error};
use anyhow::Context;
use futures::StreamExt;
use kafka_protocol::ResponseError;
use rdkafka::consumer::Consumer;
use serde_json::json;
use std::time::Duration;

const TWO_TOPICS_FIXTURE: &str = include_str!("fixtures/two_topics.flow.yaml");
const ALLOW_EMPTY_FIXTURE: &str = include_str!("fixtures/two_topics_allow_empty.flow.yaml");
const METADATA_TIMEOUT: Duration = Duration::from_secs(30);
const CONSUME_TIMEOUT: Duration = Duration::from_secs(30);

/// Publish the fixture, write two documents to `test_data_a`, and leave
/// `test_data_b` journal-less. Returns the environment once `topic_a`'s
/// metadata is serving, along with `topic_b`'s metadata error code.
async fn setup_one_populated_one_empty(
    test_name: &str,
    fixture: &str,
) -> anyhow::Result<(DekafTestEnv, i16)> {
    let env = DekafTestEnv::setup(test_name, fixture).await?;

    // A shard can briefly report primary before its proxy listener accepts, so retry.
    let docs = vec![
        json!({"id": "a1", "value": "1"}),
        json!({"id": "a2", "value": "2"}),
    ];
    let mut injected = Err(anyhow::anyhow!("not attempted"));
    for _ in 0..10 {
        injected = env.inject_documents("data_a", docs.clone()).await;
        if injected.is_ok() {
            break;
        }
        tracing::warn!(error = ?injected, "inject failed; retrying");
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
    injected?;

    let info = env.connection_info().await?;
    let token = env.dekaf_token()?;
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;

    let deadline = std::time::Instant::now() + METADATA_TIMEOUT;
    let (err_a, err_b) = loop {
        let metadata = client.metadata(&["topic_a", "topic_b"]).await?;
        let code = |name: &str| {
            metadata
                .topics
                .iter()
                .find(|t| t.name.as_ref().map(|n| n.as_str()) == Some(name))
                .map(|t| t.error_code)
        };
        let (a, b) = (
            code("topic_a").context("topic_a missing from metadata")?,
            code("topic_b").context("topic_b missing from metadata")?,
        );
        if a == 0 || std::time::Instant::now() > deadline {
            break (a, b);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    assert_eq!(err_a, 0, "topic_a has data, so its metadata should be OK");

    Ok((env, err_b))
}

/// Consume from a fresh group subscribed to both topics, polling through
/// client-side error events rather than failing on the first one. Returns the
/// number of records read and the count of assigned partitions.
async fn consume_both_topics(env: &DekafTestEnv) -> anyhow::Result<(usize, usize, Vec<String>)> {
    let group = format!("mixed-{}", uuid::Uuid::new_v4());
    let consumer = env.kafka_consumer_with_group_id(&group).await?;
    consumer.subscribe(&["topic_a", "topic_b"])?;

    let deadline = std::time::Instant::now() + CONSUME_TIMEOUT;
    let (mut count, mut errors) = (0, Vec::new());
    let mut stream = consumer.inner().stream();
    while std::time::Instant::now() < deadline && count < 2 {
        match tokio::time::timeout(Duration::from_secs(2), stream.next()).await {
            Ok(Some(Ok(_))) => count += 1,
            Ok(Some(Err(e))) => errors.push(e.to_string()),
            Ok(None) => break,
            Err(_) => continue,
        }
    }
    drop(stream);

    let assigned = consumer
        .inner()
        .assignment()
        .map(|tpl| tpl.count())
        .unwrap_or(0);

    Ok((count, assigned, errors))
}

/// https://github.com/estuary/flow/issues/3064: by default, a binding whose
/// collection has never been written has no journals, so dekaf reports the
/// retryable `LeaderNotAvailable` for its topic. A subscription mixing such a
/// topic with a populated one is assigned nothing and consumes nothing — not
/// even from the populated topic.
#[tokio::test]
async fn test_empty_binding_blocks_consumer_group() -> anyhow::Result<()> {
    super::init_tracing();

    let (env, err_b) =
        setup_one_populated_one_empty("empty_binding_group", TWO_TOPICS_FIXTURE).await?;
    assert_eq!(
        err_b,
        ResponseError::LeaderNotAvailable.code(),
        "without allow_empty_topics, an unwritten collection reports LeaderNotAvailable"
    );

    // Control: a group subscribed only to the populated topic consumes fine.
    let group = format!("populated-only-{}", uuid::Uuid::new_v4());
    let consumer = env.kafka_consumer_with_group_id(&group).await?;
    consumer.subscribe(&["topic_a"])?;
    assert_eq!(
        consumer.fetch().await?.len(),
        2,
        "subscription to the populated topic alone should consume both documents"
    );

    let (count, assigned, errors) = consume_both_topics(&env).await?;
    tracing::info!(count, assigned, ?errors, "mixed subscription");
    assert_eq!(
        count, 0,
        "the empty topic starves the whole subscription (assigned: {assigned}, errors: {errors:?})"
    );

    Ok(())
}

/// With `allow_empty_topics`, the same unwritten collection is served as a
/// valid topic with one partition sitting at offset 0: the group stabilizes and
/// the populated topic in the subscription is consumed normally.
#[tokio::test]
async fn test_allow_empty_topics_unblocks_consumer_group() -> anyhow::Result<()> {
    super::init_tracing();

    let (env, err_b) =
        setup_one_populated_one_empty("allow_empty_topics", ALLOW_EMPTY_FIXTURE).await?;
    assert_eq!(
        err_b, 0,
        "with allow_empty_topics, an unwritten collection is a valid topic"
    );

    let info = env.connection_info().await?;
    let token = env.dekaf_token()?;
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;

    let metadata = client.metadata(&["topic_b"]).await?;
    let topic_b = metadata
        .topics
        .iter()
        .find(|t| t.name.as_ref().map(|n| n.as_str()) == Some("topic_b"))
        .context("topic_b missing from metadata")?;
    assert_eq!(
        topic_b.partitions.len(),
        1,
        "an empty topic presents a single sentinel partition"
    );

    for timestamp in [-1 /* latest */, -2 /* earliest */] {
        let resp = client
            .list_offsets_with_epoch("topic_b", 0, timestamp, -1)
            .await?;
        assert_eq!(
            list_offsets_partition_error(&resp, "topic_b", 0),
            Some(0),
            "ListOffsets({timestamp}) should succeed for an empty topic"
        );
        let offset = resp
            .topics
            .iter()
            .find(|t| t.name.as_str() == "topic_b")
            .and_then(|t| t.partitions.iter().find(|p| p.partition_index == 0))
            .context("topic_b partition 0 missing from ListOffsets response")?
            .offset;
        assert_eq!(offset, 0, "an empty topic sits at offset 0");
    }

    let fetch_resp = client.fetch_with_epoch("topic_b", 0, 0, -1).await?;
    assert_eq!(
        fetch_partition_error(&fetch_resp, "topic_b", 0),
        Some(0),
        "Fetch from an empty topic should return an empty batch, not an error"
    );

    let (count, assigned, errors) = consume_both_topics(&env).await?;
    tracing::info!(count, assigned, ?errors, "mixed subscription");
    assert_eq!(
        count, 2,
        "the populated topic is consumable alongside the empty one \
         (assigned: {assigned}, errors: {errors:?})"
    );
    assert_eq!(
        assigned, 2,
        "both topics' partitions are assigned (errors: {errors:?})"
    );

    Ok(())
}
