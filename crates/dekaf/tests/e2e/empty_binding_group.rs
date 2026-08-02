use super::DekafTestEnv;
use crate::raw_kafka::{TestKafkaClient, fetch_partition_error, list_offsets_partition_error};
use anyhow::Context;
use futures::StreamExt;
use kafka_protocol::ResponseError;
use rdkafka::consumer::Consumer;
use serde_json::json;
use std::time::Duration;

const TWO_TOPICS_FIXTURE: &str = include_str!("fixtures/two_topics.flow.yaml");
const ALLOW_EMPTY_FIXTURE: &str = include_str!("fixtures/multi_topics_allow_empty.flow.yaml");
const METADATA_TIMEOUT: Duration = Duration::from_secs(30);
const CONSUME_TIMEOUT: Duration = Duration::from_secs(30);

/// Publish the fixture and write two documents to `test_data_a`, leaving every
/// other collection journal-less. Returns the environment once `topic_a`'s
/// metadata is serving, along with each empty topic's metadata error code.
async fn setup_populated_and_empty(
    test_name: &str,
    fixture: &str,
    empty_topics: &[&str],
) -> anyhow::Result<(DekafTestEnv, Vec<i16>)> {
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

    let requested: Vec<&str> = std::iter::once("topic_a")
        .chain(empty_topics.iter().copied())
        .collect();

    let deadline = std::time::Instant::now() + METADATA_TIMEOUT;
    let codes = loop {
        let metadata = client.metadata(&requested).await?;
        let codes: Vec<i16> = requested
            .iter()
            .map(|name| {
                metadata
                    .topics
                    .iter()
                    .find(|t| t.name.as_ref().map(|n| n.as_str()) == Some(*name))
                    .map(|t| t.error_code)
                    .with_context(|| format!("{name} missing from metadata"))
            })
            .collect::<anyhow::Result<_>>()?;

        if codes[0] == 0 || std::time::Instant::now() > deadline {
            break codes;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    assert_eq!(
        codes[0], 0,
        "topic_a has data, so its metadata should be OK"
    );

    Ok((env, codes[1..].to_vec()))
}

/// Consume from a fresh group subscribed to all of `topics`, polling through
/// client-side error events rather than failing on the first one. Returns the
/// number of records read, the count of assigned partitions, and any errors.
async fn consume_topics(
    env: &DekafTestEnv,
    topics: &[&str],
) -> anyhow::Result<(usize, usize, Vec<String>)> {
    let group = format!("mixed-{}", uuid::Uuid::new_v4());
    let consumer = env.kafka_consumer_with_group_id(&group).await?;
    consumer.subscribe(topics)?;

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

    let (env, empty_codes) =
        setup_populated_and_empty("empty_binding_group", TWO_TOPICS_FIXTURE, &["topic_b"]).await?;
    assert_eq!(
        empty_codes,
        vec![ResponseError::LeaderNotAvailable.code()],
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

    let (count, assigned, errors) = consume_topics(&env, &["topic_a", "topic_b"]).await?;
    tracing::info!(count, assigned, ?errors, "mixed subscription");
    assert_eq!(
        count, 0,
        "the empty topic starves the whole subscription (assigned: {assigned}, errors: {errors:?})"
    );

    Ok(())
}

/// With `allow_empty_topics`, unwritten collections are served as valid topics
/// with one partition sitting at offset 0. Several of them in one subscription
/// must all resolve, so the group stabilizes and the populated topic alongside
/// them is consumed normally.
#[tokio::test]
async fn test_allow_empty_topics_unblocks_consumer_group() -> anyhow::Result<()> {
    super::init_tracing();

    let empty_topics = ["topic_b", "topic_c"];
    let (env, empty_codes) =
        setup_populated_and_empty("allow_empty_topics", ALLOW_EMPTY_FIXTURE, &empty_topics).await?;
    assert_eq!(
        empty_codes,
        vec![0; empty_topics.len()],
        "with allow_empty_topics, every unwritten collection is a valid topic"
    );

    let info = env.connection_info().await?;
    let token = env.dekaf_token()?;
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;

    let metadata = client.metadata(&empty_topics).await?;
    for name in empty_topics {
        let topic = metadata
            .topics
            .iter()
            .find(|t| t.name.as_ref().map(|n| n.as_str()) == Some(name))
            .with_context(|| format!("{name} missing from metadata"))?;
        assert_eq!(
            topic.partitions.len(),
            1,
            "empty topic {name} presents a single sentinel partition"
        );

        for timestamp in [-1 /* latest */, -2 /* earliest */] {
            let resp = client
                .list_offsets_with_epoch(name, 0, timestamp, -1)
                .await?;
            assert_eq!(
                list_offsets_partition_error(&resp, name, 0),
                Some(0),
                "ListOffsets({timestamp}) should succeed for empty topic {name}"
            );
            let offset = resp
                .topics
                .iter()
                .find(|t| t.name.as_str() == name)
                .and_then(|t| t.partitions.iter().find(|p| p.partition_index == 0))
                .with_context(|| format!("{name} partition 0 missing from ListOffsets response"))?
                .offset;
            assert_eq!(offset, 0, "empty topic {name} sits at offset 0");
        }

        let fetch_resp = client.fetch_with_epoch(name, 0, 0, -1).await?;
        assert_eq!(
            fetch_partition_error(&fetch_resp, name, 0),
            Some(0),
            "Fetch from empty topic {name} should return an empty batch, not an error"
        );
    }

    let subscription = ["topic_a", "topic_b", "topic_c"];
    let (count, assigned, errors) = consume_topics(&env, &subscription).await?;
    tracing::info!(count, assigned, ?errors, "mixed subscription");
    assert_eq!(
        count, 2,
        "the populated topic is consumable alongside two empty ones \
         (assigned: {assigned}, errors: {errors:?})"
    );
    assert_eq!(
        assigned,
        subscription.len(),
        "every topic's partition is assigned (errors: {errors:?})"
    );

    Ok(())
}
