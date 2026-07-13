use super::DekafTestEnv;
use rdkafka::Message;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use serde_json::json;
use std::time::Duration;

const FIXTURE: &str = include_str!("fixtures/basic.flow.yaml");
const TOPIC: &str = "test_topic";
const PARTITION: i32 = 0;

/// librdkafka must not report partition EOF when a newer high watermark is visible.
///
/// Dekaf caches an empty speculative read with the old high watermark. After
/// ListOffsets observes new data, fetching again at the old watermark must
/// return that data rather than surface the cached response as `PARTITION_EOF`.
#[tokio::test]
async fn test_librdkafka_does_not_report_eof_for_cached_timeout() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("librdkafka_cached_eof", FIXTURE).await?;
    env.inject_documents("data", vec![json!({"id": "before"})])
        .await?;

    let info = env.connection_info().await?;
    let token = env.dekaf_token()?;
    let consumer: StreamConsumer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", &info.broker)
        .set("security.protocol", "SASL_PLAINTEXT")
        .set("sasl.mechanism", "PLAIN")
        .set("sasl.username", &info.username)
        .set("sasl.password", &token)
        .set("group.id", format!("test-{}", uuid::Uuid::new_v4()))
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "true")
        .set("fetch.wait.max.ms", "100")
        .create()?;

    let (_, high) = consumer.fetch_watermarks(TOPIC, PARTITION, Duration::from_secs(10))?;
    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(TOPIC, PARTITION, Offset::Offset(high))?;
    consumer.assign(&assignment)?;

    // Establish librdkafka's EOF state and start Dekaf's next speculative read,
    // then pause before librdkafka can consume that read's response.
    match tokio::time::timeout(Duration::from_secs(10), consumer.recv()).await {
        Ok(Err(KafkaError::PartitionEOF(partition))) if partition == PARTITION => {}
        Ok(event) => anyhow::bail!("expected initial EOF at offset {high}, got {event:?}"),
        Err(_) => anyhow::bail!("timed out waiting for initial EOF at offset {high}"),
    }
    consumer.pause(&assignment)?;

    // Dekaf's already-started read uses fetch.wait.max.ms as its timeout.
    // Once it expires, TimeoutNoData remains cached on this connection.
    tokio::time::sleep(Duration::from_millis(500)).await;

    env.inject_documents("data", vec![json!({"id": "after"})])
        .await?;
    let deadline = std::time::Instant::now() + Duration::from_secs(45);
    let new_high = loop {
        let (_, latest) = consumer.fetch_watermarks(TOPIC, PARTITION, Duration::from_secs(10))?;
        if latest > high {
            break latest;
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "timed out waiting for librdkafka to observe a watermark above {high}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    };

    // Reassignment resets librdkafka's EOF suppression. The cached response
    // must not cause a new EOF now that ListOffsets proved the partition grew.
    consumer.assign(&assignment)?;
    consumer.resume(&assignment)?;
    match tokio::time::timeout(Duration::from_secs(10), consumer.recv()).await {
        Ok(Ok(message)) => assert!(
            message.offset() >= high,
            "expected a record at or after batch start {high}, got offset {}",
            message.offset()
        ),
        Ok(Err(KafkaError::PartitionEOF(partition))) if partition == PARTITION => {
            anyhow::bail!(
                "librdkafka reported PARTITION_EOF at {high} after its watermark API observed {new_high}"
            )
        }
        Ok(event) => anyhow::bail!("expected a record after offset {high}, got {event:?}"),
        Err(_) => anyhow::bail!(
            "timed out waiting for data after watermark advanced from {high} to {new_high}"
        ),
    }

    Ok(())
}
