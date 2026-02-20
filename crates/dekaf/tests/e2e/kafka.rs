use anyhow::Context;
use futures::StreamExt;
use rdkafka::Message;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use schema_registry_converter::async_impl::avro::AvroDecoder;
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use std::time::Duration;

pub struct KafkaConsumer {
    consumer: StreamConsumer,
    decoder: AvroDecoder<'static>,
}

#[derive(Debug)]
pub struct DecodedRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: serde_json::Value,
    pub value: serde_json::Value,
}

impl KafkaConsumer {
    pub fn new(broker: &str, registry: &str, username: &str, password: &str) -> Self {
        Self::with_group_id(
            broker,
            registry,
            username,
            password,
            &format!("test-{}", uuid::Uuid::new_v4()),
        )
    }

    pub fn with_group_id(
        broker: &str,
        registry: &str,
        username: &str,
        password: &str,
        group_id: &str,
    ) -> Self {
        let consumer: StreamConsumer = rdkafka::ClientConfig::new()
            .set("bootstrap.servers", broker)
            .set("security.protocol", "SASL_PLAINTEXT")
            .set("sasl.mechanism", "PLAIN")
            .set("sasl.username", username)
            .set("sasl.password", password)
            .set("group.id", group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            // Enable debug logging for broker connections and protocol messages
            .set("debug", "broker,protocol,security,cgrp,fetch")
            .set_log_level(RDKafkaLogLevel::Debug)
            .create()
            .expect("consumer creation failed");

        let sr_settings = SrSettings::new_builder(registry.to_string())
            .set_basic_authorization(username, Some(password))
            .build()
            .expect("schema registry settings failed");

        let decoder = AvroDecoder::new(sr_settings);

        KafkaConsumer { consumer, decoder }
    }

    pub fn subscribe(&self, topics: &[&str]) -> anyhow::Result<()> {
        self.consumer.subscribe(topics)?;
        Ok(())
    }

    /// Fetch all available records until no more arrive within the timeout.
    pub async fn fetch(&self) -> anyhow::Result<Vec<DecodedRecord>> {
        const TIMEOUT: Duration = Duration::from_secs(10);

        let mut records = Vec::new();
        let mut stream = self.consumer.stream();

        loop {
            match tokio::time::timeout(TIMEOUT, stream.next()).await {
                Ok(Some(Ok(msg))) => {
                    let key = self
                        .decoder
                        .decode(msg.key())
                        .await
                        .context("failed to decode key")?;
                    let value = self
                        .decoder
                        .decode(msg.payload())
                        .await
                        .context("failed to decode value")?;

                    records.push(DecodedRecord {
                        topic: msg.topic().to_string(),
                        partition: msg.partition(),
                        offset: msg.offset(),
                        key: apache_avro::from_value(&key.value)?,
                        value: apache_avro::from_value(&value.value)?,
                    });
                }
                Ok(Some(Err(e))) => return Err(e.into()),
                Ok(None) => break,
                Err(_) => break, // timeout, no more records available
            }
        }

        Ok(records)
    }

    /// Fetch exactly N records, committing each as consumed.
    pub async fn fetch_n_with_commit(&self, n: usize) -> anyhow::Result<Vec<DecodedRecord>> {
        const TIMEOUT: Duration = Duration::from_secs(180);
        let deadline = std::time::Instant::now() + TIMEOUT;

        let mut records = Vec::new();
        let mut stream = self.consumer.stream();

        while records.len() < n {
            if std::time::Instant::now() > deadline {
                anyhow::bail!("timeout waiting for {} records, got {}", n, records.len());
            }

            match tokio::time::timeout(Duration::from_secs(30), stream.next()).await {
                Ok(Some(Ok(msg))) => {
                    let key = self
                        .decoder
                        .decode(msg.key())
                        .await
                        .context("failed to decode key")?;
                    let value = self
                        .decoder
                        .decode(msg.payload())
                        .await
                        .context("failed to decode value")?;

                    records.push(DecodedRecord {
                        topic: msg.topic().to_string(),
                        partition: msg.partition(),
                        offset: msg.offset(),
                        key: apache_avro::from_value(&key.value)?,
                        value: apache_avro::from_value(&value.value)?,
                    });

                    self.consumer
                        .commit_message(&msg, rdkafka::consumer::CommitMode::Sync)
                        .context("failed to commit")?;
                }
                Ok(Some(Err(e))) => return Err(e.into()),
                Ok(None) => break,
                Err(_) => continue, // per-message timeout, retry
            }
        }

        Ok(records)
    }

    /// Fetch exactly N records, then commit via `commit_consumer_state`.
    ///
    /// Unlike `fetch_n_with_commit` (which uses `commit_message`), this method
    /// commits through librdkafka's stored-position path: `rd_kafka_commit(rk, NULL, ...)`.
    /// That path compares the stored position's `leader_epoch` (from the RecordBatch)
    /// against the committed position's `leader_epoch` (from OffsetFetch) via
    /// `rd_kafka_fetch_pos_cmp`. If `stored < committed`, the commit is silently
    /// skipped and `NoOffset` is returned.
    pub async fn fetch_n_with_state_commit(&self, n: usize) -> anyhow::Result<Vec<DecodedRecord>> {
        const TIMEOUT: Duration = Duration::from_secs(180);
        let deadline = std::time::Instant::now() + TIMEOUT;

        let mut records = Vec::new();
        let mut stream = self.consumer.stream();

        while records.len() < n {
            if std::time::Instant::now() > deadline {
                anyhow::bail!("timeout waiting for {} records, got {}", n, records.len());
            }

            match tokio::time::timeout(Duration::from_secs(30), stream.next()).await {
                Ok(Some(Ok(msg))) => {
                    let key = self
                        .decoder
                        .decode(msg.key())
                        .await
                        .context("failed to decode key")?;
                    let value = self
                        .decoder
                        .decode(msg.payload())
                        .await
                        .context("failed to decode value")?;

                    records.push(DecodedRecord {
                        topic: msg.topic().to_string(),
                        partition: msg.partition(),
                        offset: msg.offset(),
                        key: apache_avro::from_value(&key.value)?,
                        value: apache_avro::from_value(&value.value)?,
                    });
                }
                Ok(Some(Err(e))) => return Err(e.into()),
                Ok(None) => break,
                Err(_) => continue, // per-message timeout, retry
            }
        }

        // commit_consumer_state uses rd_kafka_commit(rk, NULL, ...) which goes
        // through rd_kafka_fetch_pos_cmp. If the RecordBatch partition_leader_epoch
        // doesn't match the committed_leader_epoch from OffsetFetch, the commit
        // is silently skipped (returning NoOffset). In a real consumer using
        // auto-commit, this skip is silent. We match that behavior here by
        // treating NoOffset as a no-op rather than an error.
        match self
            .consumer
            .commit_consumer_state(rdkafka::consumer::CommitMode::Sync)
        {
            Ok(()) => {}
            Err(rdkafka::error::KafkaError::ConsumerCommit(
                rdkafka::types::RDKafkaErrorCode::NoOffset,
            )) => {
                tracing::warn!(
                    "commit_consumer_state returned NoOffset (commit silently skipped due to epoch mismatch)"
                );
            }
            Err(e) => return Err(e).context("failed to commit consumer state"),
        }

        Ok(records)
    }

    /// Get the inner consumer for advanced operations.
    pub fn inner(&self) -> &StreamConsumer {
        &self.consumer
    }

    /// Commit a specific offset for a topic/partition.
    pub fn commit_offset(&self, topic: &str, partition: i32, offset: i64) -> anyhow::Result<()> {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(topic, partition, Offset::Offset(offset))
            .context("failed to add partition offset")?;
        self.consumer
            .commit(&tpl, CommitMode::Sync)
            .context("failed to commit offset")
    }
}
