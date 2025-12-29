//! Thin wrapper around rdkafka with Avro decoding for E2E tests.

use anyhow::Context;
use futures::StreamExt;
use rdkafka::Message;
use rdkafka::consumer::{Consumer, StreamConsumer};
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

impl DecodedRecord {
    /// Extract key and value for snapshotting (excludes non-deterministic fields).
    pub fn snapshot_value(&self) -> serde_json::Value {
        serde_json::json!({
            "key": self.key,
            "value": self.value,
        })
    }
}

/// Convert records to a snapshot-friendly format.
pub fn snapshot_records(records: &[DecodedRecord]) -> Vec<serde_json::Value> {
    records.iter().map(|r| r.snapshot_value()).collect()
}

impl KafkaConsumer {
    pub fn new(broker: &str, registry: &str, username: &str, password: &str) -> Self {
        let consumer: StreamConsumer = rdkafka::ClientConfig::new()
            .set("bootstrap.servers", broker)
            .set("security.protocol", "SASL_PLAINTEXT")
            .set("sasl.mechanism", "PLAIN")
            .set("sasl.username", username)
            .set("sasl.password", password)
            .set("group.id", &format!("test-{}", uuid::Uuid::new_v4()))
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
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

    /// Get the inner consumer for advanced operations.
    pub fn inner(&self) -> &StreamConsumer {
        &self.consumer
    }
}
