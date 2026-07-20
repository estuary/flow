use super::DekafTestEnv;
use super::raw_kafka::TestKafkaClient;
use anyhow::Context;
use bytes::Buf;
use kafka_protocol::messages;
use kafka_protocol::records::{Record, RecordBatchDecoder};
use serde_json::json;
use std::time::Duration;

const FIXTURE: &str = include_str!("fixtures/basic.flow.yaml");
const TOPIC: &str = "test_topic";
const PARTITION: i32 = 0;

/// Decode all records, including control records, from a raw FetchResponse.
///
/// Unlike the rdkafka consumer, this surfaces exactly what Dekaf put on the
/// wire: librdkafka silently filters out control records and records below
/// the fetch offset, which masks bugs in which documents a fetch serves.
fn decode_fetch_records(resp: &messages::FetchResponse) -> anyhow::Result<Vec<Record>> {
    let partition = resp
        .responses
        .iter()
        .find(|t| t.topic.as_str() == TOPIC)
        .and_then(|t| t.partitions.iter().find(|p| p.partition_index == PARTITION))
        .context("missing partition in fetch response")?;

    anyhow::ensure!(
        partition.error_code == 0,
        "fetch returned error code {}",
        partition.error_code
    );

    let Some(mut buf) = partition.records.clone() else {
        return Ok(Vec::new());
    };

    let mut records = Vec::new();
    while buf.has_remaining() {
        records.extend(RecordBatchDecoder::decode(&mut buf)?.records);
    }
    Ok(records)
}

async fn fetch_records_at(
    client: &mut TestKafkaClient,
    offset: i64,
) -> anyhow::Result<Vec<Record>> {
    let resp = client
        .fetch_with_epoch(TOPIC, PARTITION, offset, -1)
        .await?;
    decode_fetch_records(&resp)
}

/// Fetch at `offset`, retrying empty responses (e.g. while the server-side
/// read is still starting up) until records arrive.
async fn fetch_records_at_nonempty(
    client: &mut TestKafkaClient,
    offset: i64,
) -> anyhow::Result<Vec<Record>> {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let records = fetch_records_at(client, offset).await?;
        if !records.is_empty() {
            return Ok(records);
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "timed out waiting for a non-empty fetch at offset {offset}"
        );
    }
}

/// Resolve the earliest (-2) or latest (-1) offset via ListOffsets.
async fn resolve_offset(client: &mut TestKafkaClient, timestamp: i64) -> anyhow::Result<i64> {
    let resp = client
        .list_offsets_with_epoch(TOPIC, PARTITION, timestamp, -1)
        .await?;
    let partition = resp
        .topics
        .iter()
        .find(|t| t.name.as_str() == TOPIC)
        .and_then(|t| t.partitions.iter().find(|p| p.partition_index == PARTITION))
        .context("missing partition in ListOffsets response")?;
    anyhow::ensure!(
        partition.error_code == 0,
        "ListOffsets returned error code {}",
        partition.error_code
    );
    Ok(partition.offset)
}

/// Read all records in [low, high), in order, including control records.
async fn read_all_records(
    client: &mut TestKafkaClient,
    low: i64,
    high: i64,
) -> anyhow::Result<Vec<Record>> {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut records: Vec<Record> = Vec::new();

    while records.last().map_or(true, |r| r.offset < high - 1) {
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "timed out reading records through offset {high}"
        );
        let pos = records.last().map_or(low, |r| r.offset + 1);
        records.extend(fetch_records_at(client, pos).await?);
    }
    Ok(records)
}

fn payload(i: usize) -> serde_json::Value {
    json!({"id": format!("doc-{i:02}"), "value": "x".repeat(64)})
}

/// Total `dekaf_readback_reads` recorded for journals of this test's
/// namespace. Namespaces are unique per test, so concurrent tests sharing the
/// Dekaf instance cannot contaminate each other's counts.
async fn readback_reads_for(namespace: &str) -> anyhow::Result<u64> {
    let metrics = super::fetch_dekaf_metrics().await?;

    let mut total = 0;
    for line in metrics.lines() {
        if !line.starts_with("dekaf_readback_reads{") || !line.contains(namespace) {
            continue;
        }
        let value = line
            .rsplit_once(' ')
            .context("malformed metric line")?
            .1
            .parse::<f64>()?;
        total += value as u64;
    }
    Ok(total)
}

/// A fetch at an offset which lands inside a document must serve that
/// document, not skip forward to the next one.
///
/// Dekaf maps each document to the Kafka offset of its final byte, so the
/// document containing a fetch offset is exactly the record that offset
/// addresses. Consumers like SingleStore plan per-shard fetches by
/// numerically dividing the offset space, so mid-document fetch offsets
/// occur routinely, and skipping forward silently drops the document.
#[tokio::test]
async fn test_fetch_mid_document_returns_containing_document() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("fetch_mid_doc", FIXTURE).await?;
    env.inject_documents("data", (0..8).map(payload)).await?;

    let info = env.connection_info().await?;
    let token = env.dekaf_token()?;
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;

    let low = resolve_offset(&mut client, -2).await?;
    let high = resolve_offset(&mut client, -1).await?;
    let all = read_all_records(&mut client, low, high).await?;

    // Target a data record far enough from the write head that this cannot be
    // mistaken for a data-preview fetch.
    let target_index = {
        let data_indices: Vec<usize> = (0..all.len()).filter(|&i| !all[i].control).collect();
        data_indices[data_indices.len() / 2]
    };
    let target_offset = all[target_index].offset;

    assert!(
        high - target_offset >= 13,
        "fetch offset must be far enough from the write head to avoid data-preview detection"
    );

    // A Kafka record's offset is its document's final byte, which is inside
    // the journal document. Fetching there must serve that record first.
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;
    let records = fetch_records_at_nonempty(&mut client, target_offset).await?;

    assert_eq!(records.first().map(|r| r.offset), Some(target_offset));

    // The mid-document fetch is detected by inspecting the byte before the
    // fetch offset, and served by a read which begins with readback.
    assert!(
        readback_reads_for(&env.namespace).await? >= 1,
        "expected the mid-document fetch to record a readback read"
    );

    Ok(())
}

/// A fetch at a document-boundary offset must be served without readback.
///
/// Boundary offsets are the overwhelmingly common case: a sequential consumer
/// always fetches at its last record's offset plus one, which is the next
/// document's begin. Each readback read re-reads up to OFFSET_READBACK
/// (64MB) from the journal only to discard it, so paying it for boundary
/// fetches regressed Dekaf's broker-side read volume by several orders of
/// magnitude on read-heavy workloads.
#[tokio::test]
async fn test_fetch_at_document_boundary_avoids_readback() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("fetch_boundary", FIXTURE).await?;
    env.inject_documents("data", (0..8).map(payload)).await?;

    let info = env.connection_info().await?;
    let token = env.dekaf_token()?;
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;

    let low = resolve_offset(&mut client, -2).await?;
    let high = resolve_offset(&mut client, -1).await?;
    let all = read_all_records(&mut client, low, high).await?;

    // Target the boundary following a data record far enough from the write
    // head that this cannot be mistaken for a data-preview fetch.
    let target_index = {
        let data_indices: Vec<usize> = (0..all.len()).filter(|&i| !all[i].control).collect();
        data_indices[data_indices.len() / 2]
    };
    let boundary_offset = all[target_index].offset + 1;

    assert!(
        high - boundary_offset >= 13,
        "fetch offset must be far enough from the write head to avoid data-preview detection"
    );

    // A record's offset is its document's final byte, so offset + 1 is the
    // begin of the next document: the position a sequential consumer fetches.
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;
    let records = fetch_records_at_nonempty(&mut client, boundary_offset).await?;

    assert_eq!(
        records.first().map(|r| r.offset),
        Some(all[target_index + 1].offset)
    );

    // Every fetch in this test targeted a document boundary (ListOffsets
    // results and record offsets plus one), so none may pay a readback.
    assert_eq!(
        readback_reads_for(&env.namespace).await?,
        0,
        "boundary fetches must not record readback reads"
    );

    Ok(())
}

/// A caught-up consumer polling at the write head must receive no records.
///
/// The final bytes of the journal are the trailing transaction ACK, whose
/// Kafka offset is one before the write head. Reading backwards from the
/// requested offset must not re-serve it: this regressed when readback was
/// first introduced (see 98041fc2fe7), causing consumers to periodically
/// re-read just the last ACK message.
#[tokio::test]
async fn test_fetch_at_write_head_returns_no_records() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("fetch_write_head", FIXTURE).await?;
    env.inject_documents("data", [payload(0)]).await?;

    let info = env.connection_info().await?;
    let token = env.dekaf_token()?;
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;

    let high = resolve_offset(&mut client, -1).await?;
    let records = fetch_records_at(&mut client, high).await?;
    assert!(
        records.is_empty(),
        "fetch at write head {high} must return no records, got offsets {:?}",
        records.iter().map(|r| r.offset).collect::<Vec<_>>(),
    );

    Ok(())
}
