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

/// Returns the begin offset of the record at `index`: one past the previous
/// record's (final-byte) offset, or `low` for the first record.
fn record_begin(records: &[Record], index: usize, low: i64) -> i64 {
    if index == 0 {
        low
    } else {
        records[index - 1].offset + 1
    }
}

fn payload(i: usize) -> serde_json::Value {
    json!({"id": format!("doc-{i:02}"), "value": "x".repeat(64)})
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

    // Target a data (non-control) record near the middle of the journal,
    // far enough from the write head to not look like a data-preview fetch.
    let target_index = {
        let data_indices: Vec<usize> = (0..all.len()).filter(|&i| !all[i].control).collect();
        data_indices[data_indices.len() / 2]
    };
    let target = &all[target_index];
    let begin = record_begin(&all, target_index, low);

    assert!(
        target.offset - begin >= 2,
        "target document must span at least 3 bytes"
    );
    assert!(
        high - begin > 13,
        "fetch offsets must be far enough from the write head to avoid data-preview detection"
    );

    // A fetch at the document's begin offset (the aligned case), at an offset
    // strictly inside the document, and at the document's own final-byte
    // offset must each serve the document itself as the first record.
    for fetch_offset in [begin, begin + 1, target.offset] {
        let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;
        let records = fetch_records_at_nonempty(&mut client, fetch_offset).await?;

        assert_eq!(
            records.first().map(|r| r.offset),
            Some(target.offset),
            "fetch at {fetch_offset} must first serve the document spanning [{begin}, {}]",
            target.offset,
        );
        assert_eq!(
            records.first().and_then(|r| r.value.as_ref()),
            target.value.as_ref(),
            "fetch at {fetch_offset} served a record with unexpected contents"
        );
        assert!(
            records.iter().all(|r| r.offset >= fetch_offset),
            "fetch at {fetch_offset} must not serve records at earlier offsets, got {:?}",
            records.iter().map(|r| r.offset).collect::<Vec<_>>(),
        );
    }

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
    env.inject_documents("data", (0..3).map(payload)).await?;

    let info = env.connection_info().await?;
    let token = env.dekaf_token()?;
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;

    let low = resolve_offset(&mut client, -2).await?;
    let high = resolve_offset(&mut client, -1).await?;

    // Consume through the write head, proving the read is fully caught up
    // (and not merely still warming up) before polling at `high`.
    let all = read_all_records(&mut client, low, high).await?;
    assert_eq!(all.last().map(|r| r.offset), Some(high - 1));

    // Poll at the write head several times, on this session and on a fresh
    // one (which starts a brand-new server-side read). Every response must
    // be empty: notably, none may contain the trailing ACK at `high - 1`.
    let mut fresh = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;
    for (label, client) in [("caught-up", &mut client), ("fresh", &mut fresh)] {
        for attempt in 0..3 {
            let records = fetch_records_at(client, high).await?;
            assert!(
                records.is_empty(),
                "{label} session, attempt {attempt}: fetch at write head {high} must return \
                 no records, got offsets {:?}",
                records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            );
        }
    }

    Ok(())
}

/// Consumer shards which numerically partition the offset space must
/// collectively read every document exactly once.
///
/// This mirrors how SingleStore plans pipeline batches: each shard fetches
/// from its range's start offset (usually mid-document) and discards records
/// whose offsets fall outside the planned range. A document spanning a range
/// boundary belongs to the following shard, whose fetch starts inside it.
#[tokio::test]
async fn test_sharded_offset_ranges_read_every_document() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("fetch_sharded", FIXTURE).await?;
    env.inject_documents("data", (0..12).map(payload)).await?;

    let info = env.connection_info().await?;
    let token = env.dekaf_token()?;
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;

    let low = resolve_offset(&mut client, -2).await?;
    let high = resolve_offset(&mut client, -1).await?;
    let all = read_all_records(&mut client, low, high).await?;

    let expected: Vec<i64> = all
        .iter()
        .filter(|r| !r.control)
        .map(|r| r.offset)
        .collect();
    assert_eq!(expected.len(), 12, "all injected documents must be present");

    const SHARDS: i64 = 3;
    let mut collected: Vec<i64> = Vec::new();

    for shard in 0..SHARDS {
        let range_start = low + (high - low) * shard / SHARDS;
        let range_end = low + (high - low) * (shard + 1) / SHARDS;

        // Each shard is a separate connection, as with SingleStore.
        let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        let mut pos = range_start;

        'shard: while pos < range_end {
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "shard {shard} timed out reading range [{range_start}, {range_end})"
            );
            let records = fetch_records_at(&mut client, pos).await?;
            for record in &records {
                if record.offset >= range_end {
                    break 'shard;
                }
                if !record.control {
                    collected.push(record.offset);
                }
            }
            if let Some(last) = records.last() {
                pos = last.offset + 1;
            }
        }
    }

    collected.sort();
    assert_eq!(
        collected, expected,
        "sharded ranges must read every document exactly once, with no gaps or duplicates"
    );

    Ok(())
}
