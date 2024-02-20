use anyhow::Context;
use futures::TryStreamExt;
use proto_flow::flow;
use uuid::ClockSequence;

struct Partition {
    create_revision: i64,
    mod_revision: i64,
    read: Option<(i64, i64, gazette::journal::ReadDocs)>,
    route: gazette::broker::Route,
    spec: gazette::broker::JournalSpec,
}

struct Topic {
    key: Vec<doc::Extractor>,
    partitions: Vec<Partition>,
    spec: flow::CollectionSpec,
    uuid: doc::Extractor,
}

async fn start_journal_stream(
    client: gazette::journal::Client,
    journal: &str,
    offset: i64,
    proxy: bool,
) -> anyhow::Result<(i64, i64, gazette::journal::ReadDocs)> {
    let (offset, skip_first) = if offset > 0 {
        (offset - 1, true)
    } else {
        (offset, false)
    };

    let stream = client.read_docs(gazette::broker::ReadRequest {
        offset,
        block: true,
        journal: journal
            .spec
            .as_ref()
            .map(|spec| spec.name.clone())
            .unwrap_or_default(),
        begin_mod_time: 0,
        do_not_proxy: false,
        end_offset: 0,
        header: None, // TODO(johnny): attach `journal` route.
        metadata_only: false,
    });

    Ok(())
}

async fn build_record_batch(
    key: &[doc::Extractor],
    next_offset: &mut i64,
    stream: &mut gazette::journal::ReadDocs,
    mut target_bytes: i32,
    timeout: impl std::future::Future<Output = ()>,
    uuid: &doc::Pointer,
    write_head: &mut i64,
) -> anyhow::Result<bytes::Bytes> {
    use kafka_protocol::records::{
        Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    };
    let mut records: Vec<Record> = Vec::new();
    let mut tmp = bytes::BytesMut::new();

    tokio::pin!(timeout);

    while target_bytes > 0 {
        let read = tokio::select! {
            _ = &mut timeout => break,
            read = stream.try_next() => read?.expect("gazette read never returns EOF"),
        };

        let (offset, root) = match read {
            gazette::journal::Read::Doc { offset, root } => (offset, root),
            gazette::journal::Read::Meta(response) => {
                *write_head = response.write_head;
                continue;
            }
        };

        let (producer, clock, flags) = uuid
            .query(root.get())
            .and_then(|node| match node {
                doc::ArchivedNode::String(s) => gazette::uuid::parse_str(s),
                _ => None,
            })
            .with_context(|| {
                format!("document at offset {offset} does not have a valid v1 UUID")
            })?;

        if flags.is_ack() {
            continue;
        }
        let (unix_seconds, _unix_nanos) = clock.to_unix();

        let key = doc::Extractor::extract_all(root.get(), key, &mut tmp);
        let value = serde_json::to_string(&doc::SerPolicy::noop().on(root.get())).unwrap();

        target_bytes -= (key.len() + value.len()) as i32;

        records.push(Record {
            transactional: true,
            control: false,
            partition_leader_epoch: 1,
            producer_id: producer.as_i64(),
            producer_epoch: 1,
            timestamp_type: TimestampType::LogAppend,
            offset,
            sequence: clock.0 as i32,
            timestamp: unix_seconds as i64,
            key: Some(key),
            value: Some(value.into()),
            headers: Default::default(),
        });
        *next_offset = offset + 1;
    }

    let opts = RecordEncodeOptions {
        version: 2,
        compression: Compression::Lz4,
    };

    RecordBatchEncoder::encode(&mut tmp, records.iter(), &opts)
        .expect("record encoding cannot fail");

    tracing::info!(
        first = records[0].offset,
        last = records[records.len() - 1].offset,
        "returning records with offset range"
    );

    Ok(tmp.freeze())
}
