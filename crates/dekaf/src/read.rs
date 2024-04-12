use super::{Collection, Partition};
use futures::TryStreamExt;
use gazette::journal::{ReadJsonLine, ReadJsonLines};
use gazette::uuid::{Clock, Producer};
use gazette::{broker, journal, uuid};

pub struct Read {
    // Kafka cursor of this Read.
    // This is the "offset" (from kafka's FetchPartition message) that will next be yielded by this Read.
    // It's currently encoded as journal_offset << 1, with the LSB flagging whether to skip the first
    // document at that offset. This works because kafka's FetchPartition offset increments by one
    // from the last-yielded document.
    pub(crate) kafka_cursor: i64,
    // Last reported journal write head.
    pub(crate) last_write_head: i64,

    key_ptr: Vec<doc::Pointer>, // Pointers to the document key.
    key_schema: avro::Schema,   // Avro schema when encoding keys.
    key_schema_id: u32,         // Registry ID of the key's schema.
    not_before: uuid::Clock,    // Not before this clock.
    stream: ReadJsonLines,      // Underlying document stream.
    uuid_ptr: doc::Pointer,     // Location of document UUID.
    value_schema: avro::Schema, // Avro schema when encoding values.
    value_schema_id: u32,       // Registry ID of the value's schema.
}

impl Read {
    pub fn new(
        client: journal::Client,
        collection: &Collection,
        partition: &Partition,
        kafka_cursor: i64,
        key_schema_id: u32,
        value_schema_id: u32,
    ) -> anyhow::Result<Self> {
        let (not_before_sec, _) = collection.not_before.to_unix();

        let lines = client.read_json_lines(broker::ReadRequest {
            offset: kafka_cursor >> 1, // Drop LSB flag to recover its journal offset.
            block: true,
            journal: partition.spec.name.clone(),
            begin_mod_time: not_before_sec as i64,
            ..Default::default()
        });

        Ok(Self {
            kafka_cursor,
            last_write_head: 0,

            key_ptr: collection.key_ptr.clone(),
            key_schema: collection.key_schema.clone(),
            key_schema_id,
            not_before: collection.not_before,
            stream: lines,
            uuid_ptr: collection.uuid_ptr.clone(),
            value_schema: collection.value_schema.clone(),
            value_schema_id,
        })
    }

    async fn next(&mut self) -> Result<(Producer, Clock, doc::OwnedArchivedNode), gazette::Error> {
        loop {
            let read = self
                .stream
                .try_next()
                .await?
                .expect("blocking gazette client read never returns EOF");

            match read {
                ReadJsonLine::Meta(response) => {
                    self.last_write_head = response.write_head;
                }
                ReadJsonLine::Doc { offset, root } => {
                    let Some(uuid) = self.uuid_ptr.query(root.get()).and_then(|node| match node {
                        doc::ArchivedNode::String(s) => Some(s.as_str()),
                        _ => None,
                    }) else {
                        return Err(gazette::Error::Parsing(
                            offset,
                            std::io::Error::other("document does not have a UUID"),
                        ));
                    };
                    let (producer, clock, flags) = gazette::uuid::parse_str(uuid)?;

                    if flags.is_ack() {
                        continue;
                    } else if clock < self.not_before {
                        continue;
                    } else if self.kafka_cursor == offset << 1 | 1 {
                        continue; // LSB flag tells us to skip the document at this offset.
                    }

                    // We expect that a next fetch is for the document _after_ `offset`.
                    self.kafka_cursor = offset << 1 | 1;
                    return Ok((producer, clock, root));
                }
            };
        }
    }
}

pub async fn read_record_batch(
    read: &mut Read,
    target_bytes: usize,
    timeout: impl std::future::Future<Output = ()>,
) -> anyhow::Result<bytes::Bytes> {
    use kafka_protocol::records::{
        Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    };

    tokio::pin!(timeout);

    let mut records: Vec<Record> = Vec::new();
    let mut records_bytes: usize = 0;

    // We Avro encode into Vec instead of BytesMut because Vec is
    // better optimized for pushing a single byte at a time.
    let mut tmp = Vec::new();
    let mut buf = bytes::BytesMut::new();

    while target_bytes > records_bytes {
        let (producer, clock, root) = tokio::select! {
            r = read.next() => r?,
            _ = &mut timeout => break,
        };
        tmp.reserve(root.bytes().len()); // Avoid small allocations.
        let (unix_seconds, unix_nanos) = clock.to_unix();

        // Encode the record key.
        tmp.push(0);
        tmp.extend(read.key_schema_id.to_be_bytes());
        () = avro::encode_key(&mut tmp, &read.key_schema, root.get(), &read.key_ptr)?;

        records_bytes += tmp.len();
        buf.extend_from_slice(&tmp);
        tmp.clear();
        let key = Some(buf.split().freeze());

        // Encode the record value.
        tmp.push(0);
        tmp.extend(read.value_schema_id.to_be_bytes());
        () = avro::encode(&mut tmp, &read.value_schema, root.get())?;

        records_bytes += tmp.len();
        buf.extend_from_slice(&tmp);
        tmp.clear();
        let value = Some(buf.split().freeze());

        // Note that sequence must increment at the same rate
        // as offset for efficient batch packing.
        let offset = read.kafka_cursor & !1;

        records.push(Record {
            control: false,
            headers: Default::default(),
            key,
            offset,
            partition_leader_epoch: 1,
            producer_epoch: 1,
            producer_id: producer.as_i64(),
            sequence: offset as i32,
            timestamp: unix_seconds as i64 * 1000 + unix_nanos as i64 / 1_000_000, // Map into millis.
            timestamp_type: TimestampType::LogAppend,
            transactional: true,
            value,
        });
    }

    let opts = RecordEncodeOptions {
        compression: Compression::Lz4,
        version: 2,
    };
    RecordBatchEncoder::encode(&mut buf, records.iter(), &opts)
        .expect("record encoding cannot fail");

    tracing::debug!(
        count = records.len(),
        first_offset = records.first().map(|r| r.offset >> 1).unwrap_or_default(),
        last_offset = records.last().map(|r| r.offset >> 1).unwrap_or_default(),
        target_bytes,
        records_bytes,
        ratio = buf.len() as f64 / (records_bytes + 1) as f64,
        write_head = read.last_write_head,
        "returning records"
    );

    Ok(buf.freeze())
}
