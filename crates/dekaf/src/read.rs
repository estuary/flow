use super::{Collection, Partition};
use anyhow::bail;
use futures::StreamExt;
use gazette::journal::{ReadJsonLine, ReadJsonLines};
use gazette::{broker, journal, uuid};

pub struct Read {
    /// Journal offset to be served by this Read.
    /// (Actual next offset may be larger if a fragment was removed).
    pub(crate) offset: i64,
    /// Most-recent journal write head observed by this Read.
    pub(crate) last_write_head: i64,

    key_ptr: Vec<doc::Pointer>, // Pointers to the document key.
    key_schema: avro::Schema,   // Avro schema when encoding keys.
    key_schema_id: u32,         // Registry ID of the key's schema.
    meta_op_ptr: doc::Pointer,  // Location of document op (currently always `/_meta/op`).
    not_before: uuid::Clock,    // Not before this clock.
    stream: ReadJsonLines,      // Underlying document stream.
    uuid_ptr: doc::Pointer,     // Location of document UUID.
    value_schema: avro::Schema, // Avro schema when encoding values.
    value_schema_id: u32,       // Registry ID of the value's schema.

    // Keep these details around so we can create a new ReadRequest if we need to skip forward
    journal_name: String,
}

impl Read {
    pub fn new(
        client: journal::Client,
        collection: &Collection,
        partition: &Partition,
        offset: i64,
        key_schema_id: u32,
        value_schema_id: u32,
    ) -> Self {
        let (not_before_sec, _) = collection.not_before.to_unix();

        let stream = client.clone().read_json_lines(
            broker::ReadRequest {
                offset,
                block: true,
                journal: partition.spec.name.clone(),
                begin_mod_time: not_before_sec as i64,
                ..Default::default()
            },
            // Each ReadResponse can be up to 130K. Buffer up to ~4MB so that
            // `dekaf` can do lots of useful transcoding work while waiting for
            // network delay of the next fetch request.
            30,
        );

        Self {
            offset,
            last_write_head: offset,

            key_ptr: collection.key_ptr.clone(),
            key_schema: collection.key_schema.clone(),
            key_schema_id,
            meta_op_ptr: doc::Pointer::from_str("/_meta/op"),
            not_before: collection.not_before,
            stream,
            uuid_ptr: collection.uuid_ptr.clone(),
            value_schema: collection.value_schema.clone(),
            value_schema_id,

            journal_name: partition.spec.name.clone(),
        }
    }

    #[tracing::instrument(skip_all,fields(journal_name=self.journal_name))]
    pub async fn next_batch(mut self, target_bytes: usize) -> anyhow::Result<(Self, bytes::Bytes)> {
        use kafka_protocol::records::{
            Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
        };

        let mut records: Vec<Record> = Vec::new();
        let mut records_bytes: usize = 0;

        // We Avro encode into Vec instead of BytesMut because Vec is
        // better optimized for pushing a single byte at a time.
        let mut tmp = Vec::new();
        let mut buf = bytes::BytesMut::new();

        let mut has_had_parsing_error = false;

        while records_bytes < target_bytes {
            let read = match tokio::select! {
                biased; // Attempt to read before yielding.

                read = self.stream.next() => read,

                () = std::future::ready(()), if records_bytes != 0 => {
                    break; // Yield if we have records and the stream isn't ready.
                }
            } {
                None => bail!("blocking gazette client read never returns EOF"),
                Some(resp) => match resp {
                    Ok(data) => Ok(data),
                    Err(err) if err.is_transient() => {
                        tracing::warn!(%err, "Retrying transient read error");
                        // We can retry transient errors just by continuing to poll the stream
                        // TODO: We might have a counter here and give up after a few attempts
                        continue;
                    }
                    Err(err @ gazette::Error::Parsing { .. }) if !has_had_parsing_error => {
                        tracing::debug!(%err, "Ignoring first parse error to skip past partial document");
                        has_had_parsing_error = true;

                        continue;
                    }
                    Err(err @ gazette::Error::Parsing { .. }) => {
                        tracing::warn!(%err, "Got a second parse error, something is wrong");
                        Err(err)
                    }
                    Err(e) => Err(e),
                }?,
            };

            let (root, next_offset) = match read {
                ReadJsonLine::Meta(response) => {
                    self.last_write_head = response.write_head;
                    continue;
                }
                ReadJsonLine::Doc { root, next_offset } => (root, next_offset),
            };
            let Some(doc::ArchivedNode::String(uuid)) = self.uuid_ptr.query(root.get()) else {
                anyhow::bail!(
                    "document at offset {} does not have a valid UUID",
                    self.offset
                );
            };
            let (producer, clock, flags) = gazette::uuid::parse_str(uuid.as_str())?;

            if clock < self.not_before {
                continue;
            }

            // Is this a non-content control document, such as a transaction ACK?
            let is_control = flags.is_ack();
            // Is this a deletion?
            let is_deletion = matches!(
                self.meta_op_ptr.query(root.get()),
                Some(doc::ArchivedNode::String(op)) if op.as_str() == "d",
            );

            tmp.reserve(root.bytes().len()); // Avoid small allocations.
            let (unix_seconds, unix_nanos) = clock.to_unix();

            // Encode the key.
            let key = if is_control {
                None
            } else {
                tmp.push(0);
                tmp.extend(self.key_schema_id.to_be_bytes());
                () = avro::encode_key(&mut tmp, &self.key_schema, root.get(), &self.key_ptr)?;

                records_bytes += tmp.len();
                buf.extend_from_slice(&tmp);
                tmp.clear();
                Some(buf.split().freeze())
            };

            // Encode the value.
            let value = if is_control || is_deletion {
                None
            } else {
                tmp.push(0);
                tmp.extend(self.value_schema_id.to_be_bytes());
                () = avro::encode(&mut tmp, &self.value_schema, root.get())?;

                records_bytes += tmp.len();
                buf.extend_from_slice(&tmp);
                tmp.clear();
                Some(buf.split().freeze())
            };

            self.offset = next_offset;

            // Map documents into a Kafka offset which is their last
            // inclusive byte index within the document.
            //
            // Kafka adds one for its next fetch_offset, and this behavior
            // means its next fetch will be a valid document begin offset.
            //
            // This behavior also lets us subtract one from the journal
            // write head or a fragment end offset to arrive at a
            // logically correct Kafka high water mark which a client
            // can expect to read through.
            //
            // Note that sequence must increment at the same rate
            // as offset for efficient record batch packing.
            let kafka_offset = next_offset - 1;

            if !is_control {
                records.push(Record {
                    control: false,
                    headers: Default::default(),
                    key,
                    offset: kafka_offset,
                    partition_leader_epoch: 1,
                    producer_epoch: 1,
                    producer_id: producer.as_i64(),
                    sequence: kafka_offset as i32,
                    timestamp: unix_seconds as i64 * 1000 + unix_nanos as i64 / 1_000_000, // Map into millis.
                    timestamp_type: TimestampType::LogAppend,
                    transactional: false,
                    value,
                });
            }
        }

        let opts = RecordEncodeOptions {
            compression: Compression::Lz4,
            version: 2,
        };
        RecordBatchEncoder::encode(&mut buf, records.iter(), &opts)
            .expect("record encoding cannot fail");

        tracing::debug!(
            count = records.len(),
            first_offset = records.first().map(|r| r.offset).unwrap_or_default(),
            last_offset = records.last().map(|r| r.offset).unwrap_or_default(),
            last_write_head = self.last_write_head,
            ratio = buf.len() as f64 / (records_bytes + 1) as f64,
            records_bytes,
            "returning records"
        );

        metrics::counter!("documents_read", "journal_name" => self.journal_name.to_owned())
            .increment(records.len() as u64);
        metrics::counter!("bytes_read", "journal_name" => self.journal_name.to_owned())
            .increment(records_bytes as u64);

        Ok((self, buf.freeze()))
    }
}
