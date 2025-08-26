use super::{Collection, Partition};
use crate::{
    connector::DeletionMode,
    logging,
    task_manager::{self, TaskStateListener},
    utils, SessionAuthentication,
};
use anyhow::{bail, Context};
use bytes::{Buf, BufMut, BytesMut};
use doc::AsNode;
use futures::StreamExt;
use gazette::journal::{ReadJsonLine, ReadJsonLines};
use gazette::uuid::Clock;
use gazette::{broker, uuid};
use kafka_protocol::records::{Compression, TimestampType};
use lz4_flex::frame::BlockMode;

pub struct Read {
    /// Journal offset to be served by this Read.
    /// (Actual next offset may be larger if a fragment was removed).
    pub(crate) offset: i64,
    /// Most-recent journal write head observed by this Read.
    pub(crate) last_write_head: i64,

    key_ptr: Vec<doc::Pointer>,        // Pointers to the document key.
    key_schema: avro::Schema,          // Avro schema when encoding keys.
    key_schema_id: u32,                // Registry ID of the key's schema.
    meta_op_ptr: doc::Pointer,         // Location of document op (currently always `/_meta/op`).
    not_before: Option<uuid::Clock>,   // Not before this clock.
    not_after: Option<uuid::Clock>,    // Not after this clock.
    stream: ReadJsonLines,             // Underlying document stream.
    stream_exp: std::time::SystemTime, // When the stream's authorization expires.
    listener: task_manager::TaskStateListener, // Provides up-to-date journal clients
    buffer_size: usize,                // How many read chunks to buffer
    uuid_ptr: doc::Pointer,            // Location of document UUID.
    value_schema_id: u32,              // Registry ID of the value's schema.
    extractors: Vec<(avro::Schema, utils::CustomizableExtractor)>, // Projections to apply

    // Keep these details around so we can create a new ReadRequest if we need to skip forward
    journal_name: String,
    partition_template_name: String,
    // Stats are aggregated per collection
    collection_name: String,

    // Include task name in emitted metrics
    task_name: String,

    // Offset before which no documents should be emitted
    offset_start: i64,

    deletes: DeletionMode,

    pub(crate) rewrite_offsets_from: Option<i64>,
}

pub enum BatchResult {
    /// Read some docs, stopped reading because reached target bytes
    TargetExceededBeforeTimeout(bytes::Bytes),
    /// Read some docs, stopped reading because reached timeout
    TimeoutExceededBeforeTarget(bytes::Bytes),
    /// Read no docs, stopped reading because reached timeout
    TimeoutNoData,
    // Read no docs because the journal is suspended
    Suspended,
}

#[derive(Copy, Clone)]
pub enum ReadTarget {
    Bytes(usize),
    Docs(usize),
}

impl Read {
    pub async fn new(
        task_state_listener: task_manager::TaskStateListener,
        collection: &Collection,
        partition: &Partition,
        offset: i64,
        key_schema_id: u32,
        value_schema_id: u32,
        rewrite_offsets_from: Option<i64>,
        auth: &SessionAuthentication,
        buffer_size: usize,
    ) -> anyhow::Result<Self> {
        let partition_template_name = collection
            .spec
            .partition_template
            .as_ref()
            .context("missing partition template")?
            .name
            .as_str();

        let (stream, stream_exp) = Self::new_stream(
            collection.not_before,
            task_state_listener.clone(),
            partition_template_name.to_owned(),
            partition.spec.name.clone(),
            buffer_size,
            offset,
        )
        .await?;

        Ok(Self {
            offset,
            last_write_head: offset,

            key_ptr: collection.key_ptr.clone(),
            key_schema: collection.key_schema.clone(),
            key_schema_id,
            meta_op_ptr: doc::Pointer::from_str("/_meta/op"),
            not_before: collection.not_before,
            not_after: collection.not_after,
            listener: task_state_listener,
            stream: stream,
            stream_exp: stream_exp,
            buffer_size,
            uuid_ptr: collection.uuid_ptr.clone(),
            value_schema_id,
            extractors: collection.extractors.clone(),

            partition_template_name: partition_template_name.to_owned(),
            journal_name: partition.spec.name.clone(),
            collection_name: collection.name.to_owned(),
            task_name: match auth {
                SessionAuthentication::Task(task_auth) => task_auth.task_name.clone(),
                SessionAuthentication::User(user_auth) => {
                    format!("user-auth: {:?}", user_auth.claims.email)
                }
                SessionAuthentication::Redirect { .. } => {
                    bail!("Redirected sessions cannot read data")
                }
            },
            rewrite_offsets_from,
            deletes: auth.deletions(),
            offset_start: offset,
        })
    }

    async fn new_stream(
        not_before: Option<Clock>,
        listener: TaskStateListener,
        partition_template_name: String,
        journal_name: String,
        buffer_size: usize,
        offset_start: i64,
    ) -> anyhow::Result<(ReadJsonLines, std::time::SystemTime)> {
        let (not_before_sec, _) = not_before
            .map(|c: Clock| Clock::to_unix(&c))
            .unwrap_or((0, 0));

        let task_state = listener.get().await?;

        let partitions = match task_state.as_ref() {
            crate::task_manager::TaskState::Authorized { partitions, .. } => partitions,
            crate::task_manager::TaskState::Redirect {
                target_dataplane_fqdn,
                ..
            } => {
                anyhow::bail!("Task has been redirected to {}", target_dataplane_fqdn);
            }
        };

        let (client, claims, _) = partitions
            .to_owned()
            .into_iter()
            .find_map(|(k, v)| {
                if k == partition_template_name {
                    Some(v)
                } else {
                    None
                }
            })
            .context(format!(
                "Collection {} not found in task state listener.",
                partition_template_name,
            ))??;

        Ok((
            client.read_json_lines(
                broker::ReadRequest {
                    offset: offset_start,
                    block: true,
                    journal: journal_name.clone(),
                    begin_mod_time: not_before_sec as i64,
                    ..Default::default()
                },
                // Each ReadResponse can be up to 130K. Buffer up to ~4MB so that
                // `dekaf` can do lots of useful transcoding work while waiting for
                // network delay of the next fetch request.
                buffer_size,
            ),
            std::time::UNIX_EPOCH + std::time::Duration::from_secs(claims.exp),
        ))
    }

    #[tracing::instrument(skip_all,fields(journal_name=self.journal_name))]
    pub async fn next_batch(
        mut self,
        target: ReadTarget,
        timeout: std::time::Duration,
    ) -> anyhow::Result<(Self, BatchResult)> {
        use kafka_protocol::records::{
            Compression, Record, RecordBatchEncoder, RecordEncodeOptions,
        };

        let now = std::time::SystemTime::now();

        if (now + timeout + std::time::Duration::from_secs(30)) > self.stream_exp {
            tracing::debug!("stream auth expired, fetching new token");
            (self.stream, self.stream_exp) = Self::new_stream(
                self.not_before,
                self.listener.clone(),
                self.partition_template_name.clone(),
                self.journal_name.clone(),
                self.buffer_size,
                self.offset,
            )
            .await?;
        }

        let mut records: Vec<Record> = Vec::new();
        let mut stats_bytes: u64 = 0;
        let mut stats_records = 0;
        let mut output_bytes: usize = 0;

        // We Avro encode into Vec instead of BytesMut because Vec is
        // better optimized for pushing a single byte at a time.
        let mut tmp = Vec::new();
        let mut buf = bytes::BytesMut::new();

        // If we happen to get a very long timeout, we want to make sure that
        // we don't exceed the token expiration time
        let capped_timeout = {
            let mut timeout_at = now + timeout;
            if timeout_at > self.stream_exp {
                timeout_at = self.stream_exp;
            }
            tokio::time::Instant::now() + timeout_at.duration_since(now)?
        };

        let timeout = tokio::time::sleep_until(capped_timeout);
        tokio::pin!(timeout);

        let mut did_timeout = false;

        let mut last_source_published_at: Option<Clock> = None;
        while match target {
            ReadTarget::Bytes(target_bytes) => output_bytes < target_bytes,
            ReadTarget::Docs(target_docs) => records.len() < target_docs,
        } {
            let read = match tokio::select! {
                biased; // Attempt to read before yielding.

                read = self.stream.next() => read,

                _ = &mut timeout => {
                    did_timeout = true;
                    break; // Yield if we reach a timeout
                },
            } {
                None => bail!("blocking gazette client read never returns EOF"),
                Some(resp) => match resp {
                    Ok(data @ ReadJsonLine::Meta(_)) => Ok(data),
                    Ok(ReadJsonLine::Doc { root, next_offset }) => match root.get() {
                        doc::heap::ArchivedNode::Object(_) => {
                            Ok(ReadJsonLine::Doc { root, next_offset })
                        }
                        non_object => {
                            tracing::warn!(
                                "skipping past non-object node at offset {}: {:?}",
                                self.offset,
                                non_object.to_debug_json_value()
                            );
                            continue;
                        }
                    },
                    Err(gazette::RetryError { attempt, inner })
                        if inner.is_transient() && attempt < 5 =>
                    {
                        tracing::warn!(error = ?inner, "Retrying transient read error");
                        // We can retry transient errors just by continuing to poll the stream
                        continue;
                    }
                    Err(gazette::RetryError {
                        attempt,
                        inner: err @ gazette::Error::Parsing { .. },
                    }) => {
                        if attempt == 0 {
                            tracing::debug!(%err, "Ignoring first parse error to skip past partial document");
                            continue;
                        } else {
                            tracing::warn!(%err, "Got a second parse error, something is wrong");
                            Err(err)
                        }
                    }
                    Err(gazette::RetryError {
                        inner: gazette::Error::BrokerStatus(broker::Status::Suspended),
                        ..
                    }) => return Ok((self, BatchResult::Suspended)),
                    Err(gazette::RetryError { inner, .. }) => Err(inner),
                }?,
            };

            let (root, next_offset) = match read {
                ReadJsonLine::Meta(response) => {
                    self.last_write_head = response.write_head;
                    // Skip self.offset forward in case we're skipping past a gap
                    self.offset = response.offset;
                    continue;
                }
                ReadJsonLine::Doc { root, next_offset } => (root, next_offset),
            };

            if next_offset < self.offset_start {
                continue;
            }

            let mut record_bytes: usize = 0;

            let Some(doc::ArchivedNode::String(uuid)) = self.uuid_ptr.query(root.get()) else {
                let serialized_doc = root.get().to_debug_json_value();
                anyhow::bail!(
                    "document at offset {} does not have a valid UUID: {:?}",
                    self.offset,
                    serialized_doc
                );
            };
            let (producer, clock, flags) = gazette::uuid::parse_str(uuid.as_str())?;

            // Is this a non-content control document, such as a transaction ACK?
            let is_control = flags.is_ack();

            let should_skip = match (self.not_before, self.not_after) {
                (Some(not_before), Some(not_after)) => clock < not_before || clock > not_after,
                (Some(not_before), None) => clock < not_before,
                (None, Some(not_after)) => clock > not_after,
                (None, None) => false,
            };

            // Only filter non-ack documents to allow the consumer to make and
            // record progress scanning through the offset range.
            if !is_control && should_skip {
                continue;
            }
            last_source_published_at = Some(clock);

            // Is this a deletion?
            let is_deletion = matches!(
                self.meta_op_ptr.query(root.get()),
                Some(doc::ArchivedNode::String(op)) if op.as_str() == "d",
            );

            tmp.reserve(root.bytes().len()); // Avoid small allocations.
            let (unix_seconds, unix_nanos) = clock.to_unix();

            // Encode the key.
            let key = if is_control {
                // From https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
                // Also from https://docs.google.com/document/d/11Jqy_GjUGtdXJK94XGsEIK7CP1SnQGdp2eF0wSw9ra8/edit
                // Control messages will always have a non-null key, which is used to
                // indicate the type of control message type with the following schema:
                //      ControlMessageKey => Version ControlMessageType
                //          Version => int16
                //          ControlMessageType => int16
                // Control messages with version > 0 are entirely ignored:
                // https://github.com/confluentinc/librdkafka/blob/master/src/rdkafka_msgset_reader.c#L777-L824
                // But, we don't want our message to be entirely ignored,
                // we just don't want it to be returned to the client.
                // If we send a valid version 0 control message, with an
                // invalid message type (not 0 or 1), that should do what we want:
                // https://github.com/confluentinc/librdkafka/blob/master/src/rdkafka_msgset_reader.c#L882-L902

                // Control Message keys are always 4 bytes:
                // Version: 0i16
                buf.put_i16(0);
                // ControlMessageType: != 0 or 1 i16
                buf.put_i16(-1);
                record_bytes += buf.len();
                Some(buf.split().freeze())
            } else {
                tmp.push(0);
                tmp.extend(self.key_schema_id.to_be_bytes());
                () = avro::encode_key(&mut tmp, &self.key_schema, root.get(), &self.key_ptr)?;

                record_bytes += tmp.len();
                buf.extend_from_slice(&tmp);
                tmp.clear();
                Some(buf.split().freeze())
            };

            // Encode the value.
            let value =
                if is_control || (is_deletion && matches!(self.deletes, DeletionMode::Kafka)) {
                    None
                } else {
                    tmp.push(0);
                    tmp.extend(self.value_schema_id.to_be_bytes());

                    extract_and_encode(&self.extractors, root.get(), &mut tmp)?;

                    record_bytes += tmp.len();
                    buf.extend_from_slice(&tmp);
                    tmp.clear();
                    Some(buf.split().freeze())
                };

            if !is_control {
                stats_bytes += (next_offset - self.offset) as u64;
                stats_records += 1;
            }
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
            let kafka_offset = if let Some(rewrite_from) = self.rewrite_offsets_from {
                rewrite_from + records.len() as i64
            } else {
                next_offset - 1
            };

            records.push(Record {
                control: is_control,
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
            output_bytes += record_bytes;
        }

        let opts = RecordEncodeOptions {
            compression: Compression::None,
            version: 2,
        };
        RecordBatchEncoder::encode(&mut buf, records.iter(), &opts, Some(compressor))
            .expect("record encoding cannot fail");

        tracing::debug!(
            count = records.len(),
            first_offset = records.first().map(|r| r.offset).unwrap_or_default(),
            last_offset = records.last().map(|r| r.offset).unwrap_or_default(),
            last_write_head = self.last_write_head,
            ratio = buf.len() as f64 / (output_bytes + 1) as f64,
            output_bytes,
            did_timeout,
            "batch complete"
        );

        metrics::counter!(
            "dekaf_documents_read",
            "task_name" => self.task_name.to_owned(),
            "journal_name" => self.journal_name.to_owned()
        )
        .increment(records.len() as u64);
        metrics::counter!(
            "dekaf_bytes_read_in",
            "task_name" => self.task_name.to_owned(),
            "journal_name" => self.journal_name.to_owned()
        )
        .increment(stats_bytes as u64);
        metrics::counter!(
            "dekaf_bytes_read",
            "task_name" => self.task_name.to_owned(),
            "journal_name" => self.journal_name.to_owned()
        )
        .increment(output_bytes as u64);

        // Right: Input documents from journal. Left: Input docs from destination. Out: Right Keys ⋃ Left Keys
        // Dekaf reads docs from journals, so it emits "right". It doesn't do reduction with a destination system,
        // so it does not emit "left". And right now, it does not reduce at all, so "out" is the same as "right".
        logging::get_log_forwarder().map(|f| {
            f.send_stats(
                self.collection_name.to_owned(),
                ops::stats::Binding {
                    right: Some(ops::stats::DocsAndBytes {
                        docs_total: stats_records,
                        bytes_total: stats_bytes,
                    }),
                    out: Some(ops::stats::DocsAndBytes {
                        docs_total: stats_records,
                        bytes_total: stats_bytes,
                    }),
                    left: None,
                    last_source_published_at: last_source_published_at
                        .and_then(|c| c.to_pb_json_timestamp()),
                },
            );
        });

        let frozen = buf.freeze();

        Ok((
            self,
            match (records.len() > 0, did_timeout) {
                (false, true) => BatchResult::TimeoutNoData,
                (true, true) => BatchResult::TimeoutExceededBeforeTarget(frozen),
                (true, false) => BatchResult::TargetExceededBeforeTimeout(frozen),
                (false, false) => {
                    unreachable!("shouldn't be able see no documents, and also not timeout")
                }
            },
        ))
    }
}

fn compressor<Output: BufMut>(
    input: &mut BytesMut,
    output: &mut Output,
    c: Compression,
) -> anyhow::Result<()> {
    match c {
        Compression::None => output.put(input),
        Compression::Lz4 => {
            let mut frame_info = lz4_flex::frame::FrameInfo::default();
            // This breaks Go lz4 decoding
            // frame_info.block_checksums = true;
            frame_info.block_mode = BlockMode::Independent;

            let mut encoder =
                lz4_flex::frame::FrameEncoder::with_frame_info(frame_info, output.writer());

            std::io::copy(&mut input.reader(), &mut encoder)?;

            encoder.finish()?;
        }
        unsupported @ _ => bail!("Unsupported compression type {unsupported:?}"),
    };
    Ok(())
}

/// Handles extracting and avro-encoding a particular field.
/// Note that since avro encoding can happen piecewise, there's never a need to
/// put together the whole extracted document, and instead we can build up the
/// encoded output iteratively
pub fn extract_and_encode<N: doc::AsNode>(
    extractors: &[(avro::Schema, utils::CustomizableExtractor)],
    original: &N,
    buf: &mut Vec<u8>,
) -> anyhow::Result<()> {
    extractors
        .iter()
        .try_fold(buf, |buf, (schema, extractor)| {
            // This is the value extracted from the original doc
            if let Err(e) = match extractor.extract(original) {
                Ok(value) => avro::encode(buf, schema, value),
                Err(default) => avro::encode(buf, schema, &default.into_owned()),
            }
            .context(format!(
                "Extracting field {extractor:#?}, schema: {schema:?}"
            )) {
                let debug_serialized = serde_json::to_string(&original.to_debug_json_value())?;
                tracing::debug!(extractor=?extractor, ?schema, debug_serialized, ?e, "Failed to encode");
                return Err(e);
            }

            Ok::<_, anyhow::Error>(buf)
        })?;

    Ok(())
}
