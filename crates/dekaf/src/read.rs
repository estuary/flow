use super::{Collection, Partition};
use crate::{
    SessionAuthentication,
    connector::DeletionMode,
    logging,
    task_manager::{self, TaskStateListener},
    utils,
};
use anyhow::{Context, bail};
use bytes::{Buf, BufMut, BytesMut};
use futures::StreamExt;
use gazette::journal::{ReadJsonLine, ReadJsonLines};
use gazette::uuid::Clock;
use gazette::{broker, uuid};
use kafka_protocol::records::{Compression, TimestampType};
use lz4_flex::frame::BlockMode;

pub struct Read {
    /// Journal offset to be served by this Read.
    /// (Actual next offset may be larger if a fragment was removed).
    /// If this offset lands inside a document, the underlying journal read
    /// begins up to OFFSET_READBACK bytes earlier, and documents which end
    /// at-or-before this offset are suppressed rather than served.
    pub(crate) offset: i64,
    /// Most-recent journal write head observed by this Read.
    pub(crate) last_write_head: i64,

    key_ptr: Vec<json::Pointer>,       // Pointers to the document key.
    key_schema: avro::Schema,          // Avro schema when encoding keys.
    key_schema_id: u32,                // Registry ID of the key's schema.
    meta_op_ptr: json::Pointer,        // Location of document op (currently always `/_meta/op`).
    not_before: Option<uuid::Clock>,   // Not before this clock.
    not_after: Option<uuid::Clock>,    // Not after this clock.
    stream: ReadJsonLines,             // Underlying document stream.
    stream_exp: std::time::SystemTime, // When the stream's authorization expires.
    listener: task_manager::TaskStateListener, // Provides up-to-date journal clients
    buffer_size: usize,                // How many read chunks to buffer
    uuid_ptr: json::Pointer,           // Location of document UUID.
    value_schema_id: u32,              // Registry ID of the value's schema.
    extractors: Vec<(avro::Schema, utils::CustomizableExtractor)>, // Projections to apply

    // Keep these details around so we can create a new ReadRequest if we need to skip forward
    journal_name: String,
    partition_template_name: String,
    // Stats are aggregated per collection
    collection_name: String,

    // Include task name in emitted metrics
    task_name: String,

    deletes: DeletionMode,

    pub(crate) rewrite_offsets_from: Option<i64>,

    // Leader epoch for this partition, used in RecordBatch headers.
    // Must match the epoch reported in OffsetFetch's committed_leader_epoch
    // so that librdkafka's epoch-aware commit filtering doesn't silently
    // skip offset commits.
    partition_leader_epoch: i32,
}

pub enum BatchResult {
    /// Read some docs, stopped reading because reached target bytes
    TargetExceededBeforeTimeout(bytes::Bytes),
    /// Read some docs, stopped reading because reached timeout
    TimeoutExceededBeforeTarget(bytes::Bytes),
    /// Read no docs, stopped reading because reached timeout
    TimeoutNoData,
    /// Read no docs because the journal is suspended
    Suspended,
    /// The journal no longer exists (was deleted, likely due to collection reset)
    JournalNotFound,
}

#[derive(Copy, Clone)]
pub enum ReadTarget {
    Bytes(usize),
    Docs(usize),
}

/// Dekaf maps each document to the Kafka offset of its final byte, so a
/// consumer may fetch at an offset which lands in the middle of a document,
/// for example when it plans fetches by numerically partitioning the offset
/// space. Serving such a fetch requires reading backwards to reach the
/// containing document, which a read started at the fetch offset would scan
/// past and drop. Reading backwards unconditionally is far too expensive,
/// however: almost all fetches target document boundaries, and each readback
/// re-reads and discards up to this many bytes from the journal. So
/// `stream_start_offset` first inspects the byte before the offset being
/// served to determine whether it's a document boundary; only a mid-document
/// landing begins this many bytes earlier, with `next_batch` suppressing
/// documents which end at-or-before the served offset. It's sized to be
/// larger than the maximum size of a single document (64MB).
const OFFSET_READBACK: i64 = 1 << 26; // 64MB

/// Map an offset to be served into the journal offset at which its read
/// begins, up to OFFSET_READBACK bytes earlier. Non-positive offsets have
/// special broker semantics (-1 reads from the write head) and pass through.
fn readback_offset(offset: i64) -> i64 {
    if offset > 0 {
        (offset - OFFSET_READBACK).max(0)
    } else {
        offset
    }
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

        let task_name = match auth {
            SessionAuthentication::Task(task_auth) => task_auth.task_name.clone(),
            SessionAuthentication::Redirect { .. } => {
                bail!("Redirected sessions cannot read data")
            }
        };

        // Data-preview reads pass a fragment-start offset, which is always a
        // document boundary, and don't require boundary verification.
        let stream_offset = if rewrite_offsets_from.is_none() {
            Self::stream_start_offset(
                &task_state_listener,
                partition_template_name,
                &partition.spec.name,
                &task_name,
                offset,
            )
            .await?
        } else {
            offset
        };

        let (stream, stream_exp) = Self::new_stream(
            collection.not_before,
            task_state_listener.clone(),
            partition_template_name.to_owned(),
            partition.spec.name.clone(),
            buffer_size,
            stream_offset,
        )
        .await?;

        Ok(Self {
            offset,
            last_write_head: offset,

            key_ptr: collection.key_ptr.clone(),
            key_schema: collection.key_schema.clone(),
            key_schema_id,
            meta_op_ptr: json::Pointer::from_str("/_meta/op"),
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
            task_name,
            rewrite_offsets_from,
            deletes: auth.deletions(),
            partition_leader_epoch: collection.binding_backfill_counter as i32,
        })
    }

    /// Fetch the current journal client and authorization expiry for this
    /// partition's template from the task state.
    async fn journal_client(
        listener: &TaskStateListener,
        partition_template_name: &str,
    ) -> anyhow::Result<(gazette::journal::Client, std::time::SystemTime)> {
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
            client,
            std::time::UNIX_EPOCH + std::time::Duration::from_secs(claims.exp),
        ))
    }

    /// Resolve the journal offset at which a stream serving `offset` begins.
    ///
    /// Collection journals are newline-delimited JSON: every document is
    /// written compactly and terminated by exactly one newline, and JSON
    /// forbids unescaped control characters within strings, so a raw newline
    /// byte occurs in journal content only as a document terminator. `offset`
    /// is therefore a document boundary exactly when the preceding byte is a
    /// newline, which a single-byte point read determines directly.
    ///
    /// Any other byte means `offset` lands inside a document — commonly a
    /// fetch addressing a document's final byte, which is the Kafka offset
    /// assigned to the document as a record — and the stream must begin up to
    /// OFFSET_READBACK earlier to serve the containing document, with
    /// `next_batch` suppressing its predecessors.
    ///
    /// If the preceding byte is unavailable — offset zero, at or beyond the
    /// write head, expired from the journal, or the journal is suspended or
    /// deleted — then no containing document can be served regardless, and
    /// the stream begins at `offset`.
    async fn stream_start_offset(
        listener: &TaskStateListener,
        partition_template_name: &str,
        journal_name: &str,
        task_name: &str,
        offset: i64,
    ) -> anyhow::Result<i64> {
        if offset <= 0 {
            return Ok(offset);
        }

        let (client, _exp) = Self::journal_client(listener, partition_template_name).await?;

        let stream = client.read(broker::ReadRequest {
            journal: journal_name.to_string(),
            offset: offset - 1,
            end_offset: offset,
            block: false,
            ..Default::default()
        });
        tokio::pin!(stream);

        loop {
            match stream.next().await {
                // EOF without content: no byte precedes `offset` to read.
                None => return Ok(offset),
                Some(Ok(resp)) => {
                    if resp.fragment.is_some() || resp.content.is_empty() {
                        continue; // Metadata-only response.
                    }
                    // The broker fast-forwards past deleted fragments, in which
                    // case the byte (and its containing document) is gone.
                    let index = (offset - 1) - resp.offset;
                    if index < 0 {
                        return Ok(offset);
                    }
                    if resp.content[index as usize] == b'\n' {
                        return Ok(offset);
                    }

                    metrics::counter!(
                        "dekaf_readback_reads",
                        "task_name" => task_name.to_string(),
                        "journal_name" => journal_name.to_string(),
                    )
                    .increment(1);
                    tracing::debug!(
                        offset,
                        journal_name,
                        "offset is not a document boundary; reading back"
                    );
                    return Ok(readback_offset(offset));
                }
                Some(Err(gazette::RetryError {
                    inner: gazette::Error::BrokerStatus(status),
                    ..
                })) if matches!(
                    status,
                    broker::Status::OffsetNotYetAvailable
                        | broker::Status::Suspended
                        | broker::Status::JournalNotFound
                ) =>
                {
                    // No preceding byte to inspect. Begin at `offset` and let
                    // the stream itself surface any terminal condition.
                    return Ok(offset);
                }
                Some(Err(gazette::RetryError { attempt, inner }))
                    if inner.is_transient() && attempt < 5 =>
                {
                    tracing::warn!(error = ?inner, "retrying transient error of boundary probe read");
                    continue;
                }
                Some(Err(gazette::RetryError { inner, .. })) => {
                    return Err(anyhow::Error::new(inner)
                        .context("reading the byte before the fetch offset"));
                }
            }
        }
    }

    async fn new_stream(
        not_before: Option<Clock>,
        listener: TaskStateListener,
        partition_template_name: String,
        journal_name: String,
        buffer_size: usize,
        offset: i64,
    ) -> anyhow::Result<(ReadJsonLines, std::time::SystemTime)> {
        let (not_before_sec, _) = not_before
            .map(|c: Clock| Clock::to_unix(&c))
            .unwrap_or((0, 0));

        let (client, exp) = Self::journal_client(&listener, &partition_template_name).await?;

        Ok((
            client.read_json_lines(
                broker::ReadRequest {
                    offset,
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
            exp,
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
            // Once this read has served a document, self.offset is that
            // document's end — a boundary — and the probe resolves to it
            // without readback. A refresh during an in-progress readback
            // instead re-probes the original mid-document offset and resumes
            // the readback.
            let stream_offset = if self.rewrite_offsets_from.is_none() {
                Self::stream_start_offset(
                    &self.listener,
                    &self.partition_template_name,
                    &self.journal_name,
                    &self.task_name,
                    self.offset,
                )
                .await?
            } else {
                self.offset
            };
            (self.stream, self.stream_exp) = Self::new_stream(
                self.not_before,
                self.listener.clone(),
                self.partition_template_name.clone(),
                self.journal_name.clone(),
                self.buffer_size,
                stream_offset,
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
            if timeout_at < now {
                anyhow::bail!(
                    "Encountered a read stream with token expiring in the past. This should not happen, cancelling the read."
                );
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
                        doc::heap::ArchivedNode::Object(_, _) => {
                            Ok(ReadJsonLine::Doc { root, next_offset })
                        }
                        non_object => {
                            tracing::warn!(
                                "skipping past non-object node at offset {}: {:?}",
                                self.offset,
                                doc::SerPolicy::debug_value(non_object)
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
                    Err(gazette::RetryError {
                        inner: gazette::Error::BrokerStatus(broker::Status::JournalNotFound),
                        ..
                    }) => return Ok((self, BatchResult::JournalNotFound)),
                    Err(gazette::RetryError { inner, .. }) => Err(inner),
                }?,
            };

            let (root, next_offset) = match read {
                ReadJsonLine::Meta(response) => {
                    self.last_write_head = response.write_head;
                    // Skip self.offset forward in case we're skipping past a gap,
                    // but never move it backward: the journal read begins up to
                    // OFFSET_READBACK bytes before the offset being served.
                    self.offset = self.offset.max(response.offset);
                    continue;
                }
                ReadJsonLine::Doc { root, next_offset } => (root, next_offset),
            };

            // Suppress documents which end at-or-before the offset being
            // served, scanned over because the journal read began up to
            // OFFSET_READBACK bytes earlier. This must be inclusive: a
            // document ending exactly at `self.offset` maps to Kafka offset
            // `self.offset - 1`, which consumers have already read. Notably,
            // a caught-up consumer polling at the write head must not be
            // re-served the trailing transaction ACK.
            if next_offset <= self.offset {
                continue;
            }

            let mut record_bytes: usize = 0;

            let Some(doc::ArchivedNode::String(uuid)) = self.uuid_ptr.query(root.get()) else {
                let serialized_doc = doc::SerPolicy::debug_value(root.get());
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
                partition_leader_epoch: self.partition_leader_epoch,
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
        RecordBatchEncoder::encode_with_custom_compression(
            &mut buf,
            records.iter(),
            &opts,
            Some(compressor),
        )
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
                ops::stats::MaterializeBinding {
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
                    bytes_behind: 0,
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
pub fn extract_and_encode<N: json::AsNode>(
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
                let debug_serialized = doc::SerPolicy::debug_value(original).to_string();
                tracing::debug!(extractor=?extractor, ?schema, debug_serialized, ?e, "Failed to encode");
                return Err(e);
            }

            Ok::<_, anyhow::Error>(buf)
        })?;

    Ok(())
}
