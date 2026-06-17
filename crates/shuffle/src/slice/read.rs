use super::producer::ProducerState;
use crate::ProducerMap;
use proto_gazette::{broker, uuid};

/// A control event parsed from a CONTROL document body.
///
/// CONTROL documents carry backfill metadata that is folded into the checkpoint
/// frontier rather than appended to shuffle log segments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlEvent {
    BackfillBegin,
    BackfillComplete,
}

/// State about an active read, indexed by its `read.id()`.
///
/// Each ReadState represents one (journal, binding) pair and is the
/// complete sequencing context for all producers in that journal.
/// Producer lifecycle events — begin span, extend span, commit,
/// rollback — are self-contained within the ReadState's producer maps.
#[derive(Debug)]
pub struct ReadState {
    /// Index of the binding within `SliceActor::bindings`.
    pub binding_index: u16,
    /// The journal name (canonical, without the `;suffix` read metadata).
    pub journal: Box<str>,
    /// Truncation boundary for this journal: the `estuary.dev/truncated-at`
    /// label clock, or zero when absent. Documents published before it were
    /// superseded by a backfill, so `sequence_document` suppresses their append.
    pub truncated_at: uuid::Clock,
    /// Latest `BackfillBegin` control-doc clock for this journal, awaiting the
    /// next flush (zero = none).
    pub backfill_begin: uuid::Clock,
    /// Latest `BackfillComplete` control-doc clock for this journal, awaiting
    /// the next flush (zero = none).
    pub backfill_complete: uuid::Clock,
    /// Producers whose state is settled: either from the initial checkpoint
    /// or drained from `pending` at the start of a flush cycle.
    pub settled: ProducerMap<ProducerState>,
    /// Producers updated since the last flush cycle started.
    /// Drained into `settled` at the start of each flush.
    pub pending: ProducerMap<ProducerState>,
    /// End offset of most recently processed document.
    pub read_offset: i64,
    /// Read offset as of last flush (baseline for bytes_read_delta).
    pub prev_read_offset: i64,
    /// Most recent write_head observed for this journal.
    pub write_head: i64,
    /// Write head as of last flush (baseline for bytes_behind_delta).
    pub prev_write_head: i64,
}

impl ReadState {
    /// Construct a `ReadState` for a read with `settled` producers recovered
    /// from its checkpoint.
    pub fn recovered(
        binding_index: u16,
        journal: Box<str>,
        truncated_at: uuid::Clock,
        settled: ProducerMap<ProducerState>,
    ) -> Self {
        Self {
            binding_index,
            journal,
            truncated_at,
            backfill_begin: uuid::Clock::zero(),
            backfill_complete: uuid::Clock::zero(),
            settled,
            pending: Default::default(),
            read_offset: 0,
            prev_read_offset: 0,
            write_head: 0,
            prev_write_head: 0,
        }
    }

    /// Seed all four offset fields to `offset`, the read's effective start as
    /// resolved by the journal probe. The probe may fast-forward past leading
    /// fragments that precede the read's `begin_mod_time`, so `offset` can
    /// exceed the checkpoint offset; the skipped range would be filtered by the
    /// read regardless. Initializing all four equal makes the initial "bytes
    /// behind" (`write_head - read_offset`) and the delta baseline
    /// (`prev_write_head - prev_read_offset`) zero, which is required because
    /// metrics accumulate deltas starting from these baselines.
    ///
    /// `write_head` is a placeholder here; it's overwritten with the journal's
    /// true head once the first batch resolves in `process_read_result`.
    pub fn start_at(&mut self, offset: i64) {
        self.read_offset = offset;
        self.prev_read_offset = offset;
        self.write_head = offset;
        self.prev_write_head = offset;
    }
}

/// Metadata about a document in a ReadyRead batch.
#[derive(Debug)]
pub struct Meta {
    /// Begin offset (inclusive) of `doc` within the journal.
    pub begin_offset: i64,
    /// End offset (exclusive) of `doc` within the journal.
    /// This is the offset at which the next document begins.
    pub end_offset: i64,
    /// Publication Clock of `doc` (extracted from its UUID).
    pub clock: uuid::Clock,
    /// Publication Flags of `doc` (extracted from its UUID).
    /// Bit 15 (`FLAGS_SCHEMA_VALID`) is set when the document passes schema validation.
    pub flags: uuid::Flags,
    /// Publication Producer of `doc` (extracted from its UUID).
    pub producer: uuid::Producer,
    /// Parsed control event, when this document has the CONTROL flag set.
    pub control: Option<ControlEvent>,
}

/// ReadyRead is a ReadLines which has one or more parsed documents.
pub struct ReadyRead {
    /// The underlying read stream, returned to pending_reads when `tail` is exhausted.
    pub inner: super::ReadLines,
    /// Head document content.
    pub doc: doc::OwnedArchivedNode,
    /// Metadata about the head document.
    pub meta: Meta,
    /// Remaining tail of ready documents.
    pub doc_tail: simd_doc::transcoded::OwnedIterOut,
    /// Pre-extracted metadata for remaining tail documents.
    pub meta_tail: std::vec::IntoIter<Meta>,
}

/// Extract UUID metadata and validate each document in a transcoded batch.
/// Returns a Vec<Meta> parallel to `transcoded.iter()`.
pub fn extract_metas(
    transcoded: &simd_doc::transcoded::Transcoded,
    uuid_ptr: &json::Pointer,
    validator: &mut doc::Validator,
    journal: &str,
) -> anyhow::Result<Vec<Meta>> {
    let mut begin_offset = transcoded.offset;
    let mut out = Vec::with_capacity(32);

    for (bytes, end_offset) in transcoded.iter() {
        let archived = doc::ArchivedNode::from_archive(bytes);

        let (producer, clock, flags) = uuid_ptr
            .query(archived)
            .and_then(|node| match node {
                doc::ArchivedNode::String(s) => proto_gazette::uuid::parse_str(s).ok(),
                _ => None,
            })
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "journal {journal} offset {begin_offset}: \
                         document is missing a valid UUID"
                )
            })?;

        let has = |path: &str| json::Pointer::from_str(path).query(archived).is_some();
        let control = if !flags.is_control() {
            None
        } else if has("/_meta/backfillBegin") {
            Some(ControlEvent::BackfillBegin)
        } else if has("/_meta/backfillComplete") {
            Some(ControlEvent::BackfillComplete)
        } else {
            anyhow::bail!(
                "journal {journal} offset {begin_offset}: control document not recognized"
            );
        };

        let flags = if !flags.is_ack() && control.is_none() && validator.is_valid(archived) {
            uuid::Flags(flags.0 | crate::FLAGS_SCHEMA_VALID)
        } else {
            flags
        };

        let meta = Meta {
            begin_offset,
            end_offset,
            clock,
            flags,
            producer,
            control,
        };
        begin_offset = end_offset;

        out.push(meta);
    }

    Ok(out)
}

/// Probe where a read starting at `offset` with the given `begin_mod_time` will
/// actually begin, along with the journal's current write head.
/// Returns `(start_offset, write_head, header)`.
///
/// We issue a non-blocking, metadata-only read at `offset` with `begin_mod_time`
/// wired in. The broker fast-forwards over any fragments that fall before
/// `begin_mod_time` (or over holes in the offset space) and replies with a single
/// metadata response before closing the stream — `Status OK` with a covering
/// fragment if eligible content exists, or `OFFSET_NOT_YET_AVAILABLE` if every
/// fragment was filtered. Either way the response carries:
///   * `offset`: the fast-forwarded start, at-or-after the requested `offset`, and
///   * `write_head`: the journal's current write head.
///
/// `start_offset == write_head` means the read is caught up: all content up to the
/// head precedes `begin_mod_time`. Starting the read at `start_offset` rather than
/// a stale `offset` below the head is both correct — the skipped bytes precede
/// `begin_mod_time` and would be filtered by the read regardless — and necessary
/// to classify a caught-up read as tailing rather than stalled.
///
/// Note that `client.read()` controls `ReadRequest::metadata_only` internally.
pub async fn probe_read_start(
    client: gazette::journal::Client,
    journal: &str,
    binding_state_key: &str,
    header: Option<broker::Header>,
    journal_create_revision: i64,
    offset: i64,
    begin_mod_time: i64,
) -> anyhow::Result<(i64, i64, Option<broker::Header>)> {
    use futures::StreamExt;

    let stream = client.read(broker::ReadRequest {
        journal: journal.to_string(),
        offset,
        begin_mod_time,
        block: false,
        do_not_proxy: true,
        header,
        min_etcd_revision: journal_create_revision,
        ..Default::default()
    });
    tokio::pin!(stream);

    loop {
        match stream.next().await {
            None => anyhow::bail!(
                "probe stream ended unexpectedly for {journal} (binding {binding_state_key})",
            ),
            Some(Err(gazette::RetryError {
                attempt,
                inner: err,
            })) => {
                // A non-blocking read whose `offset` is ahead of the write head
                // returns OFFSET_NOT_YET_AVAILABLE (surfaced here as a non-transient
                // BrokerStatus). This is expected transiently while a (re)assigned
                // broker loads its fragment index and its tracked end offset
                // (reported write head) lags our checkpoint `offset`. Retry until
                // the broker catches up — exactly as a blocking read at `offset` would wait.
                let retryable = err.is_transient()
                    || matches!(
                        err,
                        gazette::Error::BrokerStatus(broker::Status::OffsetNotYetAvailable)
                    );
                if retryable {
                    service_kit::event!(
                        tracing::Level::WARN,
                        "read",
                        binding = binding_state_key.to_string(),
                        journal = journal.to_string(),
                        attempt,
                        err = service_kit::event::debug(err),
                        "transient error probing journal read start (will retry)",
                    );
                } else {
                    return Err(map_read_error(
                        err,
                        journal,
                        binding_state_key,
                        "probing read start",
                    ));
                }
            }
            Some(Ok(resp)) => return Ok((resp.offset, resp.write_head, resp.header)),
        }
    }
}

/// Map a non-transient gazette Error into an anyhow::Error with context.
pub fn map_read_error(
    err: gazette::Error,
    journal: &str,
    binding_state_key: &str,
    context: &'static str,
) -> anyhow::Error {
    match err {
        gazette::Error::Grpc(status) => crate::status_to_anyhow(status),
        err => anyhow::anyhow!(err),
    }
    .context(format!(
        "read of {journal} ({context} of binding {binding_state_key})"
    ))
}

#[cfg(test)]
mod test {
    use super::*;
    use proto_gazette::uuid;

    fn producer(id: u8) -> uuid::Producer {
        uuid::Producer::from_bytes([id | 0x01, 0, 0, 0, 0, 0])
    }

    fn make_uuid_str(producer: uuid::Producer, clock: uuid::Clock, flags: uuid::Flags) -> String {
        uuid::build(producer, clock, flags).to_string()
    }

    /// Build a Transcoded batch from newline-delimited JSON at the given starting offset.
    fn transcode(json_lines: &str, offset: i64) -> simd_doc::Transcoded {
        let mut input = json_lines.as_bytes().to_vec();
        let mut off = offset;
        simd_doc::transcode_many(
            &mut simd_doc::SimdParser::new(1_000_000),
            &mut input,
            &mut off,
            Default::default(),
        )
        .expect("transcoding should succeed")
    }

    #[test]
    fn test_extract_metas() {
        // Schema requires "required_field", exercising valid, invalid, ACK bypass,
        // and control doc paths.
        let schema = br#"{"type":"object","required":["required_field"]}"#;
        let bundle = doc::validation::build_bundle(schema).unwrap();
        let mut validator = doc::Validator::new(bundle).unwrap();

        let p1 = producer(0x01);
        let mut clock = uuid::Clock::from_unix(1000, 0);
        let c1 = clock.tick();
        let c2 = clock.tick();
        let c3 = clock.tick();
        let c4 = clock.tick();
        let c5 = clock.tick();

        let json = [
            // Valid schema, OUTSIDE_TXN.
            format!(
                r#"{{"_meta":{{"uuid":"{}"}},"required_field":"present"}}"#,
                make_uuid_str(p1, c1, uuid::Flags::OUTSIDE_TXN),
            ),
            // Invalid schema, CONTINUE_TXN.
            format!(
                r#"{{"_meta":{{"uuid":"{}"}},"other":"value"}}"#,
                make_uuid_str(p1, c2, uuid::Flags::CONTINUE_TXN),
            ),
            // ACK_TXN (skips validation).
            format!(
                r#"{{"_meta":{{"uuid":"{}"}}}}"#,
                make_uuid_str(p1, c3, uuid::Flags::ACK_TXN),
            ),
            // Control: backfillBegin (skips validation, parsed as a control event).
            // CONTROL docs always carry `Flag_CONTROL` alone (0x4); the low
            // transaction-semantics bits are implicitly OUTSIDE_TXN, so these
            // documents are immediately committed and never participate in a
            // CONTINUE_TXN / ACK_TXN span.
            format!(
                r#"{{"_meta":{{"uuid":"{}","backfillBegin":true}}}}"#,
                make_uuid_str(p1, c4, uuid::Flags::CONTROL),
            ),
            // Control: backfillComplete.
            format!(
                r#"{{"_meta":{{"uuid":"{}","backfillComplete":true}}}}"#,
                make_uuid_str(p1, c5, uuid::Flags::CONTROL),
            ),
        ]
        .join("\n")
            + "\n";

        let uuid_ptr = json::Pointer::from_str("/_meta/uuid");
        let transcoded = transcode(&json, 12345);

        let metas = extract_metas(&transcoded, &uuid_ptr, &mut validator, "test/journal")
            .expect("should succeed");

        insta::assert_debug_snapshot!(metas);
    }

    #[test]
    fn test_extract_metas_missing_uuid() {
        let bundle = doc::validation::build_bundle(b"{}").unwrap();
        let mut validator = doc::Validator::new(bundle).unwrap();

        let transcoded = transcode("{\"_meta\":{},\"key\":\"value\"}\n", 12345);
        let uuid_ptr = json::Pointer::from_str("/_meta/uuid");

        let err = extract_metas(&transcoded, &uuid_ptr, &mut validator, "test/journal")
            .expect_err("should fail for missing UUID");

        insta::assert_debug_snapshot!(err.to_string());
    }
}
