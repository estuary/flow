use super::producer::{ProducerMap, ProducerState};
use proto_gazette::{broker, uuid};

/// A control event parsed from a control document body.
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

        let control = if flags.is_control() {
            // A `CONTROL` document is always `OUTSIDE_TXN`; the low two
            // transaction-semantics bits must be zero. Any other combination
            // is a protocol error from the publisher and would otherwise be
            // silently mishandled downstream (treated as a CONTINUE_TXN /
            // ACK_TXN doc with a control body).
            anyhow::ensure!(
                flags.is_outside(),
                "journal {journal} offset {begin_offset}: \
                 CONTROL document must use OUTSIDE_TXN sequencing \
                 (flags = {flags:?})"
            );

            let is_true = |path: &str| {
                let ptr = json::Pointer::from_str(path);
                matches!(ptr.query(archived), Some(doc::ArchivedNode::Bool(true)))
            };

            // TODO(whb) Relocate these strings as constants somewhere that
            // makese sense.
            if is_true("/_meta/backfillBegin") {
                Some(ControlEvent::BackfillBegin)
            } else if is_true("/_meta/backfillComplete") {
                Some(ControlEvent::BackfillComplete)
            } else {
                anyhow::bail!(
                    "journal {journal} offset {begin_offset}: \
                     control document not recognized"
                )
            }
        } else {
            None
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

/// Probe the current write head of a journal via a non-blocking read at offset -1.
/// Returns `(write_head, header)`.
pub async fn probe_write_head(
    client: gazette::journal::Client,
    journal: &str,
    binding_state_key: &str,
    header: Option<broker::Header>,
) -> anyhow::Result<(i64, Option<broker::Header>)> {
    use futures::StreamExt;

    // A non-blocking read at offset -1 returns OffsetNotYetAvailable immediately.
    // Note that the `client.read()` controls ReadRequest::metadata_only internally.
    let stream = client.read(broker::ReadRequest {
        journal: journal.to_string(),
        offset: -1,
        block: false,
        do_not_proxy: true,
        header,
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
                if err.is_transient() {
                    tracing::warn!(
                        binding = %binding_state_key,
                        %journal,
                        attempt,
                        %err,
                        "transient error probing journal write head (will retry)"
                    );
                } else {
                    return Err(map_read_error(
                        err,
                        journal,
                        binding_state_key,
                        "probing write head",
                    ));
                }
            }
            Some(Ok(resp)) => return Ok((resp.write_head, resp.header)),
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
            // Control: backfillBegin (skips validation, parsed as control event).
            // Control docs always carry `Flag_CONTROL` alone (0x4); the low
            // transaction-semantics bits are implicitly OUTSIDE_TXN, so these
            // documents are immediately committed and never participate in
            // a CONTINUE_TXN / ACK_TXN span.
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
