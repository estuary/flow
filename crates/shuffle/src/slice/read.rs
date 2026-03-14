use super::producer::{ProducerMap, ProducerState};
use proto_gazette::{broker, uuid};

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
    /// Non-ACK non-filtered document bytes accumulated since last flush drain.
    pub bytes_read_delta: i64,
    /// Most recent write_head observed for this journal.
    pub write_head: i64,
    /// End offset of most recently processed document.
    pub read_offset: i64,
    /// Bytes-behind as of last flush (0 before first flush).
    /// First delta = (write_head - read_offset) - 0 = absolute initial lag.
    pub prev_bytes_behind: i64,
}

/// Application-local flag packed into `Flags`: document passed schema validation.
/// Bit 15 of the u16, above the 10-bit UUID wire space (bits 0-9).
/// `uuid::build()` asserts flags fit in 10 bits, so accidental round-trip is impossible.
const FLAGS_SCHEMA_VALID: u16 = 0x8000;

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
}

impl Meta {
    /// Whether the document passed schema validation.
    /// This is encoded as bit 15 of `flags`, above the 10-bit UUID wire space.
    #[inline]
    pub fn is_schema_valid(&self) -> bool {
        self.flags.0 & FLAGS_SCHEMA_VALID != 0
    }
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

        let flags = if flags != uuid::Flags::ACK_TXN && validator.is_valid(archived) {
            uuid::Flags(flags.0 | FLAGS_SCHEMA_VALID)
        } else {
            flags
        };

        let meta = Meta {
            begin_offset,
            end_offset,
            clock,
            flags,
            producer,
        };
        begin_offset = end_offset;

        out.push(meta);
    }

    Ok(out)
}

/// Probe the current write head of a journal via a non-blocking read at offset -1.
/// Returns `(write_head, header)`. JournalNotFound yields `(0, None)`.
pub async fn probe_write_head(
    client: gazette::journal::Client,
    journal: &str,
    binding_state_key: &str,
    header: Option<broker::Header>,
) -> anyhow::Result<(i64, Option<broker::Header>)> {
    use futures::StreamExt;

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
                    return Err(map_read_error(err, journal, binding_state_key));
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
) -> anyhow::Error {
    match err {
        gazette::Error::Grpc(status) => crate::status_to_anyhow(status),
        err => anyhow::anyhow!(err),
    }
    .context(format!("read of {journal} (binding {binding_state_key})"))
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
        // Schema requires "required_field", exercising both valid and invalid paths.
        let schema = br#"{"type":"object","required":["required_field"]}"#;
        let bundle = doc::validation::build_bundle(schema).unwrap();
        let mut validator = doc::Validator::new(bundle).unwrap();

        let p1 = producer(0x01);
        let mut clock = uuid::Clock::from_unix(1000, 0);
        let c1 = clock.tick();
        let c2 = clock.tick();
        let c3 = clock.tick();

        // Three docs exercise: non-zero base offset, offset chaining,
        // OUTSIDE_TXN/CONTINUE_TXN/ACK_TXN flags, valid + invalid schema, ACK bypass.
        let json = [
            format!(
                r#"{{"_meta":{{"uuid":"{}"}},"required_field":"present"}}"#,
                make_uuid_str(p1, c1, uuid::Flags::OUTSIDE_TXN),
            ),
            format!(
                r#"{{"_meta":{{"uuid":"{}"}},"other":"value"}}"#,
                make_uuid_str(p1, c2, uuid::Flags::CONTINUE_TXN),
            ),
            format!(
                r#"{{"_meta":{{"uuid":"{}"}}}}"#,
                make_uuid_str(p1, c3, uuid::Flags::ACK_TXN),
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
