use super::producer::{ProducerMap, ProducerState};
use anyhow::Context;
use proto_gazette::{broker, uuid};

/// State about an active read, indexed by its `read.id()`.
#[derive(Debug)]
pub struct ReadState {
    /// Index of the binding within `SliceActor::bindings`.
    pub binding_index: u32,
    /// The journal name (canonical, without the `;suffix` read metadata).
    pub journal: Box<str>,
    /// Producers whose state is settled: either from the initial checkpoint
    /// or drained from `pending` at the start of a flush cycle.
    pub settled: ProducerMap<ProducerState>,
    /// Producers updated since the last flush cycle started.
    /// Drained into `settled` at the start of each flush.
    pub pending: ProducerMap<ProducerState>,
}

/// Metadata about the head document of a ReadyRead.
pub struct Meta {
    /// Begin offset (inclusive) of `doc` within the journal.
    pub begin_offset: i64,
    /// End offset (exclusive) of `doc` within the journal.
    /// This is the offset at which the next document begins.
    pub end_offset: i64,
    /// Publication Clock of `doc` (extracted from its UUID).
    pub clock: uuid::Clock,
    /// Publication Flags of `doc` (extracted from its UUID).
    pub flags: uuid::Flags,
    /// Publication Producer of `doc` (extracted from its UUID).
    pub producer: uuid::Producer,
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
    pub tail: simd_doc::transcoded::OwnedIterOut,
}

impl ReadyRead {
    pub fn new(
        binding: &crate::Binding,
        doc: doc::OwnedArchivedNode,
        begin_offset: i64,
        end_offset: i64,
        tail: simd_doc::transcoded::OwnedIterOut,
        read: super::ReadLines,
    ) -> anyhow::Result<Self> {
        let (producer, clock, flags) = binding
            .source_uuid_ptr
            .query(doc.get())
            .and_then(|node| match node {
                doc::ArchivedNode::String(s) => Some(proto_gazette::uuid::parse_str(s).ok()),
                _ => None,
            })
            .flatten()
            .with_context(|| {
                format!(
                    "journal {} offset {begin_offset}: document is missing a valid UUID",
                    read.fragment().journal,
                )
            })?;

        Ok(Self {
            doc,
            meta: Meta {
                begin_offset,
                end_offset,
                clock,
                flags,
                producer,
            },
            tail,
            inner: read,
        })
    }
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
