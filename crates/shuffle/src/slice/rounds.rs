use super::{read::ReadyRead, routing};
use proto_flow::shuffle;
use tokio::sync::mpsc;

/// Documents a round consumes, at which it ends. It bounds the work of a
/// round between servicing the Slice's other events, including a long run of
/// documents which aren't appended (duplicates, filtered documents, or ACKs).
pub const ROUND_DOCUMENTS: usize = 1024;

/// Rounds is the state of a Slice's sends to its Logs.
/// See "Rounds" and "Log Merge and Output" of the crate README.
pub struct Rounds {
    /// Channels to shard Log RPCs, indexed by shard index.
    pub logs: Vec<LogChannel>,
    /// Documents consumed by the current round.
    round_documents: usize,
    /// Re-usable scratch buffer for the packed key of an Append.
    packed_key: bytes::BytesMut,
    /// Re-usable scratch buffer for the target Logs of an Append.
    targets: Vec<usize>,
}

/// LogChannel is a Slice's request channel to one Log, and the Appends it
/// buffers for the current round. A round is sent as a single LogRequest, which the
/// receiver (the LogActor, or an h2 stream's encoder) takes as a whole with
/// one wake.
pub struct LogChannel {
    /// Request channel to the Log. A round begins only once every channel has
    /// capacity for it.
    pub tx: mpsc::Sender<shuffle::LogRequest>,
    /// Journal of the last Append buffered for this Log, for delta encoding.
    prev_journal: String,
    /// Appends buffered for the current round, not yet sent.
    buffer: Vec<shuffle::log_request::Append>,
}

impl Rounds {
    pub fn new(logs: Vec<LogChannel>) -> Self {
        Self {
            logs,
            round_documents: 0,
            packed_key: bytes::BytesMut::new(),
            targets: Vec::new(),
        }
    }

    /// Count a consumed document against the current round.
    pub fn on_consumed(&mut self) {
        self.round_documents += 1;
    }

    /// Whether the current round has consumed its documents, and must end.
    pub fn is_round_full(&self) -> bool {
        self.round_documents >= ROUND_DOCUMENTS
    }

    /// Buffer Appends of a consumed `ready_read` for the current round of its
    /// target Logs.
    pub fn buffer_append(
        &mut self,
        binding: &crate::Binding,
        journal: &str,
        shards: &[shuffle::Shard],
        ready_read: &ReadyRead,
    ) {
        let Self {
            logs,
            packed_key,
            targets,
            ..
        } = self;

        let ReadyRead { doc, meta, .. } = ready_read;

        // Extract into `packed_key` and hash to route the document.
        // Compute shard index `targets` to receive an Append of this document.
        packed_key.clear();
        doc::Extractor::extract_all(
            doc.get(),
            &binding.key_extractors,
            doc::Encoding::Packed,
            packed_key,
            None,
        );

        let key_hash = doc::Extractor::packed_hash(packed_key);
        let r_clock = routing::rotate_clock(meta.clock);

        targets.clear();
        targets.extend(routing::route_to_shards(
            key_hash,
            r_clock,
            binding.filter_r_clocks,
            shards,
        ));

        tracing::trace!(
            %journal,
            binding = binding.state_key(),
            producer = ?meta.producer,
            clock = ?meta.clock,
            begin_offset = meta.begin_offset,
            key_hash,
            flags = meta.flags.0,
            r_clock,
            ?targets,
            "routed document Append to Log RPC shards"
        );

        // Journal names are delta-encoded per target, as each is buffered.
        let append = shuffle::log_request::Append {
            journal_name_truncate_delta: 0,
            journal_name_suffix: String::new(),
            binding: binding.index as u32,
            read_delay: binding.read_delay.as_u64(),
            producer: meta.producer.as_i64(),
            clock: meta.clock.as_u64(),
            flags: meta.flags.0 as u32,
            packed_key: packed_key.split().freeze(),
            doc_archived: doc.bytes().clone(),
            source_byte_length: (meta.end_offset - meta.begin_offset).try_into().unwrap(),
        };

        if let Some((&last, rest)) = targets.split_last() {
            for &target in rest {
                logs[target].buffer_append(journal, append.clone());
            }
            logs[last].buffer_append(journal, append);
        }
    }

    /// Send the current round, with a flush request of `flush_cycle`, if any.
    /// A round goes to each Log having buffered Appends, and to every Log if
    /// it requests a flush. Returns whether any Log was sent to.
    pub fn send(&mut self, flush_cycle: Option<u64>) -> anyhow::Result<bool> {
        self.round_documents = 0;

        let flush = flush_cycle.map(|cycle| shuffle::log_request::Flush { cycle });
        let mut sent = false;

        for log in self.logs.iter_mut() {
            if flush.is_none() && log.buffer.is_empty() {
                continue;
            }
            log.send_round(flush)?;
            sent = true;
        }
        if sent {
            tracing::trace!(?flush, "sent round");
        }
        Ok(sent)
    }
}

impl LogChannel {
    pub fn new(tx: mpsc::Sender<shuffle::LogRequest>) -> Self {
        Self {
            tx,
            prev_journal: String::new(),
            buffer: Vec::new(),
        }
    }

    /// Buffer an Append of `journal` for the current round.
    fn buffer_append(&mut self, journal: &str, append: shuffle::log_request::Append) {
        // Delta-encode the journal name.
        let (journal_name_truncate_delta, journal_name_suffix) =
            gazette::delta::encode(&self.prev_journal, journal);
        let journal_name_suffix = journal_name_suffix.to_string();

        // Retain for next name delta-encoding.
        self.prev_journal.clear();
        self.prev_journal.push_str(journal);

        self.buffer.push(shuffle::log_request::Append {
            journal_name_truncate_delta,
            journal_name_suffix,
            ..append
        });
    }

    /// Send the Appends buffered for the current round with `flush`, as one message.
    /// The round must have begun with channel capacity.
    fn send_round(&mut self, flush: Option<shuffle::log_request::Flush>) -> anyhow::Result<()> {
        let appends = std::mem::take(&mut self.buffer);
        self.buffer.reserve(appends.len());

        crate::verify_send(
            &self.tx,
            shuffle::LogRequest {
                open: None,
                appends,
                flush,
            },
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_send_rounds() {
        let (txs, mut rxs): (Vec<_>, Vec<_>) = (0..2).map(|_| mpsc::channel(16)).unzip();
        let mut rounds = Rounds::new(txs.into_iter().map(LogChannel::new).collect());

        // Each step records whether a round was sent, and (appends, flush)
        // of the rounds each Log received.
        let mut trace = Vec::new();
        let mut step = |step: &str, sent: bool| {
            let received = rxs.iter_mut().map(|rx| {
                std::iter::from_fn(|| rx.try_recv().ok())
                    .map(|r| {
                        format!(
                            "({}, flush {:?})",
                            r.appends.len(),
                            r.flush.map(|f| f.cycle)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            });
            let received: Vec<_> = received.collect();
            trace.push(format!("{step} -> sent: {sent}, received: {received:?}"));
        };

        // An empty round is not sent.
        step("send", rounds.send(None).unwrap());

        // A round goes only to Logs having Appends.
        rounds.on_consumed();
        rounds.logs[0].buffer_append("a/journal", Default::default());
        rounds.on_consumed();
        rounds.logs[0].buffer_append("a/journal", Default::default());
        rounds.on_consumed();
        step("consume x3, buffer(0) x2, send", rounds.send(None).unwrap());

        // A flush request goes to every Log.
        rounds.logs[1].buffer_append("a/journal", Default::default());
        step("buffer(1), send(flush 5)", rounds.send(Some(5)).unwrap());

        // A round is full once it consumes ROUND_DOCUMENTS, and sending ends it.
        for _ in 0..ROUND_DOCUMENTS {
            assert!(!rounds.is_round_full());
            rounds.on_consumed();
        }
        assert!(rounds.is_round_full());
        step(
            "consume x ROUND_DOCUMENTS, send",
            rounds.send(None).unwrap(),
        );
        assert!(!rounds.is_round_full());

        insta::assert_snapshot!(trace.join("\n"));
    }
}
