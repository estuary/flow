use super::{read::ReadyRead, routing};
use proto_flow::shuffle;
use proto_gazette::uuid;
use tokio::sync::mpsc;

/// Window of quota a Slice may fill beyond what its Logs have released:
/// the archived bytes of documents it has consumed, whether or not they were
/// appended, whose Barriers a Log hasn't yet merged. Every Log is charged the
/// same quota, so the Log furthest behind in releasing it sets the limit.
///
/// It bounds the Slice's lead over the merge of its priority, and with it the
/// memory each Log buffers for the Slice.
pub const QUOTA_WINDOW_BYTES: u64 = 32 * 1024 * 1024;

/// Quota at which a round ends. It keeps many Barriers in flight to each
/// Log, so that their release pipelines with the Slice's next rounds, and
/// bounds the work of a round between servicing the Slice's other events.
pub const ROUND_QUOTA_BYTES: u64 = QUOTA_WINDOW_BYTES / 16;

/// Rounds is the state of a Slice's sends to its Logs.
/// See "Rounds and Flow Control" and "Log Merge and Output" of the crate README.
pub struct Rounds {
    /// Channels to shard Log RPCs, indexed by shard index.
    pub logs: Vec<LogChannel>,
    /// Greatest adjusted clock of any consumed document: the position of our
    /// Barriers. It's at or above every Append we've sent.
    consumed_hwm: uuid::Clock,
    /// Quota of the current round: archived bytes of its consumed documents.
    round_quota: u64,
    /// Cumulative quota of sent Barriers, which is the same to every Log.
    sent_quota: u64,
    /// Whether our last Barrier was caught up, as every Log currently holds.
    caught_up: bool,
    /// Index of the Log furthest behind in releasing quota, which sets the
    /// window's limit.
    least: usize,
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
    /// Cumulative quota of our Barriers which the Log has merged and released.
    acked_quota: u64,
}

impl Rounds {
    pub fn new(logs: Vec<LogChannel>) -> Self {
        Self {
            logs,
            consumed_hwm: uuid::Clock::zero(),
            round_quota: 0,
            sent_quota: 0,
            caught_up: false,
            least: 0,
            packed_key: bytes::BytesMut::new(),
            targets: Vec::new(),
        }
    }

    /// Try to charge the current round with the quota of a document of
    /// `doc_bytes` at `adjusted_clock`. Once charged, the Slice has consumed
    /// through `adjusted_clock`. Returns `Err(log)` with the index of the Log
    /// whose window it would exceed. A document is always admitted while the
    /// window is empty, however large.
    pub fn try_charge(&mut self, doc_bytes: u64, adjusted_clock: uuid::Clock) -> Result<(), usize> {
        let filled = self.sent_quota + self.round_quota - self.logs[self.least].acked_quota;
        if filled != 0 && filled + doc_bytes > QUOTA_WINDOW_BYTES {
            return Err(self.least);
        }
        self.round_quota += doc_bytes;
        self.consumed_hwm = self.consumed_hwm.max(adjusted_clock);
        Ok(())
    }

    /// Whether the current round has consumed its quota, and must end.
    pub fn is_round_full(&self) -> bool {
        self.round_quota >= ROUND_QUOTA_BYTES
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

    /// Send the current round to every Log, as one LogRequest each: the
    /// Appends buffered for that Log, and a Barrier at the position the Slice
    /// has consumed through. The Barrier carries the round's quota,
    /// `caught_up`, and an optional `flush_cycle`.
    /// The round is sent only if it consumed quota, requests a flush, or
    /// `caught_up` has changed. Returns whether it was sent.
    pub fn send(&mut self, caught_up: bool, flush_cycle: Option<u64>) -> anyhow::Result<bool> {
        if self.round_quota == 0 && flush_cycle.is_none() && caught_up == self.caught_up {
            return Ok(false);
        }
        let barrier = shuffle::log_request::Barrier {
            adjusted_clock: self.consumed_hwm.as_u64(),
            quota: self.round_quota,
            caught_up,
            flush_cycle: flush_cycle.unwrap_or_default(),
        };
        for log in self.logs.iter_mut() {
            log.send_round(barrier.clone())?;
        }
        tracing::trace!(?barrier, "sent round");

        self.sent_quota += std::mem::take(&mut self.round_quota);
        self.caught_up = caught_up;
        Ok(true)
    }

    /// Apply the cumulative `acked` quota released by the Log at index `log`.
    pub fn on_acked(&mut self, log: usize, acked: u64) -> anyhow::Result<()> {
        let acked_quota = &mut self.logs[log].acked_quota;

        if acked < *acked_quota || acked > self.sent_quota {
            anyhow::bail!(
                "Acked quota {acked} must be between prior Acked {acked_quota} and sent {}",
                self.sent_quota,
            );
        }
        *acked_quota = acked;

        // Acked quota only increases, so the least Log changes only when it acks.
        if log == self.least {
            self.least = (self.logs.iter().enumerate())
                .min_by_key(|(_, log)| log.acked_quota)
                .expect("a Slice has at least one Log")
                .0;
        }
        Ok(())
    }
}

impl LogChannel {
    pub fn new(tx: mpsc::Sender<shuffle::LogRequest>) -> Self {
        Self {
            tx,
            prev_journal: String::new(),
            buffer: Vec::new(),
            acked_quota: 0,
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

    /// Send the Appends buffered for the current round with `barrier`, as one message.
    /// The round must have begun with channel capacity.
    fn send_round(&mut self, barrier: shuffle::log_request::Barrier) -> anyhow::Result<()> {
        let appends = std::mem::take(&mut self.buffer);
        self.buffer.reserve(appends.len());

        crate::verify_send(
            &self.tx,
            shuffle::LogRequest {
                open: None,
                appends,
                barrier: Some(barrier),
            },
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn rounds(n: usize) -> (Rounds, Vec<mpsc::Receiver<shuffle::LogRequest>>) {
        let (txs, rxs): (Vec<_>, Vec<_>) = (0..n).map(|_| mpsc::channel(16)).unzip();
        (
            Rounds::new(txs.into_iter().map(LogChannel::new).collect()),
            rxs,
        )
    }

    #[test]
    fn test_quota_window() {
        const WINDOW: u64 = QUOTA_WINDOW_BYTES;
        let (mut rounds, _rxs) = rounds(2);
        let clock = uuid::Clock::from_u64;

        // Steps record their result, and the quota which follows it.
        // Quota is in eighths of WINDOW.
        let eighths = |bytes: u64| format!("{}/8", bytes as f64 / (WINDOW / 8) as f64);
        let mut trace = Vec::new();
        let mut step = |step: &str, result: &dyn std::fmt::Debug, r: &Rounds| {
            trace.push(format!(
                "{step} -> {result:?}\n    round: {}, sent: {}, acked: [{}, {}], hwm: {}, full: {}",
                eighths(r.round_quota),
                eighths(r.sent_quota),
                eighths(r.logs[0].acked_quota),
                eighths(r.logs[1].acked_quota),
                r.consumed_hwm.as_u64(),
                r.is_round_full(),
            ))
        };

        // While the window is empty, any document is admitted, however large.
        step(
            "consume(32/8)",
            &rounds.try_charge(WINDOW * 4, clock(10)),
            &rounds,
        );
        step("send", &rounds.send(false, None).unwrap(), &rounds);
        step(
            "consume(1/8)",
            &rounds.try_charge(WINDOW / 8, clock(20)),
            &rounds,
        );
        rounds.on_acked(0, WINDOW * 4).unwrap();
        step(
            "Log 0 acks 32/8",
            &rounds.try_charge(WINDOW / 8, clock(20)),
            &rounds,
        );
        rounds.on_acked(1, WINDOW * 4).unwrap();
        step(
            "Log 1 acks 32/8",
            &rounds.try_charge(WINDOW / 8, clock(20)),
            &rounds,
        );

        // Rounds fill the window, which is that of the Log which acked least.
        // A round is full at a quarter window. Positions are the greatest
        // consumed clock.
        step(
            "consume(4/8)",
            &rounds.try_charge(WINDOW / 2, clock(40)),
            &rounds,
        );
        step("send", &rounds.send(false, None).unwrap(), &rounds);
        step(
            "consume(3/8)",
            &rounds.try_charge(WINDOW * 3 / 8, clock(30)),
            &rounds,
        );
        step("consume(1 byte)", &rounds.try_charge(1, clock(50)), &rounds);
        rounds.on_acked(1, WINDOW * 4 + WINDOW / 2).unwrap();
        step("Log 1 acks 4/8", &rounds.try_charge(1, clock(50)), &rounds);
        rounds.on_acked(0, WINDOW * 4 + WINDOW / 8).unwrap();
        step(
            "Log 0 acks 1/8",
            &rounds.try_charge(WINDOW / 8, clock(50)),
            &rounds,
        );
        step("consume(1 byte)", &rounds.try_charge(1, clock(60)), &rounds);

        // Acked quota never decreases, nor exceeds that sent.
        step(
            "on_acked(0, 0)",
            &rounds.on_acked(0, 0).unwrap_err(),
            &rounds,
        );
        step(
            "on_acked(1, 64/8)",
            &rounds.on_acked(1, WINDOW * 8).unwrap_err(),
            &rounds,
        );

        insta::assert_snapshot!(trace.join("\n"));
    }

    #[test]
    fn test_send_rounds() {
        let (mut rounds, mut rxs) = rounds(2);
        let flush = Some(5);
        let clock = uuid::Clock::from_u64;

        // Each step records whether a round was sent, and (appends, barrier)
        // of the rounds each Log received.
        let mut trace = Vec::new();
        let mut step = |step: &str, sent: bool| {
            let received = rxs.iter_mut().map(|rx| {
                std::iter::from_fn(|| rx.try_recv().ok())
                    .map(|r| {
                        let b = r.barrier.unwrap();
                        format!(
                            "({}, @{} quota {} caught_up {} flush {})",
                            r.appends.len(),
                            b.adjusted_clock,
                            b.quota,
                            b.caught_up,
                            b.flush_cycle,
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            });
            let received: Vec<_> = received.collect();
            trace.push(format!("{step} -> sent: {sent}, received: {received:?}"));
        };

        // Logs presume a Slice isn't caught up at zero: nothing is sent.
        step("send", rounds.send(false, None).unwrap());
        // A change of caught up goes to every Log.
        step("send(caught up)", rounds.send(true, None).unwrap());
        step("send(caught up)", rounds.send(true, None).unwrap());

        // A round with consumed quota goes to every Log, whether it has Appends or not.
        _ = rounds.try_charge(100, clock(50));
        rounds.logs[0].buffer_append("a/journal", Default::default());
        _ = rounds.try_charge(10, clock(40));
        rounds.logs[0].buffer_append("a/journal", Default::default());
        step(
            "consume x2, buffer(0) x2, send",
            rounds.send(false, None).unwrap(),
        );

        // A flush request goes to every Log, though nothing was consumed.
        step("send(flush 5)", rounds.send(false, flush).unwrap());
        step("send", rounds.send(false, None).unwrap());

        // Consumption of a later document advances the position.
        _ = rounds.try_charge(20, clock(70));
        rounds.logs[1].buffer_append("a/journal", Default::default());
        step(
            "consume, buffer(1), send(caught up)",
            rounds.send(true, None).unwrap(),
        );

        insta::assert_snapshot!(trace.join("\n"));
    }
}
