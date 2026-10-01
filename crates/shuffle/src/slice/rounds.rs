use super::{
    read::{Meta, ReadyRead},
    routing,
    state::{ConstraintState, HeapState},
};
use proto_flow::shuffle;
use tokio::sync::mpsc;

/// Rounds is the state of a Slice's sends to its Logs, and the merge
/// constraint which closes each round.
/// See "Rounds and Merge Constraints" of the crate README.
pub struct Rounds {
    /// Channels to shard Log RPCs, indexed by shard index.
    pub logs: Vec<LogChannel>,
    /// State machine for tracking the Slice's merge constraint.
    pub constraint: ConstraintState,
    /// Re-usable scratch buffer for the packed key of an Append.
    packed_key: bytes::BytesMut,
    /// Re-usable scratch buffer for the target Logs of an Append.
    targets: Vec<usize>,
}

/// LogChannel is a Slice's request channel to one Log, and the round of
/// Appends queued for it. A round is sent with the Slice's merge constraint as
/// a single LogRequest, which the receiver (the LogActor, or an h2 stream's encoder)
/// takes as a whole with one wake.
///
/// It also provides sliding-window flow control of the Log's Appends, within
/// a window of `APPEND_WINDOW_BYTES`. Appends are outstanding from when
/// they're queued into a round until the receiver takes that round from the
/// channel, which acknowledges it. Acks are implicit: LogChannel is the
/// channel's only sender, and each round is one message, so the channel holds
/// exactly `max_capacity - capacity` unacked rounds. The window is updated
/// with acks once, as each round begins, and is then fixed for the round.
pub struct LogChannel {
    tx: mpsc::Sender<shuffle::LogRequest>,
    /// Journal of the last Append queued to this Log, for delta encoding.
    prev_journal: String,
    /// Appends of the current round, not yet sent.
    round: Vec<shuffle::log_request::Append>,
    /// Bytes of Appends of the current round.
    round_bytes: usize,
    /// Bytes of each round in flight: sent into the channel, and not yet
    /// acked as of the current round's beginning. Oldest first, and at most
    /// the channel's capacity.
    in_flight_rounds: std::collections::VecDeque<usize>,
    /// Sum of `in_flight_rounds`.
    in_flight_bytes: usize,
}

impl Rounds {
    pub fn new(log_request_tx: Vec<mpsc::Sender<shuffle::LogRequest>>) -> Self {
        Self {
            logs: log_request_tx.into_iter().map(LogChannel::new).collect(),
            constraint: ConstraintState::new(),
            packed_key: bytes::BytesMut::new(),
            targets: Vec::new(),
        }
    }

    /// Begin a round, ensuring each LogChannel has capacity for it and
    /// updating its window with acks of in-flight rounds. If a round cannot
    /// begin because a Log lacks capacity, return its index as an error.
    pub fn try_begin(&mut self) -> Result<(), usize> {
        match self.logs.iter_mut().position(|log| !log.try_begin_round()) {
            Some(log) => Err(log),
            None => Ok(()),
        }
    }

    /// Try to queue Appends of `ready_read` into the current round of its
    /// target Logs (all-or-nothing). Returns `Err((log, acks))` with the
    /// index of a Log whose window doesn't fit the Append, and the number of
    /// acks it must receive before it would (`LogChannel::try_admit`).
    pub fn try_queue(
        &mut self,
        binding: &crate::Binding,
        journal: &str,
        shards: &[shuffle::Shard],
        ready_read: &ReadyRead,
    ) -> Result<(), (usize, usize)> {
        let Self {
            logs,
            constraint,
            packed_key,
            targets,
        } = self;

        let ReadyRead {
            doc,
            meta:
                Meta {
                    begin_offset,
                    end_offset,
                    clock,
                    producer,
                    ..
                },
            ..
        } = ready_read;

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
        let r_clock = routing::rotate_clock(*clock);

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
            ?producer,
            ?clock,
            begin_offset,
            key_hash,
            flags = ready_read.meta.flags.0,
            r_clock,
            ?targets,
            "routed document Append to Log RPC shards"
        );

        // All-or-nothing: every target's window must fit this Append.
        let bytes = crate::merge::append_bytes(doc.bytes(), packed_key);
        for &target in targets.iter() {
            logs[target]
                .try_admit(bytes)
                .map_err(|acks| (target, acks))?;
        }

        // Journal names are delta-encoded per target, as each is queued.
        let append = shuffle::log_request::Append {
            journal_name_truncate_delta: 0,
            journal_name_suffix: String::new(),
            binding: binding.index as u32,
            priority: binding.priority,
            read_delay: binding.read_delay.as_u64(),
            producer: producer.as_i64(),
            clock: clock.as_u64(),
            flags: ready_read.meta.flags.0 as u32,
            packed_key: packed_key.split().freeze(),
            doc_archived: doc.bytes().clone(),
            source_byte_length: (end_offset - begin_offset).try_into().unwrap(),
        };
        if let Some((&last, rest)) = targets.split_last() {
            for &target in rest {
                logs[target].queue(journal, bytes, append.clone());
            }
            logs[last].queue(journal, bytes, append);
        }
        constraint.on_append(binding.merge_position(*clock));

        Ok(())
    }

    /// Close the current round with an optional `flush`. A round is always
    /// sent to any Log having queued Appends. It broadcasts to every Log if
    /// `flush`, or if the Slice's merge constraint has changed from that which
    /// was last reported to the Log. Returns whether any Log was sent to.
    pub fn close(
        &mut self,
        heap: HeapState,
        flush: Option<shuffle::log_request::Flush>,
    ) -> anyhow::Result<bool> {
        let (constraint, broadcast) = self.constraint.close_round(heap, flush.is_some());

        // A round which goes to no Log has no flush and an unchanged constraint,
        // so `close_round` recording it anyway is a no-op.
        if !broadcast && self.logs.iter().all(|log| log.round.is_empty()) {
            return Ok(false);
        }

        tracing::trace!(?constraint, ?flush, broadcast, "sending round");
        let constraint = constraint.to_proto();

        for log in self.logs.iter_mut() {
            if !broadcast && log.round.is_empty() {
                continue;
            }
            log.send_round(constraint, flush)?;
        }
        Ok(true)
    }
}

impl LogChannel {
    fn new(tx: mpsc::Sender<shuffle::LogRequest>) -> Self {
        Self {
            tx,
            prev_journal: String::new(),
            round: Vec::new(),
            round_bytes: 0,
            in_flight_rounds: Default::default(),
            in_flight_bytes: 0,
        }
    }

    /// Attempt to begin a round by crediting the window with acks of in-flight
    /// rounds, taken through a snapshot observation of channel capacity.
    /// Returns whether the channel has capacity for a next round.
    fn try_begin_round(&mut self) -> bool {
        let capacity = self.tx.capacity(); // Requires an atomic load.
        let unacked = self.tx.max_capacity() - capacity;

        while self.in_flight_rounds.len() > unacked {
            self.in_flight_bytes -= self.in_flight_rounds.pop_front().unwrap();
        }
        capacity != 0
    }

    /// Try to admit an Append of `bytes` into the round, within the window.
    /// If the window doesn't fit it, returns the number of acks which must be
    /// received before it would: of in-flight rounds, oldest first, and then
    /// of the current round once it's sent.
    ///
    /// An Append always fits if nothing else is outstanding,
    /// which allows one oversize document at a time.
    fn try_admit(&self, bytes: usize) -> Result<(), usize> {
        let fits_window = |outstanding: usize| {
            outstanding == 0 || outstanding + bytes <= crate::merge::APPEND_WINDOW_BYTES
        };

        let mut outstanding = self.in_flight_bytes + self.round_bytes;
        if fits_window(outstanding) {
            return Ok(());
        }
        let rounds = self.in_flight_rounds.iter().chain([&self.round_bytes]);

        for (index, round) in rounds.enumerate() {
            outstanding -= round;

            if fits_window(outstanding) {
                return Err(index + 1);
            }
        }
        unreachable!("acks of every outstanding round fit any Append");
    }

    /// Await acks of the `acks` oldest outstanding rounds, as returned by
    /// `try_admit`, resolving `false` if the channel is closed.
    pub fn acked(&self, acks: usize) -> impl Future<Output = bool> + 'static {
        let count = self.tx.max_capacity() - self.in_flight_rounds.len() + acks;
        let tx = self.tx.clone();

        // On Err (channel closed), we don't wake and rely on rx of a
        // causal error / fail-fast teardown.
        async move { tx.reserve_many(count).await.is_ok() }
    }

    /// Queue an Append of `journal`, accounted as `bytes`, into the current round.
    fn queue(&mut self, journal: &str, bytes: usize, append: shuffle::log_request::Append) {
        // Delta-encode the journal name.
        let (journal_name_truncate_delta, journal_name_suffix) =
            gazette::delta::encode(&self.prev_journal, journal);
        let journal_name_suffix = journal_name_suffix.to_string();

        // Retain for next name delta-encoding.
        self.prev_journal.clear();
        self.prev_journal.push_str(journal);

        self.round.push(shuffle::log_request::Append {
            journal_name_truncate_delta,
            journal_name_suffix,
            ..append
        });
        self.round_bytes += bytes;
    }

    /// Send the current round's Appends with `constraint` and `flush`, as one message.
    fn send_round(
        &mut self,
        constraint: shuffle::log_request::MergeConstraint,
        flush: Option<shuffle::log_request::Flush>,
    ) -> anyhow::Result<()> {
        let appends = std::mem::take(&mut self.round);
        self.round.reserve(appends.len());

        let bytes = std::mem::take(&mut self.round_bytes);
        self.in_flight_rounds.push_back(bytes);
        self.in_flight_bytes += bytes;

        // `try_begin_round` acked all but at most `max_capacity - 1` rounds,
        // and a round is sent at most once.
        assert!(
            self.in_flight_rounds.len() <= self.tx.max_capacity(),
            "a round was sent which didn't begin with channel capacity"
        );

        crate::verify_send(
            &self.tx,
            shuffle::LogRequest {
                open: None,
                appends,
                constraint: Some(constraint),
                flush,
            },
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_log_channel_window() {
        const WINDOW: usize = crate::merge::APPEND_WINDOW_BYTES;
        const MAX: usize = 4; // Channel capacity, in rounds.

        let (tx, mut rx) = mpsc::channel(MAX);
        let mut ch = LogChannel::new(tx);

        let queue = |ch: &mut LogChannel, bytes| ch.queue("a/journal", bytes, Default::default());
        let send = |ch: &mut LogChannel| ch.send_round(Default::default(), None).unwrap();
        let take = |rx: &mut mpsc::Receiver<_>| rx.try_recv().unwrap();

        // Steps record their result, and the window which follows it.
        // Bytes are in eighths of a WINDOW.
        let eighths = |bytes: usize| format!("{}/8", bytes as f64 / (WINDOW / 8) as f64);
        let mut trace = Vec::new();
        let mut step = |step: &str, result: &dyn std::fmt::Debug, ch: &LogChannel| {
            trace.push(format!(
                "{step} -> {result:?}\n    in_flight: [{}] = {}, round: {}, capacity: {}",
                ch.in_flight_rounds
                    .iter()
                    .map(|b| eighths(*b))
                    .collect::<Vec<_>>()
                    .join(", "),
                eighths(ch.in_flight_bytes),
                eighths(ch.round_bytes),
                ch.tx.capacity(),
            ))
        };

        // With nothing outstanding, any Append fits, however large.
        step("begin", &ch.try_begin_round(), &ch);
        step("admit(32/8)", &ch.try_admit(WINDOW * 4), &ch);

        // Round one: two Appends queued, then sent. Until it's sent, the
        // current round is the newest outstanding round which may need an ack.
        queue(&mut ch, WINDOW / 2);
        step("admit(4/8)", &ch.try_admit(WINDOW / 2), &ch);
        step("admit(4/8 + 1 byte)", &ch.try_admit(WINDOW / 2 + 1), &ch);
        queue(&mut ch, WINDOW / 4);
        step("send", &send(&mut ch), &ch);

        // Round two: a constraint only, which carries no bytes.
        step("begin", &ch.try_begin_round(), &ch);
        step("send", &send(&mut ch), &ch);

        // Round three: one Append. An Append of a quarter window must await
        // the ack of round one. A full-window Append must await the ack of
        // round three, whether or not it's yet sent.
        step("begin", &ch.try_begin_round(), &ch);
        queue(&mut ch, WINDOW / 8);
        step("admit(2/8)", &ch.try_admit(WINDOW / 4), &ch);
        step("admit(8/8)", &ch.try_admit(WINDOW), &ch);
        step("send", &send(&mut ch), &ch);
        step("admit(2/8)", &ch.try_admit(WINDOW / 4), &ch);
        step("admit(8/8)", &ch.try_admit(WINDOW), &ch);

        // The receiver takes round one, which acks it. The window is updated
        // only as a next round begins, with the ack of round one but not
        // round three.
        take(&mut rx);
        step("take, admit(2/8)", &ch.try_admit(WINDOW / 4), &ch);
        step("begin", &ch.try_begin_round(), &ch);
        step("admit(2/8)", &ch.try_admit(WINDOW / 4), &ch);
        step("admit(8/8)", &ch.try_admit(WINDOW), &ch);

        // Taking everything acks all rounds, as a next round begins.
        take(&mut rx);
        take(&mut rx);
        step("take x2, begin", &ch.try_begin_round(), &ch);
        step("admit(32/8)", &ch.try_admit(WINDOW * 4), &ch);

        // A round may begin only while the channel has capacity for it.
        queue(&mut ch, WINDOW / 2);
        send(&mut ch);
        for _ in 1..MAX {
            assert!(ch.try_begin_round());
            send(&mut ch);
        }
        step("send x4, begin", &ch.try_begin_round(), &ch);

        // Taking the oldest round permits the next, and acks it.
        take(&mut rx);
        step("take, begin", &ch.try_begin_round(), &ch);

        insta::assert_snapshot!(trace.join("\n"));
    }
}
