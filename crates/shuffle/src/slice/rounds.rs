use super::{
    read::{Meta, ReadyRead},
    routing,
    state::HeapTop,
};
use proto_flow::shuffle;
use proto_gazette::uuid;
use tokio::sync::mpsc;

/// Rounds is the state of a Slice's sends to its Logs, and of the merge
/// constraint which its rounds place on them.
/// See "Rounds and Credits" and "Log Merge and Output" of the crate README.
pub struct Rounds {
    /// Channels to shard Log RPCs, indexed by shard index.
    pub logs: Vec<LogChannel>,
    /// Greatest adjusted clock of any queued Append, which floors the merge
    /// constraint of every round.
    max_queued: uuid::Clock,
    /// Re-usable scratch buffer for the packed key of an Append.
    packed_key: bytes::BytesMut,
    /// Re-usable scratch buffer for the target Logs of an Append.
    targets: Vec<usize>,
}

/// LogChannel is a Slice's request channel to one Log, and the round of
/// Appends queued for it. A round is sent as a single LogRequest, which the
/// receiver (the LogActor, or an h2 stream's encoder) takes as a whole with
/// one wake.
///
/// It also provides credit-based flow control of the Log's Appends, within
/// the budget and with the per-Append overhead which the Log advertised
/// (`LogResponse.Opened`). Appends are outstanding from when they're queued
/// into a round until the Log merges them and returns their credits, as a
/// cumulative count of merged bytes (`LogResponse.Acked`).
///
/// Rounds aren't credited: the Log always reads them, so the channel's
/// capacity of rounds is a brief wait, and a round without Appends (a Flush)
/// may be sent even if the Log's credits are exhausted.
pub struct LogChannel {
    tx: mpsc::Sender<shuffle::LogRequest>,
    /// Journal of the last Append queued to this Log, for delta encoding.
    prev_journal: String,
    /// Appends of the current round, not yet sent.
    round: Vec<shuffle::log_request::Append>,
    /// Bytes of Appends of the current round.
    round_bytes: u64,
    /// Budget of outstanding Append bytes, as advertised by the Log.
    credit_bytes: u64,
    /// Bytes accounted for each Append, as advertised by the Log.
    overhead_bytes: u64,
    /// Cumulative bytes of Appends of sent rounds.
    sent_bytes: u64,
    /// Cumulative bytes of Appends which the Log has merged.
    acked_bytes: u64,
    /// Merge constraint of the last round sent, which the Log holds until
    /// the next. A Log presumes zero before a first round.
    constraint: Option<shuffle::log_request::MergeConstraint>,
}

impl Rounds {
    pub fn new(logs: Vec<LogChannel>) -> Self {
        Self {
            logs,
            max_queued: uuid::Clock::zero(),
            packed_key: bytes::BytesMut::new(),
            targets: Vec::new(),
        }
    }

    /// Begin a round, ensuring each LogChannel has capacity for it.
    /// If a round cannot begin because a Log lacks capacity, return its index
    /// as an error.
    pub fn try_begin(&self) -> Result<(), usize> {
        match self.logs.iter().position(|log| log.tx.capacity() == 0) {
            Some(log) => Err(log),
            None => Ok(()),
        }
    }

    /// Try to queue Appends of `ready_read` into the current round of its
    /// target Logs (all-or-nothing). Returns `Err(log)` with the index of a
    /// Log whose outstanding credits don't fit the Append.
    pub fn try_queue(
        &mut self,
        binding: &crate::Binding,
        journal: &str,
        shards: &[shuffle::Shard],
        ready_read: &ReadyRead,
    ) -> Result<(), usize> {
        let Self {
            logs,
            max_queued,
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

        // All-or-nothing: every target's credits must fit this Append.
        if let Some(&target) = targets
            .iter()
            .find(|&&target| !logs[target].admits(doc.bytes(), packed_key))
        {
            return Err(target);
        }

        // Journal names are delta-encoded per target, as each is queued.
        let append = shuffle::log_request::Append {
            journal_name_truncate_delta: 0,
            journal_name_suffix: String::new(),
            binding: binding.index as u32,
            read_delay: binding.read_delay.as_u64(),
            producer: producer.as_i64(),
            clock: clock.as_u64(),
            flags: ready_read.meta.flags.0 as u32,
            packed_key: packed_key.split().freeze(),
            doc_archived: doc.bytes().clone(),
            source_byte_length: (end_offset - begin_offset).try_into().unwrap(),
        };
        *max_queued = (*max_queued).max(crate::merge::adjusted_clock(&append));

        if let Some((&last, rest)) = targets.split_last() {
            for &target in rest {
                logs[target].queue(journal, append.clone());
            }
            logs[last].queue(journal, append);
        }

        Ok(())
    }

    /// Close the current round with the merge constraint of heap `top`, and
    /// an optional `flush`. A round is sent to each Log having queued Appends
    /// or holding a different constraint, and to every Log if `flush`.
    /// Returns whether any Log was sent to.
    ///
    /// The constraint is the adjusted clock of a due or delayed `top`, floored
    /// at the greatest queued Append, or the floor alone if `top` is deferred.
    /// An idle Slice has no constraint.
    ///
    /// The floor keeps the constraint at or above each of the Slice's Appends,
    /// as Logs enforce (`log::read_ahead::SliceReadAhead`), even as its top
    /// regresses. A Slice which awaits credits of a Log then can't hold back
    /// other Logs' merges below its Appends queued there, which is what keeps
    /// the merges of Logs live (see `log::actor::LogActor`).
    pub fn close(
        &mut self,
        top: HeapTop,
        flush: Option<shuffle::log_request::Flush>,
    ) -> anyhow::Result<bool> {
        let floored = |clock: uuid::Clock, delayed| {
            Some(shuffle::log_request::MergeConstraint {
                adjusted_clock: clock.max(self.max_queued).as_u64(),
                delayed,
            })
        };
        let constraint = match top {
            HeapTop::Due(clock) => floored(clock, false),
            HeapTop::Delayed(clock) => floored(clock, true),
            HeapTop::Deferred => floored(uuid::Clock::zero(), false),
            HeapTop::Idle => None,
        };
        let broadcast = flush.is_some();
        let mut sent = false;

        for log in self.logs.iter_mut() {
            if !broadcast && log.round.is_empty() && log.constraint == constraint {
                continue;
            }
            log.send_round(constraint.clone(), flush)?;
            sent = true;
        }
        if sent {
            tracing::trace!(?constraint, ?flush, "sent round");
        }
        Ok(sent)
    }
}

impl LogChannel {
    pub fn new(
        tx: mpsc::Sender<shuffle::LogRequest>,
        credit_bytes: u64,
        overhead_bytes: u64,
    ) -> Self {
        Self {
            tx,
            prev_journal: String::new(),
            round: Vec::new(),
            round_bytes: 0,
            credit_bytes,
            overhead_bytes,
            sent_bytes: 0,
            acked_bytes: 0,
            constraint: Some(Default::default()),
        }
    }

    /// Whether outstanding credits admit an Append of `doc_archived` and
    /// `packed_key` into the round, as accounted by the Log.
    /// An Append is always admitted if nothing else is outstanding.
    fn admits(&self, doc_archived: &[u8], packed_key: &[u8]) -> bool {
        let bytes = crate::merge::append_bytes(self.overhead_bytes, doc_archived, packed_key);
        let outstanding = self.sent_bytes + self.round_bytes - self.acked_bytes;
        outstanding == 0 || outstanding + bytes <= self.credit_bytes
    }

    /// Apply the Log's cumulative `acked` bytes of merged Appends.
    pub fn on_acked(&mut self, acked: u64) -> anyhow::Result<()> {
        if acked < self.acked_bytes || acked > self.sent_bytes {
            anyhow::bail!(
                "Acked bytes {acked} must be between prior Acked {} and sent {}",
                self.acked_bytes,
                self.sent_bytes,
            );
        }
        self.acked_bytes = acked;
        Ok(())
    }

    /// Await capacity of the channel for a next round, resolving `false` if
    /// the channel is closed.
    pub fn capacity(&self) -> impl Future<Output = bool> + 'static {
        let tx = self.tx.clone();

        // On Err (channel closed), we don't wake and rely on rx of a
        // causal error / fail-fast teardown.
        async move { tx.reserve().await.is_ok() }
    }

    /// Queue an Append of `journal` into the current round.
    fn queue(&mut self, journal: &str, append: shuffle::log_request::Append) {
        self.round_bytes += crate::merge::append_bytes(
            self.overhead_bytes,
            &append.doc_archived,
            &append.packed_key,
        );

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
    }

    /// Send the current round's Appends with `constraint` and `flush`, as one
    /// message. The round must have begun with channel capacity (`Rounds::try_begin`).
    fn send_round(
        &mut self,
        constraint: Option<shuffle::log_request::MergeConstraint>,
        flush: Option<shuffle::log_request::Flush>,
    ) -> anyhow::Result<()> {
        let appends = std::mem::take(&mut self.round);
        self.round.reserve(appends.len());
        self.sent_bytes += std::mem::take(&mut self.round_bytes);
        self.constraint = constraint.clone();

        crate::verify_send(
            &self.tx,
            shuffle::LogRequest {
                open: None,
                appends,
                flush,
                constraint,
            },
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_log_channel_credits() {
        const CREDIT: u64 = crate::merge::APPEND_CREDIT_BYTES;
        const MAX: usize = 4; // Channel capacity, in rounds.

        let (tx, mut rx) = mpsc::channel(MAX);
        let mut rounds = Rounds::new(vec![LogChannel::new(tx, CREDIT, 0)]);

        // With zero advertised overhead, an Append's bytes are those of its document.
        let doc = |bytes: u64| vec![0; bytes as usize];
        let admits = |ch: &LogChannel, bytes: u64| ch.admits(&doc(bytes), b"");
        let queue = |ch: &mut LogChannel, bytes: u64| {
            let append = shuffle::log_request::Append {
                doc_archived: doc(bytes).into(),
                ..Default::default()
            };
            ch.queue("a/journal", append)
        };
        let send = |ch: &mut LogChannel| ch.send_round(None, None).unwrap();

        // Steps record their result, and the credits which follow it.
        // Bytes are in eighths of CREDIT.
        let eighths = |bytes: u64| format!("{}/8", bytes as f64 / (CREDIT / 8) as f64);
        let mut trace = Vec::new();
        let mut step = |step: &str, result: &dyn std::fmt::Debug, ch: &LogChannel| {
            trace.push(format!(
                "{step} -> {result:?}\n    sent: {}, acked: {}, round: {}",
                eighths(ch.sent_bytes),
                eighths(ch.acked_bytes),
                eighths(ch.round_bytes),
            ))
        };
        let ch = &mut rounds.logs[0];

        // With nothing outstanding, any Append is admitted, however large.
        step("admits(32/8)", &admits(ch, CREDIT * 4), ch);

        // Appends of the current round are outstanding, as are those sent.
        queue(ch, CREDIT / 2);
        step("admits(4/8)", &admits(ch, CREDIT / 2), ch);
        step("admits(4/8 + 1 byte)", &admits(ch, CREDIT / 2 + 1), ch);
        queue(ch, CREDIT / 4);
        step("send", &send(ch), ch);
        step("admits(2/8)", &admits(ch, CREDIT / 4), ch);
        step("admits(1/8)", &admits(ch, CREDIT / 8), ch);

        // Credits are returned as the Log merges, independent of rounds.
        step("on_acked(4/8)", &ch.on_acked(CREDIT / 2).is_ok(), ch);
        step("admits(6/8)", &admits(ch, CREDIT * 3 / 4), ch);
        step("admits(6/8 + 1 byte)", &admits(ch, CREDIT * 3 / 4 + 1), ch);

        // Acked bytes never decrease, nor exceed those sent.
        step("on_acked(2/8)", &ch.on_acked(CREDIT / 4).unwrap_err(), ch);
        step("on_acked(8/8)", &ch.on_acked(CREDIT).unwrap_err(), ch);

        // Once all are acked, any Append is again admitted.
        step("on_acked(6/8)", &ch.on_acked(CREDIT * 3 / 4).is_ok(), ch);
        step("admits(32/8)", &admits(ch, CREDIT * 4), ch);

        // A round may begin only while every channel has capacity for it,
        // which is independent of credits. One sent round is not yet taken.
        for _ in 1..MAX {
            assert_eq!(rounds.try_begin(), Ok(()));
            send(&mut rounds.logs[0]);
        }
        trace.push(format!("send x3, try_begin -> {:?}", rounds.try_begin()));
        rx.try_recv().unwrap();
        trace.push(format!("take, try_begin -> {:?}", rounds.try_begin()));

        insta::assert_snapshot!(trace.join("\n"));
    }

    #[test]
    fn test_close_routing() {
        let (tx0, mut rx0) = mpsc::channel(4);
        let (tx1, mut rx1) = mpsc::channel(4);
        let mut rounds = Rounds::new(vec![
            LogChannel::new(tx0, crate::merge::APPEND_CREDIT_BYTES, 0),
            LogChannel::new(tx1, crate::merge::APPEND_CREDIT_BYTES, 0),
        ]);
        let flush = Some(shuffle::log_request::Flush { cycle: 5 });

        let zero = HeapTop::Deferred;
        let c100 = HeapTop::Due(uuid::Clock::from_u64(100));

        // Each step records whether a round was sent, and (appends, constraint,
        // flush) of the rounds each Log received.
        let mut trace = Vec::new();
        let mut step = |step: &str, sent: bool| {
            let received = [&mut rx0, &mut rx1].map(|rx| {
                std::iter::from_fn(|| rx.try_recv().ok())
                    .map(|r| {
                        (
                            r.appends.len(),
                            r.constraint.map(|c| c.adjusted_clock),
                            r.flush.map(|f| f.cycle),
                        )
                    })
                    .collect::<Vec<_>>()
            });
            trace.push(format!("{step} -> sent: {sent}, received: {received:?}"));
        };

        // Nothing queued, no flush, and the constraint Logs presume: no round is sent.
        step("close(0)", rounds.close(zero, None).unwrap());

        // A round goes only to Logs having queued Appends,
        // and carries the constraint.
        rounds.logs[0].queue("a/journal", Default::default());
        rounds.logs[0].queue("a/journal", Default::default());
        step("queue(0) x2, close(0)", rounds.close(zero, None).unwrap());

        // A changed constraint goes to every Log whose constraint differs.
        rounds.logs[1].queue("a/journal", Default::default());
        step("queue(1), close(100)", rounds.close(c100, None).unwrap());
        step("close(100)", rounds.close(c100, None).unwrap());
        rounds.logs[0].queue("a/journal", Default::default());
        step(
            "queue(0), close(idle)",
            rounds.close(HeapTop::Idle, None).unwrap(),
        );

        // A flush goes to every Log, with or without Appends.
        rounds.logs[1].queue("a/journal", Default::default());
        step(
            "queue(1), close(idle, flush 5)",
            rounds.close(HeapTop::Idle, flush).unwrap(),
        );

        insta::assert_snapshot!(trace.join("\n"), @r"
        close(0) -> sent: false, received: [[], []]
        queue(0) x2, close(0) -> sent: true, received: [[(2, Some(0), None)], []]
        queue(1), close(100) -> sent: true, received: [[(0, Some(100), None)], [(1, Some(100), None)]]
        close(100) -> sent: false, received: [[], []]
        queue(0), close(idle) -> sent: true, received: [[(1, None, None)], [(0, None, None)]]
        queue(1), close(idle, flush 5) -> sent: true, received: [[(0, None, Some(5))], [(1, None, Some(5))]]
        ");
    }

    #[test]
    fn test_close_constraint_floor() {
        let (tx, _rx) = mpsc::channel(16);
        let mut rounds = Rounds::new(vec![LogChannel::new(
            tx,
            crate::merge::APPEND_CREDIT_BYTES,
            0,
        )]);
        let clock = uuid::Clock::from_u64;

        // Each step closes a round of `top`, having queued Appends through
        // `max_queued`, and records the constraint sent.
        let steps = [
            ("deferred at start", 0, HeapTop::Deferred),
            ("due", 0, HeapTop::Due(clock(100))),
            (
                "due, Appends queued through it",
                120,
                HeapTop::Due(clock(130)),
            ),
            ("due, regressed", 150, HeapTop::Due(clock(110))),
            ("delayed, regressed", 150, HeapTop::Delayed(clock(140))),
            ("delayed", 150, HeapTop::Delayed(clock(200))),
            ("deferred", 150, HeapTop::Deferred),
            ("idle", 150, HeapTop::Idle),
            ("due", 150, HeapTop::Due(clock(160))),
        ];
        let trace: Vec<_> = steps
            .into_iter()
            .map(|(step, max_queued, top)| {
                rounds.max_queued = clock(max_queued);
                rounds.close(top, None).unwrap();

                let constraint = match &rounds.logs[0].constraint {
                    None => "idle".to_string(),
                    Some(c) if c.delayed => format!("delayed @{}", c.adjusted_clock),
                    Some(c) => format!("@{}", c.adjusted_clock),
                };
                let top = match top {
                    HeapTop::Due(clock) => format!("Due({})", clock.as_u64()),
                    HeapTop::Delayed(clock) => format!("Delayed({})", clock.as_u64()),
                    top => format!("{top:?}"),
                };
                format!("{step}: {top}, queued through {max_queued} -> {constraint}")
            })
            .collect();

        insta::assert_snapshot!(trace.join("\n"), @"
        deferred at start: Deferred, queued through 0 -> @0
        due: Due(100), queued through 0 -> @100
        due, Appends queued through it: Due(130), queued through 120 -> @130
        due, regressed: Due(110), queued through 150 -> @150
        delayed, regressed: Delayed(140), queued through 150 -> delayed @150
        delayed: Delayed(200), queued through 150 -> delayed @200
        deferred: Deferred, queued through 150 -> @150
        idle: Idle, queued through 150 -> idle
        due: Due(160), queued through 150 -> @160
        ");
    }
}
