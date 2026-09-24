//! What a tenure's journal writer owes and holds, as data.
//!
//! A [`Ledger`] is every decision the writer makes about its deltas, apart from the
//! I/O which carries them out: which state the tenure is in, whether mutations are
//! taken, how each record is stamped, when a delta is cut and what its
//! acknowledgement is, whether an acknowledgement may be appended, when a delta's
//! first record asks to open a recovery horizon, and which failure a request is
//! told. The writer's task appends, flushes, and asks the disk's compactor, and
//! tells the ledger what each of those found.
//!
//! A tenure appends until it is abandoned or fails, and leaves that state once, in
//! one direction. It holds at most one prepared delta, whose acknowledgement its
//! client holds, and takes no mutation until that acknowledgement has landed.

use crate::failure;
use crate::proto;
use proto_gazette::uuid;

/// What one tenure's writer owes and holds.
pub(super) struct Ledger {
    /// Producer of every record this tenure stamps.
    epoch: uuid::Producer,
    /// Clock of the last record stamped. It only advances.
    clock: uuid::Clock,
    state: State,
    /// Records stamped into the delta which is accumulating now. It returns to zero
    /// at each cut, which is where one delta ends and the next begins.
    delta_records: usize,
    /// Offset of the horizon this tenure opened or resumed. It is not yet complete.
    horizon: Option<i64>,
    /// Set once the owner has released its half of the recording channel. It is
    /// orthogonal to the state: a tenure which still appends may have no device left
    /// to append for.
    drained: bool,
}

/// Whether a tenure still appends, and what it owes if it does.
enum State {
    /// Appending under the claim this tenure installed. `prepared` is the delta which
    /// was cut and whose acknowledgement the client holds, and while it is `Some`
    /// the writer takes nothing — see [`Ledger::taking`].
    Appending { prepared: Option<Prepared> },
    /// The tenure was abandoned. It did not fail, so every request is refused for the
    /// tenure being over rather than for a failure.
    ///
    /// A delta this tenure had prepared goes with it. Nothing can commit it: an
    /// `Acknowledge` of it is refused here exactly as a `Prepare` is.
    Abandoned,
    /// A failure ended the tenure. It is kept here when no request was waiting on
    /// it, for the next request to take, and reported to that request alone.
    Failed(Option<anyhow::Error>),
}

/// A delta which was cut, and whose acknowledgement its client holds.
struct Prepared {
    /// Exact bytes returned to the client, which its `Acknowledge` must repeat.
    ack: bytes::Bytes,
    /// Whether the cut found the open horizon discharged. Committing this delta then
    /// completes that horizon and moves the recovery floor to it.
    completes_horizon: bool,
}

/// How a tenure came to stop, which decides how the writer reports it.
#[derive(Debug, PartialEq)]
pub(super) enum Stopped {
    /// It was already over, so nothing changed.
    AlreadyOver,
    /// Its own end cancelled what failed, which is the teardown working.
    Ended,
    /// It failed.
    Failed,
}

impl Ledger {
    /// A tenure which appends under `epoch`, at the start of a delta it has taken
    /// nothing into, resuming the `horizon` its replay left open.
    pub fn new(epoch: uuid::Producer, horizon: Option<i64>) -> Self {
        Self {
            epoch,
            clock: uuid::Clock::zero(),
            state: State::Appending { prepared: None },
            delta_records: 0,
            horizon,
            drained: false,
        }
    }

    /// Whether the tenure is still appending, rather than abandoned or failed.
    pub fn appending(&self) -> bool {
        matches!(self.state, State::Appending { .. })
    }

    /// Whether mutations are taken from the recording channel.
    ///
    /// Taking stops while a prepared delta awaits its commit. Every record carries
    /// the tenure's epoch as its producer, and Gazette sequences per producer: an
    /// `ACK_TXN` commits the pending records of its producer with clocks at or below
    /// its own and drops the rest, and the acknowledgement's clock was fixed at the
    /// cut. A record of the next delta which reached the journal ahead of it would
    /// be lost, so none is taken until it has landed. This is the rule the Gazette
    /// consumer framework applies between one transaction's `StartCommit` and the
    /// last transaction's pending acknowledgement.
    ///
    /// Meanwhile the device parks after a queue depth of mutations, so a workload
    /// which writes heavily across a boundary waits for `Acknowledge`, and a client
    /// keeps that interval short.
    ///
    /// A tenure which appends no more keeps taking, because an unmount writes and a
    /// device whose mutations nothing takes cannot be unmounted.
    pub fn taking(&self) -> bool {
        !self.drained
            && match &self.state {
                State::Appending { prepared } => prepared.is_none(),
                State::Abandoned | State::Failed(_) => true,
            }
    }

    /// Whether the delta accumulating now holds no record.
    pub fn delta_is_empty(&self) -> bool {
        self.delta_records == 0
    }

    /// Offset of the horizon this tenure opened or resumed, which is not yet
    /// complete.
    pub fn horizon(&self) -> Option<i64> {
        self.horizon
    }

    /// The journal range a horizon is judged against, where the next mutation begins
    /// a delta and no horizon is open. `head` and `floor` are the journal's.
    ///
    /// The decision is taken at the record which would carry the flag, and not at
    /// the cut before it. Both terms of the comparison have moved since that cut:
    /// the range is what a replay would read now, and the allocated size the disk's
    /// compactor weighs it against is what a horizon would have to discharge now.
    pub fn horizon_range(&self, head: i64, floor: i64) -> Option<u64> {
        if !self.delta_is_empty() || self.horizon.is_some() {
            return None;
        }
        Some(head.saturating_sub(floor).max(0) as u64)
    }

    /// Stamp `chunks`, one mutation, as this delta's next record at `now`. `opens`
    /// marks the delta's first record as opening a recovery horizon. Only that record
    /// carries the flag, however many follow it: a reader which starts at the horizon
    /// must see every chunk which discharges it.
    pub fn stamp_mutation(
        &mut self,
        now: std::time::SystemTime,
        chunks: Vec<proto::Chunk>,
        opens: bool,
    ) -> proto::DiskRecord {
        assert!(
            !opens || self.delta_is_empty(),
            "only a delta's first record opens a horizon"
        );
        let record = self.stamp(now, uuid::Flags::CONTINUE_TXN, chunks, opens);
        self.delta_records += 1;

        record
    }

    /// The record which opened a horizon has landed at `begin`, which is that
    /// horizon's offset.
    pub fn on_horizon_opened(&mut self, begin: i64) {
        self.horizon = Some(begin);
    }

    /// Refuse a `Prepare` of a tenure which is over, or which holds a prepared delta
    /// still awaiting its commit.
    pub fn begin_prepare(&mut self, ended: bool) -> anyhow::Result<()> {
        () = self.check(ended)?;

        if let State::Appending {
            prepared: Some(_), ..
        } = self.state
        {
            return Err(anyhow::Error::new(failure::Failure::OutOfOrder(
                "a prepared delta is still awaiting its commit".to_string(),
            )));
        }
        Ok(())
    }

    /// Cut the delta at `now`: stamp the acknowledgement which commits it, hold that
    /// as prepared, and begin the next delta. `completes_horizon` is whether the cut
    /// found the open horizon discharged, which is sampled here at the cut and not at
    /// the commit: mutations admitted between the two belong to the next delta.
    ///
    /// Every record of the delta is stamped already, so its acknowledgement's clock
    /// is above all of theirs and commits every one.
    pub fn cut(&mut self, now: std::time::SystemTime, completes_horizon: bool) -> bytes::Bytes {
        assert!(!self.delta_is_empty(), "an empty delta is not cut");

        let record = self.stamp(now, uuid::Flags::ACK_TXN, Vec::new(), false);
        let mut buf = bytes::BytesMut::new();
        proto_gazette::fixed_framing::encode(&record, &mut buf);
        let ack = buf.freeze();

        let State::Appending { prepared } = &mut self.state else {
            panic!("a tenure which is over cuts nothing");
        };
        *prepared = Some(Prepared {
            ack: ack.clone(),
            completes_horizon,
        });
        self.delta_records = 0;

        ack
    }

    /// Take the prepared delta which `ack` commits, refusing an `Acknowledge` of a
    /// tenure which is over, of no prepared delta, or of other bytes than were
    /// prepared. Report whether committing it completes the open horizon.
    ///
    /// The writer takes mutations again once this has been called, per
    /// [`Ledger::taking`], so it is called only as the acknowledgement is appended.
    pub fn begin_acknowledge(&mut self, ended: bool, ack: &[u8]) -> anyhow::Result<bool> {
        () = self.check(ended)?;

        let State::Appending { prepared } = &mut self.state else {
            panic!("a checked tenure is appending");
        };
        let Some(prepared) = prepared.take() else {
            return Err(anyhow::Error::new(failure::Failure::OutOfOrder(
                "no prepared delta is awaiting a commit".to_string(),
            )));
        };
        if ack != prepared.ack {
            return Err(anyhow::Error::new(failure::Failure::OutOfOrder(
                "commit acknowledgement differs from the prepared one".to_string(),
            )));
        }
        Ok(prepared.completes_horizon)
    }

    /// Complete the open horizon, which an acknowledgement just committed, and
    /// report the recovery floor it establishes.
    ///
    /// Its opening record now has a committed copy of every allocated block at or
    /// after it, so a replay may begin there.
    pub fn complete_horizon(&mut self) -> i64 {
        self.horizon
            .take()
            .expect("an acknowledgement completes only an open horizon")
    }

    /// The owner released its half of the recording channel, so nothing more will
    /// be taken.
    pub fn on_drained(&mut self) {
        self.drained = true;
    }

    /// The tenure was abandoned, if it was still appending.
    pub fn abandon(&mut self) {
        if self.appending() {
            self.state = State::Abandoned;
        }
    }

    /// End the tenure over a failure which a request is told of. `ended` is whether
    /// the tenure's own cancellation has fired.
    ///
    /// A tenure which is already over did not fail. An append which the tenure's own
    /// end cancelled is the teardown working.
    pub fn stop(&mut self, ended: bool) -> Stopped {
        if !self.appending() {
            return Stopped::AlreadyOver;
        }
        self.state = State::Failed(None);

        match ended {
            true => Stopped::Ended,
            false => Stopped::Failed,
        }
    }

    /// Keep `err`, a failure which [`Ledger::stop`] ended the tenure over and no
    /// request is waiting on, for the next request to take. Only the failure which
    /// ended the tenure is kept.
    pub fn keep(&mut self, err: anyhow::Error) {
        if let State::Failed(kept @ None) = &mut self.state {
            *kept = Some(err);
        }
    }

    /// Refuse a request once the tenure is over. `ended` is whether the tenure's own
    /// cancellation has fired, which a drain does ahead of the writer abandoning.
    ///
    /// A failure nothing has reported is reported here, to the next request and to
    /// it alone. Requests after that are refused for the tenure being over, because
    /// the client which holds the failure holds its causes.
    pub fn check(&mut self, ended: bool) -> anyhow::Result<()> {
        match &mut self.state {
            State::Appending { .. } if ended => anyhow::bail!("the tenure ended"),
            State::Appending { .. } => Ok(()),
            State::Abandoned => anyhow::bail!("the tenure ended"),
            State::Failed(kept) => match kept.take() {
                Some(err) => Err(err),
                None => anyhow::bail!("the tenure has failed"),
            },
        }
    }

    /// Build the tenure's next record at `now`.
    ///
    /// Every record but a fence carries the tenure's epoch as its producer, which is
    /// why the writer takes nothing while an acknowledgement is outstanding: see
    /// [`Ledger::taking`].
    ///
    /// The clock only advances. It therefore orders each delta's records ahead of
    /// the acknowledgement which commits them, and ahead of every record of the
    /// prior delta. It also follows the wall clock. A recovery floor is the clock of
    /// a horizon's opening record, and a recovery turns that clock back into the
    /// modification time of the fragments to read from.
    fn stamp(
        &mut self,
        now: std::time::SystemTime,
        flags: uuid::Flags,
        chunks: Vec<proto::Chunk>,
        opens_horizon: bool,
    ) -> proto::DiskRecord {
        self.clock.update(uuid::Clock::from_time(now));
        let clock = self.clock.tick();

        proto::DiskRecord {
            uuid: super::uuid_bytes(self.epoch, clock, flags),
            chunks,
            opens_horizon,
            installs_epoch: bytes::Bytes::new(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Ledger, Stopped};
    use proto_gazette::uuid;
    use std::fmt::Write as _;

    /// A wall clock this many seconds after the epoch.
    fn at(seconds: u64) -> std::time::SystemTime {
        std::time::UNIX_EPOCH + std::time::Duration::from_secs(seconds)
    }

    fn ledger() -> Ledger {
        Ledger::new(uuid::Producer::from_bytes([1, 0, 0, 0, 0, 1]), None)
    }

    /// The flags and clock of a stamped record.
    fn parsed(uuid: &[u8]) -> (uuid::Clock, uuid::Flags) {
        let (_producer, clock, flags) = uuid::parse(uuid::Uuid::from_slice(uuid).unwrap()).unwrap();
        (clock, flags)
    }

    #[derive(Debug, Clone, Copy)]
    enum Step {
        /// A mutation arrives. The writer takes it only while the ledger is taking,
        /// stamps it only while the tenure appends, and asks that it open a horizon if
        /// `.0` and it begins a delta.
        Mutation(bool),
        /// The first record of a delta opened a horizon, which landed at this offset.
        HorizonOpened(i64),
        /// The journal's head and floor are these, and a horizon's range is asked.
        Range(i64, i64),
        /// A client prepares: a cut which finds the open horizon discharged if `.0`.
        Prepare(bool),
        /// A client acknowledges exactly what was last prepared.
        Ack,
        /// A client acknowledges other bytes.
        AckOther,
        /// The writer completes the horizon an acknowledgement committed.
        Complete,
        /// A failure no request is waiting on.
        Fail(&'static str),
        /// The tenure's own cancellation fires, as a drain's does.
        Ended,
        /// The writer abandons the ledger once it sees that cancellation.
        Abandon,
        Drained,
    }
    use Step::*;

    /// Run `steps` as the writer's task drives the ledger, and render what each
    /// returned and whether the writer then takes mutations, how many records its
    /// delta holds, and its open horizon. A request which is refused ends the tenure,
    /// as it does the writer's.
    fn trace(steps: &[Step]) -> String {
        let mut ledger = ledger();
        let mut out = String::new();
        let mut last_ack = bytes::Bytes::new();
        let (mut now, mut ended) = (0, false);

        // A refused request stops the ledger, and says how.
        fn refused(ledger: &mut Ledger, ended: bool, err: anyhow::Error) -> String {
            format!("refused: {err:#}; {:?}", ledger.stop(ended))
        }

        for &step in steps {
            now += 1;
            let outcome = match step {
                Mutation(_) if !ledger.taking() => "left in the channel".to_string(),
                Mutation(_) if !ledger.appending() || ended => "discarded".to_string(),
                Mutation(opens) => {
                    let opens = opens && ledger.delta_is_empty();
                    let record = ledger.stamp_mutation(at(now), Vec::new(), opens);
                    format!("stamped, opens {}", record.opens_horizon)
                }
                HorizonOpened(begin) => {
                    ledger.on_horizon_opened(begin);
                    "-".to_string()
                }
                Range(head, floor) => format!("range {:?}", ledger.horizon_range(head, floor)),
                Prepare(completes) => match ledger.begin_prepare(ended) {
                    Err(err) => refused(&mut ledger, ended, err),
                    Ok(()) if ledger.delta_is_empty() => "nothing to commit".to_string(),
                    Ok(()) => {
                        last_ack = ledger.cut(at(now), completes);
                        "prepared".to_string()
                    }
                },
                Ack | AckOther => {
                    let ack = match step {
                        Ack => last_ack.clone(),
                        _ => bytes::Bytes::from_static(b"other"),
                    };
                    match ledger.begin_acknowledge(ended, &ack) {
                        Ok(completes) => format!("acknowledged, completes horizon {completes}"),
                        Err(err) => refused(&mut ledger, ended, err),
                    }
                }
                Complete => format!("floor {}", ledger.complete_horizon()),
                Fail(what) => {
                    let stopped = ledger.stop(ended);
                    ledger.keep(anyhow::anyhow!(what));
                    format!("{stopped:?}")
                }
                Ended => {
                    ended = true;
                    "-".to_string()
                }
                Abandon => {
                    ledger.abandon();
                    "-".to_string()
                }
                Drained => {
                    ledger.on_drained();
                    "-".to_string()
                }
            };
            let step = format!("{step:?}");

            writeln!(
                out,
                "{step:<28}{outcome:<72}taking {:<6}delta {}  horizon {:?}",
                ledger.taking(),
                ledger.delta_records,
                ledger.horizon(),
            )
            .unwrap();
        }
        out
    }

    /// A delta is cut, held while its client commits, and acknowledged. The writer
    /// takes nothing between the two, and an empty delta commits nothing.
    #[test]
    fn test_a_delta_is_prepared_and_acknowledged() {
        let trace = trace(&[
            Prepare(false),
            Mutation(false),
            Mutation(false),
            Prepare(false),
            Mutation(false),
            Ack,
            Mutation(false),
            Prepare(false),
            Ack,
        ]);
        insta::assert_snapshot!(trace, @"
        Prepare(false)              nothing to commit                                                       taking true  delta 0  horizon None
        Mutation(false)             stamped, opens false                                                    taking true  delta 1  horizon None
        Mutation(false)             stamped, opens false                                                    taking true  delta 2  horizon None
        Prepare(false)              prepared                                                                taking false delta 0  horizon None
        Mutation(false)             left in the channel                                                     taking false delta 0  horizon None
        Ack                         acknowledged, completes horizon false                                   taking true  delta 0  horizon None
        Mutation(false)             stamped, opens false                                                    taking true  delta 1  horizon None
        Prepare(false)              prepared                                                                taking false delta 0  horizon None
        Ack                         acknowledged, completes horizon false                                   taking true  delta 0  horizon None
        ");
    }

    /// Only one delta is prepared at a time, only the exact acknowledgement which was
    /// prepared commits it, and it commits once. Any other request is refused, and
    /// ends the tenure.
    #[test]
    fn test_an_out_of_order_request_ends_the_tenure() {
        let trace = [
            trace(&[
                Mutation(false),
                Prepare(false),
                Prepare(false),
                Prepare(false),
            ]),
            trace(&[Mutation(false), Prepare(false), AckOther, Ack]),
            trace(&[Ack, Mutation(false)]),
        ]
        .join("\n");
        insta::assert_snapshot!(trace, @"
        Mutation(false)             stamped, opens false                                                    taking true  delta 1  horizon None
        Prepare(false)              prepared                                                                taking false delta 0  horizon None
        Prepare(false)              refused: a prepared delta is still awaiting its commit; Failed          taking true  delta 0  horizon None
        Prepare(false)              refused: the tenure has failed; AlreadyOver                             taking true  delta 0  horizon None

        Mutation(false)             stamped, opens false                                                    taking true  delta 1  horizon None
        Prepare(false)              prepared                                                                taking false delta 0  horizon None
        AckOther                    refused: commit acknowledgement differs from the prepared one; Failed   taking true  delta 0  horizon None
        Ack                         refused: the tenure has failed; AlreadyOver                             taking true  delta 0  horizon None

        Ack                         refused: no prepared delta is awaiting a commit; Failed                 taking true  delta 0  horizon None
        Mutation(false)             discarded                                                               taking true  delta 0  horizon None
        ");
    }

    /// A delta's first record asks to open a horizon over the journal's range, and a
    /// delta which the cut found discharging it moves the floor when it commits.
    #[test]
    fn test_a_horizon_opens_at_a_deltas_first_record_and_completes_at_its_commit() {
        let trace = trace(&[
            Range(900, 100),
            Mutation(true),
            Range(900, 100),
            HorizonOpened(1000),
            Mutation(false),
            Prepare(false),
            Ack,
            // Open already, so no delta asks again.
            Range(2000, 100),
            Mutation(false),
            Prepare(true),
            Ack,
            Complete,
            Range(3000, 1000),
            // A floor ahead of the head asks over no range at all.
            Range(10, 1000),
        ]);
        insta::assert_snapshot!(trace, @"
        Range(900, 100)             range Some(800)                                                         taking true  delta 0  horizon None
        Mutation(true)              stamped, opens true                                                     taking true  delta 1  horizon None
        Range(900, 100)             range None                                                              taking true  delta 1  horizon None
        HorizonOpened(1000)         -                                                                       taking true  delta 1  horizon Some(1000)
        Mutation(false)             stamped, opens false                                                    taking true  delta 2  horizon Some(1000)
        Prepare(false)              prepared                                                                taking false delta 0  horizon Some(1000)
        Ack                         acknowledged, completes horizon false                                   taking true  delta 0  horizon Some(1000)
        Range(2000, 100)            range None                                                              taking true  delta 0  horizon Some(1000)
        Mutation(false)             stamped, opens false                                                    taking true  delta 1  horizon Some(1000)
        Prepare(true)               prepared                                                                taking false delta 0  horizon Some(1000)
        Ack                         acknowledged, completes horizon true                                    taking true  delta 0  horizon Some(1000)
        Complete                    floor 1000                                                              taking true  delta 0  horizon None
        Range(3000, 1000)           range Some(2000)                                                        taking true  delta 0  horizon None
        Range(10, 1000)             range Some(0)                                                           taking true  delta 0  horizon None
        ");
    }

    /// A failure no request waited on reaches the next request, and it alone. A
    /// tenure which is over keeps taking, so its device can still be unmounted, but
    /// appends nothing.
    #[test]
    fn test_a_failure_is_reported_once_and_the_tenure_keeps_taking() {
        let trace = trace(&[
            Mutation(false),
            Fail("the broker refused"),
            Fail("a second failure"),
            Mutation(false),
            Prepare(false),
            Prepare(false),
            Drained,
            Mutation(false),
        ]);
        insta::assert_snapshot!(trace, @r#"
        Mutation(false)             stamped, opens false                                                    taking true  delta 1  horizon None
        Fail("the broker refused")  Failed                                                                  taking true  delta 1  horizon None
        Fail("a second failure")    AlreadyOver                                                             taking true  delta 1  horizon None
        Mutation(false)             discarded                                                               taking true  delta 1  horizon None
        Prepare(false)              refused: the broker refused; AlreadyOver                                taking true  delta 1  horizon None
        Prepare(false)              refused: the tenure has failed; AlreadyOver                             taking true  delta 1  horizon None
        Drained                     -                                                                       taking false delta 1  horizon None
        Mutation(false)             left in the channel                                                     taking false delta 1  horizon None
        "#);
    }

    /// An abandoned tenure refuses every request for being over, including the
    /// acknowledgement of a delta it had prepared, and discards what it takes. A
    /// request which the tenure's own end refuses is that end rather than a failure.
    #[test]
    fn test_an_ended_tenure_commits_nothing() {
        let trace = [
            trace(&[
                Mutation(false),
                Prepare(false),
                Ended,
                Abandon,
                Ack,
                Mutation(false),
            ]),
            // A drain cancels the tenure under a request already in flight, ahead of
            // the writer abandoning it.
            trace(&[Mutation(false), Ended, Prepare(false), Mutation(false)]),
        ]
        .join("\n");
        insta::assert_snapshot!(trace, @"
        Mutation(false)             stamped, opens false                                                    taking true  delta 1  horizon None
        Prepare(false)              prepared                                                                taking false delta 0  horizon None
        Ended                       -                                                                       taking false delta 0  horizon None
        Abandon                     -                                                                       taking true  delta 0  horizon None
        Ack                         refused: the tenure ended; AlreadyOver                                  taking true  delta 0  horizon None
        Mutation(false)             discarded                                                               taking true  delta 0  horizon None

        Mutation(false)             stamped, opens false                                                    taking true  delta 1  horizon None
        Ended                       -                                                                       taking true  delta 1  horizon None
        Prepare(false)              refused: the tenure ended; Ended                                        taking true  delta 1  horizon None
        Mutation(false)             discarded                                                               taking true  delta 1  horizon None
        ");
    }

    /// A request which fails because the tenure's own end cancelled it is the
    /// teardown working, and is told apart from a failure.
    #[test]
    fn test_a_cancelled_request_is_an_end_rather_than_a_failure() {
        let mut ended = ledger();
        let mut failed = ledger();

        assert_eq!(ended.stop(true), Stopped::Ended);
        assert_eq!(failed.stop(false), Stopped::Failed);
        assert_eq!(failed.stop(false), Stopped::AlreadyOver);
    }

    /// Clocks only advance, whatever the wall clock does, so each delta's records
    /// order ahead of the acknowledgement which commits them and ahead of every
    /// record of the next delta.
    #[test]
    fn test_clocks_advance_though_the_wall_clock_steps_back() {
        let mut ledger = ledger();

        let first = ledger.stamp_mutation(at(100), Vec::new(), false);
        let second = ledger.stamp_mutation(at(50), Vec::new(), false);
        let ack = ledger.cut(at(40), false);
        () = ledger.begin_acknowledge(false, &ack).map(|_| ()).unwrap();
        let next = ledger.stamp_mutation(at(200), Vec::new(), false);

        let mut ack = bytes::BytesMut::from(&ack[..]);
        let proto_gazette::fixed_framing::Frame::Record { message: ack, .. } =
            proto_gazette::fixed_framing::unpack::<crate::proto::DiskRecord>(&mut ack).unwrap()
        else {
            panic!("an acknowledgement is one framed record");
        };

        let stamped = [&first, &second, &ack, &next].map(|record| parsed(&record.uuid));
        for pair in stamped.windows(2) {
            assert!(pair[0].0 < pair[1].0, "a clock stepped back: {stamped:?}");
        }
        assert!(stamped[2].1.is_ack(), "the cut stamped {:?}", stamped[2].1);
        assert!(!stamped[0].1.is_ack() && !stamped[3].1.is_ack());
    }
}
