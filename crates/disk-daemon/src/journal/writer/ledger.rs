use crate::proto;
use proto_gazette::uuid;

/// What one tenure's writer owes and holds.
pub(super) struct Ledger {
    /// Producer of every delta record this tenure builds.
    producer: uuid::Producer,
    /// Clock of the last record built. It only advances.
    clock: uuid::Clock,
    /// The delta which was cut and whose acknowledgement the client holds. While it
    /// is held the writer takes nothing: see [`Ledger::taking`].
    prepared: Option<Prepared>,
    /// Records built into the delta which is accumulating now. It returns to zero
    /// at each cut, which is where one delta ends and the next begins.
    delta_records: usize,
    /// Offset of the horizon this tenure opened or resumed. It is not yet complete.
    horizon: Option<i64>,
    /// The failure which ended the tenure while no request was waiting on it. The
    /// next request is told of it, and that request alone.
    unreported: Option<anyhow::Error>,
}

/// A delta which was cut, and whose acknowledgement its client holds.
struct Prepared {
    /// Exact bytes returned to the client, which its `Acknowledge` must repeat.
    ack: bytes::Bytes,
    /// Whether the cut found the open horizon discharged. Committing this delta then
    /// completes that horizon and moves the recovery floor to it.
    completes_horizon: bool,
}

impl Ledger {
    /// A tenure which appends as `producer`, at the start of a delta it has taken
    /// nothing into, resuming the `horizon` its replay left open.
    pub fn new(producer: uuid::Producer, horizon: Option<i64>) -> Self {
        Self {
            producer,
            clock: uuid::Clock::zero(),
            prepared: None,
            delta_records: 0,
            horizon,
            unreported: None,
        }
    }

    /// Whether to consume recorded mutations.
    ///
    /// Pause while a prepared delta awaits acknowledgement. Gazette's `ACK_TXN`
    /// commits this producer's records through its clock and drops later ones.
    /// The ack clock is fixed at the cut, so appending the next delta before
    /// that ack would lose its records. Meanwhile the bounded recording channel
    /// fills and parks device writes.
    ///
    /// [`Ledger::end`] clears the prepared delta so teardown can keep draining
    /// unmount writes without appending them.
    pub fn taking(&self) -> bool {
        self.prepared.is_none()
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

    /// Return the recovery range for a delta's first record if no horizon is open.
    /// Decide here, rather than at the prior cut: both journal range and live
    /// allocation may have changed.
    pub fn horizon_range(&self, head: i64, floor: i64) -> Option<u64> {
        if !self.delta_is_empty() || self.horizon.is_some() {
            return None;
        }
        Some(head.saturating_sub(floor).max(0) as u64)
    }

    /// Build one mutation's `CONTINUE_TXN` record. Only the first record may open
    /// a horizon, ensuring replay from its offset sees every discharging chunk.
    pub fn build_uncommitted(
        &mut self,
        now: std::time::SystemTime,
        chunks: Vec<proto::Chunk>,
        opens: bool,
    ) -> proto::DiskRecord {
        assert!(
            !opens || self.delta_is_empty(),
            "only a delta's first record opens a horizon"
        );
        let record = self.build(now, uuid::Flags::CONTINUE_TXN, chunks, opens);
        self.delta_records += 1;

        record
    }

    /// The record which opened a horizon has landed at `begin`, which is that
    /// horizon's offset.
    pub fn on_horizon_opened(&mut self, begin: i64) {
        self.horizon = Some(begin);
    }

    /// Refuse a `Prepare` of a tenure which is `over`, or which holds a prepared
    /// delta still awaiting its commit.
    pub fn begin_prepare(&mut self, over: bool) -> anyhow::Result<()> {
        () = self.check(over)?;

        if self.prepared.is_some() {
            return Err(proto_grpc::status_to_anyhow(
                tonic::Status::failed_precondition(
                    "a prepared delta is still awaiting its commit".to_string(),
                ),
            ));
        }
        Ok(())
    }

    /// Build and hold the acknowledgement at a clock above every data record.
    /// Sample horizon completion at this cut: mutations admitted before the later
    /// acknowledgement belong to the next delta.
    pub fn cut(&mut self, now: std::time::SystemTime, completes_horizon: bool) -> bytes::Bytes {
        assert!(!self.delta_is_empty(), "an empty delta is not cut");

        let record = self.build(now, uuid::Flags::ACK_TXN, Vec::new(), false);
        let mut buf = bytes::BytesMut::new();
        proto_gazette::fixed_framing::encode(&record, &mut buf);
        let ack = buf.freeze();

        assert!(
            self.prepared.is_none(),
            "a delta is cut only once the one before it has committed"
        );
        self.prepared = Some(Prepared {
            ack: ack.clone(),
            completes_horizon,
        });
        self.delta_records = 0;

        ack
    }

    /// Validate and take the exact prepared acknowledgement, returning any floor
    /// it completes. This enables consumption again, so the actor must append and
    /// confirm the ack before taking another mutation. Append failure ends the
    /// tenure.
    pub fn begin_acknowledge(&mut self, over: bool, ack: &[u8]) -> anyhow::Result<Option<i64>> {
        () = self.check(over)?;

        let Some(prepared) = self.prepared.take() else {
            return Err(proto_grpc::status_to_anyhow(
                tonic::Status::failed_precondition(
                    "no prepared delta is awaiting a commit".to_string(),
                ),
            ));
        };
        if ack != prepared.ack {
            return Err(proto_grpc::status_to_anyhow(
                tonic::Status::failed_precondition(
                    "commit acknowledgement differs from the prepared one".to_string(),
                ),
            ));
        }
        Ok(prepared.completes_horizon.then(|| {
            self.horizon
                .take()
                .expect("a cut finds discharged only a horizon which is open")
        }))
    }

    /// End the tenure once, retaining an unreported failure for the next request.
    /// Drop the prepared delta so unmount mutations can drain without appending.
    pub fn end(&mut self, unreported: Option<anyhow::Error>) {
        self.prepared = None;
        self.unreported = unreported;
    }

    /// Refuse a request of a tenure which is `over`.
    ///
    /// The failure which ended the tenure, if no request was told of it, is reported
    /// here, to this request alone. Requests after that are refused for the tenure
    /// being over, because the client which holds the failure holds its causes.
    fn check(&mut self, over: bool) -> anyhow::Result<()> {
        if !over {
            return Ok(());
        }
        match self.unreported.take() {
            Some(err) => Err(err),
            None => anyhow::bail!("the tenure is over"),
        }
    }

    /// Build a UUID with this producer and a strictly advancing clock. Follow
    /// wall time without allowing clock rollback to reorder records or their
    /// acknowledgements.
    fn build(
        &mut self,
        now: std::time::SystemTime,
        flags: uuid::Flags,
        chunks: Vec<proto::Chunk>,
        opens_horizon: bool,
    ) -> proto::DiskRecord {
        self.clock.update(uuid::Clock::from_time(now));
        let clock = self.clock.tick();

        proto::DiskRecord {
            uuid: crate::journal::uuid_bytes(self.producer, clock, flags),
            chunks,
            opens_horizon,
            installs_author: bytes::Bytes::new(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::Ledger;
    use proto_gazette::uuid;
    use std::fmt::Write as _;

    /// A wall clock this many seconds after the epoch.
    fn at(seconds: u64) -> std::time::SystemTime {
        std::time::UNIX_EPOCH + std::time::Duration::from_secs(seconds)
    }

    fn ledger() -> Ledger {
        Ledger::new(uuid::Producer::from_bytes([1, 0, 0, 0, 0, 1]), None)
    }

    /// The flags and clock of a built record.
    fn parsed(uuid: &[u8]) -> (uuid::Clock, uuid::Flags) {
        let (_producer, clock, flags) = uuid::parse(uuid::Uuid::from_slice(uuid).unwrap()).unwrap();
        (clock, flags)
    }

    #[derive(Debug, Clone, Copy)]
    enum Step {
        /// A mutation arrives. The writer takes it only while the ledger is taking,
        /// builds it only while the tenure appends, and asks that it open a horizon if
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
        /// A failure no request is waiting on.
        Fail(&'static str),
        /// The tenure's own cancellation fires, as a drain's does.
        Ended,
        /// The writer sees that cancellation, and ends the tenure.
        End,
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
        // Whether the writer still holds its appender, and whether the tenure's own
        // cancellation has fired. The tenure is over once either is false or true.
        let (mut now, mut appending, mut ended) = (0, true, false);

        // The tenure ends as the writer ends it: once, by whatever ended it first,
        // and keeping a failure only where no request is told of it.
        fn end(ledger: &mut Ledger, appending: &mut bool, unreported: Option<anyhow::Error>) {
            if std::mem::take(appending) {
                ledger.end(unreported);
            }
        }

        for &step in steps {
            now += 1;
            let over = !appending || ended;

            let outcome = match step {
                Mutation(_) if !ledger.taking() => "left in the channel".to_string(),
                Mutation(_) if over => "discarded".to_string(),
                Mutation(opens) => {
                    let opens = opens && ledger.delta_is_empty();
                    let record = ledger.build_uncommitted(at(now), Vec::new(), opens);
                    format!("built, opens {}", record.opens_horizon)
                }
                HorizonOpened(begin) => {
                    ledger.on_horizon_opened(begin);
                    "-".to_string()
                }
                Range(head, floor) => format!("range {:?}", ledger.horizon_range(head, floor)),
                Prepare(completes) => match ledger.begin_prepare(over) {
                    Err(err) => {
                        end(&mut ledger, &mut appending, None);
                        format!("refused: {err:#}")
                    }
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
                    match ledger.begin_acknowledge(over, &ack) {
                        Ok(floor) => format!("acknowledged, floor {floor:?}"),
                        Err(err) => {
                            end(&mut ledger, &mut appending, None);
                            format!("refused: {err:#}")
                        }
                    }
                }
                Fail(what) => {
                    end(&mut ledger, &mut appending, Some(anyhow::anyhow!(what)));
                    "-".to_string()
                }
                Ended => {
                    ended = true;
                    "-".to_string()
                }
                End => {
                    end(&mut ledger, &mut appending, None);
                    "-".to_string()
                }
            };
            let step = format!("{step:?}");

            writeln!(
                out,
                "{step:<28}{outcome:<92}taking {:<6}delta {}  horizon {:?}",
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
        Prepare(false)              nothing to commit                                                                           taking true  delta 0  horizon None
        Mutation(false)             built, opens false                                                                          taking true  delta 1  horizon None
        Mutation(false)             built, opens false                                                                          taking true  delta 2  horizon None
        Prepare(false)              prepared                                                                                    taking false delta 0  horizon None
        Mutation(false)             left in the channel                                                                         taking false delta 0  horizon None
        Ack                         acknowledged, floor None                                                                    taking true  delta 0  horizon None
        Mutation(false)             built, opens false                                                                          taking true  delta 1  horizon None
        Prepare(false)              prepared                                                                                    taking false delta 0  horizon None
        Ack                         acknowledged, floor None                                                                    taking true  delta 0  horizon None
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
        Mutation(false)             built, opens false                                                                          taking true  delta 1  horizon None
        Prepare(false)              prepared                                                                                    taking false delta 0  horizon None
        Prepare(false)              refused: a prepared delta is still awaiting its commit (FailedPrecondition)                 taking true  delta 0  horizon None
        Prepare(false)              refused: the tenure is over                                                                 taking true  delta 0  horizon None

        Mutation(false)             built, opens false                                                                          taking true  delta 1  horizon None
        Prepare(false)              prepared                                                                                    taking false delta 0  horizon None
        AckOther                    refused: commit acknowledgement differs from the prepared one (FailedPrecondition)          taking true  delta 0  horizon None
        Ack                         refused: the tenure is over                                                                 taking true  delta 0  horizon None

        Ack                         refused: no prepared delta is awaiting a commit (FailedPrecondition)                        taking true  delta 0  horizon None
        Mutation(false)             discarded                                                                                   taking true  delta 0  horizon None
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
            Range(3000, 1000),
            // A floor ahead of the head asks over no range at all.
            Range(10, 1000),
        ]);
        insta::assert_snapshot!(trace, @"
        Range(900, 100)             range Some(800)                                                                             taking true  delta 0  horizon None
        Mutation(true)              built, opens true                                                                           taking true  delta 1  horizon None
        Range(900, 100)             range None                                                                                  taking true  delta 1  horizon None
        HorizonOpened(1000)         -                                                                                           taking true  delta 1  horizon Some(1000)
        Mutation(false)             built, opens false                                                                          taking true  delta 2  horizon Some(1000)
        Prepare(false)              prepared                                                                                    taking false delta 0  horizon Some(1000)
        Ack                         acknowledged, floor None                                                                    taking true  delta 0  horizon Some(1000)
        Range(2000, 100)            range None                                                                                  taking true  delta 0  horizon Some(1000)
        Mutation(false)             built, opens false                                                                          taking true  delta 1  horizon Some(1000)
        Prepare(true)               prepared                                                                                    taking false delta 0  horizon Some(1000)
        Ack                         acknowledged, floor Some(1000)                                                              taking true  delta 0  horizon None
        Range(3000, 1000)           range Some(2000)                                                                            taking true  delta 0  horizon None
        Range(10, 1000)             range Some(0)                                                                               taking true  delta 0  horizon None
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
        ]);
        insta::assert_snapshot!(trace, @r#"
        Mutation(false)             built, opens false                                                                          taking true  delta 1  horizon None
        Fail("the broker refused")  -                                                                                           taking true  delta 1  horizon None
        Fail("a second failure")    -                                                                                           taking true  delta 1  horizon None
        Mutation(false)             discarded                                                                                   taking true  delta 1  horizon None
        Prepare(false)              refused: the broker refused                                                                 taking true  delta 1  horizon None
        Prepare(false)              refused: the tenure is over                                                                 taking true  delta 1  horizon None
        "#);
    }

    /// A tenure which has ended refuses every request for being over, including the
    /// acknowledgement of a delta it had prepared, and discards what it takes. Its
    /// own cancellation refuses a request before the writer has seen it.
    #[test]
    fn test_an_ended_tenure_commits_nothing() {
        let trace = [
            trace(&[
                Mutation(false),
                Prepare(false),
                Ended,
                End,
                Ack,
                Mutation(false),
            ]),
            // A drain cancels the tenure under a request already in flight, ahead of
            // the writer ending it.
            trace(&[Mutation(false), Ended, Prepare(false), Mutation(false)]),
        ]
        .join("\n");
        insta::assert_snapshot!(trace, @"
        Mutation(false)             built, opens false                                                                          taking true  delta 1  horizon None
        Prepare(false)              prepared                                                                                    taking false delta 0  horizon None
        Ended                       -                                                                                           taking false delta 0  horizon None
        End                         -                                                                                           taking true  delta 0  horizon None
        Ack                         refused: the tenure is over                                                                 taking true  delta 0  horizon None
        Mutation(false)             discarded                                                                                   taking true  delta 0  horizon None

        Mutation(false)             built, opens false                                                                          taking true  delta 1  horizon None
        Ended                       -                                                                                           taking true  delta 1  horizon None
        Prepare(false)              refused: the tenure is over                                                                 taking true  delta 1  horizon None
        Mutation(false)             discarded                                                                                   taking true  delta 1  horizon None
        ");
    }

    /// Clocks only advance, whatever the wall clock does, so each delta's records
    /// order ahead of the acknowledgement which commits them and ahead of every
    /// record of the next delta.
    #[test]
    fn test_clocks_advance_though_the_wall_clock_steps_back() {
        let mut ledger = ledger();

        let first = ledger.build_uncommitted(at(100), Vec::new(), false);
        let second = ledger.build_uncommitted(at(50), Vec::new(), false);
        let ack = ledger.cut(at(40), false);
        () = ledger.begin_acknowledge(false, &ack).map(|_| ()).unwrap();
        let next = ledger.build_uncommitted(at(200), Vec::new(), false);

        let mut ack = bytes::BytesMut::from(&ack[..]);
        let proto_gazette::fixed_framing::Frame::Record { message: ack, .. } =
            proto_gazette::fixed_framing::unpack::<crate::proto::DiskRecord>(&mut ack).unwrap()
        else {
            panic!("an acknowledgement is one framed record");
        };

        let built = [&first, &second, &ack, &next].map(|record| parsed(&record.uuid));
        for pair in built.windows(2) {
            assert!(pair[0].0 < pair[1].0, "a clock stepped back: {built:?}");
        }
        assert!(built[2].1.is_ack(), "the cut built {:?}", built[2].1);
        assert!(!built[0].1.is_ack() && !built[3].1.is_ack());
    }
}
