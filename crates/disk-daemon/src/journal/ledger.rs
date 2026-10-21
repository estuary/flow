use crate::proto;
use proto_gazette::uuid;

/// Delta and commit state of one attachment's writer: the delta being built,
/// the delta awaiting acknowledgement, and the open horizon.
pub(super) struct Ledger {
    /// Producer of every delta record this attachment builds.
    producer: uuid::Producer,
    /// Clock of the last record built. It only advances.
    clock: uuid::Clock,
    /// A cut delta awaiting acknowledgement. No chunk is taken until it commits.
    prepared: Option<Prepared>,
    /// Records built into the delta which is accumulating now.
    delta_records: usize,
    /// Offset of the open horizon's opening record. At first, it is the last
    /// committed opener, whose horizon the device may since have discharged: the
    /// device's next opener then replaces it, where a discharge completes it.
    horizon: Option<i64>,
    /// The floor which the delta accumulating now installs when it commits: the
    /// opener of a horizon which one of its records discharged.
    completes: Option<i64>,
}

impl Ledger {
    pub(super) fn new(producer: uuid::Producer, horizon: Option<i64>) -> Self {
        Self {
            producer,
            clock: uuid::Clock::zero(),
            prepared: None,
            delta_records: 0,
            horizon,
            completes: None,
        }
    }

    pub(super) fn awaiting_ack(&self) -> bool {
        self.prepared.is_some()
    }

    pub(super) fn delta_is_empty(&self) -> bool {
        self.delta_records == 0
    }

    /// Build one live chunk's `CONTINUE_TXN` record. It opens a horizon at
    /// `opens_at`, its own offset, if given, and `discharges` says whether it
    /// discharged the open horizon's last pending block.
    pub(super) fn build_uncommitted(
        &mut self,
        now: std::time::SystemTime,
        chunk: proto::Chunk,
        opens_at: Option<i64>,
        discharges: bool,
    ) -> proto::DiskRecord {
        // A device opens a horizon only where none is open, so an opener replaces
        // only a recovered horizon which the device had already discharged.
        if opens_at.is_some() {
            self.horizon = opens_at;
        }
        if discharges {
            let opener = self
                .horizon
                .take()
                .expect("only an open horizon is discharged");
            self.completes = Some(opener);
        }
        let record = self.build(
            now,
            uuid::Flags::CONTINUE_TXN,
            Some(chunk),
            opens_at.is_some(),
        );
        self.delta_records += 1;

        record
    }

    /// End the delta: build its `ACK_TXN` record, clocked after each of its
    /// records, and hold it until acknowledged.
    pub(super) fn cut(&mut self, now: std::time::SystemTime) -> bytes::Bytes {
        let record = self.build(now, uuid::Flags::ACK_TXN, None, false);
        let mut buf = bytes::BytesMut::new();
        proto_gazette::fixed_framing::encode(&record, &mut buf);
        let ack = buf.freeze();

        self.prepared = Some(Prepared {
            ack: ack.clone(),
            floor: self.completes.take(),
        });
        self.delta_records = 0;

        ack
    }

    /// Release the prepared delta which `ack` acknowledges, returning the
    /// recovery floor it installs, if any.
    pub(super) fn begin_acknowledge(&mut self, ack: &[u8]) -> anyhow::Result<Option<i64>> {
        let Some(prepared) = self.prepared.take_if(|prepared| prepared.ack == ack) else {
            return Err(crate::failed_precondition(
                "the acknowledgement matches no prepared delta",
            ));
        };
        Ok(prepared.floor)
    }

    /// Build a UUID with this producer and a strictly advancing clock.
    fn build(
        &mut self,
        now: std::time::SystemTime,
        flags: uuid::Flags,
        chunk: Option<proto::Chunk>,
        opens_horizon: bool,
    ) -> proto::DiskRecord {
        self.clock.update(uuid::Clock::from_time(now));
        let clock = self.clock.tick();

        proto::DiskRecord {
            uuid: super::uuid_bytes(self.producer, clock, flags),
            chunk,
            opens_horizon,
        }
    }
}

/// A delta which has been cut and awaits commit.
struct Prepared {
    ack: bytes::Bytes,
    floor: Option<i64>,
}

#[cfg(test)]
mod test {
    use super::Ledger;
    use crate::proto;
    use proto_gazette::uuid;
    use std::fmt::Write as _;

    /// A wall clock this many seconds after the epoch.
    fn at(seconds: u64) -> std::time::SystemTime {
        std::time::UNIX_EPOCH + std::time::Duration::from_secs(seconds)
    }

    /// A ledger which starts from `opener`, the last committed horizon opener.
    fn ledger(opener: Option<i64>) -> Ledger {
        Ledger::new(crate::test_support::producer('a'), opener)
    }

    /// The flags and clock of a built record.
    fn parsed(uuid: &[u8]) -> (uuid::Clock, uuid::Flags) {
        let (_producer, clock, flags) = uuid::parse(uuid::Uuid::from_slice(uuid).unwrap()).unwrap();
        (clock, flags)
    }

    #[derive(Debug, Clone, Copy)]
    enum Step {
        /// A live chunk arrives. The writer takes it only while no acknowledgement
        /// is awaited.
        Chunk,
        /// As `Chunk`, where it opens a horizon at this offset.
        Opens(i64),
        /// As `Chunk`, where it discharges the open horizon.
        Discharges,
        /// A client prepares.
        Prepare,
        /// A client acknowledges exactly what was last prepared.
        Ack,
        /// A client acknowledges other bytes.
        AckOther,
    }
    use Step::*;

    /// Run `steps` as the writer's task drives the ledger, and render what each
    /// returned and whether the ledger then awaits an acknowledgement, how many
    /// records its delta holds, its open horizon, and the floor its delta completes.
    fn trace(opener: Option<i64>, steps: &[Step]) -> String {
        let mut ledger = ledger(opener);
        let mut out = String::new();
        let mut last_ack = bytes::Bytes::new();
        let mut now = 0;

        for &step in steps {
            now += 1;

            let outcome = match step {
                Chunk | Opens(_) | Discharges if ledger.awaiting_ack() => {
                    "left in the channel".to_string()
                }
                Chunk | Opens(_) | Discharges => {
                    let opens_at = match step {
                        Opens(at) => Some(at),
                        _ => None,
                    };
                    let discharges = matches!(step, Discharges);
                    let record = ledger.build_uncommitted(
                        at(now),
                        proto::Chunk::default(),
                        opens_at,
                        discharges,
                    );
                    format!("built, opens {}", record.opens_horizon)
                }
                Prepare if ledger.delta_is_empty() => "nothing to commit".to_string(),
                Prepare => {
                    last_ack = ledger.cut(at(now));
                    "prepared".to_string()
                }
                Ack | AckOther => {
                    let ack = match step {
                        Ack => last_ack.clone(),
                        _ => bytes::Bytes::from_static(b"other"),
                    };
                    match ledger.begin_acknowledge(&ack) {
                        Ok(floor) => format!("acknowledged, floor {floor:?}"),
                        Err(err) => format!("refused: {err:#}"),
                    }
                }
            };
            let step = format!("{step:?}");

            writeln!(
                out,
                "{step:<12}{outcome:<78}awaiting ack {:<6}delta {}  horizon {:<11}completes {:?}",
                ledger.awaiting_ack(),
                ledger.delta_records,
                format!("{:?}", ledger.horizon),
                ledger.completes,
            )
            .unwrap();
        }
        out
    }

    /// A delta is cut, held while its client commits, and acknowledged. The writer
    /// takes nothing between the two, and an empty delta commits nothing.
    #[test]
    fn test_a_delta_is_prepared_and_acknowledged() {
        let trace = trace(
            None,
            &[
                Prepare, Chunk, Chunk, Prepare, Chunk, Ack, Chunk, Prepare, Ack,
            ],
        );
        insta::assert_snapshot!(trace, @"
        Prepare     nothing to commit                                                             awaiting ack false delta 0  horizon None       completes None
        Chunk       built, opens false                                                            awaiting ack false delta 1  horizon None       completes None
        Chunk       built, opens false                                                            awaiting ack false delta 2  horizon None       completes None
        Prepare     prepared                                                                      awaiting ack true  delta 0  horizon None       completes None
        Chunk       left in the channel                                                           awaiting ack true  delta 0  horizon None       completes None
        Ack         acknowledged, floor None                                                      awaiting ack false delta 0  horizon None       completes None
        Chunk       built, opens false                                                            awaiting ack false delta 1  horizon None       completes None
        Prepare     prepared                                                                      awaiting ack true  delta 0  horizon None       completes None
        Ack         acknowledged, floor None                                                      awaiting ack false delta 0  horizon None       completes None
        ");
    }

    /// Only the exact acknowledgement which was prepared commits its delta, and it
    /// commits once. A refused acknowledgement changes nothing: ending the
    /// attachment over it is the writer's.
    #[test]
    fn test_an_acknowledgement_out_of_turn_is_refused() {
        let trace = trace(None, &[Chunk, Prepare, AckOther, Ack, Ack]);
        insta::assert_snapshot!(trace, @"
        Chunk       built, opens false                                                            awaiting ack false delta 1  horizon None       completes None
        Prepare     prepared                                                                      awaiting ack true  delta 0  horizon None       completes None
        AckOther    refused: the acknowledgement matches no prepared delta (FailedPrecondition)   awaiting ack true  delta 0  horizon None       completes None
        Ack         acknowledged, floor None                                                      awaiting ack false delta 0  horizon None       completes None
        Ack         refused: the acknowledgement matches no prepared delta (FailedPrecondition)   awaiting ack false delta 0  horizon None       completes None
        ");
    }

    /// A horizon opens at any record, and the delta whose record discharged it
    /// moves the floor to its opener when that delta commits. One delta may
    /// discharge a horizon and open the next.
    #[test]
    fn test_a_horizon_completes_at_the_commit_of_the_delta_which_discharged_it() {
        let trace = trace(
            None,
            &[
                Chunk,
                Opens(1000),
                Prepare,
                Ack,
                Chunk,
                Discharges,
                Opens(2000),
                Prepare,
                Ack,
                Discharges,
                Prepare,
                Ack,
            ],
        );
        insta::assert_snapshot!(trace, @"
        Chunk       built, opens false                                                            awaiting ack false delta 1  horizon None       completes None
        Opens(1000) built, opens true                                                             awaiting ack false delta 2  horizon Some(1000) completes None
        Prepare     prepared                                                                      awaiting ack true  delta 0  horizon Some(1000) completes None
        Ack         acknowledged, floor None                                                      awaiting ack false delta 0  horizon Some(1000) completes None
        Chunk       built, opens false                                                            awaiting ack false delta 1  horizon Some(1000) completes None
        Discharges  built, opens false                                                            awaiting ack false delta 2  horizon None       completes Some(1000)
        Opens(2000) built, opens true                                                             awaiting ack false delta 3  horizon Some(2000) completes Some(1000)
        Prepare     prepared                                                                      awaiting ack true  delta 0  horizon Some(2000) completes None
        Ack         acknowledged, floor Some(1000)                                                awaiting ack false delta 0  horizon Some(2000) completes None
        Discharges  built, opens false                                                            awaiting ack false delta 1  horizon None       completes Some(2000)
        Prepare     prepared                                                                      awaiting ack true  delta 0  horizon None       completes None
        Ack         acknowledged, floor Some(2000)                                                awaiting ack false delta 0  horizon None       completes None
        ");
    }

    /// A writer starts from the last committed opener, whose horizon the device may
    /// hold open still. A discharge then completes it. Or an opener replaces it,
    /// as a device opens one only where its recovery had discharged that horizon.
    #[test]
    fn test_a_recovered_opener_is_completed_or_replaced() {
        let mut out = String::new();

        for (case, steps) in [
            ("held open", &[Discharges, Prepare, Ack][..]),
            ("discharged", &[Opens(2000), Discharges, Prepare, Ack]),
        ] {
            writeln!(out, "{case}:").unwrap();
            out.push_str(&trace(Some(500), steps));
        }
        insta::assert_snapshot!(out, @"
        held open:
        Discharges  built, opens false                                                            awaiting ack false delta 1  horizon None       completes Some(500)
        Prepare     prepared                                                                      awaiting ack true  delta 0  horizon None       completes None
        Ack         acknowledged, floor Some(500)                                                 awaiting ack false delta 0  horizon None       completes None
        discharged:
        Opens(2000) built, opens true                                                             awaiting ack false delta 1  horizon Some(2000) completes None
        Discharges  built, opens false                                                            awaiting ack false delta 2  horizon None       completes Some(2000)
        Prepare     prepared                                                                      awaiting ack true  delta 0  horizon None       completes None
        Ack         acknowledged, floor Some(2000)                                                awaiting ack false delta 0  horizon None       completes None
        ");
    }

    /// Clocks only advance, whatever the wall clock does, so each delta's records
    /// order ahead of the acknowledgement which commits them and ahead of every
    /// record of the next delta.
    #[test]
    fn test_clocks_advance_though_the_wall_clock_steps_back() {
        let mut ledger = ledger(None);

        let first = ledger.build_uncommitted(at(100), proto::Chunk::default(), None, false);
        let second = ledger.build_uncommitted(at(50), proto::Chunk::default(), None, false);
        let ack = ledger.cut(at(40));
        () = ledger.begin_acknowledge(&ack).map(|_| ()).unwrap();
        let next = ledger.build_uncommitted(at(200), proto::Chunk::default(), None, false);

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
