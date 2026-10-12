use crate::proto;
use proto_gazette::uuid;

/// Delta and commit state of one attachment's writer: the delta being built,
/// the delta awaiting acknowledgement, and the open horizon.
pub(super) struct Ledger {
    /// Producer of every delta record this attachment builds.
    producer: uuid::Producer,
    /// Clock of the last record built. It only advances.
    clock: uuid::Clock,
    /// A cut delta awaiting acknowledgement. No mutation is taken until it commits.
    prepared: Option<Prepared>,
    /// Records built into the delta which is accumulating now.
    delta_records: usize,
    /// Offset of the horizon this attachment opened or resumed.
    horizon: Option<i64>,
}

/// A delta which has been cut and awaits commit.
struct Prepared {
    ack: bytes::Bytes,
    completes_horizon: bool,
}

impl Ledger {
    pub(super) fn new(producer: uuid::Producer, horizon: Option<i64>) -> Self {
        Self {
            producer,
            clock: uuid::Clock::zero(),
            prepared: None,
            delta_records: 0,
            horizon,
        }
    }

    pub(super) fn awaiting_ack(&self) -> bool {
        self.prepared.is_some()
    }

    pub(super) fn delta_is_empty(&self) -> bool {
        self.delta_records == 0
    }

    /// Journal bytes between the recovery floor and `head`, which the device's
    /// horizon policy weighs.
    pub(super) fn range(&self, head: i64, floor: i64) -> u64 {
        let floor = match &self.prepared {
            Some(Prepared {
                completes_horizon: true,
                ..
            }) => self
                .horizon
                .expect("the device reported a discharge with no horizon open"),
            _ => floor,
        };
        head.saturating_sub(floor).max(0) as u64
    }

    /// Build one mutation's `CONTINUE_TXN` record, which opens a horizon at
    /// `opens_at`, its own offset, if given.
    pub(super) fn build_uncommitted(
        &mut self,
        now: std::time::SystemTime,
        chunks: Vec<proto::Chunk>,
        opens_at: Option<i64>,
    ) -> proto::DiskRecord {
        if opens_at.is_some() {
            assert!(
                self.delta_is_empty() && self.horizon.is_none(),
                "only a delta's first record opens a horizon, and only where none is open"
            );
            self.horizon = opens_at;
        }
        let record = self.build(now, uuid::Flags::CONTINUE_TXN, chunks, opens_at.is_some());
        self.delta_records += 1;

        record
    }

    /// End the delta: build its `ACK_TXN` record, clocked after each of its
    /// records, and hold it until acknowledged.
    pub(super) fn cut(
        &mut self,
        now: std::time::SystemTime,
        completes_horizon: bool,
    ) -> bytes::Bytes {
        assert!(!self.delta_is_empty(), "an empty delta is not cut");

        let record = self.build(now, uuid::Flags::ACK_TXN, Vec::new(), false);
        let mut buf = bytes::BytesMut::new();
        proto_gazette::fixed_framing::encode(&record, &mut buf);
        let ack = buf.freeze();

        self.prepared = Some(Prepared {
            ack: ack.clone(),
            completes_horizon,
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

        Ok(prepared.completes_horizon.then(|| {
            self.horizon
                .take()
                .expect("the device reported a discharge with no horizon open")
        }))
    }

    /// Build a UUID with this producer and a strictly advancing clock.
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
            uuid: super::uuid_bytes(self.producer, clock, flags),
            chunks,
            opens_horizon,
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
        Ledger::new(crate::test_support::producer('a'), None)
    }

    /// The flags and clock of a built record.
    fn parsed(uuid: &[u8]) -> (uuid::Clock, uuid::Flags) {
        let (_producer, clock, flags) = uuid::parse(uuid::Uuid::from_slice(uuid).unwrap()).unwrap();
        (clock, flags)
    }

    #[derive(Debug, Clone, Copy)]
    enum Step {
        /// A mutation arrives. The writer takes it only while no acknowledgement
        /// is awaited.
        Mutation,
        /// As `Mutation`, where it opens a horizon at this offset.
        Opens(i64),
        /// Ask the next delta's range at this head and floor.
        Range(i64, i64),
        /// A client prepares. The `bool` is whether the device found the open
        /// horizon discharged at this cut.
        Prepare(bool),
        /// A client acknowledges exactly what was last prepared.
        Ack,
        /// A client acknowledges other bytes.
        AckOther,
    }
    use Step::*;

    /// Run `steps` as the writer's task drives the ledger, and render what each
    /// returned and whether the ledger then awaits an acknowledgement, how many
    /// records its delta holds, and its open horizon.
    fn trace(steps: &[Step]) -> String {
        let mut ledger = ledger();
        let mut out = String::new();
        let mut last_ack = bytes::Bytes::new();
        let mut now = 0;

        for &step in steps {
            now += 1;

            let outcome = match step {
                Mutation | Opens(_) if ledger.awaiting_ack() => "left in the channel".to_string(),
                Mutation | Opens(_) => {
                    let opens_at = match step {
                        Opens(at) => Some(at),
                        _ => None,
                    };
                    let record = ledger.build_uncommitted(at(now), Vec::new(), opens_at);
                    format!("built, opens {}", record.opens_horizon)
                }
                Range(head, floor) => format!("range {}", ledger.range(head, floor)),
                Prepare(_) if ledger.delta_is_empty() => "nothing to commit".to_string(),
                Prepare(completes) => {
                    last_ack = ledger.cut(at(now), completes);
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
                "{step:<28}{outcome:<92}awaiting ack {:<6}delta {}  horizon {:?}",
                ledger.awaiting_ack(),
                ledger.delta_records,
                ledger.horizon,
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
            Mutation,
            Mutation,
            Prepare(false),
            Mutation,
            Ack,
            Mutation,
            Prepare(false),
            Ack,
        ]);
        insta::assert_snapshot!(trace, @"
        Prepare(false)              nothing to commit                                                                           awaiting ack false delta 0  horizon None
        Mutation                    built, opens false                                                                          awaiting ack false delta 1  horizon None
        Mutation                    built, opens false                                                                          awaiting ack false delta 2  horizon None
        Prepare(false)              prepared                                                                                    awaiting ack true  delta 0  horizon None
        Mutation                    left in the channel                                                                         awaiting ack true  delta 0  horizon None
        Ack                         acknowledged, floor None                                                                    awaiting ack false delta 0  horizon None
        Mutation                    built, opens false                                                                          awaiting ack false delta 1  horizon None
        Prepare(false)              prepared                                                                                    awaiting ack true  delta 0  horizon None
        Ack                         acknowledged, floor None                                                                    awaiting ack false delta 0  horizon None
        ");
    }

    /// Only the exact acknowledgement which was prepared commits its delta, and it
    /// commits once. A refused acknowledgement changes nothing: ending the
    /// attachment over it is the writer's.
    #[test]
    fn test_an_acknowledgement_out_of_turn_is_refused() {
        let trace = trace(&[Mutation, Prepare(false), AckOther, Ack, Ack]);
        insta::assert_snapshot!(trace, @"
        Mutation                    built, opens false                                                                          awaiting ack false delta 1  horizon None
        Prepare(false)              prepared                                                                                    awaiting ack true  delta 0  horizon None
        AckOther                    refused: the acknowledgement matches no prepared delta (FailedPrecondition)                 awaiting ack true  delta 0  horizon None
        Ack                         acknowledged, floor None                                                                    awaiting ack false delta 0  horizon None
        Ack                         refused: the acknowledgement matches no prepared delta (FailedPrecondition)                 awaiting ack false delta 0  horizon None
        ");
    }

    /// A delta's first record opens a horizon, and a delta which the cut found
    /// discharging it moves the floor when it commits. The range after its
    /// prepare counts from that floor.
    #[test]
    fn test_a_horizon_opens_at_a_deltas_first_record_and_completes_at_its_commit() {
        let trace = trace(&[
            Range(900, 100),
            Opens(1000),
            Mutation,
            Prepare(false),
            Range(1100, 100),
            Ack,
            Mutation,
            Prepare(true),
            Range(2000, 100),
            Ack,
            Range(3000, 1000),
            // A floor ahead of the head is no range at all.
            Range(10, 1000),
        ]);
        insta::assert_snapshot!(trace, @"
        Range(900, 100)             range 800                                                                                   awaiting ack false delta 0  horizon None
        Opens(1000)                 built, opens true                                                                           awaiting ack false delta 1  horizon Some(1000)
        Mutation                    built, opens false                                                                          awaiting ack false delta 2  horizon Some(1000)
        Prepare(false)              prepared                                                                                    awaiting ack true  delta 0  horizon Some(1000)
        Range(1100, 100)            range 1000                                                                                  awaiting ack true  delta 0  horizon Some(1000)
        Ack                         acknowledged, floor None                                                                    awaiting ack false delta 0  horizon Some(1000)
        Mutation                    built, opens false                                                                          awaiting ack false delta 1  horizon Some(1000)
        Prepare(true)               prepared                                                                                    awaiting ack true  delta 0  horizon Some(1000)
        Range(2000, 100)            range 1000                                                                                  awaiting ack true  delta 0  horizon Some(1000)
        Ack                         acknowledged, floor Some(1000)                                                              awaiting ack false delta 0  horizon None
        Range(3000, 1000)           range 2000                                                                                  awaiting ack false delta 0  horizon None
        Range(10, 1000)             range 0                                                                                     awaiting ack false delta 0  horizon None
        ");
    }

    /// Clocks only advance, whatever the wall clock does, so each delta's records
    /// order ahead of the acknowledgement which commits them and ahead of every
    /// record of the next delta.
    #[test]
    fn test_clocks_advance_though_the_wall_clock_steps_back() {
        let mut ledger = ledger();

        let first = ledger.build_uncommitted(at(100), Vec::new(), None);
        let second = ledger.build_uncommitted(at(50), Vec::new(), None);
        let ack = ledger.cut(at(40), false);
        () = ledger.begin_acknowledge(&ack).map(|_| ()).unwrap();
        let next = ledger.build_uncommitted(at(200), Vec::new(), None);

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
