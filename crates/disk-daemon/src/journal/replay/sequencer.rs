use crate::proto;
use anyhow::Context;
use proto_gazette::uuid;

/// What one record does to the delta a rebuild holds.
#[derive(Debug, PartialEq)]
pub(in crate::journal) struct Step {
    /// Producer of an uncommitted delta which this record displaced, and which the
    /// rebuild must drop before anything else.
    pub displaced: Option<uuid::Producer>,
    /// How to handle this record after dropping any displaced delta.
    pub action: Action,
}

/// What becomes of one record.
#[derive(Debug, PartialEq)]
pub(in crate::journal) enum Action {
    /// Nothing: it changes no disk content, repeats a record already sequenced, or is
    /// a fragment of a displaced delta.
    Skip,
    /// Hold it, as the first or a later record of the delta held.
    Hold,
    /// Apply the delta held, which this acknowledgement commits. `opens_at` is the
    /// offset at which that delta's first record opened a horizon, if it opened one.
    Commit { opens_at: Option<i64> },
}

/// What a recovered acknowledgement does, judged against what a rebuild sequenced.
#[derive(Debug, PartialEq)]
pub(in crate::journal) enum RecoveredAck {
    /// What it acknowledges is already committed, so there is nothing to append. It
    /// is at or below its writer's last commit, or the rebuild saw nothing of its writer:
    /// a writer whose records are all below the floor committed what the floor's
    /// horizon covers. `older` is an acknowledgement strictly below its writer's last
    /// commit, which a client whose own state went back hands over.
    Committed { older: bool },
    /// It commits the delta the rebuild holds, so appending it commits that delta.
    Commits,
    /// It commits records this rebuild no longer holds: part of the delta held, or a
    /// delta another producer displaced, or a delta whose records are not in the
    /// journal. Appending it would fail every replay from then on.
    Refused,
}

/// Sequencing state of one rebuild of a disk from its journal.
#[derive(Default)]
pub(in crate::journal) struct Sequencer {
    /// Sequencing state of each producer of the range.
    producers: std::collections::HashMap<uuid::Producer, Sequence>,
    /// Producer of the held delta. Cleared on commit or displacement by another
    /// producer's delta records.
    open: Option<uuid::Producer>,
    /// Offset at which the uncommitted delta's first record opened a horizon. It goes with
    /// the delta: a horizon belongs to the delta which opened it.
    horizon_at: Option<i64>,
}

impl Sequencer {
    /// Sequence every record in journal order. Return its effect without applying
    /// it: chunks and horizon changes remain pending together until commit.
    pub fn on_record(&mut self, record: &proto::DiskRecord, offset: i64) -> anyhow::Result<Step> {
        let uuid =
            uuid::Uuid::from_slice(&record.uuid).context("record carries no message UUID")?;
        let (producer, clock, flags) = uuid::parse(uuid)?;

        let state = self.producers.entry(producer).or_default();

        let outcome = uuid::sequence(
            flags,
            clock,
            &mut state.last_commit,
            &mut state.max_continue,
        )?;

        anyhow::ensure!(
            !record.opens_horizon
                || matches!(
                    outcome,
                    uuid::SequenceOutcome::ContinueBeginSpan
                        | uuid::SequenceOutcome::ContinueDuplicate
                ),
            "record of {producer:?} at {clock:?} opens a horizon but does not begin a delta",
        );

        let step = |action| Step {
            displaced: None,
            action,
        };

        match outcome {
            // A fence carries the author it installs and changes no disk content.
            uuid::SequenceOutcome::OutsideCommit | uuid::SequenceOutcome::OutsideDuplicate => {
                anyhow::ensure!(
                    record.installs_author.len() == std::mem::size_of::<uuid::Producer>(),
                    "fence record of {producer:?} installs {} bytes of author",
                    record.installs_author.len(),
                );
                () = ensure_no_chunks(record, "a fence")?;

                // A fence leaves the pending delta available for promotion to repair
                // its recovered acknowledgement.
                Ok(step(Action::Skip))
            }
            uuid::SequenceOutcome::ContinueBeginSpan => {
                let displaced = self.displace(producer);
                self.open = Some(producer);

                // Hold the opening offset with the delta until it commits.
                self.horizon_at = record.opens_horizon.then_some(offset);

                Ok(Step {
                    displaced,
                    action: Action::Hold,
                })
            }
            uuid::SequenceOutcome::ContinueExtendSpan if self.open == Some(producer) => {
                Ok(step(Action::Hold))
            }
            // This displaced delta has lost its initial records. Skip the fragment
            // and refuse its acknowledgement, but still displace any other pending
            // delta it interleaves.
            uuid::SequenceOutcome::ContinueExtendSpan => Ok(Step {
                displaced: self.displace(producer),
                action: Action::Skip,
            }),
            uuid::SequenceOutcome::ContinueDuplicate => Ok(step(Action::Skip)),

            // Another producer's records interleaved this delta, so its
            // acknowledgement cannot be honored: those records displaced what this
            // rebuild held, and nothing states the order the two deltas committed in.
            uuid::SequenceOutcome::AckCommit => {
                anyhow::ensure!(
                    self.open == Some(producer),
                    "acknowledgement of a delta of {producer:?} which another producer's \
                     records interleaved",
                );
                self.open = None;
                () = ensure_no_chunks(record, "an acknowledgement")?;

                Ok(step(Action::Commit {
                    opens_at: self.horizon_at.take(),
                }))
            }
            // A delta whose records are all below the floor, or an
            // acknowledgement which was appended twice.
            uuid::SequenceOutcome::AckEmpty | uuid::SequenceOutcome::AckDuplicate => {
                () = ensure_no_chunks(record, "an acknowledgement")?;

                Ok(step(Action::Skip))
            }

            // A rollback takes back records, and a deep one takes back records
            // this rebuild already applied. The append barrier makes one impossible
            // from this daemon, and this daemon is the only writer a disk journal
            // has.
            uuid::SequenceOutcome::AckCleanRollback | uuid::SequenceOutcome::AckDeepRollback => {
                anyhow::bail!(
                    "acknowledgement of {producer:?} at {clock:?} rolls back records this rebuild sequenced",
                )
            }
        }
    }

    /// Classify a recovered acknowledgement before appending it. Only an ack of
    /// the entire held delta needs repair; incompatible acks would break future
    /// replays.
    ///
    /// An ack repeating the producer's last commit needs no repair, even if that
    /// producer has since appended an uncommitted delta.
    pub fn recovered_ack(&self, producer: uuid::Producer, clock: uuid::Clock) -> RecoveredAck {
        let Some(mut state) = self.producers.get(&producer).copied() else {
            return RecoveredAck::Committed { older: false };
        };
        let last_commit = state.last_commit;

        match uuid::sequence(
            uuid::Flags::ACK_TXN,
            clock,
            &mut state.last_commit,
            &mut state.max_continue,
        ) {
            Ok(uuid::SequenceOutcome::AckCommit) if self.open == Some(producer) => {
                RecoveredAck::Commits
            }
            Ok(
                uuid::SequenceOutcome::AckDuplicate
                | uuid::SequenceOutcome::AckCleanRollback
                | uuid::SequenceOutcome::AckDeepRollback,
            ) => RecoveredAck::Committed {
                older: clock < last_commit,
            },
            // A writer the rebuild saw, whose delta records are not where its
            // acknowledgement says, or are no longer held.
            _ => RecoveredAck::Refused,
        }
    }

    /// Give up the uncommitted delta, unless it is `producer`'s, and report whose it was.
    ///
    /// A record of another producer is a delta which displaced the one held. Only an
    /// acknowledgement of the delta at the head can be honored, and this record
    /// proves that none arrived.
    fn displace(&mut self, producer: uuid::Producer) -> Option<uuid::Producer> {
        let held = self.open.filter(|held| *held != producer)?;

        self.horizon_at = None;
        self.open = None;

        Some(held)
    }
}

#[derive(Default, Clone, Copy)]
struct Sequence {
    /// Clocks which [`uuid::sequence`] transitions.
    last_commit: uuid::Clock,
    max_continue: uuid::Clock,
}

fn ensure_no_chunks(record: &proto::DiskRecord, what: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        record.chunks.is_empty(),
        "{what} carries {} chunks, which change disk content it does not commit",
        record.chunks.len(),
    );
    Ok(())
}

#[cfg(test)]
mod test {
    use super::{Action, Sequencer, Step};
    use crate::proto;
    use proto_gazette::uuid;
    use std::fmt::Write as _;

    fn producer(seed: u8) -> uuid::Producer {
        uuid::Producer::from_bytes([seed | 0x01, 0, 0, 0, 0, seed])
    }

    /// A producer as a letter: seed `0x10` is `a`, `0x20` is `b`, and so on.
    fn name(producer: uuid::Producer) -> char {
        (b'a' + producer.as_bytes()[5] / 0x10 - 1) as char
    }

    /// A clock `ticks` microseconds after the epoch.
    fn clock(ticks: u64) -> uuid::Clock {
        let mut clock = uuid::Clock::UNIX_EPOCH;
        for _ in 0..ticks {
            _ = clock.tick();
        }
        clock
    }

    #[derive(Clone, Copy)]
    enum Record {
        /// A record of the producer seeded `.0` at clock `.1`, extending or beginning
        /// its delta.
        Write(u8, u64),
        /// As `Write`, where the record opens a horizon.
        Opens(u8, u64),
        Ack(u8, u64),
        /// A fence of the producer seeded `.0`, which installs the author seeded `.2`.
        Fence(u8, u64, u8),
    }
    use Record::*;

    fn build(record: Record) -> proto::DiskRecord {
        let (seed, ticks, flags) = match record {
            Write(seed, ticks) | Opens(seed, ticks) => (seed, ticks, uuid::Flags::CONTINUE_TXN),
            Ack(seed, ticks) => (seed, ticks, uuid::Flags::ACK_TXN),
            Fence(seed, ticks, _) => (seed, ticks, uuid::Flags::OUTSIDE_TXN),
        };
        proto::DiskRecord {
            uuid: bytes::Bytes::copy_from_slice(
                uuid::build(producer(seed), clock(ticks), flags)
                    .as_bytes()
                    .as_slice(),
            ),
            chunks: Vec::new(),
            opens_horizon: matches!(record, Opens(..)),
            installs_author: match record {
                Fence(_, _, installs) => {
                    bytes::Bytes::copy_from_slice(producer(installs).as_bytes())
                }
                _ => bytes::Bytes::new(),
            },
        }
    }

    fn label(record: Record) -> String {
        match record {
            Write(seed, ticks) => format!("write {}@{ticks}", name(producer(seed))),
            Opens(seed, ticks) => format!("opens {}@{ticks}", name(producer(seed))),
            Ack(seed, ticks) => format!("ack {}@{ticks}", name(producer(seed))),
            Fence(seed, ticks, installs) => format!(
                "fence {}@{ticks} for {}",
                name(producer(seed)),
                name(producer(installs)),
            ),
        }
    }

    /// Sequence `records`, each 100 bytes past the last, and render the producer whose
    /// delta is then held and the step each took.
    fn trace(records: &[Record]) -> String {
        let mut sequencer = Sequencer::default();
        let mut out = String::new();

        for (index, &record) in records.iter().enumerate() {
            let outcome = match sequencer.on_record(&build(record), index as i64 * 100) {
                Ok(Step { displaced, action }) => {
                    let displaced = displaced
                        .map(|held| format!("displaces {}, ", name(held)))
                        .unwrap_or_default();
                    let action = match action {
                        Action::Skip => "skip".to_string(),
                        Action::Hold => "hold".to_string(),
                        Action::Commit { opens_at } => format!("commit, opens at {opens_at:?}"),
                    };
                    format!("{displaced}{action}")
                }
                Err(err) => format!("refused: {err:#}"),
            };
            let open = sequencer.open.map_or('-', name);

            writeln!(out, "{:<20}open {open}  {outcome}", label(record),).unwrap();
        }
        out
    }

    /// A delta is held from its first record within the range, even where it began
    /// below it, and commits at its acknowledgement. Duplicates which at-least-once
    /// appends repeat are skipped, so a delta commits once. A fence changes no content.
    #[test]
    fn test_a_delta_is_held_until_it_commits() {
        let trace = trace(&[
            Write(0x10, 2),
            Write(0x10, 3),
            Write(0x10, 3),
            Ack(0x10, 4),
            Ack(0x10, 4),
            Fence(0x20, 5, 0x10),
            Write(0x10, 6),
            Ack(0x10, 7),
        ]);
        insta::assert_snapshot!(trace, @"
        write a@2           open a  hold
        write a@3           open a  hold
        write a@3           open a  skip
        ack a@4             open -  commit, opens at None
        ack a@4             open -  skip
        fence b@5 for a     open -  skip
        write a@6           open a  hold
        ack a@7             open -  commit, opens at None
        ");
    }

    /// A horizon belongs to the delta which opened it: it is handed over at that
    /// delta's commit, and goes with it if another producer displaces it.
    #[test]
    fn test_a_horizon_goes_with_the_delta_which_opened_it() {
        let trace = trace(&[
            Opens(0x10, 1),
            Write(0x10, 2),
            Ack(0x10, 3),
            Opens(0x10, 4),
            Write(0x30, 5),
            Ack(0x30, 6),
            // Only a delta's first record may open a horizon.
            Write(0x10, 7),
            Opens(0x10, 8),
        ]);
        insta::assert_snapshot!(trace, @"
        opens a@1           open a  hold
        write a@2           open a  hold
        ack a@3             open -  commit, opens at Some(0)
        opens a@4           open a  hold
        write c@5           open c  displaces a, hold
        ack c@6             open -  commit, opens at None
        write a@7           open -  skip
        opens a@8           open -  refused: record of Producer(11:00:00:00:00:10) at Clock(0s 8000ns) opens a horizon but does not begin a delta
        ");
    }

    /// Another producer's records displace the delta held, and a displaced delta is
    /// never taken up again: what resumes is a fragment, which is not held and whose
    /// acknowledgement is refused. A fence displaces nothing, so a promotion may still
    /// commit the delta it repairs.
    #[test]
    fn test_a_displaced_delta_is_never_committed() {
        let trace = [
            trace(&[
                Write(0x10, 1),
                Write(0x30, 2),
                Ack(0x30, 3),
                Write(0x10, 4),
                Ack(0x10, 5),
            ]),
            trace(&[Write(0x10, 1), Fence(0x20, 2, 0x30), Ack(0x10, 3)]),
        ]
        .join("\n");
        insta::assert_snapshot!(trace, @"
        write a@1           open a  hold
        write c@2           open c  displaces a, hold
        ack c@3             open -  commit, opens at None
        write a@4           open -  skip
        ack a@5             open -  refused: acknowledgement of a delta of Producer(11:00:00:00:00:10) which another producer's records interleaved

        write a@1           open a  hold
        fence b@2 for c     open a  skip
        ack a@3             open -  commit, opens at None
        ");
    }

    #[test]
    fn test_an_acknowledgement_which_rolls_back_is_refused() {
        let trace = trace(&[Write(0x10, 5), Ack(0x10, 6), Write(0x10, 7), Ack(0x10, 6)]);
        insta::assert_snapshot!(trace, @"
        write a@5           open a  hold
        ack a@6             open -  commit, opens at None
        write a@7           open a  hold
        ack a@6             open a  refused: acknowledgement of Producer(11:00:00:00:00:10) at Clock(0s 6000ns) rolls back records this rebuild sequenced
        ");
    }

    /// A recovered acknowledgement at or below its writer's last commit is already
    /// committed, whether or not records follow that commit, and so is one of a writer
    /// the rebuild never saw. One which covers exactly the delta held commits it. Any
    /// other is refused.
    #[test]
    fn test_what_a_recovered_acknowledgement_does() {
        let (a, b) = (producer(0x10), producer(0x30));
        let sequenced = |records: &[Record]| {
            let mut sequencer = Sequencer::default();
            for (index, &record) in records.iter().enumerate() {
                _ = sequencer
                    .on_record(&build(record), index as i64 * 100)
                    .unwrap();
            }
            sequencer
        };

        // `a` committed at 2 and then 4, and holds a delta of records at 5 and 6.
        let held = sequenced(&[
            Write(0x10, 1),
            Ack(0x10, 2),
            Write(0x10, 3),
            Ack(0x10, 4),
            Write(0x10, 5),
            Write(0x10, 6),
        ]);
        // `a` committed at 4, and holds nothing.
        let idle = sequenced(&[Write(0x10, 3), Ack(0x10, 4)]);
        // `b` displaced the delta `a` held.
        let displaced = sequenced(&[Write(0x10, 1), Write(0x30, 2)]);

        let cases = [
            (
                "the last commit, records after it",
                held.recovered_ack(a, clock(4)),
            ),
            (
                "an older commit, records after it",
                held.recovered_ack(a, clock(2)),
            ),
            (
                "the last commit, nothing after it",
                idle.recovered_ack(a, clock(4)),
            ),
            (
                "an older commit, nothing after it",
                idle.recovered_ack(a, clock(2)),
            ),
            ("a writer never seen", held.recovered_ack(b, clock(9))),
            ("the delta held", held.recovered_ack(a, clock(7))),
            ("part of the delta held", held.recovered_ack(a, clock(5))),
            (
                "records not in the journal",
                idle.recovered_ack(a, clock(9)),
            ),
            ("a displaced delta", displaced.recovered_ack(a, clock(3))),
            (
                "the delta which displaced it",
                displaced.recovered_ack(b, clock(3)),
            ),
        ];
        let mut trace = String::new();
        for (what, got) in cases {
            writeln!(trace, "{what:<36}{got:?}").unwrap();
        }
        insta::assert_snapshot!(trace, @"
        the last commit, records after it   Committed { older: false }
        an older commit, records after it   Committed { older: true }
        the last commit, nothing after it   Committed { older: false }
        an older commit, nothing after it   Committed { older: true }
        a writer never seen                 Committed { older: false }
        the delta held                      Commits
        part of the delta held              Refused
        records not in the journal          Refused
        a displaced delta                   Refused
        the delta which displaced it        Commits
        ");
    }
}
