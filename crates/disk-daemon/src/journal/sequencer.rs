use crate::proto;
use anyhow::Context;
use proto_gazette::uuid;

/// Sequencing state of one replay of a disk from its journal.
#[derive(Default)]
pub struct Sequencer {
    /// Sequencing state of each producer of the range.
    producers: std::collections::HashMap<uuid::Producer, ProducerState>,
    /// Producer of the held delta.
    open: Option<uuid::Producer>,
}

impl Sequencer {
    /// Sequence every record in journal order. Return its effect without applying
    /// it: chunks and horizon changes remain pending together until commit.
    pub fn on_record(&mut self, record: &proto::DiskRecord) -> anyhow::Result<Step> {
        let uuid =
            uuid::Uuid::from_slice(&record.uuid).context("record carries no message UUID")?;
        let (producer, clock, flags) = uuid::parse(uuid)?;

        let step = |action| Step {
            displaced: None,
            action,
        };

        // A fence is the only OUTSIDE_TXN record this daemon appends, and the only
        // record at a zero clock. It changes no content, so it is never sequenced.
        if flags.is_outside() {
            anyhow::ensure!(
                clock == uuid::Clock::zero(),
                "an OUTSIDE_TXN record of {producer:?} at {clock:?}, though a fence's clock is zero",
            );
            () = ensure_no_chunks(record, "a fence")?;

            return Ok(step(Action::Skip));
        }
        anyhow::ensure!(
            clock != uuid::Clock::zero(),
            "a record of {producer:?} at a zero clock, though only a fence's clock is zero",
        );

        let state = self.producers.entry(producer).or_default();

        let outcome = uuid::sequence(
            flags,
            clock,
            &mut state.last_commit,
            &mut state.max_continue,
        )
        .with_context(|| {
            format!(
                "sequencing a record of {producer:?} at {clock:?} (last_commit {:?}, max_continue {:?})",
                state.last_commit, state.max_continue,
            )
        })?;

        match outcome {
            uuid::SequenceOutcome::OutsideCommit | uuid::SequenceOutcome::OutsideDuplicate => {
                unreachable!("an OUTSIDE_TXN record returns before it's sequenced")
            }
            uuid::SequenceOutcome::ContinueBeginSpan => {
                let displaced = self.displace(producer);
                self.open = Some(producer);

                Ok(Step {
                    displaced,
                    action: Action::Hold,
                })
            }
            uuid::SequenceOutcome::ContinueExtendSpan if self.open == Some(producer) => {
                Ok(step(Action::Hold))
            }
            // A later record of a delta which another producer's records displaced.
            // Its earlier records were dropped, so it can never commit: skip it. It
            // still interleaves any other producer's delta held now, so displace that.
            uuid::SequenceOutcome::ContinueExtendSpan => Ok(Step {
                displaced: self.displace(producer),
                action: Action::Skip,
            }),
            uuid::SequenceOutcome::ContinueDuplicate => Ok(step(Action::Skip)),

            // Another producer's records interleaved this delta, so its
            // acknowledgement cannot be honored: those records displaced what this
            // replay held, and nothing states the order the two deltas committed in.
            uuid::SequenceOutcome::AckCommit => {
                anyhow::ensure!(
                    self.open == Some(producer),
                    "acknowledgement of a delta of {producer:?} which another producer's \
                     records interleaved",
                );
                self.open = None;
                () = ensure_no_chunks(record, "an acknowledgement")?;

                Ok(step(Action::Commit))
            }
            // A delta whose records are all below the floor, or an
            // acknowledgement which was appended twice.
            uuid::SequenceOutcome::AckEmpty | uuid::SequenceOutcome::AckDuplicate => {
                () = ensure_no_chunks(record, "an acknowledgement")?;

                Ok(step(Action::Skip))
            }

            // A rollback takes back records, and a deep one takes back records
            // this replay already applied. The append barrier makes one impossible
            // from this daemon, and this daemon is the only writer a disk journal
            // has.
            uuid::SequenceOutcome::AckCleanRollback | uuid::SequenceOutcome::AckDeepRollback => {
                anyhow::bail!(
                    "acknowledgement of {producer:?} at {clock:?} rolls back records this replay sequenced",
                )
            }
        }
    }

    /// Classify a recovered acknowledgement before appending it. Only an ack of
    /// the entire held delta needs repair; incompatible acks would break future
    /// replays.
    pub fn recovered_ack(&self, producer: uuid::Producer, clock: uuid::Clock) -> RecoveredAck {
        let Some(mut state) = self.producers.get(&producer).copied() else {
            return RecoveredAck::Committed;
        };

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
            ) => RecoveredAck::Committed,
            // A writer the replay saw, whose delta records are not where its
            // acknowledgement says, or are no longer held.
            _ => RecoveredAck::Refused,
        }
    }

    /// Give up the uncommitted delta, unless it is `producer`'s, and report whose it was.
    fn displace(&mut self, producer: uuid::Producer) -> Option<uuid::Producer> {
        let held = self.open.filter(|held| *held != producer)?;
        self.open = None;

        Some(held)
    }
}

/// What one record does to the delta a replay holds.
#[derive(Debug, PartialEq)]
pub struct Step {
    /// Producer of an uncommitted delta which this record displaced, and which the
    /// replay must drop before anything else.
    pub displaced: Option<uuid::Producer>,
    /// How to handle this record after dropping any displaced delta.
    pub action: Action,
}

/// What becomes of one record.
#[derive(Debug, PartialEq)]
pub enum Action {
    /// Nothing: it changes no disk content, repeats a record already sequenced, or is
    /// a fragment of a displaced delta.
    Skip,
    /// Hold it, as the first or a later record of the delta held.
    Hold,
    /// Apply the delta held, which this acknowledgement commits.
    Commit,
}

/// What a recovered acknowledgement does, judged against what a replay sequenced.
#[derive(Debug, PartialEq)]
pub enum RecoveredAck {
    /// Already committed, so there is nothing to append. Either it is at or below its
    /// writer's last commit, which a client whose state went back may hand over, or
    /// the replay saw no records of its writer, so they are all below the floor.
    Committed,
    /// It commits the delta the replay holds, so appending it commits that delta.
    Commits,
    /// It commits records this replay no longer holds: part of the delta held, or a
    /// delta another producer displaced, or a delta whose records are not in the
    /// journal. Appending it would fail every replay from then on.
    Refused,
}

/// The clocks [`uuid::sequence`] transitions for one producer.
#[derive(Default, Clone, Copy)]
struct ProducerState {
    /// Clock of the producer's last commit.
    last_commit: uuid::Clock,
    /// Greatest clock of the producer's uncommitted records, or zero where it has
    /// none.
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
    use crate::test_support;
    use proto_gazette::uuid;
    use std::fmt::Write as _;

    #[derive(Clone, Copy)]
    enum Record {
        /// A record of the named producer at the given clock, extending or beginning
        /// its delta.
        Write(char, u64),
        /// The named producer's acknowledgement at the given clock.
        Ack(char, u64),
        /// A fence, appended by the named author.
        Fence(char),
        /// An `OUTSIDE_TXN` record of the named producer at a nonzero clock.
        Outside(char, u64),
    }
    use Record::*;

    fn build(record: Record) -> proto::DiskRecord {
        let producer = test_support::producer;

        match record {
            Write(name, ticks) => {
                test_support::record(producer(name), ticks, uuid::Flags::CONTINUE_TXN, Vec::new())
            }
            Ack(name, ticks) => {
                test_support::record(producer(name), ticks, uuid::Flags::ACK_TXN, Vec::new())
            }
            Fence(name) => test_support::fence(producer(name)),
            Outside(name, ticks) => {
                test_support::record(producer(name), ticks, uuid::Flags::OUTSIDE_TXN, Vec::new())
            }
        }
    }

    fn label(record: Record) -> String {
        match record {
            Write(name, ticks) => format!("write {name}@{ticks}"),
            Ack(name, ticks) => format!("ack {name}@{ticks}"),
            Fence(name) => format!("fence {name}"),
            Outside(name, ticks) => format!("outside {name}@{ticks}"),
        }
    }

    /// Sequence `records`, and render the producer whose delta is then held and the
    /// step each took.
    fn trace(records: &[Record]) -> String {
        let mut sequencer = Sequencer::default();
        let mut out = String::new();

        for &record in records {
            let outcome = match sequencer.on_record(&build(record)) {
                Ok(Step { displaced, action }) => {
                    let displaced = displaced
                        .map(|held| format!("displaces {}, ", test_support::name(held)))
                        .unwrap_or_default();
                    let action = match action {
                        Action::Skip => "skip".to_string(),
                        Action::Hold => "hold".to_string(),
                        Action::Commit => "commit".to_string(),
                    };
                    format!("{displaced}{action}")
                }
                Err(err) => format!("refused: {err:#}"),
            };
            let open = sequencer.open.map_or('-', test_support::name);

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
            Write('a', 2),
            Write('a', 3),
            Write('a', 3),
            Ack('a', 4),
            Ack('a', 4),
            Fence('b'),
            Write('a', 6),
            Ack('a', 7),
        ]);
        insta::assert_snapshot!(trace, @"
        write a@2           open a  hold
        write a@3           open a  hold
        write a@3           open a  skip
        ack a@4             open -  commit
        ack a@4             open -  skip
        fence b             open -  skip
        write a@6           open a  hold
        ack a@7             open -  commit
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
                Write('a', 1),
                Write('c', 2),
                Ack('c', 3),
                Write('a', 4),
                Ack('a', 5),
            ]),
            trace(&[Write('a', 1), Fence('b'), Ack('a', 3)]),
        ]
        .join("\n");
        insta::assert_snapshot!(trace, @"
        write a@1           open a  hold
        write c@2           open c  displaces a, hold
        ack c@3             open -  commit
        write a@4           open -  skip
        ack a@5             open -  refused: acknowledgement of a delta of Producer(61:00:00:00:00:61) which another producer's records interleaved

        write a@1           open a  hold
        fence b             open a  skip
        ack a@3             open -  commit
        ");
    }

    /// A fence is never sequenced, so it is skipped wherever it lands: within its own
    /// author's delta, repeated, or within another author's. An `OUTSIDE_TXN` record
    /// at any other clock is not one this daemon appends.
    #[test]
    fn test_a_fence_is_never_sequenced() {
        let trace = trace(&[
            Write('a', 1),
            Fence('a'),
            Fence('a'),
            Fence('b'),
            Ack('a', 2),
            Outside('c', 3),
        ]);
        insta::assert_snapshot!(trace, @"
        write a@1           open a  hold
        fence a             open a  skip
        fence a             open a  skip
        fence b             open a  skip
        ack a@2             open -  commit
        outside c@3         open -  refused: an OUTSIDE_TXN record of Producer(63:00:00:00:00:63) at Clock(0s 3000ns), though a fence's clock is zero
        ");
    }

    /// An acknowledgement commits the whole delta held. One which rolls back records
    /// this replay sequenced is refused, as is one which commits only part of them.
    #[test]
    fn test_an_acknowledgement_of_less_than_the_delta_held_is_refused() {
        let trace = [
            trace(&[Write('a', 5), Ack('a', 6), Write('a', 7), Ack('a', 6)]),
            trace(&[Write('a', 5), Write('a', 7), Ack('a', 6)]),
        ]
        .join("\n");
        insta::assert_snapshot!(trace, @"
        write a@5           open a  hold
        ack a@6             open -  commit
        write a@7           open a  hold
        ack a@6             open a  refused: acknowledgement of Producer(61:00:00:00:00:61) at Clock(0s 6000ns) rolls back records this replay sequenced

        write a@5           open a  hold
        write a@7           open a  hold
        ack a@6             open a  refused: sequencing a record of Producer(61:00:00:00:00:61) at Clock(0s 6000ns) (last_commit Clock(0s 0ns), max_continue Clock(0s 7000ns)): unexpected ACK_TXN which only partially acknowledges preceding CONTINUE_TXN messages
        ");
    }

    /// A recovered acknowledgement at or below its writer's last commit is already
    /// committed, whether or not records follow that commit, and so is one of a writer
    /// the replay never saw. One which covers exactly the delta held commits it. Any
    /// other is refused.
    #[test]
    fn test_what_a_recovered_acknowledgement_does() {
        let (a, b) = (test_support::producer('a'), test_support::producer('b'));
        let sequenced = |records: &[Record]| {
            let mut sequencer = Sequencer::default();
            for &record in records {
                _ = sequencer.on_record(&build(record)).unwrap();
            }
            sequencer
        };

        // `a` committed at 2 and then 4, and holds a delta of records at 5 and 6.
        let held = sequenced(&[
            Write('a', 1),
            Ack('a', 2),
            Write('a', 3),
            Ack('a', 4),
            Write('a', 5),
            Write('a', 6),
        ]);
        // `a` committed at 4, and holds nothing.
        let idle = sequenced(&[Write('a', 3), Ack('a', 4)]);
        // `b` displaced the delta `a` held.
        let displaced = sequenced(&[Write('a', 1), Write('b', 2)]);

        let cases = [
            (
                "the last commit, records after it",
                held.recovered_ack(a, test_support::clock(4)),
            ),
            (
                "an older commit, records after it",
                held.recovered_ack(a, test_support::clock(2)),
            ),
            (
                "the last commit, nothing after it",
                idle.recovered_ack(a, test_support::clock(4)),
            ),
            (
                "an older commit, nothing after it",
                idle.recovered_ack(a, test_support::clock(2)),
            ),
            (
                "a writer never seen",
                held.recovered_ack(b, test_support::clock(9)),
            ),
            (
                "the delta held",
                held.recovered_ack(a, test_support::clock(7)),
            ),
            (
                "part of the delta held",
                held.recovered_ack(a, test_support::clock(5)),
            ),
            (
                "records not in the journal",
                idle.recovered_ack(a, test_support::clock(9)),
            ),
            (
                "a displaced delta",
                displaced.recovered_ack(a, test_support::clock(3)),
            ),
            (
                "the delta which displaced it",
                displaced.recovered_ack(b, test_support::clock(3)),
            ),
        ];
        let mut trace = String::new();
        for (what, got) in cases {
            writeln!(trace, "{what:<36}{got:?}").unwrap();
        }
        insta::assert_snapshot!(trace, @"
        the last commit, records after it   Committed
        an older commit, records after it   Committed
        the last commit, nothing after it   Committed
        an older commit, nothing after it   Committed
        a writer never seen                 Committed
        the delta held                      Commits
        part of the delta held              Refused
        records not in the journal          Refused
        a displaced delta                   Refused
        the delta which displaced it        Commits
        ");
    }
}
