//! Sequencing a journal's records into the deltas a replay holds and commits.
//!
//! [`uuid::sequence`] orders each producer's records: it drops the duplicates
//! at-least-once appends produce, and says which records begin a delta, extend one,
//! and acknowledge one. A [`Sequencer`] applies the rules of `replay` on top of that
//! across producers: which delta is held, when another producer's records displace
//! it, that a displaced delta is never taken up again, and which acknowledgements
//! can be honored. For each record it says what becomes of the delta held, as a
//! [`Step`], and does no I/O: `replay::Pass` holds, drops, and applies accordingly.

use crate::proto;
use anyhow::Context;
use proto_gazette::uuid;

/// Sequencing state of one pass over a journal.
#[derive(Default)]
pub(super) struct Sequencer {
    /// Sequencing state of each producer of the range.
    producers: std::collections::HashMap<uuid::Producer, Sequence>,
    /// Producer of the delta which began and is not yet acknowledged, and whose
    /// records the pass holds. Only its acknowledgement can be honored: another
    /// producer's records mean a delta whose commit order nothing states. It is
    /// `None` once a delta commits or is displaced.
    open: Option<uuid::Producer>,
    /// Offset at which the held delta's first record opened a horizon. It goes with
    /// the delta: a horizon belongs to the delta which opened it.
    horizon_at: Option<i64>,
    /// Offset through which committed records are applied. A held delta is not part
    /// of it, so this never runs ahead of what the client committed.
    applied: i64,
}

#[derive(Default, Clone, Copy)]
struct Sequence {
    /// Clocks which [`uuid::sequence`] transitions.
    last_commit: uuid::Clock,
    max_continue: uuid::Clock,
}

/// What one record does to the delta a pass holds.
#[derive(Debug, PartialEq)]
pub(super) struct Step {
    /// Producer of a held delta which this record displaced, and which the pass must
    /// drop before anything else.
    pub displaced: Option<uuid::Producer>,
    pub action: Action,
}

/// What becomes of one record.
#[derive(Debug, PartialEq)]
pub(super) enum Action {
    /// Nothing: it changes no disk content, repeats a record already sequenced, or is
    /// a fragment of a displaced delta.
    Skip,
    /// Hold it, as the first or a later record of the delta held.
    Hold,
    /// Apply the delta held, which this acknowledgement commits. `opens_at` is the
    /// offset at which that delta's first record opened a horizon, if it opened one.
    Commit { opens_at: Option<i64> },
}

impl Sequencer {
    /// Offset through which committed records are applied.
    pub fn applied(&self) -> i64 {
        self.applied
    }

    /// Sequence `record`, which begins at `offset` and frames `len` bytes, and say
    /// what it does to the delta held.
    ///
    /// Sequencing must see every record in order, because it decides the producer,
    /// the duplicates, and whether a delta is acknowledged. What that means for the
    /// image is deferred: a delta's chunks, the horizon it opens, and the horizon
    /// blocks it discharges are all held together, and all land at its commit.
    pub fn on_record(
        &mut self,
        record: &proto::DiskRecord,
        offset: i64,
        len: usize,
    ) -> anyhow::Result<Step> {
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
            // A fence carries the epoch it installs and changes no disk content.
            uuid::SequenceOutcome::OutsideCommit | uuid::SequenceOutcome::OutsideDuplicate => {
                anyhow::ensure!(
                    record.installs_epoch.len() == std::mem::size_of::<uuid::Producer>(),
                    "fence record of {producer:?} installs {} bytes of epoch",
                    record.installs_epoch.len(),
                );
                () = ensure_no_chunks(record, "a fence")?;

                // The delta this epoch displaced is not doomed by the fence itself. A
                // promotion appends the acknowledgement its client recovered
                // immediately after its own fence, and that still commits the delta.
                self.applied = offset + len as i64;

                Ok(step(Action::Skip))
            }
            uuid::SequenceOutcome::ContinueBeginSpan => {
                let displaced = self.displace(producer);
                self.open = Some(producer);

                // The horizon this record opens is held with it, and so is dropped
                // with it. A horizon belongs to its delta, so a delta which is never
                // acknowledged never opened one.
                self.horizon_at = record.opens_horizon.then_some(offset);

                Ok(Step {
                    displaced,
                    action: Action::Hold,
                })
            }
            uuid::SequenceOutcome::ContinueExtendSpan if self.open == Some(producer) => {
                Ok(step(Action::Hold))
            }
            // A delta which began, was displaced, and now resumes. The records it
            // began with were dropped, so these are a fragment of it and are not
            // held: were they held, its acknowledgement would commit the fragment.
            // Leaving `open` elsewhere refuses that acknowledgement. The fragment
            // still interleaves whatever is held, as any other producer's record
            // does.
            uuid::SequenceOutcome::ContinueExtendSpan => Ok(Step {
                displaced: self.displace(producer),
                action: Action::Skip,
            }),
            uuid::SequenceOutcome::ContinueDuplicate => Ok(step(Action::Skip)),

            // Another producer's records interleaved this delta, so its
            // acknowledgement cannot be honored: those records displaced what this
            // pass held, and nothing states the order the two deltas committed in.
            uuid::SequenceOutcome::AckCommit => {
                anyhow::ensure!(
                    self.open == Some(producer),
                    "acknowledgement of a delta of {producer:?} which another producer's \
                     records interleaved",
                );
                self.open = None;
                () = ensure_no_chunks(record, "an acknowledgement")?;

                self.applied = offset + len as i64;

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
            // this pass already applied. The append barrier makes one impossible
            // from this daemon, and this daemon is the only writer a disk journal
            // has.
            uuid::SequenceOutcome::AckCleanRollback | uuid::SequenceOutcome::AckDeepRollback => {
                anyhow::bail!(
                    "acknowledgement of {producer:?} at {clock:?} rolls back records this pass sequenced",
                )
            }
        }
    }

    /// Whether an acknowledgement of `producer` at `clock` is one this pass can
    /// honor: it commits the delta the pass holds, or it commits nothing. Any other
    /// acknowledgement fails [`Sequencer::on_record`], and one which reaches the
    /// journal fails every replay from then on, so a caller asks here before
    /// appending one.
    pub fn can_acknowledge(&self, producer: uuid::Producer, clock: uuid::Clock) -> bool {
        let mut state = self.producers.get(&producer).copied().unwrap_or_default();

        match uuid::sequence(
            uuid::Flags::ACK_TXN,
            clock,
            &mut state.last_commit,
            &mut state.max_continue,
        ) {
            Ok(uuid::SequenceOutcome::AckCommit) => self.open == Some(producer),
            Ok(uuid::SequenceOutcome::AckEmpty | uuid::SequenceOutcome::AckDuplicate) => true,
            _ => false,
        }
    }

    /// Give up the held delta, unless it is `producer`'s, and report whose it was.
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
        /// A fence of the producer seeded `.0`, which installs the epoch seeded `.2`.
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
            installs_epoch: match record {
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

    /// Sequence `records`, each framing 100 bytes, and render the producer whose delta
    /// is then held, how far committed state is then applied, and the step each took.
    fn trace(records: &[Record]) -> String {
        let mut sequencer = Sequencer::default();
        let mut out = String::new();

        for (index, &record) in records.iter().enumerate() {
            let outcome = match sequencer.on_record(&build(record), index as i64 * 100, 100) {
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

            writeln!(
                out,
                "{:<20}open {open}  applied {:<5}{outcome}",
                label(record),
                sequencer.applied(),
            )
            .unwrap();
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
        write a@2           open a  applied 0    hold
        write a@3           open a  applied 0    hold
        write a@3           open a  applied 0    skip
        ack a@4             open -  applied 400  commit, opens at None
        ack a@4             open -  applied 400  skip
        fence b@5 for a     open -  applied 600  skip
        write a@6           open a  applied 600  hold
        ack a@7             open -  applied 800  commit, opens at None
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
        opens a@1           open a  applied 0    hold
        write a@2           open a  applied 0    hold
        ack a@3             open -  applied 300  commit, opens at Some(0)
        opens a@4           open a  applied 300  hold
        write c@5           open c  applied 300  displaces a, hold
        ack c@6             open -  applied 600  commit, opens at None
        write a@7           open -  applied 600  skip
        opens a@8           open -  applied 600  refused: record of Producer(11:00:00:00:00:10) at Clock(0s 8000ns) opens a horizon but does not begin a delta
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
        write a@1           open a  applied 0    hold
        write c@2           open c  applied 0    displaces a, hold
        ack c@3             open -  applied 300  commit, opens at None
        write a@4           open -  applied 300  skip
        ack a@5             open -  applied 300  refused: acknowledgement of a delta of Producer(11:00:00:00:00:10) which another producer's records interleaved

        write a@1           open a  applied 0    hold
        fence b@2 for c     open a  applied 200  skip
        ack a@3             open -  applied 300  commit, opens at None
        ");
    }

    #[test]
    fn test_an_acknowledgement_which_rolls_back_is_refused() {
        let trace = trace(&[Write(0x10, 5), Ack(0x10, 6), Write(0x10, 7), Ack(0x10, 6)]);
        insta::assert_snapshot!(trace, @"
        write a@5           open a  applied 0    hold
        ack a@6             open -  applied 200  commit, opens at None
        write a@7           open a  applied 200  hold
        ack a@6             open a  applied 200  refused: acknowledgement of Producer(11:00:00:00:00:10) at Clock(0s 6000ns) rolls back records this pass sequenced
        ");
    }

    /// A recovered acknowledgement is appended only if a replay could honor it: it must
    /// commit the delta held, or commit nothing at all.
    #[test]
    fn test_which_acknowledgements_can_be_honored() {
        let (a, b) = (producer(0x10), producer(0x30));
        let sequenced = |records: &[Record]| {
            let mut sequencer = Sequencer::default();
            for (index, &record) in records.iter().enumerate() {
                _ = sequencer
                    .on_record(&build(record), index as i64 * 100, 100)
                    .unwrap();
            }
            sequencer
        };

        // Nothing is sequenced, so any acknowledgement commits nothing.
        assert!(sequenced(&[]).can_acknowledge(a, clock(5)));

        // A delta of `a` is held. Its acknowledgement commits it, one at or below its
        // last commit rolls back, and another producer's commits nothing.
        let held = sequenced(&[Write(0x10, 1), Ack(0x10, 2), Write(0x10, 3)]);
        assert!(held.can_acknowledge(a, clock(3)));
        assert!(held.can_acknowledge(a, clock(4)));
        assert!(!held.can_acknowledge(a, clock(2)));
        assert!(!held.can_acknowledge(a, clock(1)));
        assert!(held.can_acknowledge(b, clock(9)));

        // `b` displaced the delta of `a`, whose acknowledgement can no longer be honored.
        let displaced = sequenced(&[Write(0x10, 1), Write(0x30, 2)]);
        assert!(!displaced.can_acknowledge(a, clock(3)));
        assert!(displaced.can_acknowledge(b, clock(3)));
    }
}
