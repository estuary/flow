use anyhow::Context;
use proto_gazette::uuid;

/// Sequencing state of one replay of a disk from its journal.
#[derive(Default)]
pub(super) struct Sequencer {
    /// Producer of the latest delta record or acknowledgement.
    producer: Option<uuid::Producer>,
    /// Clock of the producer's last commit.
    last_commit: uuid::Clock,
    /// Greatest clock of the producer's held delta.
    max_continue: uuid::Clock,
}

impl Sequencer {
    /// Sequence each record in journal order.
    pub fn on_record(&mut self, uuid: &[u8]) -> anyhow::Result<(bool, Action)> {
        let uuid = uuid::Uuid::from_slice(uuid).context("record carries no message UUID")?;
        let (producer, clock, flags) = uuid::parse(uuid)?;

        // A fence is the only OUTSIDE_TXN record this daemon appends. It's the
        // new primary's first record, and doesn't roll anything back: the
        // promotion which appended it may yet re-acknowledge, and so commit,
        // the delta held. So it neither changes the producer nor sequences
        // against its clocks.
        if flags.is_outside() {
            return Ok((false, Action::Skip));
        }

        // A record of a different producer shows that a fence stopped the
        // previous producer, and that it can never commit. The new producer
        // starts with zero clocks.
        let new_producer = self
            .producer
            .replace(producer)
            .is_some_and(|previous| previous != producer);
        if new_producer {
            (self.last_commit, self.max_continue) = Default::default();
        }

        let outcome = uuid::sequence(flags, clock, &mut self.last_commit, &mut self.max_continue)
            .with_context(|| {
                format!(
                    "sequencing a record of {producer:?} at {clock:?} (last_commit {:?}, max_continue {:?})",
                    self.last_commit, self.max_continue,
                )
            })?;

        Ok(match outcome {
            uuid::SequenceOutcome::OutsideCommit | uuid::SequenceOutcome::OutsideDuplicate => {
                unreachable!("an OUTSIDE_TXN record returns before it's sequenced")
            }
            uuid::SequenceOutcome::ContinueBeginSpan
            | uuid::SequenceOutcome::ContinueExtendSpan => (new_producer, Action::Hold),
            uuid::SequenceOutcome::AckCommit => (new_producer, Action::Commit),
            uuid::SequenceOutcome::ContinueDuplicate
            | uuid::SequenceOutcome::AckDuplicate
            | uuid::SequenceOutcome::AckEmpty => (new_producer, Action::Skip),
            uuid::SequenceOutcome::AckCleanRollback | uuid::SequenceOutcome::AckDeepRollback => {
                (true, Action::Skip)
            }
        })
    }
}

/// What becomes of one record.
#[derive(Debug, PartialEq)]
pub(super) enum Action {
    /// Nothing: it changes no disk content, or repeats a record already sequenced.
    Skip,
    /// Hold it, as the first or a later record of the delta held.
    Hold,
    /// Apply the delta held, which this acknowledgement commits.
    Commit,
}

#[cfg(test)]
mod test {
    use super::Action::{Commit, Hold, Skip};
    use super::Sequencer;
    use proto_gazette::uuid;

    #[test]
    fn test_sequencing() {
        let [a, b, c] = [1, 2, 3].map(|n| uuid::Producer::from_bytes([0x01, 0, 0, 0, 0, n]));
        let (write, ack, fence) = (
            uuid::Flags::CONTINUE_TXN,
            uuid::Flags::ACK_TXN,
            uuid::Flags::OUTSIDE_TXN,
        );

        // (name, records sequenced in order as (flags, producer, clock, rolled_back, action))
        let cases = [
            // Duplicates are skipped. A fence rolls nothing back, so the promotion which
            // appended it may still commit the delta it re-acknowledges.
            (
                "commit across a fence",
                vec![
                    (write, a, 1, false, Hold),
                    (write, a, 1, false, Skip),
                    (fence, b, 0, false, Skip),
                    (ack, a, 2, false, Commit),
                    (ack, a, 2, false, Skip),
                ],
            ),
            // Another producer's record rolls back the delta held.
            (
                "another producer",
                vec![
                    (write, a, 1, false, Hold),
                    (fence, b, 0, false, Skip),
                    (write, b, 2, true, Hold),
                    (ack, b, 3, false, Commit),
                ],
            ),
            // A later promotion re-appended an earlier producer's acknowledgement. It
            // commits nothing, not even the delta it once acknowledged.
            (
                "earlier producer's ack",
                vec![
                    (write, a, 1, false, Hold),
                    (fence, b, 0, false, Skip),
                    (write, b, 2, true, Hold),
                    (fence, c, 0, false, Skip),
                    (ack, a, 3, true, Skip),
                ],
            ),
            // Re-acknowledging the last commit rolls back the records after it.
            (
                "re-ack of last commit",
                vec![
                    (write, a, 1, false, Hold),
                    (ack, a, 2, false, Commit),
                    (write, a, 3, false, Hold),
                    (fence, b, 0, false, Skip),
                    (ack, a, 2, true, Skip),
                ],
            ),
        ];

        for (name, records) in cases {
            let mut sequencer = Sequencer::default();

            for (index, (flags, producer, clock, rolled_back, action)) in
                records.into_iter().enumerate()
            {
                let uuid = uuid::build(producer, uuid::Clock::from_u64(clock), flags);
                let step = sequencer.on_record(uuid.as_bytes()).unwrap();
                assert_eq!(step, (rolled_back, action), "{name}: record {index}");
            }
        }
    }
}
