//! Mutation admission and recovery-horizon progress.
//!
//! Mutations queue in arrival order. The owner records and applies each one
//! before admitting another or processing a cut, so backpressure cannot
//! reorder mutations or split one across deltas.
//!
//! Recorded changes discharge covered horizon blocks and earn copy budget.
//! Copies run only when no device mutation is parked, and a cut ends the
//! current delta's budget.
//!
//! | Mode   | Mutations         | Horizon copies            | A cut                         |
//! |--------|-------------------|---------------------------|-------------------------------|
//! | Open   | admitted in order | offered if none is parked | closes admission, ends budget |
//! | Closed | parked            | not offered               | keeps it closed               |
//! | Failed | admitted in order | not offered               | refused                       |
//!
//! Two sequences must remain unbroken:
//!
//! - Reserve, record, apply or fail, and complete a device mutation.
//! - Reserve, select, read, and record a horizon copy. [`HorizonCopy`] borrows
//!   admission throughout, preventing a mutation or cut between read and record.
//!
//! This module selects work; the backend performs image I/O and replies.

use super::recording;
use crate::horizon;
use crate::image;
use crate::proto;

/// The recording gate and the horizon discharged by admitted mutations.
pub(super) struct Admission {
    /// Mutation channel to the journal writer.
    recorder: recording::Recorder,
    /// Admission and cut state.
    mode: Mode,
    /// Mutations waiting for room, or for a cut to end, in arrival order.
    parked: std::collections::VecDeque<Mutation>,
    /// The horizon this disk is discharging. Its bitmap is as large as the image's
    /// allocated one, so it is held only while a horizon is open.
    horizon: Option<horizon::Horizon>,
    /// Horizon opening thresholds and copy budget.
    policy: horizon::Policy,
}

enum Mode {
    Open,
    /// The cut of a prepare, until admission resumes.
    Closed,
    /// The first image-write or horizon-copy failure; all later cuts fail.
    /// Mutations remain admissible for unmount writes during teardown.
    /// Copies stop because the delta cannot commit.
    Failed(anyhow::Error),
}

impl Admission {
    /// Resume the horizon left open by replay, if any.
    pub fn new(
        recorder: recording::Recorder,
        horizon: Option<horizon::Horizon>,
        policy: horizon::Policy,
    ) -> Self {
        Self {
            recorder,
            mode: Mode::Open,
            parked: std::collections::VecDeque::new(),
            horizon,
            policy,
        }
    }

    /// Park `mutation` behind every mutation offered before it, until
    /// [`Admission::admit`] takes it. It is never dropped or refused.
    pub fn offer(&mut self, mutation: Mutation) {
        self.parked.push_back(mutation);
    }

    /// Record the oldest parked mutation and return it for immediate application.
    /// Recorded blocks discharge the horizon and changed bytes earn copy budget.
    /// Closed admission leaves all parked mutations for the next delta.
    pub fn admit(&mut self) -> Option<Admitted> {
        // Only a mutation which could be admitted now waits on room. A closed
        // admission reopens by a command, and a command wakes the owner itself.
        if matches!(self.mode, Mode::Closed) || self.parked.is_empty() {
            return None;
        }
        let permit = self.recorder.reserve()?;
        let Mutation { chunks, admitted } = self.parked.pop_front().expect("one is parked");

        if let Some(horizon) = &mut self.horizon {
            () = horizon.published(admitted.change.range());
            () = horizon.changed(crate::chunk::data_bytes(&chunks));
        }
        () = permit.send(chunks);
        Some(admitted)
    }

    pub fn parked(&self) -> usize {
        self.parked.len()
    }

    /// Cut the delta and its copy budget. The caller must have applied every
    /// previously admitted mutation.
    pub fn close(&mut self) -> Result<(), &anyhow::Error> {
        match self.mode {
            Mode::Failed(ref failed) => Err(failed),
            Mode::Open | Mode::Closed => {
                self.mode = Mode::Closed;

                if let Some(horizon) = &mut self.horizon {
                    () = horizon.cut();
                }
                Ok(())
            }
        }
    }

    /// Reopen closed admission, preserving any failure.
    pub fn resume(&mut self) {
        if let Mode::Closed = self.mode {
            self.mode = Mode::Open;
        }
    }

    /// Keep the first failure for all later cuts. Return it only when newly kept.
    pub fn fail_cuts(&mut self, err: anyhow::Error) -> Option<&anyhow::Error> {
        match self.mode {
            Mode::Open => self.mode = Mode::Failed(err),
            Mode::Failed(_) => return None,
            // What fails is an admitted mutation or a copy, and a closed admission
            // offers neither.
            Mode::Closed => panic!("a disk failed while its admission was closed: {err:#}"),
        }
        let Mode::Failed(kept) = &self.mode else {
            unreachable!("the disk has just failed")
        };
        Some(kept)
    }

    /// Open a horizon over `allocated`, the image's allocated blocks, if a journal
    /// `range` of that many bytes above the floor warrants one. Report whether it
    /// did.
    pub fn open_horizon(&mut self, range: u64, allocated: &image::Bitmap) -> bool {
        // The writer opens a horizon only while it has none, and a replay hands a
        // horizon it left open to both of them.
        assert!(
            self.horizon.is_none(),
            "asked to open a recovery horizon over one already open"
        );
        let bytes = allocated.count_ones() as u64 * crate::BLOCK_SIZE as u64;

        if !self.policy.opens(range, bytes) {
            return false;
        }
        self.horizon = Some(horizon::Horizon::open(allocated));
        true
    }

    /// The next horizon run of at most `limit` blocks to copy, if a copy may be
    /// offered now and the delta's budget pays for one.
    ///
    /// Room is reserved before a run is selected: a full channel is the usual
    /// reason to stop, and selecting scans the horizon's bitmap, while a permit left
    /// unsent costs nothing.
    pub fn next_copy(&mut self, limit: u32) -> Option<HorizonCopy<'_>> {
        let Self {
            recorder,
            mode,
            parked,
            horizon,
            policy,
        } = self;
        let horizon = horizon.as_mut()?;

        if !matches!(mode, Mode::Open) || !parked.is_empty() {
            return None;
        }
        let permit = recorder.reserve()?;
        let run = horizon.next_copy(policy, limit)?;

        Some(HorizonCopy {
            run,
            permit,
            horizon,
        })
    }

    /// Blocks which still owe the open horizon a copy, and zero if none is open.
    pub fn horizon_pending(&self) -> u32 {
        self.horizon.as_ref().map_or(0, horizon::Horizon::pending)
    }

    /// Drop the horizon a commit has completed, and its bitmap with it.
    pub fn close_horizon(&mut self) {
        self.horizon = None;
    }
}

/// What a device request changes in the image.
pub(super) enum Change {
    /// Retain request data until the parked write can be applied.
    Write { start: u32, data: bytes::Bytes },
    /// A discard or write-zeroes, which carries no data. Both deallocate, per
    /// `chunk::encode_punch`.
    Punch(std::ops::Range<u32>),
}

impl Change {
    /// The blocks this change covers, which it publishes.
    pub fn range(&self) -> std::ops::Range<u32> {
        match self {
            Change::Write { start, data } => {
                *start..*start + (data.len() / crate::BLOCK_SIZE as usize) as u32
            }
            Change::Punch(range) => range.clone(),
        }
    }
}

/// One device request's mutation, which is recorded and then applied.
pub(super) struct Mutation {
    /// The chunks which make the mutation durable.
    chunks: Vec<proto::Chunk>,
    /// What the owner applies once `chunks` are recorded.
    admitted: Admitted,
}

impl Mutation {
    pub fn new(tag: u16, change: Change) -> Self {
        let chunks = match &change {
            Change::Write { start, data } => crate::chunk::encode_write(*start, data),
            Change::Punch(range) => {
                vec![crate::chunk::encode_punch(range.start, range.len() as u32)]
            }
        };

        Self {
            chunks,
            admitted: Admitted { tag, change },
        }
    }
}

/// A recorded mutation the owner must apply before admitting another.
/// During teardown, the recorder may discard its chunks; the image must still
/// accept the mutation so unmount writes can finish.
pub(super) struct Admitted {
    pub tag: u16,
    pub change: Change,
}

/// A horizon run which may be copied now, and the room reserved for it. It holds
/// its admission until it is sent, so nothing is admitted or cut in between.
pub(super) struct HorizonCopy<'a> {
    run: std::ops::Range<u32>,
    permit: recording::Permit<'a>,
    horizon: &'a mut horizon::Horizon,
}

impl HorizonCopy<'_> {
    /// The blocks the owner reads out of the image for this copy.
    pub fn run(&self) -> std::ops::Range<u32> {
        self.run.clone()
    }

    /// Record `data`, which the owner read out of the image at
    /// [`HorizonCopy::run`], and discharge the run. A copy dropped unsent records
    /// and discharges nothing.
    pub fn send(self, data: bytes::Bytes) {
        assert_eq!(
            data.len(),
            self.run.len() * crate::BLOCK_SIZE as usize,
            "a horizon copy holds its whole run",
        );
        let chunks = crate::chunk::encode_write(self.run.start, &data);

        () = self
            .horizon
            .copied(self.run, crate::chunk::data_bytes(&chunks));
        () = self.permit.send(chunks);
    }
}

#[cfg(test)]
mod test {
    use super::{Admission, Admitted, Change, Mutation};
    use crate::horizon;
    use crate::image;
    use std::fmt::Write as _;

    /// Any journal range beyond the allocated size opens a horizon, and a delta may
    /// copy as many bytes as it changed.
    const POLICY: horizon::Policy = horizon::Policy {
        open_ratio: 1.0,
        copy_ratio: 1.0,
        minimum_bytes: 0,
    };
    /// Most blocks one copy takes.
    const COPY_LIMIT: u32 = 8;
    const BLOCKS: u32 = 16;

    #[derive(Debug, Clone, Copy)]
    enum Step {
        /// Offer a one-block punch, whose tag and block are both this. It
        /// discharges its block and earns no copy budget.
        Punch(u16),
        /// Offer a one-block write, whose tag and block are both this. It
        /// discharges its block and earns one block of copy budget.
        Write(u16),
        Admit,
        /// Have the consumer take one mutation.
        Take,
        Close,
        Resume,
        Fail(&'static str),
        /// Open a horizon over these allocated blocks, if a journal range of this
        /// many bytes warrants one.
        Open(u64, &'static [u32]),
        /// Take the next horizon copy, if one may be offered, and send it.
        Copy,
        CloseHorizon,
    }
    use Step::*;

    /// Run `steps` against an admission over a channel of `capacity`, and render
    /// what each returned, the blocks the horizon still owes, and what is parked.
    fn run(capacity: usize, steps: &[Step]) -> String {
        let (recorder, mut recorded) =
            crate::device::recording::channel(capacity, std::task::Waker::noop().clone());
        let mut admission = Admission::new(recorder, None, POLICY);
        let mut out = String::new();

        for &step in steps {
            let outcome = match step {
                Punch(tag) => {
                    let punch = Change::Punch(tag as u32..tag as u32 + 1);
                    () = admission.offer(Mutation::new(tag, punch));
                    "-".to_string()
                }
                Write(tag) => {
                    let write = Change::Write {
                        start: tag as u32,
                        data: vec![0xff; crate::BLOCK_SIZE as usize].into(),
                    };
                    () = admission.offer(Mutation::new(tag, write));
                    "-".to_string()
                }
                Admit => match admission.admit() {
                    Some(Admitted { tag, .. }) => format!("admitted {tag}"),
                    None => "-".to_string(),
                },
                Take => match recorded.try_recv() {
                    Ok(chunks) => format!("took block {}", chunks[0].block),
                    Err(_) => "nothing to take".to_string(),
                },
                Close => match admission.close() {
                    Ok(()) => "closed".to_string(),
                    Err(err) => format!("refused: {err:#}"),
                },
                Resume => {
                    admission.resume();
                    "resumed".to_string()
                }
                Fail(what) => match admission.fail_cuts(anyhow::anyhow!(what)) {
                    Some(_) => "kept".to_string(),
                    None => "dropped".to_string(),
                },
                Open(range, blocks) => {
                    let mut allocated = image::Bitmap::repeat(false, BLOCKS as usize);
                    for &block in blocks {
                        allocated.set(block as usize, true);
                    }
                    match admission.open_horizon(range, &allocated) {
                        true => "opened".to_string(),
                        false => "refused".to_string(),
                    }
                }
                Copy => match admission.next_copy(COPY_LIMIT) {
                    Some(copy) => {
                        let run = copy.run();
                        () = copy.send(vec![0xff; run.len() * crate::BLOCK_SIZE as usize].into());
                        format!("copied {run:?}")
                    }
                    None => "refused".to_string(),
                },
                CloseHorizon => {
                    admission.close_horizon();
                    "closed".to_string()
                }
            };
            let horizon = match &admission.horizon {
                Some(horizon) => horizon.pending().to_string(),
                None => "-".to_string(),
            };
            let parked: Vec<u16> = admission.parked.iter().map(|m| m.admitted.tag).collect();
            let step = format!("{step:?}");

            writeln!(
                out,
                "{step:<26}{outcome:<16}horizon {horizon:<4}parked {parked:?}"
            )
            .unwrap();
        }
        out
    }

    #[test]
    fn test_a_full_channel_parks_in_arrival_order() {
        let trace = run(
            1,
            &[
                Punch(1),
                Admit,
                Punch(2),
                Punch(3),
                Admit,
                Take,
                // The channel has room, and only the oldest parked mutation may
                // take it.
                Punch(4),
                Admit,
                Admit,
                Take,
                Admit,
                Take,
                Admit,
                Admit,
                Take,
            ],
        );
        insta::assert_snapshot!(trace, @"
        Punch(1)                  -               horizon -   parked [1]
        Admit                     admitted 1      horizon -   parked []
        Punch(2)                  -               horizon -   parked [2]
        Punch(3)                  -               horizon -   parked [2, 3]
        Admit                     -               horizon -   parked [2, 3]
        Take                      took block 1    horizon -   parked [2, 3]
        Punch(4)                  -               horizon -   parked [2, 3, 4]
        Admit                     admitted 2      horizon -   parked [3, 4]
        Admit                     -               horizon -   parked [3, 4]
        Take                      took block 2    horizon -   parked [3, 4]
        Admit                     admitted 3      horizon -   parked [4]
        Take                      took block 3    horizon -   parked [4]
        Admit                     admitted 4      horizon -   parked []
        Admit                     -               horizon -   parked []
        Take                      took block 4    horizon -   parked []
        ");
    }

    #[test]
    fn test_a_closed_admission_parks_until_resumed() {
        let trace = run(
            8,
            &[
                Open(1 << 20, &[9]),
                Write(1),
                Admit,
                Close,
                Write(2),
                Write(3),
                Admit,
                Copy,
                Resume,
                // Resuming admits nothing by itself, and the parked mutations still
                // come ahead of any copy.
                Copy,
                Admit,
                Admit,
                Copy,
                Punch(4),
                Admit,
            ],
        );
        insta::assert_snapshot!(trace, @"
        Open(1048576, [9])        opened          horizon 1   parked []
        Write(1)                  -               horizon 1   parked [1]
        Admit                     admitted 1      horizon 1   parked []
        Close                     closed          horizon 1   parked []
        Write(2)                  -               horizon 1   parked [2]
        Write(3)                  -               horizon 1   parked [2, 3]
        Admit                     -               horizon 1   parked [2, 3]
        Copy                      refused         horizon 1   parked [2, 3]
        Resume                    resumed         horizon 1   parked [2, 3]
        Copy                      refused         horizon 1   parked [2, 3]
        Admit                     admitted 2      horizon 1   parked [3]
        Admit                     admitted 3      horizon 1   parked []
        Copy                      copied 9..10    horizon 0   parked []
        Punch(4)                  -               horizon 0   parked [4]
        Admit                     admitted 4      horizon 0   parked []
        ");
    }

    #[test]
    fn test_a_horizon_copy_waits_for_room() {
        let trace = run(
            1,
            &[
                Open(1 << 20, &[9, 11]),
                Write(1),
                Admit,
                Take,
                Write(2),
                Admit,
                Take,
                // The budget pays for two blocks, and the channel has room for one
                // copy.
                Copy,
                Copy,
                Write(3),
                Take,
                // The room a take frees goes to the parked mutation.
                Copy,
                Admit,
                Take,
                Copy,
            ],
        );
        insta::assert_snapshot!(trace, @"
        Open(1048576, [9, 11])    opened          horizon 2   parked []
        Write(1)                  -               horizon 2   parked [1]
        Admit                     admitted 1      horizon 2   parked []
        Take                      took block 1    horizon 2   parked []
        Write(2)                  -               horizon 2   parked [2]
        Admit                     admitted 2      horizon 2   parked []
        Take                      took block 2    horizon 2   parked []
        Copy                      copied 9..10    horizon 1   parked []
        Copy                      refused         horizon 1   parked []
        Write(3)                  -               horizon 1   parked [3]
        Take                      took block 9    horizon 1   parked [3]
        Copy                      refused         horizon 1   parked [3]
        Admit                     admitted 3      horizon 1   parked []
        Take                      took block 3    horizon 1   parked []
        Copy                      copied 11..12   horizon 0   parked []
        ");
    }

    #[test]
    fn test_a_delta_earns_and_spends_its_copy_budget() {
        let trace = run(
            8,
            &[
                // Three allocated blocks are 12 KiB, which this range does not exceed.
                Open(8 << 10, &[1, 2, 3]),
                Open(1 << 20, &[1, 2, 3]),
                Copy,
                // A punch discharges the block it covers, but changes no content, so
                // it earns nothing to copy with.
                Punch(2),
                Admit,
                Copy,
                Write(5),
                Admit,
                Copy,
                Copy,
                // A cut ends the delta, and the budget this write earned with it.
                Write(6),
                Admit,
                Close,
                Resume,
                Copy,
                Write(7),
                Admit,
                Copy,
                CloseHorizon,
            ],
        );
        insta::assert_snapshot!(trace, @"
        Open(8192, [1, 2, 3])     refused         horizon -   parked []
        Open(1048576, [1, 2, 3])  opened          horizon 3   parked []
        Copy                      refused         horizon 3   parked []
        Punch(2)                  -               horizon 3   parked [2]
        Admit                     admitted 2      horizon 2   parked []
        Copy                      refused         horizon 2   parked []
        Write(5)                  -               horizon 2   parked [5]
        Admit                     admitted 5      horizon 2   parked []
        Copy                      copied 1..2     horizon 1   parked []
        Copy                      refused         horizon 1   parked []
        Write(6)                  -               horizon 1   parked [6]
        Admit                     admitted 6      horizon 1   parked []
        Close                     closed          horizon 1   parked []
        Resume                    resumed         horizon 1   parked []
        Copy                      refused         horizon 1   parked []
        Write(7)                  -               horizon 1   parked [7]
        Admit                     admitted 7      horizon 1   parked []
        Copy                      copied 3..4     horizon 0   parked []
        CloseHorizon              closed          horizon -   parked []
        ");
    }

    #[test]
    fn test_a_failed_disk_refuses_every_cut_and_copy() {
        let trace = run(
            4,
            &[
                Open(1 << 20, &[9]),
                Write(1),
                Admit,
                Fail("first"),
                Fail("second"),
                Close,
                // Admission stays open for the unmount a teardown makes, and no copy
                // is offered, though the budget would pay for one.
                Write(2),
                Admit,
                Copy,
                // A mutation still discharges what it covers.
                Punch(9),
                Admit,
                Resume,
                Close,
            ],
        );
        insta::assert_snapshot!(trace, @r#"
        Open(1048576, [9])        opened          horizon 1   parked []
        Write(1)                  -               horizon 1   parked [1]
        Admit                     admitted 1      horizon 1   parked []
        Fail("first")             kept            horizon 1   parked []
        Fail("second")            dropped         horizon 1   parked []
        Close                     refused: first  horizon 1   parked []
        Write(2)                  -               horizon 1   parked [2]
        Admit                     admitted 2      horizon 1   parked []
        Copy                      refused         horizon 1   parked []
        Punch(9)                  -               horizon 1   parked [9]
        Admit                     admitted 9      horizon 0   parked []
        Resume                    resumed         horizon 0   parked []
        Close                     refused: first  horizon 0   parked []
        "#);
    }

    #[test]
    #[should_panic(expected = "a disk failed while its admission was closed")]
    fn test_a_closed_admission_cannot_fail() {
        run(4, &[Close, Fail("first")]);
    }

    #[test]
    #[should_panic(expected = "asked to open a recovery horizon over one already open")]
    fn test_a_horizon_cannot_open_over_another() {
        run(4, &[Open(1 << 20, &[1]), Open(1 << 20, &[1])]);
    }
}
