use super::queue;
use super::recording;
use crate::horizon;
use crate::image;
use crate::proto;
use crate::ublk;

/// Most blocks one horizon copy reads, so a copy is a mutation of the same order
/// as the largest device request.
const COPY_BLOCKS: u32 = ublk::MAX_IO_BUF_BYTES / crate::BLOCK_SIZE;

/// The disk's ublk target, which answers its requests and records each mutation
/// for the journal writer.
///
/// Mutations are recorded, applied, and answered one at a time in arrival order,
/// so backpressure cannot reorder them or split one across deltas.
pub(super) struct Target {
    /// Kernel device ID, which names this disk in logs and errors.
    dev_id: u32,
    /// The disk's content, which reads are served from and mutations applied to.
    image: image::Image,
    /// Mutation channel to the journal writer.
    recorder: recording::Recorder,
    /// Mutations waiting for room, or for a cut to end, oldest first.
    parked: std::collections::VecDeque<(u16, Change)>,
    /// Whether a mutation is admitted now, and whether a cut may close admission.
    admission: Admission,
    /// The horizon this disk is discharging, held only while one is open.
    horizon: Option<horizon::Horizon>,
    /// Horizon opening thresholds and copy budget.
    policy: horizon::Policy,
    /// The next delta's journal range above the floor, which decides whether it
    /// opens a horizon. Zero until the bootstrap commit resumes admission, so setup
    /// writes open no horizon.
    range: u64,
    /// Whether the delta since the last cut has recorded anything.
    recorded: bool,
}

impl Target {
    /// `horizon` resumes any horizon left open by replay.
    pub fn new(
        dev_id: u32,
        image: image::Image,
        recorder: recording::Recorder,
        horizon: Option<horizon::Horizon>,
        policy: horizon::Policy,
    ) -> Self {
        Self {
            dev_id,
            image,
            recorder,
            parked: std::collections::VecDeque::new(),
            admission: Admission::Open,
            horizon,
            policy,
            range: 0,
            recorded: false,
        }
    }

    /// Size of the disk, in blocks.
    pub fn blocks(&self) -> u32 {
        self.image.blocks()
    }

    /// Close admission, ending the delta, and return whether it discharged the
    /// open horizon. That horizon is dropped now: a failed commit ends the
    /// attachment anyway.
    pub fn close_admission(&mut self) -> anyhow::Result<bool> {
        if let Admission::Failed(failed) = &self.admission {
            anyhow::bail!("device {} failed: {failed:#}", self.dev_id);
        }
        self.admission = Admission::Closed;
        let recorded = std::mem::take(&mut self.recorded);

        let Some(horizon) = &mut self.horizon else {
            return Ok(false);
        };
        // An empty delta commits nothing, so completes nothing.
        if recorded && horizon.pending() == 0 {
            self.horizon = None;
            return Ok(true);
        }
        () = horizon.cut();
        Ok(false)
    }

    /// Reopen admission with the next delta's journal `range`.
    pub fn resume_admission(&mut self, range: u64) {
        self.range = range;

        if matches!(self.admission, Admission::Closed) {
            self.admission = Admission::Open;
        }
    }

    /// Handle a request. A read is answered at once.
    pub fn on_request(
        &mut self,
        tag: u16,
        request: queue::Request,
        replies: &mut Vec<(u16, queue::Reply)>,
    ) {
        let change = match request {
            queue::Request::Read(range) => return replies.push((tag, self.read(range))),
            queue::Request::Write { start, data } => Change::Write { start, data },
            queue::Request::Punch(range) => Change::Punch(range),
        };
        self.parked.push_back((tag, change))
    }

    /// Record, apply, and answer parked mutations, oldest first, until admission is
    /// closed or the channel has no room. Each is applied before the next is
    /// recorded, so the image takes them in journal order.
    pub fn admit(&mut self, replies: &mut Vec<(u16, queue::Reply)>) {
        // A closed admission reopens by a command, which wakes the owner itself, so
        // only a mutation which could be admitted now waits on room.
        while !matches!(self.admission, Admission::Closed) && !self.parked.is_empty() {
            let Some(permit) = self.recorder.reserve() else {
                return;
            };
            let (tag, change) = self.parked.pop_front().expect("one is parked");
            let chunks = change.chunks();

            // Only a delta's first mutation opens a horizon, so a replay from its record
            // sees every chunk which discharges it. Snapshot allocation before the
            // mutation applies, as replay does.
            let opens_horizon = !self.recorded
                && self.horizon.is_none()
                && self.policy.opens(
                    self.range,
                    self.image.allocated().count_ones() as u64 * crate::BLOCK_SIZE as u64,
                );
            if opens_horizon {
                let horizon = horizon::Horizon::open(self.image.allocated());
                tracing::info!(
                    dev_id = self.dev_id,
                    range = self.range,
                    pending = horizon.pending(),
                    "opened a recovery horizon"
                );
                self.horizon = Some(horizon);
            }
            if let Some(horizon) = &mut self.horizon {
                () = horizon.published(change.range());
                () = horizon.changed(crate::chunk::data_bytes(&chunks));
            }
            () = permit.send(recording::Mutation {
                chunks,
                opens_horizon,
            });
            self.recorded = true;
            replies.push((tag, self.apply(change)));
        }
    }

    /// Copy runs of the open horizon's pending blocks into the recording channel, as
    /// far as the copy budget this delta's mutations earned allows. Copies yield to
    /// mutations, running only while admission is open and nothing is parked.
    pub fn compact(&mut self) {
        let Some(horizon) = &mut self.horizon else {
            return;
        };
        if !matches!(self.admission, Admission::Open) || !self.parked.is_empty() {
            return;
        }
        // Room is reserved before a run is selected: a full channel is the usual
        // reason to stop, and selecting scans the horizon's bitmap.
        while let Some(permit) = self.recorder.reserve() {
            let Some(run) = horizon.next_copy(&self.policy, COPY_BLOCKS) else {
                return;
            };
            let mut data = vec![0; run.len() * crate::BLOCK_SIZE as usize];

            // An image which cannot be read back has failed, and every pass would
            // retry this run. No request asked for the copy, so fail the next cut,
            // which ends the attachment.
            if let Err(err) = self.image.read_at(run.start, &mut data) {
                let err = anyhow::Error::new(err).context("copying a horizon run out of the image");
                self.admission = Admission::Failed(err);
                return;
            }
            let chunks = crate::chunk::encode_write(run.start, &data.into());

            () = horizon.copied(run, crate::chunk::data_bytes(&chunks));
            () = permit.send(recording::Mutation {
                chunks,
                opens_horizon: false,
            });
            self.recorded = true;
        }
    }

    /// Read `range` out of the image.
    fn read(&self, range: std::ops::Range<u32>) -> queue::Reply {
        let mut buf = vec![0; range.len() * crate::BLOCK_SIZE as usize];

        match self.image.read_at(range.start, &mut buf) {
            Ok(()) => queue::Reply::Data(buf),
            Err(err) => {
                tracing::error!(dev_id = self.dev_id, ?err, "reading the image failed");
                queue::Reply::Failed(libc::EIO)
            }
        }
    }

    /// Apply a recorded mutation to the image.
    fn apply(&mut self, change: Change) -> queue::Reply {
        let (applied, bytes) = match &change {
            Change::Write { start, data } => (self.image.write_at(*start, data), data.len() as u32),
            Change::Punch(range) => (self.image.punch(range.start, range.len() as u32), 0),
        };

        match applied {
            Ok(()) => queue::Reply::Done(bytes),
            // This mutation was already sent to the writer, but the image refused it,
            // perhaps after taking part of it. Its delta must never commit, so refuse
            // every later cut: the attachment ends, and replay discards the delta.
            Err(err) => {
                if matches!(self.admission, Admission::Open) {
                    let err = anyhow::Error::new(err).context("applying a mutation to the image");
                    self.admission = Admission::Failed(err);
                }
                queue::Reply::Failed(libc::EIO)
            }
        }
    }
}

/// Whether the target admits mutations, and whether a cut may close it.
enum Admission {
    /// Mutations are admitted in arrival order, and horizon copies run while
    /// none is parked.
    Open,
    /// A prepare's cut closed admission until it resumes.
    Closed,
    /// A recorded mutation failed to reach the image, or a copy could not read
    /// it. Every later cut is refused, so the delta never commits.
    Failed(anyhow::Error),
}

/// What a device request changes in the image.
enum Change {
    Write { start: u32, data: bytes::Bytes },
    Punch(std::ops::Range<u32>),
}

impl Change {
    /// The blocks this change covers, which it publishes to a horizon.
    fn range(&self) -> std::ops::Range<u32> {
        match self {
            Change::Write { start, data } => {
                *start..*start + (data.len() / crate::BLOCK_SIZE as usize) as u32
            }
            Change::Punch(range) => range.clone(),
        }
    }

    /// The chunks which make this change durable.
    fn chunks(&self) -> Vec<proto::Chunk> {
        match self {
            Change::Write { start, data } => crate::chunk::encode_write(*start, data),
            Change::Punch(range) => {
                vec![crate::chunk::encode_punch(range.start, range.len() as u32)]
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::Target;
    use crate::BLOCK_SIZE;
    use crate::device;
    use crate::device::queue;
    use crate::horizon;
    use crate::image;
    use crate::proto;
    use std::fmt::Write as _;

    /// Blocks of the image a trace or property case serves.
    const BLOCKS: u32 = 16;

    /// Any journal range beyond the allocated size opens a horizon, and a delta may
    /// copy as many bytes as it changed.
    const POLICY: horizon::Policy = horizon::Policy {
        open_ratio: 1.0,
        copy_ratio: 1.0,
        minimum_bytes: 0,
    };

    /// A journal range which opens a horizon over any disk, under [`POLICY`].
    const WIDE: u64 = u64::MAX;

    /// A target over a recording channel, and the consumer's end of that channel.
    struct Case {
        /// The target under test.
        target: Target,
        /// `None` once the consumer has gone away.
        recorded: Option<device::Recorded>,
        /// Every mutation the consumer took, in the order a journal would hold them.
        taken: Vec<Vec<proto::Chunk>>,
        /// Whether each mutation of `taken` opened a horizon.
        opens: Vec<bool>,
    }

    impl Case {
        fn new(image: image::Image, capacity: usize, policy: horizon::Policy) -> Self {
            let (recorder, recorded) =
                crate::device::recording::channel(capacity, std::task::Waker::noop().clone());

            Self {
                target: Target::new(0, image, recorder, None, policy),
                recorded: Some(recorded),
                taken: Vec::new(),
                opens: Vec::new(),
            }
        }

        /// Hand over `request`, then admit what admission lets through, as one pass
        /// of the owner does.
        fn request(&mut self, tag: u16, request: queue::Request) -> Vec<(u16, queue::Reply)> {
            let mut replies = Vec::new();
            () = self.target.on_request(tag, request, &mut replies);
            () = self.target.admit(&mut replies);
            replies
        }

        fn write(
            &mut self,
            tag: u16,
            start: u32,
            blocks: u32,
            fill: u8,
        ) -> Vec<(u16, queue::Reply)> {
            let data = vec![fill; (blocks * BLOCK_SIZE) as usize].into();
            self.request(tag, queue::Request::Write { start, data })
        }

        fn punch(&mut self, tag: u16, start: u32, blocks: u32) -> Vec<(u16, queue::Reply)> {
            self.request(tag, queue::Request::Punch(start..start + blocks))
        }

        /// Admit what admission lets through, as each pass of the owner does.
        fn admit(&mut self) -> Vec<(u16, queue::Reply)> {
            let mut replies = Vec::new();
            () = self.target.admit(&mut replies);
            replies
        }

        /// Have the consumer take one mutation, if one is queued.
        fn take(&mut self) -> Option<device::Mutation> {
            let mutation = self.recorded.as_mut()?.try_recv().ok()?;
            self.taken.push(mutation.chunks.clone());
            self.opens.push(mutation.opens_horizon);
            Some(mutation)
        }

        /// Take every queued mutation, and return how many.
        fn take_all(&mut self) -> usize {
            std::iter::from_fn(|| self.take()).count()
        }
    }

    fn fresh_image() -> image::Image {
        image::Image::create(&std::env::temp_dir(), BLOCKS).unwrap()
    }

    /// `/dev/full`, which refuses every write.
    fn full_image() -> image::Image {
        let full = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/full")
            .unwrap();
        image::Image::from_file(full, BLOCKS)
    }

    /// A write-only image, which takes every write and refuses every read.
    fn unreadable_image() -> image::Image {
        let mut options = std::fs::OpenOptions::new();
        options.write(true);
        std::os::unix::fs::OpenOptionsExt::custom_flags(&mut options, libc::O_TMPFILE);
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);

        let file = options.open(std::env::temp_dir()).unwrap();
        file.set_len((BLOCKS * BLOCK_SIZE) as u64).unwrap();
        image::Image::from_file(file, BLOCKS)
    }

    /// Render replies readably: the bytes a request transferred, the first byte of a
    /// read's data, or the name of the errno it failed with.
    fn render(replies: &[(u16, queue::Reply)]) -> String {
        if replies.is_empty() {
            return "-".to_string();
        }
        let replies: Vec<String> = replies
            .iter()
            .map(|(tag, reply)| match reply {
                queue::Reply::Done(bytes) => format!("{tag}: {bytes} bytes"),
                queue::Reply::Data(buf) => format!("{tag}: read {}", buf[0]),
                queue::Reply::Failed(libc::EIO) => format!("{tag}: EIO"),
                queue::Reply::Failed(errno) => format!("{tag}: errno {errno}"),
            })
            .collect();
        format!("replied [{}]", replies.join(", "))
    }

    /// Report allocated blocks and their fill bytes, using `?` for unreadable blocks.
    fn allocated(image: &image::Image) -> String {
        let mut block = vec![0u8; BLOCK_SIZE as usize];

        let blocks: Vec<String> = image
            .allocated()
            .iter_ones()
            .map(|index| match image.read_at(index as u32, &mut block) {
                Ok(()) => format!("({index}, {})", block[0]),
                Err(_) => format!("({index}, ?)"),
            })
            .collect();
        format!("[{}]", blocks.join(", "))
    }

    #[derive(Debug, Clone, Copy)]
    enum Step {
        /// The request at `.0` writes the block at `.1`, filled with `.2`.
        Write(u16, u32, u8),
        /// The request at `.0` reads the block at `.1`.
        Read(u16, u32),
        /// Admit what admission lets through, as each pass of the owner does.
        Admit,
        Take,
        DropConsumer,
        Cut,
        /// Resume over a range which opens no horizon.
        Resume,
        /// Resume over a range which opens a horizon at the next delta.
        ResumeWide,
        /// Spend the open delta's copy budget, as each pass of the owner does.
        Compact,
    }
    use Step::*;

    /// Trace replies, parked mutations, pending horizon blocks, and image allocation.
    fn trace(case: &mut Case, steps: &[Step]) -> String {
        let mut out = String::new();

        for &step in steps {
            let outcome = match step {
                Write(tag, block, fill) => render(&case.write(tag, block, 1, fill)),
                Read(tag, block) => {
                    render(&case.request(tag, queue::Request::Read(block..block + 1)))
                }
                Admit => render(&case.admit()),
                Take => match case.take() {
                    Some(device::Mutation {
                        chunks,
                        opens_horizon,
                    }) => {
                        let blocks = crate::chunk::covered_blocks(&chunks[0]);
                        let opens = if opens_horizon {
                            ", opens a horizon"
                        } else {
                            ""
                        };
                        format!("took {blocks:?}{opens}")
                    }
                    None => "nothing to take".to_string(),
                },
                DropConsumer => {
                    case.recorded = None;
                    "dropped".to_string()
                }
                Cut => match case.target.close_admission() {
                    Ok(false) => "closed".to_string(),
                    Ok(true) => "closed, discharged".to_string(),
                    Err(err) => format!("refused: {err:#}"),
                },
                Resume => {
                    case.target.resume_admission(0);
                    "resumed".to_string()
                }
                ResumeWide => {
                    case.target.resume_admission(WIDE);
                    "resumed".to_string()
                }
                Compact => {
                    () = case.target.compact();
                    "-".to_string()
                }
            };
            let parked = case.target.parked.len();
            let horizon = match &case.target.horizon {
                Some(horizon) => horizon.pending().to_string(),
                None => "-".to_string(),
            };
            let image = allocated(&case.target.image);
            let step = format!("{step:?}");

            writeln!(
                out,
                "{step:<16}{outcome:<36}  parked {parked}  horizon {horizon:<2} image {image}"
            )
            .unwrap();
        }
        out
    }

    #[test]
    fn test_requests_parking_and_failures() {
        let cases: &[(&str, fn() -> image::Image, usize, &[Step])] = &[
            (
                "a dropped consumer frees parked requests",
                fresh_image,
                1,
                &[
                    Write(1, 0, 1),
                    Write(2, 1, 2),
                    Write(3, 2, 3),
                    Admit,
                    // A channel without a consumer takes every mutation, and discards
                    // it, so the unmount of a disk being torn down completes.
                    DropConsumer,
                    Admit,
                ],
            ),
            (
                "a cut parks mutations until admission resumes",
                fresh_image,
                4,
                &[
                    Write(1, 0, 1),
                    Cut,
                    Write(2, 1, 2),
                    // Reads go on while admission is closed, and a parked write has
                    // not reached the image.
                    Read(3, 1),
                    Admit,
                    Resume,
                    Admit,
                ],
            ),
            (
                "a delta's first mutation opens a horizon, and the cut which finds it \
                 discharged drops it",
                fresh_image,
                8,
                &[
                    Write(1, 0, 1),
                    Write(2, 1, 2),
                    Cut,
                    // An empty delta opens nothing, whatever the range.
                    ResumeWide,
                    Cut,
                    ResumeWide,
                    // Blocks 0 and 1 are allocated before the opener, which itself
                    // publishes block 2.
                    Write(3, 2, 3),
                    Write(4, 0, 4),
                    Take,
                    Take,
                    Take,
                    Take,
                    Cut,
                    // Already open, so this delta opens nothing.
                    ResumeWide,
                    Write(5, 1, 5),
                    Take,
                    Cut,
                    Resume,
                    Write(6, 3, 6),
                    Take,
                    Cut,
                ],
            ),
            (
                "a copy waits for room, and behind any parked mutation",
                fresh_image,
                1,
                &[
                    Write(1, 0, 1),
                    Take,
                    Cut,
                    ResumeWide,
                    // A write elsewhere opens a horizon over block 0, earns the budget
                    // of its copy, and fills the channel.
                    Write(2, 5, 2),
                    Compact,
                    Write(3, 6, 3),
                    Take,
                    // The room a take frees goes to the parked mutation first.
                    Compact,
                    Admit,
                    Take,
                    Compact,
                ],
            ),
            // An image write the host refuses fails its own request, and then every
            // cut which follows: its mutation was recorded, so its delta must never
            // commit.
            (
                "a failed image write fails every later cut",
                full_image,
                4,
                &[
                    Write(1, 0, 1),
                    Take,
                    Cut,
                    // Admission stays open for the unmount a teardown makes, so a later
                    // request is answered rather than parked.
                    Write(2, 0, 2),
                    Resume,
                    Cut,
                ],
            ),
            // A read the image refuses recorded nothing, so the cut goes ahead.
            (
                "a failed image read fails only its request",
                unreadable_image,
                4,
                &[Write(1, 0, 1), Read(2, 0), Cut],
            ),
            // A horizon run which cannot be read out of the image records nothing, and
            // no request asked for it. It fails every later cut instead.
            (
                "an unreadable horizon run fails every later cut",
                unreadable_image,
                4,
                &[
                    Write(1, 0, 1),
                    Cut,
                    ResumeWide,
                    // A write elsewhere opens a horizon over block 0, and earns the
                    // budget of its copy.
                    Write(2, 5, 2),
                    Compact,
                    Cut,
                ],
            ),
        ];

        let mut out = String::new();
        for (name, image, capacity, steps) in cases {
            let mut case = Case::new(image(), *capacity, POLICY);
            writeln!(out, "{name}:").unwrap();
            out.push_str(&trace(&mut case, steps));
        }
        insta::assert_snapshot!(out, @"
        a dropped consumer frees parked requests:
        Write(1, 0, 1)  replied [1: 4096 bytes]               parked 0  horizon -  image [(0, 1)]
        Write(2, 1, 2)  -                                     parked 1  horizon -  image [(0, 1)]
        Write(3, 2, 3)  -                                     parked 2  horizon -  image [(0, 1)]
        Admit           -                                     parked 2  horizon -  image [(0, 1)]
        DropConsumer    dropped                               parked 2  horizon -  image [(0, 1)]
        Admit           replied [2: 4096 bytes, 3: 4096 bytes]  parked 0  horizon -  image [(0, 1), (1, 2), (2, 3)]
        a cut parks mutations until admission resumes:
        Write(1, 0, 1)  replied [1: 4096 bytes]               parked 0  horizon -  image [(0, 1)]
        Cut             closed                                parked 0  horizon -  image [(0, 1)]
        Write(2, 1, 2)  -                                     parked 1  horizon -  image [(0, 1)]
        Read(3, 1)      replied [3: read 0]                   parked 1  horizon -  image [(0, 1)]
        Admit           -                                     parked 1  horizon -  image [(0, 1)]
        Resume          resumed                               parked 1  horizon -  image [(0, 1)]
        Admit           replied [2: 4096 bytes]               parked 0  horizon -  image [(0, 1), (1, 2)]
        a delta's first mutation opens a horizon, and the cut which finds it discharged drops it:
        Write(1, 0, 1)  replied [1: 4096 bytes]               parked 0  horizon -  image [(0, 1)]
        Write(2, 1, 2)  replied [2: 4096 bytes]               parked 0  horizon -  image [(0, 1), (1, 2)]
        Cut             closed                                parked 0  horizon -  image [(0, 1), (1, 2)]
        ResumeWide      resumed                               parked 0  horizon -  image [(0, 1), (1, 2)]
        Cut             closed                                parked 0  horizon -  image [(0, 1), (1, 2)]
        ResumeWide      resumed                               parked 0  horizon -  image [(0, 1), (1, 2)]
        Write(3, 2, 3)  replied [3: 4096 bytes]               parked 0  horizon 2  image [(0, 1), (1, 2), (2, 3)]
        Write(4, 0, 4)  replied [4: 4096 bytes]               parked 0  horizon 1  image [(0, 4), (1, 2), (2, 3)]
        Take            took 0..1                             parked 0  horizon 1  image [(0, 4), (1, 2), (2, 3)]
        Take            took 1..2                             parked 0  horizon 1  image [(0, 4), (1, 2), (2, 3)]
        Take            took 2..3, opens a horizon            parked 0  horizon 1  image [(0, 4), (1, 2), (2, 3)]
        Take            took 0..1                             parked 0  horizon 1  image [(0, 4), (1, 2), (2, 3)]
        Cut             closed                                parked 0  horizon 1  image [(0, 4), (1, 2), (2, 3)]
        ResumeWide      resumed                               parked 0  horizon 1  image [(0, 4), (1, 2), (2, 3)]
        Write(5, 1, 5)  replied [5: 4096 bytes]               parked 0  horizon 0  image [(0, 4), (1, 5), (2, 3)]
        Take            took 1..2                             parked 0  horizon 0  image [(0, 4), (1, 5), (2, 3)]
        Cut             closed, discharged                    parked 0  horizon -  image [(0, 4), (1, 5), (2, 3)]
        Resume          resumed                               parked 0  horizon -  image [(0, 4), (1, 5), (2, 3)]
        Write(6, 3, 6)  replied [6: 4096 bytes]               parked 0  horizon -  image [(0, 4), (1, 5), (2, 3), (3, 6)]
        Take            took 3..4                             parked 0  horizon -  image [(0, 4), (1, 5), (2, 3), (3, 6)]
        Cut             closed                                parked 0  horizon -  image [(0, 4), (1, 5), (2, 3), (3, 6)]
        a copy waits for room, and behind any parked mutation:
        Write(1, 0, 1)  replied [1: 4096 bytes]               parked 0  horizon -  image [(0, 1)]
        Take            took 0..1                             parked 0  horizon -  image [(0, 1)]
        Cut             closed                                parked 0  horizon -  image [(0, 1)]
        ResumeWide      resumed                               parked 0  horizon -  image [(0, 1)]
        Write(2, 5, 2)  replied [2: 4096 bytes]               parked 0  horizon 1  image [(0, 1), (5, 2)]
        Compact         -                                     parked 0  horizon 1  image [(0, 1), (5, 2)]
        Write(3, 6, 3)  -                                     parked 1  horizon 1  image [(0, 1), (5, 2)]
        Take            took 5..6, opens a horizon            parked 1  horizon 1  image [(0, 1), (5, 2)]
        Compact         -                                     parked 1  horizon 1  image [(0, 1), (5, 2)]
        Admit           replied [3: 4096 bytes]               parked 0  horizon 1  image [(0, 1), (5, 2), (6, 3)]
        Take            took 6..7                             parked 0  horizon 1  image [(0, 1), (5, 2), (6, 3)]
        Compact         -                                     parked 0  horizon 0  image [(0, 1), (5, 2), (6, 3)]
        a failed image write fails every later cut:
        Write(1, 0, 1)  replied [1: EIO]                      parked 0  horizon -  image []
        Take            took 0..1                             parked 0  horizon -  image []
        Cut             refused: device 0 failed: applying a mutation to the image: No space left on device (os error 28)  parked 0  horizon -  image []
        Write(2, 0, 2)  replied [2: EIO]                      parked 0  horizon -  image []
        Resume          resumed                               parked 0  horizon -  image []
        Cut             refused: device 0 failed: applying a mutation to the image: No space left on device (os error 28)  parked 0  horizon -  image []
        a failed image read fails only its request:
        Write(1, 0, 1)  replied [1: 4096 bytes]               parked 0  horizon -  image [(0, ?)]
        Read(2, 0)      replied [2: EIO]                      parked 0  horizon -  image [(0, ?)]
        Cut             closed                                parked 0  horizon -  image [(0, ?)]
        an unreadable horizon run fails every later cut:
        Write(1, 0, 1)  replied [1: 4096 bytes]               parked 0  horizon -  image [(0, ?)]
        Cut             closed                                parked 0  horizon -  image [(0, ?)]
        ResumeWide      resumed                               parked 0  horizon -  image [(0, ?)]
        Write(2, 5, 2)  replied [2: 4096 bytes]               parked 0  horizon 1  image [(0, ?), (5, ?)]
        Compact         -                                     parked 0  horizon 1  image [(0, ?), (5, ?)]
        Cut             refused: device 0 failed: copying a horizon run out of the image: Bad file descriptor (os error 9)  parked 0  horizon 1  image [(0, ?), (5, ?)]
        ");
    }

    #[derive(Clone, Debug)]
    enum Op {
        Write { start: u32, blocks: u32, fill: u8 },
        Punch { start: u32, blocks: u32 },
        Admit,
        Take,
        Cut,
        Resume { wide: bool },
        Compact,
    }

    impl quickcheck::Arbitrary for Op {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            // Small, overlapping ranges, so that mutations of the same blocks park
            // behind one another and race the copies of an open horizon. A copy goes
            // only while the channel has room and nothing is parked, so takes and
            // compactions are as common as the mutations which fill the channel.
            let start = u32::arbitrary(g) % BLOCKS;
            let blocks = 1 + u32::arbitrary(g) % std::cmp::min(4, BLOCKS - start);

            match u8::arbitrary(g) % 14 {
                0..=2 => Op::Write {
                    start,
                    blocks,
                    fill: u8::arbitrary(g),
                },
                3 => Op::Punch { start, blocks },
                4 | 5 => Op::Admit,
                6..=8 => Op::Take,
                9 | 10 => Op::Cut,
                11 | 12 => Op::Resume {
                    wide: bool::arbitrary(g),
                },
                _ => Op::Compact,
            }
        }
    }

    /// Run `ops` against a channel of `capacity`, then drain it, compacting as the
    /// owner does while the consumer takes. Every request must be answered once, in
    /// the order it arrived, and the stream the consumer took must replay to the
    /// image served. Its horizons must sequence as a writer requires.
    fn answers_in_order_and_replays_identically(capacity: u8, ops: Vec<Op>) -> bool {
        let mut case = Case::new(fresh_image(), 1 + capacity as usize % 3, POLICY);
        let (mut expected, mut answered) = (Vec::new(), Vec::new());
        // Each cut's position in `case.taken`, and whether it discharged the horizon.
        let mut cuts = Vec::new();

        for op in ops {
            let tag = expected.len() as u16;

            match op {
                Op::Write {
                    start,
                    blocks,
                    fill,
                } => {
                    expected.push((tag, queue::Reply::Done(blocks * BLOCK_SIZE)));
                    answered.extend(case.write(tag, start, blocks, fill));
                }
                Op::Punch { start, blocks } => {
                    expected.push((tag, queue::Reply::Done(0)));
                    answered.extend(case.punch(tag, start, blocks));
                }
                Op::Admit => answered.extend(case.admit()),
                Op::Take => _ = case.take(),
                Op::Cut => {
                    let discharged = case.target.close_admission().unwrap();
                    // As a writer's prepare drains the delta.
                    _ = case.take_all();
                    cuts.push((case.taken.len(), discharged));
                }
                Op::Resume { wide } => case.target.resume_admission(if wide { WIDE } else { 0 }),
                Op::Compact => case.target.compact(),
            }
        }

        case.target.resume_admission(0);
        while !case.target.parked.is_empty() {
            _ = case.take_all();
            answered.extend(case.admit());
        }
        // Whatever budget the open delta has left goes to copies, each of which must
        // carry the image's content to the replay.
        loop {
            () = case.target.compact();

            if case.take_all() == 0 {
                break;
            }
        }

        assert_eq!(
            answered, expected,
            "requests were answered out of order, or not at all"
        );
        () = crate::test_support::assert_replays_identically(&case.target.image, &case.taken);
        () = assert_horizons_sequence(&case.opens, &cuts);
        true
    }

    /// Check that horizons sequence as the ledger and replay require: one opens only at
    /// a delta's first mutation and while none is open, and a cut discharges only
    /// an open horizon of a non-empty delta.
    fn assert_horizons_sequence(opens: &[bool], cuts: &[(usize, bool)]) {
        let (mut open, mut begins) = (false, 0);
        let mut cuts = cuts.iter().peekable();

        for index in 0..=opens.len() {
            while let Some(&(end, discharged)) = cuts.next_if(|(end, _)| *end == index) {
                if discharged {
                    assert!(open, "a cut at {end} discharged no open horizon");
                    assert!(end > begins, "an empty delta at {end} was discharged");
                    open = false;
                }
                begins = end;
            }
            if opens.get(index) == Some(&true) {
                assert_eq!(index, begins, "a horizon opened within a delta");
                assert!(!open, "a horizon opened at {index} over an open one");
                open = true;
            }
        }
    }

    #[test]
    fn test_any_interleaving_answers_in_order_and_replays_identically() {
        quickcheck::QuickCheck::new()
            .tests(64)
            .quickcheck(answers_in_order_and_replays_identically as fn(u8, Vec<Op>) -> bool);
    }
}
