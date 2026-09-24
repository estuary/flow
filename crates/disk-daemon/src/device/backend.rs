//! What serves one disk's requests once its queue has handed them over: the image,
//! and the admission in front of the recording channel. This is the disk's block
//! backend, as QEMU and NBD say it. `ublk` calls it the server's target.
//!
//! A backend knows nothing of the ring or the character device. The queue hands over
//! each request as a [`Request`], a write's data already taken from the device, and
//! the backend answers each one it finishes with a [`Reply`], which the queue then
//! completes. Every decision about a request is therefore here, and a case exercises
//! it over an ordinary temporary file, with no device and no privilege.
//!
//! A read is served out of the image. A write or a punch is offered to admission, and
//! applied to the image the moment admission records it, so the image takes
//! mutations in exactly the order the journal does, with nothing to track in between.
//! Its request is answered once it is applied. Every step is a blocking call on the
//! owner's thread, so a slow call into the host filesystem holds up every other
//! request of the disk while it runs, which the crate README weighs.

use super::Command;
use super::admission::{Admission, Admitted, Change, Mutation};
use super::queue::{Reply, Request};
use crate::image::Image;
use crate::ublk;

/// Most blocks one horizon copy reads, so a copy is a mutation of the same order
/// as the largest device request.
const COPY_BLOCKS: u32 = ublk::MAX_IO_BUF_BYTES / crate::BLOCK_SIZE;

/// One disk's image, and the admission which decides when a mutation reaches it.
pub(super) struct Backend {
    dev_id: u32,
    admission: Admission,
    image: Image,
}

impl Backend {
    pub fn new(dev_id: u32, admission: Admission, image: Image) -> Self {
        Self {
            dev_id,
            admission,
            image,
        }
    }

    pub fn blocks(&self) -> u32 {
        self.image.blocks()
    }

    /// Mutations offered and not yet applied.
    pub fn parked(&self) -> usize {
        self.admission.parked()
    }

    pub fn on_command(&mut self, command: Command) {
        match command {
            Command::CloseAdmission(closed) => {
                let cut = self
                    .admission
                    .close()
                    .map_err(|failed| anyhow::anyhow!("device {} failed: {failed:#}", self.dev_id));

                _ = closed.send(cut);
            }
            Command::ResumeAdmission => self.admission.resume(),
            Command::OpenHorizon(range, reply) => {
                let opened = self.admission.open_horizon(range, self.image.allocated());

                if opened {
                    tracing::info!(
                        dev_id = self.dev_id,
                        range,
                        pending = self.admission.horizon_pending(),
                        "opened a recovery horizon"
                    );
                }
                _ = reply.send(opened);
            }
            Command::HorizonPending(reply) => _ = reply.send(self.admission.horizon_pending()),
            Command::CloseHorizon => self.admission.close_horizon(),
        }
    }

    /// Serve the request the queue handed over at `tag`, and append to `replies` the
    /// reply of each request this finishes.
    ///
    /// A read, or a request this daemon does not serve, finishes at once. A write or
    /// a punch parks behind every mutation offered before it, and finishes once it is
    /// recorded and applied: now, or at a later [`Backend::admit`]. Admitting it may
    /// finish mutations parked ahead of it too.
    pub fn on_request(&mut self, tag: u16, request: Request, replies: &mut Vec<(u16, Reply)>) {
        let change = match request {
            Request::Read(range) => return replies.push((tag, self.read(range))),
            Request::Write { start, data } => Change::Write { start, data },
            Request::Punch(range) => Change::Punch(range),
            Request::Unsupported(op) => {
                tracing::warn!(dev_id = self.dev_id, op, "unsupported device request");
                return replies.push((tag, Reply::Failed(libc::EOPNOTSUPP)));
            }
        };
        () = self.admission.offer(Mutation::new(tag, change));
        self.admit(replies)
    }

    /// Apply every mutation admission lets through, oldest first, and append each
    /// one's reply to `replies`.
    ///
    /// Each is applied before the next is admitted, so the image takes them in
    /// journal order.
    pub fn admit(&mut self, replies: &mut Vec<(u16, Reply)>) {
        while let Some(Admitted { tag, change }) = self.admission.admit() {
            replies.push((tag, self.apply(change)));
        }
    }

    /// Spend this delta's copy budget on the open horizon, interleaving
    /// compaction with the traffic paying for it.
    ///
    /// Each copy holds admission from its selection until it is sent, so no
    /// mutation can land between the read of its run and its record.
    pub fn compact(&mut self) {
        while let Some(copy) = self.admission.next_copy(COPY_BLOCKS) {
            let run = copy.run();
            let mut data = vec![0; run.len() * crate::BLOCK_SIZE as usize];

            // A copy which failed recorded nothing. But no request asked for it,
            // so none can be told, and the horizon cannot complete without the
            // run: every trip around the loop would fail it again.
            if let Err(err) = self.image.read_at(run.start, &mut data) {
                return self.fail_cuts(
                    anyhow::Error::new(err).context("copying a horizon run out of the image"),
                );
            }
            () = copy.send(data.into());
        }
    }

    /// Read `range` out of the image. A read the image refuses fails its request, and
    /// only it: nothing was recorded, so no cut need fail.
    fn read(&self, range: std::ops::Range<u32>) -> Reply {
        let mut buf = vec![0; range.len() * crate::BLOCK_SIZE as usize];

        match self.image.read_at(range.start, &mut buf) {
            Ok(()) => Reply::Data(buf),
            Err(err) => {
                tracing::error!(dev_id = self.dev_id, ?err, "reading the image failed");
                Reply::Failed(libc::EIO)
            }
        }
    }

    /// Apply a mutation which the recording channel has just accepted to the image,
    /// and return its request's reply.
    ///
    /// This is the one place a mutation reaches the image. Because it runs as soon as
    /// the mutation is recorded, two overlapping mutations land in the image in the
    /// order they were recorded, whatever order their requests arrived in.
    fn apply(&mut self, change: Change) -> Reply {
        let (applied, bytes) = match &change {
            Change::Write { start, data } => (self.image.write_at(*start, data), data.len() as u32),
            Change::Punch(range) => (self.image.punch(range.start, range.len() as u32), 0),
        };

        match applied {
            Ok(()) => Reply::Done(bytes),
            // In practice this is the host filesystem out of space. The recording
            // channel holds the mutation already, so the delta now open holds what
            // the image lacks. The request fails, and so does the next cut, which
            // ends the tenure before that delta can commit. A replay drops a delta
            // which never commits, and the image goes with the tenure.
            Err(err) => {
                () = self
                    .fail_cuts(anyhow::Error::new(err).context("applying a mutation to the image"));
                Reply::Failed(libc::EIO)
            }
        }
    }

    /// Fail every later cut of this disk with `err`, and report it if it is the
    /// first.
    fn fail_cuts(&mut self, err: anyhow::Error) {
        if let Some(err) = self.admission.fail_cuts(err) {
            tracing::error!(
                dev_id = self.dev_id,
                ?err,
                "a disk failed, so its next cut will"
            );
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Backend, COPY_BLOCKS};
    use crate::BLOCK_SIZE;
    use crate::device::Command;
    use crate::device::admission::Admission;
    use crate::device::queue::{Reply, Request};
    use crate::horizon::Policy;
    use crate::image::Image;
    use crate::proto::{Chunk, chunk::Content};
    use crate::recording::Recorded;
    use std::fmt::Write as _;

    /// Blocks of the image a trace or property case serves.
    const BLOCKS: u32 = 16;

    /// Any journal range beyond the allocated size opens a horizon, and a delta may
    /// copy as many bytes as it changed.
    const POLICY: Policy = Policy {
        open_ratio: 1.0,
        copy_ratio: 1.0,
        minimum_bytes: 0,
    };

    /// A backend over a recording channel, and the consumer's end of that channel.
    struct Case {
        backend: Backend,
        /// `None` once the consumer has gone away.
        recorded: Option<Recorded>,
        /// Every mutation the consumer took, in the order a journal would hold them.
        taken: Vec<Vec<Chunk>>,
    }

    impl Case {
        fn new(image: Image, capacity: usize, policy: Policy) -> Self {
            let (recorder, recorded) =
                crate::recording::channel(capacity, std::task::Waker::noop().clone());

            Self {
                backend: Backend::new(0, Admission::new(recorder, None, policy), image),
                recorded: Some(recorded),
                taken: Vec::new(),
            }
        }

        /// A case over a fresh image of [`BLOCKS`].
        fn fresh(capacity: usize) -> Self {
            let image = Image::create(&std::env::temp_dir(), BLOCKS).unwrap();
            Self::new(image, capacity, POLICY)
        }

        /// Hand the backend the request at `tag`, and return the replies it finishes.
        fn request(&mut self, tag: u16, request: Request) -> Vec<(u16, Reply)> {
            let mut replies = Vec::new();
            () = self.backend.on_request(tag, request, &mut replies);
            replies
        }

        /// The write of the request at `tag`: `blocks` blocks from `start`, each byte
        /// of which is `fill`.
        fn write(&mut self, tag: u16, start: u32, blocks: u32, fill: u8) -> Vec<(u16, Reply)> {
            let data = vec![fill; (blocks * BLOCK_SIZE) as usize].into();
            self.request(tag, Request::Write { start, data })
        }

        fn punch(&mut self, tag: u16, start: u32, blocks: u32) -> Vec<(u16, Reply)> {
            self.request(tag, Request::Punch(start..start + blocks))
        }

        /// Admit what admission lets through, as each pass of the owner does.
        fn admit(&mut self) -> Vec<(u16, Reply)> {
            let mut replies = Vec::new();
            () = self.backend.admit(&mut replies);
            replies
        }

        /// Have the consumer take one mutation, if one is queued.
        fn take(&mut self) -> Option<Vec<Chunk>> {
            let chunks = self.recorded.as_mut()?.try_recv().ok()?;
            self.taken.push(chunks.clone());
            Some(chunks)
        }

        fn take_all(&mut self) -> Vec<Vec<Chunk>> {
            std::iter::from_fn(|| self.take()).collect()
        }

        fn cut(&mut self) -> anyhow::Result<()> {
            let (closed, mut is_closed) = tokio::sync::oneshot::channel();
            () = self.backend.on_command(Command::CloseAdmission(closed));
            is_closed.try_recv().expect("a cut replies at once")
        }

        fn resume(&mut self) {
            self.backend.on_command(Command::ResumeAdmission)
        }

        /// Cut the disk as a prepare does, and take the delta which ends there,
        /// alongside what its open horizon still owes.
        fn take_delta(&mut self) -> (Vec<Vec<Chunk>>, u32) {
            () = self.cut().unwrap();
            let delta = self.take_all();
            let pending = self.pending();
            () = self.resume();

            (delta, pending)
        }

        fn open_horizon(&mut self, range: u64) -> bool {
            let (reply, mut replied) = tokio::sync::oneshot::channel();
            () = self.backend.on_command(Command::OpenHorizon(range, reply));
            replied
                .try_recv()
                .expect("opening a horizon replies at once")
        }

        fn pending(&mut self) -> u32 {
            let (reply, mut replied) = tokio::sync::oneshot::channel();
            () = self.backend.on_command(Command::HorizonPending(reply));
            replied
                .try_recv()
                .expect("a horizon's pending blocks reply at once")
        }

        fn close_horizon(&mut self) {
            self.backend.on_command(Command::CloseHorizon)
        }
    }

    /// A write-only image, which takes every write and refuses every read.
    fn unreadable_image() -> Image {
        let mut options = std::fs::OpenOptions::new();
        options.write(true);
        std::os::unix::fs::OpenOptionsExt::custom_flags(&mut options, libc::O_TMPFILE);
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);

        let file = options.open(std::env::temp_dir()).unwrap();
        file.set_len((BLOCKS * BLOCK_SIZE) as u64).unwrap();
        Image::from_file(file, BLOCKS)
    }

    /// Render replies readably: the bytes a request transferred, the first byte of a
    /// read's data, or the name of the errno it failed with.
    fn render(replies: &[(u16, Reply)]) -> String {
        if replies.is_empty() {
            return "-".to_string();
        }
        let replies: Vec<String> = replies
            .iter()
            .map(|(tag, reply)| match reply {
                Reply::Done(bytes) => format!("{tag}: {bytes} bytes"),
                Reply::Data(buf) => format!("{tag}: read {}", buf[0]),
                Reply::Failed(libc::EIO) => format!("{tag}: EIO"),
                Reply::Failed(libc::EOPNOTSUPP) => format!("{tag}: EOPNOTSUPP"),
                Reply::Failed(errno) => format!("{tag}: errno {errno}"),
            })
            .collect();
        format!("replied [{}]", replies.join(", "))
    }

    #[derive(Debug, Clone, Copy)]
    enum Step {
        /// The request at `.0` writes the block at `.1`, filled with `.2`.
        Write(u16, u32, u8),
        /// The request at `.0` punches the block at `.1`.
        Punch(u16, u32),
        /// The request at `.0` reads the block at `.1`.
        Read(u16, u32),
        /// The request at `.0` is a flush, which this daemon does not serve.
        Flush(u16),
        /// Admit what admission lets through, as each pass of the owner does.
        Admit,
        /// Have the consumer take one mutation.
        Take,
        DropConsumer,
        Cut,
        Resume,
    }
    use Step::*;

    /// Run `steps` against `case`, and render what each returned, what is parked
    /// after it, and each allocated block of the image with the byte filling it.
    fn trace(case: &mut Case, steps: &[Step]) -> String {
        let mut out = String::new();

        for &step in steps {
            let outcome = match step {
                Write(tag, block, fill) => render(&case.write(tag, block, 1, fill)),
                Punch(tag, block) => render(&case.punch(tag, block, 1)),
                Read(tag, block) => render(&case.request(tag, Request::Read(block..block + 1))),
                Flush(tag) => render(&case.request(tag, Request::Unsupported(2))),
                Admit => render(&case.admit()),
                Take => match case.take() {
                    Some(chunks) => {
                        let kind = match &chunks[0].content {
                            Some(Content::Punch(_)) => "punch",
                            _ => "write",
                        };
                        format!("took {kind} {:?}", crate::chunk::covered_blocks(&chunks[0]))
                    }
                    None => "nothing to take".to_string(),
                },
                DropConsumer => {
                    case.recorded = None;
                    "dropped".to_string()
                }
                Cut => match case.cut() {
                    Ok(()) => "closed".to_string(),
                    Err(err) => format!("refused: {err:#}"),
                },
                Resume => {
                    case.resume();
                    "resumed".to_string()
                }
            };
            let (parked, image) = (
                case.backend.parked(),
                crate::test_support::allocated(&case.backend.image),
            );
            let step = format!("{step:?}");

            writeln!(
                out,
                "{step:<16}{outcome:<36}  parked {parked}  image {image:?}"
            )
            .unwrap();
        }
        out
    }

    #[test]
    fn test_each_request_is_answered_by_its_kind() {
        let mut case = Case::fresh(4);

        let trace = trace(
            &mut case,
            &[
                Write(1, 0, 7),
                Read(2, 0),
                Punch(3, 0),
                // A punched block reads back as zeroes.
                Read(4, 0),
                Flush(5),
            ],
        );
        insta::assert_snapshot!(trace, @"
        Write(1, 0, 7)  replied [1: 4096 bytes]               parked 0  image [(0, 7)]
        Read(2, 0)      replied [2: read 7]                   parked 0  image [(0, 7)]
        Punch(3, 0)     replied [3: 0 bytes]                  parked 0  image []
        Read(4, 0)      replied [4: read 0]                   parked 0  image []
        Flush(5)        replied [5: EOPNOTSUPP]               parked 0  image []
        ");
    }

    #[test]
    fn test_a_full_channel_parks_requests_until_it_drains() {
        let mut case = Case::fresh(2);

        let trace = trace(
            &mut case,
            &[
                Write(1, 0, 1),
                // Block 0 again fills the channel, so the write of block 1 parks.
                Write(2, 0, 2),
                Write(3, 1, 3),
                Read(4, 0),
                Write(5, 0, 5),
                Admit,
                // A parked write has not reached the image, so a read does not see it.
                Read(6, 0),
                Take,
                Admit,
                Take,
                Admit,
                Read(7, 0),
                // A punch frees the block it covers.
                Punch(8, 1),
                Take,
                Admit,
            ],
        );
        insta::assert_snapshot!(trace, @"
        Write(1, 0, 1)  replied [1: 4096 bytes]               parked 0  image [(0, 1)]
        Write(2, 0, 2)  replied [2: 4096 bytes]               parked 0  image [(0, 2)]
        Write(3, 1, 3)  -                                     parked 1  image [(0, 2)]
        Read(4, 0)      replied [4: read 2]                   parked 1  image [(0, 2)]
        Write(5, 0, 5)  -                                     parked 2  image [(0, 2)]
        Admit           -                                     parked 2  image [(0, 2)]
        Read(6, 0)      replied [6: read 2]                   parked 2  image [(0, 2)]
        Take            took write 0..1                       parked 2  image [(0, 2)]
        Admit           replied [3: 4096 bytes]               parked 1  image [(0, 2), (1, 3)]
        Take            took write 0..1                       parked 1  image [(0, 2), (1, 3)]
        Admit           replied [5: 4096 bytes]               parked 0  image [(0, 5), (1, 3)]
        Read(7, 0)      replied [7: read 5]                   parked 0  image [(0, 5), (1, 3)]
        Punch(8, 1)     -                                     parked 1  image [(0, 5), (1, 3)]
        Take            took write 1..2                       parked 1  image [(0, 5), (1, 3)]
        Admit           replied [8: 0 bytes]                  parked 0  image [(0, 5)]
        ");
    }

    #[test]
    fn test_a_dropped_consumer_frees_parked_requests() {
        let mut case = Case::fresh(1);

        let trace = trace(
            &mut case,
            &[
                Write(1, 0, 1),
                Write(2, 1, 2),
                Write(3, 2, 3),
                Admit,
                // A channel without a consumer takes every mutation, and discards it,
                // so the unmount of a disk being torn down completes.
                DropConsumer,
                Admit,
            ],
        );
        insta::assert_snapshot!(trace, @"
        Write(1, 0, 1)  replied [1: 4096 bytes]               parked 0  image [(0, 1)]
        Write(2, 1, 2)  -                                     parked 1  image [(0, 1)]
        Write(3, 2, 3)  -                                     parked 2  image [(0, 1)]
        Admit           -                                     parked 2  image [(0, 1)]
        DropConsumer    dropped                               parked 2  image [(0, 1)]
        Admit           replied [2: 4096 bytes, 3: 4096 bytes]  parked 0  image [(0, 1), (1, 2), (2, 3)]
        ");
    }

    /// An image write the host refuses fails its own request, and then every cut of
    /// the disk which follows. Its mutation reached the recording channel before the
    /// image refused it, so the delta holding it must never commit.
    #[test]
    fn test_a_failed_image_write_fails_every_later_cut() {
        let full = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/full")
            .unwrap();
        let mut case = Case::new(Image::from_file(full, BLOCKS), 4, POLICY);

        let trace = trace(
            &mut case,
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
        );
        insta::assert_snapshot!(trace, @"
        Write(1, 0, 1)  replied [1: EIO]                      parked 0  image []
        Take            took write 0..1                       parked 0  image []
        Cut             refused: device 0 failed: applying a mutation to the image: No space left on device (os error 28)  parked 0  image []
        Write(2, 0, 2)  replied [2: EIO]                      parked 0  image []
        Resume          resumed                               parked 0  image []
        Cut             refused: device 0 failed: applying a mutation to the image: No space left on device (os error 28)  parked 0  image []
        ");
    }

    /// A read the image refuses fails its own request, and nothing else. It recorded
    /// nothing, so the delta is whole and its cut goes ahead.
    #[test]
    fn test_a_failed_image_read_fails_only_its_request() {
        let mut case = Case::new(unreadable_image(), 4, POLICY);

        assert_eq!(case.write(1, 0, 1, 1), [(1, Reply::Done(BLOCK_SIZE))]);
        assert_eq!(
            case.request(2, Request::Read(0..1)),
            [(2, Reply::Failed(libc::EIO))]
        );
        () = case.cut().unwrap();
    }

    /// A horizon run which cannot be read out of the image records nothing, and no
    /// request asked for it. It fails every later cut instead.
    #[test]
    fn test_an_unreadable_horizon_run_fails_every_later_cut() {
        let mut case = Case::new(unreadable_image(), 4, POLICY);

        assert_eq!(case.write(1, 0, 1, 1), [(1, Reply::Done(BLOCK_SIZE))]);
        assert!(case.open_horizon(u64::MAX), "no horizon opened");

        // A write elsewhere earns the budget of one copy.
        assert_eq!(case.write(2, 5, 1, 2), [(2, Reply::Done(BLOCK_SIZE))]);
        () = case.backend.compact();

        let err = format!("{:#}", case.cut().unwrap_err());
        insta::assert_snapshot!(err, @"device 0 failed: copying a horizon run out of the image: Bad file descriptor (os error 9)");
    }

    /// Open a horizon over a disk's cold blocks, discharge it with the budget a hot
    /// region's rewrites earn, and hold the invariant the whole scheme rests on: the
    /// mutations from the horizon onward rebuild the entire disk by themselves.
    ///
    /// What a delta changed rations what it copies, and a block the disk rewrites
    /// costs no copy at all.
    #[test]
    fn test_a_horizon_discharges_and_bounds_recovery() {
        /// Blocks every delta rewrites, earning the copy budget. One run of them is
        /// also the largest request the device accepts, and the largest copy.
        const HOT_BLOCKS: u32 = COPY_BLOCKS;
        /// Blocks written once, which only a horizon copy publishes again.
        const COLD_BLOCKS: u32 = 8 * HOT_BLOCKS;
        /// Generous against the fifteen a discharge of this disk needs.
        const DELTAS: usize = 40;

        let policy = Policy {
            open_ratio: 2.0,
            copy_ratio: 0.5,
            minimum_bytes: 1 << 20,
        };
        let image = Image::create(&std::env::temp_dir(), COLD_BLOCKS).unwrap();
        let mut case = Case::new(image, crate::ublk::QUEUE_DEPTH as usize, policy);
        let hot = COLD_BLOCKS - HOT_BLOCKS;

        let fills = COLD_BLOCKS / HOT_BLOCKS;
        let answered: usize = (0..fills)
            .map(|run| {
                case.write(run as u16, run * HOT_BLOCKS, HOT_BLOCKS, 0x10 + run as u8)
                    .len()
            })
            .sum();
        assert_eq!(answered, fills as usize, "a fill parked");
        let (filled, _pending) = case.take_delta();

        // A range within the minimum opens nothing, whatever the disk holds. A range
        // beyond it opens a horizon over every allocated block, which the first
        // delta below checks. The writes which allocated those blocks are before the
        // horizon, so none of them is in the replay compared below.
        assert!(
            !case.open_horizon(policy.minimum_bytes),
            "a range within the minimum opened a horizon"
        );
        assert!(
            case.open_horizon(1 << 30),
            "a range beyond the minimum opened no horizon"
        );
        assert_eq!(filled.len(), fills as usize, "a fill was lost");

        // A replay which begins at the horizon reads every mutation from here on, and
        // nothing else.
        let mut after_horizon: Vec<Vec<Chunk>> = Vec::new();
        let mut copied_total = 0;

        for delta in 0..DELTAS {
            // The first delta writes nothing, as on a disk no connector is using. An
            // open horizon must then publish nothing at all.
            if delta != 0 {
                let answered = case.write(0, hot, HOT_BLOCKS, 0xa0 + delta as u8);
                assert_eq!(answered.len(), 1, "delta {delta}'s write parked");
            }
            () = case.backend.compact();
            let (mutations, pending) = case.take_delta();
            let (mut changed, mut copied) = (0, 0);

            for chunk in mutations.iter().flatten() {
                let bytes = crate::chunk::data_bytes(std::slice::from_ref(chunk));

                match chunk.block < hot {
                    true => copied += bytes,
                    false => changed += bytes,
                }
            }

            // Write amplification per delta is at most one plus the copy ratio, which
            // is a half here.
            assert!(
                2 * copied <= changed,
                "delta {delta} copied {copied} for {changed} changed"
            );

            if delta == 0 {
                assert_eq!(changed, 0, "the first delta wrote to the disk");
                assert_eq!(copied, 0, "an open horizon published without a budget");
                assert_eq!(pending, COLD_BLOCKS, "the horizon began part-discharged");
            }

            after_horizon.extend(mutations);
            copied_total += copied;

            if pending == 0 {
                break;
            }
        }

        // The horizon is discharged, and the hot blocks cost it nothing: the disk's
        // own rewrites of those blocks discharged them.
        assert_eq!(
            case.pending(),
            0,
            "the horizon never discharged in {DELTAS} deltas"
        );
        assert_eq!(
            copied_total,
            ((COLD_BLOCKS - HOT_BLOCKS) * BLOCK_SIZE) as u64,
            "the copies did not cover the cold blocks exactly",
        );
        () = crate::test_support::assert_replays_identically(&case.backend.image, &after_horizon);
    }

    #[derive(Clone, Debug)]
    enum Op {
        Write { start: u32, blocks: u32, fill: u8 },
        Punch { start: u32, blocks: u32 },
        Admit,
        Take,
        Cut,
        Resume,
        OpenHorizon,
        CloseHorizon,
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
                9 => Op::Cut,
                10 => Op::Resume,
                11 => Op::OpenHorizon,
                12 => Op::CloseHorizon,
                _ => Op::Compact,
            }
        }
    }

    /// Run `ops` against a channel of `capacity`, then drain it, compacting as the
    /// owner does while the consumer takes. Every request must be answered once, in
    /// the order it arrived, and the stream the consumer took must replay to the
    /// image served.
    fn answers_in_order_and_replays_identically(capacity: u8, ops: Vec<Op>) -> bool {
        let mut case = Case::fresh(1 + capacity as usize % 3);
        let (mut expected, mut answered) = (Vec::new(), Vec::new());
        let mut horizon = false;

        for op in ops {
            let tag = expected.len() as u16;

            match op {
                Op::Write {
                    start,
                    blocks,
                    fill,
                } => {
                    expected.push((tag, Reply::Done(blocks * BLOCK_SIZE)));
                    answered.extend(case.write(tag, start, blocks, fill));
                }
                Op::Punch { start, blocks } => {
                    expected.push((tag, Reply::Done(0)));
                    answered.extend(case.punch(tag, start, blocks));
                }
                Op::Admit => answered.extend(case.admit()),
                Op::Take => _ = case.take(),
                Op::Cut => () = case.cut().unwrap(),
                Op::Resume => case.resume(),
                Op::OpenHorizon if !horizon => horizon = case.open_horizon(u64::MAX),
                Op::OpenHorizon => (),
                Op::CloseHorizon => {
                    case.close_horizon();
                    horizon = false;
                }
                Op::Compact => case.backend.compact(),
            }
        }

        case.resume();
        while case.backend.parked() != 0 {
            _ = case.take_all();
            answered.extend(case.admit());
        }
        // Whatever budget the open delta has left goes to copies, each of which must
        // carry the image's content to the replay.
        loop {
            () = case.backend.compact();

            if case.take_all().is_empty() {
                break;
            }
        }

        assert_eq!(
            answered, expected,
            "requests were answered out of order, or not at all"
        );
        () = crate::test_support::assert_replays_identically(&case.backend.image, &case.taken);
        true
    }

    #[test]
    fn test_any_interleaving_answers_in_order_and_replays_identically() {
        quickcheck::QuickCheck::new()
            .tests(64)
            .quickcheck(answers_in_order_and_replays_identically as fn(u8, Vec<Op>) -> bool);
    }
}
