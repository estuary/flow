use super::admission;
use super::queue;
use crate::image;
use crate::ublk;

/// Most blocks one horizon copy reads, so a copy is a mutation of the same order
/// as the largest device request.
const COPY_BLOCKS: u32 = ublk::MAX_IO_BUF_BYTES / crate::BLOCK_SIZE;

/// One disk's image, and the admission which decides when a mutation reaches it.
pub(super) struct Backend {
    dev_id: u32,
    admission: admission::Admission,
    image: image::Image,
}

impl Backend {
    pub fn new(dev_id: u32, admission: admission::Admission, image: image::Image) -> Self {
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

    pub fn on_command(&mut self, command: super::Command) {
        match command {
            super::Command::CloseAdmission(closed) => {
                let cut = self
                    .admission
                    .close()
                    .map_err(|failed| anyhow::anyhow!("device {} failed: {failed:#}", self.dev_id));

                _ = closed.send(cut);
            }
            super::Command::ResumeAdmission => self.admission.resume(),
            super::Command::OpenHorizon(range, reply) => {
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
            super::Command::HorizonPending(reply) => {
                _ = reply.send(self.admission.horizon_pending())
            }
            super::Command::CloseHorizon => self.admission.close_horizon(),
        }
    }

    /// Handle a request and append any completed replies. Reads and unsupported
    /// requests finish immediately. Mutations wait behind earlier mutations;
    /// admission may complete several requests, including this one.
    pub fn on_request(
        &mut self,
        tag: u16,
        request: queue::Request,
        replies: &mut Vec<(u16, queue::Reply)>,
    ) {
        let change = match request {
            queue::Request::Read(range) => return replies.push((tag, self.read(range))),
            queue::Request::Write { start, data } => admission::Change::Write { start, data },
            queue::Request::Punch(range) => admission::Change::Punch(range),
            queue::Request::Unsupported(op) => {
                tracing::warn!(dev_id = self.dev_id, op, "unsupported device request");
                return replies.push((tag, queue::Reply::Failed(libc::EOPNOTSUPP)));
            }
        };
        () = self.admission.offer(admission::Mutation::new(tag, change));
        self.admit(replies)
    }

    /// Apply every mutation admission lets through, oldest first, and append each
    /// one's reply to `replies`.
    ///
    /// Each is applied before the next is admitted, so the image takes them in
    /// journal order.
    pub fn admit(&mut self, replies: &mut Vec<(u16, queue::Reply)>) {
        while let Some(admission::Admitted { tag, change }) = self.admission.admit() {
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

            // No request can report this failure, and retrying the same run would
            // fail on every pass. Fail the next cut instead.
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

    /// Apply a newly recorded mutation before admitting another, preserving
    /// journal order even for overlapping writes.
    fn apply(&mut self, change: admission::Change) -> queue::Reply {
        let (applied, bytes) = match &change {
            admission::Change::Write { start, data } => {
                (self.image.write_at(*start, data), data.len() as u32)
            }
            admission::Change::Punch(range) => {
                (self.image.punch(range.start, range.len() as u32), 0)
            }
        };

        match applied {
            Ok(()) => queue::Reply::Done(bytes),
            // The mutation is recorded but failed to reach the image, so this
            // delta must never commit. Failing the next cut ends the tenure;
            // replay then discards the delta.
            Err(err) => {
                () = self
                    .fail_cuts(anyhow::Error::new(err).context("applying a mutation to the image"));
                queue::Reply::Failed(libc::EIO)
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
    use crate::device;
    use crate::device::admission;
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

    /// A backend over a recording channel, and the consumer's end of that channel.
    struct Case {
        backend: Backend,
        /// `None` once the consumer has gone away.
        recorded: Option<device::Recorded>,
        /// Every mutation the consumer took, in the order a journal would hold them.
        taken: Vec<Vec<proto::Chunk>>,
    }

    impl Case {
        fn new(image: image::Image, capacity: usize, policy: horizon::Policy) -> Self {
            let (recorder, recorded) =
                crate::device::recording::channel(capacity, std::task::Waker::noop().clone());

            Self {
                backend: Backend::new(0, admission::Admission::new(recorder, None, policy), image),
                recorded: Some(recorded),
                taken: Vec::new(),
            }
        }

        fn fresh(capacity: usize) -> Self {
            let image = image::Image::create(&std::env::temp_dir(), BLOCKS).unwrap();
            Self::new(image, capacity, POLICY)
        }

        fn request(&mut self, tag: u16, request: queue::Request) -> Vec<(u16, queue::Reply)> {
            let mut replies = Vec::new();
            () = self.backend.on_request(tag, request, &mut replies);
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
            () = self.backend.admit(&mut replies);
            replies
        }

        /// Have the consumer take one mutation, if one is queued.
        fn take(&mut self) -> Option<Vec<proto::Chunk>> {
            let chunks = self.recorded.as_mut()?.try_recv().ok()?;
            self.taken.push(chunks.clone());
            Some(chunks)
        }

        fn take_all(&mut self) -> Vec<Vec<proto::Chunk>> {
            std::iter::from_fn(|| self.take()).collect()
        }

        fn cut(&mut self) -> anyhow::Result<()> {
            let (closed, mut is_closed) = tokio::sync::oneshot::channel();
            () = self
                .backend
                .on_command(device::Command::CloseAdmission(closed));
            is_closed.try_recv().expect("a cut replies at once")
        }

        fn resume(&mut self) {
            self.backend.on_command(device::Command::ResumeAdmission)
        }

        /// Cut the disk as a prepare does, and take the delta which ends there,
        /// alongside what its open horizon still owes.
        fn take_delta(&mut self) -> (Vec<Vec<proto::Chunk>>, u32) {
            () = self.cut().unwrap();
            let delta = self.take_all();
            let pending = self.pending();
            () = self.resume();

            (delta, pending)
        }

        fn open_horizon(&mut self, range: u64) -> bool {
            let (reply, mut replied) = tokio::sync::oneshot::channel();
            () = self
                .backend
                .on_command(device::Command::OpenHorizon(range, reply));
            replied
                .try_recv()
                .expect("opening a horizon replies at once")
        }

        fn pending(&mut self) -> u32 {
            let (reply, mut replied) = tokio::sync::oneshot::channel();
            () = self
                .backend
                .on_command(device::Command::HorizonPending(reply));
            replied
                .try_recv()
                .expect("a horizon's pending blocks reply at once")
        }

        fn close_horizon(&mut self) {
            self.backend.on_command(device::Command::CloseHorizon)
        }
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
                queue::Reply::Failed(libc::EOPNOTSUPP) => format!("{tag}: EOPNOTSUPP"),
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
        /// The request at `.0` punches the block at `.1`.
        Punch(u16, u32),
        /// The request at `.0` reads the block at `.1`.
        Read(u16, u32),
        /// The request at `.0` is a flush, which this daemon does not serve.
        Flush(u16),
        /// Admit what admission lets through, as each pass of the owner does.
        Admit,
        Take,
        DropConsumer,
        Cut,
        Resume,
        /// Use a journal range large enough to open a horizon.
        OpenHorizon,
        /// Spend the open delta's copy budget, as each pass of the owner does.
        Compact,
    }
    use Step::*;

    /// Trace replies, parked mutations, image allocation, and pending horizon blocks.
    fn trace(case: &mut Case, steps: &[Step]) -> String {
        let mut out = String::new();

        for &step in steps {
            let outcome = match step {
                Write(tag, block, fill) => render(&case.write(tag, block, 1, fill)),
                Punch(tag, block) => render(&case.punch(tag, block, 1)),
                Read(tag, block) => {
                    render(&case.request(tag, queue::Request::Read(block..block + 1)))
                }
                Flush(tag) => render(&case.request(tag, queue::Request::Unsupported(2))),
                Admit => render(&case.admit()),
                Take => match case.take() {
                    Some(chunks) => {
                        let kind = match &chunks[0].content {
                            Some(proto::chunk::Content::Punch(_)) => "punch",
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
                OpenHorizon => match case.open_horizon(u64::MAX) {
                    true => format!("opened, pending {}", case.pending()),
                    false => "refused".to_string(),
                },
                Compact => {
                    () = case.backend.compact();
                    format!("pending {}", case.pending())
                }
            };
            let (parked, image) = (case.backend.parked(), allocated(&case.backend.image));
            let step = format!("{step:?}");

            writeln!(
                out,
                "{step:<16}{outcome:<36}  parked {parked}  image {image}"
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
        let mut case = Case::new(image::Image::from_file(full, BLOCKS), 4, POLICY);

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

        let trace = trace(&mut case, &[Write(1, 0, 1), Read(2, 0), Cut]);
        insta::assert_snapshot!(trace, @"
        Write(1, 0, 1)  replied [1: 4096 bytes]               parked 0  image [(0, ?)]
        Read(2, 0)      replied [2: EIO]                      parked 0  image [(0, ?)]
        Cut             closed                                parked 0  image [(0, ?)]
        ");
    }

    /// A horizon run which cannot be read out of the image records nothing, and no
    /// request asked for it. It fails every later cut instead.
    #[test]
    fn test_an_unreadable_horizon_run_fails_every_later_cut() {
        let mut case = Case::new(unreadable_image(), 4, POLICY);

        let trace = trace(
            &mut case,
            &[
                Write(1, 0, 1),
                OpenHorizon,
                // A write elsewhere earns the budget of one copy, of block 0.
                Write(2, 5, 2),
                Compact,
                Cut,
            ],
        );
        insta::assert_snapshot!(trace, @"
        Write(1, 0, 1)  replied [1: 4096 bytes]               parked 0  image [(0, ?)]
        OpenHorizon     opened, pending 1                     parked 0  image [(0, ?)]
        Write(2, 5, 2)  replied [2: 4096 bytes]               parked 0  image [(0, ?), (5, ?)]
        Compact         pending 1                             parked 0  image [(0, ?), (5, ?)]
        Cut             refused: device 0 failed: copying a horizon run out of the image: Bad file descriptor (os error 9)  parked 0  image [(0, ?), (5, ?)]
        ");
    }

    /// Verify that mutations from a completed horizon rebuild the entire disk.
    /// Hot rewrites fund copies of cold blocks and need no copies themselves.
    #[test]
    fn test_a_horizon_discharges_and_bounds_recovery() {
        /// Blocks every delta rewrites, earning the copy budget. One run of them is
        /// also the largest request the device accepts, and the largest copy.
        const HOT_BLOCKS: u32 = COPY_BLOCKS;
        /// Blocks written once, which only a horizon copy publishes again.
        const COLD_BLOCKS: u32 = 8 * HOT_BLOCKS;
        /// Generous against the fifteen a discharge of this disk needs.
        const DELTAS: usize = 40;

        let policy = horizon::Policy {
            open_ratio: 2.0,
            copy_ratio: 0.5,
            minimum_bytes: 1 << 20,
        };
        let image = image::Image::create(&std::env::temp_dir(), COLD_BLOCKS).unwrap();
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

        // The replay below excludes these initial writes, so only mutations
        // at or after the horizon can rebuild their blocks.
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
        let mut after_horizon: Vec<Vec<proto::Chunk>> = Vec::new();
        let mut copied_total = 0;
        let mut trace = String::new();

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

            let label = format!("delta {delta}");
            writeln!(
                trace,
                "{label:<10}changed {changed:<8}copied {copied:<8}pending {pending}"
            )
            .unwrap();

            after_horizon.extend(mutations);
            copied_total += copied;

            if pending == 0 {
                break;
            }
        }
        writeln!(
            trace,
            "{:<26}copied {copied_total:<8}pending {}",
            "total",
            case.pending()
        )
        .unwrap();

        // After the idle delta, rewriting 32 hot blocks pays to copy 16 cold ones.
        // Fourteen such deltas copy all 224 cold blocks; writes discharge the hot
        // blocks without copying them.
        insta::assert_snapshot!(trace, @"
        delta 0   changed 0       copied 0       pending 256
        delta 1   changed 131072  copied 65536   pending 208
        delta 2   changed 131072  copied 65536   pending 192
        delta 3   changed 131072  copied 65536   pending 176
        delta 4   changed 131072  copied 65536   pending 160
        delta 5   changed 131072  copied 65536   pending 144
        delta 6   changed 131072  copied 65536   pending 128
        delta 7   changed 131072  copied 65536   pending 112
        delta 8   changed 131072  copied 65536   pending 96
        delta 9   changed 131072  copied 65536   pending 80
        delta 10  changed 131072  copied 65536   pending 64
        delta 11  changed 131072  copied 65536   pending 48
        delta 12  changed 131072  copied 65536   pending 32
        delta 13  changed 131072  copied 65536   pending 16
        delta 14  changed 131072  copied 65536   pending 0
        total                     copied 917504  pending 0
        ");
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
                    expected.push((tag, queue::Reply::Done(blocks * BLOCK_SIZE)));
                    answered.extend(case.write(tag, start, blocks, fill));
                }
                Op::Punch { start, blocks } => {
                    expected.push((tag, queue::Reply::Done(0)));
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
