//! The sparse local image which backs one disk.
//!
//! Byte N of the image is byte N of the block device. The image is created with
//! `O_TMPFILE`, so it has no directory entry and cannot outlive the daemon.
//! `ftruncate` then gives it the device's logical size, and leaves every block a
//! hole. The image is disposable. The journal is the disk.

use crate::bitmap::Bitmap;
use crate::horizon::{Horizon, Policy};

/// An image and the bitmaps which track it.
///
/// Only the disk's owner mutates this, so nothing here is synchronized. On the
/// serving path the owner submits image I/O to its ring rather than through this
/// type. It then records the effect with [`Image::allocate`] or
/// [`Image::deallocate`].
pub struct Image {
    file: std::fs::File,
    block_size: u32,
    allocated: Bitmap,
    horizon: Option<Horizon>,
}

impl Image {
    /// Create a `blocks` × `block_size` image within `dir`.
    pub fn create(dir: &std::path::Path, blocks: u32, block_size: u32) -> std::io::Result<Self> {
        assert!(
            block_size != 0 && block_size.is_power_of_two(),
            "block size {block_size} must be a power of two",
        );
        assert!(blocks != 0, "a device has at least one block");

        let mut options = std::fs::OpenOptions::new();
        options.read(true).write(true);
        std::os::unix::fs::OpenOptionsExt::custom_flags(&mut options, libc::O_TMPFILE);
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);

        let file = options.open(dir)?;
        file.set_len(blocks as u64 * block_size as u64)?;

        Ok(Self {
            file,
            block_size,
            allocated: Bitmap::new(blocks),
            horizon: None,
        })
    }

    pub fn file(&self) -> &std::fs::File {
        &self.file
    }

    pub fn block_size(&self) -> u32 {
        self.block_size
    }

    pub fn blocks(&self) -> u32 {
        self.allocated.blocks()
    }

    /// Byte offset at which `block` begins.
    pub fn offset(&self, block: u32) -> u64 {
        block as u64 * self.block_size as u64
    }

    pub fn allocated(&self) -> &Bitmap {
        &self.allocated
    }

    /// Open a recovery horizon over the blocks allocated now, replacing any
    /// horizon which was open, and report what it must discharge.
    ///
    /// A horizon's bitmap is as large as the allocated bitmap, so it is held
    /// only while a horizon is open.
    pub fn open_horizon(&mut self) -> u32 {
        let horizon = Horizon::open(&self.allocated);
        let pending = horizon.pending();
        self.horizon = Some(horizon);

        pending
    }

    pub fn horizon(&mut self) -> Option<&mut Horizon> {
        self.horizon.as_mut()
    }

    /// Blocks which still owe the open horizon a copy, and zero when none is
    /// open.
    pub fn horizon_pending(&self) -> u32 {
        self.horizon.as_ref().map_or(0, Horizon::pending)
    }

    pub fn close_horizon(&mut self) {
        self.horizon = None;
    }

    pub fn read_at(&self, block: u32, buf: &mut [u8]) -> std::io::Result<()> {
        std::os::unix::fs::FileExt::read_exact_at(&self.file, buf, self.offset(block))
    }

    /// Write whole blocks. A partial block would leave the bitmap describing
    /// less than the image holds.
    pub fn write_at(&mut self, block: u32, data: &[u8]) -> std::io::Result<()> {
        let blocks = data.len() / self.block_size as usize;
        assert_eq!(
            data.len() % self.block_size as usize,
            0,
            "an image write is a whole number of {}-byte blocks",
            self.block_size,
        );

        std::os::unix::fs::FileExt::write_all_at(&self.file, data, self.offset(block))?;
        self.allocate(block..block + blocks as u32);
        Ok(())
    }

    pub fn punch(&mut self, block: u32, blocks: u32) -> std::io::Result<()> {
        punch_hole(
            &self.file,
            self.offset(block),
            blocks as u64 * self.block_size as u64,
        )?;
        self.deallocate(block..block + blocks);
        Ok(())
    }

    /// Record that `range` now occupies space in the image.
    pub fn allocate(&mut self, range: std::ops::Range<u32>) {
        for block in range {
            self.allocated.set(block);
        }
    }

    /// Record that `range` no longer occupies space in the image.
    pub fn deallocate(&mut self, range: std::ops::Range<u32>) {
        for block in range {
            self.allocated.clear(block);
        }
    }

    /// Apply a journal chunk. This is how replay rebuilds an image.
    ///
    /// A horizon opens at a record, and a replay is a forward pass. Every chunk
    /// a replay reads is therefore at or after any horizon it has opened.
    /// Applying a chunk also discharges the blocks it covers.
    pub fn apply(&mut self, chunk: &crate::proto::Chunk) -> std::io::Result<()> {
        crate::chunk::apply(chunk, self.block_size, &self.file, &mut self.allocated)?;

        if let Some(horizon) = &mut self.horizon {
            horizon.published(crate::chunk::covered_blocks(chunk, self.block_size));
        }
        Ok(())
    }

    /// Read the next run of horizon blocks back as the chunks which publish and
    /// so discharge them, or `None` when the delta's copy budget is spent or no
    /// horizon is open.
    ///
    /// A run is at most `run_blocks` long, so one copy is one mutation of the
    /// same order as a device request.
    pub fn copy_horizon(
        &mut self,
        policy: &Policy,
        run_blocks: u32,
    ) -> std::io::Result<Option<Vec<crate::proto::Chunk>>> {
        let block_size = self.block_size;

        let Some(horizon) = &mut self.horizon else {
            return Ok(None);
        };
        let Some(run) = horizon.next_copy(policy, run_blocks, block_size) else {
            return Ok(None);
        };
        let mut data = vec![0u8; run.len() * block_size as usize];

        () = std::os::unix::fs::FileExt::read_exact_at(
            &self.file,
            &mut data,
            run.start as u64 * block_size as u64,
        )?;
        let chunks = crate::chunk::encode_write(run.start, &data.into(), block_size);

        horizon.copied(run, crate::chunk::data_bytes(&chunks));
        Ok(Some(chunks))
    }

    /// Discard everything the image holds, leaving it as it was created.
    pub fn reset(&mut self) -> std::io::Result<()> {
        punch_hole(&self.file, 0, self.blocks() as u64 * self.block_size as u64)?;
        self.allocated = Bitmap::new(self.allocated.blocks());
        self.horizon = None;

        Ok(())
    }

    /// Read the image back as the chunks which reproduce it. The read begins at
    /// block `from` and stops once `max_bytes` are read. Each run of at most
    /// `run_blocks` contiguous allocated blocks becomes one mutation. This also
    /// reports the block to resume at, or `None` where the image is exhausted.
    ///
    /// A fresh disk publishes this ahead of its first delta. Its formatted
    /// filesystem is content the journal has never seen, and the image already
    /// holds exactly that content. Unallocated blocks are not read, so the holes
    /// a prezeroed format left are holes in a rebuilt image too.
    ///
    /// A caller takes this in batches, because the image is as large as the
    /// device. A caller which appends each batch never holds more than one.
    pub fn snapshot(
        &self,
        from: u32,
        run_blocks: u32,
        max_bytes: usize,
    ) -> std::io::Result<(Vec<Vec<crate::proto::Chunk>>, Option<u32>)> {
        assert!(run_blocks != 0, "a snapshot run covers at least one block");
        let (mut runs, mut cursor, mut read) = (Vec::new(), from, 0);

        while let Some(start) = self.allocated.first_set_at_or_after(cursor) {
            if read >= max_bytes {
                return Ok((runs, Some(start)));
            }
            let limit = std::cmp::min(self.blocks(), start.saturating_add(run_blocks));
            let mut end = start + 1;

            while end < limit && self.allocated.test(end) {
                end += 1;
            }
            let mut data = vec![0u8; (end - start) as usize * self.block_size as usize];
            () = self.read_at(start, &mut data)?;
            read += data.len();

            runs.push(crate::chunk::encode_write(
                start,
                &data.into(),
                self.block_size,
            ));
            cursor = end;
        }
        Ok((runs, None))
    }
}

/// Deallocate `[offset, offset+len)` of `file`, leaving a hole which reads as
/// zeroes. `FALLOC_FL_KEEP_SIZE` keeps the image's logical size, which is the
/// device's fixed capacity.
#[cfg(target_os = "linux")]
pub(crate) fn punch_hole(file: &std::fs::File, offset: u64, len: u64) -> std::io::Result<()> {
    // SAFETY: `file` keeps the descriptor open across the call, and fallocate
    // reads no user memory.
    let rc = unsafe {
        libc::fallocate(
            std::os::fd::AsRawFd::as_raw_fd(file),
            PUNCH_MODE,
            offset as libc::off_t,
            len as libc::off_t,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn punch_hole(_file: &std::fs::File, _offset: u64, _len: u64) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "hole punching requires Linux fallocate()",
    ))
}

/// `fallocate` mode which punches a hole. The owner's ring submissions use it
/// too, so both paths deallocate identically.
#[cfg(target_os = "linux")]
pub(crate) const PUNCH_MODE: i32 = libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE;

#[cfg(test)]
mod test {
    use super::Image;

    const BLOCK_SIZE: u32 = 4096;
    const BLOCKS: u32 = 64;

    fn image(dir: &tempfile::TempDir) -> Image {
        Image::create(dir.path(), BLOCKS, BLOCK_SIZE).unwrap()
    }

    #[test]
    fn test_fresh_image_is_all_holes() {
        let dir = tempfile::tempdir().unwrap();
        let image = image(&dir);

        assert_eq!(image.blocks(), BLOCKS);
        assert_eq!(image.allocated().count_ones(), 0);
        assert_eq!(
            image.file().metadata().unwrap().len(),
            BLOCKS as u64 * BLOCK_SIZE as u64
        );
        assert_eq!(
            std::os::unix::fs::MetadataExt::blocks(&image.file().metadata().unwrap()),
            0
        );

        // The image has no directory entry, so the directory it was created in
        // stays empty.
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);

        let mut buf = vec![0xff; BLOCK_SIZE as usize];
        image.read_at(BLOCKS - 1, &mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_write_read_and_punch() {
        let dir = tempfile::tempdir().unwrap();
        let mut image = image(&dir);

        let data = vec![0xab; 3 * BLOCK_SIZE as usize];
        image.write_at(5, &data).unwrap();
        assert_eq!(image.allocated().iter().collect::<Vec<_>>(), vec![5, 6, 7]);

        let mut buf = vec![0; 3 * BLOCK_SIZE as usize];
        image.read_at(5, &mut buf).unwrap();
        assert_eq!(buf, data);

        // A punch of the middle block clears only its bit. That block reads back
        // as zeroes, and its neighbours are untouched.
        image.punch(6, 1).unwrap();
        assert_eq!(image.allocated().iter().collect::<Vec<_>>(), vec![5, 7]);

        image.read_at(5, &mut buf).unwrap();
        assert!(buf[..BLOCK_SIZE as usize].iter().all(|&b| b == 0xab));
        assert!(
            buf[BLOCK_SIZE as usize..2 * BLOCK_SIZE as usize]
                .iter()
                .all(|&b| b == 0)
        );
        assert!(buf[2 * BLOCK_SIZE as usize..].iter().all(|&b| b == 0xab));
    }

    #[test]
    fn test_punching_an_unallocated_range_is_a_no_op() {
        let dir = tempfile::tempdir().unwrap();
        let mut image = image(&dir);

        image.write_at(0, &vec![1; BLOCK_SIZE as usize]).unwrap();
        image.punch(10, 20).unwrap();

        assert_eq!(image.allocated().iter().collect::<Vec<_>>(), vec![0]);
    }

    /// A horizon opens over what the image holds. Its copies then read those
    /// blocks back until every one is discharged.
    #[test]
    fn test_a_horizon_copies_the_image_back() {
        let dir = tempfile::tempdir().unwrap();
        let mut image = image(&dir);

        let policy = crate::horizon::Policy {
            open_ratio: 2.0,
            copy_ratio: 1.0,
            minimum_bytes: 0,
        };
        assert_eq!(image.horizon_pending(), 0);
        assert!(image.copy_horizon(&policy, 4).unwrap().is_none());

        image
            .write_at(2, &vec![0xab; 3 * BLOCK_SIZE as usize])
            .unwrap();
        image.write_at(30, &vec![0; BLOCK_SIZE as usize]).unwrap();

        assert_eq!(image.open_horizon(), 4);

        // What the delta changed rations its copies. A device write discharges
        // its own blocks without a copy.
        assert!(image.copy_horizon(&policy, 4).unwrap().is_none());
        image.horizon().unwrap().changed(8 * BLOCK_SIZE as u64);
        image.horizon().unwrap().published(2..3);

        let mut copied = Vec::new();
        while let Some(chunks) = image.copy_horizon(&policy, 2).unwrap() {
            copied.extend(chunks);
        }
        assert_eq!(image.horizon_pending(), 0);

        // The zeroed block copies as an empty-data chunk, so a replay keeps it
        // allocated without carrying its bytes.
        let mut replayed = Image::create(dir.path(), BLOCKS, BLOCK_SIZE).unwrap();
        for chunk in &copied {
            () = replayed.apply(chunk).unwrap();
        }
        assert_eq!(
            replayed.allocated().iter().collect::<Vec<_>>(),
            vec![3, 4, 30]
        );

        let mut buf = vec![0; BLOCK_SIZE as usize];
        replayed.read_at(3, &mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 0xab));

        image.close_horizon();
        assert!(image.copy_horizon(&policy, 4).unwrap().is_none());
    }

    /// A snapshot of an image, replayed into another, reproduces it in bytes and
    /// in allocation. A fresh disk can therefore publish its filesystem without
    /// having kept the writes which made it.
    #[test]
    fn test_a_snapshot_replays_into_an_identical_image() {
        let dir = tempfile::tempdir().unwrap();
        let mut image = image(&dir);

        // This covers runs which cross the split, an isolated block, an all-zero
        // block which is still allocated, and a hole punched through a written
        // run.
        image
            .write_at(0, &vec![0xaa; 20 * BLOCK_SIZE as usize])
            .unwrap();
        image.punch(5, 2).unwrap();
        image.write_at(30, &vec![0; BLOCK_SIZE as usize]).unwrap();
        image
            .write_at(63, &vec![0xbb; BLOCK_SIZE as usize])
            .unwrap();

        // The run is short enough to split the first written range across
        // several mutations. The batch is short enough that those mutations do
        // not all fit in one.
        let mut batches = Vec::new();
        let mut from = Some(0);

        while let Some(cursor) = from {
            let (runs, next) = image.snapshot(cursor, 8, 16 * BLOCK_SIZE as usize).unwrap();
            batches.push(runs);
            from = next;
        }
        assert_eq!(batches.iter().map(Vec::len).collect::<Vec<_>>(), vec![3, 2]);

        let mut replayed = Image::create(dir.path(), BLOCKS, BLOCK_SIZE).unwrap();
        for chunk in batches.iter().flatten().flatten() {
            () = replayed.apply(chunk).unwrap();
        }

        assert_eq!(
            replayed.allocated().iter().collect::<Vec<_>>(),
            image.allocated().iter().collect::<Vec<_>>(),
        );

        let mut expect = vec![0u8; BLOCKS as usize * BLOCK_SIZE as usize];
        let mut actual = expect.clone();

        image.read_at(0, &mut expect).unwrap();
        replayed.read_at(0, &mut actual).unwrap();
        assert_eq!(expect, actual);
    }

    #[test]
    fn test_reset_leaves_the_image_as_it_was_created() {
        let dir = tempfile::tempdir().unwrap();
        let mut image = image(&dir);

        image
            .write_at(0, &vec![0xcc; 4 * BLOCK_SIZE as usize])
            .unwrap();
        image.reset().unwrap();

        assert_eq!(image.allocated().count_ones(), 0);
        assert_eq!(
            std::os::unix::fs::MetadataExt::blocks(&image.file().metadata().unwrap()),
            0
        );
        assert_eq!(
            image.file().metadata().unwrap().len(),
            BLOCKS as u64 * BLOCK_SIZE as u64
        );
    }
}
