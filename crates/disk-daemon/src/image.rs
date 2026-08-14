//! The sparse local image which backs one disk.
//!
//! Byte *N* of the image is byte *N* of the block device. The image is created
//! with `O_TMPFILE`, so it has no directory entry and cannot outlive the
//! daemon, and `ftruncate` gives it the device's logical size while leaving
//! every block a hole. It is disposable: the journal is the disk.

use crate::bitmap::Bitmap;

/// An image and the bitmaps which track it.
///
/// The disk's owner is the only mutator, so nothing here is synchronized. Image
/// I/O on the serving path is submitted to the owner's ring rather than issued
/// through this type, and the owner then records the effect with
/// [`Image::allocate`] or [`Image::deallocate`].
pub struct Image {
    file: std::fs::File,
    block_size: u32,
    allocated: Bitmap,
    horizon: Option<Bitmap>,
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

    /// The horizon bitmap, allocated on first use.
    ///
    /// A bitmap is `blocks / 8` bytes and is a disk's fixed memory cost, but a
    /// horizon exists only while one is active. Deferring the allocation
    /// therefore halves the steady-state cost of an idle disk.
    pub fn horizon(&mut self) -> &mut Bitmap {
        let blocks = self.allocated.blocks();
        self.horizon.get_or_insert_with(|| Bitmap::new(blocks))
    }

    pub fn read_at(&self, block: u32, buf: &mut [u8]) -> std::io::Result<()> {
        std::os::unix::fs::FileExt::read_exact_at(&self.file, buf, self.offset(block))
    }

    /// Write whole blocks. A partial block would leave the bitmap describing
    /// less than the image holds, which is why the device's logical block size
    /// is the tracking block size.
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

    /// Apply a journal chunk, which is how replay rebuilds an image.
    pub fn apply(&mut self, chunk: &crate::proto::Chunk) -> std::io::Result<()> {
        crate::chunk::apply(chunk, self.block_size, &self.file, &mut self.allocated)
    }

    /// Discard everything the image holds, leaving it as it was created.
    pub fn reset(&mut self) -> std::io::Result<()> {
        punch_hole(&self.file, 0, self.blocks() as u64 * self.block_size as u64)?;
        self.allocated = Bitmap::new(self.allocated.blocks());

        Ok(())
    }

    /// Read the image back as the chunks which reproduce it, one mutation per
    /// run of at most `run_blocks` contiguous allocated blocks.
    ///
    /// This is what a fresh disk publishes ahead of its first delta, because its
    /// formatted filesystem is content the journal has never seen and the image
    /// already holds exactly that content. Unallocated blocks are not read, so
    /// the holes a prezeroed format left are holes in a rebuilt image too.
    pub fn snapshot(&self, run_blocks: u32) -> std::io::Result<Vec<Vec<crate::proto::Chunk>>> {
        assert!(run_blocks != 0, "a snapshot run covers at least one block");
        let (mut runs, mut cursor) = (Vec::new(), 0);

        while let Some(start) = self.allocated.first_set_at_or_after(cursor) {
            let limit = std::cmp::min(self.blocks(), start.saturating_add(run_blocks));
            let mut end = start + 1;

            while end < limit && self.allocated.test(end) {
                end += 1;
            }
            let mut data = vec![0u8; (end - start) as usize * self.block_size as usize];
            () = self.read_at(start, &mut data)?;

            runs.push(crate::chunk::encode_write(
                start,
                &data.into(),
                self.block_size,
            ));
            cursor = end;
        }
        Ok(runs)
    }
}

/// Deallocate `[offset, offset+len)` of `file`, leaving a hole which reads as
/// zeroes. `FALLOC_FL_KEEP_SIZE` preserves the image's logical size, which is
/// the device's fixed capacity.
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

/// `fallocate` mode which punches a hole, shared with the owner's ring
/// submissions so both paths deallocate identically.
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

        // A punch of the middle block clears only its bit, and it reads back as
        // zeroes while its neighbours are untouched.
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

    #[test]
    fn test_horizon_bitmap_is_allocated_lazily() {
        let dir = tempfile::tempdir().unwrap();
        let mut image = image(&dir);

        assert!(image.horizon.is_none());
        image.horizon().set(3);

        assert_eq!(image.horizon().iter().collect::<Vec<_>>(), vec![3]);
        assert_eq!(image.horizon().blocks(), BLOCKS);
    }

    /// A snapshot of an image, replayed into another, reproduces it in bytes and
    /// in allocation, which is what lets a fresh disk publish its filesystem
    /// without having retained the writes which made it.
    #[test]
    fn test_a_snapshot_replays_into_an_identical_image() {
        let dir = tempfile::tempdir().unwrap();
        let mut image = image(&dir);

        // Runs which cross the split, an isolated block, an all-zero block which
        // is nonetheless allocated, and a hole punched through a written run.
        image
            .write_at(0, &vec![0xaa; 20 * BLOCK_SIZE as usize])
            .unwrap();
        image.punch(5, 2).unwrap();
        image.write_at(30, &vec![0; BLOCK_SIZE as usize]).unwrap();
        image
            .write_at(63, &vec![0xbb; BLOCK_SIZE as usize])
            .unwrap();

        // Short enough that the first run is split across several mutations.
        let runs = image.snapshot(8).unwrap();
        assert_eq!(runs.len(), 5);

        let mut replayed = Image::create(dir.path(), BLOCKS, BLOCK_SIZE).unwrap();
        for chunk in runs.iter().flatten() {
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
