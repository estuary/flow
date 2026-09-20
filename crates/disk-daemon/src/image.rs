//! The sparse local image which backs one disk.
//!
//! Byte N of the image is byte N of the block device. The image is created with
//! `O_TMPFILE`, so it has no directory entry and cannot outlive the daemon.
//! `ftruncate` then gives it the device's logical size, and leaves every block a
//! hole. The image is disposable. The journal is the disk.
//!
//! An image is a file and the allocation it holds, and nothing else. A recovery
//! horizon is compaction state over that allocation rather than part of it, so
//! whoever is compacting owns it: the disk's owner while it is served, and
//! the replay's own pass while it is replayed.

use crate::bitmap::Bitmap;

/// An image and the bitmap which tracks what it has allocated.
///
/// Only the disk's owner mutates this, so nothing here is synchronized. On the
/// serving path the owner submits image I/O to its ring rather than through this
/// type. It then records the effect with [`Image::allocate`] or
/// [`Image::deallocate`].
pub struct Image {
    file: std::fs::File,
    allocated: Bitmap,
}

impl Image {
    /// Create a `blocks` × [`crate::BLOCK_SIZE`] image within `dir`.
    pub fn create(dir: &std::path::Path, blocks: u32) -> std::io::Result<Self> {
        assert!(blocks != 0, "a device has at least one block");

        let mut options = std::fs::OpenOptions::new();
        options.read(true).write(true);
        std::os::unix::fs::OpenOptionsExt::custom_flags(&mut options, libc::O_TMPFILE);
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);

        let file = options.open(dir)?;
        file.set_len(blocks as u64 * crate::BLOCK_SIZE as u64)?;

        Ok(Self {
            file,
            allocated: Bitmap::new(blocks),
        })
    }

    pub fn file(&self) -> &std::fs::File {
        &self.file
    }

    pub fn blocks(&self) -> u32 {
        self.allocated.blocks()
    }

    /// Byte offset at which `block` begins.
    pub fn offset(&self, block: u32) -> u64 {
        block as u64 * crate::BLOCK_SIZE as u64
    }

    pub fn allocated(&self) -> &Bitmap {
        &self.allocated
    }

    /// Read blocks back.
    ///
    /// The owner reads through its ring, so this is not the serving path. It is
    /// what a horizon copies out of the image with, and what a test inspects an
    /// image with.
    pub fn read_at(&self, block: u32, buf: &mut [u8]) -> std::io::Result<()> {
        std::os::unix::fs::FileExt::read_exact_at(&self.file, buf, self.offset(block))
    }

    /// Write whole blocks. A partial block would leave the bitmap describing
    /// less than the image holds.
    #[cfg(test)]
    pub fn write_at(&mut self, block: u32, data: &[u8]) -> std::io::Result<()> {
        let blocks = data.len() / crate::BLOCK_SIZE as usize;
        assert_eq!(
            data.len() % crate::BLOCK_SIZE as usize,
            0,
            "an image write is a whole number of {}-byte blocks",
            crate::BLOCK_SIZE,
        );

        std::os::unix::fs::FileExt::write_all_at(&self.file, data, self.offset(block))?;
        self.allocate(block..block + blocks as u32);
        Ok(())
    }

    #[cfg(test)]
    pub fn punch(&mut self, block: u32, blocks: u32) -> std::io::Result<()> {
        punch_hole(
            &self.file,
            self.offset(block),
            blocks as u64 * crate::BLOCK_SIZE as u64,
        )?;
        self.deallocate(block..block + blocks);
        Ok(())
    }

    /// Record that `range` now occupies space in the image.
    pub fn allocate(&mut self, range: std::ops::Range<u32>) {
        self.allocated.set_range(range);
    }

    /// Record that `range` no longer occupies space in the image.
    pub fn deallocate(&mut self, range: std::ops::Range<u32>) {
        self.allocated.clear_range(range);
    }

    /// Apply a journal chunk. This is how replay rebuilds an image.
    ///
    /// Discharging the horizon the chunk publishes belongs to whoever holds that
    /// horizon, which is [`crate::journal::buffer::Buffer::drain`] here.
    pub fn apply(&mut self, chunk: &crate::proto::Chunk) -> std::io::Result<()> {
        crate::chunk::apply(chunk, &self.file, &mut self.allocated)
    }

    /// Discard everything the image holds, leaving it as it was created.
    pub fn reset(&mut self) -> std::io::Result<()> {
        punch_hole(
            &self.file,
            0,
            self.blocks() as u64 * crate::BLOCK_SIZE as u64,
        )?;
        self.allocated = Bitmap::new(self.allocated.blocks());

        Ok(())
    }
}

/// Deallocate `[offset, offset+len)` of `file`, leaving a hole which reads as
/// zeroes. `FALLOC_FL_KEEP_SIZE` keeps the image's logical size, which is the
/// device's fixed capacity.
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

/// `fallocate` mode which punches a hole. The owner's ring submissions use it
/// too, so both paths deallocate identically.
pub(crate) const PUNCH_MODE: i32 = libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE;

#[cfg(test)]
mod test {
    use super::Image;
    use crate::BLOCK_SIZE;

    const BLOCKS: u32 = 64;

    fn image(dir: &tempfile::TempDir) -> Image {
        Image::create(dir.path(), BLOCKS).unwrap()
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
