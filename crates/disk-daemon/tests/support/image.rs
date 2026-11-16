use super::{BLOCK, DEVICE_SIZE, pattern};

/// Largest extent of `Writes::random`, in bytes. It is larger than the largest device
/// request, so many extents span several requests.
const MAX_RANDOM_BYTES: u64 = 1 << 20;

/// Changes a case makes to a device, each at its byte offset, in order.
#[derive(Clone, Debug, Default)]
pub struct Writes {
    extents: Vec<(u64, Extent)>,
}

#[derive(Clone, Debug)]
enum Extent {
    /// Content written through the page cache.
    Data(Vec<u8>),
    /// Content written with direct I/O, around the page cache.
    Direct(Vec<u8>),
    /// A punch of this many bytes, which reads back as zeroes.
    Punch(u64),
    /// A discard of this many bytes, which also reads back as zeroes.
    Discard(u64),
}

impl Writes {
    pub fn new() -> Self {
        Self::default()
    }

    /// Short, one-block, unaligned, and multi-record extents, one alternating between
    /// content and zeroes, and one alternating between content and a punch. Content
    /// differs by generation, so a recovery of stale content is visible.
    pub fn generation(generation: u8) -> Self {
        let seed = |offset: u8| generation.wrapping_mul(7).wrapping_add(offset);
        let odd = generation % 2 == 1;

        let writes = Self::new()
            .data(0, pattern(seed(0), 11))
            .data(BLOCK as u64, pattern(seed(1), BLOCK))
            .data(
                4 * BLOCK as u64,
                match odd {
                    true => pattern(seed(2), 3 * BLOCK),
                    false => vec![0; 3 * BLOCK],
                },
            )
            .data(1 << 20, pattern(seed(3), 3 * (1 << 20) + 17))
            .data((8 << 20) + 7, pattern(seed(4), BLOCK + 7));

        match odd {
            true => writes.data(12 << 20, pattern(seed(5), 2 * BLOCK)),
            false => writes.punch(12 << 20, 2 * BLOCK as u64),
        }
    }

    /// `count` random extents within `region`, whose bounds are multiples of the
    /// block size. A quarter are buffered writes of any offset and length. The rest
    /// are unbuffered extents of whole blocks.
    pub fn random(rng: &mut impl rand::Rng, region: std::ops::Range<u64>, count: usize) -> Self {
        let mut writes = Self::new();

        for _ in 0..count {
            if rng.random_ratio(1, 4) {
                let len = rng.random_range(1..=MAX_RANDOM_BYTES);
                let offset = rng.random_range(region.start..=region.end - len);

                writes = writes.data(offset, random_content(rng, offset, len));
            } else {
                writes = writes.random_blocks(rng, &region, MAX_RANDOM_BYTES / BLOCK as u64);
            }
        }
        writes
    }

    /// One random extent of up to `max_blocks` blocks within `region`, whose bounds are
    /// multiples of the block size. It bypasses the page cache, so it completes only
    /// once the device has served it.
    pub fn random_unbuffered(
        rng: &mut impl rand::Rng,
        region: std::ops::Range<u64>,
        max_blocks: u64,
    ) -> Self {
        Self::new().random_blocks(rng, &region, max_blocks)
    }

    /// Also write `content` at `offset`.
    pub fn data(mut self, offset: u64, content: Vec<u8>) -> Self {
        self.extents.push((offset, Extent::Data(content)));
        self
    }

    /// Also write `content` at `offset` with direct I/O. Both offset and length are
    /// multiples of the block size.
    pub fn direct(mut self, offset: u64, content: Vec<u8>) -> Self {
        self.extents.push((offset, Extent::Direct(content)));
        self
    }

    /// Also punch `len` bytes at `offset`. Both are multiples of the block size.
    pub fn punch(mut self, offset: u64, len: u64) -> Self {
        self.extents.push((offset, Extent::Punch(len)));
        self
    }

    /// Also discard `len` bytes at `offset`. Both are multiples of the block size.
    pub fn discard(mut self, offset: u64, len: u64) -> Self {
        self.extents.push((offset, Extent::Discard(len)));
        self
    }

    /// Write each extent to `device`, then flush the device, as a client does before
    /// it prepares.
    pub fn write(&self, device: &std::path::Path) {
        let files = DeviceFiles::open(device);

        () = self
            .apply(&files)
            .unwrap_or_else(|err| panic!("writing {device:?}: {err}"));
        () = files
            .buffered
            .sync_all()
            .unwrap_or_else(|err| panic!("flushing {device:?}: {err}"));
    }

    /// Apply each extent to `files`, without flushing buffered writes.
    pub fn apply(&self, files: &DeviceFiles) -> std::io::Result<()> {
        for (offset, extent) in &self.extents {
            let result = match extent {
                Extent::Data(content) => {
                    std::os::unix::fs::FileExt::write_all_at(&files.buffered, content, *offset)
                }
                Extent::Direct(content) => {
                    // Direct I/O requires an aligned buffer.
                    let mut buffer = vec![0; content.len() + BLOCK];
                    let start = buffer.as_ptr().align_offset(BLOCK);
                    let aligned = &mut buffer[start..start + content.len()];
                    aligned.copy_from_slice(content);

                    std::os::unix::fs::FileExt::write_all_at(&files.direct, aligned, *offset)
                }
                // On a block device, a punch is a write-zeroes request.
                Extent::Punch(len) => rustix::fs::fallocate(
                    &files.direct,
                    rustix::fs::FallocateFlags::PUNCH_HOLE | rustix::fs::FallocateFlags::KEEP_SIZE,
                    *offset,
                    *len,
                )
                .map_err(std::io::Error::from),
                Extent::Discard(len) => {
                    // `BLKDISCARD` of linux/fs.h, which takes a byte offset and length.
                    const BLKDISCARD: rustix::ioctl::Opcode =
                        rustix::ioctl::opcode::none(0x12, 119);

                    // SAFETY: `BLKDISCARD` reads a `[u64; 2]`.
                    unsafe {
                        rustix::ioctl::ioctl(
                            &files.direct,
                            rustix::ioctl::Setter::<BLKDISCARD, [u64; 2]>::new([*offset, *len]),
                        )
                    }
                    .map_err(std::io::Error::from)
                }
            };
            () = result.map_err(|err| {
                std::io::Error::new(err.kind(), format!("{} at {offset}: {err}", extent.verb()))
            })?;
        }
        Ok(())
    }

    /// Also a direct write, punch, or discard of up to `max_blocks` random blocks of
    /// `region`. Writes are three in five.
    fn random_blocks(
        self,
        rng: &mut impl rand::Rng,
        region: &std::ops::Range<u64>,
        max_blocks: u64,
    ) -> Self {
        let block = BLOCK as u64;
        let len = rng.random_range(1..=max_blocks) * block;
        let offset = rng.random_range(region.start / block..=(region.end - len) / block) * block;

        match rng.random_range(0..5) {
            0..3 => self.direct(offset, random_content(rng, offset, len)),
            3 => self.punch(offset, len),
            _ => self.discard(offset, len),
        }
    }
}

impl Extent {
    fn verb(&self) -> &'static str {
        match self {
            Extent::Data(_) => "writing",
            Extent::Direct(_) => "writing directly",
            Extent::Punch(_) => "punching",
            Extent::Discard(_) => "discarding",
        }
    }
}

/// Random content for `len` bytes at `offset`, where a quarter of the device blocks
/// are zeroes. So trailing-zero trimming and empty-data chunks occur.
fn random_content(rng: &mut impl rand::Rng, offset: u64, len: u64) -> Vec<u8> {
    let mut content = vec![0; len as usize];
    rng.fill(&mut content[..]);

    let block = BLOCK as u64;
    let mut start = offset;

    while start < offset + len {
        let end = (start / block + 1) * block;
        if rng.random_ratio(1, 4) {
            let zeroes = (start - offset) as usize..(end.min(offset + len) - offset) as usize;
            content[zeroes].fill(0);
        }
        start = end;
    }
    content
}

/// A block device, opened for buffered I/O and again for direct I/O.
pub struct DeviceFiles {
    buffered: std::fs::File,
    direct: std::fs::File,
}

impl DeviceFiles {
    pub fn open(device: &std::path::Path) -> Self {
        let open = |custom_flags| {
            let mut options = std::fs::OpenOptions::new();
            options.read(true).write(true);
            std::os::unix::fs::OpenOptionsExt::custom_flags(&mut options, custom_flags);

            options
                .open(device)
                .unwrap_or_else(|err| panic!("opening {device:?}: {err}"))
        };

        Self {
            buffered: open(0),
            direct: open(libc::O_DIRECT),
        }
    }
}

/// Expected bytes of a whole device of `DEVICE_SIZE`.
#[derive(Clone, PartialEq, Eq)]
pub struct Image {
    bytes: Vec<u8>,
}

impl Image {
    /// A device which nothing wrote, so it reads as zeroes.
    pub fn zeroed() -> Self {
        Self {
            bytes: vec![0; DEVICE_SIZE as usize],
        }
    }

    /// This image after `writes`.
    pub fn with(mut self, writes: &Writes) -> Self {
        for (offset, extent) in &writes.extents {
            let offset = *offset as usize;

            match extent {
                Extent::Data(content) | Extent::Direct(content) => {
                    self.bytes[offset..offset + content.len()].copy_from_slice(content)
                }
                Extent::Punch(len) | Extent::Discard(len) => {
                    self.bytes[offset..offset + *len as usize].fill(0)
                }
            }
        }
        self
    }

    /// Require `device` to hold exactly these bytes.
    pub fn assert_matches(&self, device: &std::path::Path) {
        let found = read_device(device);

        if let Some(at) = (0..found.len()).find(|&at| found[at] != self.bytes[at]) {
            panic!(
                "{device:?} differs from what was committed at offset {at} (block {}): \
                 it holds {:#04x} rather than {:#04x}",
                at / BLOCK,
                found[at],
                self.bytes[at],
            );
        }
    }

    /// Require each block of `device` to equal that block of some candidate, and
    /// return what `device` holds.
    pub fn assert_blocks_match_any(candidates: &[Image], device: &std::path::Path) -> Image {
        let found = read_device(device);

        for (block, content) in found.chunks(BLOCK).enumerate() {
            let range = block * BLOCK..(block + 1) * BLOCK;

            assert!(
                candidates
                    .iter()
                    .any(|candidate| candidate.bytes[range.clone()] == *content),
                "{device:?} block {block} matches none of the {} images it may hold",
                candidates.len(),
            );
        }
        Image { bytes: found }
    }
}

/// Every byte of `device`, which must be `DEVICE_SIZE` bytes.
fn read_device(device: &std::path::Path) -> Vec<u8> {
    let found = std::fs::read(device).unwrap_or_else(|err| panic!("reading {device:?}: {err}"));

    assert_eq!(
        found.len(),
        DEVICE_SIZE as usize,
        "{device:?} holds {} bytes rather than {DEVICE_SIZE}",
        found.len(),
    );
    found
}
