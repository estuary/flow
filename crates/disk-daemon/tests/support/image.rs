use super::{BLOCK, DEVICE_SIZE, pattern};

/// Changes a case makes to a device, each at its byte offset, in order.
#[derive(Clone, Debug, Default)]
pub struct Writes {
    extents: Vec<(u64, Extent)>,
}

#[derive(Clone, Debug)]
enum Extent {
    /// Content written at the extent's offset.
    Data(Vec<u8>),
    /// A punch of this many bytes, which reads back as zeroes.
    Punch(u64),
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

    /// Also write `content` at `offset`.
    pub fn data(mut self, offset: u64, content: Vec<u8>) -> Self {
        self.extents.push((offset, Extent::Data(content)));
        self
    }

    /// Also punch `len` bytes at `offset`. Both are multiples of the block size.
    pub fn punch(mut self, offset: u64, len: u64) -> Self {
        self.extents.push((offset, Extent::Punch(len)));
        self
    }

    /// Write each extent to `device`, then flush the device, as a client does before
    /// it prepares.
    pub fn write(&self, device: &std::path::Path) {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(device)
            .unwrap_or_else(|err| panic!("opening {device:?}: {err}"));

        for (offset, extent) in &self.extents {
            match extent {
                Extent::Data(content) => {
                    std::os::unix::fs::FileExt::write_all_at(&file, content, *offset)
                        .unwrap_or_else(|err| panic!("writing {device:?} at {offset}: {err}"))
                }

                // On a block device, a punch is a write-zeroes request, which the
                // device serves as a punch of its image.
                Extent::Punch(len) => rustix::fs::fallocate(
                    &file,
                    rustix::fs::FallocateFlags::PUNCH_HOLE | rustix::fs::FallocateFlags::KEEP_SIZE,
                    *offset,
                    *len,
                )
                .unwrap_or_else(|err| panic!("punching {device:?} at {offset}: {err}")),
            }
        }
        () = file
            .sync_all()
            .unwrap_or_else(|err| panic!("flushing {device:?}: {err}"));
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
                Extent::Data(content) => {
                    self.bytes[offset..offset + content.len()].copy_from_slice(content)
                }
                Extent::Punch(len) => self.bytes[offset..offset + *len as usize].fill(0),
            }
        }
        self
    }

    /// Require `device` to hold exactly these bytes.
    pub fn assert_matches(&self, device: &std::path::Path) {
        let found = std::fs::read(device).unwrap_or_else(|err| panic!("reading {device:?}: {err}"));

        assert_eq!(
            found.len(),
            self.bytes.len(),
            "{device:?} holds {} bytes rather than {}",
            found.len(),
            self.bytes.len(),
        );
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
}
