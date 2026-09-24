//! Infrastructure the crate's own tests need, and nothing a case asserts.
//!
//! One kind of test here reaches outside the process. [`broker`] runs against a real
//! Gazette broker, which needs a data plane. That is a prerequisite of the host
//! rather than of the code, and it fails with what to do about it rather than
//! skipping. No test here serves a device or needs privilege: what the daemon does
//! with a real device is the black-box suite's.
//!
//! [`allocated`] needs nothing of the host. It is here because the replay, buffer,
//! backend, and broker cases all report a rebuilt image the same way. Nor does
//! [`assert_replays_identically`], which holds an image a backend served to a replay
//! of what it recorded.
//!
//! The cases themselves live beside the code they cover, inline in the file of the
//! module they test, because that is where a reader of `device/` or `journal/` looks
//! for what its module promises.
//!
//! Probing a journal is `e2e_support`'s, which the black-box suite uses too.
//!
//! That suite, in `tests/`, covers the daemon as a whole, over [`crate::client`] and
//! its journals, and shares nothing with this.

pub mod broker;

/// Each allocated block of `image`, paired with the byte it is filled with: a
/// readable report of what a replay left allocated.
pub fn allocated(image: &crate::image::Image) -> Vec<(u32, u8)> {
    let mut block = vec![0u8; crate::BLOCK_SIZE as usize];

    image
        .allocated()
        .iter()
        .map(|index| {
            image.read_at(index, &mut block).unwrap();
            (index, block[0])
        })
        .collect()
}

/// The recorded stream, replayed onto a fresh image, reproduces the image `served`
/// byte for byte and block for block. This is why the local image is disposable.
///
/// Holes are compared as well as bytes. A replay which matched every byte but
/// allocated differently would have cost the disk its sparseness.
pub fn assert_replays_identically(
    served: &crate::image::Image,
    mutations: &[Vec<crate::proto::Chunk>],
) {
    let mut replayed = crate::image::Image::create(&std::env::temp_dir(), served.blocks()).unwrap();

    for chunk in mutations.iter().flatten() {
        () = replayed.apply(chunk).unwrap();
    }
    assert_eq!(
        served.allocated(),
        replayed.allocated(),
        "the replay tracked other blocks",
    );

    // Delayed allocation settles the extents only once they are written back.
    served.file().sync_all().unwrap();
    replayed.file().sync_all().unwrap();

    assert_eq!(
        data_extents(served.file()),
        data_extents(replayed.file()),
        "the replay allocated other ranges of the host filesystem",
    );
    assert_eq!(
        first_difference(served.file(), replayed.file()),
        None,
        "the replay differs from the image served",
    );
}

/// Byte ranges the host filesystem reports as allocated.
fn data_extents(file: &std::fs::File) -> Vec<(u64, u64)> {
    let fd = std::os::fd::AsRawFd::as_raw_fd(file);
    let size = file.metadata().unwrap().len() as i64;

    let mut extents = Vec::new();
    let mut cursor = 0;

    while cursor < size {
        // SAFETY: `file` holds the descriptor open across both calls.
        let start = unsafe { libc::lseek(fd, cursor, libc::SEEK_DATA) };
        if start < 0 {
            break; // ENXIO: no data at or after `cursor`.
        }
        let end = unsafe { libc::lseek(fd, start, libc::SEEK_HOLE) };
        assert!(end > start, "SEEK_HOLE must advance past SEEK_DATA");

        extents.push((start as u64, end as u64));
        cursor = end;
    }
    extents
}

/// Offset at which two images first differ, which names where a replay went wrong
/// instead of dumping either image.
fn first_difference(left: &std::fs::File, right: &std::fs::File) -> Option<u64> {
    let len = left.metadata().unwrap().len();
    assert_eq!(
        len,
        right.metadata().unwrap().len(),
        "the images differ in size",
    );

    let mut buffers = [vec![0u8; 1 << 20], vec![0u8; 1 << 20]];
    let mut offset = 0;

    while offset < len {
        let take = std::cmp::min(buffers[0].len() as u64, len - offset) as usize;

        for (file, buf) in [left, right].into_iter().zip(buffers.iter_mut()) {
            () = std::os::unix::fs::FileExt::read_exact_at(file, &mut buf[..take], offset).unwrap();
        }
        let [left_buf, right_buf] = &buffers;

        if let Some(index) = (0..take).find(|&index| left_buf[index] != right_buf[index]) {
            return Some(offset + index as u64);
        }
        offset += take as u64;
    }
    None
}
