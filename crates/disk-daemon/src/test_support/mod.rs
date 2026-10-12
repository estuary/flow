pub mod broker;

/// Each allocated block of `image`, paired with the byte it is filled with: a
/// readable report of what a replay left allocated.
pub fn allocated(image: &crate::image::Image) -> Vec<(u32, u8)> {
    let mut block = vec![0u8; crate::BLOCK_SIZE as usize];

    image
        .allocated()
        .iter_ones()
        .map(|index| {
            let index = index as u32;
            image.read_at(index, &mut block).unwrap();
            (index, block[0])
        })
        .collect()
}

/// Render chunk ranges and run-length encoded data for compact snapshots.
pub fn render_chunk(chunk: &crate::proto::Chunk) -> String {
    let range = crate::chunk::covered_blocks(chunk);
    let what = match &chunk.content {
        Some(crate::proto::chunk::Content::Data(data)) if data.is_empty() => {
            "empty data (one zeroed block)".to_string()
        }
        Some(crate::proto::chunk::Content::Data(data)) => {
            format!("data len {} [{}]", data.len(), run_lengths(data))
        }
        Some(crate::proto::chunk::Content::Punch(blocks)) => format!("punch {blocks}"),
        None => "no content".to_string(),
    };
    format!("blocks {}..{}: {what}", range.start, range.end)
}

/// Render bytes as runs of one value, such as `0xabx4096 0x00x4`.
pub fn run_lengths(data: &[u8]) -> String {
    let mut runs: Vec<(u8, usize)> = Vec::new();
    for &byte in data {
        match runs.last_mut() {
            Some((value, count)) if *value == byte => *count += 1,
            _ => runs.push((byte, 1)),
        }
    }
    runs.iter()
        .map(|(value, count)| format!("{value:#04x}x{count}"))
        .collect::<Vec<_>>()
        .join(" ")
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
