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

/// The recorded stream, replayed onto a fresh image, reproduces the image `served`:
/// the same bytes in the same allocated blocks. This is why the local image is
/// disposable.
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

    assert_eq!(
        first_difference(served, &replayed),
        None,
        "the replay differs from the image served",
    );
}

/// Block at which two images first differ, which names where a replay went wrong
/// instead of dumping either image.
fn first_difference(left: &crate::image::Image, right: &crate::image::Image) -> Option<u32> {
    let block_size = crate::BLOCK_SIZE as usize;
    let (mut left_buf, mut right_buf) = (vec![0u8; block_size], vec![0u8; block_size]);

    (0..left.blocks()).find(|&block| {
        () = left.read_at(block, &mut left_buf).unwrap();
        () = right.read_at(block, &mut right_buf).unwrap();
        left_buf != right_buf
    })
}
