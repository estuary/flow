//! What a served device does with the traffic a filesystem puts through it.
//!
//! These cases work [`Device`](crate::device::Device) as a library, which reaches what the tenure protocol
//! does not offer: a queue depth shallow enough to force backpressure, and the
//! mutation stream itself. The black-box suite in `tests/` covers everything the
//! protocol does offer.

use crate::bitmap::Bitmap;
use crate::image::Image;
use crate::proto::{Chunk, chunk::Content};
use crate::test_support::device::{
    self, BLOCKS, Mount, NO_COMPACTION, Scenario, assert_replays_identically, collect,
    privileged_test, replay,
};
use crate::{BLOCK_SIZE, chunk, ublk};

privileged_test! {
    /// Freeing space through the mount reaches the disk as discards, which become
    /// punches, which clear allocated bits.
    fn test_discards_become_punches_which_clear_allocated_bits(dir) {
        const FILE_BLOCKS: u32 = 4096;

        let scenario = Scenario::new(dir);
        let (mut disk, captured) = scenario.disk(ublk::QUEUE_DEPTH, NO_COMPACTION);
        let collector = collect(captured);

        let block_path = disk.block_path();
        () = device::format(&block_path);

        let mut mount = Mount::new(&block_path, &scenario.dir.join("mnt"));
        let filler = mount.path.join("filler");

        let file = std::fs::File::create(&filler).unwrap();
        () = std::os::unix::fs::FileExt::write_all_at(
            &file,
            &device::pattern(0x5a, (FILE_BLOCKS * BLOCK_SIZE) as usize),
            0,
        )
        .unwrap();
        file.sync_all().unwrap();
        drop(file);

        std::fs::remove_file(&filler).unwrap();
        // `-o discard` issues discards as ext4 commits the deletion. `fstrim` then
        // covers everything else the filesystem considers free.
        device::run(&mut std::process::Command::new("sync")).unwrap();
        device::run(std::process::Command::new("fstrim").arg(&mount.path)).unwrap();

        mount.unmount().unwrap();
        let image = disk.stop().unwrap().expect("the disk was live");
        let mutations = collector.join().expect("collector panicked");

        // Replay tracks the allocated set as it moves. Its peak is what the filler
        // occupied, and its final value is what the discards left.
        let mut peak = 0;
        let mut moving = Bitmap::new(BLOCKS);
        let scratch = Image::create(&scenario.dir, BLOCKS).unwrap();

        for chunk in mutations.iter().flatten() {
            () = chunk::apply(chunk, scratch.file(), &mut moving).unwrap();
            peak = std::cmp::max(peak, moving.count_ones());
        }

        let punches: Vec<&Chunk> = mutations
            .iter()
            .flatten()
            .filter(|chunk| matches!(chunk.content, Some(Content::Punch(_))))
            .collect();
        let punched: u32 = punches
            .iter()
            .map(|chunk| chunk::covered_blocks(chunk).len() as u32)
            .sum();
        let left = image.allocated().count_ones();

        assert!(!punches.is_empty(), "the deletion issued no discard");
        assert!(punched >= FILE_BLOCKS, "punched {punched} of {FILE_BLOCKS} blocks");
        assert!(peak >= FILE_BLOCKS, "the filler allocated {peak} blocks");
        assert!(left * 4 < peak, "{left} of a peak {peak} blocks are still allocated");

        let (replayed, allocated) = replay(&scenario.dir, &mutations);
        () = assert_replays_identically(&image, &replayed, &allocated);
    }
}

privileged_test! {
    /// A stalled capture sink parks writes rather than failing them.
    fn test_backpressure_parks_writes(dir) {
        // The capture channel holds one mutation per queue slot, so a shallow queue
        // gives a shallow channel.
        const QUEUE_DEPTH: u16 = 2;
        const WRITES: usize = 16;
        const STALL: std::time::Duration = std::time::Duration::from_millis(500);

        let scenario = Scenario::new(dir);
        let (mut disk, captured) = scenario.disk(QUEUE_DEPTH, NO_COMPACTION);

        let done = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let failed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let writer = {
            let (block_path, done, failed) = (disk.block_path(), done.clone(), failed.clone());

            // Each write is one device request, which does not return until the
            // device completes it.
            std::thread::spawn(move || {
                let device = device::open_direct(&block_path);
                let block = device::aligned_block(0x7e);

                for index in 0..WRITES {
                    let offset = index as u64 * BLOCK_SIZE as u64;

                    match std::os::unix::fs::FileExt::write_all_at(&device, block, offset) {
                        Ok(()) => _ = done.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                        Err(err) => {
                            tracing::error!(?err, offset, "device write failed");
                            failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
            })
        };

        std::thread::sleep(STALL);
        let during_stall = done.load(std::sync::atomic::Ordering::Relaxed);

        // Taking from the channel frees the parked writes.
        let collector = collect(captured);
        () = writer.join().expect("writer panicked");

        let completed = done.load(std::sync::atomic::Ordering::Relaxed);
        let failed = failed.load(std::sync::atomic::Ordering::Relaxed);

        let image = disk.stop().unwrap().expect("the disk was live");
        let mutations = collector.join().expect("collector panicked");

        // Only what fit the channel completed under the stall. Once the channel
        // drained, every write completed, and none was dropped or errored.
        assert!(during_stall <= QUEUE_DEPTH as usize, "{during_stall} writes outran the channel");
        assert!(during_stall < WRITES, "the stall parked nothing");

        assert_eq!(failed, 0, "{failed} writes failed");
        assert_eq!(completed, WRITES, "only {completed} writes completed");
        assert_eq!(mutations.len(), WRITES, "the capture lost a write");

        let (replayed, allocated) = replay(&scenario.dir, &mutations);
        () = assert_replays_identically(&image, &replayed, &allocated);
    }
}
