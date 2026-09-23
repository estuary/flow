//! What a served device does with the traffic a filesystem puts through it, and
//! what a recovery horizon costs it.
//!
//! These cases work [`Device`](super::Device) as a library, which reaches what the
//! tenure protocol does not offer: a queue depth shallow enough to force
//! backpressure, the mutation stream itself, and the copies which discharge a
//! horizon. The black-box suite in `tests/` covers everything the protocol does
//! offer, including the floor a horizon derives in `tests/pruning.rs`.

use crate::bitmap::Bitmap;
use crate::horizon::Policy;
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

privileged_test! {
    /// Overlapping writes which are in flight together reach the image in the order
    /// they were captured, so the captured stream rebuilds exactly the image served.
    ///
    /// At each step, every thread writes two blocks of the same four-block window,
    /// starting one block apart, so the writes overlap partly as well as wholly. A
    /// barrier releases them together, and each is `O_DIRECT`, so they are device
    /// requests in flight at once, in no order the block layer promises. Windows do
    /// not overlap one another, so every window's final content records the order its
    /// own writes were applied in, and a later step cannot hide an earlier mistake.
    fn test_overlapping_writes_replay_identically(dir) {
        const THREADS: usize = 8;
        const STEPS: usize = 256;

        let scenario = Scenario::new(dir);
        let (mut disk, captured) = scenario.disk(ublk::QUEUE_DEPTH, NO_COMPACTION);
        let collector = collect(captured);
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(THREADS));

        let writers: Vec<_> = (0..THREADS)
            .map(|thread| {
                let (block_path, barrier) = (disk.block_path(), barrier.clone());

                std::thread::spawn(move || {
                    let device = device::open_direct(&block_path);
                    let buf = device::aligned_buffer(2);

                    for step in 0..STEPS {
                        // Distinct among a step's writers, and never zero.
                        buf.fill(1 + ((thread * STEPS + step) % 255) as u8);
                        _ = barrier.wait();
                        () = device::write_blocks(&device, (4 * step + thread % 3) as u32, buf);
                    }
                })
            })
            .collect();

        for writer in writers {
            () = writer.join().expect("writer panicked");
        }
        let image = disk.stop().unwrap().expect("the disk was live");
        let mutations = collector.join().expect("collector panicked");

        // The block layer may merge writes which abut into one request, so the
        // capture is checked by the blocks it covers rather than by its requests.
        let written: usize = mutations
            .iter()
            .flatten()
            .map(|chunk| chunk::covered_blocks(chunk).len())
            .sum();
        assert_eq!(written, THREADS * STEPS * 2, "the capture lost a write");

        let (replayed, allocated) = replay(&scenario.dir, &mutations);
        () = assert_replays_identically(&image, &replayed, &allocated);
    }
}

privileged_test! {
    /// Open a horizon over a disk's cold blocks, discharge it with the budget a hot
    /// region's rewrites earn, and hold the invariant the whole scheme rests on: the
    /// mutations from the horizon onward rebuild the entire disk by themselves.
    ///
    /// What a delta changed rations what it copies, and a block the device rewrites
    /// costs no copy at all.
    fn test_a_horizon_discharges_and_bounds_recovery(dir) {
        /// Blocks every delta rewrites, earning the copy budget. One run of them is
        /// also the largest request the device accepts.
        const HOT_BLOCKS: u32 = ublk::MAX_IO_BUF_BYTES / BLOCK_SIZE;
        /// Blocks written once, which only a horizon copy publishes again.
        const COLD_BLOCKS: u32 = 8 * HOT_BLOCKS;
        /// Generous against the fifteen a discharge of this disk needs.
        const DELTAS: usize = 40;

        let policy = Policy {
            open_ratio: 2.0,
            copy_ratio: 0.5,
            minimum_bytes: 1 << 20,
        };
        let scenario = Scenario::new(dir);
        let (mut disk, mut captured) = scenario.disk(ublk::QUEUE_DEPTH, policy);

        let device = device::open_direct(&disk.block_path());
        let buf = device::aligned_buffer(HOT_BLOCKS);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let compactor = disk.compactor().unwrap();
        let hot = COLD_BLOCKS - HOT_BLOCKS;

        for run in 0..COLD_BLOCKS / HOT_BLOCKS {
            buf.fill(0x10 + run as u8);
            () = device::write_blocks(&device, run * HOT_BLOCKS, buf);
        }
        let (filled, _pending) = runtime.block_on(cut(&disk, &mut captured, &compactor));

        // A range within the minimum opens nothing, whatever the disk holds. A range
        // beyond it opens a horizon over every allocated block. The writes which
        // allocated those blocks are before the horizon, so none of them is in the
        // replay compared below.
        let declined = runtime.block_on(compactor.open(policy.minimum_bytes)).unwrap();
        let opened = runtime.block_on(compactor.open(1 << 30)).unwrap();

        assert_eq!(declined, None, "a range within the minimum opened a horizon");
        assert_eq!(opened, Some(COLD_BLOCKS), "the horizon missed an allocated block");
        assert_eq!(filled.len(), (COLD_BLOCKS / HOT_BLOCKS) as usize, "a fill was lost");

        // A replay which begins at the horizon reads every mutation from here on, and
        // nothing else.
        let mut after_horizon: Vec<Vec<Chunk>> = Vec::new();
        let mut copied_total = 0;

        for delta in 0..DELTAS {
            // The first delta writes nothing, as on a disk no connector is using. An
            // open horizon must then publish nothing at all.
            if delta != 0 {
                buf.fill(0xa0 + delta as u8);
                () = device::write_blocks(&device, hot, buf);
            }
            let (mutations, pending) = runtime.block_on(cut(&disk, &mut captured, &compactor));
            let (mut changed, mut copied) = (0, 0);

            for chunk in mutations.iter().flatten() {
                let bytes = chunk::data_bytes(std::slice::from_ref(chunk));

                match chunk.block < hot {
                    true => copied += bytes,
                    false => changed += bytes,
                }
            }

            // Write amplification per delta is at most one plus the copy ratio, which
            // is a half here.
            assert!(2 * copied <= changed, "delta {delta} copied {copied} for {changed} changed");

            if delta == 0 {
                assert_eq!(changed, 0, "the first delta wrote to the disk");
                assert_eq!(copied, 0, "an open horizon published without a budget");
                assert_eq!(pending, COLD_BLOCKS, "the horizon began part-discharged");
            }

            after_horizon.extend(mutations);
            copied_total += copied;

            if pending == 0 {
                break;
            }
        }
        drop(device);

        // The horizon is discharged, and the hot blocks cost it nothing: the disk's
        // own rewrites of those blocks discharged them.
        assert_eq!(
            runtime.block_on(compactor.pending()).unwrap(),
            0,
            "the horizon never discharged in {DELTAS} deltas",
        );
        assert_eq!(
            copied_total,
            ((COLD_BLOCKS - HOT_BLOCKS) * BLOCK_SIZE) as u64,
            "the copies did not cover the cold blocks exactly",
        );

        let image = disk.stop().unwrap().expect("the disk was live");
        let (replayed, allocated) = replay(&scenario.dir, &after_horizon);
        () = assert_replays_identically(&image, &replayed, &allocated);
    }
}

/// Cut the disk as a prepare does. Take the delta which ends there, alongside what
/// its open horizon still owes.
///
/// Admission is closed throughout. The mutations taken are therefore exactly the
/// delta's, and the horizon is sampled where a commit would honor it.
async fn cut(
    disk: &super::Device,
    captured: &mut crate::capture::Captured,
    compactor: &super::Compactor,
) -> (Vec<Vec<Chunk>>, u32) {
    () = disk.close_admission().await.unwrap();
    let mut mutations = Vec::new();

    while let Some(chunks) = captured.try_recv() {
        mutations.push(chunks);
    }
    let pending = compactor.pending().await.unwrap();
    () = disk.resume_admission().unwrap();

    (mutations, pending)
}
