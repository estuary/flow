//! What a recovery horizon costs a live disk, measured over a real device.
//!
//! The block-level accounting here is what the tenure protocol does not offer: a
//! client sees a floor on a journal, and not the copies which discharged the
//! horizon that derived it. `tests/pruning.rs` covers the floor.

use crate::chunk;
use crate::horizon::Policy;
use crate::proto::Chunk;
use crate::test_support::device::{
    self, Scenario, assert_replays_identically, privileged_test, replay,
};
use crate::{BLOCK_SIZE, ublk};

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
    disk: &crate::device::Device,
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
