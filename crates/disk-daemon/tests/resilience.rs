mod support;

use disk_daemon::proto;
use rand::{Rng, SeedableRng};
use std::sync::atomic::Ordering;

/// Disks the soak drives at once, rounds of traffic per disk, and random extents
/// each round writes to each disk.
const SOAK_DISKS: usize = 6;
const SOAK_ROUNDS: usize = 4;
const SOAK_EXTENTS: usize = 16;
/// Seeds the soak's writes and the deltas it drops, so a failure reproduces.
const SOAK_SEED: u64 = 0x50a4;

/// Prepares the churn makes, and threads writing its disk throughout.
const CHURN_PREPARES: u64 = 32;
const CHURN_WRITERS: usize = 4;
/// Largest churn write, punch, or discard, in blocks. The journal's append rate
/// limit bounds how fast writes complete, so these are small.
const CHURN_MAX_BLOCKS: u64 = 16;
/// Seeds the churn's writes, the waits between its prepares, and the deltas it
/// drops. Thread timing still varies from run to run.
const CHURN_SEED: u64 = 0xc4c4;

/// Rounds of the stalled prepare. In about half of them, the attachment takes the
/// `Prepare` before the waiting chunk.
const STALL_ROUNDS: u64 = 10;
/// Seeds the writes of the stalled prepare.
const STALL_SEED: u64 = 0x57a1;

#[tokio::test]
async fn disk_resilience() {
    let fixture = support::Fixture::start().await;
    let daemon = support::Daemon::start(&fixture, "resilience").await;

    a_broker_outage_delays_prepare_and_commit_confirmation(&fixture, &daemon).await;
    a_disk_torn_down_in_a_broker_outage_releases_its_parked_writes(&fixture, &daemon).await;
    a_prepare_behind_a_stalled_attachment_holds_every_completed_write(&fixture, &daemon).await;

    daemon.drain().await;
    () = fixture.assert_no_leaks();

    // These start and stop their own daemons.
    a_drain_cancels_attachments_stalled_in_a_broker_outage(&fixture).await;
    a_drain_tears_every_disk_down(&fixture).await;
    a_killed_daemons_socket_is_reclaimed(&fixture).await;

    fixture.stop().await;
}

/// Stops every broker until dropped, so even a panicking case resumes them for
/// teardown.
struct Outage<'a>(&'a support::Fixture);

impl<'a> Outage<'a> {
    fn start(fixture: &'a support::Fixture) -> Self {
        () = fixture.signal_brokers(rustix::process::Signal::STOP);
        Self(fixture)
    }
}

impl Drop for Outage<'_> {
    fn drop(&mut self) {
        self.0.signal_brokers(rustix::process::Signal::CONT);
    }
}

/// `prepare` and `acknowledged` stall while the brokers are stopped, including when
/// called again after a timeout, and complete once they resume.
async fn a_broker_outage_delays_prepare_and_commit_confirmation(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/outage";
    let client = daemon.client().await;

    let (mut disk, device) = fixture
        .open_disk(&client, journal, Default::default())
        .await;

    // Before the writes, so that their appends are what the prepare stalls on.
    let outage = Outage::start(fixture);
    let writes = support::Writes::generation(1);
    () = writes.write(&device);

    () = assert_stalls(disk.prepare(), "the prepare").await;
    () = assert_stalls(disk.prepare(), "the retried prepare").await;
    drop(outage);

    let ack_intent = tokio::time::timeout(support::TEARDOWN, disk.prepare())
        .await
        .expect("the prepare did not complete once the brokers resumed")
        .unwrap();
    assert!(!ack_intent.is_empty(), "the disk changed");

    let outage = Outage::start(fixture);
    () = disk.acknowledge().await.unwrap();

    () = assert_stalls(disk.acknowledged(), "the commit's confirmation").await;
    () = assert_stalls(disk.acknowledged(), "the retried confirmation").await;
    drop(outage);

    () = tokio::time::timeout(support::TEARDOWN, disk.acknowledged())
        .await
        .expect("the commit was not confirmed once the brokers resumed")
        .unwrap();
    () = disk.close().await.unwrap();

    let (disk, device) = fixture
        .open_disk(&client, journal, Default::default())
        .await;
    () = support::Image::zeroed()
        .with(&writes)
        .assert_matches(&device);
    () = disk.close().await.unwrap();
}

async fn assert_stalls<T: std::fmt::Debug>(
    future: impl std::future::Future<Output = T>,
    what: &str,
) {
    let early = tokio::time::timeout(std::time::Duration::from_millis(250), future).await;

    assert!(
        early.is_err(),
        "{what} completed while its brokers were stopped: {early:?}",
    );
}

/// A disk torn down during a broker outage releases its parked writes. Otherwise the
/// device stop, which waits on every request in flight, would hang.
async fn a_disk_torn_down_in_a_broker_outage_releases_its_parked_writes(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let client = daemon.client().await;

    let (disk, device) = fixture
        .open_disk(&client, "acmeCo/disk/parked", Default::default())
        .await;
    let outage = Outage::start(fixture);

    // Far more than the recording channel and append buffer hold, so writes park.
    let mut writer = support::spawn_writer(&device, 0, 24 << 20, true);
    () = support::wait_for_parked_writes(&device).await;

    drop(disk);
    () = fixture.wait_for_teardown().await;
    drop(outage);

    _ = support::abandon_writer(&mut writer).await;
}

/// A prepare which finds the attachment behind the device still holds every write the
/// device completed before it.
///
/// A broker outage stalls the attachment under churn:
/// 1. Its appender buffers records until it holds 8 MB. Its next append then waits on
///    the stopped brokers (`publisher::Appender::checkpoint`).
/// 2. The device's next chunk fills the live-chunk channel, and that chunk's write
///    completes. Later writes park.
///
/// `prepare` is called in that state, and then the brokers resume. The attachment
/// finds both the `Prepare` and the waiting chunk ready, and its unbiased `select!`
/// takes either first (`attachment/promoted.rs`). If it takes the `Prepare` first, it
/// must still append the waiting chunk to the delta.
///
/// Each round acknowledges the delta, then drops the disk before it prepares again. So
/// a chunk left out of the delta is lost, and the recovery shows it.
async fn a_prepare_behind_a_stalled_attachment_holds_every_completed_write(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/stalled";
    let client = daemon.client().await;
    let mut rng = rand::rngs::StdRng::seed_from_u64(STALL_SEED);

    // No append rate limit, so each prepare quickly flushes the backlog of its outage.
    let mut spec = fixture.spec(journal);
    spec.max_append_rate = 0;
    () = fixture.create_journal(spec).await;

    let churn = std::sync::Arc::new(Churn::default());
    let (mut disk, mut device) = fixture
        .open_disk(&client, journal, Default::default())
        .await;
    let mut expected = support::Image::zeroed();

    for round in 0..STALL_ROUNDS {
        let writers = churn.spawn_writers(&device, &mut rng);
        let outage = Outage::start(fixture);
        () = support::wait_for_parked_writes(&device).await;

        churn.epoch.store(2 * round + 1, Ordering::SeqCst);
        // Resume once the `Prepare` has had time to reach the stalled attachment.
        let (prepared, ()) = tokio::join!(disk.prepare(), async {
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            drop(outage);
        });
        _ = prepared.expect("prepare");
        churn.epoch.store(2 * round + 2, Ordering::SeqCst);

        () = disk.acknowledge().await.unwrap();
        () = disk.acknowledged().await.unwrap();

        churn.stop.store(true, Ordering::SeqCst);
        drop(disk);

        for writer in futures::future::join_all(writers).await {
            _ = writer.expect("a churn writer");
        }
        () = support::wait_for_device_removal(&device).await;

        (disk, device) = fixture
            .open_disk(&client, journal, Default::default())
            .await;
        expected = churn.assert_recovered(expected, Some(round), &device);
        churn.stop.store(false, Ordering::SeqCst);
    }
    () = disk.close().await.unwrap();
}

/// A drain cancels attachments whose `Open` or `Promote` is stalled on stopped
/// brokers, rather than waiting on them.
async fn a_drain_cancels_attachments_stalled_in_a_broker_outage(fixture: &support::Fixture) {
    let daemon = support::Daemon::start(fixture, "stalled").await;

    let open = fixture.open("acmeCo/disk/stalled-open").await;
    let mut opening = daemon.attach().await;

    let mut promoting = daemon.attach().await;
    let replies = promoting
        .exchange(vec![proto::request::Request::Open(
            fixture.open("acmeCo/disk/stalled-promote").await,
        )])
        .await
        .unwrap();
    assert!(
        matches!(replies[..], [proto::response::Response::Opened(_)]),
        "{replies:?}"
    );

    let outage = Outage::start(fixture);

    () = opening.send(proto::request::Request::Open(open)).await;
    () = promoting
        .send(proto::request::Request::Promote(Default::default()))
        .await;

    for attachment in [&mut opening, &mut promoting] {
        let early =
            tokio::time::timeout(std::time::Duration::from_millis(250), attachment.reply()).await;
        assert!(
            early.is_err(),
            "replied while its brokers were stopped: {early:?}"
        );
    }

    () = tokio::time::timeout(support::TEARDOWN, daemon.drain())
        .await
        .expect("the daemon drained while its brokers were stopped");

    for attachment in [&mut opening, &mut promoting] {
        _ = attachment
            .reply()
            .await
            .expect_err("a drained daemon serves nothing");
    }
    drop(outage);

    () = fixture.assert_no_leaks();
}

/// `SIGTERM` drains the daemon: it cancels every attachment, stops and deletes each
/// device, and exits cleanly.
///
/// One disk is under load when the drain begins, and its client sees the attachment
/// fail. The other disk's client already left, with ext4 still mounted on its device
/// and dirty pages in the page cache. That disk's teardown has no stream, so the drain
/// must wait on it, and the filesystem doesn't hold it up.
async fn a_drain_tears_every_disk_down(fixture: &support::Fixture) {
    let daemon = support::Daemon::start(fixture, "drained").await;

    let client = daemon.client().await;
    let (mut loaded, loaded_device) = fixture
        .open_disk(&client, "acmeCo/disk/sigterm", Default::default())
        .await;

    let departed_client = daemon.client().await;
    let (departed, departed_device) = fixture
        .open_disk(&departed_client, "acmeCo/disk/departed", Default::default())
        .await;
    let mounted = support::Mounted::format(fixture.dir.path(), &departed_device);

    let mut dirty = support::spawn_writer(&mounted.path().join("dirty"), 0, 16 << 20, false);
    () = support::wait_for_writer(&mut dirty).await;

    // With `fsync`, the writer waits on the device, so the drain lands under load.
    let mut load = support::spawn_writer(&loaded_device, 0, 24 << 20, true);
    () = tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    drop((departed, departed_client));
    () = tokio::time::timeout(support::TEARDOWN, daemon.drain())
        .await
        .expect("the daemon drained while a departed client's disk tore down");

    _ = support::abandon_writer(&mut load).await;
    _ = loaded
        .prepare()
        .await
        .expect_err("a drained daemon serves nothing");

    () = fixture.wait_for_teardown_under(mounted).await;
}

/// A daemon killed without draining leaves its socket behind, and the next daemon on
/// that path reclaims it.
async fn a_killed_daemons_socket_is_reclaimed(fixture: &support::Fixture) {
    let daemon = support::Daemon::start(fixture, "reclaimed").await;
    let uds_path = daemon.uds_path.clone();

    () = daemon.kill().await;
    assert!(uds_path.exists(), "a killed daemon removed its socket");

    // `start` returns once the new daemon serves on the same path.
    let daemon = support::Daemon::start(fixture, "reclaimed").await;
    () = daemon.drain().await;
    () = fixture.assert_no_leaks();
}

/// Many disks under concurrent random traffic. Each round, every device reads back
/// what was written to it, then some disks drop their prepared delta and recover.
/// Every disk ends holding what it last committed, and every attachment releases its
/// device.
#[tokio::test]
async fn disk_soak() {
    let fixture = support::Fixture::start().await;
    let daemon = support::Daemon::start(&fixture, "soak").await;
    let client = daemon.client().await;
    let mut rng = rand::rngs::StdRng::seed_from_u64(SOAK_SEED);

    let mut disks = Vec::new();

    for index in 0..SOAK_DISKS {
        let journal = format!("acmeCo/disk/soak-{index}");
        let (mut disk, device) = fixture
            .open_disk(&client, &journal, Default::default())
            .await;

        let writes = support::Writes::random(&mut rng, 0..support::DEVICE_SIZE, SOAK_EXTENTS);
        () = writes.write(&device);
        _ = support::commit(&mut disk).await;

        disks.push(SoakDisk {
            journal,
            device,
            disk: Some(disk),
            committed: support::Image::zeroed().with(&writes),
        });
    }
    let mut killed = 0;

    for _ in 0..SOAK_ROUNDS {
        // Each disk's writes differ, so a disk recovering another's journal fails.
        let writes: Vec<_> = disks
            .iter()
            .map(|_| support::Writes::random(&mut rng, 0..support::DEVICE_SIZE, SOAK_EXTENTS))
            .collect();

        // Drive every disk's I/O at once, off the runtime.
        let load = disks.iter().zip(&writes).map(|(disk, writes)| {
            let (device, writes) = (disk.device.clone(), writes.clone());

            tokio::task::spawn_blocking(move || writes.write(&device))
        });

        for outcome in futures::future::join_all(load).await {
            () = outcome.expect("a round of soak traffic");
        }

        for (disk, writes) in disks.iter_mut().zip(&writes) {
            let written = disk.committed.clone().with(writes);
            () = written.assert_matches(&disk.device);

            let handle = disk.disk.as_mut().expect("a live attachment");
            _ = support::prepare(handle).await;

            // Some disks drop their prepared delta.
            if rng.random_ratio(1, 3) {
                _ = disk.disk.take();
                killed += 1;
            } else {
                () = handle.acknowledge().await.unwrap();
                () = handle.acknowledged().await.unwrap();
                disk.committed = written;
            }
        }

        // Wait for every teardown before any recovery, which may reuse a freed device's
        // number.
        for disk in disks.iter().filter(|disk| disk.disk.is_none()) {
            () = support::wait_for_device_removal(&disk.device).await;
        }
        for disk in disks.iter_mut().filter(|disk| disk.disk.is_none()) {
            let (handle, device) = fixture
                .open_disk(&client, &disk.journal, Default::default())
                .await;

            () = disk.committed.assert_matches(&device);

            disk.device = device;
            disk.disk = Some(handle);
        }
    }
    assert!(killed > 0, "no disk lost a delta over {SOAK_ROUNDS} rounds");

    for disk in disks.iter_mut() {
        () = disk.committed.assert_matches(&disk.device);
        () = disk
            .disk
            .take()
            .expect("a live attachment")
            .close()
            .await
            .unwrap();
    }
    daemon.drain().await;
    fixture.stop().await;
}

struct SoakDisk {
    /// The disk's journal, which each recovery reopens.
    journal: String,
    /// Block device of the current attachment.
    device: std::path::PathBuf,
    /// `None` from a dropped delta until recovery.
    disk: Option<disk_daemon::client::Disk>,
    /// What the disk last committed, which a recovery must reproduce.
    committed: support::Image,
}

/// Writers keep writing one disk through every prepare, and some prepared deltas are
/// dropped.
///
/// A delta holds what the device completed before `Prepare` arrived
/// (`Disk::prepare`). So a write which completed before `prepare` was called is in
/// the delta, and one which started after `prepare` returned is not. A write in
/// between races the prepare. It may land in this delta or the next, and a write of
/// several requests may land partly in each.
///
/// Each recovery must hold the last commit. A block which racing writes touched may
/// hold its content before or after each of them.
#[tokio::test]
async fn disk_commits_under_load() {
    let fixture = support::Fixture::start().await;
    // Horizons open after a few megabytes of journal rather than a gigabyte. So they
    // complete during the churn, and recoveries replay from the floors they store.
    let daemon = support::Daemon::start_with(
        &fixture,
        "churn",
        &[
            "--horizon-open-ratio",
            "0.25",
            "--horizon-copy-ratio",
            "1.0",
            "--horizon-minimum-bytes",
            "1048576",
        ],
    )
    .await;
    let client = daemon.client().await;
    let journal = "acmeCo/disk/churn";
    let mut rng = rand::rngs::StdRng::seed_from_u64(CHURN_SEED);

    let churn = std::sync::Arc::new(Churn::default());
    let (mut disk, mut device) = fixture
        .open_disk(&client, journal, Default::default())
        .await;
    let mut writers = churn.spawn_writers(&device, &mut rng);
    // What the disk held at its last recovery, with each logged write folded in once
    // a commit certainly holds it.
    let mut expected = support::Image::zeroed();
    // The last prepare acknowledged since the last recovery.
    let mut acknowledged = None;
    let mut dropped = 0;

    for prepare in 0..CHURN_PREPARES {
        () = churn.wait_for_writes(rng.random_range(8..64)).await;

        churn.epoch.store(2 * prepare + 1, Ordering::SeqCst);
        _ = disk.prepare().await.expect("prepare");
        churn.epoch.store(2 * prepare + 2, Ordering::SeqCst);

        if rng.random_ratio(1, 4) {
            churn.stop.store(true, Ordering::SeqCst);
            // The teardown releases writes parked behind the prepared delta. Later
            // writes fail.
            drop(disk);

            for writer in futures::future::join_all(writers).await {
                _ = writer.expect("a churn writer");
            }
            () = support::wait_for_device_removal(&device).await;

            (disk, device) = fixture
                .open_disk(&client, journal, Default::default())
                .await;
            expected = churn.assert_recovered(expected, acknowledged, &device);
            acknowledged = None;
            dropped += 1;

            churn.stop.store(false, Ordering::SeqCst);
            writers = churn.spawn_writers(&device, &mut rng);
        } else {
            () = disk.acknowledge().await.unwrap();
            () = disk.acknowledged().await.unwrap();

            expected = churn.fold(expected, prepare);
            acknowledged = Some(prepare);
        }
    }
    assert!(
        dropped > 0,
        "no delta was dropped over {CHURN_PREPARES} prepares"
    );

    // No prepare is pending, so every write completes.
    churn.stop.store(true, Ordering::SeqCst);
    for writer in futures::future::join_all(writers).await {
        () = writer
            .expect("a churn writer")
            .expect("a write to a live disk");
    }
    // Every logged write completed before the next prepare, so it commits.
    expected = churn.fold(expected, CHURN_PREPARES);
    () = expected.assert_matches(&device);

    _ = disk.prepare().await.expect("prepare");
    () = disk.acknowledge().await.unwrap();
    () = disk.close().await.unwrap();

    let (disk, device) = fixture
        .open_disk(&client, journal, Default::default())
        .await;
    () = expected.assert_matches(&device);
    () = disk.close().await.unwrap();

    daemon.drain().await;
    fixture.stop().await;
}

/// What the churn's writers share with the task committing their disk.
#[derive(Default)]
struct Churn {
    /// Prepare `j` sets this to `2j + 1` as it's called, and `2j + 2` once it returns.
    epoch: std::sync::atomic::AtomicU64,
    /// Ends the writers.
    stop: std::sync::atomic::AtomicBool,
    /// Writes completed by all writers.
    completed: std::sync::atomic::AtomicU64,
    /// Each writer's writes in order, apart from those folded into the expected image.
    logs: [std::sync::Mutex<Vec<Logged>>; CHURN_WRITERS],
}

/// A churn write, and the epochs read before it started and after it completed.
struct Logged {
    writes: support::Writes,
    started: u64,
    ended: u64,
}

impl Churn {
    fn spawn_writers(
        self: &std::sync::Arc<Self>,
        device: &std::path::Path,
        rng: &mut impl rand::Rng,
    ) -> Vec<tokio::task::JoinHandle<std::io::Result<()>>> {
        (0..CHURN_WRITERS)
            .map(|index| {
                let churn = self.clone();
                let device = device.to_path_buf();
                let rng = rand::rngs::StdRng::seed_from_u64(rng.random());

                tokio::task::spawn_blocking(move || churn.write(index, &device, rng))
            })
            .collect()
    }

    /// Write random blocks of writer `index`'s own region of `device` until stopped.
    /// Each block has one writer, whose writes are in order. Returns the first failed
    /// write, which only a teardown may cause.
    fn write(
        &self,
        index: usize,
        device: &std::path::Path,
        mut rng: rand::rngs::StdRng,
    ) -> std::io::Result<()> {
        let files = support::DeviceFiles::open(device);
        let len = support::DEVICE_SIZE / CHURN_WRITERS as u64;
        let region = index as u64 * len..(index as u64 + 1) * len;

        while !self.stop.load(Ordering::SeqCst) {
            let writes =
                support::Writes::random_unbuffered(&mut rng, region.clone(), CHURN_MAX_BLOCKS);

            let started = self.epoch.load(Ordering::SeqCst);
            let result = writes.apply(&files);
            let ended = self.epoch.load(Ordering::SeqCst);

            self.logs[index].lock().unwrap().push(Logged {
                writes,
                started,
                ended,
            });
            self.completed.fetch_add(1, Ordering::SeqCst);

            if let Err(err) = result {
                assert!(
                    self.stop.load(Ordering::SeqCst),
                    "churn writer {index} failed on a live disk: {err}",
                );
                return Err(err);
            }
        }
        Ok(())
    }

    /// Wait for writers to complete `count` more writes.
    async fn wait_for_writes(&self, count: u64) {
        let target = self.completed.load(Ordering::SeqCst) + count;
        let deadline = std::time::Instant::now() + support::TEARDOWN;

        while self.completed.load(Ordering::SeqCst) < target {
            assert!(
                std::time::Instant::now() < deadline,
                "churn writers stalled for {:?}",
                support::TEARDOWN,
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    /// Fold into `expected` each logged write which completed before prepare `j` was
    /// called. Each is in the delta of `j` or an earlier prepare, and `j` was
    /// acknowledged.
    fn fold(&self, mut expected: support::Image, j: u64) -> support::Image {
        for log in &self.logs {
            let mut log = log.lock().unwrap();
            let committed = log
                .iter()
                .take_while(|logged| logged.ended <= 2 * j)
                .count();

            for logged in log.drain(..committed) {
                expected = expected.with(&logged.writes);
            }
        }
        expected
    }

    /// Require `device`, recovered after its disk was dropped, to hold what prepare
    /// `acknowledged` committed, or `expected` where none was. Every writer has exited.
    /// Returns what `device` holds, which the next writes build on.
    fn assert_recovered(
        &self,
        expected: support::Image,
        acknowledged: Option<u64>,
        device: &std::path::Path,
    ) -> support::Image {
        let expected = match acknowledged {
            Some(j) => self.fold(expected, j),
            None => expected,
        };
        // Writes left which started before the acknowledged prepare returned raced it.
        // Each writer's racing writes come first in its log. Later writes are lost.
        let racing: Vec<Vec<support::Writes>> = self
            .logs
            .iter()
            .map(|log| {
                log.lock()
                    .unwrap()
                    .drain(..)
                    .take_while(|logged| acknowledged.is_some_and(|j| logged.started < 2 * j + 2))
                    .map(|logged| logged.writes)
                    .collect()
            })
            .collect();

        // Candidate `i` adds each writer's first `i` racing writes. A block has one
        // writer, so each of its possible contents is in some candidate.
        let depth = racing.iter().map(Vec::len).max().unwrap_or(0);
        let candidates: Vec<support::Image> = (0..=depth)
            .map(|i| {
                racing
                    .iter()
                    .flat_map(|writes| writes.iter().take(i))
                    .fold(expected.clone(), |image, writes| image.with(writes))
            })
            .collect();

        support::Image::assert_blocks_match_any(&candidates, device)
    }
}
