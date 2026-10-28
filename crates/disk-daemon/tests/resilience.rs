mod support;

use disk_daemon::proto;

/// Disks the soak drives at once, and rounds of traffic per disk.
const SOAK_DISKS: usize = 6;
const SOAK_ROUNDS: usize = 4;

#[tokio::test]
async fn disk_resilience() {
    let fixture = support::Fixture::start().await;
    let daemon = support::Daemon::start(&fixture, "resilience").await;

    a_broker_outage_delays_commit_confirmation(&fixture, &daemon).await;
    a_disk_torn_down_in_a_broker_outage_releases_its_parked_writes(&fixture, &daemon).await;

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

/// `acknowledged` waits for the append to be durable. It stalls while the brokers are
/// stopped, and completes once they resume.
async fn a_broker_outage_delays_commit_confirmation(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/outage";
    let client = daemon.client().await;

    let (mut disk, device) = fixture
        .open_disk(&client, journal, Default::default())
        .await;

    () = support::Writes::generation(1).write(&device);
    _ = support::prepare(&mut disk).await;

    let outage = Outage::start(fixture);

    () = disk.acknowledge().await.unwrap();
    {
        let mut confirmation = std::pin::pin!(disk.acknowledged());
        let early =
            tokio::time::timeout(std::time::Duration::from_millis(250), &mut confirmation).await;
        drop(outage);

        assert!(
            early.is_err(),
            "the commit was confirmed while its brokers were stopped: {early:?}",
        );
        () = tokio::time::timeout(support::TEARDOWN, confirmation)
            .await
            .expect("the commit was not confirmed once the brokers resumed")
            .unwrap();
    }
    () = disk.close().await.unwrap();
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

/// Many disks under concurrent mixed traffic. Each round, some disks drop their
/// prepared delta and recover. Every disk ends holding what it last committed, and
/// every attachment releases its device.
#[tokio::test]
async fn disk_soak() {
    let fixture = support::Fixture::start().await;
    let daemon = support::Daemon::start(&fixture, "soak").await;
    let client = daemon.client().await;

    let mut disks = Vec::new();

    for index in 0..SOAK_DISKS {
        let journal = format!("acmeCo/disk/soak-{index}");
        let (mut disk, device) = fixture
            .open_disk(&client, &journal, Default::default())
            .await;

        let writes = soak_writes(0);
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

    for round in 1..=SOAK_ROUNDS {
        // Drive every disk's I/O at once, off the runtime.
        let load = disks.iter().map(|disk| {
            let device = disk.device.clone();

            tokio::task::spawn_blocking(move || soak_load(&device, round))
        });

        for outcome in futures::future::join_all(load).await {
            () = outcome.expect("a round of soak traffic");
        }

        for (index, disk) in disks.iter_mut().enumerate() {
            let handle = disk.disk.as_mut().expect("a live attachment");
            _ = support::prepare(handle).await;

            // Some disks drop their prepared delta.
            if roll(index, round).is_multiple_of(3) {
                _ = disk.disk.take();
                killed += 1;
            } else {
                () = handle.acknowledge().await.unwrap();
                () = handle.acknowledged().await.unwrap();
                disk.committed = std::mem::replace(&mut disk.committed, support::Image::zeroed())
                    .with(&soak_writes(round));
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

/// What a round writes: a generation, scratch in one of two regions, and a punch of the
/// previous round's scratch in the other.
fn soak_writes(round: usize) -> support::Writes {
    const SCRATCH: u64 = 4 << 20;
    let scratch = |round: usize| (16 << 20) + (round % 2) as u64 * SCRATCH;

    let writes = support::Writes::generation(round as u8 + 1).data(
        scratch(round),
        support::pattern(round as u8, SCRATCH as usize),
    );
    match round {
        0 => writes,
        _ => writes.punch(scratch(round - 1), SCRATCH),
    }
}

/// One round of traffic on `device`: its writes, then a read back.
fn soak_load(device: &std::path::Path, round: usize) {
    () = soak_writes(round).write(device);

    let file = std::fs::File::open(device).expect("opening a device");
    let mut read = vec![0; 3 << 20];
    () = std::os::unix::fs::FileExt::read_exact_at(&file, &mut read, 1 << 20)
        .expect("reading a device back");
}

/// Deterministic, so runs are reproducible, but the dropped disks vary by round.
fn roll(index: usize, round: usize) -> u64 {
    let mut state = 0x9e3779b97f4a7c15 ^ ((index as u64) << 32) ^ round as u64;

    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;

    state
}
