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

    let (mut disk, mount) = fixture
        .open_disk(&client, journal, Default::default())
        .await;

    () = support::Tree::generation(1).write(&mount.join("data"));
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

    let (disk, mount) = fixture
        .open_disk(&client, "acmeCo/disk/parked", Default::default())
        .await;
    let outage = Outage::start(fixture);

    // Far more than the recording channel and append buffer hold, so writes park.
    let mut writer = support::spawn_writer(&mount.join("parked"), 64 << 20, true);
    () = support::wait_for_parked_writes(&mount).await;

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

/// `SIGTERM` drains the daemon: it cancels every attachment, waits for each teardown,
/// and exits cleanly.
///
/// One disk is under load when the drain begins, and its client sees the attachment
/// fail. The other disk's client already disconnected, leaving dirty pages. Its
/// unmount is still writing them back when the drain begins, so the drain must wait
/// on a teardown which has no stream.
async fn a_drain_tears_every_disk_down(fixture: &support::Fixture) {
    let daemon = support::Daemon::start(fixture, "drained").await;

    let client = daemon.client().await;
    let (mut loaded, loaded_mount) = fixture
        .open_disk(&client, "acmeCo/disk/sigterm", Default::default())
        .await;

    let departed_client = daemon.client().await;
    let (departed, departed_mount) = fixture
        .open_disk(&departed_client, "acmeCo/disk/departed", Default::default())
        .await;

    let mut dirty = support::spawn_writer(&departed_mount.join("dirty"), 96 << 20, false);
    () = support::wait_for_writer(&mut dirty).await;

    // With `fsync`, the writer waits on the device, so the drain lands under load.
    let mut load = support::spawn_writer(&loaded_mount.join("load"), 64 << 20, true);
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

    // Teardown removes the mount point once it unmounts.
    assert!(
        !departed_mount.exists(),
        "the teardown did not finish: {departed_mount:?}"
    );
    () = fixture.assert_no_leaks();
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
/// prepared delta and recover. Every disk ends holding the generation it last
/// committed, and every attachment releases its mount and device.
#[tokio::test]
async fn disk_soak() {
    let fixture = support::Fixture::start().await;
    let daemon = support::Daemon::start(&fixture, "soak").await;
    let client = daemon.client().await;

    // `generations[round]` is what a disk holds once it commits that round.
    let generations: Vec<support::Tree> = (0..=SOAK_ROUNDS)
        .map(|generation| support::Tree::generation(generation as u8 + 1))
        .collect();

    let mut disks = Vec::new();

    for index in 0..SOAK_DISKS {
        let journal = format!("acmeCo/disk/soak-{index}");
        let (mut disk, mount) = fixture
            .open_disk(&client, &journal, Default::default())
            .await;

        () = generations[0].write(&mount.join("data"));
        _ = support::commit(&mut disk).await;

        disks.push(SoakDisk {
            journal,
            mount,
            disk: Some(disk),
            committed: 0,
        });
    }
    let mut killed = 0;

    for round in 1..=SOAK_ROUNDS {
        // Drive every disk's file I/O at once, off the runtime.
        let load = disks.iter().map(|disk| {
            let (mount, tree) = (disk.mount.clone(), generations[round].clone());

            tokio::task::spawn_blocking(move || soak_load(&mount, &tree, round))
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
                disk.committed = round;
            }
        }

        // Wait for every teardown before any recovery. Mount paths name devices, and a
        // recovery may reuse a freed device id, so a later wait could see its mount.
        for disk in disks.iter().filter(|disk| disk.disk.is_none()) {
            () = support::wait_unmounted(&disk.mount).await;
        }
        for disk in disks.iter_mut().filter(|disk| disk.disk.is_none()) {
            let (handle, mount) = fixture
                .open_disk(&client, &disk.journal, Default::default())
                .await;

            () = generations[disk.committed].assert_matches(&mount.join("data"));

            disk.mount = mount;
            disk.disk = Some(handle);
        }
    }
    assert!(killed > 0, "no disk lost a delta over {SOAK_ROUNDS} rounds");

    for disk in disks.iter() {
        () = generations[disk.committed].assert_matches(&disk.mount.join("data"));
    }
    let mounts: Vec<std::path::PathBuf> = disks.iter().map(|disk| disk.mount.clone()).collect();

    for disk in disks.iter_mut() {
        () = disk
            .disk
            .take()
            .expect("a live attachment")
            .close()
            .await
            .unwrap();
    }
    for mount in mounts {
        () = support::wait_unmounted(&mount).await;
    }
    daemon.drain().await;
    fixture.stop().await;
}

struct SoakDisk {
    journal: String,
    /// Mount of the current attachment.
    mount: std::path::PathBuf,
    /// `None` from a dropped delta until recovery.
    disk: Option<disk_daemon::client::Disk>,
    /// Round last committed, which a recovery must reproduce.
    committed: usize,
}

/// One round of traffic: write a generation and scratch, read back, and delete the
/// previous scratch so the filesystem discards.
fn soak_load(mount: &std::path::Path, tree: &support::Tree, round: usize) {
    () = tree.write(&mount.join("data"));
    () = std::fs::write(
        mount.join(format!("scratch-{round}")),
        support::pattern(round as u8, 4 << 20),
    )
    .unwrap();

    _ = std::fs::read(mount.join("data/large")).expect("reading a file back");
    _ = std::fs::remove_file(mount.join(format!("scratch-{}", round - 1)));
}

/// Deterministic, so runs are reproducible, but the dropped disks vary by round.
fn roll(index: usize, round: usize) -> u64 {
    let mut state = 0x9e3779b97f4a7c15 ^ ((index as u64) << 32) ^ round as u64;

    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;

    state
}
