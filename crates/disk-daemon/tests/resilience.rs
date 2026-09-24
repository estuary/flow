mod support;

/// Disks a soak works at once, and the rounds of traffic it puts through each.
const SOAK_DISKS: usize = 6;
const SOAK_ROUNDS: usize = 4;

#[tokio::test]
async fn disk_resilience() {
    let fixture = support::Fixture::start().await;
    let daemon = support::Daemon::start(&fixture, "resilience").await;

    a_disks_owner_is_an_io_flusher(&fixture, &daemon).await;
    a_broker_outage_delays_commit_confirmation(&fixture, &daemon).await;
    an_abrupt_disconnect_tears_the_disk_down(&fixture, &daemon).await;
    a_disk_torn_down_in_a_broker_outage_releases_its_parked_writes(&fixture, &daemon).await;

    daemon.drain().await;
    () = fixture.assert_no_leaks();

    // This starts and ends a daemon of its own.
    a_sigterm_under_load_tears_every_disk_down(&fixture).await;

    fixture.stop().await;
}

/// Stops every broker, and resumes them however the case which holds it ends, or
/// nothing could tear down.
struct Outage<'a>(&'a support::Fixture);

impl<'a> Outage<'a> {
    fn start(fixture: &'a support::Fixture) -> Self {
        () = fixture.signal_brokers(libc::SIGSTOP);
        Self(fixture)
    }
}

impl Drop for Outage<'_> {
    fn drop(&mut self) {
        self.0.signal_brokers(libc::SIGCONT);
    }
}

/// A disk's owner is an I/O flusher, per `PR_SET_IO_FLUSHER`. The host then throttles
/// its image writes against the host device alone, rather than against the host-wide
/// dirty limit, and its allocations never wait on I/O, which could be I/O to its own
/// disk. Without that, one disk whose writeback stalls behind its journal fills the
/// host's dirty budget with pages only its owner can clean, and throttles every other.
async fn a_disks_owner_is_an_io_flusher(fixture: &support::Fixture, daemon: &support::Daemon) {
    /// Bits of a task's `/proc` flags word which `PR_SET_IO_FLUSHER` sets:
    /// `PF_MEMALLOC_NOIO` and `PF_LOCAL_THROTTLE`, per the kernel's
    /// `include/linux/sched.h`.
    const IO_FLUSHER: u64 = 0x0008_0000 | 0x0010_0000;

    let client = daemon.client().await;
    let (disk, _mount) = client
        .open(fixture.open("acmeCo/disk/flusher").await, Vec::new())
        .await
        .unwrap();

    // Each disk is served by a thread of the daemon named for its device.
    let owners: Vec<_> = daemon
        .threads()
        .into_iter()
        .filter(|(name, _)| name.starts_with("disk-"))
        .collect();
    assert!(
        !owners.is_empty(),
        "no thread of the daemon serves the disk"
    );

    for (name, flags) in owners {
        assert_eq!(
            flags & IO_FLUSHER,
            IO_FLUSHER,
            "{name}'s flags are {flags:#x}"
        );
    }
    () = disk.close().await.unwrap();
}

/// An acknowledgement is confirmed only once its append is durable. Brokers which
/// answer nothing hold the confirmation, and it completes when they return.
async fn a_broker_outage_delays_commit_confirmation(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/outage";
    let client = daemon.client().await;

    let (mut disk, mount) = client
        .open(fixture.open(journal).await, Vec::new())
        .await
        .unwrap();

    () = support::Tree::generation(1).write(&mount.join("data"));
    let ack = support::cut(&mut disk).await;

    let outage = Outage::start(fixture);

    () = disk.acknowledge(ack).await.unwrap();
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

/// A client which disappears mid-write leaves no device and no mount behind.
async fn an_abrupt_disconnect_tears_the_disk_down(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let client = daemon.client().await;

    let (disk, mount) = client
        .open(fixture.open("acmeCo/disk/disconnected").await, Vec::new())
        .await
        .unwrap();

    // `fsync` binds the writer to the device, so it is still writing when the disk goes.
    let mut writer = support::spawn_writer(&mount.join("big"), 64 << 20, true);
    () = tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    drop(disk);

    _ = support::abandon_writer(&mut writer).await;
    () = fixture.wait_for_teardown().await;
}

/// A disk torn down while its brokers answer nothing, with its writes parked, still
/// tears down, and leaves nothing behind.
///
/// Its writer is stuck in an append no broker answers, and a stopping device waits
/// for every write it has in flight, parked ones included. The teardown must
/// therefore abandon that append, and discard what the parked writes and the unmount
/// record, before it stops the device.
async fn a_disk_torn_down_in_a_broker_outage_releases_its_parked_writes(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let client = daemon.client().await;

    let (disk, mount) = client
        .open(fixture.open("acmeCo/disk/parked").await, Vec::new())
        .await
        .unwrap();
    let outage = Outage::start(fixture);

    // Far more than the recording channel and the appender's buffer hold, so the
    // writeback parks at the device once the writer stops taking.
    let mut writer = support::spawn_writer(&mount.join("parked"), 64 << 20, true);
    () = support::wait_for_parked_writes(&mount).await;

    drop(disk);
    () = fixture.wait_for_teardown().await;
    drop(outage);

    _ = support::abandon_writer(&mut writer).await;
}

/// A daemon signalled while a disk is being written ends its tenures, tears down what
/// they held, and exits cleanly. Its client sees the tenure end.
async fn a_sigterm_under_load_tears_every_disk_down(fixture: &support::Fixture) {
    let daemon = support::Daemon::start(fixture, "sigterm").await;
    let client = daemon.client().await;

    let (mut disk, mount) = client
        .open(fixture.open("acmeCo/disk/sigterm").await, Vec::new())
        .await
        .unwrap();

    // `fsync` binds the writer to the device, so the drain lands under load.
    let mut writer = support::spawn_writer(&mount.join("load"), 64 << 20, true);
    () = tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    () = daemon.drain().await;

    _ = support::abandon_writer(&mut writer).await;
    _ = disk
        .prepare()
        .await
        .expect_err("a drained daemon serves nothing");

    () = fixture.assert_no_leaks();
}

/// Many disks under mixed traffic at once. Each round, a share of them lose the delta
/// they prepared and recover it. Every disk ends holding exactly the generation it last
/// committed, and every tenure releases its mount and device.
#[tokio::test]
async fn disk_soak() {
    let fixture = support::Fixture::start().await;
    let daemon = support::Daemon::start(&fixture, "soak").await;
    let client = daemon.client().await;

    // One tree per generation, so a disk which lost a delta is compared against the
    // generation it last committed.
    let generations: Vec<support::Tree> = (0..=SOAK_ROUNDS)
        .map(|generation| support::Tree::generation(generation as u8 + 1))
        .collect();

    let mut disks = Vec::new();

    for index in 0..SOAK_DISKS {
        let journal = format!("acmeCo/disk/soak-{index}");
        let (mut disk, mount) = client
            .open(fixture.open(&journal).await, Vec::new())
            .await
            .unwrap();

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
        // Blocking file I/O off the runtime, so every disk of the round is driven at once.
        let load = disks.iter().map(|disk| {
            let (mount, tree) = (disk.mount.clone(), generations[round].clone());

            tokio::task::spawn_blocking(move || soak_load(&mount, &tree, round))
        });

        for outcome in futures::future::join_all(load).await {
            () = outcome.expect("a round of soak traffic");
        }

        for (index, disk) in disks.iter_mut().enumerate() {
            let handle = disk.disk.as_mut().expect("a live tenure");
            let ack = support::cut(handle).await;

            // A share of the disks lose the delta they just prepared.
            if roll(index, round).is_multiple_of(3) {
                _ = disk.disk.take();
                killed += 1;
            } else {
                () = handle.acknowledge(ack).await.unwrap();
                () = handle.acknowledged().await.unwrap();
                disk.committed = round;
            }
        }

        for disk in disks.iter_mut().filter(|disk| disk.disk.is_none()) {
            () = support::wait_unmounted(&disk.mount).await;

            let (handle, mount) = client
                .open(fixture.open(&disk.journal).await, Vec::new())
                .await
                .unwrap_or_else(|err| panic!("recovering {}: {err}", disk.journal));

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
            .expect("a live tenure")
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

/// One disk of a soak, and the generation it last committed.
struct SoakDisk {
    journal: String,
    mount: std::path::PathBuf,
    /// Taken when the disk loses a delta, until the tenure which recovers it.
    disk: Option<disk_daemon::client::Disk>,
    committed: usize,
}

/// One round of traffic: a generation of files, scratch, a read back, and the removal of
/// the previous round's scratch so that the filesystem discards.
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

/// Deterministic, so a run is reproducible while the disks it kills vary by round.
fn roll(index: usize, round: usize) -> u64 {
    let mut state = 0x9e3779b97f4a7c15 ^ ((index as u64) << 32) ^ round as u64;

    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;

    state
}
