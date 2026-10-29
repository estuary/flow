mod support;

use proto_gazette::broker;

#[tokio::test]
async fn disk_lifecycle() {
    let fixture = support::Fixture::start().await;
    let daemon = support::Daemon::start(&fixture, "lifecycle").await;

    a_disk_which_is_never_written_recovers_its_format(&fixture, &daemon).await;
    a_standby_leaves_a_suspended_journal_asleep(&fixture, &daemon).await;
    an_absent_journal_is_refused_at_open(&fixture, &daemon).await;
    the_next_daemon_reaps_an_orphan(&fixture).await;

    daemon.drain().await;
    fixture.stop().await;
}

/// The daemon commits its own format and mount writes as the setup delta. So a client
/// which writes nothing has nothing to commit, on its first attachment or any later
/// one. A second attachment recovers the filesystem rather than formatting another.
///
/// The format's journal cost scales with what it allocated, not the device size: a
/// prezeroed format leaves inode tables and the ext4 journal as unrecorded holes.
///
/// Each mount belongs to the client which opened it, fresh or recovered, so a client
/// needs no privilege. A daemon thread serves the disk as an I/O flusher.
async fn a_disk_which_is_never_written_recovers_its_format(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    /// `PF_MEMALLOC_NOIO | PF_LOCAL_THROTTLE`, which `PR_SET_IO_FLUSHER` sets in a
    /// task's `/proc` flags word (see the kernel's `include/linux/sched.h`).
    const IO_FLUSHER: u64 = 0x0008_0000 | 0x0010_0000;

    let journal = "acmeCo/disk/untouched";
    let client = daemon.client().await;

    let ours = owner(fixture.dir.path());
    assert_ne!(ours.0, 0, "this suite must not run as root");

    let usable = |mount: &std::path::Path| {
        assert_eq!(
            std::fs::read_dir(mount)
                .expect("the mount")
                .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
                .collect::<Vec<_>>(),
            vec!["lost+found"],
            "{mount:?} is not a usable empty filesystem",
        );
        assert_eq!(owner(mount), ours, "{mount:?} does not belong to this test");
    };

    let (mut disk, mount) = fixture
        .open_disk(&client, journal, Default::default())
        .await;
    () = usable(&mount);

    let owners: Vec<_> = daemon
        .threads()
        .into_iter()
        .filter(|(name, _)| name == "disk-owner")
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

    // The setup delta holds the format, so the client's first prepare is empty and
    // appends nothing.
    let formatted = fixture.head(journal).await;
    () = support::prepare_nothing(&mut disk).await;
    assert_eq!(fixture.head(journal).await, formatted);

    () = disk.close().await.unwrap();

    assert!(formatted > 0, "the format of {journal} was not committed");
    assert!(
        (formatted as u64) < support::DEVICE_SIZE / 4,
        "formatting a {} byte device cost {formatted} bytes of journal",
        support::DEVICE_SIZE,
    );

    let (mut disk, mount) = fixture
        .open_disk(&client, journal, Default::default())
        .await;
    () = usable(&mount);

    // The recovery's mount writes are in its setup delta too.
    () = support::prepare_nothing(&mut disk).await;
    () = disk.close().await.unwrap();

    // The reopen appended only its fence and mount writes. A second format would cost
    // as much as the first.
    let recovered = fixture.head(journal).await - formatted;
    assert!(
        recovered < formatted / 4,
        "{journal} grew by {recovered} bytes where its format cost {formatted}: that is \
         a second format and not a recovery",
    );
}

/// User and group which own `path`.
fn owner(path: &std::path::Path) -> (u32, u32) {
    let meta = std::fs::metadata(path).unwrap_or_else(|err| panic!("{path:?}: {err}"));

    (
        std::os::unix::fs::MetadataExt::uid(&meta),
        std::os::unix::fs::MetadataExt::gid(&meta),
    )
}

/// A standby doesn't resume a suspended journal, yet still becomes ready. A partially
/// suspended journal still serves reads, and a fully suspended one has no content.
/// Promotion's fence resumes it.
async fn a_standby_leaves_a_suspended_journal_asleep(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    use broker::journal_spec::suspend;

    let client = daemon.client().await;
    let tree = support::Tree::generation(1);

    // `Now` suspends without waiting for a flush, so the content stays readable.
    let content = "acmeCo/disk/suspended-content";
    let (mut disk, mount) = fixture
        .open_disk(&client, content, Default::default())
        .await;
    () = tree.write(&mount.join("data"));
    _ = support::commit(&mut disk).await;
    () = disk.close().await.unwrap();
    () = fixture
        .suspend(content, broker::append_request::Suspend::Now)
        .await;

    // A journal never appended to holds no content, so it suspends fully.
    let empty = "acmeCo/disk/suspended-empty";
    () = fixture.create_journal(fixture.spec(empty)).await;
    () = fixture
        .suspend(empty, broker::append_request::Suspend::IfFlushed)
        .await;

    // (journal, its suspension, what it holds)
    let cases = [
        (content, suspend::Level::Partial, Some(&tree)),
        (empty, suspend::Level::Full, None),
    ];

    for (journal, suspended, holds) in cases {
        let level = async || fixture.suspension(journal).await.expect("suspended").level;
        assert_eq!(level().await, suspended as i32, "{journal}");

        let mut standby = client.standby(fixture.open(journal).await).await.unwrap();
        () = standby.ready().await.unwrap();
        assert_eq!(
            level().await,
            suspended as i32,
            "the standby of {journal} woke it"
        );

        let (disk, mount) = standby.promote(Default::default()).await.unwrap();
        assert_eq!(
            level().await,
            suspend::Level::None as i32,
            "promotion left {journal} asleep"
        );

        if let Some(tree) = holds {
            () = tree.assert_matches(&mount.join("data"));
        }
        () = disk.close().await.unwrap();
    }
}

/// A daemon killed while it serves a disk leaves an orphan. The kernel stops its
/// device but does not delete it, and its mount stays. The next daemon reaps the
/// orphan: it detaches the mount, removes the mount point, and deletes the device.
async fn the_next_daemon_reaps_an_orphan(fixture: &support::Fixture) {
    let killed = support::Daemon::start(fixture, "reaped").await;
    let client = killed.client().await;
    let (disk, mount) = fixture
        .open_disk(&client, "acmeCo/disk/orphaned", Default::default())
        .await;

    // A mount point names its device.
    let block = mount.file_name().unwrap().to_str().unwrap().to_string();
    let dev_id = block.trim_start_matches("ublkb");
    let block_device = std::path::PathBuf::from(format!("/sys/block/{block}"));
    let char_device = std::path::PathBuf::from(format!("/sys/class/ublk-char/ublkc{dev_id}"));

    let gone = async |path: &std::path::Path| {
        let deadline = std::time::Instant::now() + support::TEARDOWN;
        while path.exists() {
            assert!(std::time::Instant::now() < deadline, "{path:?} remains");
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    };

    // Nothing is in flight, so the kill can't wedge the device.
    () = killed.kill().await;
    drop((disk, client));

    () = gone(&block_device).await;
    assert!(
        char_device.exists(),
        "the kernel deleted the device of a dead server"
    );
    assert!(
        support::is_mounted(&mount),
        "the mount of {block} left with its server"
    );

    let daemon = support::Daemon::start(fixture, "reaped").await;

    () = support::wait_unmounted(&mount).await;
    () = gone(&char_device).await;
    assert!(!mount.exists(), "the reaper left the mount point {mount:?}");
    () = fixture.assert_no_leaks();

    daemon.drain().await;
}

/// `Open` of a nonexistent journal fails as invalid, before any device exists. The
/// daemon never creates journals, so a retry can't succeed.
async fn an_absent_journal_is_refused_at_open(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/never-created";
    let client = daemon.client().await;

    let status = support::expect_invalid(
        client
            .open(fixture.open_absent(journal), Default::default())
            .await,
    )
    .await;

    assert!(status.message().contains("does not exist"), "{status}");
    assert!(
        fixture.listed(journal).await.is_none(),
        "the refused Open created {journal}",
    );
    () = fixture.assert_no_leaks();
}
