mod support;

use proto_gazette::broker;

#[tokio::test]
async fn disk_lifecycle() {
    let fixture = support::Fixture::start().await;
    let daemon = support::Daemon::start(&fixture, "lifecycle").await;

    a_disk_which_is_never_written_recovers_its_format(&fixture, &daemon).await;
    an_unchanged_transaction_appends_nothing(&fixture, &daemon).await;
    a_mount_belongs_to_its_client(&fixture, &daemon).await;
    a_suspended_journal_with_content_is_resumed_and_recovered(&fixture, &daemon).await;
    an_absent_journal_is_refused_at_open(&fixture, &daemon).await;
    an_unrecoverable_journal_is_terminal(&fixture, &daemon).await;

    daemon.drain().await;
    fixture.stop().await;
}

/// A disk which is formatted, mounted, and never written owes its client nothing, on
/// its first tenure or any later one: the daemon commits what its own `mkfs` and mount
/// wrote before it hands over the mount. A second tenure recovers that filesystem
/// rather than formatting a second one over it.
async fn a_disk_which_is_never_written_recovers_its_format(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/untouched";
    let client = daemon.client().await;

    let empty = |mount: &std::path::Path| {
        assert_eq!(
            std::fs::read_dir(mount)
                .expect("the mount")
                .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
                .collect::<Vec<_>>(),
            vec!["lost+found"],
            "{mount:?} is not a usable empty filesystem",
        );
    };

    let (mut disk, mount) = client
        .open(fixture.open(journal).await, Vec::new())
        .await
        .unwrap();

    () = empty(&mount);

    // The bootstrap commit already took the format, so the client's own first
    // transaction owes nothing at all.
    () = support::cut_nothing(&mut disk).await;
    () = disk.close().await.unwrap();

    let formatted = fixture.head(journal).await;
    assert!(formatted > 0, "the format of {journal} was not committed");

    let (mut disk, mount) = client
        .open(fixture.open(journal).await, Vec::new())
        .await
        .unwrap();

    () = empty(&mount);

    // A recovered disk's mount writes too, and the daemon committed that as well, so
    // the client owes nothing on a reopen either.
    () = support::cut_nothing(&mut disk).await;
    () = disk.close().await.unwrap();

    // The second tenure appended its fence and what its own mount wrote. A tenure
    // which had formatted again would have committed a whole filesystem over this.
    let recovered = fixture.head(journal).await - formatted;
    assert!(
        recovered < formatted / 4,
        "{journal} grew by {recovered} bytes where its format cost {formatted}: that is \
         a second format and not a recovery",
    );
}

/// A transaction which changed nothing prepares no acknowledgement and appends nothing.
async fn an_unchanged_transaction_appends_nothing(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/unchanged";
    let client = daemon.client().await;

    let (mut disk, mount) = client
        .open(fixture.open(journal).await, Vec::new())
        .await
        .unwrap();

    () = support::Tree::generation(1).write(&mount.join("data"));
    _ = support::commit(&mut disk).await;

    let head = fixture.head(journal).await;

    () = support::cut_nothing(&mut disk).await;
    assert_eq!(fixture.head(journal).await, head);

    () = disk.close().await.unwrap();
}

/// The mount a tenure returns belongs to the client which opened it, whether the
/// daemon formatted it or rebuilt it, so a client needs no privilege of its own.
async fn a_mount_belongs_to_its_client(fixture: &support::Fixture, daemon: &support::Daemon) {
    let journal = "acmeCo/disk/unprivileged";
    let client = daemon.client().await;

    let ours = owner(fixture.dir.path());
    assert_ne!(ours.0, 0, "this suite must not run as root");

    let (mut disk, mount) = client
        .open(fixture.open(journal).await, Vec::new())
        .await
        .unwrap();

    assert_eq!(
        owner(&mount),
        ours,
        "{mount:?} does not belong to this test"
    );

    // A write, so that the reopen below is a recovery and not another format.
    () = std::fs::write(
        mount.join("unprivileged"),
        b"written by an unprivileged client",
    )
    .unwrap();
    _ = support::commit(&mut disk).await;
    () = disk.close().await.unwrap();

    let (disk, mount) = client
        .open(fixture.open(journal).await, Vec::new())
        .await
        .unwrap();

    assert_eq!(
        owner(&mount),
        ours,
        "{mount:?} does not belong to this test"
    );
    () = disk.close().await.unwrap();
}

/// User and group which own `path`.
fn owner(path: &std::path::Path) -> (u32, u32) {
    let meta = std::fs::metadata(path).unwrap_or_else(|err| panic!("{path:?}: {err}"));

    (
        std::os::unix::fs::MetadataExt::uid(&meta),
        std::os::unix::fs::MetadataExt::gid(&meta),
    )
}

/// A journal suspended over committed content cannot be proved empty, so the next open
/// wakes it and recovers the disk from it.
async fn a_suspended_journal_with_content_is_resumed_and_recovered(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/suspended-content";
    let client = daemon.client().await;
    let tree = support::Tree::generation(1);

    let (mut disk, mount) = client
        .open(fixture.open(journal).await, Vec::new())
        .await
        .unwrap();

    () = tree.write(&mount.join("data"));
    _ = support::commit(&mut disk).await;
    () = disk.close().await.unwrap();

    // `Now` suspends over content without waiting for an idle flush interval.
    () = fixture
        .suspend(journal, broker::append_request::Suspend::Now)
        .await;

    let suspend = fixture.suspension(journal).await.expect("suspended");
    assert_ne!(
        suspend.level,
        broker::journal_spec::suspend::Level::None as i32
    );
    assert!(suspend.offset > 0);

    let (disk, mount) = client
        .open(fixture.open(journal).await, Vec::new())
        .await
        .unwrap();

    () = tree.assert_matches(&mount.join("data"));

    let suspend = fixture
        .suspension(journal)
        .await
        .expect("a suspension record");
    assert_eq!(
        suspend.level,
        broker::journal_spec::suspend::Level::None as i32,
        "the open did not resume it"
    );
    () = disk.close().await.unwrap();
}

/// A journal nothing created is terminal at the `Open`, before any device exists.
/// The daemon creates none, so no retry of that `Open` could ever find one.
async fn an_absent_journal_is_refused_at_open(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/never-created";
    let client = daemon.client().await;

    let status =
        support::expect_invalid(client.open(fixture.open_absent(journal), Vec::new()).await).await;

    assert!(status.message().contains("does not exist"), "{status}");
    assert!(
        !fixture.exists(journal).await,
        "the refused Open created {journal}",
    );
    () = fixture.assert_no_leaks();
}

/// A journal whose live spec a disk could not be recovered from is terminal. The
/// daemon refuses rather than fixes it, because that journal belongs to whoever
/// applied it.
async fn an_unrecoverable_journal_is_terminal(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/unrecoverable";
    let client = daemon.client().await;

    () = fixture
        .create_journal(with_retention(fixture.spec(journal)))
        .await;

    let status =
        support::expect_invalid(client.open(fixture.open_absent(journal), Vec::new()).await).await;

    assert!(status.message().contains("retention"), "{status}");
}

/// `spec` with a fragment retention, which a disk cannot be recovered from: Gazette
/// deletes by age, and age cannot see the recovery floor.
fn with_retention(mut spec: broker::JournalSpec) -> broker::JournalSpec {
    spec.fragment.as_mut().unwrap().retention = Some(std::time::Duration::from_secs(86400).into());
    spec
}
