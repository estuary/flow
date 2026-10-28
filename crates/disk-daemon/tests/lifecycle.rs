mod support;

use proto_gazette::broker;

#[tokio::test]
async fn disk_lifecycle() {
    let fixture = support::Fixture::start().await;
    let daemon = support::Daemon::start(&fixture, "lifecycle").await;

    a_disk_which_is_never_written_commits_nothing(&fixture, &daemon).await;
    a_standby_leaves_a_suspended_journal_asleep(&fixture, &daemon).await;
    an_absent_journal_is_refused_at_open(&fixture, &daemon).await;

    daemon.drain().await;
    fixture.stop().await;
}

/// A disk which is never written reads as zeroes. Its first prepare is empty and
/// appends nothing, on its first attachment and on a later one. A daemon thread
/// serves the disk as an I/O flusher.
async fn a_disk_which_is_never_written_commits_nothing(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    /// `PF_MEMALLOC_NOIO | PF_LOCAL_THROTTLE`, which `PR_SET_IO_FLUSHER` sets in a
    /// task's `/proc` flags word (see the kernel's `include/linux/sched.h`).
    const IO_FLUSHER: u64 = 0x0008_0000 | 0x0010_0000;

    let journal = "acmeCo/disk/untouched";
    let client = daemon.client().await;

    for attachment in ["first", "second"] {
        let (mut disk, device) = fixture
            .open_disk(&client, journal, Default::default())
            .await;
        () = support::Image::zeroed().assert_matches(&device);

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

        // Reading the device wrote nothing, so the prepare appends nothing.
        let fenced = fixture.head(journal).await;
        () = support::prepare_nothing(&mut disk).await;
        assert_eq!(
            fixture.head(journal).await,
            fenced,
            "the {attachment} attachment's empty prepare appended"
        );

        () = disk.close().await.unwrap();
    }
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
    let writes = support::Writes::generation(1);
    let committed = support::Image::zeroed().with(&writes);

    // `Now` suspends without waiting for a flush, so the content stays readable.
    let content = "acmeCo/disk/suspended-content";
    let (mut disk, device) = fixture
        .open_disk(&client, content, Default::default())
        .await;
    () = writes.write(&device);
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
        (content, suspend::Level::Partial, committed),
        (empty, suspend::Level::Full, support::Image::zeroed()),
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

        let (disk, device) = standby.promote(Default::default()).await.unwrap();
        assert_eq!(
            level().await,
            suspend::Level::None as i32,
            "promotion left {journal} asleep"
        );

        () = holds.assert_matches(&device);
        () = disk.close().await.unwrap();
    }
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
