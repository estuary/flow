mod support;

use disk_daemon::client;

#[tokio::test]
async fn disk_failover() {
    let fixture = support::Fixture::start().await;
    let daemon = support::Daemon::start(&fixture, "failover").await;

    repeated_failover_hands_the_disk_along(&fixture, &daemon).await;
    a_standby_promotes_across_fences_it_never_saw(&fixture, &daemon).await;

    daemon.drain().await;
    fixture.stop().await;
}

/// A standby appends nothing while it tails the primary, and promotes onto exactly
/// what the primary committed. It drops an unacknowledged delta and a write synced
/// after it. This repeats three times, so later standbys also replay the fences and
/// rolled-back deltas of earlier failovers.
async fn repeated_failover_hands_the_disk_along(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/handed-along";
    let client = daemon.client().await;

    let (mut primary, mut mount) = fixture
        .open_disk(&client, journal, Default::default())
        .await;

    let mut committed = support::Tree::generation(1);
    () = committed.write(&mount.join("data"));
    _ = support::commit(&mut primary).await;

    for generation in 2..=4u8 {
        let head = fixture.head(journal).await;

        let mut standby = client.standby(fixture.open(journal).await).await.unwrap();
        () = standby.ready().await.unwrap();

        // An unpromoted standby appends nothing.
        assert_eq!(fixture.head(journal).await, head);

        committed = support::Tree::generation(generation);
        () = committed.write(&mount.join("data"));
        _ = support::commit(&mut primary).await;

        () = std::fs::write(mount.join("in-doubt"), b"a delta nothing acknowledged").unwrap();
        _ = support::prepare(&mut primary).await;

        // Synced, but never appended, since the prepare is never acknowledged.
        () = std::fs::write(
            mount.join("behind-the-prepare"),
            b"a write behind a prepare",
        )
        .unwrap();
        () = rustix::fs::syncfs(std::fs::File::open(&mount).unwrap()).unwrap();

        drop(primary);
        () = fixture.wait_for_teardown().await;

        let (promoted, promoted_mount) = standby.promote(Default::default()).await.unwrap();

        () = committed.assert_matches(&promoted_mount.join("data"));
        for held in ["in-doubt", "behind-the-prepare"] {
            assert!(
                !promoted_mount.join(held).exists(),
                "the promoted disk holds {held}",
            );
        }
        (primary, mount) = (promoted, promoted_mount);
    }
    () = primary.close().await.unwrap();
}

/// A standby decides fresh versus recovered when it promotes, not when it opens, and
/// promotes across fences it never saw.
///
/// The standby opens on an empty journal. Two attachments then commit in turn, and
/// the second is still primary when the standby promotes. The standby's fence
/// displaces it, and replay reads through to both commits. The displaced attachment
/// fails as fenced at its next prepare.
async fn a_standby_promotes_across_fences_it_never_saw(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/standby-fenced";
    let client = daemon.client().await;

    let mut standby = client.standby(fixture.open(journal).await).await.unwrap();
    () = standby.ready().await.unwrap();

    // An unpromoted standby appends nothing.
    assert_eq!(fixture.head(journal).await, 0);

    let (mut first, mount) = fixture
        .open_disk(&client, journal, Default::default())
        .await;

    let mut committed = support::Tree::generation(1);
    () = committed.write(&mount.join("data"));
    _ = support::commit(&mut first).await;
    () = first.close().await.unwrap();

    // A second attachment installs an author the standby never saw.
    let (mut second, second_mount) = fixture
        .open_disk(&client, journal, Default::default())
        .await;

    committed = committed.with("second", b"committed by the attachment between".to_vec());
    () = std::fs::write(
        second_mount.join("data/second"),
        committed.content("second"),
    )
    .unwrap();
    _ = support::commit(&mut second).await;

    let (standby, mount) = standby.promote(Default::default()).await.unwrap();
    () = committed.assert_matches(&mount.join("data"));

    // A new file stays in the page cache until the prepare syncs it. Rewriting an
    // existing file would write back on close (ext4's `auto_da_alloc`), so the fenced
    // append would fail at once and tear the disk down under later writes.
    () = std::fs::write(second_mount.join("fenced-out"), b"written past the fence").unwrap();
    let err = second
        .prepare()
        .await
        .expect_err("a fenced attachment cannot commit");

    assert!(matches!(err, client::Error::Fenced(_)), "{err:?}");
    assert!(!err.is_transient(), "{err:?}");

    drop(second);
    () = standby.close().await.unwrap();
}
