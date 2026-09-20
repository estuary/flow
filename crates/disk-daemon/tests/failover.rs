mod support;

use disk_daemon::client;

#[tokio::test]
async fn disk_failover() {
    let fixture = support::Fixture::start().await;
    let daemon = support::Daemon::start(&fixture, "failover").await;

    repeated_failover_hands_the_disk_along(&fixture, &daemon).await;

    a_standby_of_an_empty_journal_promotes_onto_committed_state(&fixture, &daemon).await;
    a_standby_promotes_across_another_tenures_fence(&fixture, &daemon).await;
    a_replacement_tenure_fences_the_first(&fixture, &daemon).await;

    daemon.drain().await;
    fixture.stop().await;
}

/// A standby appends nothing while it follows the serving tenure, and promotes onto
/// exactly what that tenure committed. A delta cut and never acknowledged, and a write
/// synced behind that cut, are both left behind. Three times over, so a standby also
/// opens against the fences and orphans of the failovers before it.
async fn repeated_failover_hands_the_disk_along(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/handed-along";
    let client = daemon.client().await;

    let (mut serving, mut mount) = client
        .open(fixture.open(journal).await, Vec::new())
        .await
        .unwrap();

    let mut committed = support::Tree::generation(1);
    () = committed.write(&mount.join("data"));
    _ = support::commit(&mut serving).await;

    for generation in 2..=4u8 {
        let head = fixture.head(journal).await;

        let mut standby = client.standby(fixture.open(journal).await).await.unwrap();
        () = standby.ready().await.unwrap();

        // A standby has not claimed the journal, so nothing of its replay reached it.
        assert_eq!(fixture.head(journal).await, head);

        committed = support::Tree::generation(generation);
        () = committed.write(&mount.join("data"));
        _ = support::commit(&mut serving).await;

        () = std::fs::write(mount.join("in-doubt"), b"a delta nothing acknowledged").unwrap();
        _ = support::cut(&mut serving).await;

        // Synced to the device, where the capture channel holds it until the tenure ends.
        () = std::fs::write(mount.join("behind-the-cut"), b"a write behind a cut").unwrap();
        () = support::run("sync", &["-f", mount.to_str().unwrap()]).await;

        drop(serving);
        () = fixture.wait_for_teardown().await;

        let (promoted, promoted_mount) = standby.promote(Vec::new()).await.unwrap();

        () = committed.assert_matches(&promoted_mount.join("data"));
        for held in ["in-doubt", "behind-the-cut"] {
            assert!(
                !promoted_mount.join(held).exists(),
                "the promoted disk holds {held}",
            );
        }
        (serving, mount) = (promoted, promoted_mount);
    }
    () = serving.close().await.unwrap();
}

/// A standby decides fresh versus recovered from what the journal holds when it
/// promotes, not when it opened. A standby of an empty journal reads nothing, so the
/// head it saw stays zero however much a primary commits in the meantime.
async fn a_standby_of_an_empty_journal_promotes_onto_committed_state(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/standby-empty";
    let client = daemon.client().await;

    let mut standby = client.standby(fixture.open(journal).await).await.unwrap();
    () = standby.ready().await.unwrap();

    // A standby has not claimed the journal, so nothing of it reached the journal.
    assert_eq!(fixture.head(journal).await, 0);

    let (mut primary, mount) = client
        .open(fixture.open(journal).await, Vec::new())
        .await
        .unwrap();

    let committed = support::Tree::generation(1);
    () = committed.write(&mount.join("data"));

    _ = support::commit(&mut primary).await;
    () = primary.close().await.unwrap();

    let (standby, mount) = standby.promote(Vec::new()).await.unwrap();

    () = committed.assert_matches(&mount.join("data"));
    () = standby.close().await.unwrap();
}

/// A standby claims the journal against the author it finds when it promotes, not the
/// one its open read. Every fence between the two was another writer's, and a claim
/// against the stale author would fail although this standby is the rightful writer.
async fn a_standby_promotes_across_another_tenures_fence(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/standby-fenced";
    let client = daemon.client().await;

    let (mut first, mount) = client
        .open(fixture.open(journal).await, Vec::new())
        .await
        .unwrap();

    let mut committed = support::Tree::generation(1);
    () = committed.write(&mount.join("data"));
    _ = support::commit(&mut first).await;

    let mut standby = client.standby(fixture.open(journal).await).await.unwrap();
    () = standby.ready().await.unwrap();
    () = first.close().await.unwrap();

    // A second tenure installs an author the standby never saw.
    let (mut second, mount) = client
        .open(fixture.open(journal).await, Vec::new())
        .await
        .unwrap();

    committed = committed.with("second", b"committed by the tenure between".to_vec());
    () = std::fs::write(mount.join("data/second"), committed.content("second")).unwrap();

    _ = support::commit(&mut second).await;
    () = second.close().await.unwrap();

    let (standby, mount) = standby.promote(Vec::new()).await.unwrap();

    () = committed.assert_matches(&mount.join("data"));
    () = standby.close().await.unwrap();
}

/// A tenure which opens a journal holding committed state claims it at once, and the
/// tenure it displaces learns of that at its next append. The loser reports a lost
/// fence, which no retry undoes. The winner holds what the loser committed.
async fn a_replacement_tenure_fences_the_first(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/fenced";
    let client = daemon.client().await;

    let (mut loser, loser_mount) = client
        .open(fixture.open(journal).await, Vec::new())
        .await
        .unwrap();

    let committed = support::Tree::generation(1);
    () = committed.write(&loser_mount.join("data"));
    _ = support::commit(&mut loser).await;

    let (winner, mount) = client
        .open(fixture.open(journal).await, Vec::new())
        .await
        .unwrap();

    () = committed.assert_matches(&mount.join("data"));

    () = support::Tree::generation(2).write(&loser_mount.join("data"));
    let err = loser
        .prepare()
        .await
        .expect_err("a fenced tenure cannot commit");

    assert!(matches!(err, client::Error::Fenced(_)), "{err:?}");
    assert!(!err.is_transient(), "{err:?}");

    drop(loser);
    () = winner.close().await.unwrap();
}
