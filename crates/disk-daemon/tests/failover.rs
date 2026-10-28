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
/// what the primary committed. It drops an unacknowledged delta and a write flushed
/// after it. This repeats three times, so later standbys also replay the fences and
/// rolled-back deltas of earlier failovers.
async fn repeated_failover_hands_the_disk_along(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/handed-along";
    let client = daemon.client().await;

    let (mut primary, mut device) = fixture
        .open_disk(&client, journal, Default::default())
        .await;

    let first = support::Writes::generation(1);
    let mut committed = support::Image::zeroed().with(&first);

    () = first.write(&device);
    _ = support::commit(&mut primary).await;

    for generation in 2..=4u8 {
        let head = fixture.head(journal).await;

        let mut standby = client.standby(fixture.open(journal).await).await.unwrap();
        () = standby.ready().await.unwrap();

        // An unpromoted standby appends nothing.
        assert_eq!(fixture.head(journal).await, head);

        let writes = support::Writes::generation(generation);
        committed = committed.with(&writes);

        () = writes.write(&device);
        _ = support::commit(&mut primary).await;

        () = support::Writes::new()
            .data(20 << 20, b"a delta nothing acknowledged".to_vec())
            .write(&device);
        _ = support::prepare(&mut primary).await;

        // A client which breaks the pause. Its flushed write waits at the device, and
        // is never appended, since the prepare is never acknowledged.
        let mut writer = support::spawn_writer(&device, 24 << 20, 1 << 20, true);
        () = support::wait_for_parked_writes(&device).await;

        drop(primary);
        () = fixture.wait_for_teardown().await;
        _ = support::abandon_writer(&mut writer).await;

        let (promoted, promoted_device) = standby.promote(Default::default()).await.unwrap();

        () = committed.assert_matches(&promoted_device);
        (primary, device) = (promoted, promoted_device);
    }
    () = primary.close().await.unwrap();
}

/// A standby promotes across fences it never saw.
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

    let (mut first, device) = fixture
        .open_disk(&client, journal, Default::default())
        .await;

    let first_writes = support::Writes::generation(1);
    () = first_writes.write(&device);
    _ = support::commit(&mut first).await;
    () = first.close().await.unwrap();

    // A second attachment installs an author the standby never saw.
    let (mut second, second_device) = fixture
        .open_disk(&client, journal, Default::default())
        .await;

    let second_writes =
        support::Writes::new().data(20 << 20, b"committed by the attachment between".to_vec());
    () = second_writes.write(&second_device);
    _ = support::commit(&mut second).await;

    let (standby, device) = standby.promote(Default::default()).await.unwrap();
    () = support::Image::zeroed()
        .with(&first_writes)
        .with(&second_writes)
        .assert_matches(&device);

    // The record of this small write stays in the displaced writer's buffer, so only
    // the prepare's flush of it fails as fenced.
    () = support::Writes::new()
        .data(24 << 20, b"written past the fence".to_vec())
        .write(&second_device);
    let err = second
        .prepare()
        .await
        .expect_err("a fenced attachment cannot commit");

    assert!(matches!(err, client::Error::Fenced(_)), "{err:?}");
    assert!(!err.is_transient(), "{err:?}");

    drop(second);
    () = standby.close().await.unwrap();
}
