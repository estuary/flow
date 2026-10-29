mod support;

use proto_gazette::broker;

/// Deltas to churn before giving up on a horizon completing. These thresholds need a
/// handful.
const CHURN_DELTAS: usize = 20;

#[tokio::test]
async fn disk_pruning() {
    // Brokers re-list the fragment store every second, so they soon drop a deleted
    // fragment.
    let fixture = support::Fixture::start_refreshing(1).await;

    // The defaults open a horizon only after a gigabyte of journal.
    let daemon = support::Daemon::start_with(
        &fixture,
        "pruning",
        &[
            "--horizon-open-ratio",
            "0.1",
            "--horizon-copy-ratio",
            "1.0",
            "--horizon-minimum-bytes",
            "1048576",
        ],
    )
    .await;

    // The second case prunes the first case's journal.
    let journal = "acmeCo/disk/floors";
    let floor = a_recovery_installs_the_floors_its_replay_found(&fixture, &daemon, journal).await;
    fragments_below_the_stored_floor_can_be_deleted(&fixture, &daemon, journal, floor).await;

    daemon.drain().await;
    fixture.stop().await;
}

/// Sustained traffic completes horizons, and each completing delta's acknowledgement
/// stores its floor. The floor only advances, and is always a candidate's record,
/// since recovery replays from it.
///
/// The case then removes the label, as a primary which failed before storing it would.
/// Replay finds a completed horizon and a later one still open. The recovered device's
/// first chunk names the replayed floor, which restores the label. Completing the open
/// horizon then stores its opener as the floor, which is returned.
async fn a_recovery_installs_the_floors_its_replay_found(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
    journal: &str,
) -> u64 {
    let client = daemon.client().await;
    let committed = support::Tree::generation(1);

    let (mut disk, mount) = fixture
        .open_disk(&client, journal, Default::default())
        .await;

    () = committed.write(&mount.join("data"));
    _ = support::commit(&mut disk).await;

    // Churn until a candidate lies past the stored floor. The first commit writes
    // megabytes, so it may already complete a horizon. A commit stores the floor of any
    // horizon it completes before `acknowledged` returns, so a candidate past the
    // stored floor opened the horizon still open.
    let mut stored = None;
    let mut found = None;

    for _ in 0..CHURN_DELTAS {
        let current = fixture.stored_floor(journal).await;
        assert!(
            current >= stored,
            "the floor moved from {stored:?} to {current:?}"
        );
        stored = current;

        if let Some(floor) = current {
            let records = fixture.records_from(journal, floor).await;
            assert!(
                records[0].1.opens_horizon,
                "the floor {floor} of {journal} is no candidate's record"
            );

            let open: Vec<u64> = records[1..]
                .iter()
                .filter(|(_, record)| record.opens_horizon)
                .map(|(at, _)| *at)
                .collect();

            match open[..] {
                [] => (),
                [candidate] => {
                    found = Some((floor, candidate));
                    break;
                }
                _ => panic!("{journal} holds candidates {open:?} past its floor {floor}"),
            }
        }
        // Earns copy budget, but clears few of the horizon's pending blocks itself.
        () = std::fs::write(mount.join("churn"), support::pattern(0x5a, 1 << 20)).unwrap();
        _ = support::commit(&mut disk).await;
    }
    let (floor, candidate) =
        found.unwrap_or_else(|| panic!("no horizon of {journal} opened past a stored floor"));
    () = disk.close().await.unwrap();

    // As a primary which failed after its commit but before storing the floor.
    () = fixture.plant_floor(journal, None).await;

    let (mut disk, mount) = fixture
        .open_disk(&client, journal, Default::default())
        .await;
    () = committed.assert_matches(&mount.join("data"));

    // The reopen's mount wrote the first chunks served, which named the replayed floor,
    // and its setup delta stored it. Those few chunks can't complete the open horizon.
    assert_eq!(
        fixture.stored_floor(journal).await,
        Some(floor),
        "the recovery left {journal} without the floor its replay found"
    );

    // No other horizon opens while one is open, so the next floor is its opener.
    let mut advanced = None;

    for _ in 0..CHURN_DELTAS {
        () = std::fs::write(mount.join("churn"), support::pattern(0x5a, 1 << 20)).unwrap();
        _ = support::commit(&mut disk).await;

        let current = fixture.stored_floor(journal).await;
        assert!(
            current >= Some(floor),
            "the floor moved from {floor} to {current:?}"
        );
        if current > Some(floor) {
            advanced = current;
            break;
        }
    }
    assert_eq!(
        advanced,
        Some(candidate),
        "the horizon {journal}'s replay left open did not install its opener"
    );
    () = disk.close().await.unwrap();

    candidate
}

/// A pruner may delete fragments wholly below the stored floor. Recovery reads from
/// the floor, so it never needs them.
async fn fragments_below_the_stored_floor_can_be_deleted(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
    journal: &str,
    floor: u64,
) {
    let client = daemon.client().await;
    let committed = support::Tree::generation(1);

    // Wait until each fragment lists a backing store. Until then a broker serves it
    // from a local spool, which deleting from the store won't remove.
    let below = fragments_below(fixture, journal, floor, |below| {
        !below.is_empty()
            && below
                .iter()
                .all(|fragment| !fragment.backing_store.is_empty())
    })
    .await;

    for fragment in &below {
        let dir = fixture.fragment_root().join(&fragment.journal);

        // Persisted fragments are named `{begin:016x}-{end:016x}-{digest}{extension}`,
        // so the offsets alone identify the file.
        let prefix = format!("{:016x}-{:016x}", fragment.begin, fragment.end);

        let entry = std::fs::read_dir(&dir)
            .expect("reading a fragment directory")
            .map(|entry| entry.expect("reading a fragment directory entry"))
            .find(|entry| entry.file_name().to_string_lossy().starts_with(&prefix))
            .unwrap_or_else(|| panic!("no file in {dir:?} holds the fragment {prefix}"));

        () = std::fs::remove_file(entry.path()).expect("removing a fragment");
    }

    // Wait for the brokers to re-list the store and drop the deleted fragments.
    _ = fragments_below(fixture, journal, floor, |below| below.is_empty()).await;

    let (disk, mount) = fixture
        .open_disk(&client, journal, Default::default())
        .await;

    () = committed.assert_matches(&mount.join("data"));
    assert!(
        fixture.stored_floor(journal).await >= Some(floor),
        "the recovery moved the floor back",
    );
    () = disk.close().await.unwrap();
}

/// Poll the brokers' listing of `journal`'s fragments ending at or below `floor` until
/// `ready` holds of it. A pruner works from this listing.
async fn fragments_below(
    fixture: &support::Fixture,
    journal: &str,
    floor: u64,
    ready: impl Fn(&[broker::Fragment]) -> bool,
) -> Vec<broker::Fragment> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);

    loop {
        let listed = fixture
            .client
            .list_fragments(broker::FragmentsRequest {
                journal: journal.to_string(),
                ..Default::default()
            })
            .await
            .expect("listing the fragments of a journal");
        assert_eq!(listed.next_page_token, 0, "{journal} lists on one page");

        let below: Vec<broker::Fragment> = listed
            .fragments
            .into_iter()
            .filter_map(|fragment| fragment.spec)
            .filter(|fragment| fragment.end <= floor as i64)
            .collect();

        if ready(&below) {
            return below;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the brokers list {below:?} below the floor {floor} of {journal}",
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}
