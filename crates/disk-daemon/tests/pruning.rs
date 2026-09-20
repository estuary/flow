mod support;

use proto_gazette::broker;

/// Deltas a case runs before it gives up on a horizon completing. Generous against the
/// handful these thresholds need.
const CHURN_DELTAS: usize = 20;

#[tokio::test]
async fn disk_pruning() {
    // Brokers re-list their store every second, so a fragment deleted from it is one no
    // broker still serves from a local spool.
    let fixture = support::Fixture::start_refreshing(1).await;

    // Thresholds small enough that a disk this size completes horizons. The shipped
    // ones open a horizon only after a gigabyte of journal.
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

    fragments_below_the_stored_floor_can_be_deleted(&fixture, &daemon).await;
    a_planted_floor_only_seeks_a_recovery(&fixture, &daemon).await;

    daemon.drain().await;
    fixture.stop().await;
}

/// Sustained traffic completes horizons, and the acknowledgement which completes one
/// stores its floor on the journal. Every fragment below that floor can then be deleted
/// and the disk still recovers from what remains. Twice, because the second tenure
/// resumes the horizon its replay found open, so the floor advances across tenures
/// and never moves back.
async fn fragments_below_the_stored_floor_can_be_deleted(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/horizons";
    let client = daemon.client().await;
    let committed = support::Tree::generation(1);

    let (mut disk, mut mount) = client
        .open(fixture.open(journal).await, Vec::new())
        .await
        .unwrap();

    () = committed.write(&mount.join("data"));
    _ = support::commit(&mut disk).await;
    assert_eq!(fixture.stored_floor(journal).await, None);

    let mut floor = None;

    for _ in 0..2 {
        let mut advanced = None;

        for _ in 0..CHURN_DELTAS {
            // A megabyte of ordinary traffic earns copy budget without discharging much
            // of a horizon itself.
            () = std::fs::write(mount.join("churn"), support::pattern(0x5a, 1 << 20)).unwrap();
            _ = support::commit(&mut disk).await;

            let current = fixture.stored_floor(journal).await;
            assert!(
                current >= floor,
                "the floor moved from {floor:?} to {current:?}"
            );

            if current > floor {
                advanced = current;
                break;
            }
        }
        let advanced =
            advanced.unwrap_or_else(|| panic!("no horizon of {journal} completed past {floor:?}"));
        () = disk.close().await.unwrap();

        // Long enough for the brokers to re-list the store, before and after the deletion.
        () = tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        assert!(delete_fragments_below(fixture, journal, advanced).await > 0);
        () = tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        assert!(
            retained_range(fixture, journal).await < fixture.head(journal).await,
            "the deletion left the whole journal in place",
        );

        // The daemon reads its own floor, which seeks this recovery past what is gone.
        (disk, mount) = client
            .open(fixture.open(journal).await, Vec::new())
            .await
            .unwrap();

        () = committed.assert_matches(&mount.join("data"));
        assert!(
            fixture.stored_floor(journal).await >= Some(advanced),
            "the recovery moved the floor back",
        );
        floor = Some(advanced);
    }
    () = disk.close().await.unwrap();
}

/// A floor the journal carries seeks a recovery and does nothing more. One which is
/// absent, at zero, or above the head costs recovery work and rebuilds the same disk.
/// The last is the dangerous one: a recovery which seeked from it would read nothing
/// and call the disk fresh.
async fn a_planted_floor_only_seeks_a_recovery(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/floored";
    let client = daemon.client().await;
    let committed = support::Tree::generation(1);

    let (mut disk, mount) = client
        .open(fixture.open(journal).await, Vec::new())
        .await
        .unwrap();

    () = committed.write(&mount.join("data"));
    _ = support::commit(&mut disk).await;
    () = disk.close().await.unwrap();

    for floor in [None, Some(0), Some(u64::MAX / 2)] {
        if let Some(floor) = floor {
            () = fixture.store_floor(journal, floor).await;
        }
        let (disk, mount) = client
            .open(fixture.open(journal).await, Vec::new())
            .await
            .unwrap_or_else(|err| panic!("recovering with a floor of {floor:?}: {err}"));

        () = committed.assert_matches(&mount.join("data"));
        () = disk.close().await.unwrap();
    }
}

/// Fragments of `journal` which its brokers list. That listing is what a pruner of a
/// fragment store works from.
async fn fragments(fixture: &support::Fixture, journal: &str) -> Vec<broker::Fragment> {
    let listed = fixture
        .client
        .list_all_fragments(broker::FragmentsRequest {
            journal: journal.to_string(),
            ..Default::default()
        })
        .await
        .expect("listing the fragments of a journal");

    listed
        .fragments
        .into_iter()
        .filter_map(|fragment| fragment.spec)
        .collect()
}

/// Delete every persisted fragment of `journal` which ends at or below `floor`, and
/// report how many.
async fn delete_fragments_below(fixture: &support::Fixture, journal: &str, floor: u64) -> usize {
    let below = fragments(fixture, journal)
        .await
        .into_iter()
        .filter(|fragment| fragment.end <= floor as i64 && !fragment.backing_store.is_empty());

    let mut deleted = 0;

    for fragment in below {
        let dir = fixture.fragment_root().join(&fragment.journal);

        // Gazette names a persisted fragment
        // `{begin:016x}-{end:016x}-{digest}{codec extension}`, so the offsets alone
        // identify the file without either the digest or the extension of its codec.
        let prefix = format!("{:016x}-{:016x}", fragment.begin, fragment.end);

        for entry in std::fs::read_dir(&dir).expect("reading a fragment directory") {
            let entry = entry.expect("reading a fragment directory entry");

            if !entry.file_name().to_string_lossy().starts_with(&prefix) {
                continue;
            }
            () = std::fs::remove_file(entry.path()).expect("removing a fragment");
            deleted += 1;
        }
    }
    deleted
}

/// Journal range a replay would now read, from the earliest fragment the brokers still
/// list through to the write head.
async fn retained_range(fixture: &support::Fixture, journal: &str) -> i64 {
    let begin = fragments(fixture, journal)
        .await
        .into_iter()
        .map(|fragment| fragment.begin)
        .min()
        .expect("a fragment of the journal survived");

    fixture.head(journal).await - begin
}
