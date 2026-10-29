mod support;

/// Payload size of each two-phase commit participant.
const PAYLOAD: usize = 1 << 20; // 1 MiB

#[tokio::test]
async fn disk_durability() {
    let fixture = support::Fixture::start().await;
    let daemon = support::Daemon::start(&fixture, "durability").await;

    posix_mutations_recover_as_the_client_left_them(&fixture, &daemon).await;

    // Each journal names its crash point, so a panic identifies the case.
    for (journal, progress, expect) in [
        ("acmeCo/disk/second-unprepared", Progress::Unprepared, 1),
        ("acmeCo/disk/second-prepared", Progress::Prepared, 1),
        ("acmeCo/disk/second-stored", Progress::Stored, 2),
        ("acmeCo/disk/second-acknowledged", Progress::Acknowledged, 2),
        ("acmeCo/disk/second-committed", Progress::Committed, 2),
    ] {
        () = an_interrupted_second_transaction(&fixture, &daemon, journal, progress, expect).await;
    }

    an_ack_intent_of_lost_state_is_refused(&fixture, &daemon).await;
    a_write_behind_a_prepared_delta_waits_for_its_ack(&fixture, &daemon).await;
    a_transaction_decided_by_a_log_disk(&fixture, &daemon).await;
    a_commit_left_in_flight_is_awaited_by_whatever_comes_next(&fixture, &daemon).await;
    a_prepare_during_writeback_recovers_a_mountable_filesystem(&fixture, &daemon).await;
    an_ext4_error_fails_the_next_prepare(&fixture, &daemon).await;

    daemon.drain().await;
    fixture.stop().await;
}

/// Common filesystem mutations recover as committed. A second round is prepared and
/// never acknowledged, and none of it survives, including its deletions.
async fn posix_mutations_recover_as_the_client_left_them(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/mutations";
    let client = daemon.client().await;

    let (mut disk, mount) = fixture
        .open_disk(&client, journal, Default::default())
        .await;

    let data = mount.join("data");
    let first = support::Tree::generation(1).with("zero-length", Vec::new());

    () = first.write(&data);
    std::fs::create_dir_all(data.join("empty/kept")).unwrap();
    std::fs::create_dir(data.join("removed-dir")).unwrap();
    _ = support::commit(&mut disk).await;

    // Zero blocks encode as no data, so holes are easy to lose.
    let sparse = std::fs::File::create(data.join("sparse")).unwrap();
    () = sparse.set_len(4 * support::BLOCK as u64).unwrap();
    () = std::os::unix::fs::FileExt::write_all_at(
        &sparse,
        &support::pattern(7, support::BLOCK),
        2 * support::BLOCK as u64,
    )
    .unwrap();
    drop(sparse);

    let overwritten = support::pattern(9, 5 * support::BLOCK);

    () = std::fs::write(data.join("one-block"), &overwritten).unwrap();
    () = std::fs::File::options()
        .write(true)
        .open(data.join("large"))
        .unwrap()
        .set_len(support::BLOCK as u64 + 3)
        .unwrap();
    () = std::fs::remove_file(data.join("all-zeroes")).unwrap();
    () = std::fs::rename(data.join("small"), data.join("nested/moved")).unwrap();

    std::fs::remove_dir(data.join("removed-dir")).unwrap();
    std::fs::create_dir(data.join("empty/committed")).unwrap();

    let committed = first
        .clone()
        .with("one-block", overwritten)
        .truncated("large", support::BLOCK + 3)
        .without("all-zeroes")
        .renamed("small", "nested/moved")
        .with(
            "sparse",
            [
                vec![0; 2 * support::BLOCK],
                support::pattern(7, support::BLOCK),
                vec![0; support::BLOCK],
            ]
            .concat(),
        );

    _ = support::commit(&mut disk).await;
    () = committed.assert_matches(&data);

    // Second round: prepared, never acknowledged.
    std::fs::remove_dir_all(data.join("empty")).unwrap();
    std::fs::create_dir(data.join("uncommitted-dir")).unwrap();
    std::fs::remove_file(data.join("zero-length")).unwrap();
    () = std::fs::remove_file(data.join("nested/deep")).unwrap();
    () = std::fs::rename(data.join("one-block"), data.join("renamed-again")).unwrap();
    () = std::fs::write(data.join("added"), support::pattern(11, 2 * support::BLOCK)).unwrap();

    _ = support::prepare(&mut disk).await;
    drop(disk);
    () = fixture.wait_for_teardown().await;

    let (disk, mount) = fixture
        .open_disk(&client, journal, Default::default())
        .await;

    () = committed.assert_matches(&mount.join("data"));
    assert!(mount.join("data/empty/kept").is_dir());
    assert!(mount.join("data/empty/committed").is_dir());
    assert!(!mount.join("data/removed-dir").exists());
    assert!(!mount.join("data/uncommitted-dir").exists());
    () = disk.close().await.unwrap();
}

/// How far the second transaction got before its attachment was dropped.
enum Progress {
    Unprepared,
    Prepared,
    Stored,
    Acknowledged,
    Committed,
}

/// Interrupt generation two at `progress`, then recover generation `expect`.
/// Generations differ in content, so a partly discarded delta is visible.
///
/// Recover twice with the same ACK intent. Both must match, so recovery is
/// repeatable and re-acknowledging a committed delta changes nothing.
async fn an_interrupted_second_transaction(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
    journal: &str,
    progress: Progress,
    expect: u8,
) {
    let client = daemon.client().await;

    let (mut disk, mount) = fixture
        .open_disk(&client, journal, Default::default())
        .await;

    () = support::Tree::generation(1).write(&mount.join("data"));
    _ = support::commit(&mut disk).await;

    () = support::Tree::generation(2).write(&mount.join("data"));

    // Each arm yields the ACK intent to recover with.
    let carried = match progress {
        Progress::Unprepared => {
            // Synced to the device, but never prepared.
            () = rustix::fs::syncfs(std::fs::File::open(&mount).unwrap()).unwrap();
            bytes::Bytes::new()
        }
        Progress::Prepared => {
            _ = support::prepare(&mut disk).await;
            bytes::Bytes::new()
        }
        Progress::Stored => support::prepare(&mut disk).await,
        Progress::Acknowledged => {
            let ack_intent = support::prepare(&mut disk).await;

            () = disk.acknowledge().await.unwrap();
            ack_intent
        }
        Progress::Committed => {
            _ = support::commit(&mut disk).await;
            bytes::Bytes::new()
        }
    };

    drop(disk);
    () = fixture.wait_for_teardown().await;

    for _ in 0..2 {
        let (disk, mount) = fixture.open_disk(&client, journal, carried.clone()).await;

        () = support::Tree::generation(expect).assert_matches(&mount.join("data"));
        () = disk.close().await.unwrap();
    }
}

/// An ACK intent for a journal with no committed state means that state was lost,
/// e.g. the journal was recreated empty. Promotion refuses it before it fences,
/// rather than format a fresh disk under the client's checkpoint.
async fn an_ack_intent_of_lost_state_is_refused(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let client = daemon.client().await;

    let (mut disk, mount) = fixture
        .open_disk(&client, "acmeCo/disk/lost-original", Default::default())
        .await;

    () = support::Tree::generation(1).write(&mount.join("data"));
    let ack_intent = support::prepare(&mut disk).await;

    drop(disk);
    () = fixture.wait_for_teardown().await;

    // A second journal stands in for the original, recreated empty.
    let status = support::expect_invalid(
        client
            .open(fixture.open("acmeCo/disk/lost-recreated").await, ack_intent)
            .await,
    )
    .await;

    assert!(status.message().contains("has no content"), "{status}");
    () = fixture.assert_no_leaks();
}

/// A write synced after a prepare parks at the device until the acknowledgement
/// lands. So the write's records follow the acknowledgement, and recovering the
/// acknowledged delta excludes them.
async fn a_write_behind_a_prepared_delta_waits_for_its_ack(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/write-behind-prepared";
    let client = daemon.client().await;

    let (mut disk, mount) = fixture
        .open_disk(&client, journal, Default::default())
        .await;
    let tree = support::Tree::generation(1);
    () = tree.write(&mount.join("data"));
    let ack_intent = support::prepare(&mut disk).await;

    // Far more than the recording channel holds.
    let mut writer = support::spawn_writer(&mount.join("parked"), 8 << 20, true);
    () = support::wait_for_parked_writes(&mount).await;

    () = disk.acknowledge().await.unwrap();
    () = disk.acknowledged().await.unwrap();
    () = support::wait_for_writer(&mut writer).await;
    () = disk.close().await.unwrap();

    let (disk, mount) = fixture.open_disk(&client, journal, ack_intent).await;

    () = tree.assert_matches(&mount.join("data"));
    assert!(
        !mount.join("parked").exists(),
        "a write which was never prepared recovered"
    );
    () = disk.close().await.unwrap();
}

/// Two-phase commit with a log disk as coordinator. Three participants prepare, and
/// the log disk commits the ACK intents of the first two. Then every attachment is
/// dropped, leaving all three deltas in doubt.
///
/// Only the recovered log records which deltas committed. The first two participants
/// recover with its ACK intents. The third recovers only its setup delta. Each then
/// commits again, so a re-acknowledged delta must compose with the next commit.
async fn a_transaction_decided_by_a_log_disk(fixture: &support::Fixture, daemon: &support::Daemon) {
    let log = "acmeCo/disk/coordinator";
    let client = daemon.client().await;

    let participants: Vec<String> = (0..3)
        .map(|part| format!("acmeCo/disk/participant-{part}"))
        .collect();
    let decided = 2;

    // Phase one: each participant prepares.
    let mut prepared = Vec::new();

    for journal in &participants {
        let (mut disk, mount) = fixture
            .open_disk(&client, journal, Default::default())
            .await;

        () = payload(journal).write(&mount.join("data"));

        prepared.push((support::prepare(&mut disk).await, disk));
    }

    // The decision: one commit of the log disk.
    let (mut log_disk, mount) = fixture.open_disk(&client, log, Default::default()).await;

    for (index, (ack_intent, _disk)) in prepared.iter().take(decided).enumerate() {
        () = std::fs::write(mount.join(format!("ack-{index}")), ack_intent).unwrap();
    }
    _ = support::commit(&mut log_disk).await;

    // No participant was told to commit.
    drop((prepared, log_disk));
    () = fixture.wait_for_teardown().await;

    // Phase two: recover each participant as the log decided.
    let (log_disk, mount) = fixture.open_disk(&client, log, Default::default()).await;

    for (index, journal) in participants.iter().enumerate() {
        let path = mount.join(format!("ack-{index}"));

        let ack_intent = match std::fs::read(&path) {
            Ok(ack_intent) => Some(bytes::Bytes::from(ack_intent)),
            // Absent means undecided.
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => panic!("reading {path:?}: {err}"),
        };

        assert_eq!(
            ack_intent.is_some(),
            index < decided,
            "the recovered decision covers exactly the first {decided} participants",
        );

        let (mut disk, mount) = fixture
            .open_disk(&client, journal, ack_intent.unwrap_or_default())
            .await;

        if index < decided {
            () = payload(journal).assert_matches(&mount.join("data"));
        } else {
            assert!(
                !mount.join("data").exists(),
                "{journal} kept a delta which no decision covers",
            );
        }

        let next = payload(journal).with("after", b"the transaction which followed".to_vec());

        () = next.write(&mount.join("data"));
        _ = support::commit(&mut disk).await;
        () = disk.close().await.unwrap();

        let (disk, mount) = fixture
            .open_disk(&client, journal, Default::default())
            .await;

        () = next.assert_matches(&mount.join("data"));
        () = disk.close().await.unwrap();
    }
    () = log_disk.close().await.unwrap();
}

/// A participant's tree, seeded by its journal name, so a disk holding another
/// participant's delta fails the comparison.
fn payload(journal: &str) -> support::Tree {
    let seed = journal.bytes().fold(0u8, u8::wrapping_add);

    support::Tree::empty().with("payload", support::pattern(seed, PAYLOAD))
}

/// After `acknowledge`, the next `acknowledged`, `prepare`, or `close` waits for the
/// commit to land.
async fn a_commit_left_in_flight_is_awaited_by_whatever_comes_next(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/in-flight";
    let client = daemon.client().await;

    let (mut disk, mount) = fixture
        .open_disk(&client, journal, Default::default())
        .await;

    let mut tree = support::Tree::generation(1);
    () = tree.write(&mount.join("data"));

    _ = support::prepare(&mut disk).await;
    () = disk.acknowledge().await.unwrap();

    // A second `acknowledged` has nothing to wait for, and the next prepare is empty.
    () = disk.acknowledged().await.unwrap();
    () = disk.acknowledged().await.unwrap();
    () = support::prepare_nothing(&mut disk).await;

    // A second transaction, whose commit only `close` waits for.
    tree = tree.with("closed", b"committed as the attachment closed".to_vec());
    () = std::fs::write(mount.join("data/closed"), tree.content("closed")).unwrap();

    _ = support::prepare(&mut disk).await;
    () = disk.acknowledge().await.unwrap();
    () = disk.close().await.unwrap();

    let (disk, mount) = fixture
        .open_disk(&client, journal, Default::default())
        .await;

    () = tree.assert_matches(&mount.join("data"));
    () = disk.close().await.unwrap();
}

/// A delta prepared during writeback recovers to a filesystem which mounts and holds
/// every earlier commit. The file being written is not checked: it was never flushed.
async fn a_prepare_during_writeback_recovers_a_mountable_filesystem(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/writeback";
    let client = daemon.client().await;
    let committed = support::Tree::generation(1);

    let (mut disk, mount) = fixture
        .open_disk(&client, journal, Default::default())
        .await;

    () = committed.write(&mount.join("data"));
    _ = support::commit(&mut disk).await;

    // No `fsync`, so the prepare races ext4's own writeback.
    let mut writer = support::spawn_writer(&mount.join("churn"), 48 << 20, false);
    () = tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    _ = support::commit(&mut disk).await;

    () = support::wait_for_writer(&mut writer).await;
    drop(disk);
    () = fixture.wait_for_teardown().await;

    let (mut disk, mount) = fixture
        .open_disk(&client, journal, Default::default())
        .await;

    () = committed.assert_matches(&mount.join("data"));

    // Fails if a failed ext4 journal replay left the mount read-only.
    () = std::fs::write(mount.join("data/after"), b"written onto a recovered disk").unwrap();
    _ = support::commit(&mut disk).await;

    () = disk.close().await.unwrap();
}

/// An ext4 error stops its writes, including the client's writes still in the page
/// cache. The next `Prepare` fails rather than commit a delta missing them. A reopen
/// recovers the last commit and commits normally.
async fn an_ext4_error_fails_the_next_prepare(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/ext4-error";
    let client = daemon.client().await;
    let committed = support::Tree::generation(1);

    let (mut disk, mount) = fixture
        .open_disk(&client, journal, Default::default())
        .await;

    () = committed.write(&mount.join("data"));
    _ = support::commit(&mut disk).await;

    () = support::Tree::generation(2).write(&mount.join("data"));

    // The mount point and the ext4 sysfs directory are both named for the device.
    let device = mount.file_name().unwrap().to_str().unwrap();
    _ = support::sudo(&[
        "sh",
        "-c",
        &format!("echo injected by a test > /sys/fs/ext4/{device}/trigger_fs_error"),
    ]);

    let err = disk.prepare().await.unwrap_err();
    assert!(err.to_string().contains("ext4 recorded an error"), "{err}");

    drop(disk);
    () = fixture.wait_for_teardown().await;

    let (mut disk, mount) = fixture
        .open_disk(&client, journal, Default::default())
        .await;

    () = committed.assert_matches(&mount.join("data"));
    _ = support::commit(&mut disk).await;

    () = disk.close().await.unwrap();
}
