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

    a_write_behind_a_prepared_delta_waits_for_its_ack(&fixture, &daemon).await;
    a_transaction_decided_by_a_log_disk(&fixture, &daemon).await;
    a_commit_left_in_flight_is_awaited_by_whatever_comes_next(&fixture, &daemon).await;
    a_prepare_during_writeback_recovers_a_mountable_filesystem(&fixture, &daemon).await;
    a_runaway_delta_fails_its_attachment(&fixture, &daemon).await;

    daemon.drain().await;
    fixture.stop().await;
}

/// Common filesystem mutations of ext4 on a disk recover as committed. A second round
/// is prepared and never acknowledged, and none of it survives, including its
/// deletions.
async fn posix_mutations_recover_as_the_client_left_them(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/mutations";
    let client = daemon.client().await;

    let (mut disk, device) = fixture
        .open_disk(&client, journal, Default::default())
        .await;
    let mounted = support::Mounted::format(fixture.dir.path(), &device);

    let data = mounted.path().join("data");
    let first = support::Tree::generation(1).with("zero-length", Vec::new());

    () = first.write(&data);
    std::fs::create_dir_all(data.join("empty/kept")).unwrap();
    std::fs::create_dir(data.join("removed-dir")).unwrap();
    () = mounted.sync();
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

    () = mounted.sync();
    _ = support::commit(&mut disk).await;
    () = committed.assert_matches(&data);

    // Second round: prepared, never acknowledged.
    std::fs::remove_dir_all(data.join("empty")).unwrap();
    std::fs::create_dir(data.join("uncommitted-dir")).unwrap();
    std::fs::remove_file(data.join("zero-length")).unwrap();
    () = std::fs::remove_file(data.join("nested/deep")).unwrap();
    () = std::fs::rename(data.join("one-block"), data.join("renamed-again")).unwrap();
    () = std::fs::write(data.join("added"), support::pattern(11, 2 * support::BLOCK)).unwrap();

    () = mounted.sync();
    _ = support::prepare(&mut disk).await;

    // An unmount would write, and the device serves no write until the prepared delta
    // is acknowledged. So the attachment goes first, and its teardown shuts the
    // filesystem down.
    drop(disk);
    () = fixture.wait_for_teardown_under(mounted).await;

    let (disk, device) = fixture
        .open_disk(&client, journal, Default::default())
        .await;
    let mounted = support::Mounted::mount(fixture.dir.path(), &device);
    let data = mounted.path().join("data");

    () = committed.assert_matches(&data);
    assert!(data.join("empty/kept").is_dir());
    assert!(data.join("empty/committed").is_dir());
    assert!(!data.join("removed-dir").exists());
    assert!(!data.join("uncommitted-dir").exists());

    () = mounted.unmount();
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

    let (mut disk, device) = fixture
        .open_disk(&client, journal, Default::default())
        .await;

    let first = support::Writes::generation(1);
    let second = support::Writes::generation(2);

    () = first.write(&device);
    _ = support::commit(&mut disk).await;

    // Flushed to the device, so `Unprepared` loses writes the device completed.
    () = second.write(&device);

    // Each arm yields the ACK intent to recover with.
    let carried = match progress {
        Progress::Unprepared => bytes::Bytes::new(),
        Progress::Prepared => {
            _ = support::prepare(&mut disk).await;
            bytes::Bytes::new()
        }
        Progress::Stored => support::prepare(&mut disk).await,
        Progress::Acknowledged => {
            let ack_intent = support::prepare(&mut disk).await;

            () = disk.acknowledge();
            ack_intent
        }
        Progress::Committed => {
            _ = support::commit(&mut disk).await;
            bytes::Bytes::new()
        }
    };

    drop(disk);
    () = fixture.wait_for_teardown().await;

    let committed = support::Image::zeroed().with(&first);
    let committed = match expect {
        1 => committed,
        _ => committed.with(&second),
    };

    for _ in 0..2 {
        let (disk, device) = fixture.open_disk(&client, journal, carried.clone()).await;

        () = committed.assert_matches(&device);
        () = disk.close().await.unwrap();
    }
}

/// A write flushed after a prepare parks at the device until the acknowledgement
/// lands. So the write's records follow the acknowledgement, and recovering the
/// acknowledged delta excludes them.
async fn a_write_behind_a_prepared_delta_waits_for_its_ack(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/write-behind-prepared";
    let client = daemon.client().await;

    let (mut disk, device) = fixture
        .open_disk(&client, journal, Default::default())
        .await;
    let writes = support::Writes::generation(1);
    () = writes.write(&device);
    let ack_intent = support::prepare(&mut disk).await;

    // Far more than the recording channel holds.
    let mut writer = support::spawn_writer(&device, 16 << 20, 8 << 20, true);
    () = support::wait_for_parked_writes(&device).await;

    () = disk.acknowledge();
    () = disk.acknowledged().await.unwrap();
    () = support::wait_for_writer(&mut writer).await;
    () = disk.close().await.unwrap();

    let (disk, device) = fixture.open_disk(&client, journal, ack_intent).await;

    () = support::Image::zeroed()
        .with(&writes)
        .assert_matches(&device);
    () = disk.close().await.unwrap();
}

/// Two-phase commit with a log disk as coordinator. Three participants prepare, and
/// the log disk commits the ACK intents of the first two. Then every attachment is
/// dropped, leaving all three deltas in doubt.
///
/// Only the recovered log records which deltas committed. The first two participants
/// recover with its ACK intents. The third recovers to zeroes. Each then commits
/// again, so a re-acknowledged delta must compose with the next commit.
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
        let (mut disk, device) = fixture
            .open_disk(&client, journal, Default::default())
            .await;

        () = payload(journal).write(&device);

        prepared.push((support::prepare(&mut disk).await, disk));
    }

    // The decision: one commit of the log disk, which records each decided ACK
    // intent at its participant's slot.
    let (mut log_disk, device) = fixture.open_disk(&client, log, Default::default()).await;

    let mut decision = support::Writes::new();
    for (index, (ack_intent, _disk)) in prepared.iter().take(decided).enumerate() {
        decision = decision.data(slot(index), framed(ack_intent));
    }
    () = decision.write(&device);
    _ = support::commit(&mut log_disk).await;

    // No participant was told to commit.
    drop((prepared, log_disk));
    () = fixture.wait_for_teardown().await;

    // Phase two: recover each participant as the log decided.
    let (log_disk, log_device) = fixture.open_disk(&client, log, Default::default()).await;
    let log_content = std::fs::read(&log_device).unwrap();

    for (index, journal) in participants.iter().enumerate() {
        let ack_intent = unframed(&log_content[slot(index) as usize..]);

        assert_eq!(
            ack_intent.is_some(),
            index < decided,
            "the recovered decision covers exactly the first {decided} participants",
        );

        let (mut disk, device) = fixture
            .open_disk(&client, journal, ack_intent.unwrap_or_default())
            .await;

        let recovered = match index < decided {
            true => support::Image::zeroed().with(&payload(journal)),
            // No decision covers its delta.
            false => support::Image::zeroed(),
        };
        () = recovered.assert_matches(&device);

        let next =
            support::Writes::new().data(PAYLOAD as u64, b"the transaction which followed".to_vec());

        () = next.write(&device);
        _ = support::commit(&mut disk).await;
        () = disk.close().await.unwrap();

        let (disk, device) = fixture
            .open_disk(&client, journal, Default::default())
            .await;

        () = recovered.with(&next).assert_matches(&device);
        () = disk.close().await.unwrap();
    }
    () = log_disk.close().await.unwrap();
}

/// A participant's payload, seeded by its journal name, so a disk holding another
/// participant's delta fails the comparison.
fn payload(journal: &str) -> support::Writes {
    let seed = journal.bytes().fold(0u8, u8::wrapping_add);

    support::Writes::new().data(0, support::pattern(seed, PAYLOAD))
}

/// Offset of a participant's slot on the log disk.
fn slot(index: usize) -> u64 {
    index as u64 * support::BLOCK as u64
}

/// `ack_intent`, prefixed by its little-endian `u32` length.
fn framed(ack_intent: &[u8]) -> Vec<u8> {
    [&(ack_intent.len() as u32).to_le_bytes()[..], ack_intent].concat()
}

/// The ACK intent `framed` wrote at the start of `slot`, or `None` where the slot
/// is zeroes.
fn unframed(slot: &[u8]) -> Option<bytes::Bytes> {
    let len = u32::from_le_bytes(slot[..4].try_into().unwrap()) as usize;

    (len != 0).then(|| bytes::Bytes::copy_from_slice(&slot[4..4 + len]))
}

/// After `acknowledge`, the next `acknowledged`, `prepare`, or `close` waits for the
/// commit to land.
async fn a_commit_left_in_flight_is_awaited_by_whatever_comes_next(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/in-flight";
    let client = daemon.client().await;

    let (mut disk, device) = fixture
        .open_disk(&client, journal, Default::default())
        .await;

    let first = support::Writes::generation(1);
    () = first.write(&device);

    _ = support::prepare(&mut disk).await;
    () = disk.acknowledge();

    // A second `acknowledged` has nothing to wait for, and the next prepare is empty.
    () = disk.acknowledged().await.unwrap();
    () = disk.acknowledged().await.unwrap();
    () = support::prepare_nothing(&mut disk).await;

    // A second transaction, whose commit only `close` waits for.
    let closed =
        support::Writes::new().data(20 << 20, b"committed as the attachment closed".to_vec());
    () = closed.write(&device);

    _ = support::prepare(&mut disk).await;
    () = disk.acknowledge();
    () = disk.close().await.unwrap();

    let (disk, device) = fixture
        .open_disk(&client, journal, Default::default())
        .await;

    () = support::Image::zeroed()
        .with(&first)
        .with(&closed)
        .assert_matches(&device);
    () = disk.close().await.unwrap();
}

/// A delta prepared during ext4's writeback recovers to a filesystem which mounts and
/// holds every earlier commit. The file being written is not checked: it was never
/// flushed.
async fn a_prepare_during_writeback_recovers_a_mountable_filesystem(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/writeback";
    let client = daemon.client().await;
    let committed = support::Tree::generation(1);

    let (mut disk, device) = fixture
        .open_disk(&client, journal, Default::default())
        .await;
    let mounted = support::Mounted::format(fixture.dir.path(), &device);

    () = committed.write(&mounted.path().join("data"));
    () = mounted.sync();
    _ = support::commit(&mut disk).await;

    // No `fsync`, so the flush and prepare race ext4's own writeback, which breaks the
    // client's pause.
    let mut writer = support::spawn_writer(&mounted.path().join("churn"), 0, 16 << 20, false);
    () = tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    () = mounted.sync();
    _ = support::commit(&mut disk).await;

    () = support::wait_for_writer(&mut writer).await;
    drop(disk);
    () = fixture.wait_for_teardown_under(mounted).await;

    let (mut disk, device) = fixture
        .open_disk(&client, journal, Default::default())
        .await;
    let mounted = support::Mounted::mount(fixture.dir.path(), &device);

    () = committed.assert_matches(&mounted.path().join("data"));

    // Fails if a failed ext4 journal replay left the mount read-only.
    () = std::fs::write(
        mounted.path().join("data/after"),
        b"written onto a recovered disk",
    )
    .unwrap();
    () = mounted.sync();
    _ = support::commit(&mut disk).await;

    () = mounted.unmount();
    () = disk.close().await.unwrap();
}

/// A delta past twice the device size fails its attachment.
async fn a_runaway_delta_fails_its_attachment(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    const DEVICE_SIZE: u64 = 1 << 20; // 1 MiB

    let client = daemon.client().await;
    let open = disk_daemon::proto::Open {
        device_size: DEVICE_SIZE,
        ..fixture.open("acmeCo/disk/runaway-delta").await
    };
    let disk_daemon::client::Promoted {
        mut disk,
        device_path: device,
        ..
    } = client.open(open, Default::default()).await.unwrap();

    // Rewrite the device until three device sizes are written, or the failed
    // attachment's teardown stops the device.
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&device)
        .unwrap();
    let round = vec![0x5a; DEVICE_SIZE as usize];

    for _ in 0..3 {
        let rewrote = std::os::unix::fs::FileExt::write_all_at(&file, &round, 0)
            .and_then(|()| file.sync_data());
        if rewrote.is_err() {
            break;
        }
    }
    drop(file);

    let err = disk.prepare().await.unwrap_err();
    assert!(err.to_string().contains("exceeds the limit"), "{err}");
}
