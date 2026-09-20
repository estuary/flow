mod support;

#[tokio::test]
async fn disk_durability() {
    let fixture = support::Fixture::start().await;
    let daemon = support::Daemon::start(&fixture, "durability").await;

    a_committed_disk_reopens_with_its_contents(&fixture, &daemon).await;
    a_formatted_disk_costs_a_bounded_journal(&fixture, &daemon).await;
    posix_mutations_recover_as_the_client_left_them(&fixture, &daemon).await;

    // Journal names carry the crash point, so a panic in the driver names its variant.
    for (journal, progress, expect) in [
        ("acmeCo/disk/second-uncut", Progress::Uncut, 1),
        ("acmeCo/disk/second-cut", Progress::Cut, 1),
        ("acmeCo/disk/second-stored", Progress::Stored, 2),
        ("acmeCo/disk/second-acknowledged", Progress::Acknowledged, 2),
        ("acmeCo/disk/second-committed", Progress::Committed, 2),
    ] {
        () = an_interrupted_second_transaction(&fixture, &daemon, journal, progress, expect).await;
    }

    an_acknowledgement_lost_after_commit_is_repaired(&fixture, &daemon).await;
    a_recovered_ack_whose_replay_applies_nothing_is_terminal(&fixture, &daemon).await;
    a_commit_left_in_flight_is_awaited_by_whatever_comes_next(&fixture, &daemon).await;
    a_cut_during_writeback_recovers_a_mountable_filesystem(&fixture, &daemon).await;

    daemon.drain().await;
    fixture.stop().await;
}

/// A disk reopens holding what its last tenure committed, over several sequential
/// tenures. Two further recoveries reproduce that same filesystem, so a recovery is
/// repeatable rather than a state each one arrives at differently.
async fn a_committed_disk_reopens_with_its_contents(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/reopened";
    let client = daemon.client().await;
    let mut committed = support::Tree::empty();

    for generation in 1..=3u8 {
        let (mut disk, mount) = client
            .open(fixture.open(journal).await, Vec::new())
            .await
            .unwrap();

        () = committed.assert_matches(&mount.join("data"));

        committed = support::Tree::generation(generation);
        () = committed.write(&mount.join("data"));

        _ = support::commit(&mut disk).await;
        () = disk.close().await.unwrap();
    }

    for _ in 0..2 {
        let (disk, mount) = client
            .open(fixture.open(journal).await, Vec::new())
            .await
            .unwrap();

        () = committed.assert_matches(&mount.join("data"));
        () = disk.close().await.unwrap();
    }
}

/// A fresh disk's journal holds the whole formatted filesystem, because the daemon
/// commits the `mkfs` and mount itself. What that costs is bounded by what the format
/// allocated rather than by the size of the device: a prezeroed format leaves the
/// inode tables and the ext4 journal as holes, which nothing captures.
async fn a_formatted_disk_costs_a_bounded_journal(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/first-write";
    let client = daemon.client().await;

    let (mut disk, mount) = client
        .open(fixture.open(journal).await, Vec::new())
        .await
        .unwrap();

    () = std::fs::write(mount.join("greeting"), b"a disk in a journal").unwrap();
    _ = support::commit(&mut disk).await;
    () = disk.close().await.unwrap();

    let head = fixture.head(journal).await as u64;

    assert!(
        head < support::DEVICE_SIZE / 4,
        "formatting and writing a {} byte device cost {head} bytes of journal",
        support::DEVICE_SIZE,
    );
}

/// Every ordinary way a client changes a filesystem recovers as it left it. A second
/// round of the same mutations is never committed, and none of them survives — not even
/// a deletion, which a recovery could keep by doing nothing.
async fn posix_mutations_recover_as_the_client_left_them(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/mutations";
    let client = daemon.client().await;

    let (mut disk, mount) = client
        .open(fixture.open(journal).await, Vec::new())
        .await
        .unwrap();

    let data = mount.join("data");
    let first = support::Tree::generation(1).with("zero-length", Vec::new());

    () = first.write(&data);
    std::fs::create_dir_all(data.join("empty/kept")).unwrap();
    std::fs::create_dir(data.join("removed-dir")).unwrap();
    _ = support::commit(&mut disk).await;

    // A run of zero blocks encodes as no data, so a hole is the easiest content to lose.
    let sparse = std::fs::File::create(data.join("sparse")).unwrap();
    () = sparse.set_len(4 * support::tree::BLOCK as u64).unwrap();
    () = std::os::unix::fs::FileExt::write_all_at(
        &sparse,
        &support::pattern(7, support::tree::BLOCK),
        2 * support::tree::BLOCK as u64,
    )
    .unwrap();
    drop(sparse);

    let overwritten = support::pattern(9, 5 * support::tree::BLOCK);

    () = std::fs::write(data.join("one-block"), &overwritten).unwrap();
    () = std::fs::File::options()
        .write(true)
        .open(data.join("large"))
        .unwrap()
        .set_len(support::tree::BLOCK as u64 + 3)
        .unwrap();
    () = std::fs::remove_file(data.join("all-zeroes")).unwrap();
    () = std::fs::rename(data.join("small"), data.join("nested/moved")).unwrap();

    std::fs::remove_dir(data.join("removed-dir")).unwrap();
    std::fs::create_dir(data.join("empty/committed")).unwrap();

    let committed = first
        .clone()
        .with("one-block", overwritten)
        .truncated("large", support::tree::BLOCK + 3)
        .without("all-zeroes")
        .renamed("small", "nested/moved")
        .with(
            "sparse",
            [
                vec![0; 2 * support::tree::BLOCK],
                support::pattern(7, support::tree::BLOCK),
                vec![0; support::tree::BLOCK],
            ]
            .concat(),
        );

    _ = support::commit(&mut disk).await;
    () = committed.assert_matches(&data);

    // A second round of mutations, cut and never acknowledged.
    std::fs::remove_dir_all(data.join("empty")).unwrap();
    std::fs::create_dir(data.join("uncommitted-dir")).unwrap();
    std::fs::remove_file(data.join("zero-length")).unwrap();
    () = std::fs::remove_file(data.join("nested/deep")).unwrap();
    () = std::fs::rename(data.join("one-block"), data.join("renamed-again")).unwrap();
    () = std::fs::write(
        data.join("added"),
        support::pattern(11, 2 * support::tree::BLOCK),
    )
    .unwrap();

    _ = support::cut(&mut disk).await;
    drop(disk);
    () = fixture.wait_for_teardown().await;

    let (disk, mount) = client
        .open(fixture.open(journal).await, Vec::new())
        .await
        .unwrap();

    () = committed.assert_matches(&mount.join("data"));
    assert!(mount.join("data/empty/kept").is_dir());
    assert!(mount.join("data/empty/committed").is_dir());
    assert!(!mount.join("data/removed-dir").exists());
    assert!(!mount.join("data/uncommitted-dir").exists());
    () = disk.close().await.unwrap();
}

/// How far the second transaction got before its tenure ended.
enum Progress {
    Uncut,
    Cut,
    Stored,
    Acknowledged,
    Committed,
}

/// A disk commits generation one, writes generation two over it, and takes that second
/// transaction to `progress` before its tenure is lost without a close.
///
/// The reopened disk holds generation `expect` and nothing else. Every generation
/// differs from every other in every file, so a delta which is discarded is discarded
/// whole rather than leaving part of generation two over generation one.
async fn an_interrupted_second_transaction(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
    journal: &str,
    progress: Progress,
    expect: u8,
) {
    let client = daemon.client().await;

    let (mut disk, mount) = client
        .open(fixture.open(journal).await, Vec::new())
        .await
        .unwrap();

    () = support::Tree::generation(1).write(&mount.join("data"));
    _ = support::commit(&mut disk).await;

    () = support::Tree::generation(2).write(&mount.join("data"));

    // Each arm yields what the client carries into its next open.
    let carried = match progress {
        Progress::Uncut => {
            // A `sync` puts the write on the device, where no cut ever reaches it.
            () = support::run("sync", &["-f", mount.to_str().unwrap()]).await;
            Vec::new()
        }
        Progress::Cut => {
            _ = support::cut(&mut disk).await;
            Vec::new()
        }
        Progress::Stored => vec![support::cut(&mut disk).await],
        Progress::Acknowledged => {
            let ack = support::cut(&mut disk).await;

            () = disk.acknowledge(ack.clone()).await.unwrap();
            vec![ack]
        }
        Progress::Committed => {
            _ = support::commit(&mut disk).await;
            Vec::new()
        }
    };

    drop(disk);
    () = fixture.wait_for_teardown().await;

    let (disk, mount) = client
        .open(fixture.open(journal).await, carried)
        .await
        .unwrap();

    () = support::Tree::generation(expect).assert_matches(&mount.join("data"));
    () = disk.close().await.unwrap();
}

/// A client which stored an acknowledgement and failed before it could send it hands
/// that acknowledgement back at its next open, which repairs the delta. Handing the
/// same acknowledgement back again recovers the same disk.
async fn an_acknowledgement_lost_after_commit_is_repaired(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/repaired";
    let client = daemon.client().await;
    let ack = a_lost_first_use(fixture, &client, journal).await;

    for repair in 0..2 {
        let (disk, mount) = client
            .open(fixture.open(journal).await, vec![ack.clone()])
            .await
            .unwrap_or_else(|err| panic!("repair {repair}: {err}"));

        () = support::Tree::generation(1).assert_matches(&mount.join("data"));

        // Nothing commits, so the next repair repeats this one and not a later state.
        () = disk.close().await.unwrap();
    }
}

/// A recovered acknowledgement proves a broker confirmed the data records of its
/// delta, so a journal whose replay applies nothing lost committed state. That is
/// terminal, rather than a fresh disk which hides the loss.
async fn a_recovered_ack_whose_replay_applies_nothing_is_terminal(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/emptied";
    let client = daemon.client().await;

    // Staged, so the journal exists and holds nothing, as an emptied store leaves it.
    // No tenure served this disk, so not even a format was committed into it.
    () = fixture.create_journal(fixture.spec(journal)).await;
    let foreign = a_foreign_ack(fixture, &client, "acmeCo/disk/emptied-donor").await;

    let status = support::expect_invalid(
        client
            .open(fixture.open_absent(journal), vec![foreign])
            .await,
    )
    .await;

    assert!(status.message().contains("applied nothing"), "{status}");

    // The tenure claims, and it repairs an acknowledgement its replay could honor
    // before that replay proves the loss, so both its fence and that acknowledgement
    // are in the journal. Neither is committed state.
    assert!(fixture.head(journal).await > 0);
}

/// A `close` which follows an `acknowledge` still promises that the commit landed, as
/// a `prepare` or an `acknowledged` does.
async fn a_commit_left_in_flight_is_awaited_by_whatever_comes_next(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/in-flight";
    let client = daemon.client().await;

    let (mut disk, mount) = client
        .open(fixture.open(journal).await, Vec::new())
        .await
        .unwrap();

    let mut tree = support::Tree::generation(1);
    () = tree.write(&mount.join("data"));

    let ack = support::cut(&mut disk).await;
    () = disk.acknowledge(ack).await.unwrap();

    // The second `acknowledged` owes nothing, and the `prepare` behind it finds no delta.
    () = disk.acknowledged().await.unwrap();
    () = disk.acknowledged().await.unwrap();
    () = support::cut_nothing(&mut disk).await;

    // A second transaction, whose commit only `close` waits for.
    tree = tree.with("closed", b"committed as the tenure ended".to_vec());
    () = std::fs::write(mount.join("data/closed"), tree.content("closed")).unwrap();

    let ack = support::cut(&mut disk).await;
    () = disk.acknowledge(ack).await.unwrap();
    () = disk.close().await.unwrap();

    let (disk, mount) = client
        .open(fixture.open(journal).await, Vec::new())
        .await
        .unwrap();

    () = tree.assert_matches(&mount.join("data"));
    () = disk.close().await.unwrap();
}

/// A cut taken while the filesystem is writing back rebuilds into a filesystem which
/// mounts, and which holds every transaction that committed before it. The file being
/// written is not part of that promise: those writes were never flushed.
async fn a_cut_during_writeback_recovers_a_mountable_filesystem(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/writeback";
    let client = daemon.client().await;
    let committed = support::Tree::generation(1);

    let (mut disk, mount) = client
        .open(fixture.open(journal).await, Vec::new())
        .await
        .unwrap();

    () = committed.write(&mount.join("data"));
    _ = support::commit(&mut disk).await;

    // There is no `fsync`, so the cut lands amongst ext4's own writeback traffic.
    let mut writer = support::spawn_writer(&mount.join("churn"), 48 << 20, false);
    () = tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    _ = support::commit(&mut disk).await;

    () = support::wait_for_writer(&mut writer).await;
    drop(disk);
    () = fixture.wait_for_teardown().await;

    let (mut disk, mount) = client
        .open(fixture.open(journal).await, Vec::new())
        .await
        .unwrap();

    () = committed.assert_matches(&mount.join("data"));

    // A filesystem mounted read-only after a failed replay could not take this write.
    () = std::fs::write(mount.join("data/after"), b"written onto a recovered disk").unwrap();
    _ = support::commit(&mut disk).await;

    () = disk.close().await.unwrap();
}

/// A first-use delta of generation one, cut and then lost with its tenure. What a
/// case hands back at the next open is its subject, so the acknowledgement is returned.
async fn a_lost_first_use(
    fixture: &support::Fixture,
    client: &disk_daemon::client::Client,
    journal: &str,
) -> bytes::Bytes {
    let (mut disk, mount) = client
        .open(fixture.open(journal).await, Vec::new())
        .await
        .unwrap();

    () = support::Tree::generation(1).write(&mount.join("data"));
    let ack = support::cut(&mut disk).await;

    drop(disk);
    () = fixture.wait_for_teardown().await;

    ack
}

/// A committed acknowledgement of `journal`, for a case which hands it back to some
/// other journal. Real bytes, so the refusal is of that journal and not of the bytes.
async fn a_foreign_ack(
    fixture: &support::Fixture,
    client: &disk_daemon::client::Client,
    journal: &str,
) -> bytes::Bytes {
    let (mut donor, mount) = client
        .open(fixture.open(journal).await, Vec::new())
        .await
        .unwrap();

    () = support::Tree::generation(2).write(&mount.join("data"));
    let ack = support::commit(&mut donor).await;
    () = donor.close().await.unwrap();

    ack
}
