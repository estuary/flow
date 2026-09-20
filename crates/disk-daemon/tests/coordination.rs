mod support;

/// Size of each participant's payload.
const PAYLOAD: usize = 1 << 20;

#[tokio::test]
async fn disk_coordination() {
    let fixture = support::Fixture::start().await;
    let daemon = support::Daemon::start(&fixture, "coordination").await;

    // Journal names carry the crash point, so a panic in the driver names its variant.
    for (log, participants, decision) in [
        (
            "acmeCo/disk/coordinator",
            "acmeCo/disk/participant",
            Decision::Committed,
        ),
        (
            "acmeCo/disk/coordinator-in-doubt",
            "acmeCo/disk/in-doubt-participant",
            Decision::Prepared,
        ),
        (
            "acmeCo/disk/coordinator-unconfirmed",
            "acmeCo/disk/unconfirmed-participant",
            Decision::Acknowledged,
        ),
    ] {
        () = a_transaction_decided_by_a_log_disk(&fixture, &daemon, log, participants, decision)
            .await;
    }

    daemon.drain().await;
    fixture.stop().await;
}

/// How far the coordinator's commit of its decision got before every tenure ended.
enum Decision {
    Committed,
    Prepared,
    Acknowledged,
}

/// Three participants prepare. The coordinator records two of their acknowledgements in
/// its log disk and takes the log's own commit to `decision`. Every tenure then ends.
///
/// The recovered log decides each participant: a recorded acknowledgement repairs its
/// delta, and the participant without one recovers as a fresh disk, its orphaned first
/// use discarded. Each participant then commits again and reopens, to show a repaired
/// disk is an ordinary one.
async fn a_transaction_decided_by_a_log_disk(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
    log: &str,
    participant_prefix: &str,
    decision: Decision,
) {
    let client = daemon.client().await;

    let participants: Vec<String> = (0..3)
        .map(|part| format!("{participant_prefix}-{part}"))
        .collect();
    let decided = 2;

    // Phase one. Each participant prepares a delta.
    let mut prepared = Vec::new();

    for journal in &participants {
        let (mut disk, mount) = client
            .open(fixture.open(journal).await, Vec::new())
            .await
            .unwrap();

        () = payload(journal).write(&mount.join("data"));

        prepared.push((support::cut(&mut disk).await, disk));
    }

    // The decision. Each arm yields what the coordinator carries into its next open.
    let (mut log_disk, mount) = client
        .open(fixture.open(log).await, Vec::new())
        .await
        .unwrap();

    for (index, (ack, _disk)) in prepared.iter().take(decided).enumerate() {
        () = std::fs::write(mount.join(format!("ack-{index}")), ack).unwrap();
    }
    let carried = match decision {
        Decision::Committed => {
            _ = support::commit(&mut log_disk).await;
            Vec::new()
        }
        Decision::Prepared => vec![support::cut(&mut log_disk).await],
        Decision::Acknowledged => {
            let ack = support::cut(&mut log_disk).await;

            () = log_disk.acknowledge(ack.clone()).await.unwrap();
            vec![ack]
        }
    };

    // No participant was told to commit, so all three deltas are in doubt.
    for (_ack, disk) in prepared {
        drop(disk);
    }
    drop(log_disk);
    () = fixture.wait_for_teardown().await;

    // Phase two. The recovered log is the only record of which deltas committed.
    let (log_disk, mount) = client.open(fixture.open(log).await, carried).await.unwrap();

    for (index, journal) in participants.iter().enumerate() {
        let path = mount.join(format!("ack-{index}"));

        let ack = match std::fs::read(&path) {
            Ok(ack) => Some(bytes::Bytes::from(ack)),
            // Absence means undecided. Any other error is an unreadable log disk.
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => panic!("reading {path:?}: {err}"),
        };

        assert_eq!(
            ack.is_some(),
            index < decided,
            "the recovered decision covers exactly the first {decided} participants",
        );

        let (mut disk, mount) = client
            .open(fixture.open(journal).await, Vec::from_iter(ack))
            .await
            .unwrap_or_else(|err| panic!("reopening {journal}: {err}"));

        if index < decided {
            () = payload(journal).assert_matches(&mount.join("data"));
        } else {
            assert!(
                !mount.join("data").exists(),
                "{journal} kept a delta which no decision covers",
            );
        }

        // A repaired delta and the commit after it must compose.
        let next = payload(journal).with("after", b"the transaction which followed".to_vec());

        () = next.write(&mount.join("data"));
        _ = support::commit(&mut disk).await;
        () = disk.close().await.unwrap();

        let (disk, mount) = client
            .open(fixture.open(journal).await, Vec::new())
            .await
            .unwrap();

        () = next.assert_matches(&mount.join("data"));
        () = disk.close().await.unwrap();
    }
    () = log_disk.close().await.unwrap();
}

/// One participant's tree, seeded by its journal name so that a disk holding another
/// participant's delta fails the comparison.
fn payload(journal: &str) -> support::Tree {
    let seed = journal.bytes().fold(0u8, u8::wrapping_add);

    support::Tree::empty().with("payload", support::pattern(seed, PAYLOAD))
}
