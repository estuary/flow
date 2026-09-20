//! Demonstrates how to coordinate a transaction across several disk-daemon disks.
//!
//! The program writes a pending change to three disks, then uses a fourth disk as a
//! transaction log to record that only the first two changes should be kept. It closes
//! every disk before telling the three participants the outcome, simulating an
//! interruption at that point in the transaction.
//!
//! After reopening the disks, the program reads the recorded decision and uses it to
//! recover the first two changes and discard the third.
//!
//! ```text
//! examples/demo-services.sh start
//! cargo run -p disk-daemon --example two_phase_commit
//! examples/demo-services.sh stop
//! ```

mod common;

use anyhow::Context;
use disk_daemon::client::Client;
use disk_daemon::proto;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let (daemon_socket, broker) = common::config()?;
    let journal_client = common::journal_client(&broker)?;
    let client = Client::connect(&daemon_socket).await?;

    // Each open below names a journal of its own, and a recovery its own
    // acknowledgements. The daemon creates no journal, so the example creates each.
    let open = proto::Open {
        journal: String::new(),
        device_size: 128 << 20,
    };

    let prefix = common::prefix()?;
    let recovery_log = format!("{prefix}/log");
    let parts: Vec<String> = (1..=3)
        .map(|part| format!("{prefix}/part-{part}"))
        .collect();
    let committed = parts.len() - 1;

    for journal in parts.iter().chain(std::iter::once(&recovery_log)) {
        _ = common::create_journal(&journal_client, common::journal_spec(journal)).await?;
    }

    // Phase one. Each participant prepares a delta.
    let mut prepared = Vec::new();

    for journal in &parts {
        let (mut disk, mnt_path) = client
            .open(
                proto::Open {
                    journal: journal.clone(),
                    ..open.clone()
                },
                Vec::new(),
            )
            .await?;

        assert_eq!(
            common::floor_of(&journal_client, journal).await?,
            None,
            "a fresh disk stores no recovery floor",
        );
        () = std::fs::write(mnt_path.join("payload"), payload(journal))?;
        let ack = disk
            .prepare()
            .await?
            .with_context(|| format!("{journal} prepared nothing"))?;
        prepared.push((ack, disk));
    }

    // The decision. One commit of the log disk stores the acknowledgements this
    // transaction covers. That commit is atomic, and it decides the transaction.
    let (mut log_disk, mnt_path) = client
        .open(
            proto::Open {
                journal: recovery_log.clone(),
                ..open.clone()
            },
            Vec::new(),
        )
        .await?;

    for (index, (ack, _disk)) in prepared.iter().take(committed).enumerate() {
        () = std::fs::write(mnt_path.join(format!("ack-{index}")), ack)?;
    }
    let ack = log_disk
        .prepare()
        .await?
        .context("the log disk prepared nothing")?;
    () = log_disk.acknowledge(ack).await?;

    // The commit is left in flight by `acknowledge`, and the decision is not durable
    // until the daemon confirms it. Everything below reads what that commit left.
    () = log_disk.acknowledged().await?;

    assert_eq!(
        common::floor_of(&journal_client, &recovery_log).await?,
        None,
        "one delta completes no recovery horizon, so it stores no floor",
    );

    // No participant was told to commit, so all three deltas are in doubt. Ending a
    // tenure leaves the journal as a disk's only state.
    for (_cut, disk) in prepared {
        () = disk.close().await?;
    }
    () = log_disk.close().await?;

    // Phase two. The log disk rebuilds from its journal, and the decision with it.
    let (log_disk, mnt_path) = client
        .open(
            proto::Open {
                journal: recovery_log.clone(),
                ..open.clone()
            },
            Vec::new(),
        )
        .await?;

    for (index, journal) in parts.iter().enumerate() {
        let ack = std::fs::read(mnt_path.join(format!("ack-{index}"))).ok();

        assert_eq!(
            ack.is_some(),
            index < committed,
            "the recovered decision covers exactly the first {committed} participants",
        );
        let acks = Vec::from_iter(ack.clone().map(bytes::Bytes::from));
        let (disk, mnt_path) = client
            .open(
                proto::Open {
                    journal: journal.clone(),
                    ..open.clone()
                },
                acks,
            )
            .await?;

        let recovered = std::fs::read(mnt_path.join("payload"));

        match ack {
            Some(_) => assert!(
                recovered.is_ok_and(|found| found == payload(journal)),
                "{journal} did not recover the files of its committed delta",
            ),
            None => assert!(
                recovered.is_err(),
                "{journal} kept a delta which no decision covers",
            ),
        }
        () = disk.close().await?;
    }
    () = log_disk.close().await?;

    let deleted = common::delete_journals(&journal_client, &prefix).await?;

    assert_eq!(
        deleted,
        parts.len() + 1,
        "every journal this example created is deleted again",
    );

    Ok(())
}

/// A megabyte which differs for each disk, so a wrong recovery is visible.
fn payload(journal: &str) -> Vec<u8> {
    let seed = journal.bytes().fold(0u8, u8::wrapping_add);

    (0..1 << 20)
        .map(|byte| (byte as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}
