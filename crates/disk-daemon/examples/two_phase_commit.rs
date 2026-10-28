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
//! mise exec -- crates/disk-daemon/examples/demo-services.sh  # in another terminal
//! cargo run -p disk-daemon --example two_phase_commit
//! ```

mod common;

use disk_daemon::client;
use disk_daemon::proto;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let (daemon_socket, broker) = common::config()?;
    let journal_client = common::journal_client(&broker)?;
    let client = client::Client::connect(&daemon_socket).await?;

    // Use a separate journal and recovered ACK intent for each disk.
    let open = proto::Open {
        journal: String::new(),
        device_size: 128 << 20, // 128 MiB
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
                bytes::Bytes::new(),
            )
            .await?;

        () = std::fs::write(mnt_path.join("payload"), payload(journal))?;
        let ack_intent = disk.prepare().await?;
        anyhow::ensure!(!ack_intent.is_empty(), "{journal} prepared nothing");
        prepared.push((ack_intent, disk));
    }

    // The decision. One commit of the log disk stores the ACK intents this
    // transaction covers. That commit is atomic, and it decides the transaction.
    let (mut log_disk, mnt_path) = client
        .open(
            proto::Open {
                journal: recovery_log.clone(),
                ..open.clone()
            },
            bytes::Bytes::new(),
        )
        .await?;

    for (index, (ack_intent, _disk)) in prepared.iter().take(committed).enumerate() {
        () = std::fs::write(mnt_path.join(format!("ack-{index}")), ack_intent)?;
    }
    let ack_intent = log_disk.prepare().await?;
    anyhow::ensure!(!ack_intent.is_empty(), "the log disk prepared nothing");
    () = log_disk.acknowledge().await?;

    // The commit is left in flight by `acknowledge`, and the decision is not durable
    // until the daemon confirms it. Everything below reads what that commit left.
    () = log_disk.acknowledged().await?;

    // No participant was told to commit, so all three deltas are in doubt. Closing an
    // attachment leaves the journal as a disk's only state.
    for (_ack_intent, disk) in prepared {
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
            bytes::Bytes::new(),
        )
        .await?;

    // A participant whose ACK intent the decision lacks was not committed.
    for (index, journal) in parts.iter().enumerate() {
        let ack_intent = std::fs::read(mnt_path.join(format!("ack-{index}")))
            .ok()
            .map(bytes::Bytes::from);
        let covered = ack_intent.is_some();

        let (disk, mnt_path) = client
            .open(
                proto::Open {
                    journal: journal.clone(),
                    ..open.clone()
                },
                ack_intent.unwrap_or_default(),
            )
            .await?;

        let recovered = std::fs::read(mnt_path.join("payload"));

        match covered {
            true => assert!(
                recovered.is_ok_and(|found| found == payload(journal)),
                "{journal} did not recover the files of its committed delta",
            ),
            false => assert!(
                recovered.is_err(),
                "{journal} kept a delta which no decision covers",
            ),
        }
        () = disk.close().await?;
    }
    () = log_disk.close().await?;

    () = common::delete_journals(&journal_client, &prefix).await?;

    Ok(())
}

/// A megabyte which differs for each disk, so a wrong recovery is visible.
fn payload(journal: &str) -> Vec<u8> {
    let seed = journal.bytes().fold(0u8, u8::wrapping_add);

    (0..1 << 20)
        .map(|byte| (byte as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}
