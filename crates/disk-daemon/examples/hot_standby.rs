//! Promote a standby after the primary leaves a prepared delta unacknowledged.
//! The standby follows committed state without fencing the primary. On promotion,
//! it must serve the last committed contents and discard the uncommitted delta.
//! The example checks that result; it cannot observe the standby's internal buffer.
//!
//! ```text
//! mise exec -- crates/disk-daemon/examples/demo-services.sh  # in another terminal
//! cargo run -p disk-daemon --example hot_standby
//! ```

mod common;

use disk_daemon::client;
use disk_daemon::proto;

const FIRST: &[u8] = b"the first committed change";
const SECOND: &[u8] = b"the second committed change";
const UNCOMMITTED: &[u8] = b"a change which no acknowledgement covers";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let (daemon_socket, broker) = common::config()?;
    let journal_client = common::journal_client(&broker)?;
    let journal = format!("{}/hot-standby", common::prefix()?);

    _ = common::create_journal(&journal_client, common::journal_spec(&journal)).await?;

    let open = proto::Open {
        journal: journal.clone(),
        device_size: 128 << 20, // 128 MiB
    };

    // For simplicity, this one daemon serves both the primary and the standby. In a
    // real deployment they run on different hosts, so that losing one host doesn't
    // take down both.
    let client = client::Client::connect(&daemon_socket).await?;
    let (mut primary, mnt_path) = client.open(open.clone(), bytes::Bytes::new()).await?;

    () = std::fs::write(mnt_path.join("state"), FIRST)?;

    _ = primary.prepare().await?;
    () = primary.acknowledge().await?;

    // `acknowledge` leaves the commit in flight, and the standby below replays what
    // that commit writes, so this waits for the daemon to confirm it.
    () = primary.acknowledged().await?;

    let mut standby = client.standby(open).await?;

    // Wait for the standby to reach the head observed at Open.
    () = standby.ready().await?;

    () = std::fs::write(mnt_path.join("state"), SECOND)?;

    _ = primary.prepare().await?;
    () = primary.acknowledge().await?;

    // A change the primary prepares and never acknowledges. Its records reach the
    // journal, so the standby reads them, but nothing commits them.
    () = std::fs::write(mnt_path.join("uncommitted"), UNCOMMITTED)?;
    _ = primary.prepare().await?;

    // Drop the primary without acknowledging its prepared delta.
    drop(primary);

    let (promoted, mnt_path) = standby.promote(bytes::Bytes::new()).await?;

    assert_eq!(std::fs::read(mnt_path.join("state"))?, SECOND);
    assert!(
        !mnt_path.join("uncommitted").exists(),
        "the promoted disk holds a change which no acknowledgement covers",
    );

    () = promoted.close().await?;
    () = common::delete_journals(&journal_client, &journal).await?;

    Ok(())
}
