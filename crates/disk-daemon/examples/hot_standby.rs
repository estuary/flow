//! Promote a standby after the primary leaves a prepared delta unacknowledged.
//! The standby follows committed state without fencing the primary. On promotion,
//! it must serve the last committed contents and discard the pending delta.
//! The example checks that result; it cannot observe the standby's internal buffer.
//!
//! ```text
//! examples/demo-services.sh start
//! cargo run -p disk-daemon --example hot_standby
//! examples/demo-services.sh stop
//! ```

mod common;

use anyhow::Context;
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

    // The daemon creates no journal, so the example creates its own before it opens.
    _ = common::create_journal(&journal_client, common::journal_spec(&journal)).await?;

    let open = proto::Open {
        journal: journal.clone(),
        device_size: 128 << 20,
    };

    // One connection carries both tenures. A deployment runs them on separate hosts,
    // so that a host which fails takes only one of the two.
    let client = client::Client::connect(&daemon_socket).await?;
    let (mut primary, mnt_path) = client.open(open.clone(), Default::default()).await?;

    () = std::fs::write(mnt_path.join("state"), FIRST)?;

    let ack = primary
        .prepare()
        .await?
        .context("the write changed the disk")?;
    () = primary.acknowledge(ack).await?;

    // `acknowledge` leaves the commit in flight, and the standby below replays what
    // that commit writes, so this waits for the daemon to confirm it.
    () = primary.acknowledged().await?;

    let mut standby = client.standby(open).await?;

    // Wait for the standby to reach the head observed at Open.
    () = standby.ready().await?;

    () = std::fs::write(mnt_path.join("state"), SECOND)?;

    let ack = primary
        .prepare()
        .await?
        .context("the write changed the disk")?;
    () = primary.acknowledge(ack).await?;

    // A change the primary prepares and never acknowledges. Its records reach the
    // journal, so the standby reads them, but nothing commits them.
    () = std::fs::write(mnt_path.join("uncommitted"), UNCOMMITTED)?;
    _ = primary
        .prepare()
        .await?
        .context("the write changed the disk")?;

    // End the primary without acknowledging its prepared delta.
    drop(primary);

    let (promoted, mnt_path) = standby.promote(Default::default()).await?;

    assert_eq!(std::fs::read(mnt_path.join("state"))?, SECOND);
    assert!(
        !mnt_path.join("uncommitted").exists(),
        "the promoted disk holds a change which no acknowledgement covers",
    );

    () = promoted.close().await?;
    _ = common::delete_journals(&journal_client, &journal).await?;

    Ok(())
}
