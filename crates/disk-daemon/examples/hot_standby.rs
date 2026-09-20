//! A hot standby of a disk, which takes that disk over.
//!
//! One tenure serves the disk. A second tenure opens the same journal without
//! serving it. That tenure rebuilds the disk from the journal and then follows new
//! records, so it holds an image which is current. It does not fence the journal, so
//! the first tenure goes on writing.
//!
//! The first tenure then prepares a change and never acknowledges it. The standby
//! holds those records instead of applying them. The first tenure disappears, the
//! standby promotes, and the disk it serves holds the last acknowledged change and
//! nothing after it.
//!
//! Nothing reports the records a standby holds, so this asserts the outcome of
//! holding them and not the act.
//!
//! ```text
//! examples/demo-services.sh start
//! cargo run -p disk-daemon --example hot_standby
//! examples/demo-services.sh stop
//! ```

mod common;

use anyhow::Context;
use disk_daemon::client::Client;
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
    let client = Client::connect(&daemon_socket).await?;
    let (mut primary, mnt_path) = client.open(open.clone(), Vec::new()).await?;

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

    // The daemon reports this once the replay has read the history of the journal and
    // follows it. The standby is hot from here: promoting it costs a fence, the
    // records which arrive after that fence, and a mount.
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

    // The primary disappears without ending its tenure, which is what a failover
    // looks like from here.
    drop(primary);

    let (promoted, mnt_path) = standby.promote(Vec::new()).await?;

    assert_eq!(std::fs::read(mnt_path.join("state"))?, SECOND);
    assert!(
        !mnt_path.join("uncommitted").exists(),
        "the promoted disk holds a change which no acknowledgement covers",
    );

    () = promoted.close().await?;
    _ = common::delete_journals(&journal_client, &journal).await?;

    Ok(())
}
