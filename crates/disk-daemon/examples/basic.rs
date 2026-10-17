//! The smallest use of a disk.
//!
//! It writes one file and commits it. Closing the attachment removes the device, the
//! mount, and the local image. Opening the same journal again rebuilds the disk, and
//! the file is still there.
//!
//! ```text
//! examples/demo-services.sh start
//! cargo run -p disk-daemon --example basic
//! examples/demo-services.sh stop
//! ```

mod common;

use anyhow::Context;
use disk_daemon::client;
use disk_daemon::proto;

const CONTENT: &[u8] = b"a disk whose durable state lives in a journal";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let (daemon_socket, broker) = common::config()?;
    let journal_client = common::journal_client(&broker)?;
    let journal = format!("{}/basic", common::prefix()?);

    _ = common::create_journal(&journal_client, common::journal_spec(&journal)).await?;

    let open = proto::Open {
        journal: journal.clone(),
        device_size: 128 << 20,
    };

    // First, open the disk and write a file to its mounted filesystem. Preparing
    // and then acknowledging the change commits it to the journal.
    let client = client::Client::connect(&daemon_socket).await?;
    let (mut disk, mnt_path) = client.open(open.clone(), None).await?;

    () = std::fs::write(mnt_path.join("greeting"), CONTENT)?;

    let ack = disk
        .prepare()
        .await?
        .context("the write changed the disk")?;
    () = disk.acknowledge(ack).await?;

    // Then close the disk, which unmounts it and removes its local device and
    // image. The journal now holds the only copy of the file.
    () = disk.close().await?;

    // Opening the same journal again rebuilds the disk, and the file is read back.
    let (disk, mnt_path) = client.open(open, None).await?;

    assert_eq!(std::fs::read(mnt_path.join("greeting"))?, CONTENT);

    () = disk.close().await?;
    () = common::delete_journals(&journal_client, &journal).await?;

    Ok(())
}
