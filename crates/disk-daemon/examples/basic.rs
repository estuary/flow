//! The smallest use of a disk.
//!
//! It writes one file and commits it. Closing the attachment removes the device, the
//! mount, and the local image. Opening the same journal again rebuilds the disk, and
//! the file is still there.
//!
//! ```text
//! mise exec -- crates/disk-daemon/examples/demo-services.sh  # in another terminal
//! cargo run -p disk-daemon --example basic
//! ```

mod common;

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
        device_size: 128 << 20, // 128 MiB
    };

    // First, open the disk and write a file to its mounted filesystem. Preparing
    // and then acknowledging the change commits it to the journal.
    let client = client::Client::connect(&daemon_socket).await?;
    let (mut disk, mnt_path) = client.open(open.clone(), bytes::Bytes::new()).await?;

    () = std::fs::write(mnt_path.join("greeting"), CONTENT)?;

    _ = disk.prepare().await?;
    () = disk.acknowledge().await?;

    // Then close the disk, which unmounts it and removes its local device and
    // image. The journal now holds the only copy of the file.
    () = disk.close().await?;

    // Opening the same journal again rebuilds the disk, and the file is read back.
    let (disk, mnt_path) = client.open(open, bytes::Bytes::new()).await?;

    assert_eq!(std::fs::read(mnt_path.join("greeting"))?, CONTENT);

    () = disk.close().await?;
    () = common::delete_journals(&journal_client, &journal).await?;

    Ok(())
}
