//! A container writes to a disk, and the journal keeps what it wrote.
//!
//! The daemon gives each mount to the client which opened the disk, so that client
//! can hand the directory to a rootless container as a volume. This opens a disk,
//! lets a `podman` container write a file into it, commits, and then rebuilds the
//! disk from its journal to find the container's file still there.
//!
//! ```text
//! examples/demo-services.sh start
//! cargo run -p disk-daemon --example container
//! examples/demo-services.sh stop
//! ```
//!
//! It needs rootless `podman`, and it pulls `busybox` on its first run.

mod common;

use anyhow::Context;
use disk_daemon::client::Client;
use disk_daemon::proto;

const IMAGE: &str = "docker.io/library/busybox:latest";
const CONTENT: &str = "written by a container, kept by a journal";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let (daemon_socket, broker) = common::config()?;
    let journal_client = common::journal_client(&broker)?;
    let journal = format!("{}/container", common::prefix()?);

    () = common::create_journal(&journal_client, &journal).await?;

    let open = proto::Open {
        journal: journal.clone(),
        device_size: 128 << 20,
        broker: Some(broker.clone()),
        recovered_acks: Vec::new(),
        floor_hint: 0,
    };

    let client = Client::connect(&daemon_socket).await?;
    let (mut disk, mnt_path, _floor) = client.open(open.clone()).await?;

    // Rootless podman maps the container's root onto this process's user, which is
    // the user the daemon gave the mount to. So the container writes as its owner.
    let run = std::process::Command::new("podman")
        .args(["run", "--rm", "--volume"])
        .arg(format!("{}:/disk", mnt_path.display()))
        .args([
            IMAGE,
            "sh",
            "-c",
            &format!("echo '{CONTENT}' > /disk/from-container"),
        ])
        .output()
        .context("spawning podman")?;

    anyhow::ensure!(
        run.status.success(),
        "podman exited with {}: {}",
        run.status,
        String::from_utf8_lossy(&run.stderr),
    );

    let path = mnt_path.join("from-container");
    let written = std::fs::read_to_string(&path).context("the container wrote no file")?;
    let owner = std::fs::metadata(&path)?;

    assert_eq!(written.trim_end(), CONTENT);
    println!("mounted {} into a container as /disk", mnt_path.display());
    println!(
        "the container wrote {}, owned by uid {} gid {}",
        path.display(),
        std::os::unix::fs::MetadataExt::uid(&owner),
        std::os::unix::fs::MetadataExt::gid(&owner),
    );

    let ack = disk
        .prepare()
        .await?
        .context("the container changed the disk")?;
    _ = disk.commit(ack).await?;

    () = disk.close().await?;

    let (disk, mnt_path, _floor) = client.open(open).await?;
    let recovered = std::fs::read_to_string(mnt_path.join("from-container"))
        .context("the journal lost the container's file")?;

    assert_eq!(recovered.trim_end(), CONTENT);
    println!("rebuilt the disk from its journal, and the file reads: {recovered:?}");

    () = disk.close().await?;
    _ = common::delete_journals(&journal_client, &journal).await?;

    Ok(())
}
