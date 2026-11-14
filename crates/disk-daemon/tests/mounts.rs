mod support;

use disk_daemon::proto;
use proto::{request, response};

#[tokio::test]
async fn disk_mounts() {
    let fixture = support::Fixture::start().await;
    let daemon = support::Daemon::start(&fixture, "mounts").await;

    a_disk_mounts_for_its_client(&fixture, &daemon).await;
    a_mount_belongs_to_its_clients_uid(&fixture, &daemon).await;
    a_disk_mounts_once(&fixture, &daemon).await;
    an_unsupported_mount_is_refused(&fixture, &daemon).await;
    the_next_daemon_clears_what_a_killed_one_mounted(&fixture).await;

    daemon.drain().await;
    fixture.stop().await;
}

/// The daemon mounts a disk its client formatted, adding `nodev` and `nosuid`.
/// Writes through the mount commit without the client syncing, since `Prepare`
/// syncs the filesystem. Closing the disk unmounts it and removes its directory.
async fn a_disk_mounts_for_its_client(fixture: &support::Fixture, daemon: &support::Daemon) {
    let client = daemon.client().await;
    let (mut disk, device) = fixture
        .open_disk(&client, "acmeCo/disk/mounted", Default::default())
        .await;
    assert!(support::mkfs(&device).status().unwrap().success());

    let root = disk.mount("ext4", "noatime,discard").await.expect("mount");

    assert_eq!(
        root,
        support::Daemon::mount_dir(fixture, "mounts")
            .join(device.file_name().unwrap())
            .join("fs")
    );
    let options = support::mount_options(&root).expect("the disk is mounted");
    for option in ["nodev", "nosuid", "noatime", "discard"] {
        assert!(
            options.iter().any(|o| o == option),
            "{option} of {options:?}"
        );
    }

    () = std::fs::write(root.join("note"), b"written through the mount").unwrap();
    let ack_intent = support::commit(&mut disk).await;

    () = disk.close().await.expect("close");
    assert!(
        !support::is_mounted(&root),
        "a closed disk is still mounted"
    );
    assert!(
        !root.parent().unwrap().exists(),
        "a closed disk's directory remains"
    );
    () = support::wait_for_device_removal(&device).await;

    // Closing wrote the filesystem's remaining changes to the device only, so
    // the note is on the reopened disk only if it committed.
    let (mut disk, device) = fixture
        .open_disk(&client, "acmeCo/disk/mounted", ack_intent)
        .await;
    let root = disk.mount("ext4", "noatime,discard").await.expect("mount");
    assert_eq!(
        std::fs::read(root.join("note")).unwrap(),
        b"written through the mount"
    );
    () = disk.close().await.expect("close");
    () = support::wait_for_device_removal(&device).await;
}

/// The daemon gives a disk's device and mount to the uid its client connected as,
/// and no other uid can reach the mount.
async fn a_mount_belongs_to_its_clients_uid(fixture: &support::Fixture, daemon: &support::Daemon) {
    const CLIENT: u32 = 65534;
    const OTHER: u32 = 65533;

    let open = fixture.open("acmeCo/disk/owned").await;
    let mut attachment = support::Attachment::open(daemon.connect_as(CLIENT).await).await;

    let replies = attachment
        .exchange(vec![
            request::Request::Open(open),
            request::Request::Promote(proto::Promote::default()),
        ])
        .await
        .expect("promote");
    let [
        response::Response::Opened(_),
        response::Response::Promoted(proto::Promoted { device_path, .. }),
    ] = replies.as_slice()
    else {
        panic!("unexpected replies {replies:?}");
    };
    let device = std::path::PathBuf::from(device_path);
    assert_eq!(
        std::os::unix::fs::MetadataExt::uid(&std::fs::metadata(&device).unwrap()),
        CLIENT
    );

    // A bare `root_owner` gives the filesystem's root to the formatting uid.
    let mut mkfs = std::process::Command::new("mkfs.ext4");
    mkfs.args(["-q", "-F", "-b", "4096", "-E", "root_owner"])
        .arg(&device);
    assert!(
        support::run_as(CLIENT, &mkfs),
        "the client can't format its device"
    );

    let mount = request::Request::Mount(proto::Mount {
        fstype: "ext4".to_string(),
        options: "noatime".to_string(),
    });
    let replies = attachment.exchange(vec![mount]).await.expect("mount");
    let [response::Response::Mounted(proto::Mounted { path })] = replies.as_slice() else {
        panic!("unexpected replies {replies:?}");
    };
    let root = std::path::PathBuf::from(path);

    let mut touch = std::process::Command::new("touch");
    touch.arg(root.join("note"));
    assert!(
        support::run_as(CLIENT, &touch),
        "the client can't write to its mount"
    );
    let mut stat = std::process::Command::new("stat");
    stat.arg(&root);
    assert!(
        !support::run_as(OTHER, &stat),
        "another uid reached the mount"
    );

    () = attachment.close().await;
    assert!(
        !root.parent().unwrap().exists(),
        "a closed disk's directory remains"
    );
}

/// A second `Mount` is refused, which fails its attachment.
async fn a_disk_mounts_once(fixture: &support::Fixture, daemon: &support::Daemon) {
    let client = daemon.client().await;
    let (mut disk, device) = fixture
        .open_disk(&client, "acmeCo/disk/mounted-twice", Default::default())
        .await;
    assert!(support::mkfs(&device).status().unwrap().success());
    let _root = disk.mount("ext4", "noatime").await.expect("mount");

    let err = disk.mount("ext4", "noatime").await.unwrap_err();
    let disk_daemon::client::Error::Invalid(status) = err else {
        panic!("expected a refusal, got {err:?}");
    };
    assert_eq!(status.code(), tonic::Code::FailedPrecondition, "{status}");

    () = support::wait_for_device_removal(&device).await;
}

/// A filesystem type which ignores the device is refused, as is a filesystem's
/// own option other than `discard`. Each would mount without the refusal.
async fn an_unsupported_mount_is_refused(fixture: &support::Fixture, daemon: &support::Daemon) {
    let client = daemon.client().await;

    for (journal, fstype, options) in [
        ("acmeCo/disk/mounted-tmpfs", "tmpfs", ""),
        ("acmeCo/disk/mounted-panicky", "ext4", "errors=panic"),
    ] {
        let (mut disk, device) = fixture
            .open_disk(&client, journal, Default::default())
            .await;
        assert!(support::mkfs(&device).status().unwrap().success());

        let err = disk.mount(fstype, options).await.unwrap_err();
        let disk_daemon::client::Error::Invalid(status) = err else {
            panic!("expected a refusal of {fstype} with {options:?}, got {err:?}");
        };
        assert_eq!(status.code(), tonic::Code::InvalidArgument, "{status}");

        () = support::wait_for_device_removal(&device).await;
    }
}

/// A daemon which is killed leaves its mounts behind. The next daemon over the same
/// mount directory clears them before it serves, and reaps their devices.
async fn the_next_daemon_clears_what_a_killed_one_mounted(fixture: &support::Fixture) {
    let killed = support::Daemon::start(fixture, "remounted").await;
    let client = killed.client().await;
    let (mut disk, device) = fixture
        .open_disk(&client, "acmeCo/disk/remounted", Default::default())
        .await;
    assert!(support::mkfs(&device).status().unwrap().success());

    let root = disk.mount("ext4", "noatime").await.expect("mount");
    () = std::fs::write(root.join("note"), b"written before the kill").unwrap();

    // Flushed, so nothing is in flight and the kill can't wedge the device.
    () = rustix::fs::syncfs(std::fs::File::open(&root).unwrap()).unwrap();

    () = killed.kill().await;
    drop((disk, client));
    assert!(
        support::is_mounted(&root),
        "a killed daemon unmounted its disk"
    );

    // The kernel stops a dead server's device on its own, and the reaper then
    // finds it an orphan.
    () = support::wait_for_device_removal(&device).await;

    let daemon = support::Daemon::start(fixture, "remounted").await;
    assert!(!support::is_mounted(&root), "the next daemon left a mount");
    assert!(
        !root.parent().unwrap().exists(),
        "the next daemon left a directory"
    );

    () = fixture.wait_for_teardown().await;
    daemon.drain().await;
}
