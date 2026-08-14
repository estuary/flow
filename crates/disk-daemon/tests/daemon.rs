//! End-to-end tests of the daemon binary over a real broker.
//!
//! Each case drives `flow-disk-daemon` exactly as it ships: the daemon runs in
//! a `sudo -n` child, because serving a device and mounting a filesystem need
//! `CAP_SYS_ADMIN` and cargo must not run as root, and the test speaks its
//! session gRPC over the Unix socket. Its mounts are root-owned, so file I/O
//! through them is privileged too and runs in `sudo -n` children of its own.
//!
//! A data-plane is expensive to start, so every case works a journal of its own
//! and they share one, along with the daemon.

mod common;

use disk_daemon::proto;
use disk_daemon::{bitmap::Bitmap, chunk};
use gazette::journal::framing;
use proto_gazette::{broker, uuid};

/// 128 MiB, which `mkfs.ext4` accepts comfortably and which keeps a case to a
/// few seconds.
const DEVICE_SIZE: u64 = 128 * 1024 * 1024;
const BLOCK_SIZE: u32 = 4096;

/// How long a teardown which cannot be observed over the session is waited for.
const TEARDOWN: std::time::Duration = std::time::Duration::from_secs(30);

/// Label the daemon under test derives its recovery floor from.
const FLOOR_LABEL: &str = "acmeCo/truncated-at";

#[tokio::test]
async fn disk_daemon_tests() {
    common::check_prerequisites();

    let data_plane = e2e_support::DataPlane::start(e2e_support::DataPlaneArgs { broker_count: 1 })
        .await
        .expect("DataPlane start");

    let fixture = Fixture {
        dir: tempfile::tempdir().expect("tempdir"),
        endpoint: data_plane.gazette.brokers[0].endpoint.clone(),
        credential: credential(&data_plane.gazette),
        client: data_plane.journal_client.clone(),
    };
    let daemon = Daemon::start(&fixture, "shared").await;

    a_committed_disk_replays_into_an_identical_filesystem(&fixture, &daemon).await;
    an_unchanged_transaction_appends_nothing(&fixture, &daemon).await;
    a_broker_replacement_has_no_reply(&fixture, &daemon).await;
    a_disk_which_is_never_written_creates_no_journal(&fixture, &daemon).await;
    the_first_write_publishes_a_snapshot_of_the_formatted_image(&fixture, &daemon).await;
    a_journal_without_a_store_is_terminal(&fixture, &daemon).await;
    protocol_violations_are_terminal(&fixture, &daemon).await;
    a_committed_disk_reopens_with_its_contents(&fixture, &daemon).await;
    an_acknowledgement_lost_after_commit_is_repaired(&fixture, &daemon).await;
    an_uncommitted_delta_is_discarded(&fixture, &daemon).await;
    an_orphaned_first_use_yields_a_fresh_disk(&fixture, &daemon).await;
    the_floor_label_is_only_a_seek_hint(&fixture, &daemon).await;
    a_cut_during_writeback_recovers_a_consistent_filesystem(&fixture, &daemon).await;
    an_abrupt_disconnect_tears_the_disk_down(&fixture, &daemon).await;

    daemon.drain().await;
    fixture.assert_no_leaks();

    a_killed_daemon_leaves_no_mounts(&fixture).await;

    data_plane
        .graceful_stop()
        .await
        .expect("DataPlane graceful_stop");
}

/// Files written through the mount and committed are exactly the files a replay
/// of the journal reproduces, over several sequential transactions.
async fn a_committed_disk_replays_into_an_identical_filesystem(fixture: &Fixture, daemon: &Daemon) {
    let journal = "acmeCo/disk/committed";
    let mut session = daemon.session().await;

    let mount = session.open(fixture.open(journal)).await.unwrap();
    let source = fixture.dir.path().join("source");

    for generation in 1..=3u8 {
        write_source(&source, generation);
        copy_through(&source, &mount);

        let ack = session.publish().await.unwrap();
        assert!(!ack.is_empty(), "generation {generation} changed the disk");

        () = session.commit(ack).await.unwrap();
    }
    () = session.close().await;

    let image = fixture.dir.path().join("replay.img");
    let covered = fixture.replay(journal, &image).await;

    assert!(covered > 0, "the journal replayed no blocks");
    fixture.assert_content_matches(&image, &source);
}

/// A transaction which changed nothing publishes no acknowledgement, and
/// appends nothing to the journal.
async fn an_unchanged_transaction_appends_nothing(fixture: &Fixture, daemon: &Daemon) {
    let journal = "acmeCo/disk/unchanged";
    let mut session = daemon.session().await;

    let mount = session.open(fixture.open(journal)).await.unwrap();
    copy_through(&fixture.source("once"), &mount);

    let ack = session.publish().await.unwrap();
    assert!(!ack.is_empty());
    () = session.commit(ack).await.unwrap();

    let head = fixture.head(journal).await;

    // Nothing touches the disk between the two publications, so the second
    // finds no delta at all and the journal does not move.
    assert!(session.publish().await.unwrap().is_empty());
    assert_eq!(fixture.head(journal).await, head);

    () = session.close().await;
}

/// A broker replacement has no reply, so the next request's reply is its own.
async fn a_broker_replacement_has_no_reply(fixture: &Fixture, daemon: &Daemon) {
    let journal = "acmeCo/disk/refreshed";
    let mut session = daemon.session().await;

    let mount = session.open(fixture.open(journal)).await.unwrap();
    copy_through(&fixture.source("once"), &mount);

    session
        .send(proto::request::Request::Broker(proto::Broker {
            endpoint: fixture.endpoint.clone(),
            credential: fixture.credential.clone(),
        }))
        .await;

    let ack = session.publish().await.unwrap();
    assert!(!ack.is_empty());

    () = session.commit(ack).await.unwrap();
    () = session.close().await;
}

/// A disk which is formatted and mounted but never written creates no journal:
/// it carries no information, because formatting it again reproduces it.
async fn a_disk_which_is_never_written_creates_no_journal(fixture: &Fixture, daemon: &Daemon) {
    let journal = "acmeCo/disk/untouched";
    let mut session = daemon.session().await;

    _ = session.open(fixture.open(journal)).await.unwrap();
    assert!(session.publish().await.unwrap().is_empty());

    () = session.close().await;
    assert!(!fixture.exists(journal).await);
}

/// The first mutation after mount publishes a snapshot of the image, so the
/// first delta holds the whole formatted filesystem and nothing more: the ranges
/// a prezeroed format left as holes are holes in a replay of it too.
async fn the_first_write_publishes_a_snapshot_of_the_formatted_image(
    fixture: &Fixture,
    daemon: &Daemon,
) {
    let journal = "acmeCo/disk/first-write";
    let mut session = daemon.session().await;

    let mount = session.open(fixture.open(journal)).await.unwrap();
    let source = fixture.source("tiny");

    copy_through(&source, &mount);

    let ack = session.publish().await.unwrap();
    () = session.commit(ack).await.unwrap();
    () = session.close().await;

    let image = fixture.dir.path().join("first-write.img");
    let covered = fixture.replay(journal, &image).await;

    // A few small files could not account for a filesystem's metadata, and the
    // replay mounts, which only the snapshot makes possible.
    assert!(
        covered > 512,
        "the first delta covered only {covered} blocks"
    );
    let allocated =
        std::os::unix::fs::MetadataExt::blocks(&std::fs::metadata(&image).unwrap()) * 512;

    assert!(
        allocated < DEVICE_SIZE / 4,
        "the replayed image allocated {allocated} of the device's {DEVICE_SIZE} bytes",
    );
    fixture.assert_content_matches(&image, &source);
}

/// A journal which resolves to no fragment store is terminal, before any device
/// exists.
async fn a_journal_without_a_store_is_terminal(fixture: &Fixture, daemon: &Daemon) {
    let mut session = daemon.session().await;

    let open = proto::Open {
        journal_config: Some(proto::JournalConfig {
            journal: "acmeCo/disk/storeless".to_string(),
            ..Default::default()
        }),
        ..fixture.open("")
    };
    let status = session.open(open).await.unwrap_err();

    assert!(status.message().contains("no fragment store"), "{status}");
    () = session.ended().await;
}

/// Every protocol violation ends its session, and the disk it held goes with it.
async fn protocol_violations_are_terminal(fixture: &Fixture, daemon: &Daemon) {
    // Publishing twice, when the first delta is still owed a commit.
    let mut session = daemon.session().await;
    let mount = session
        .open(fixture.open("acmeCo/disk/twice-published"))
        .await
        .unwrap();

    copy_through(&fixture.source("once"), &mount);
    assert!(!session.publish().await.unwrap().is_empty());

    let status = session.publish().await.unwrap_err();
    assert!(status.message().contains("awaiting its commit"), "{status}");
    () = session.ended().await;

    // Committing with nothing published.
    let mut session = daemon.session().await;
    _ = session
        .open(fixture.open("acmeCo/disk/early-commit"))
        .await
        .unwrap();

    let status = session
        .commit(bytes::Bytes::from_static(b"not an acknowledgement"))
        .await
        .unwrap_err();

    assert!(status.message().contains("no published delta"), "{status}");
    () = session.ended().await;

    // Committing bytes which are not the ones published.
    let mut session = daemon.session().await;
    let mount = session
        .open(fixture.open("acmeCo/disk/wrong-commit"))
        .await
        .unwrap();

    copy_through(&fixture.source("once"), &mount);
    let mut ack = session.publish().await.unwrap().to_vec();
    *ack.last_mut().unwrap() ^= 0xff;

    let status = session.commit(ack.into()).await.unwrap_err();
    assert!(
        status.message().contains("differs from the published one"),
        "{status}",
    );
    () = session.ended().await;

    // Opening a second disk on one session.
    let mut session = daemon.session().await;
    _ = session
        .open(fixture.open("acmeCo/disk/twice-opened"))
        .await
        .unwrap();

    let status = session
        .open(fixture.open("acmeCo/disk/twice-opened"))
        .await
        .unwrap_err();

    assert!(status.message().contains("exactly one disk"), "{status}");
    () = session.ended().await;

    // A request before Open, which every session must begin with.
    let mut session = daemon.session().await;
    let status = session.publish().await.unwrap_err();

    assert!(status.message().contains("must be Open"), "{status}");
    () = session.ended().await;

    fixture.assert_no_leaks();
}

/// A disk which committed reopens holding the files it committed, over several
/// sequential transactions, and two recoveries of one journal agree.
async fn a_committed_disk_reopens_with_its_contents(fixture: &Fixture, daemon: &Daemon) {
    let journal = "acmeCo/disk/reopened";
    let source = fixture.dir.path().join("reopened");

    for generation in 1..=3u8 {
        let mut session = daemon.session().await;
        let mount = session.open(fixture.open(journal)).await.unwrap();

        // Every generation but the first opens a disk rebuilt from the journal,
        // which holds what the generation before it committed.
        if generation != 1 {
            assert_tree_matches(&source, &format!("{mount}/data"));
        }
        write_source(&source, generation);
        copy_through(&source, &mount);

        let ack = session.publish().await.unwrap();
        assert!(!ack.is_empty(), "generation {generation} changed the disk");

        () = session.commit(ack).await.unwrap();
        () = session.close().await;
    }

    // Recovery is deterministic, so recovering twice more without committing
    // anything reproduces the same filesystem both times.
    for _ in 0..2 {
        let mut session = daemon.session().await;
        let mount = session.open(fixture.open(journal)).await.unwrap();

        assert_tree_matches(&source, &format!("{mount}/data"));
        () = session.close().await;
    }
}

/// A client which made an acknowledgement durable and then failed before it
/// could commit hands that acknowledgement back, which repairs the delta.
async fn an_acknowledgement_lost_after_commit_is_repaired(fixture: &Fixture, daemon: &Daemon) {
    let journal = "acmeCo/disk/repaired";
    let mut session = daemon.session().await;

    let mount = session.open(fixture.open(journal)).await.unwrap();
    let source = fixture.source("repaired");

    copy_through(&source, &mount);
    let ack = session.publish().await.unwrap();

    drop(session);
    fixture.wait_for_teardown().await;

    let mut session = daemon.session().await;
    let mount = session
        .open(proto::Open {
            recovered_acks: vec![ack],
            ..fixture.open(journal)
        })
        .await
        .unwrap();

    assert_tree_matches(&source, &format!("{mount}/data"));
    () = session.close().await;
}

/// A delta which was published but never committed is not disk state, so it is
/// discarded and the disk recovers to the transaction before it.
async fn an_uncommitted_delta_is_discarded(fixture: &Fixture, daemon: &Daemon) {
    let journal = "acmeCo/disk/uncommitted";
    let mut session = daemon.session().await;

    let mount = session.open(fixture.open(journal)).await.unwrap();
    let source = fixture.dir.path().join("uncommitted");

    write_source(&source, 1);
    copy_through(&source, &mount);

    let ack = session.publish().await.unwrap();
    () = session.commit(ack).await.unwrap();

    // A second generation which is published and never committed.
    let discarded = fixture.dir.path().join("uncommitted-discarded");
    write_source(&discarded, 2);
    copy_through(&discarded, &mount);

    assert!(!session.publish().await.unwrap().is_empty());
    drop(session);
    fixture.wait_for_teardown().await;

    let mut session = daemon.session().await;
    let mount = session.open(fixture.open(journal)).await.unwrap();

    assert_tree_matches(&source, &format!("{mount}/data"));
    () = session.close().await;
}

/// A journal holding only the records of a first use which failed holds no
/// committed state, so its disk is formatted afresh.
async fn an_orphaned_first_use_yields_a_fresh_disk(fixture: &Fixture, daemon: &Daemon) {
    let journal = "acmeCo/disk/orphaned";
    let mut session = daemon.session().await;

    let mount = session.open(fixture.open(journal)).await.unwrap();
    copy_through(&fixture.source("orphaned"), &mount);

    assert!(!session.publish().await.unwrap().is_empty());
    drop(session);
    fixture.wait_for_teardown().await;

    assert!(fixture.exists(journal).await, "the delta created a journal");

    let mut session = daemon.session().await;
    let mount = session.open(fixture.open(journal)).await.unwrap();

    assert_eq!(
        sudo(&["ls", "-A", &mount]).trim(),
        "lost+found",
        "the disk was not formatted afresh",
    );
    () = session.close().await;
}

/// The floor label seeks a replay and nothing more: a stale one costs replay
/// work and rebuilds the same disk, while one which cannot be parsed is
/// terminal rather than silently ignored.
async fn the_floor_label_is_only_a_seek_hint(fixture: &Fixture, daemon: &Daemon) {
    let journal = "acmeCo/disk/floored";
    let mut session = daemon.session().await;

    let mount = session.open(fixture.open(journal)).await.unwrap();
    let source = fixture.source("floored");

    copy_through(&source, &mount);
    let ack = session.publish().await.unwrap();

    () = session.commit(ack).await.unwrap();
    () = session.close().await;

    // A clock long before the disk was written, which every fragment is at or
    // after.
    () = fixture.set_floor(journal, "0000000000000001").await;

    let mut session = daemon.session().await;
    let mount = session.open(fixture.open(journal)).await.unwrap();

    assert_tree_matches(&source, &format!("{mount}/data"));
    () = session.close().await;

    () = fixture.set_floor(journal, "nonsense").await;

    let mut session = daemon.session().await;
    let status = session.open(fixture.open(journal)).await.unwrap_err();

    assert!(status.message().contains("malformed"), "{status}");
    () = session.ended().await;
}

/// A boundary cut while the filesystem is writing back rebuilds into a
/// filesystem which mounts and which passes a consistency check, because ext4
/// replays its own journal over whatever the cut caught mid-flight.
async fn a_cut_during_writeback_recovers_a_consistent_filesystem(
    fixture: &Fixture,
    daemon: &Daemon,
) {
    let journal = "acmeCo/disk/writeback";
    let mut session = daemon.session().await;
    let mount = session.open(fixture.open(journal)).await.unwrap();

    // No `fsync`, so the cut lands amongst ext4's own writeback and journal
    // traffic rather than after it.
    let mut writer = std::process::Command::new("sudo")
        .args(["-n", "dd", "if=/dev/urandom", "bs=1M", "count=48"])
        .arg(format!("of={mount}/churn"))
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawning dd");

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let ack = session.publish().await.unwrap();
    () = session.commit(ack).await.unwrap();

    _ = writer.wait().expect("waiting for dd");
    drop(session);
    fixture.wait_for_teardown().await;

    // The daemon mounts what it rebuilds, so opening at all is ext4 having
    // replayed its journal over the rebuilt image.
    let mut session = daemon.session().await;
    let mount = session.open(fixture.open(journal)).await.unwrap();

    _ = sudo(&["ls", "-A", &mount]);
    () = session.close().await;

    let image = fixture.dir.path().join("writeback.img");
    _ = fixture.replay(journal, &image).await;

    () = assert_fsck_clean(&image);
}

/// A client which disappears mid-write leaves no device and no mount behind.
async fn an_abrupt_disconnect_tears_the_disk_down(fixture: &Fixture, daemon: &Daemon) {
    let mut session = daemon.session().await;
    let mount = session
        .open(fixture.open("acmeCo/disk/disconnected"))
        .await
        .unwrap();

    let mut writer = std::process::Command::new("sudo")
        .args([
            "-n",
            "dd",
            "if=/dev/zero",
            "bs=1M",
            "count=64",
            "conv=fsync",
        ])
        .arg(format!("of={mount}/big"))
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawning dd");

    // Long enough that the write is in flight through the device rather than
    // still starting up.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    drop(session);

    _ = writer.wait().expect("waiting for dd");
    fixture.wait_for_teardown().await;
}

/// A daemon which is killed outright leaves nothing behind once a daemon takes
/// its directory again.
///
/// The kernel removes the block device when the server it was served by dies,
/// so nothing can reach the disk. What a killed daemon cannot do is unmount the
/// filesystem over that device or delete the character device under it, so the
/// next daemon reclaims both.
async fn a_killed_daemon_leaves_no_mounts(fixture: &Fixture) {
    let daemon = Daemon::start(fixture, "killed").await;
    let mut session = daemon.session().await;

    let mount = session
        .open(fixture.open("acmeCo/disk/killed"))
        .await
        .unwrap();

    copy_through(&fixture.source("once"), &mount);
    () = daemon.kill().await;

    let dev_id = mount
        .rsplit_once("disk-")
        .expect("a mount names its device")
        .1;
    assert!(
        !std::path::Path::new(&format!("/sys/block/ublkb{dev_id}")).exists(),
        "the kernel left a block device with no server",
    );
    assert!(is_mounted(&mount), "the kill unmounted something");

    let daemon = Daemon::start(fixture, "killed").await;
    assert!(!is_mounted(&mount), "the daemon did not reclaim its mount");
    assert!(
        !std::path::Path::new(&format!("/dev/ublkc{dev_id}")).exists(),
        "the daemon did not delete the device its mount named",
    );

    () = daemon.drain().await;

    // Nothing is left for a reaper to find, because the daemon deleted the one
    // device it could prove was its own.
    assert_eq!(reap_dead_devices(), 0);
    fixture.assert_no_leaks();
}

/// Delete the devices a killed daemon left, and return how many there were.
fn reap_dead_devices() -> usize {
    let report = sudo(&[env!("CARGO_BIN_EXE_disk-daemon-scenario"), "reap"]);
    let report: serde_json::Value = serde_json::from_str(&report).expect("a JSON report");

    report["deleted"].as_array().expect("deleted devices").len()
}

fn is_mounted(path: &str) -> bool {
    std::fs::read_to_string("/proc/mounts")
        .unwrap()
        .lines()
        .any(|line| line.contains(path))
}

struct Fixture {
    dir: tempfile::TempDir,
    endpoint: String,
    credential: String,
    /// Client of the test itself, which reads journals back.
    client: gazette::journal::Client,
}

impl Fixture {
    /// Open of a disk stored in the test broker's file root.
    fn open(&self, journal: &str) -> proto::Open {
        proto::Open {
            journal_config: Some(proto::JournalConfig {
                journal: journal.to_string(),
                fragment_stores: vec!["file:///".to_string()],
                replication: 1,
                labels: Vec::new(),
                fragment_length: 1 << 20,
                flush_interval_seconds: Some(48 * 3600),
                refresh_interval_seconds: 5 * 60,
                max_append_rate: Some(1 << 22),
                // The codec the design specifies for disk journals. Its
                // fragments live on the broker's own filesystem, which the test
                // has no transport to fetch, so the broker is what decompresses
                // them here; `gazette::journal::read` covers its own decoder.
                compression_codec: proto_gazette::broker::CompressionCodec::Snappy as i32,
            }),
            device_size: DEVICE_SIZE,
            block_size: BLOCK_SIZE,
            broker: Some(proto::Broker {
                endpoint: self.endpoint.clone(),
                credential: self.credential.clone(),
            }),
            recovered_acks: Vec::new(),
        }
    }

    /// A source tree of files, which a case copies through a mount.
    fn source(&self, name: &str) -> std::path::PathBuf {
        let path = self.dir.path().join(name);
        write_source(&path, 1);
        path
    }

    /// Apply every committed delta of `journal` to a fresh image at `path`, and
    /// return the blocks they covered.
    ///
    /// This is the recovering session of a later phase, in miniature: records
    /// are grouped by the acknowledgement which commits them, and a delta which
    /// was never acknowledged is dropped.
    async fn replay(&self, journal: &str, path: &std::path::Path) -> usize {
        let image = std::fs::File::create(path).expect("creating a replay image");
        image.set_len(DEVICE_SIZE).expect("sizing a replay image");

        let mut allocated = Bitmap::new((DEVICE_SIZE / BLOCK_SIZE as u64) as u32);
        let mut delta: Vec<proto::Chunk> = Vec::new();
        let mut covered = 0;

        for (flags, record) in self.read(journal).await {
            if flags.is_outside() {
                assert!(record.chunks.is_empty(), "a fence carries no chunks");
                continue;
            }
            delta.extend(record.chunks);

            if !flags.is_ack() {
                continue;
            }
            for chunk in delta.drain(..) {
                covered += chunk::covered_blocks(&chunk, BLOCK_SIZE).len();
                chunk::apply(&chunk, BLOCK_SIZE, &image, &mut allocated).expect("applying a chunk");
            }
        }
        image.sync_all().expect("syncing a replay image");

        covered
    }

    /// Mount `image` and compare what it holds with the `source` tree which was
    /// copied through the disk.
    fn assert_content_matches(&self, image: &std::path::Path, source: &std::path::Path) {
        let mount = self.dir.path().join("replay-mnt");
        std::fs::create_dir_all(&mount).unwrap();

        // A loop mount is how an unprivileged test reads a filesystem which
        // only the kernel can interpret. It replays the ext4 journal, exactly
        // as a recovered disk's mount does.
        sudo(&["mount", "-o", "loop", path(image), path(&mount)]);

        let diff = tree_diff(source, path(&mount.join("data")));
        let _ = sudo(&["umount", path(&mount)]);

        assert!(diff.is_none(), "{}", diff.unwrap());
    }

    /// Write the daemon's floor label onto `journal`'s spec, which is what
    /// completing a horizon will do.
    async fn set_floor(&self, journal: &str, value: &str) {
        let listing = self
            .client
            .list(broker::ListRequest {
                selector: Some(broker::LabelSelector {
                    include: Some(broker::LabelSet {
                        labels: vec![broker::Label {
                            name: "name".to_string(),
                            value: journal.to_string(),
                            prefix: false,
                        }],
                    }),
                    exclude: None,
                }),
                ..Default::default()
            })
            .await
            .expect("listing a journal");

        let listed = &listing.journals[0];
        let mut spec = listed.spec.clone().expect("a listed journal has a spec");

        let labels = &mut spec.labels.get_or_insert_default().labels;
        labels.retain(|label| label.name != FLOOR_LABEL);

        labels.push(broker::Label {
            name: FLOOR_LABEL.to_string(),
            value: value.to_string(),
            prefix: false,
        });

        self.client
            .apply(broker::ApplyRequest {
                changes: vec![broker::apply_request::Change {
                    expect_mod_revision: listed.mod_revision,
                    upsert: Some(spec),
                    delete: String::new(),
                }],
            })
            .await
            .expect("applying a floor label");
    }

    /// Every record of `journal`, paired with the flags of its UUID.
    async fn read(&self, journal: &str) -> Vec<(uuid::Flags, proto::DiskRecord)> {
        let head = self.head(journal).await;

        let mut content = bytes::BytesMut::new();
        let stream = self.client.clone().read(broker::ReadRequest {
            journal: journal.to_string(),
            offset: 0,
            end_offset: head,
            block: false,
            ..Default::default()
        });
        futures::pin_mut!(stream);

        while let Some(response) = futures::StreamExt::next(&mut stream).await {
            content.extend_from_slice(&response.expect("reading a journal").content);
        }

        let mut records = Vec::new();
        let mut rest = &content[..];

        while !rest.is_empty() {
            match framing::decode::<proto::DiskRecord>(rest).expect("a record decodes") {
                framing::Frame::Record { message, consumed } => {
                    let uuid = uuid::Uuid::from_slice(&message.uuid).unwrap();
                    let (_producer, _clock, flags) = uuid::parse(uuid).unwrap();

                    records.push((flags, message));
                    rest = &rest[consumed..];
                }
                frame => panic!("expected a record of {journal}, got {frame:?}"),
            }
        }
        records
    }

    /// Broker-confirmed write head, which bounds a read and which is unchanged
    /// by a transaction that appended nothing.
    async fn head(&self, journal: &str) -> i64 {
        disk_daemon::journal::fence::probe(&self.client, journal)
            .await
            .expect("probing a journal")
            .head
    }

    async fn exists(&self, journal: &str) -> bool {
        disk_daemon::journal::fence::probe(&self.client, journal)
            .await
            .expect("probing a journal")
            .exists
    }

    fn assert_no_leaks(&self) {
        common::assert_no_leaked_devices();
        common::assert_no_mounts_under(path(self.dir.path()));
    }

    /// Wait for a teardown which the session cannot report, because the client
    /// or the daemon is gone.
    async fn wait_for_teardown(&self) {
        let deadline = std::time::Instant::now() + TEARDOWN;

        while std::time::Instant::now() < deadline {
            if std::fs::read_dir("/sys/block")
                .unwrap()
                .filter_map(Result::ok)
                .all(|entry| !entry.file_name().to_string_lossy().starts_with("ublkb"))
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        self.assert_no_leaks();
    }
}

/// A `flow-disk-daemon` process.
struct Daemon {
    uds_path: std::path::PathBuf,
    child: async_process::Child,
}

impl Daemon {
    async fn start(fixture: &Fixture, name: &str) -> Self {
        let dir = fixture.dir.path();
        let uds_path = dir.join(format!("{name}.sock"));

        for sub in ["images", "mounts"] {
            std::fs::create_dir_all(dir.join(format!("{name}-{sub}"))).unwrap();
        }
        let mut command = async_process::Command::new("sudo");
        command
            .args(["-n", env!("CARGO_BIN_EXE_flow-disk-daemon")])
            .arg("--uds-path")
            .arg(&uds_path)
            .arg("--image-dir")
            .arg(dir.join(format!("{name}-images")))
            .arg("--mount-dir")
            .arg(dir.join(format!("{name}-mounts")))
            .args(["--floor-label", FLOOR_LABEL]);

        let child: async_process::Child = command.spawn().expect("spawning the daemon").into();

        let daemon = Self { uds_path, child };
        () = daemon.wait_until_serving().await;

        daemon
    }

    async fn wait_until_serving(&self) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);

        while std::time::Instant::now() < deadline {
            if self.connect().await.is_ok() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        panic!("the daemon did not listen on {:?}", self.uds_path);
    }

    async fn connect(&self) -> Result<tonic::transport::Channel, tonic::transport::Error> {
        tonic::transport::Endpoint::from_shared(format!("unix://{}", self.uds_path.display()))
            .expect("a socket path is a URI")
            .connect()
            .await
    }

    async fn session(&self) -> Session {
        let mut client =
            proto_grpc::disk::disk_client::DiskClient::new(self.connect().await.expect("connect"));

        let (requests, receiver) = tokio::sync::mpsc::channel(1);
        let responses = client
            .session(tokio_stream::wrappers::ReceiverStream::new(receiver))
            .await
            .expect("opening a session")
            .into_inner();

        Session {
            requests,
            responses,
        }
    }

    /// End the daemon as systemd does, and wait for it to have torn down every
    /// disk it served.
    async fn drain(mut self) {
        () = self.signal("TERM");
        let status = self.child.wait().await.expect("waiting for the daemon");

        assert!(status.success(), "the daemon drained with {status}");
    }

    /// End the daemon the way it cannot handle.
    async fn kill(mut self) {
        () = self.signal("KILL");
        _ = self.child.wait().await.expect("waiting for the daemon");
    }

    /// Signal the daemon, which runs as root and so is only reachable through
    /// `sudo`. The pattern matches the daemon and not the `sudo` which spawned
    /// it, whose command line holds the same socket path.
    fn signal(&self, signal: &str) {
        _ = std::process::Command::new("sudo")
            .args(["-n", "pkill", &format!("-{signal}"), "-f"])
            .arg(format!(
                "^{} --uds-path {}",
                env!("CARGO_BIN_EXE_flow-disk-daemon"),
                self.uds_path.display(),
            ))
            .status()
            .expect("spawning pkill");
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        // A test which failed part way leaves a privileged process behind
        // otherwise, and the next test's leak checks would see its devices.
        self.signal("KILL");
    }
}

/// One session's stream, which serves exactly one disk.
struct Session {
    requests: tokio::sync::mpsc::Sender<proto::Request>,
    responses: tonic::Streaming<proto::Response>,
}

impl Session {
    /// Create the disk and return its mount path.
    async fn open(&mut self, open: proto::Open) -> tonic::Result<String> {
        match self.request(proto::request::Request::Open(open)).await? {
            proto::response::Response::Opened(opened) => Ok(opened.mount_path),
            response => panic!("expected Opened, got {response:?}"),
        }
    }

    /// Cut a delta and return its acknowledgement, which is empty when the disk
    /// did not change.
    async fn publish(&mut self) -> tonic::Result<bytes::Bytes> {
        match self
            .request(proto::request::Request::Publish(proto::Publish {}))
            .await?
        {
            proto::response::Response::Published(published) => Ok(published.ack),
            response => panic!("expected Published, got {response:?}"),
        }
    }

    async fn commit(&mut self, ack: bytes::Bytes) -> tonic::Result<()> {
        match self
            .request(proto::request::Request::Commit(proto::Commit { ack }))
            .await?
        {
            proto::response::Response::Committed(proto::Committed {}) => Ok(()),
            response => panic!("expected Committed, got {response:?}"),
        }
    }

    async fn request(
        &mut self,
        request: proto::request::Request,
    ) -> tonic::Result<proto::response::Response> {
        () = self.send(request).await;

        match self.responses.message().await? {
            Some(proto::Response { response }) => Ok(response.expect("a reply carries a message")),
            None => Err(tonic::Status::unknown("the session ended without a reply")),
        }
    }

    async fn send(&self, request: proto::request::Request) {
        self.requests
            .send(proto::Request {
                request: Some(request),
            })
            .await
            .expect("the session is open");
    }

    /// End the session as a client does, and wait for the daemon to finish
    /// tearing its disk down.
    async fn close(mut self) {
        drop(self.requests);
        assert_eq!(self.responses.message().await.expect("a clean close"), None,);
    }

    /// Wait for a failed session to end, which the daemon does only once the
    /// disk is destroyed.
    async fn ended(mut self) {
        drop(self.requests);
        assert!(matches!(self.responses.message().await, Ok(None) | Err(_)));
    }
}

/// Copy a source tree onto the disk mounted at `mount`, replacing what an
/// earlier generation put there.
fn copy_through(source: &std::path::Path, mount: &str) {
    sudo(&["cp", "-rT", path(source), &format!("{mount}/data")]);
}

/// Require `dir`, which a mount holds, to be exactly the `source` tree.
fn assert_tree_matches(source: &std::path::Path, dir: &str) {
    if let Some(diff) = tree_diff(source, dir) {
        panic!("{diff}");
    }
}

/// How `dir` differs from the `source` tree, or `None` when it does not. The
/// mounts are root-owned, so comparing them is privileged too.
fn tree_diff(source: &std::path::Path, dir: &str) -> Option<String> {
    let diff = std::process::Command::new("sudo")
        .args(["-n", "diff", "-r"])
        .arg(source)
        .arg(dir)
        .output()
        .expect("spawning diff");

    if diff.status.success() {
        return None;
    }
    Some(format!(
        "{dir} differs from what was written:\n{}{}",
        String::from_utf8_lossy(&diff.stdout),
        String::from_utf8_lossy(&diff.stderr),
    ))
}

/// Require `image` to hold a filesystem which is consistent once its own
/// journal is replayed, and which needs no repair beyond that.
fn assert_fsck_clean(image: &std::path::Path) {
    // The first pass replays the filesystem journal, which modifies the image
    // and which e2fsck reports as one. The second must find nothing at all.
    for (args, may_modify) in [("-fy", true), ("-fn", false)] {
        let output = std::process::Command::new("sudo")
            .args(["-n", "e2fsck", args])
            .arg(image)
            .output()
            .expect("spawning e2fsck");

        let code = output.status.code().unwrap_or(-1);
        assert!(
            code == 0 || (code == 1 && may_modify),
            "e2fsck {args} exited {code} over {image:?}:\n{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
}

/// Run a privileged command, which is how a test reaches a root-owned mount.
fn sudo(args: &[&str]) -> String {
    let output = std::process::Command::new("sudo")
        .arg("-n")
        .args(args)
        .output()
        .unwrap_or_else(|err| panic!("spawning sudo {args:?}: {err}"));

    assert!(
        output.status.success(),
        "sudo {args:?} failed ({}): {}{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// Write a source tree whose content exercises the chunk codec: a file smaller
/// than a block, a file of whole blocks, an entirely zero file, and one large
/// enough to span records.
fn write_source(dir: &std::path::Path, generation: u8) {
    _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).expect("creating a source tree");

    let block = BLOCK_SIZE as usize;
    let files: [(&str, Vec<u8>); 4] = [
        ("small", pattern(generation, 11)),
        ("one-block", pattern(generation.wrapping_add(1), block)),
        ("all-zeroes", vec![0; 3 * block]),
        (
            "large",
            pattern(generation.wrapping_add(2), 3 * (1 << 20) + 17),
        ),
    ];

    for (name, content) in files {
        std::fs::write(dir.join(name), content).expect("writing a source file");
    }
}

/// Content in which every third block is entirely zero, so that trailing zero
/// trimming and empty-data chunks both occur.
fn pattern(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|index| {
            if (index / BLOCK_SIZE as usize) % 3 == 2 {
                0
            } else {
                seed.wrapping_add((index % 251) as u8)
            }
        })
        .collect()
}

fn path(path: &std::path::Path) -> &str {
    path.to_str().expect("paths of a tempdir are UTF-8")
}

/// Sign a token carrying the capabilities a disk journal writer needs.
fn credential(cluster: &e2e_support::GazetteCluster) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let claims = proto_gazette::Claims {
        cap: proto_gazette::capability::LIST
            | proto_gazette::capability::APPLY
            | proto_gazette::capability::READ
            | proto_gazette::capability::APPEND,
        exp: now + 3600,
        iat: now,
        iss: "disk-daemon-test".to_string(),
        sel: broker::LabelSelector::default(),
        sub: "disk-daemon-test".to_string(),
    };

    tokens::jwt::sign(claims, &cluster.encode_key).unwrap()
}
